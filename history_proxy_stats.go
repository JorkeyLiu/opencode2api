package main

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Bounded proxy-observability aggregate over persisted history.
//
// Grain (operator-visible, stated once in WebUI):
//   - Attempt-level: attempts/success/failed/success_rate/avg_duration_ms and
//     the failure breakdown come from attempt records, so retries and failure
//     classes stay observable.
//   - Request-level: input/output/cache tokens and usage_requests come only
//     from completed request records whose usage_reported is true. No
//     estimation; proxies without reported usage expose zeros with
//     has_usage=false and the WebUI renders "—".
//
// Failure buckets (partition of failed attempts, 2xx never fails):
//   - rate_limited: status 429 or rate_limited class.
//   - auth_failures: status 401.
//   - upstream_failures: status 403 or status >= 500.
//   - transport_failures: transport_failure class.
//   - other_failures: any other failure (400/408/425/client rejections, etc.).
// This splits the shared auth_failure class (401+403) by status so 401 stays
// authentication while 403 joins the target/upstream bucket, matching the
// scheduler's 401-credential vs 403/5xx-target cooldown ownership.
//
// Identity is strictly pool-qualified (proxy_pool + already-redacted
// proxy_node). No raw proxy URLs, IPs, or credentials are accepted or
// emitted; nodes are re-redacted defensively and empty nodes fold to
// "unavailable". Only identities present in [from,to] are returned, sorted
// deterministically (attempts desc, then pool/node asc) with a bounded limit.
// Over-limit results set truncated=true with total_proxies preserved, never
// silent loss and never an _other bucket.

const (
	historyProxyStatsDefaultLimit = 200
	historyProxyStatsMaxLimit     = 500
)

type proxyStatRow struct {
	ProxyPool         string  `json:"proxy_pool"`
	ProxyNode         string  `json:"proxy_node"`
	Attempts          uint64  `json:"attempts"`
	Success           uint64  `json:"success"`
	Failed            uint64  `json:"failed"`
	SuccessRate       float64 `json:"success_rate"`
	AvgDurationMS     float64 `json:"avg_duration_ms"`
	RateLimited       uint64  `json:"rate_limited"`
	AuthFailures      uint64  `json:"auth_failures"`
	UpstreamFailures  uint64  `json:"upstream_failures"`
	TransportFailures uint64  `json:"transport_failures"`
	OtherFailures     uint64  `json:"other_failures"`
	UsageRequests     uint64  `json:"usage_requests"`
	InputTokens       uint64  `json:"input_tokens"`
	OutputTokens      uint64  `json:"output_tokens"`
	CacheHitTokens    uint64  `json:"cache_hit_tokens"`
	HasUsage          bool    `json:"has_usage"`
}

type proxyStatsResponse struct {
	Items        []proxyStatRow `json:"items"`
	Truncated    bool           `json:"truncated"`
	TotalProxies int            `json:"total_proxies"`
	Gap          bool           `json:"gap"`
	Dropped      uint64         `json:"dropped"`
	Active       bool           `json:"active"`
	LastError    string         `json:"last_error,omitempty"`
}

type proxyStatAccum struct {
	pool, node   string
	attempts     uint64
	success      uint64
	rateLimited  uint64
	authFailures uint64
	upstream     uint64
	transport    uint64
	other        uint64
	durationSum  int64
	usageReqs    uint64
	inputTokens  uint64
	outputTokens uint64
	cacheHit     uint64
}

func normalizeProxyStatIdentity(pool, node string) (string, string) {
	pool = strings.TrimSpace(pool)
	node = strings.TrimSpace(node)
	if node == "" {
		node = "unavailable"
	}
	if node != "direct" && node != "unavailable" {
		// Defensive re-redaction: stored lines are already redacted, but
		// re-applying the strip is idempotent and prevents any residual
		// userinfo from reaching the admin response.
		node = redactURL(node)
		if strings.TrimSpace(node) == "" {
			node = "unavailable"
		}
	}
	return pool, node
}

func proxyStatKey(pool, node string) string { return pool + "\x00" + node }

func queryHistoryProxyStats(ctx context.Context, store *HistoryStore, from, to time.Time, limit int) (proxyStatsResponse, error) {
	st := store.Status()
	limit = normalizeHistoryLimit(limit, historyProxyStatsDefaultLimit, historyProxyStatsMaxLimit)
	if err := historyCheckCtx(ctx); err != nil {
		return proxyStatsResponse{}, err
	}
	dir := store.historyReadDir()
	if dir == "" {
		return proxyStatsResponse{Items: []proxyStatRow{}, Gap: false, Dropped: st.Dropped, Active: st.Active, LastError: st.LastError}, nil
	}
	accums := make(map[string]*proxyStatAccum)
	gap := false

	// Attempts: attempt-level calls/success/failures/latency/breakdown.
	for _, name := range historySegmentNamesFor(dir, historyKindAttempt, from, to) {
		if err := historyCheckCtx(ctx); err != nil {
			return proxyStatsResponse{}, err
		}
		lines, segGap, err := readHistorySegmentNewestFirst(ctx, joinHistoryPath(dir, name))
		if err != nil {
			return proxyStatsResponse{}, err
		}
		if segGap {
			gap = true
		}
		for _, line := range lines {
			if err := historyCheckCtx(ctx); err != nil {
				return proxyStatsResponse{}, err
			}
			var env historyEnvelope
			if err := json.Unmarshal(line, &env); err != nil || env.V != historySchemaV || env.Kind != string(historyKindAttempt) {
				gap = true
				continue
			}
			var v historyAttemptLine
			if err := json.Unmarshal(line, &v); err != nil {
				gap = true
				continue
			}
			t, err := time.Parse(time.RFC3339Nano, v.Time)
			if err != nil {
				gap = true
				continue
			}
			if t.Before(from) || t.After(to) {
				continue
			}
			pool, node := normalizeProxyStatIdentity(v.ProxyPool, v.ProxyNode)
			key := proxyStatKey(pool, node)
			acc, ok := accums[key]
			if !ok {
				acc = &proxyStatAccum{pool: pool, node: node}
				accums[key] = acc
			}
			acc.attempts++
			acc.durationSum += max(v.DurationMS, 0)
			isSuccess := v.Success || (v.Status >= 200 && v.Status < 300)
			if isSuccess {
				acc.success++
				continue
			}
			switch {
			case v.Status == 429 || v.FailureClass == AttemptClassRateLimited:
				acc.rateLimited++
			case v.Status == 401:
				acc.authFailures++
			case v.Status == 403 || v.Status >= 500:
				acc.upstream++
			case v.FailureClass == AttemptClassTransportFailure:
				acc.transport++
			default:
				acc.other++
			}
		}
	}

	// Requests: request-level returned usage only when usage_reported.
	for _, name := range historySegmentNamesFor(dir, historyKindRequest, from, to) {
		if err := historyCheckCtx(ctx); err != nil {
			return proxyStatsResponse{}, err
		}
		lines, segGap, err := readHistorySegmentNewestFirst(ctx, joinHistoryPath(dir, name))
		if err != nil {
			return proxyStatsResponse{}, err
		}
		if segGap {
			gap = true
		}
		for _, line := range lines {
			if err := historyCheckCtx(ctx); err != nil {
				return proxyStatsResponse{}, err
			}
			var env historyEnvelope
			if err := json.Unmarshal(line, &env); err != nil || env.V != historySchemaV || env.Kind != string(historyKindRequest) {
				gap = true
				continue
			}
			var v historyRequestLine
			if err := json.Unmarshal(line, &v); err != nil {
				gap = true
				continue
			}
			t, err := time.Parse(time.RFC3339Nano, v.Time)
			if err != nil {
				gap = true
				continue
			}
			if t.Before(from) || t.After(to) {
				continue
			}
			if !v.UsageReported {
				continue
			}
			pool, node := normalizeProxyStatIdentity(v.ProxyPool, v.ProxyNode)
			key := proxyStatKey(pool, node)
			acc, ok := accums[key]
			if !ok {
				acc = &proxyStatAccum{pool: pool, node: node}
				accums[key] = acc
			}
			acc.usageReqs++
			acc.inputTokens += uint64(max(v.InputTokens, 0))
			acc.outputTokens += uint64(max(v.OutputTokens, 0))
			acc.cacheHit += uint64(max(v.CacheHitTokens, 0))
		}
	}

	if store.GapFlag() {
		gap = true
	}
	rows := make([]proxyStatRow, 0, len(accums))
	for _, acc := range accums {
		// Include only identities with attempts or reported usage in range.
		// Request-only keys (usage without attempts) are observable via the
		// request-level columns; attempt-only keys show "—" for usage.
		if acc.attempts == 0 && acc.usageReqs == 0 {
			continue
		}
		failed := uint64(0)
		if acc.attempts >= acc.success {
			failed = acc.attempts - acc.success
		}
		row := proxyStatRow{
			ProxyPool: acc.pool, ProxyNode: acc.node,
			Attempts: acc.attempts, Success: acc.success, Failed: failed,
			RateLimited: acc.rateLimited, AuthFailures: acc.authFailures,
			UpstreamFailures: acc.upstream, TransportFailures: acc.transport,
			OtherFailures:  acc.other,
			UsageRequests:  acc.usageReqs,
			InputTokens:    acc.inputTokens,
			OutputTokens:   acc.outputTokens,
			CacheHitTokens: acc.cacheHit,
			HasUsage:       acc.usageReqs > 0,
		}
		if acc.attempts > 0 {
			row.SuccessRate = float64(acc.success) / float64(acc.attempts)
			row.AvgDurationMS = float64(acc.durationSum) / float64(acc.attempts)
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Attempts != rows[j].Attempts {
			return rows[i].Attempts > rows[j].Attempts
		}
		if rows[i].ProxyPool != rows[j].ProxyPool {
			return rows[i].ProxyPool < rows[j].ProxyPool
		}
		return rows[i].ProxyNode < rows[j].ProxyNode
	})
	total := len(rows)
	truncated := false
	if len(rows) > limit {
		rows = rows[:limit]
		truncated = true
	}
	if rows == nil {
		rows = []proxyStatRow{}
	}
	return proxyStatsResponse{
		Items: rows, Truncated: truncated, TotalProxies: total,
		Gap: gap, Dropped: st.Dropped, Active: st.Active, LastError: st.LastError,
	}, nil
}

func disabledProxyStats(s *HistoryStore) proxyStatsResponse {
	if s == nil {
		return proxyStatsResponse{Items: []proxyStatRow{}}
	}
	st := s.Status()
	return proxyStatsResponse{
		Items: []proxyStatRow{}, Gap: st.Gap, Dropped: st.Dropped,
		Active: false, LastError: st.LastError,
	}
}

func (a *AdminServer) handleHistoryProxyStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !a.allowHistory(clientIP(r)) {
		writeAdminError(w, http.StatusTooManyRequests, "history_rate_limited", "too many history requests; retry in one minute")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	store := historyStoreForQuery(a)
	if store == nil {
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, http.StatusOK, proxyStatsResponse{Items: []proxyStatRow{}})
		return
	}
	st := store.Status()
	if !st.EnabledConfig || !st.Active {
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, http.StatusOK, disabledProxyStats(store))
		return
	}
	q := r.URL.Query()
	maxRange := historyMaxRange(st.RetentionDays)
	from, to, err := parseHistoryRange(map[string][]string(q), historyDefWindow(24*time.Hour, st.RetentionDays), maxRange)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_range", err.Error())
		return
	}
	limit, err := parseHistoryLimit(map[string][]string(q), historyProxyStatsDefaultLimit, historyProxyStatsMaxLimit)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_limit", err.Error())
		return
	}
	res, err := queryHistoryProxyStats(ctx, store, from, to, limit)
	if err != nil {
		if writeHistoryTimeout(w, err) {
			return
		}
		writeAdminError(w, http.StatusGatewayTimeout, "history_timeout", "history query timed out")
		return
	}
	select {
	case <-ctx.Done():
		writeAdminError(w, http.StatusGatewayTimeout, "history_timeout", "history query timed out")
	default:
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, http.StatusOK, res)
	}
}
