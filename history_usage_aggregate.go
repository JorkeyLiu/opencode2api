package main

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Bounded range usage aggregate over persisted request history.
//
// Grain (operator-visible):
//   - calls counts every valid request line in [from,to] (current UI call
//     semantics), regardless of usage_reported.
//   - usage_calls counts only usage_reported lines; all token sums accumulate
//     only usage_reported lines. No estimation; old usage lines without the
//     explicit usage_detail_complete marker stay zero and set
//     legacy_incomplete. Each model/upstream row additionally carries
//     total_complete/reasoning_complete: false when any included
//     usage-bearing record lacks complete detail (UI renders —), true when
//     all included usage records are complete (including legitimate zero)
//     or when the row has no usage-bearing records.
//
// Identity: per model plus per upstream where upstream is the tier for
// zen/go and the full channel for tier custom (legacy "custom" keeps its
// merged row, "custom:<id>" splits per channel). Only identities present
// in range are returned, sorted deterministically (total_tokens desc, then
// name asc) with a bounded limit. Over-limit sets truncated=true with
// total_models/total_upstreams preserved, never silent loss.
// Emits no key/session/proxy/url/body/error fields.

const (
	historyUsageAggregateDefaultLimit = 200
	historyUsageAggregateMaxLimit     = 500
)

type usageAggregateRow struct {
	Calls      uint64 `json:"calls"`
	UsageCalls uint64 `json:"usage_calls"`
	Input      uint64 `json:"input_tokens"`
	Output     uint64 `json:"output_tokens"`
	Cached     uint64 `json:"cached_tokens"`
	Reasoning  uint64 `json:"reasoning_tokens"`
	Total      uint64 `json:"total_tokens"`
	// Row-level completeness for total/reasoning detail. False when any
	// included usage-bearing history record lacks complete usage detail
	// (UsageDetailComplete=false / legacy row predating persisted
	// total/reasoning fields); the summed numeric total/reasoning is then a
	// partial sum and the UI must render — instead of 0 or the partial sum.
	// True when all included usage records are complete (including
	// legitimate zero), or when the row has no usage-bearing records.
	// Additive: old clients ignore these fields.
	TotalComplete     bool `json:"total_complete"`
	ReasoningComplete bool `json:"reasoning_complete"`
}

type usageAggregateModel struct {
	Model string `json:"model"`
	usageAggregateRow
}

type usageAggregateUpstream struct {
	Upstream string `json:"upstream"`
	usageAggregateRow
}

type usageAggregateTotals struct {
	Calls      uint64 `json:"calls"`
	UsageCalls uint64 `json:"usage_calls"`
	Input      uint64 `json:"input_tokens"`
	Output     uint64 `json:"output_tokens"`
	Cached     uint64 `json:"cached_tokens"`
	Reasoning  uint64 `json:"reasoning_tokens"`
	Total      uint64 `json:"total_tokens"`
}

type usageAggregateResponse struct {
	Models           []usageAggregateModel    `json:"models"`
	Upstreams        []usageAggregateUpstream `json:"upstreams"`
	Totals           usageAggregateTotals     `json:"totals"`
	TotalModels      int                      `json:"total_models"`
	TotalUpstreams   int                      `json:"total_upstreams"`
	Truncated        bool                     `json:"truncated"`
	Gap              bool                     `json:"gap"`
	Dropped          uint64                   `json:"dropped"`
	Active           bool                     `json:"active"`
	LastError        string                   `json:"last_error,omitempty"`
	From             string                   `json:"from"`
	To               string                   `json:"to"`
	LegacyIncomplete bool                     `json:"legacy_incomplete"`
}

type usageAggregateAccum struct {
	calls      uint64
	usageCalls uint64
	input      uint64
	output     uint64
	cached     uint64
	reasoning  uint64
	total      uint64
	// Set when any included usage-bearing record lacks complete detail.
	// Completeness output is !incomplete (vacuously complete when no usage).
	totalIncomplete     bool
	reasoningIncomplete bool
}

func historyUsageUpstream(tier, channel string) string {
	t := strings.TrimSpace(tier)
	c := strings.TrimSpace(channel)
	if strings.EqualFold(t, string(TierCustom)) {
		if c == "" {
			return string(TierCustom)
		}
		return c
	}
	if t != "" {
		return t
	}
	if c != "" {
		return c
	}
	return "unknown"
}

func queryHistoryUsageAggregate(ctx context.Context, store *HistoryStore, from, to time.Time, limit int) (usageAggregateResponse, error) {
	st := store.Status()
	limit = normalizeHistoryLimit(limit, historyUsageAggregateDefaultLimit, historyUsageAggregateMaxLimit)
	if err := historyCheckCtx(ctx); err != nil {
		return usageAggregateResponse{}, err
	}
	dir := store.historyReadDir()
	if dir == "" {
		return usageAggregateResponse{
			Models: []usageAggregateModel{}, Upstreams: []usageAggregateUpstream{},
			From: from.UTC().Format(time.RFC3339Nano), To: to.UTC().Format(time.RFC3339Nano),
			Gap: false, Dropped: st.Dropped, Active: st.Active, LastError: st.LastError,
		}, nil
	}
	models := make(map[string]*usageAggregateAccum)
	upstreams := make(map[string]*usageAggregateAccum)
	var totals usageAggregateTotals
	legacyIncomplete := false
	gap := false
	for _, name := range historySegmentNamesFor(dir, historyKindRequest, from, to) {
		if err := historyCheckCtx(ctx); err != nil {
			return usageAggregateResponse{}, err
		}
		lines, segGap, err := readHistorySegmentNewestFirst(ctx, joinHistoryPath(dir, name))
		if err != nil {
			return usageAggregateResponse{}, err
		}
		if segGap {
			gap = true
		}
		for _, line := range lines {
			if err := historyCheckCtx(ctx); err != nil {
				return usageAggregateResponse{}, err
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
			model := strings.TrimSpace(v.Model)
			if model == "" {
				gap = true
				continue
			}
			upstream := historyUsageUpstream(v.Tier, v.Channel)
			macc, ok := models[model]
			if !ok {
				macc = &usageAggregateAccum{}
				models[model] = macc
			}
			uacc, ok := upstreams[upstream]
			if !ok {
				uacc = &usageAggregateAccum{}
				upstreams[upstream] = uacc
			}
			macc.calls++
			uacc.calls++
			totals.Calls++
			if !v.UsageReported {
				continue
			}
			macc.usageCalls++
			uacc.usageCalls++
			totals.UsageCalls++
			input := uint64(max(v.InputTokens, 0))
			output := uint64(max(v.OutputTokens, 0))
			cached := uint64(max(v.CacheHitTokens, 0))
			reasoning := uint64(max(v.ReasoningTokens, 0))
			total := uint64(max(v.TotalTokens, 0))
			macc.input += input
			macc.output += output
			macc.cached += cached
			macc.reasoning += reasoning
			macc.total += total
			uacc.input += input
			uacc.output += output
			uacc.cached += cached
			uacc.reasoning += reasoning
			uacc.total += total
			totals.Input += input
			totals.Output += output
			totals.Cached += cached
			totals.Reasoning += reasoning
			totals.Total += total
			// Explicit marker only: UsageReported=true without
			// usage_detail_complete means an old line whose zero
			// reasoning/total are incomplete. New complete writes set the
			// marker even when reasoning/total are zero, so zeros stay
			// complete. Never infer from omitempty keys or zero values.
			// Never synthesize totals from input+output: keep the persisted
			// (possibly zero) total/reasoning sums and mark the row
			// incomplete instead.
			if v.UsageReported && !v.UsageDetailComplete {
				if !legacyIncomplete {
					legacyIncomplete = true
				}
				macc.totalIncomplete = true
				macc.reasoningIncomplete = true
				uacc.totalIncomplete = true
				uacc.reasoningIncomplete = true
			}
		}
	}
	if store.GapFlag() {
		gap = true
	}
	modelRows := make([]usageAggregateModel, 0, len(models))
	for name, acc := range models {
		modelRows = append(modelRows, usageAggregateModel{
			Model: name,
			usageAggregateRow: usageAggregateRow{
				Calls: acc.calls, UsageCalls: acc.usageCalls,
				Input: acc.input, Output: acc.output, Cached: acc.cached,
				Reasoning: acc.reasoning, Total: acc.total,
				TotalComplete: !acc.totalIncomplete, ReasoningComplete: !acc.reasoningIncomplete,
			},
		})
	}
	sort.Slice(modelRows, func(i, j int) bool {
		if modelRows[i].Total != modelRows[j].Total {
			return modelRows[i].Total > modelRows[j].Total
		}
		return modelRows[i].Model < modelRows[j].Model
	})
	upRows := make([]usageAggregateUpstream, 0, len(upstreams))
	for name, acc := range upstreams {
		upRows = append(upRows, usageAggregateUpstream{
			Upstream: name,
			usageAggregateRow: usageAggregateRow{
				Calls: acc.calls, UsageCalls: acc.usageCalls,
				Input: acc.input, Output: acc.output, Cached: acc.cached,
				Reasoning: acc.reasoning, Total: acc.total,
				TotalComplete: !acc.totalIncomplete, ReasoningComplete: !acc.reasoningIncomplete,
			},
		})
	}
	sort.Slice(upRows, func(i, j int) bool {
		if upRows[i].Total != upRows[j].Total {
			return upRows[i].Total > upRows[j].Total
		}
		return upRows[i].Upstream < upRows[j].Upstream
	})
	totalModels := len(modelRows)
	totalUpstreams := len(upRows)
	truncated := false
	if len(modelRows) > limit {
		modelRows = modelRows[:limit]
		truncated = true
	}
	if len(upRows) > limit {
		upRows = upRows[:limit]
		truncated = true
	}
	if modelRows == nil {
		modelRows = []usageAggregateModel{}
	}
	if upRows == nil {
		upRows = []usageAggregateUpstream{}
	}
	return usageAggregateResponse{
		Models: modelRows, Upstreams: upRows, Totals: totals,
		TotalModels: totalModels, TotalUpstreams: totalUpstreams, Truncated: truncated,
		Gap: gap, Dropped: st.Dropped, Active: st.Active, LastError: st.LastError,
		From: from.UTC().Format(time.RFC3339Nano), To: to.UTC().Format(time.RFC3339Nano),
		LegacyIncomplete: legacyIncomplete,
	}, nil
}

func disabledUsageAggregate(s *HistoryStore) usageAggregateResponse {
	if s == nil {
		return usageAggregateResponse{Models: []usageAggregateModel{}, Upstreams: []usageAggregateUpstream{}}
	}
	st := s.Status()
	return usageAggregateResponse{
		Models: []usageAggregateModel{}, Upstreams: []usageAggregateUpstream{},
		Gap: st.Gap, Dropped: st.Dropped, Active: false, LastError: st.LastError,
	}
}

func (a *AdminServer) handleHistoryUsageAggregate(w http.ResponseWriter, r *http.Request) {
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
		writeJSON(w, http.StatusOK, usageAggregateResponse{Models: []usageAggregateModel{}, Upstreams: []usageAggregateUpstream{}})
		return
	}
	st := store.Status()
	if !st.EnabledConfig || !st.Active {
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, http.StatusOK, disabledUsageAggregate(store))
		return
	}
	q := r.URL.Query()
	maxRange := historyMaxRange(st.RetentionDays)
	from, to, err := parseHistoryRange(map[string][]string(q), historyDefWindow(24*time.Hour, st.RetentionDays), maxRange)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_range", err.Error())
		return
	}
	limit, err := parseHistoryLimit(map[string][]string(q), historyUsageAggregateDefaultLimit, historyUsageAggregateMaxLimit)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_limit", err.Error())
		return
	}
	res, err := queryHistoryUsageAggregate(ctx, store, from, to, limit)
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
