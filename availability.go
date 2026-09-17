package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Management-plane availability bulk detection.
//
// Design notes (stdlib only, no new dependency):
//   - Bounded concurrency uses a fixed semaphore (bulkProbeConcurrency = 4).
//   - Bounded overall timeout (bulkOverallTimeout) plus bounded per-send
//     timeout (bulkPerSendTimeout). Operation timeout/cancel is diagnostic
//     only and never writes transport health.
//   - Request amplification is bounded: Zen public tests all Zen-channel
//     nodes up to bulkMaxPublicNodes; each configured credential tests at
//     most bulkMaxNodesPerCredential nodes in its assigned pool; total sends
//     are capped at bulkMaxTotalSends. Truncation/skipped counts are reported
//     explicitly, never silently sampled.
//   - State writes are deterministic and comparative only (see applyBulkWrites).
//     All other/ambiguous outcomes are display-only.
//   - The latest-result snapshot is an admin-only in-memory projection stored
//     per-Gateway (atomic pointer). It never drives routing; scheduler layers
//     remain authoritative. Restart clears (fresh Gateway has nil) and Apply
//     clears (new Gateway starts nil).

const (
	bulkProbeConcurrency = 4
	bulkPerSendTimeout   = 10 * time.Second
	bulkOverallTimeout   = 30 * time.Second

	bulkMaxPublicNodes        = 32
	bulkMaxNodesPerCredential = 4
	bulkMaxTotalSends         = 128

	bulkMaxSnapshotNodes = 256
	bulkMaxSnapshotCreds = 128

	bulkRateLimit = 3
)

// bulkNodeAvailability is the per-proxy observation row. Anonymous and
// Authenticated carry the latest real probe outcome for that lane (success
// only on exact HTTP 200 with a valid body; 429 stays rate_limited; other
// HTTP/transport outcomes stay real; no_model/unconfigured/transport etc. are
// machine outcomes, never a generic available/unavailable verdict). The HTTP
// status is present whenever an HTTP response exists.
type bulkNodeAvailability struct {
	Pool                    string `json:"proxy_pool"`
	Index                   int    `json:"index"`
	ProxyNode               string `json:"proxy_node"`
	Transport               string `json:"transport"`
	Anonymous               string `json:"anonymous"`
	AnonymousHTTPStatus     int    `json:"anonymous_http_status,omitempty"`
	Authenticated           string `json:"authenticated"`
	AuthenticatedHTTPStatus int    `json:"authenticated_http_status,omitempty"`
	// Deprecated aliases for test compilation; never serialized.
	Zen         string     `json:"-"`
	Go          string     `json:"-"`
	Reason      string     `json:"reason,omitempty"`
	LastChecked *time.Time `json:"last_checked,omitempty"`
}

type bulkCredentialAvailability struct {
	Tier        string     `json:"tier"`
	KeyTail     string     `json:"key_tail"`
	Fingerprint string     `json:"fingerprint,omitempty"`
	Pool        string     `json:"proxy_pool"`
	Status      string     `json:"status"`
	Reason      string     `json:"reason,omitempty"`
	TestedNodes int        `json:"tested_nodes,omitempty"`
	LastChecked *time.Time `json:"last_checked,omitempty"`
	CredID      string     `json:"-"`
}

type bulkCustomAvailability struct {
	ID          string     `json:"id,omitempty"`
	Name        string     `json:"name"`
	BaseURL     string     `json:"base_url"`
	Model       string     `json:"model"`
	Status      string     `json:"status"`
	Reason      string     `json:"reason,omitempty"`
	LastChecked *time.Time `json:"last_checked,omitempty"`
}

// customAvailabilityKey returns the stable machine key for a custom row:
// the channel ID when present, otherwise the legacy display name.
func customAvailabilityKey(c bulkCustomAvailability) string {
	if strings.TrimSpace(c.ID) != "" {
		return strings.TrimSpace(c.ID)
	}
	return strings.TrimSpace(c.Name)
}

type bulkAvailabilitySnapshot struct {
	CheckedAt    time.Time                    `json:"checked_at"`
	TotalNodes   int                          `json:"total_nodes"`
	TestedNodes  int                          `json:"tested_nodes"`
	SkippedNodes int                          `json:"skipped_nodes"`
	Truncated    bool                         `json:"truncated"`
	Partial      bool                         `json:"partial,omitempty"`
	Nodes        []bulkNodeAvailability       `json:"nodes,omitempty"`
	Credentials  []bulkCredentialAvailability `json:"credentials,omitempty"`
	Custom       []bulkCustomAvailability     `json:"custom,omitempty"`
	NoModel      []string                     `json:"no_model,omitempty"`
}

type bulkCheckRequest struct{}

type bulkCheckResponse struct {
	CheckedAt    time.Time                    `json:"checked_at"`
	TotalNodes   int                          `json:"total_nodes"`
	TestedNodes  int                          `json:"tested_nodes"`
	SkippedNodes int                          `json:"skipped_nodes"`
	Truncated    bool                         `json:"truncated"`
	Partial      bool                         `json:"partial,omitempty"`
	Error        string                       `json:"error,omitempty"`
	Nodes        []bulkNodeAvailability       `json:"nodes,omitempty"`
	Credentials  []bulkCredentialAvailability `json:"credentials,omitempty"`
	Custom       []bulkCustomAvailability     `json:"custom,omitempty"`
	NoModel      []string                     `json:"no_model,omitempty"`
}

type bulkSendTarget struct {
	PoolName string
	Index    int
	Proxy    *proxyTransport
	Raw      string
	Tier     Tier
	CredKey  string
	CredID   string
	CredDisp string
	IsPublic bool
	// ProbeModel/ProbeProtocol select the real minimal inference request for
	// this tier. Empty ProbeModel means no directory model was available and
	// the target must not be built (caller reports no_model instead).
	ProbeModel    string
	ProbeProtocol Protocol
}

type bulkSendResult struct {
	Target             bulkSendTarget
	StartedNanos       int64
	DurationMS         int64
	TransportErr       error
	IsProxyFailure     bool
	AdminCancelled     bool
	ProbeContextCaused bool
	Status             int
	RetryAfter         time.Duration
	Success            bool
	ParseError         bool
	Models             int
}

func (a *AdminServer) allowBulk(client string) bool {
	return a.allowWindow(&a.bulkAttempts, client, bulkRateLimit, rateLimitWindowMinute)
}

func allBulkCancelled(results []bulkSendResult) bool {
	if len(results) == 0 {
		return true
	}
	for _, r := range results {
		if !r.AdminCancelled {
			return false
		}
	}
	return true
}

// decodeBulkCheckRequest accepts only exactly one empty JSON object `{}` with
// optional whitespace. JSON null, arrays, scalars, unknown fields, and trailing
// values are all rejected.
func decodeBulkCheckRequest(w http.ResponseWriter, r *http.Request) error {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 2<<20))
	if err != nil {
		return err
	}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return errors.New("request body must be exactly one empty JSON object {}")
	}
	inner := bytes.TrimSpace(trimmed[1 : len(trimmed)-1])
	if len(inner) != 0 {
		return errors.New("request body must be exactly one empty JSON object {}")
	}
	return nil
}

func (a *AdminServer) handleBulkCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	client := clientIP(r)
	if !a.allowBulk(client) {
		writeAdminError(w, http.StatusTooManyRequests, "bulk_rate_limited", "too many availability checks; retry in one minute")
		return
	}
	if err := decodeBulkCheckRequest(w, r); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	runtime := a.manager.current.Load()
	if runtime == nil || runtime.gateway == nil {
		writeAdminError(w, http.StatusServiceUnavailable, "gateway_unavailable", "gateway runtime is unavailable")
		return
	}
	gateway := runtime.gateway
	if !gateway.bulkMu.TryLock() {
		writeAdminError(w, http.StatusConflict, "bulk_busy", "an availability check is already running")
		return
	}
	defer gateway.bulkMu.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), bulkOverallTimeout)
	defer cancel()
	resp := gateway.runBulkCheck(ctx)
	if ctx.Err() != nil && resp.TestedNodes == 0 {
		// Overall timeout/cancel with nothing tested is still a partial
		// diagnostic result, never a transport/state write beyond what
		// already completed sends established.
		resp.Partial = true
	}
	writeJSON(w, http.StatusOK, resp)
}

func (g *Gateway) runBulkCheck(ctx context.Context) bulkCheckResponse {
	checkedAt := time.Now().UTC()
	// Snapshot live gateway/config at operation start. Exactly two channels:
	// anonymous and authenticated, both Zen upstream.
	authPoolName := g.authPoolName()
	anonPoolName := g.cfg.ProxyRouting.Anonymous
	authCreds := append([]credentialRef(nil), g.credentials()...)
	// Enumerate active pool-qualified nodes including unhealthy ones.
	type nodeRef struct {
		poolName string
		index    int
		proxy    *proxyTransport
		raw      string
	}
	poolNodes := make(map[string][]nodeRef)
	totalNodes := 0
	for _, poolName := range g.cfg.UniqueActivePools() {
		pool := g.pools[poolName]
		if pool == nil {
			continue
		}
		for _, proxy := range pool.items {
			if proxy == nil {
				continue
			}
			poolNodes[poolName] = append(poolNodes[poolName], nodeRef{poolName: poolName, index: proxy.index, proxy: proxy, raw: proxy.name})
			totalNodes++
		}
	}
	// Real-inference probe models per lane (directory-driven, never hardcoded).
	// A lane without a servable model reports no_model and sends nothing.
	// The authenticated lane without any configured key reports unconfigured
	// and performs no inference send.
	anonModel, anonProto, anonOK := g.bulkProbeModel(TierZen, true)
	authModel, authProto, authOK := g.bulkProbeModel(TierZen, false)
	hasAuthKeys := len(authCreds) > 0
	noModel := map[string]bool{}
	if !anonOK {
		noModel["anonymous"] = true
	}
	if !hasAuthKeys {
		noModel["unconfigured"] = true
	} else if !authOK {
		noModel["authenticated"] = true
	}
	// Build send targets with bounded amplification.
	targets := make([]bulkSendTarget, 0, bulkMaxTotalSends)
	skipped := 0
	truncated := false
	addTargets := func(poolName string, tier Tier, credKey, credID, credDisp string, isPublic bool, cap int, probeModel string, probeProto Protocol, probeOK bool) int {
		if !probeOK {
			return 0
		}
		nodes, ok := poolNodes[poolName]
		if !ok || len(nodes) == 0 {
			return 0
		}
		// Deterministic pool order (by index).
		ordered := append([]nodeRef(nil), nodes...)
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].index < ordered[j].index })
		limit := len(ordered)
		if cap > 0 && limit > cap {
			skipped += limit - cap
			limit = cap
			truncated = true
		}
		added := 0
		for _, n := range ordered[:limit] {
			if len(targets) >= bulkMaxTotalSends {
				skipped += limit - added
				truncated = true
				break
			}
			targets = append(targets, bulkSendTarget{
				PoolName: n.poolName, Index: n.index, Proxy: n.proxy, Raw: n.raw,
				Tier: tier, CredKey: credKey, CredID: credID, CredDisp: credDisp, IsPublic: isPublic,
				ProbeModel: probeModel, ProbeProtocol: probeProto,
			})
			added++
		}
		return added
	}
	// Anonymous tests the anonymous pool; authenticated tests the
	// authenticated pool. Each key probes enough nodes to establish success,
	// node-specific failure, or two-distinct-429 evidence. Caps keep
	// amplification bounded; truncation is reported.
	publicTested := addTargets(anonPoolName, TierZen, anonymousZenKey, anonymousSchedulerCredentialID, anonymousCredentialID, true, bulkMaxPublicNodes, anonModel, anonProto, anonOK)
	_ = publicTested
	if hasAuthKeys {
		for _, cred := range authCreds {
			addTargets(authPoolName, TierZen, cred.key, cred.id, cred.display, false, bulkMaxNodesPerCredential, authModel, authProto, authOK)
		}
	}
	tested := len(targets)
	// Bounded concurrency execution.
	results := make([]bulkSendResult, len(targets))
	sem := make(chan struct{}, bulkProbeConcurrency)
	var wg sync.WaitGroup
	for i, tgt := range targets {
		// Overall cancel stops launching new sends; already-launched sends
		// finish their per-send timeout and report diagnostic-only.
		if ctx.Err() != nil {
			results[i] = bulkSendResult{Target: tgt, AdminCancelled: true}
			continue
		}
		wg.Add(1)
		go func(idx int, t bulkSendTarget) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results[idx] = bulkSendResult{Target: t, AdminCancelled: true}
				return
			}
			results[idx] = g.bulkProbeOnce(ctx, t)
		}(i, tgt)
	}
	wg.Wait()
	partial := ctx.Err() != nil
	if tested == 0 && totalNodes > 0 {
		// Zero-tested runs must not masquerade as complete.
		partial = true
	}
	// Apply deterministic state writes (comparative only) and transport
	// health updates. All other outcomes remain display-only.
	g.applyBulkWrites(ctx, results)
	// Native proxy lanes only. Custom fallback channels are never probed
	// here; per-channel custom checks use POST /api/availability/check-custom
	// and the batch must not overwrite/clear stored custom snapshots.
	// Build redacted per-node/per-credential projection.
	resp := g.buildBulkResponse(checkedAt, totalNodes, tested, skipped, truncated, partial, results, []bulkCustomResult{}, sortedNoModelList(noModel))
	resp.Custom = nil
	// Atomically replace the admin-only latest-result snapshot (fixed
	// bounded). It is a projection and never routing authority. Preserve
	// useful partial node rows but visibly mark partial; never overwrite a
	// newer complete snapshot with a fully cancelled zero-tested result.
	// Stored per-channel custom rows are preserved verbatim.
	existingSnap := g.bulkSnapshot.Load()
	preservedCustom := []bulkCustomAvailability(nil)
	if existingSnap != nil {
		preservedCustom = append([]bulkCustomAvailability(nil), existingSnap.Custom...)
	}
	snap := &bulkAvailabilitySnapshot{
		CheckedAt: checkedAt, TotalNodes: totalNodes, TestedNodes: tested,
		SkippedNodes: skipped, Truncated: truncated, Partial: partial,
		Custom: preservedCustom, NoModel: resp.NoModel,
	}
	if len(resp.Nodes) > bulkMaxSnapshotNodes {
		snap.Nodes = append([]bulkNodeAvailability(nil), resp.Nodes[:bulkMaxSnapshotNodes]...)
	} else {
		snap.Nodes = append([]bulkNodeAvailability(nil), resp.Nodes...)
	}
	if len(resp.Credentials) > bulkMaxSnapshotCreds {
		snap.Credentials = append([]bulkCredentialAvailability(nil), resp.Credentials[:bulkMaxSnapshotCreds]...)
	} else {
		snap.Credentials = append([]bulkCredentialAvailability(nil), resp.Credentials...)
	}
	if tested == 0 || allBulkCancelled(results) {
		if totalNodes > 0 {
			if existing := g.bulkSnapshot.Load(); existing != nil && !existing.Partial && !existing.CheckedAt.IsZero() {
				// Keep the newer complete snapshot; still return the partial
				// diagnostic response without overwriting.
				return resp
			}
		}
	}
	g.bulkSnapshot.Store(snap)
	return resp
}

// bulkProbeScope returns the same target-bound route-session scope the
// gateway uses for one candidate: upstream authority, tier, internal
// credential identity, pool, raw proxy (proxy-affine for anonymous only),
// and target protocol. Model is excluded, matching gateway semantics.
func bulkProbeScope(base string, tgt bulkSendTarget, protocol Protocol) routeSessionScope {
	cand := targetCandidate{Tier: tgt.Tier, CredID: tgt.CredID, PoolName: tgt.PoolName, ProxyRaw: tgt.Raw}
	return routeScopeForCandidate(base, cand, protocol)
}

// bulkProbeCanonicalBody builds the probe body through the single gateway
// preparation path: minimal client payload for the lane protocol,
// prepareUpstreamRequest normalization (same-protocol passthrough plus
// target-protocol reasoning repair), then applyRouteSessionToBody for the
// target-bound route session (Responses prompt_cache_key/store defaults).
// Body model and protocol stay aligned to the directory-driven selection.
func bulkProbeCanonicalBody(model string, protocol Protocol, baseURL, routeSession string) ([]byte, error) {
	var input map[string]any
	switch protocol {
	case ProtocolResponses:
		input = map[string]any{"model": model, "input": "hi", "max_output_tokens": 1, "stream": false}
	case ProtocolAnthropic:
		input = map[string]any{"model": model, "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "max_tokens": 1}
	default:
		input = map[string]any{"model": model, "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "max_tokens": 1, "stream": false}
	}
	prepared, err := prepareUpstreamRequest(protocol, protocol, input, baseURL)
	if err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(prepared)
	if err != nil {
		return nil, errors.New("request contains unsupported JSON values")
	}
	return applyRouteSessionToBody(canonical, routeSession, protocol, false)
}

// buildBulkProbeRequest constructs one scheduler-neutral minimal-inference
// probe through the shared gateway helpers only: bulkProbeScope for the
// target-bound scope, stateless deriveFirstRouteSession (never the scheduler
// override store), bulkProbeCanonicalBody for preparation, and
// newUpstreamRequest for endpoint/auth/OpenCode/protocol headers. It never
// reads scheduler cooldowns, route-session overrides, or pins, and never
// writes metrics/history. Header and body share one internally consistent
// canonical session/request/project triple.
func buildBulkProbeRequest(ctx context.Context, base string, tgt bulkSendTarget, protocol Protocol, model string) (*http.Request, requestIDs, string, []byte, error) {
	ids := bulkProbeIDs()
	scope := bulkProbeScope(base, tgt, protocol)
	routeSession := deriveFirstRouteSession(ids.Session, scope)
	body, err := bulkProbeCanonicalBody(model, protocol, base, routeSession)
	if err != nil {
		return nil, ids, routeSession, nil, err
	}
	req, err := newUpstreamRequest(ctx, base, protocol, body, ids, tgt.CredKey, routeSession)
	if err != nil {
		return nil, ids, routeSession, nil, err
	}
	return req, ids, routeSession, body, nil
}

func (g *Gateway) bulkProbeOnce(parent context.Context, tgt bulkSendTarget) bulkSendResult {
	base := g.cfg.Upstream.Zen
	protocol := tgt.ProbeProtocol
	if protocol != ProtocolChat && protocol != ProtocolResponses && protocol != ProtocolAnthropic {
		protocol = ProtocolChat
	}
	model := strings.TrimSpace(tgt.ProbeModel)
	if model == "" {
		return bulkSendResult{Target: tgt, StartedNanos: time.Now().UnixNano(), ParseError: true}
	}
	sendCtx, cancel := context.WithTimeout(parent, bulkPerSendTimeout)
	defer cancel()
	started := time.Now()
	startedNanos := started.UnixNano()
	req, _, _, _, err := buildBulkProbeRequest(sendCtx, base, tgt, protocol, model)
	if err != nil {
		return bulkSendResult{Target: tgt, StartedNanos: startedNanos, DurationMS: max(time.Since(started).Milliseconds(), 0), TransportErr: err, AdminCancelled: parent.Err() != nil, ProbeContextCaused: sendCtx.Err() != nil}
	}
	resp, err := tgt.Proxy.client.Do(req)
	durationMS := max(time.Since(started).Milliseconds(), 0)
	if err != nil {
		adminCancelled := parent.Err() != nil
		// Per-send timeout, cancel, or DeadlineExceeded from the probe
		// context itself is diagnostic-only for transport health: it must
		// not flip proxy healthy. Only independent conclusive transport
		// evidence not caused by operation/per-send context may use the
		// existing applyProxyHealthResult semantics. Any HTTP response below
		// still counts as reachability evidence.
		probeCaused := sendCtx.Err() != nil
		return bulkSendResult{
			Target: tgt, StartedNanos: startedNanos, DurationMS: durationMS,
			TransportErr: err, IsProxyFailure: isProxyFailure(err), AdminCancelled: adminCancelled,
			ProbeContextCaused: probeCaused,
		}
	}
	defer resp.Body.Close()
	retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
	status := resp.StatusCode
	// Exactly HTTP 200 with a valid body is success; other HTTP statuses
	// (including other 2xx) remain their real outcomes.
	if status != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return bulkSendResult{Target: tgt, StartedNanos: startedNanos, DurationMS: durationMS, Status: status, RetryAfter: retryAfter}
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return bulkSendResult{Target: tgt, StartedNanos: startedNanos, DurationMS: durationMS, Status: status, RetryAfter: retryAfter, ParseError: true}
	}
	if !bulkProbeSuccessBody(protocol, raw) {
		return bulkSendResult{Target: tgt, StartedNanos: startedNanos, DurationMS: durationMS, Status: status, RetryAfter: retryAfter, ParseError: true}
	}
	return bulkSendResult{Target: tgt, StartedNanos: startedNanos, DurationMS: durationMS, Status: status, RetryAfter: retryAfter, Success: true, Models: 1}
}

func (g *Gateway) applyBulkWrites(parent context.Context, results []bulkSendResult) {
	// Transport health reuses the single gateway authority
	// (applyProxyHealthResult with its existing global semantics). Any HTTP
	// response proves reachability and restores; only independent conclusive
	// transport evidence not caused by operation/per-send context may mark
	// unhealthy. Per-send timeout/cancel/DeadlineExceeded from the probe
	// context itself, overall operation cancel, and inconclusive errors are
	// diagnostic-only and never flip proxy healthy. No tier-scoped health is
	// introduced here.
	for _, r := range results {
		if r.Target.Proxy == nil {
			continue
		}
		if r.TransportErr == nil && r.Status != 0 {
			// Any HTTP response is reachability/success evidence.
			g.applyProxyHealthResult(proxyHealthResult{proxy: r.Target.Proxy, err: nil, wasHealthy: r.Target.Proxy.healthy.Load()}, "bulk_probe", r.Status)
			continue
		}
		if r.TransportErr == nil || !r.IsProxyFailure || r.AdminCancelled || r.ProbeContextCaused || parent.Err() != nil {
			continue
		}
		g.applyProxyHealthResult(proxyHealthResult{proxy: r.Target.Proxy, err: r.TransportErr, failed: true, wasHealthy: r.Target.Proxy.healthy.Load()}, "bulk_probe", 0)
	}
	// Group comparative outcomes by (tier, credential).
	type groupKey struct {
		tier   Tier
		credID string
	}
	type nodeOutcome struct {
		pool       string
		raw        string
		status     int
		retryAfter time.Duration
		started    int64
	}
	groups := make(map[groupKey]*struct {
		tier     Tier
		credID   string
		credDisp string
		isPublic bool
		success  []nodeOutcome
		fail429  []nodeOutcome
		fail4035 []nodeOutcome
		has401   bool
		first401 nodeOutcome
	})
	groupOrder := []groupKey{}
	for _, r := range results {
		if r.AdminCancelled || r.TransportErr != nil || r.ParseError {
			continue
		}
		if r.Status == 0 {
			continue
		}
		// 408/425/other4xx/parse/timeout/cancel are diagnostic only.
		if r.Status == 408 || r.Status == 425 {
			continue
		}
		if r.Status >= 400 && r.Status < 500 && r.Status != 401 && r.Status != 403 && r.Status != 429 {
			continue
		}
		key := groupKey{tier: r.Target.Tier, credID: r.Target.CredID}
		grp, ok := groups[key]
		if !ok {
			grp = &struct {
				tier     Tier
				credID   string
				credDisp string
				isPublic bool
				success  []nodeOutcome
				fail429  []nodeOutcome
				fail4035 []nodeOutcome
				has401   bool
				first401 nodeOutcome
			}{tier: r.Target.Tier, credID: r.Target.CredID, credDisp: r.Target.CredDisp, isPublic: r.Target.IsPublic}
			groups[key] = grp
			groupOrder = append(groupOrder, key)
		}
		out := nodeOutcome{pool: r.Target.PoolName, raw: r.Target.Raw, status: r.Status, retryAfter: r.RetryAfter, started: r.StartedNanos}
		switch {
		case r.Success:
			grp.success = append(grp.success, out)
		case r.Status == 401:
			if !grp.has401 {
				grp.has401 = true
				grp.first401 = out
			}
		case r.Status == 429:
			grp.fail429 = append(grp.fail429, out)
		case r.Status == 403 || r.Status >= 500:
			grp.fail4035 = append(grp.fail4035, out)
		}
	}
	// Also collect successes for groups that had only successes (no failures
	// above still need clearing). The loop above already records successes
	// for any group with at least one classifiable outcome; groups with only
	// successes have success non-empty and empty failure lists, which still
	// triggers per-node success clearing below.
	// Deterministic group order for stable backoff/jitter and logs.
	sort.Slice(groupOrder, func(i, j int) bool {
		if groupOrder[i].tier != groupOrder[j].tier {
			return groupOrder[i].tier < groupOrder[j].tier
		}
		return groupOrder[i].credID < groupOrder[j].credID
	})
	for _, key := range groupOrder {
		grp := groups[key]
		// 401: real configured credentials write tier+credential 401 state
		// once per group. Public Zen 401 is diagnostic only.
		if grp.has401 && !grp.isPublic {
			change := g.scheduler.noteCredentialAuthFailure(grp.credID)
			if g.logger != nil && change.Changed {
				g.logger.Warn("credential cooldown extended",
					"component", "scheduler", "event", "credential_cooldown_set",
					"tier", string(grp.tier), "key_id", grp.credDisp,
					"failures", change.Failures,
					"cooldown_until", time.Unix(0, change.CooldownUntil).UTC(),
					"status", 401, "source", "bulk_probe")
			}
		}
		// Per-node success clearing for channel/proxy429 (stale-fenced).
		// Probe success never clears credential401/429 layers.
		for _, s := range grp.success {
			if ch := g.scheduler.noteChannelSuccess(grp.tier, s.pool, s.raw, s.started); ch.Changed && g.logger != nil {
				g.logger.Debug("channel availability cooldown cleared",
					"component", "scheduler", "event", "channel_cooldown_cleared",
					"tier", string(grp.tier), "key_id", grp.credDisp,
					"proxy_pool", s.pool, "proxy_node", redactURL(s.raw),
					"failures", ch.Failures, "source", "bulk_probe")
			}
			if px := g.scheduler.noteProxy429Success(grp.tier, s.pool, s.raw, s.started); px.Changed && g.logger != nil {
				g.logger.Debug("proxy rate-limit cooldown cleared",
					"component", "scheduler", "event", "proxy_rate_limit_cooldown_cleared",
					"tier", string(grp.tier), "key_id", grp.credDisp,
					"proxy_pool", s.pool, "proxy_node", redactURL(s.raw),
					"failures", px.Failures, "source", "bulk_probe")
			}
		}
		if grp.isPublic {
			// Public Zen: never writes configured-credential cooldowns.
			// Tier+pool+proxy 429 requires at least one comparative success
			// in the same public-Zen run; without public success all public
			// 429s are display-only. Each 403/5xx node writes Zen channel
			// state only with comparative success.
			if len(grp.success) > 0 {
				for _, f := range grp.fail429 {
					ch := g.scheduler.noteProxy429Failure(grp.tier, f.pool, f.raw, AttemptClassRateLimited, 429, f.retryAfter, f.started)
					if g.logger != nil && ch.Changed {
						g.logger.Debug("proxy rate-limit cooldown extended",
							"component", "scheduler", "event", "proxy_rate_limit_cooldown_set",
							"tier", string(grp.tier), "key_id", grp.credDisp,
							"proxy_pool", f.pool, "proxy_node", redactURL(f.raw),
							"status", 429, "failures", ch.Failures, "source", "bulk_probe")
					}
				}
			}
			if len(grp.success) > 0 {
				for _, f := range grp.fail4035 {
					class := AttemptClassAuthFailure
					if f.status >= 500 {
						class = AttemptClassUpstreamFailure
					}
					ch := g.scheduler.noteChannelFailure(grp.tier, f.pool, f.raw, class, f.status, f.retryAfter, f.started)
					g.logChannelCooldownSet(grp.tier, f.pool, f.raw, grp.credDisp, class, f.status, ch)
				}
			}
			continue
		}
		// Real credentials: 429 comparative rules.
		if len(grp.success) > 0 {
			for _, f := range grp.fail429 {
				ch := g.scheduler.noteProxy429Failure(grp.tier, f.pool, f.raw, AttemptClassRateLimited, 429, f.retryAfter, f.started)
				if g.logger != nil && ch.Changed {
					g.logger.Debug("proxy rate-limit cooldown extended",
						"component", "scheduler", "event", "proxy_rate_limit_cooldown_set",
						"tier", string(grp.tier), "key_id", grp.credDisp,
						"proxy_pool", f.pool, "proxy_node", redactURL(f.raw),
						"status", 429, "failures", ch.Failures, "source", "bulk_probe")
				}
			}
		} else {
			// No comparative success: two distinct 429 nodes write
			// tier+credential 429 using the second 429's Retry-After and
			// send-start timestamp. Single 429 is display-only.
			seen := map[string]nodeOutcome{}
			ordered := append([]nodeOutcome(nil), grp.fail429...)
			sort.Slice(ordered, func(i, j int) bool {
				if ordered[i].pool != ordered[j].pool {
					return ordered[i].pool < ordered[j].pool
				}
				return ordered[i].raw < ordered[j].raw
			})
			var distinct []nodeOutcome
			for _, o := range ordered {
				k := o.pool + "\x00" + o.raw
				if _, ok := seen[k]; ok {
					continue
				}
				seen[k] = o
				distinct = append(distinct, o)
			}
			if len(distinct) >= 2 {
				second := distinct[1]
				change := g.scheduler.noteCredential429Failure(grp.credID, AttemptClassRateLimited, 429, second.retryAfter, second.started)
				if g.logger != nil && change.Changed {
					g.logger.Debug("credential rate-limit cooldown extended",
						"component", "scheduler", "event", "credential_rate_limit_cooldown_set",
						"tier", string(grp.tier), "key_id", grp.credDisp,
						"failures", change.Failures, "source", "bulk_probe")
				}
			}
		}
		// Real credentials: 403/5xx comparative channel rule.
		if len(grp.success) > 0 {
			for _, f := range grp.fail4035 {
				class := AttemptClassAuthFailure
				if f.status >= 500 {
					class = AttemptClassUpstreamFailure
				}
				ch := g.scheduler.noteChannelFailure(grp.tier, f.pool, f.raw, class, f.status, f.retryAfter, f.started)
				g.logChannelCooldownSet(grp.tier, f.pool, f.raw, grp.credDisp, class, f.status, ch)
			}
		}
	}
}

func bulkOutcomeLabel(r bulkSendResult) string {
	if r.AdminCancelled {
		return "cancelled"
	}
	if r.ProbeContextCaused {
		// Per-send timeout/cancel from the probe context itself is
		// diagnostic-only, never a conclusive transport signal.
		return "inconclusive"
	}
	if r.TransportErr != nil {
		if r.IsProxyFailure {
			return "transport_failure"
		}
		return "inconclusive"
	}
	if r.ParseError {
		return "parse_error"
	}
	switch {
	case r.Success:
		return "success"
	case r.Status == 401:
		return "auth_failure"
	case r.Status == 429:
		return "rate_limited"
	case r.Status == 403 || r.Status >= 500:
		return "upstream_failure"
	case r.Status == 408 || r.Status == 425:
		return "transient_client"
	case r.Status >= 400 && r.Status < 500:
		return "client_rejected"
	case r.Status != 0:
		return "other_response"
	default:
		return "inconclusive"
	}
}

func (g *Gateway) buildBulkResponse(checkedAt time.Time, total, tested, skipped int, truncated, partial bool, results []bulkSendResult, extraCustom ...any) bulkCheckResponse {
	var customResults []bulkCustomResult
	var noModel []string
	if len(extraCustom) > 0 {
		if v, ok := extraCustom[0].([]bulkCustomResult); ok {
			customResults = v
		}
	}
	if len(extraCustom) > 1 {
		if v, ok := extraCustom[1].([]string); ok {
			noModel = v
		}
	}
	// Per pool-qualified node aggregation records the latest real probe
	// observation per lane, never a generic available/unavailable verdict.
	// Within one run any observed HTTP response proves reachable and must
	// never be overwritten by another credential's conclusive transport
	// error: track sawHTTP per node and keep the highest-confidence outcome.
	// Routing health writes remain separately governed in applyBulkWrites.
	type nodeAgg struct {
		pool              string
		index             int
		raw               string
		redacted          string
		transport         string
		anonymous         string
		anonymousHTTP     int
		authenticated     string
		authenticatedHTTP int
		reason            string
		sawHTTP           bool
	}
	aggOrder := []string{}
	aggs := map[string]*nodeAgg{}
	keyFor := func(pool, raw string) string { return pool + "\x00" + raw }
	// Seed with all active nodes so untested nodes still appear as untested.
	for _, poolName := range g.cfg.UniqueActivePools() {
		pool := g.pools[poolName]
		if pool == nil {
			continue
		}
		for _, proxy := range pool.items {
			if proxy == nil {
				continue
			}
			k := keyFor(poolName, proxy.name)
			if _, ok := aggs[k]; !ok {
				aggs[k] = &nodeAgg{pool: poolName, index: proxy.index, raw: proxy.name, redacted: redactURL(proxy.name), transport: "untested", anonymous: "untested", authenticated: "untested"}
				aggOrder = append(aggOrder, k)
			}
		}
	}
	sort.Strings(aggOrder)
	// Per-credential aggregation by internal credential ID (tier-qualified).
	// Display renders only the key tail; tail collisions remain separate rows.
	type credAgg struct {
		tier     Tier
		credID   string
		disp     string
		fp       string
		pool     string
		tested   int
		success  int
		lastCode int
		lastErr  string
	}
	credOrder := []string{}
	creds := map[string]*credAgg{}
	credKeyFor := func(tier Tier, credID, pool string) string { return string(tier) + "\x00" + credID + "\x00" + pool }
	for _, r := range results {
		nk := keyFor(r.Target.PoolName, r.Target.Raw)
		na, ok := aggs[nk]
		if !ok {
			na = &nodeAgg{pool: r.Target.PoolName, index: r.Target.Index, raw: r.Target.Raw, redacted: redactURL(r.Target.Raw), transport: "untested", anonymous: "untested", authenticated: "untested"}
			aggs[nk] = na
			aggOrder = append(aggOrder, nk)
		}
		label := bulkOutcomeLabel(r)
		// Transport display: any HTTP response proves reachable for the whole
		// run and must not later be overwritten by another credential's
		// conclusive transport error. Probe-context timeouts are
		// diagnostic-only and never mark unhealthy.
		if r.TransportErr == nil && r.Status != 0 {
			na.transport = "healthy"
			na.sawHTTP = true
			// An earlier transport-only failure must not mask the higher
			// confidence HTTP outcome in the display reason.
			if na.reason == "transport_failure" {
				na.reason = ""
			}
		} else if r.TransportErr != nil && r.IsProxyFailure && !r.AdminCancelled && !r.ProbeContextCaused {
			if !na.sawHTTP {
				na.transport = "unhealthy"
				na.reason = "transport_failure"
			}
		} else if na.transport == "untested" {
			na.transport = "inconclusive"
		}
		setLane := func(current *string, currentHTTP *int) {
			if *current == "untested" || label == "success" {
				*current = label
				if r.Status != 0 {
					*currentHTTP = r.Status
				}
			} else if *current != "success" && (label == "rate_limited" || label == "upstream_failure" || label == "auth_failure") {
				*current = label
				if r.Status != 0 {
					*currentHTTP = r.Status
				}
			} else if r.Status != 0 && *currentHTTP == 0 {
				*currentHTTP = r.Status
			}
		}
		if r.Target.IsPublic {
			setLane(&na.anonymous, &na.anonymousHTTP)
		} else {
			setLane(&na.authenticated, &na.authenticatedHTTP)
		}
		if r.TransportErr == nil && r.Status != 0 && na.reason == "" {
			if label != "success" {
				na.reason = label
			}
		} else if r.TransportErr != nil && na.reason == "" {
			na.reason = label
		}
		if !r.Target.IsPublic {
			ck := credKeyFor(r.Target.Tier, r.Target.CredID, r.Target.PoolName)
			ca, ok := creds[ck]
			if !ok {
				ca = &credAgg{tier: r.Target.Tier, credID: r.Target.CredID, disp: r.Target.CredDisp, fp: credentialFingerprint(r.Target.Tier, r.Target.CredKey), pool: r.Target.PoolName}
				creds[ck] = ca
				credOrder = append(credOrder, ck)
			}
			ca.tested++
			if r.Success {
				ca.success++
			}
			ca.lastCode = r.Status
			ca.lastErr = bulkOutcomeLabel(r)
		}
	}
	// Overlay lane-level no_model/unconfigured onto untested per-node lanes so
	// every row carries what the last check observed.
	noSetOverlay := map[string]bool{}
	for _, n := range noModel {
		noSetOverlay[n] = true
	}
	anonPoolOverlay := g.cfg.ProxyRouting.Anonymous
	authPoolOverlay := g.authPoolName()
	for _, na := range aggs {
		if na.pool == anonPoolOverlay && na.anonymous == "untested" && noSetOverlay["anonymous"] {
			na.anonymous = "no_model"
		}
		if na.pool == authPoolOverlay && na.authenticated == "untested" {
			if noSetOverlay["unconfigured"] {
				na.authenticated = "unconfigured"
			} else if noSetOverlay["authenticated"] {
				na.authenticated = "no_model"
			}
		}
		// Shared pool carries both lanes; the checks above already cover it.
	}
	sort.Strings(aggOrder)
	sort.Strings(credOrder)
	nodes := make([]bulkNodeAvailability, 0, len(aggOrder))
	for _, k := range aggOrder {
		na := aggs[k]
		nodes = append(nodes, bulkNodeAvailability{
			Pool: na.pool, Index: na.index, ProxyNode: na.redacted,
			Transport: na.transport, Anonymous: na.anonymous, AnonymousHTTPStatus: na.anonymousHTTP,
			Authenticated: na.authenticated, AuthenticatedHTTPStatus: na.authenticatedHTTP,
			Zen: na.anonymous, Go: na.authenticated, Reason: na.reason,
			LastChecked: &checkedAt,
		})
		if len(nodes) >= bulkMaxSnapshotNodes {
			break
		}
	}
	credentials := make([]bulkCredentialAvailability, 0, len(credOrder))
	for _, k := range credOrder {
		ca := creds[k]
		status := "available"
		reason := ""
		if ca.success > 0 {
			status = "available"
			reason = "success"
		} else if ca.lastCode == 401 {
			status = "unavailable"
			reason = "auth_failure"
		} else if ca.lastCode == 429 {
			status = "rate_limited"
			reason = "rate_limited"
		} else if ca.lastCode == 403 || ca.lastCode >= 500 {
			status = "unavailable"
			reason = "upstream_failure"
		} else if ca.lastErr == "transport_failure" {
			status = "unavailable"
			reason = "transport_failure"
		} else {
			status = "unavailable"
			reason = ca.lastErr
		}
		credentials = append(credentials, bulkCredentialAvailability{
			Tier: string(ca.tier), KeyTail: ca.disp, Fingerprint: ca.fp, Pool: ca.pool,
			Status: status, Reason: reason, TestedNodes: ca.tested, LastChecked: &checkedAt,
			CredID: ca.credID,
		})
		if len(credentials) >= bulkMaxSnapshotCreds {
			break
		}
	}
	custom := make([]bulkCustomAvailability, 0, len(customResults))
	for _, r := range customResults {
		label := bulkCustomOutcomeLabel(r)
		status, reason := bulkCustomStatusFor(label)
		custom = append(custom, bulkCustomAvailability{
			Name: r.Target.Name, BaseURL: redactURL(r.Target.BaseURL), Model: r.Target.Model,
			Status: status, Reason: reason, LastChecked: &checkedAt,
		})
	}
	// Lanes without a directory probe model surface explicit no_model rows
	// so callers never mistake an untested lane for success. The
	// authenticated lane without keys is unconfigured, never probed.
	if len(noModel) > 0 {
		noSet := map[string]bool{}
		for _, n := range noModel {
			noSet[n] = true
		}
		if noSet["authenticated"] {
			for _, cred := range g.credentials() {
				found := false
				for _, c := range credentials {
					if c.Tier == string(TierZen) && c.CredID == cred.id {
						found = true
						break
					}
				}
				if !found && len(credentials) < bulkMaxSnapshotCreds {
					credentials = append(credentials, bulkCredentialAvailability{Tier: string(TierZen), KeyTail: cred.display, Fingerprint: credentialFingerprint(TierZen, cred.key), Pool: g.authPoolName(), Status: "inconclusive", Reason: "no_model", LastChecked: &checkedAt, CredID: cred.id})
				}
			}
		}
	}
	return bulkCheckResponse{
		CheckedAt: checkedAt, TotalNodes: total, TestedNodes: tested,
		SkippedNodes: skipped, Truncated: truncated, Partial: partial,
		Nodes: nodes, Credentials: credentials, Custom: custom, NoModel: noModel,
	}
}
