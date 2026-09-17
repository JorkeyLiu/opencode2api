package main

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Per-row availability checks for one credential or one custom channel.
//
// Contract:
//   - Strict admin auth/CSRF/Origin/no-store (wired in admin.go), strict JSON
//     bodies (DisallowUnknownFields + single value), redaction throughout.
//   - Share the bulk rate (3/min), concurrency (4), overall (30s), per-send
//     (10s), and send-cap controls plus bulkMu with the batch and scoped
//     checks.
//   - Credential: exactly one configured credential identified by the stable
//     redacted fingerprint (SHA-256 prefix of tier+key, never raw key and
//     never tail-only). Tier, key, and assigned pool resolve server-side.
//     Candidates are only active proxies in the assigned pool that are
//     transport-healthy AND not under tier-qualified (tier,pool,proxy)
//     proxy429 cooldown. proxy429-cooled nodes are never sent to and never
//     contribute evidence. Zero eligible with a directory model returns 503
//     no_available_proxy without snapshot/scheduler mutation. One newly
//     observed 429 writes proxy429 only (display-only for credential state);
//     two distinct eligible 429s write credential429 (existing invariant).
//     Snapshot merge updates only that credential row/time.
//   - Custom: exactly one configured channel by unique name, all config
//     resolved server-side. One real minimal inference with the existing
//     custom probe/classification; never writes scheduler/health/metrics/
//     history. Snapshot merge updates only that custom row by name.

type credentialCheckRequest struct {
	Fingerprint string `json:"fingerprint"`
}

type credentialCheckResponse struct {
	CheckedAt   time.Time                  `json:"checked_at"`
	Credential  bulkCredentialAvailability `json:"credential"`
	TestedNodes int                        `json:"tested_nodes,omitempty"`
	NoModel     []string                   `json:"no_model,omitempty"`
	Partial     bool                       `json:"partial,omitempty"`
}

type customCheckRequest struct {
	Name string `json:"name"`
}

type customCheckResponse struct {
	CheckedAt time.Time              `json:"checked_at"`
	Custom    bulkCustomAvailability `json:"custom"`
	Partial   bool                   `json:"partial,omitempty"`
}

func (a *AdminServer) handleCredentialCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	client := clientIP(r)
	if !a.allowBulk(client) {
		writeAdminError(w, http.StatusTooManyRequests, "bulk_rate_limited", "too many availability checks; retry in one minute")
		return
	}
	var input credentialCheckRequest
	if err := decodeAdminJSON(w, r, &input); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	fp := strings.TrimSpace(input.Fingerprint)
	if fp == "" {
		writeAdminError(w, http.StatusBadRequest, "invalid_request", "fingerprint is required")
		return
	}
	runtime := a.manager.current.Load()
	if runtime == nil || runtime.gateway == nil {
		writeAdminError(w, http.StatusServiceUnavailable, "gateway_unavailable", "gateway runtime is unavailable")
		return
	}
	gateway := runtime.gateway
	// Resolve exactly one configured credential server-side. Only the single
	// authenticated (Zen) lane exists; legacy Go fingerprints never resolve.
	type resolvedCred struct {
		tier Tier
		cred credentialRef
		pool string
	}
	var matches []resolvedCred
	for _, cred := range gateway.credentials() {
		if credentialFingerprint(TierZen, cred.key) == fp {
			matches = append(matches, resolvedCred{tier: TierZen, cred: cred, pool: gateway.authPoolName()})
		}
	}
	if len(matches) != 1 {
		writeAdminError(w, http.StatusBadRequest, "unknown_credential", "credential fingerprint does not match exactly one configured credential")
		return
	}
	resolved := matches[0]
	if !gateway.bulkMu.TryLock() {
		writeAdminError(w, http.StatusConflict, "bulk_busy", "an availability check is already running")
		return
	}
	defer gateway.bulkMu.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), bulkOverallTimeout)
	defer cancel()
	resp, status, code, msg := gateway.runCredentialCheck(ctx, resolved.tier, resolved.cred, resolved.pool)
	if status != http.StatusOK {
		writeAdminError(w, status, code, msg)
		return
	}
	_ = resp
	writeJSON(w, http.StatusOK, resp)
}

func (a *AdminServer) handleCustomCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	client := clientIP(r)
	if !a.allowBulk(client) {
		writeAdminError(w, http.StatusTooManyRequests, "bulk_rate_limited", "too many availability checks; retry in one minute")
		return
	}
	var input customCheckRequest
	if err := decodeAdminJSON(w, r, &input); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	name := strings.TrimSpace(input.Name)
	if name == "" {
		writeAdminError(w, http.StatusBadRequest, "invalid_request", "name is required")
		return
	}
	runtime := a.manager.current.Load()
	if runtime == nil || runtime.gateway == nil {
		writeAdminError(w, http.StatusServiceUnavailable, "gateway_unavailable", "gateway runtime is unavailable")
		return
	}
	gateway := runtime.gateway
	ch, ok := fallbackChannelByName(gateway.cfg, name)
	if !ok {
		writeAdminError(w, http.StatusBadRequest, "unknown_channel", "fallback channel does not exist")
		return
	}
	if !gateway.bulkMu.TryLock() {
		writeAdminError(w, http.StatusConflict, "bulk_busy", "an availability check is already running")
		return
	}
	defer gateway.bulkMu.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), bulkOverallTimeout)
	defer cancel()
	resp := gateway.runCustomCheck(ctx, ch)
	if ctx.Err() != nil {
		resp.Partial = true
	}
	writeJSON(w, http.StatusOK, resp)
}

func (g *Gateway) runCredentialCheck(ctx context.Context, tier Tier, cred credentialRef, poolName string) (credentialCheckResponse, int, string, string) {
	checkedAt := time.Now().UTC()
	if tier == TierGo {
		freshGo := bulkCredentialAvailability{
			Tier: string(tier), KeyTail: cred.display,
			Fingerprint: credentialFingerprint(tier, cred.key), Pool: poolName,
			Status: "inconclusive", Reason: "no_model", LastChecked: &checkedAt,
			CredID: cred.id,
		}
		return credentialCheckResponse{CheckedAt: checkedAt, Credential: freshGo, TestedNodes: 0, NoModel: []string{"go"}}, http.StatusOK, "", ""
	}
	model, proto, ok := g.bulkProbeModel(tier, false)
	noModelList := []string(nil)
	if !ok {
		noModelList = []string{"authenticated"}
		fresh := bulkCredentialAvailability{
			Tier: string(tier), KeyTail: cred.display,
			Fingerprint: credentialFingerprint(tier, cred.key), Pool: poolName,
			Status: "inconclusive", Reason: "no_model", LastChecked: &checkedAt,
			CredID: cred.id,
		}
		g.mergeCredentialSnapshotRow(checkedAt, fresh)
		return credentialCheckResponse{CheckedAt: checkedAt, Credential: fresh, TestedNodes: 0, NoModel: noModelList}, http.StatusOK, "", ""
	}
	// Eligibility: active pool, transport-healthy, not proxy429-cooled.
	// Ordering: authenticated credential soft-affinity
	// (proxyAffinityScore descending, proxy name tie-break) so the
	// credential's preferred eligible proxy runs first. No pins,
	// route-session overrides, or generation state participate.
	nowNanos := time.Now().UnixNano()
	eligible := []*proxyTransport{}
	if pool := g.pools[poolName]; pool != nil {
		active := false
		for _, name := range g.cfg.UniqueActivePools() {
			if name == poolName {
				active = true
				break
			}
		}
		if active {
			for _, proxy := range pool.items {
				if proxy == nil || !proxy.healthy.Load() {
					continue
				}
				if until, _, ok := g.scheduler.proxy429CooldownStatus(tier, poolName, proxy.name); ok && until > nowNanos {
					continue
				}
				eligible = append(eligible, proxy)
			}
			sort.SliceStable(eligible, func(i, j int) bool {
				si := proxyAffinityScore(cred.id, poolName, eligible[i].name)
				sj := proxyAffinityScore(cred.id, poolName, eligible[j].name)
				if si != sj {
					return si > sj
				}
				return eligible[i].name < eligible[j].name
			})
		}
	}
	if len(eligible) > bulkMaxNodesPerCredential {
		eligible = eligible[:bulkMaxNodesPerCredential]
	}
	if len(eligible) == 0 {
		return credentialCheckResponse{}, http.StatusServiceUnavailable, "no_available_proxy", "no eligible proxies for this credential (transport-healthy and not proxy429-cooled)"
	}
	targets := make([]bulkSendTarget, 0, len(eligible))
	for _, proxy := range eligible {
		if len(targets) >= bulkMaxTotalSends {
			break
		}
		targets = append(targets, bulkSendTarget{
			PoolName: poolName, Index: proxy.index, Proxy: proxy, Raw: proxy.name,
			Tier: tier, CredKey: cred.key, CredID: cred.id, CredDisp: cred.display,
			ProbeModel: model, ProbeProtocol: proto,
		})
	}
	tested := len(targets)
	results := make([]bulkSendResult, len(targets))
	sem := make(chan struct{}, bulkProbeConcurrency)
	var wg sync.WaitGroup
	for i, tgt := range targets {
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
	g.applyBulkWrites(ctx, results)
	// Reuse batch classification so labels match batch exactly.
	totalNodes := len(eligible)
	if pool := g.pools[poolName]; pool != nil {
		totalNodes = len(pool.items)
	}
	full := g.buildBulkResponse(checkedAt, totalNodes, tested, 0, false, partial, results, []bulkCustomResult{}, nil)
	var fresh *bulkCredentialAvailability
	for _, c := range full.Credentials {
		if c.Tier == string(tier) && c.CredID == cred.id && c.Pool == poolName {
			cp := c
			fresh = &cp
			break
		}
	}
	if fresh == nil {
		// Defensive: classification must always yield the selected row.
		lc := checkedAt
		fresh = &bulkCredentialAvailability{
			Tier: string(tier), KeyTail: cred.display,
			Fingerprint: credentialFingerprint(tier, cred.key), Pool: poolName,
			Status: "unavailable", Reason: "inconclusive", TestedNodes: tested, LastChecked: &lc,
			CredID: cred.id,
		}
	}
	g.mergeCredentialSnapshotRow(checkedAt, *fresh)
	return credentialCheckResponse{CheckedAt: checkedAt, Credential: *fresh, TestedNodes: tested, Partial: partial}, http.StatusOK, "", ""
}

func (g *Gateway) mergeCredentialSnapshotRow(checkedAt time.Time, fresh bulkCredentialAvailability) {
	existing := g.bulkSnapshot.Load()
	if existing == nil || existing.CheckedAt.IsZero() {
		snap := &bulkAvailabilitySnapshot{
			CheckedAt:   checkedAt,
			Credentials: []bulkCredentialAvailability{fresh},
		}
		g.bulkSnapshot.Store(snap)
		return
	}
	snap := &bulkAvailabilitySnapshot{
		CheckedAt: existing.CheckedAt, TotalNodes: existing.TotalNodes,
		TestedNodes: existing.TestedNodes, SkippedNodes: existing.SkippedNodes,
		Truncated: existing.Truncated, Partial: existing.Partial,
		Nodes:       append([]bulkNodeAvailability(nil), existing.Nodes...),
		Credentials: append([]bulkCredentialAvailability(nil), existing.Credentials...),
		Custom:      append([]bulkCustomAvailability(nil), existing.Custom...),
		NoModel:     append([]string(nil), existing.NoModel...),
	}
	replaced := false
	for i, old := range snap.Credentials {
		if old.Tier == fresh.Tier && old.Pool == fresh.Pool && old.CredID == fresh.CredID && fresh.CredID != "" {
			snap.Credentials[i] = fresh
			replaced = true
			break
		}
		if old.Tier == fresh.Tier && old.Pool == fresh.Pool && old.CredID == "" && fresh.CredID == "" && old.KeyTail == fresh.KeyTail {
			snap.Credentials[i] = fresh
			replaced = true
			break
		}
	}
	if !replaced && len(snap.Credentials) < bulkMaxSnapshotCreds {
		snap.Credentials = append(snap.Credentials, fresh)
	}
	g.bulkSnapshot.Store(snap)
}

func (g *Gateway) runCustomCheck(ctx context.Context, ch FallbackChannelConfig) customCheckResponse {
	checkedAt := time.Now().UTC()
	tgt := bulkCustomTarget{
		Name: ch.Name, BaseURL: ch.BaseURL, Model: ch.Model,
		APIKey: ch.APIKey, Protocol: fallbackChannelProtocol(ch),
	}
	// Single send on the shared bulk budget (sem guards against concurrent
	// batch/scoped/per-row checks only via bulkMu; the semaphore keeps the
	// per-send admission consistent).
	sem := make(chan struct{}, bulkProbeConcurrency)
	var result bulkCustomResult
	select {
	case sem <- struct{}{}:
		func() {
			defer func() { <-sem }()
			result = g.bulkCustomProbeOnce(ctx, tgt)
		}()
	case <-ctx.Done():
		result = bulkCustomResult{Target: tgt, Cancelled: true}
	}
	label := bulkCustomOutcomeLabel(result)
	status, reason := bulkCustomStatusFor(label)
	fresh := bulkCustomAvailability{
		Name: ch.Name, BaseURL: redactURL(ch.BaseURL), Model: ch.Model,
		Status: status, Reason: reason, LastChecked: &checkedAt,
	}
	g.mergeCustomSnapshotRow(checkedAt, fresh)
	return customCheckResponse{CheckedAt: checkedAt, Custom: fresh, Partial: result.Cancelled}
}

func (g *Gateway) mergeCustomSnapshotRow(checkedAt time.Time, fresh bulkCustomAvailability) {
	existing := g.bulkSnapshot.Load()
	if existing == nil || existing.CheckedAt.IsZero() {
		snap := &bulkAvailabilitySnapshot{
			CheckedAt: checkedAt,
			Custom:    []bulkCustomAvailability{fresh},
		}
		g.bulkSnapshot.Store(snap)
		return
	}
	snap := &bulkAvailabilitySnapshot{
		CheckedAt: existing.CheckedAt, TotalNodes: existing.TotalNodes,
		TestedNodes: existing.TestedNodes, SkippedNodes: existing.SkippedNodes,
		Truncated: existing.Truncated, Partial: existing.Partial,
		Nodes:       append([]bulkNodeAvailability(nil), existing.Nodes...),
		Credentials: append([]bulkCredentialAvailability(nil), existing.Credentials...),
		Custom:      append([]bulkCustomAvailability(nil), existing.Custom...),
		NoModel:     append([]string(nil), existing.NoModel...),
	}
	replaced := false
	for i, old := range snap.Custom {
		if old.Name == fresh.Name {
			snap.Custom[i] = fresh
			replaced = true
			break
		}
	}
	if !replaced {
		snap.Custom = append(snap.Custom, fresh)
	}
	g.bulkSnapshot.Store(snap)
}
