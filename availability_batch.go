package main

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// Scoped availability batch checks: one batch per table, each with a single
// backend operation (no client per-row loops).
//
//   - POST /api/availability/check-credentials checks all configured
//     authenticated credentials only. It never probes anonymous nodes or
//     custom channels. It reuses the per-credential target selection
//     (credentialProbeEligible), the shared minimal Zen request path
//     (bulkProbeOnce), the exact-status classification (buildBulkResponse),
//     the comparative applyBulkWrites rules, and the per-row snapshot merge
//     semantics. One rate-limit admission, one bulkMu acquisition,
//     concurrency 4, overall/per-send timeouts, total-send cap, and
//     partial/truncated reporting. No keys sends nothing (unconfigured);
//     directory without a model yields inconclusive/no_model rows.
//     Availability is only ever observed, never inferred from cooldown state.
//   - POST /api/availability/check-customs checks every configured custom
//     fallback channel exactly once, including inactive channels. It never
//     probes proxy nodes or Zen credentials. One rate-limit admission, one
//     bulkMu acquisition, concurrency 4, overall/per-send timeouts,
//     total-send cap, and partial/truncated reporting. Results merge only
//     custom snapshot rows while preserving node/credential rows and the
//     global timestamp. Custom probes stay isolated: no Zen
//     scheduler/cooldown/transport-health writes, no inference
//     metrics/history, no fallback takeover binding/session state.
//
// The existing POST /api/availability/check keeps its proxy-table scope
// (all proxy nodes, anonymous and authenticated lanes; never custom
// channels). All three batch operations plus the per-row checks share the
// bulk rate limiter and the bulkMu single-flight gate, so a second
// operation reports 409 bulk_busy while one is running.

type credentialsBatchResponse struct {
	CheckedAt        time.Time                    `json:"checked_at"`
	TotalCredentials int                          `json:"total_credentials"`
	TestedNodes      int                          `json:"tested_nodes,omitempty"`
	SkippedNodes     int                          `json:"skipped_nodes,omitempty"`
	Truncated        bool                         `json:"truncated"`
	Partial          bool                         `json:"partial,omitempty"`
	Credentials      []bulkCredentialAvailability `json:"credentials,omitempty"`
	NoModel          []string                     `json:"no_model,omitempty"`
}

type customsBatchResponse struct {
	CheckedAt       time.Time                `json:"checked_at"`
	TotalChannels   int                      `json:"total_channels"`
	TestedChannels  int                      `json:"tested_channels,omitempty"`
	SkippedChannels int                      `json:"skipped_channels,omitempty"`
	Truncated       bool                     `json:"truncated"`
	Partial         bool                     `json:"partial,omitempty"`
	Custom          []bulkCustomAvailability `json:"custom,omitempty"`
}

func (a *AdminServer) handleCredentialsBatch(w http.ResponseWriter, r *http.Request) {
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
	resp := gateway.runCredentialsBatch(ctx)
	if ctx.Err() != nil {
		resp.Partial = true
	}
	writeJSON(w, http.StatusOK, resp)
}

func (a *AdminServer) handleCustomsBatch(w http.ResponseWriter, r *http.Request) {
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
	resp := gateway.runCustomsBatch(ctx)
	if ctx.Err() != nil {
		resp.Partial = true
	}
	writeJSON(w, http.StatusOK, resp)
}

func (g *Gateway) runCredentialsBatch(ctx context.Context) credentialsBatchResponse {
	checkedAt := time.Now().UTC()
	creds := append([]credentialRef(nil), g.credentials()...)
	poolName := g.authPoolName()
	if len(creds) == 0 {
		// No keys: unconfigured, send nothing, mutate nothing.
		return credentialsBatchResponse{
			CheckedAt: checkedAt, TotalCredentials: 0,
			NoModel: []string{"unconfigured"},
		}
	}
	model, proto, ok := g.bulkProbeModel(TierZen, false)
	if !ok {
		// Directory without a model: inconclusive/no_model rows for every
		// credential, merged with per-row merge semantics; send nothing.
		rows := make([]bulkCredentialAvailability, 0, len(creds))
		for _, cred := range creds {
			rows = append(rows, bulkCredentialAvailability{
				Tier: string(TierZen), KeyTail: cred.display,
				Fingerprint: credentialFingerprint(TierZen, cred.key), Pool: poolName,
				Status: "inconclusive", Reason: "no_model", LastChecked: &checkedAt,
				CredID: cred.id,
			})
		}
		for _, row := range rows {
			g.mergeCredentialSnapshotRow(checkedAt, row)
		}
		return credentialsBatchResponse{
			CheckedAt: checkedAt, TotalCredentials: len(creds),
			Credentials: rows, NoModel: []string{"authenticated"},
			Partial: ctx.Err() != nil,
		}
	}
	// Bounded send targets: per-credential eligibility in config order,
	// per-credential cap plus the shared total-send cap. Overflow is
	// reported as skipped/truncated, never silently sampled.
	targets := make([]bulkSendTarget, 0, bulkMaxTotalSends)
	skipped := 0
	truncated := false
	cappedCreds := map[string]bool{}
	eligibleByCred := make([][]*proxyTransport, len(creds))
	for i, cred := range creds {
		eligibleByCred[i] = g.credentialProbeEligible(TierZen, cred.id, poolName)
		if len(eligibleByCred[i]) > bulkMaxNodesPerCredential {
			skipped += len(eligibleByCred[i]) - bulkMaxNodesPerCredential
			truncated = true
			eligibleByCred[i] = eligibleByCred[i][:bulkMaxNodesPerCredential]
		}
	}
capped:
	for i, cred := range creds {
		for k, proxy := range eligibleByCred[i] {
			if len(targets) >= bulkMaxTotalSends {
				// This and all remaining eligible sends are skipped.
				// The current credential is partially probed; every later
				// credential with eligible sends is fully skipped.
				cappedCreds[cred.id] = true
				skipped += len(eligibleByCred[i]) - k
				for j := i + 1; j < len(creds); j++ {
					if len(eligibleByCred[j]) > 0 {
						cappedCreds[creds[j].id] = true
					}
					skipped += len(eligibleByCred[j])
				}
				truncated = true
				break capped
			}
			targets = append(targets, bulkSendTarget{
				PoolName: poolName, Index: proxy.index, Proxy: proxy, Raw: proxy.name,
				Tier: TierZen, CredKey: cred.key, CredID: cred.id, CredDisp: cred.display,
				ProbeModel: model, ProbeProtocol: proto,
			})
		}
	}
	tested := len(targets)
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
	// Comparative scheduler/transport writes only; ambiguous outcomes stay
	// display-only. Never touches custom state.
	g.applyBulkWrites(ctx, results)
	// Reuse the exact batch classification so labels match the per-row and
	// proxy-batch probes. Node rows are classification byproducts here and
	// are discarded: this operation merges credential rows only.
	totalNodes := 0
	if pool := g.pools[poolName]; pool != nil {
		totalNodes = len(pool.items)
	}
	full := g.buildBulkResponse(checkedAt, totalNodes, tested, 0, false, partial, results, []bulkCustomResult{}, nil)
	byCred := map[string]bulkCredentialAvailability{}
	for _, c := range full.Credentials {
		if c.Tier == string(TierZen) && c.Pool == poolName {
			if _, dup := byCred[c.CredID]; !dup {
				byCred[c.CredID] = c
			}
		}
	}
	rows := make([]bulkCredentialAvailability, 0, len(creds))
	for i, cred := range creds {
		if row, found := byCred[cred.id]; found {
			rows = append(rows, row)
			continue
		}
		// No sends for this credential: explicit inconclusive rows rather
		// than fake outcomes or silent omission.
		reason := "no_available_proxy"
		if cappedCreds[cred.id] {
			reason = "skipped"
		} else if len(eligibleByCred[i]) == 0 {
			reason = "no_available_proxy"
		}
		lc := checkedAt
		rows = append(rows, bulkCredentialAvailability{
			Tier: string(TierZen), KeyTail: cred.display,
			Fingerprint: credentialFingerprint(TierZen, cred.key), Pool: poolName,
			Status: "inconclusive", Reason: reason, TestedNodes: 0, LastChecked: &lc,
			CredID: cred.id,
		})
	}
	if tested == 0 {
		// Zero-tested runs with configured credentials must not masquerade
		// as complete.
		partial = true
	}
	for _, row := range rows {
		g.mergeCredentialSnapshotRow(checkedAt, row)
	}
	return credentialsBatchResponse{
		CheckedAt: checkedAt, TotalCredentials: len(creds),
		TestedNodes: tested, SkippedNodes: skipped, Truncated: truncated,
		Partial: partial, Credentials: rows,
	}
}

func (g *Gateway) runCustomsBatch(ctx context.Context) customsBatchResponse {
	checkedAt := time.Now().UTC()
	// Every configured channel exactly once, including inactive ones, in
	// config order (deterministic). No filtering by active selection.
	channels := append([]FallbackChannelConfig(nil), g.cfg.Fallback.Channels...)
	if len(channels) == 0 {
		return customsBatchResponse{CheckedAt: checkedAt, TotalChannels: 0}
	}
	// Bounded sends: each channel probes at most once; overflow counts
	// toward the shared total-send cap and is reported explicitly.
	targets := make([]bulkCustomTarget, 0, bulkMaxTotalSends)
	skipped := 0
	truncated := false
	for _, ch := range channels {
		if len(targets) >= bulkMaxTotalSends {
			skipped++
			truncated = true
			continue
		}
		targets = append(targets, bulkCustomTarget{
			ID: ch.ID, Name: ch.Name, BaseURL: ch.BaseURL, Model: ch.Model,
			APIKey: ch.APIKey, Protocol: fallbackChannelProtocol(ch),
			Effort: fallbackChannelEffort(ch),
		})
	}
	probed := make([]bulkCustomTarget, len(targets))
	copy(probed, targets)
	results := make([]bulkCustomResult, len(probed))
	sem := make(chan struct{}, bulkProbeConcurrency)
	var wg sync.WaitGroup
	for i, tgt := range probed {
		// Overall cancel stops launching new sends; already-launched sends
		// finish their per-send timeout and report diagnostic-only.
		if ctx.Err() != nil {
			results[i] = bulkCustomResult{Target: tgt, Cancelled: true}
			continue
		}
		wg.Add(1)
		go func(idx int, t bulkCustomTarget) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results[idx] = bulkCustomResult{Target: t, Cancelled: true}
				return
			}
			results[idx] = g.bulkCustomProbeOnce(ctx, t)
		}(i, tgt)
	}
	wg.Wait()
	partial := ctx.Err() != nil
	// No applyBulkWrites/recordUpstreamAttempt/takeover writes here by
	// construction: custom probes stay isolated from Zen scheduler,
	// transport health, metrics/history, and binding state.
	byID := map[string]bulkCustomResult{}
	for _, r := range results {
		key := r.Target.ID
		if key == "" {
			key = r.Target.Name
		}
		if _, dup := byID[key]; !dup {
			byID[key] = r
		}
	}
	rows := make([]bulkCustomAvailability, 0, len(channels))
	tested := 0
	for _, ch := range channels {
		key := ch.ID
		if key == "" {
			key = ch.Name
		}
		if r, found := byID[key]; found {
			tested++
			label := bulkCustomOutcomeLabel(r)
			status, reason := bulkCustomStatusFor(label)
			rows = append(rows, bulkCustomAvailability{
				ID: ch.ID, Name: ch.Name, BaseURL: redactURL(ch.BaseURL), Model: ch.Model,
				Status: status, Reason: reason, LastChecked: &checkedAt,
			})
			continue
		}
		// Capped channels: explicit skipped/inconclusive, never fake success.
		lc := checkedAt
		rows = append(rows, bulkCustomAvailability{
			ID: ch.ID, Name: ch.Name, BaseURL: redactURL(ch.BaseURL), Model: ch.Model,
			Status: "inconclusive", Reason: "skipped", LastChecked: &lc,
		})
	}
	if tested == 0 && len(channels) > 0 {
		// Zero-tested runs must not masquerade as complete.
		partial = true
	}
	// Merge custom rows only; node/credential rows and the global timestamp
	// keep the per-row merge semantics (global CheckedAt preserved).
	for _, row := range rows {
		g.mergeCustomSnapshotRow(checkedAt, row)
	}
	return customsBatchResponse{
		CheckedAt: checkedAt, TotalChannels: len(channels),
		TestedChannels: tested, SkippedChannels: skipped, Truncated: truncated,
		Partial: partial, Custom: rows,
	}
}
