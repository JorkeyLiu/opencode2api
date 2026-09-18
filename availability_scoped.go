package main

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Scoped single-node availability check.
//
// Contract (mirrors bulk, restricted to one pool+index):
//   - Same real minimal-inference lane logic (bulkProbeOnce), same outcome
//     classification (bulkOutcomeLabel via buildBulkResponse), same scheduler
//     writes (applyBulkWrites), same response schema shape as batch.
//   - Restricted to the selected pool-qualified proxy only. Custom fallback
//     channels are never probed here and never appear in the response.
//   - Merges the fresh node/credential rows into the admin-only
//     bulkSnapshot without erasing unrelated nodes' last batch state, so the
//     proxy table's per-row 上次检测 refreshes immediately. The global batch
//     timestamp is preserved when a prior snapshot exists.
//   - Preserves admin auth/CSRF/Origin/no-store (wired in admin.go),
//     strict JSON decoding, redaction, and the shared concurrency (4),
//     per-send (10s), overall (30s), and send-cap limits. No per-client
//     rate limit applies; single-flight 409, timeouts, and caps remain.

type scopedCheckRequest struct {
	Pool  string `json:"pool"`
	Index *int   `json:"index"`
}

func (a *AdminServer) handleScopedCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	var input scopedCheckRequest
	if err := decodeAdminJSON(w, r, &input); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	poolName := strings.TrimSpace(input.Pool)
	if poolName == "" {
		writeAdminError(w, http.StatusBadRequest, "invalid_pool", "pool is required")
		return
	}
	if input.Index == nil || *input.Index < 0 {
		writeAdminError(w, http.StatusBadRequest, "invalid_index", "index must be a non-negative pool-local proxy index")
		return
	}
	runtime := a.manager.current.Load()
	if runtime == nil || runtime.gateway == nil {
		writeAdminError(w, http.StatusServiceUnavailable, "gateway_unavailable", "gateway runtime is unavailable")
		return
	}
	gateway := runtime.gateway
	active := false
	for _, name := range gateway.cfg.UniqueActivePools() {
		if name == poolName {
			active = true
			break
		}
	}
	if !active {
		writeAdminError(w, http.StatusBadRequest, "unknown_pool", "pool is not an active configured pool")
		return
	}
	pool := gateway.pools[poolName]
	if pool == nil || *input.Index >= len(pool.items) || pool.items[*input.Index] == nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_index", "index is outside the configured pool")
		return
	}
	if !gateway.bulkMu.TryLock() {
		writeAdminError(w, http.StatusConflict, "bulk_busy", "an availability check is already running")
		return
	}
	defer gateway.bulkMu.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), bulkOverallTimeout)
	defer cancel()
	resp := gateway.runScopedCheck(ctx, poolName, *input.Index)
	if ctx.Err() != nil && resp.TestedNodes == 0 {
		resp.Partial = true
	}
	writeJSON(w, http.StatusOK, resp)
}

func (g *Gateway) runScopedCheck(ctx context.Context, poolName string, index int) bulkCheckResponse {
	checkedAt := time.Now().UTC()
	pool := g.pools[poolName]
	if pool == nil || index < 0 || index >= len(pool.items) || pool.items[index] == nil {
		return bulkCheckResponse{CheckedAt: checkedAt, TotalNodes: 1, Partial: true}
	}
	proxy := pool.items[index]
	raw := proxy.name
	authPoolName := g.authPoolName()
	anonPoolName := g.cfg.ProxyRouting.Anonymous
	anonModel, anonProto, anonOK := g.bulkProbeModel(TierZen, true)
	authModel, authProto, authOK := g.bulkProbeModel(TierZen, false)
	hasAuthKeys := len(g.credentials()) > 0
	noModel := map[string]bool{}
	if !anonOK {
		noModel["anonymous"] = true
	}
	if !hasAuthKeys {
		noModel["unconfigured"] = true
	} else if !authOK {
		noModel["authenticated"] = true
	}
	targets := make([]bulkSendTarget, 0, 8)
	truncated := false
	skipped := 0
	addOne := func(tier Tier, credKey, credID, credDisp string, isPublic bool, model string, proto Protocol, ok bool) {
		if !ok {
			return
		}
		if len(targets) >= bulkMaxTotalSends {
			skipped++
			truncated = true
			return
		}
		targets = append(targets, bulkSendTarget{
			PoolName: poolName, Index: proxy.index, Proxy: proxy, Raw: raw,
			Tier: tier, CredKey: credKey, CredID: credID, CredDisp: credDisp, IsPublic: isPublic,
			ProbeModel: model, ProbeProtocol: proto,
		})
	}
	// Anonymous applies when this pool serves the anonymous channel;
	// authenticated applies when it serves the authenticated channel.
	if poolName == anonPoolName {
		addOne(TierZen, anonymousZenKey, anonymousSchedulerCredentialID, anonymousCredentialID, true, anonModel, anonProto, anonOK)
	}
	if poolName == authPoolName && hasAuthKeys {
		for _, cred := range g.credentials() {
			addOne(TierZen, cred.key, cred.id, cred.display, false, authModel, authProto, authOK)
		}
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
	if tested == 0 {
		partial = true
	}
	g.applyBulkWrites(ctx, results)
	// Reuse the batch aggregation so outcome labels match batch exactly.
	// buildBulkResponse seeds all active nodes; filter to the scoped node for
	// the scoped response shape.
	full := g.buildBulkResponse(checkedAt, 1, tested, 0, truncated, partial, results, []bulkCustomResult{}, sortedNoModelList(noModel))
	wantRedacted := redactURL(raw)
	var single *bulkNodeAvailability
	for _, n := range full.Nodes {
		if n.Pool == poolName && n.Index == proxy.index {
			cp := n
			single = &cp
			break
		}
	}
	if single == nil {
		for _, n := range full.Nodes {
			if n.Pool == poolName && n.ProxyNode == wantRedacted {
				cp := n
				single = &cp
				break
			}
		}
	}
	nodes := []bulkNodeAvailability{}
	if single != nil {
		nodes = append(nodes, *single)
	}
	scoped := bulkCheckResponse{
		CheckedAt: checkedAt, TotalNodes: 1, TestedNodes: tested,
		SkippedNodes: skipped, Truncated: truncated, Partial: partial,
		Nodes: nodes, Credentials: full.Credentials, NoModel: full.NoModel,
	}
	g.mergeScopedSnapshotRows(checkedAt, nodes, full.Credentials)
	return scoped
}

// mergeScopedSnapshotRows updates the fresh pool+index rows (and any tested
// credential rows for that pool) while preserving all unrelated nodes' last
// batch state. The global batch timestamp is preserved when a prior snapshot
// exists so 上次批量检测 does not move on a single-node check.
func (g *Gateway) mergeScopedSnapshotRows(checkedAt time.Time, nodes []bulkNodeAvailability, creds []bulkCredentialAvailability) {
	existing := g.bulkSnapshot.Load()
	if existing == nil || existing.CheckedAt.IsZero() {
		snap := &bulkAvailabilitySnapshot{
			CheckedAt: checkedAt, TotalNodes: 1, TestedNodes: len(nodes),
			Nodes:       append([]bulkNodeAvailability(nil), nodes...),
			Credentials: append([]bulkCredentialAvailability(nil), creds...),
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
	for _, fresh := range nodes {
		replaced := false
		for i, old := range snap.Nodes {
			if old.Pool == fresh.Pool && old.Index == fresh.Index {
				snap.Nodes[i] = fresh
				replaced = true
				break
			}
		}
		if !replaced {
			for i, old := range snap.Nodes {
				if old.Pool == fresh.Pool && old.ProxyNode == fresh.ProxyNode {
					snap.Nodes[i] = fresh
					replaced = true
					break
				}
			}
		}
		if !replaced && len(snap.Nodes) < bulkMaxSnapshotNodes {
			snap.Nodes = append(snap.Nodes, fresh)
		}
	}
	for _, fresh := range creds {
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
	}
	// Keep deterministic order for stable projections.
	sort.Slice(snap.Nodes, func(i, j int) bool {
		if snap.Nodes[i].Pool != snap.Nodes[j].Pool {
			return snap.Nodes[i].Pool < snap.Nodes[j].Pool
		}
		return snap.Nodes[i].Index < snap.Nodes[j].Index
	})
	g.bulkSnapshot.Store(snap)
}
