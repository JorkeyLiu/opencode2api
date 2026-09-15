package main

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Management operability endpoints: safe manual proxy probing and manual
// model/metadata refresh. Both live on the Admin plane only (never on the
// Gateway inference plane), require session auth + CSRF + Origin, and emit
// no-store responses.
//
// Safety properties (also asserted by tests):
//   - The probe addresses a proxy only by (pool, pool-local index) drawn
//     from the active config. Raw URLs and secrets are never accepted, so
//     the endpoint cannot be steered at an arbitrary target (no SSRF).
//   - The probe may flip proxy transport health (the explicit management
//     probe) but never reads or writes scheduler credential/target/proxy429
//     state and never clears or sets proxy429 cooldowns.
//   - Refresh is stateless: it never reads or writes foreground
//     credential/target/proxy429 cooldowns and never changes proxy
//     healthy/checking.
//   - Manual and scheduled refreshes share one TryLock gate per component,
//     so a busy component returns 409 instead of stacking work.

const (
	proxyProbeTimeout   = 10 * time.Second
	modelsRefreshWindow = 35 * time.Second

	proxyProbeRateLimit   = 10
	refreshRateLimit      = 3
	rateLimitWindowMinute = time.Minute
)

// proxyProbeTarget is the health-check URL used by the management probe.
// Tests repoint it at a local server; production always uses the default.
var proxyProbeTarget = proxyHealthCheckURL

// allowProbe enforces an independent 10/min/client-IP budget for the probe
// endpoint. The login/debug windows are intentionally not reused so tuning
// one management action never affects the others.
func (a *AdminServer) allowProbe(client string) bool {
	return a.allowWindow(&a.probeAttempts, client, proxyProbeRateLimit, rateLimitWindowMinute)
}

// allowModelsRefresh enforces an independent 3/min/client-IP budget for the
// manual refresh endpoint.
func (a *AdminServer) allowModelsRefresh(client string) bool {
	return a.allowWindow(&a.refreshAttempts, client, refreshRateLimit, rateLimitWindowMinute)
}

func (a *AdminServer) allowWindow(windows *map[string]loginWindow, client string, limit int, window time.Duration) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	if *windows == nil {
		*windows = make(map[string]loginWindow)
	}
	if len(*windows) > 4096 {
		for key, candidate := range *windows {
			if now.Sub(candidate.Started) >= window {
				delete(*windows, key)
			}
		}
	}
	entry := (*windows)[client]
	if entry.Started.IsZero() || now.Sub(entry.Started) >= window {
		entry = loginWindow{Started: now}
	}
	if entry.Count >= limit {
		return false
	}
	entry.Count++
	(*windows)[client] = entry
	return true
}

type proxyProbeRequest struct {
	Pool  string `json:"pool"`
	Index *int   `json:"index"`
}

type proxyProbeResponse struct {
	Pool            string `json:"pool"`
	Index           int    `json:"index"`
	ProxyNode       string `json:"proxy_node"`
	Healthy         bool   `json:"healthy"`
	PreviousHealthy bool   `json:"previous_healthy"`
	Changed         bool   `json:"changed"`
	Checking        bool   `json:"checking"`
	DurationMS      int64  `json:"duration_ms"`
	Result          string `json:"result"`
}

func (a *AdminServer) handleProxyProbe(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	client := clientIP(r)
	if !a.allowProbe(client) {
		writeAdminError(w, http.StatusTooManyRequests, "probe_rate_limited", "too many probe requests; retry in one minute")
		return
	}
	var input proxyProbeRequest
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
	proxy := pool.items[*input.Index]
	ctx, cancel := context.WithTimeout(r.Context(), proxyProbeTimeout)
	defer cancel()
	if !proxy.checking.CompareAndSwap(false, true) {
		writeAdminError(w, http.StatusConflict, "proxy_busy", "a check for this proxy is already running")
		return
	}
	previous := proxy.healthy.Load()
	started := time.Now()
	// checkClaimedProxy owns the checking flag from here: it releases the
	// claim and may flip transport health. It never touches scheduler
	// credential/target/proxy429 state.
	result := pool.checkClaimedProxy(ctx, proxy, proxyProbeTarget, proxyHealthCheckTimeout)
	duration := time.Since(started)
	healthy := proxy.healthy.Load()
	outcome := "inconclusive"
	if result.err == nil {
		outcome = "healthy"
	} else if result.failed {
		outcome = "unhealthy"
	}
	redacted := redactURL(proxy.name)
	if a.logger != nil {
		fields := []any{
			"component", "proxy", "event", "proxy_probe_completed",
			"pool", poolName, "index", *input.Index,
			"proxy_node", redacted,
			"duration_ms", max(duration.Milliseconds(), 0),
			"result", outcome, "changed", healthy != previous,
		}
		if result.err != nil {
			fields = append(fields, "error", result.err)
			a.logger.Warn("proxy probe completed", fields...)
		} else {
			a.logger.Info("proxy probe completed", fields...)
		}
	}
	writeJSON(w, http.StatusOK, proxyProbeResponse{
		Pool: poolName, Index: *input.Index, ProxyNode: redacted,
		Healthy: healthy, PreviousHealthy: previous, Changed: healthy != previous,
		Checking: proxy.checking.Load(), DurationMS: max(duration.Milliseconds(), 0), Result: outcome,
	})
}

type modelsRefreshRequest struct {
	Scope string `json:"scope"`
}

type refreshComponentResult struct {
	Refreshed bool   `json:"refreshed"`
	Error     string `json:"error,omitempty"`
}

type modelsRefreshResponse struct {
	Scope            string                 `json:"scope"`
	DurationMS       int64                  `json:"duration_ms"`
	Catalog          refreshComponentResult `json:"catalog"`
	Metadata         refreshComponentResult `json:"metadata"`
	CatalogSnapshot  modelCatalogSnapshot   `json:"catalog_snapshot"`
	MetadataSnapshot MetadataSnapshot       `json:"metadata_snapshot"`
}

func (a *AdminServer) handleModelsRefresh(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	client := clientIP(r)
	if !a.allowModelsRefresh(client) {
		writeAdminError(w, http.StatusTooManyRequests, "refresh_rate_limited", "too many refresh requests; retry in one minute")
		return
	}
	var input modelsRefreshRequest
	if err := decodeAdminJSON(w, r, &input); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	scope := strings.TrimSpace(input.Scope)
	if scope != "catalog" && scope != "metadata" && scope != "all" {
		writeAdminError(w, http.StatusBadRequest, "invalid_scope", `scope must be "catalog", "metadata", or "all"`)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), modelsRefreshWindow)
	defer cancel()
	response, busy, err := a.manager.refreshModels(ctx, scope)
	if err != nil {
		writeAdminError(w, http.StatusServiceUnavailable, "refresh_unavailable", err.Error())
		return
	}
	if busy != "" {
		writeAdminError(w, http.StatusConflict, "refresh_busy", "a "+busy+" refresh is already running")
		return
	}
	writeJSON(w, http.StatusOK, response)
}

// refreshModels runs a manual catalog and/or metadata refresh against the
// captured runtime. It claims exactly the gates its scope needs before doing
// any work: when a needed component is already refreshing (manually or on
// its schedule) the call returns busy without starting anything new.
func (m *RuntimeManager) refreshModels(ctx context.Context, scope string) (*modelsRefreshResponse, string, error) {
	runtime := m.current.Load()
	if runtime == nil || runtime.gateway == nil {
		return nil, "", contextError("gateway runtime is unavailable")
	}
	gateway := runtime.gateway
	store := m.metadata
	needCatalog := scope == "catalog" || scope == "all"
	needMetadata := scope == "metadata" || scope == "all"

	started := time.Now()
	// Claim every needed gate up front so a partially busy scope never
	// starts half of the work.
	var catalogClaimed, metadataClaimed bool
	if needCatalog {
		if gateway.catalogRefreshMu.TryLock() {
			catalogClaimed = true
		} else {
			return nil, "catalog", nil
		}
	}
	if needMetadata {
		if store != nil && store.refreshMu.TryLock() {
			metadataClaimed = true
		} else if store == nil {
			if catalogClaimed {
				gateway.catalogRefreshMu.Unlock()
			}
			return nil, "", contextError("metadata store is unavailable")
		} else {
			if catalogClaimed {
				gateway.catalogRefreshMu.Unlock()
			}
			return nil, "metadata", nil
		}
	}

	response := &modelsRefreshResponse{Scope: scope}
	var wg sync.WaitGroup
	var catalogResult, metadataResult refreshComponentResult
	if needCatalog {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer gateway.catalogRefreshMu.Unlock()
			catalogResult.Refreshed = gateway.runCatalogRefresh(ctx, "manual")
			if !catalogResult.Refreshed && ctx.Err() == nil {
				catalogResult.Error = "catalog refresh did not complete"
			} else if ctx.Err() != nil && !catalogResult.Refreshed {
				catalogResult.Error = "catalog refresh did not complete"
			}
		}()
	}
	if needMetadata {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer store.refreshMu.Unlock()
			refreshStarted := time.Now()
			err := store.Refresh(ctx)
			duration := time.Since(refreshStarted)
			if m.logger != nil {
				if err != nil {
					m.logger.Warn("models.dev metadata refresh completed", "component", "models", "event", "metadata_refresh_completed", "source", "manual", "duration_ms", duration.Milliseconds(), "refreshed", false, "error", err)
				} else {
					m.logger.Info("models.dev metadata refresh completed", "component", "models", "event", "metadata_refresh_completed", "source", "manual", "duration_ms", duration.Milliseconds(), "refreshed", true, "models", store.Snapshot().Models)
				}
			}
			if err != nil {
				metadataResult.Error = redactRefreshError(m, err)
			} else {
				metadataResult.Refreshed = true
			}
		}()
	}
	wg.Wait()
	_ = catalogClaimed
	_ = metadataClaimed
	response.Catalog = catalogResult
	response.Metadata = metadataResult
	response.CatalogSnapshot = gateway.catalog.Snapshot()
	if store != nil {
		response.MetadataSnapshot = store.Snapshot()
	}
	response.DurationMS = max(time.Since(started).Milliseconds(), 0)
	return response, "", nil
}

// redactRefreshError keeps manual refresh failures operable without leaking
// configured secrets into the admin response. Transport errors only carry
// dial/timeout text, but any embedded secret is still scrubbed.
func redactRefreshError(m *RuntimeManager, err error) string {
	if err == nil || m == nil || m.redactor == nil {
		if err == nil {
			return ""
		}
		return err.Error()
	}
	return m.redactor.String(err.Error())
}

type contextErrorString string

func (e contextErrorString) Error() string { return string(e) }

func contextError(message string) error { return contextErrorString(message) }
