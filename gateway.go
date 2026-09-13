package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

const maxRequestBody = 32 << 20

const anonymousZenKey = "public"

const (
	proxyHealthCheckURL      = "https://cloudflare.com/cdn-cgi/trace"
	proxyHealthCheckInterval = 15 * time.Minute
	proxyHealthCheckTimeout  = 10 * time.Second
)

type Gateway struct {
	cfg       Config
	logger    *slog.Logger
	pools     map[string]*transportPool
	scheduler *targetScheduler
	zenCreds  []credentialRef
	goCreds   []credentialRef
	catalog   *modelCatalog
	monitor   *Monitor
}

type healthResponse struct {
	Status  string        `json:"status"`
	Ready   bool          `json:"ready"`
	Version string        `json:"version"`
	Models  healthModels  `json:"models"`
	Keys    healthKeys    `json:"keys"`
	Proxies healthProxies `json:"proxies"`
	Issues  []string      `json:"issues,omitempty"`
}

type healthModels struct {
	Status            string     `json:"status"`
	Total             int        `json:"total"`
	Exposed           int        `json:"exposed"`
	Zen               int        `json:"zen"`
	Go                int        `json:"go"`
	LastRefresh       *time.Time `json:"last_refresh,omitempty"`
	StaleAfterSeconds int        `json:"stale_after_seconds"`
	CacheSource       string     `json:"cache_source,omitempty"`
	Stale             bool       `json:"stale"`
}

type healthKeys struct {
	Zen       int  `json:"zen"`
	Go        int  `json:"go"`
	Total     int  `json:"total"`
	Anonymous bool `json:"anonymous"`
}

type healthProxies struct {
	Total     int `json:"total"`
	Healthy   int `json:"healthy"`
	Unhealthy int `json:"unhealthy"`
}

func NewGateway(cfg Config, logger *slog.Logger, monitor *Monitor) (*Gateway, error) {
	timeout := time.Duration(cfg.Retry.TimeoutSeconds) * time.Second
	pools := make(map[string]*transportPool, len(cfg.UniqueActivePools()))
	for _, name := range cfg.UniqueActivePools() {
		transports, err := newTransportPool(name, cfg.RuntimeProxiesFor(name), cfg.Performance, timeout)
		if err != nil {
			return nil, fmt.Errorf("proxy pool %q: %w", name, err)
		}
		if existing, ok := pools[name]; ok && existing != nil {
			continue
		}
		pools[name] = transports
	}
	// Same routing reference shares one transportPool pointer; different
	// references stay isolated. A referenced pool resolves to the same
	// instance when two channels name the same pool.
	if pools[cfg.ProxyRouting.Zen] == nil || pools[cfg.ProxyRouting.Go] == nil || pools[cfg.ProxyRouting.Anonymous] == nil {
		return nil, fmt.Errorf("proxy_routing must reference existing pools")
	}
	cooldown := time.Duration(cfg.Performance.FailureCooldownSeconds) * time.Second
	catalog := newModelCatalog(cfg.Prefer, cfg.Models.Protocols)
	catalog.SetRefreshInterval(time.Duration(cfg.Models.RefreshSeconds) * time.Second)
	return &Gateway{
		cfg:       cfg,
		logger:    logger,
		pools:     pools,
		scheduler: newTargetScheduler(cooldown),
		zenCreds:  credentialsForKeys(TierZen, cfg.ZenKeys),
		goCreds:   credentialsForKeys(TierGo, cfg.GoKeys),
		catalog:   catalog,
		monitor:   monitor,
	}, nil
}

func (g *Gateway) uniquePools() []*transportPool {
	seen := map[*transportPool]bool{}
	out := []*transportPool{}
	for _, name := range g.cfg.UniqueActivePools() {
		pool := g.pools[name]
		if pool == nil || seen[pool] {
			continue
		}
		seen[pool] = true
		out = append(out, pool)
	}
	return out
}

func (g *Gateway) healthyClients() []*http.Client {
	var clients []*http.Client
	for _, pool := range g.uniquePools() {
		for _, proxy := range pool.items {
			if proxy != nil && proxy.healthy.Load() {
				clients = append(clients, proxy.client)
			}
		}
	}
	return clients
}

func (g *Gateway) poolForProxy(proxy *proxyTransport) *transportPool {
	if proxy == nil {
		return nil
	}
	for _, pool := range g.uniquePools() {
		if pool.containsProxy(proxy) {
			return pool
		}
	}
	// Fallback by pool name for proxies constructed outside the gateway
	// (tests): match the named pool directly.
	if proxy.pool != "" {
		return g.pools[proxy.pool]
	}
	return nil
}

func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/models", g.authenticate(g.handleModels))
	mux.HandleFunc("POST /v1/chat/completions", g.authenticate(g.handleInference(ProtocolChat)))
	mux.HandleFunc("POST /v1/responses", g.authenticate(g.handleInference(ProtocolResponses)))
	mux.HandleFunc("POST /v1/messages", g.authenticate(g.handleInference(ProtocolAnthropic)))
	mux.HandleFunc("GET /healthz", g.handleHealth)
	return recoveryMiddleware(g.logger, mux)
}

func (g *Gateway) handleHealth(w http.ResponseWriter, _ *http.Request) {
	models := g.catalog.Snapshot()
	proxyTotal, proxyHealthy := 0, 0
	for _, pool := range g.uniquePools() {
		total, healthy := pool.healthCounts()
		proxyTotal += total
		proxyHealthy += healthy
	}
	zenKeys, goKeys := len(g.zenCreds), len(g.goCreds)
	staleAfter := max(2*time.Duration(g.cfg.Models.RefreshSeconds)*time.Second, time.Minute)

	modelStatus := "ready"
	var lastRefresh *time.Time
	issues := make([]string, 0, 3)
	if models.UpdatedAt.IsZero() {
		modelStatus = "pending"
		issues = append(issues, "model_catalog_pending")
	} else {
		updatedAt := models.UpdatedAt.UTC()
		lastRefresh = &updatedAt
		if models.Exposed == 0 {
			modelStatus = "empty"
			issues = append(issues, "model_catalog_empty")
		} else if models.Stale || time.Since(models.UpdatedAt) > staleAfter {
			modelStatus = "stale"
			issues = append(issues, "model_catalog_stale")
		}
	}
	if zenKeys+goKeys == 0 && !g.cfg.Anonymous {
		issues = append(issues, "no_upstream_keys")
	}
	if proxyHealthy == 0 {
		issues = append(issues, "no_healthy_proxies")
	}

	status := "ok"
	if len(issues) > 0 {
		status = "degraded"
	}
	blocking := modelStatus == "pending" || modelStatus == "empty" || proxyHealthy == 0
	httpStatus := http.StatusOK
	ready := !blocking
	if blocking {
		httpStatus = http.StatusServiceUnavailable
		if modelStatus == "pending" {
			status = "starting"
		}
	}
	writeJSON(w, httpStatus, healthResponse{
		Status:  status,
		Ready:   ready,
		Version: version,
		Models: healthModels{
			Status:            modelStatus,
			Total:             models.Total,
			Exposed:           models.Exposed,
			Zen:               models.Zen,
			Go:                models.Go,
			LastRefresh:       lastRefresh,
			StaleAfterSeconds: int(staleAfter / time.Second),
			CacheSource:       models.CacheSource,
			Stale:             models.Stale,
		},
		Keys: healthKeys{Zen: zenKeys, Go: goKeys, Total: zenKeys + goKeys, Anonymous: g.cfg.Anonymous},
		Proxies: healthProxies{
			Total:     proxyTotal,
			Healthy:   proxyHealthy,
			Unhealthy: proxyTotal - proxyHealthy,
		},
		Issues: issues,
	})
}

func (g *Gateway) authenticate(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		candidates := []string{strings.TrimSpace(r.Header.Get("x-api-key"))}
		if auth := r.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			candidates = append(candidates, strings.TrimSpace(auth[7:]))
		}
		valid := false
		for _, key := range g.cfg.ServerKeys {
			for _, candidate := range candidates {
				if len(candidate) == len(key) && subtle.ConstantTimeCompare([]byte(candidate), []byte(key)) == 1 {
					valid = true
				}
			}
		}
		if !valid {
			protocol := ProtocolChat
			if r.URL.Path == "/v1/messages" {
				protocol = ProtocolAnthropic
			}
			writeAPIError(w, protocol, http.StatusUnauthorized, "invalid local API key", "authentication_error", "")
			return
		}
		next(w, r)
	}
}

func (g *Gateway) handleModels(w http.ResponseWriter, _ *http.Request) {
	now := time.Now().Unix()
	models := g.catalog.List()
	data := make([]map[string]any, 0, len(models))
	for _, model := range models {
		if g.cfg.Anonymous && len(g.cfg.ZenKeys) == 0 && len(g.cfg.GoKeys) == 0 && !g.catalog.anonymousDecision(model).Allowed {
			continue
		}
		route, err := g.catalog.Route(model, len(g.cfg.ZenKeys) > 0, len(g.cfg.GoKeys) > 0, g.cfg.Anonymous)
		if err != nil {
			continue
		}
		// Tier-scoped metadata: the advertised context window must match
		// the tier that will serve the request (anonymous ⇒ Zen).
		md := g.catalog.MetadataForTier(model, route.Tier)
		entry := map[string]any{
			"id": model, "object": "model", "created": now, "owned_by": "opencode",
			"metadata": md,
		}
		// Top-level OpenAI-standard fields: discovery clients (jcode, Pi, …)
		// read context/reasoning at the top level of each model entry.
		if md.ContextWindow > 0 {
			entry["context_window"] = md.ContextWindow
			entry["context_length"] = md.ContextWindow
		}
		if md.MaxInput > 0 {
			entry["max_input"] = md.MaxInput
		}
		if md.MaxOutput > 0 {
			entry["max_output"] = md.MaxOutput
		}
		if md.Reasoning {
			entry["reasoning"] = true
			entry["supports_reasoning"] = true
		}
		if md.ToolCall {
			entry["tool_call"] = true
		}
		if md.StructuredOutput {
			entry["structured_output"] = true
		}
		data = append(data, entry)
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

func (g *Gateway) handleInference(external Protocol) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody))
		if err != nil {
			writeAPIError(w, external, http.StatusBadRequest, "request body is too large or unreadable", "invalid_request_error", "")
			return
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			writeAPIError(w, external, http.StatusBadRequest, "request body must be a JSON object", "invalid_request_error", "")
			return
		}
		model := stringAt(payload, "model")
		meta := metaFromRequest(r)
		if meta != nil {
			meta.Model = model
		}
		if model == "" {
			writeAPIError(w, external, http.StatusBadRequest, "model is required", "invalid_request_error", "model")
			return
		}
		if !g.catalog.Supported(model) {
			writeAPIError(w, external, http.StatusBadRequest, "the model uses an upstream protocol that opencode2api does not expose", "invalid_request_error", "model")
			return
		}
		route, err := g.catalog.Route(model, len(g.cfg.ZenKeys) > 0, len(g.cfg.GoKeys) > 0, g.cfg.Anonymous)
		if err != nil {
			writeAPIError(w, external, http.StatusBadRequest, err.Error(), "invalid_request_error", "model")
			return
		}
		if meta != nil {
			meta.Tier = string(route.Tier)
		}
		bodies, err := g.prepareRouteBodies(external, route, payload)
		if err != nil {
			writeAPIError(w, external, http.StatusBadRequest, err.Error(), "invalid_request_error", "")
			return
		}
		ids := deriveRequestIDs(r, payload)
		if meta != nil {
			meta.Request = ids.Request
		}
		stream := boolAt(payload, "stream")
		requestCtx, cancel := context.WithTimeout(r.Context(), time.Duration(g.cfg.Retry.TimeoutSeconds)*time.Second)
		defer cancel()
		resp, upstreamRoute, err := g.doUpstream(requestCtx, route, bodies, ids)
		if err != nil {
			finalTier := route.Tier
			if meta != nil && meta.Tier != "" {
				finalTier = Tier(meta.Tier)
			}
			keyID, channel, anonymous := requestCredential(requestCtx)
			g.logger.Warn("all upstream attempts failed", "component", "upstream", "event", "request_failed", "request_id", ids.Request, "tier", finalTier, "key_id", keyID, "channel", channel, "anonymous", anonymous, "error", err)
			writeAPIError(w, external, http.StatusBadGateway, "all upstream attempts failed", "upstream_error", ids.Request)
			return
		}
		defer resp.Body.Close()
		if meta != nil {
			meta.Tier = string(upstreamRoute.Tier)
		}
		w.Header().Set("x-request-id", ids.Request)
		if resp.StatusCode/100 != 2 {
			copyErrorResponse(w, external, resp, ids.Request)
			return
		}
		if stream {
			if meta != nil {
				meta.Stream = true
			}
			if g.monitor != nil {
				g.monitor.activeStreams.Add(1)
				defer g.monitor.activeStreams.Add(-1)
			}
			w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("X-Accel-Buffering", "no")
			w.WriteHeader(resp.StatusCode)
			var usage bridgeUsage
			var usageReported bool
			if external == upstreamRoute.Protocol {
				usage, usageReported, err = forwardSSEWithUsageContext(r.Context(), w, resp.Body, upstreamRoute.Protocol, model)
			} else {
				usage, usageReported, err = transcodeStreamWithUsageContext(r.Context(), w, resp.Body, upstreamRoute.Protocol, external, model)
			}
			if meta != nil {
				meta.Usage, meta.UsageReported = usage, usageReported
			}
			if err != nil && !errors.Is(err, context.Canceled) {
				g.logger.Debug("downstream stream ended with an error", "component", "stream", "event", "stream_failed", "request_id", ids.Request, "model", model, "tier", route.Tier, "error", err)
			}
			return
		}
		responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		if err != nil {
			writeAPIError(w, external, http.StatusBadGateway, "failed to read upstream response", "upstream_error", ids.Request)
			return
		}
		if usage, reported := extractResponseUsage(upstreamRoute.Protocol, responseBody); meta != nil {
			meta.Usage, meta.UsageReported = usage, reported
		}
		if external != upstreamRoute.Protocol {
			responseBody, err = convertResponse(upstreamRoute.Protocol, external, responseBody)
			if err != nil {
				g.logger.Warn("response protocol conversion failed", "component", "conversion", "event", "response_conversion_failed", "request_id", ids.Request, "model", model, "source_protocol", upstreamRoute.Protocol, "target_protocol", external, "error", err)
				writeAPIError(w, external, http.StatusBadGateway, "unsupported upstream response", "upstream_error", ids.Request)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(responseBody)
	}
}

func (g *Gateway) prepareRouteBodies(from Protocol, route modelRoute, input map[string]any) (map[Tier][]byte, error) {
	tiers := make([]Tier, 0, len(route.KeyTiers)+1)
	seen := make(map[Tier]bool, len(route.KeyTiers)+1)
	addTier := func(tier Tier) {
		if tier != TierZen && tier != TierGo || seen[tier] {
			return
		}
		seen[tier] = true
		tiers = append(tiers, tier)
	}
	addTier(route.Tier)
	for _, tier := range route.KeyTiers {
		addTier(tier)
	}
	if len(tiers) == 0 {
		return nil, errors.New("no usable upstream tier")
	}
	bodies := make(map[Tier][]byte, len(tiers))
	for _, tier := range tiers {
		protocol := route.ProtocolFor(tier)
		baseURL := g.cfg.Upstream.Zen
		if tier == TierGo {
			baseURL = g.cfg.Upstream.Go
		}
		upstreamPayload, err := prepareUpstreamRequest(from, protocol, input, baseURL)
		if err != nil {
			if tier != route.Tier {
				// A fallback tier may use a stricter wire format than the
				// preferred tier. Do not reject a request before the preferred
				// upstream has even been tried; that tier is attempted only if
				// the request actually falls back.
				continue
			}
			return nil, fmt.Errorf("prepare %s upstream request: %w", tier, err)
		}
		encoded, err := json.Marshal(upstreamPayload)
		if err != nil {
			return nil, errors.New("request contains unsupported JSON values")
		}
		bodies[tier] = encoded
	}
	return bodies, nil
}

func (g *Gateway) doUpstream(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs) (*http.Response, modelRoute, error) {
	resp, effectiveRoute, attempts, err := g.doUpstreamTiers(ctx, route, bodies, ids, 0)
	if err != nil || resp == nil || resp.StatusCode != http.StatusBadRequest {
		return resp, effectiveRoute, err
	}
	origBody := resp.Body
	errBody, readErr := io.ReadAll(io.LimitReader(origBody, 1<<20))
	// The original network body is always closed here: the retry path below
	// reuses the connection, otherwise downstream receives a fresh in-memory
	// reader over the cached bytes.
	drainAndClose(origBody)
	if readErr != nil {
		resp.Body = io.NopCloser(bytes.NewReader(errBody))
		return resp, effectiveRoute, nil
	}
	// Restore the body so downstream error handling still sees the original
	// payload when no retry happens below.
	resp.Body = io.NopCloser(bytes.NewReader(errBody))
	if !isStaleReasoningReference(errBody) {
		return resp, effectiveRoute, nil
	}
	stripped, changed := stripStaleReasoningInputs(effectiveRoute, bodies)
	if !changed {
		return resp, effectiveRoute, nil
	}
	// The referenced reasoning items belong to an upstream chain this session
	// can no longer address (e.g. an interrupted stream). Replay once without
	// them under a fresh upstream session instead of failing the client
	// request outright. Attempt numbering continues from the first round so
	// monitoring never shows duplicate attempt numbers for one request.
	retryIDs := ids
	retryIDs.Session = randomID("ses", 12)
	g.logger.Info("retrying upstream without stale reasoning references", "component", "upstream", "event", "reasoning_reference_retry", "request_id", ids.Request, "model", route.ID, "tier", effectiveRoute.Tier, "attempt_offset", attempts)
	retryResp, retryRoute, _, retryErr := g.doUpstreamTiers(ctx, effectiveRoute, stripped, retryIDs, attempts)
	if retryErr != nil || retryResp == nil || retryResp.StatusCode/100 != 2 {
		retryStatus := 0
		if retryResp != nil {
			retryStatus = retryResp.StatusCode
			drainAndClose(retryResp.Body)
		}
		g.logger.Warn("reasoning reference retry failed; returning original error", "component", "upstream", "event", "reasoning_reference_retry_failed", "request_id", ids.Request, "model", route.ID, "tier", effectiveRoute.Tier, "error", retryErr, "retry_status", retryStatus)
		fallback := *resp
		fallback.Body = io.NopCloser(bytes.NewReader(errBody))
		return &fallback, effectiveRoute, nil
	}
	return retryResp, retryRoute, nil
}

// isStaleReasoningReference reports whether an upstream 400 body describes a
// reasoning item/reference the server no longer recognizes, such as
// "Referenced reasoning item 'rs_...' was not found or has expired".
// Generic validation errors that merely mention reasoning (e.g. "unknown
// reasoning field") must NOT match, so both the target phrase and the
// gone/expired marker are required.
func isStaleReasoningReference(body []byte) bool {
	text := strings.ToLower(string(body))
	if !strings.Contains(text, "reasoning item") && !strings.Contains(text, "reasoning reference") {
		return false
	}
	for _, marker := range []string{"not found", "expir", "does not exist", "no longer"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

// stripStaleReasoningInputs removes server-issued reasoning references from
// Responses-protocol upstream payloads: replayed "reasoning" input items and
// any previous_response_id chain link. Other tiers/protocols are passed
// through untouched. It reports whether any payload actually changed.
func stripStaleReasoningInputs(route modelRoute, bodies map[Tier][]byte) (map[Tier][]byte, bool) {
	changed := false
	out := make(map[Tier][]byte, len(bodies))
	for tier, body := range bodies {
		if len(body) == 0 || route.ProtocolFor(tier) != ProtocolResponses {
			out[tier] = body
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			out[tier] = body
			continue
		}
		tierChanged := false
		if _, ok := payload["previous_response_id"]; ok {
			delete(payload, "previous_response_id")
			tierChanged = true
		}
		if raw, ok := payload["input"].([]any); ok {
			kept := make([]any, 0, len(raw))
			for _, item := range raw {
				if m, ok := item.(map[string]any); ok && stringAt(m, "type") == "reasoning" {
					tierChanged = true
					continue
				}
				kept = append(kept, item)
			}
			if tierChanged {
				payload["input"] = kept
			}
		}
		if !tierChanged {
			out[tier] = body
			continue
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			out[tier] = body
			continue
		}
		out[tier] = encoded
		changed = true
	}
	return out, changed
}

func (g *Gateway) doUpstreamTiers(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, attemptOffset int) (*http.Response, modelRoute, int, error) {
	var lastResponse *http.Response
	var lastErr error
	effectiveRoute := route
	attempts := attemptOffset
	if route.Anonymous {
		resp, err, used := g.doAnonymousUpstream(ctx, route, bodies, ids, attempts)
		attempts += used
		if err == nil && resp != nil && resp.StatusCode/100 == 2 {
			return resp, route, attempts, nil
		}
		lastResponse, lastErr = resp, err
		if len(route.KeyTiers) > 0 {
			g.logger.Debug("anonymous request did not succeed; entering preferred key tiers", "component", "upstream", "event", "anonymous_fallback", "request_id", ids.Request, "attempts", attempts, "key_tiers", route.KeyTiers)
		}
	}

	keyTiers := route.KeyTiers
	if !route.Anonymous && len(keyTiers) == 0 && (route.Tier == TierZen || route.Tier == TierGo) {
		keyTiers = []Tier{route.Tier}
	}
	for _, tier := range keyTiers {
		if lastResponse != nil {
			drainAndClose(lastResponse.Body)
			lastResponse = nil
		}
		keyRoute := route
		keyRoute.Tier = tier
		keyRoute.Anonymous = false
		keyRoute.Protocol = route.ProtocolFor(tier)
		effectiveRoute = keyRoute
		resp, err, used := g.doKeyUpstream(ctx, keyRoute, bodies, ids, attempts)
		attempts += used
		if err == nil && resp != nil && resp.StatusCode/100 == 2 {
			return resp, keyRoute, attempts, nil
		}
		lastResponse, lastErr = resp, err
	}
	if lastResponse != nil {
		return lastResponse, effectiveRoute, attempts, nil
	}
	if lastErr == nil {
		lastErr = errors.New("no usable upstream route")
	}
	return nil, effectiveRoute, attempts, lastErr
}

// doAnonymousUpstream walks the frozen anonymous target list once: the fixed
// anonymous credential x every currently available proxy. Any failure,
// including an HTTP error response, advances to the next target. Only a
// successful response ends the anonymous phase; exhausting the list returns
// control to the preferred authenticated tiers. The list is never truncated
// by retry.max_attempts.
func (g *Gateway) doAnonymousUpstream(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, attemptOffset int) (*http.Response, error, int) {
	var lastResponse *http.Response
	var lastErr error
	if !g.cfg.Anonymous {
		return nil, errors.New("anonymous channel is disabled"), 0
	}
	pool := g.pools[g.cfg.ProxyRouting.Anonymous]
	body := bodies[TierZen]
	if len(body) == 0 {
		return nil, errors.New("no prepared Zen request body"), 0
	}
	now := time.Now().UnixNano()
	cands := g.scheduler.orderCandidates(g.scheduler.buildAnonymousCandidates(pool, route.ID, now), ids.Session)
	attempts := 0
	for _, cand := range cands {
		attempts++
		if meta, _ := ctx.Value(requestMetaKey{}).(*requestMeta); meta != nil {
			meta.Attempts = attemptOffset + attempts
			meta.Tier = string(TierZen)
		}
		if lastResponse != nil {
			drainAndClose(lastResponse.Body)
			lastResponse = nil
		}
		req, err := newUpstreamRequest(ctx, g.cfg.Upstream.Zen, route.Protocol, body, ids, cand.CredKey)
		if err != nil {
			return nil, err, attempts
		}
		setRequestCredential(ctx, TierZen, "anonymous", "anonymous", true, cand.Proxy)
		started := time.Now()
		resp, err := cand.Proxy.client.Do(req)
		duration := time.Since(started)
		class := g.applyAttemptOutcome(ctx, cand, resp, err)
		g.recordUpstreamAttemptWithClass(route, ids, attemptOffset+attempts, "anonymous", "anonymous", true, cand.Proxy, resp, err, duration, class)
		if err == nil && resp.StatusCode/100 == 2 {
			g.logger.Debug("anonymous upstream accepted request", "component", "upstream", "event", "anonymous_attempt_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "status", resp.StatusCode, "duration_ms", duration.Milliseconds())
			return resp, nil, attempts
		}
		lastResponse = resp
		lastErr = err
		if err != nil {
			g.logger.Debug("anonymous transport attempt failed", "component", "upstream", "event", "anonymous_attempt_transport_failed", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "duration_ms", duration.Milliseconds(), "error", err)
		} else {
			g.logger.Debug("anonymous upstream returned an error response; trying the next proxy", "component", "upstream", "event", "anonymous_attempt_response_failed", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "status", resp.StatusCode, "duration_ms", duration.Milliseconds())
		}
	}
	if lastResponse != nil {
		return lastResponse, nil, attempts
	}
	if lastErr == nil {
		lastErr = errors.New("no healthy anonymous proxies available")
	}
	return nil, lastErr, attempts
}

func (g *Gateway) doKeyUpstream(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, attemptOffset int) (*http.Response, error, int) {
	var lastResponse *http.Response
	var lastErr error
	var creds []credentialRef
	poolName := g.cfg.ProxyRouting.Zen
	baseURL := g.cfg.Upstream.Zen
	if route.Tier == TierGo {
		creds = g.goCreds
		poolName = g.cfg.ProxyRouting.Go
		baseURL = g.cfg.Upstream.Go
	} else {
		creds = g.zenCreds
	}
	pool := g.pools[poolName]
	if len(creds) == 0 {
		return nil, fmt.Errorf("no %s nodes configured", route.Tier), 0
	}
	body := bodies[route.Tier]
	if len(body) == 0 {
		return nil, fmt.Errorf("no prepared %s request body", route.Tier), 0
	}
	// Freeze the candidate order at request start; failover walks the frozen
	// list without dynamic re-sorting. The tier budget is retry.max_attempts
	// with the first attempt included.
	now := time.Now().UnixNano()
	cands := g.scheduler.orderCandidates(g.scheduler.buildAuthCandidates(route.Tier, creds, pool, route.ID, now), ids.Session)
	limit := min(len(cands), g.cfg.Retry.MaxAttempts)
	attempts := 0
	for _, cand := range cands[:limit] {
		attempts++
		// Keep the request-level trace synchronized with the attempt that is
		// about to be sent. Only the redacted key suffix is retained.
		if meta, _ := ctx.Value(requestMetaKey{}).(*requestMeta); meta != nil {
			meta.Attempts = attemptOffset + attempts
			meta.Tier = string(route.Tier)
		}
		if lastResponse != nil {
			drainAndClose(lastResponse.Body)
			lastResponse = nil
		}
		req, err := newUpstreamRequest(ctx, baseURL, route.Protocol, body, ids, cand.CredKey)
		if err != nil {
			return nil, err, attempts
		}
		setRequestCredential(ctx, route.Tier, cand.CredDisplay, "key", false, cand.Proxy)
		attemptStarted := time.Now()
		resp, err := cand.Proxy.client.Do(req)
		attemptDuration := time.Since(attemptStarted)
		class := g.applyAttemptOutcome(ctx, cand, resp, err)
		g.recordUpstreamAttemptWithClass(route, ids, attemptOffset+attempts, cand.CredDisplay, "key", false, cand.Proxy, resp, err, attemptDuration, class)
		if err == nil && resp.StatusCode/100 == 2 {
			g.logger.Debug("upstream accepted request", "component", "upstream", "event", "attempt_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "key_id", cand.CredDisplay, "proxy", redactURL(cand.Proxy.name), "status", resp.StatusCode, "duration_ms", attemptDuration.Milliseconds())
			return resp, nil, attempts
		}
		// Request-shape errors are deterministic and must leave this tier without
		// rotating through unrelated keys. The outer route may still try the next
		// tier in prefer order. Authentication, throttling, server, and transport
		// failures remain retryable inside this tier.
		if class.Class == AttemptClassClientRejected {
			g.logger.Debug("upstream rejected a non-retryable request", "component", "upstream", "event", "attempt_rejected", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "key_id", cand.CredDisplay, "status", resp.StatusCode, "proxy", redactURL(cand.Proxy.name), "duration_ms", attemptDuration.Milliseconds())
			return resp, nil, attempts
		}
		lastResponse = resp
		lastErr = err
		if err != nil {
			g.logger.Debug("upstream transport attempt failed", "component", "upstream", "event", "attempt_transport_failed", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "key_id", cand.CredDisplay, "proxy", redactURL(cand.Proxy.name), "duration_ms", attemptDuration.Milliseconds(), "error", err)
		} else {
			g.logger.Debug("upstream returned a retryable response", "component", "upstream", "event", "attempt_retryable_response", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "key_id", cand.CredDisplay, "status", resp.StatusCode, "proxy", redactURL(cand.Proxy.name), "duration_ms", attemptDuration.Milliseconds())
		}
	}
	if lastResponse != nil {
		return lastResponse, nil, attempts
	}
	return nil, lastErr, attempts
}

// applyAttemptOutcome is the single state-update entry point for every
// upstream attempt. It classifies once, updates the three state layers, and
// returns the classification so recordUpstreamAttempt reuses the same result.
//
//   - Downstream cancellation (request ctx done): no state is touched.
//   - 2xx: clears this target and the credential 401 state; a healthy
//     response also restores proxy transport health.
//   - 401: global credential cooldown; the target is not cooled twice.
//   - 403/429/5xx: target cooldown (Retry-After wins when larger, capped).
//   - Transport error with isProxyFailure: proxy unhealthy, no target cool.
//   - Other transport errors: short target cool plus an async neutral check.
//   - Ordinary 4xx (client_rejected): neutral no-op; never clears state.
func (g *Gateway) applyAttemptOutcome(ctx context.Context, cand targetCandidate, resp *http.Response, err error) attemptClassification {
	class := classifyUpstreamAttempt(resp, err)
	if ctx != nil && ctx.Err() != nil {
		return class
	}
	status := 0
	if resp != nil {
		status = resp.StatusCode
	}
	switch {
	case err == nil && status >= 200 && status < 300:
		g.scheduler.noteTargetSuccess(cand.Identity)
		g.scheduler.noteCredentialSuccess(cand.CredID)
		if cand.Proxy != nil && !cand.Proxy.healthy.Load() {
			wasHealthy := cand.Proxy.healthy.Swap(true)
			if !wasHealthy && g.logger != nil {
				g.logger.Info("proxy connectivity restored", "component", "proxy", "event", "proxy_restored", "proxy", redactURL(cand.Proxy.name), "proxy_pool", cand.Proxy.pool)
			}
		}
	case err != nil:
		if isProxyFailure(err) {
			g.markProxyUnavailable(cand.Proxy)
			return class
		}
		g.scheduler.noteTargetFailure(cand.Identity, class.Class, 0, 0)
		g.verifyProxyAfterError(ctx, cand.Proxy, 0)
	case status == http.StatusUnauthorized:
		g.scheduler.noteCredentialAuthFailure(cand.CredID)
	case status == http.StatusForbidden || status == http.StatusTooManyRequests || status >= 500:
		var retryAfter time.Duration
		if resp != nil {
			retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
		}
		g.scheduler.noteTargetFailure(cand.Identity, class.Class, status, retryAfter)
		g.verifyProxyAfterError(ctx, cand.Proxy, status)
	default:
		// Ordinary 4xx and other responses: neutral, no state change.
	}
	return class
}

func setRequestCredential(ctx context.Context, tier Tier, keyID, channel string, anonymous bool, proxy *proxyTransport) {
	meta, _ := ctx.Value(requestMetaKey{}).(*requestMeta)
	if meta == nil {
		return
	}
	meta.Tier = string(tier)
	meta.KeyID = keyID
	meta.Channel = channel
	meta.Anonymous = anonymous
	meta.Proxy = ""
	meta.ProxyPool = ""
	if proxy != nil {
		meta.Proxy = redactURL(proxy.name)
		meta.ProxyPool = proxy.pool
	}
}

func requestCredential(ctx context.Context) (string, string, bool) {
	meta, _ := ctx.Value(requestMetaKey{}).(*requestMeta)
	if meta == nil {
		return "", "", false
	}
	return meta.KeyID, meta.Channel, meta.Anonymous
}

func extractResponseUsage(protocol Protocol, body []byte) (bridgeUsage, bool) {
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		return bridgeUsage{}, false
	}
	usage := mapAt(payload, "usage")
	if len(usage) == 0 {
		return bridgeUsage{}, false
	}
	if protocol == ProtocolAnthropic {
		return decodeAnthropicUsage(usage), true
	}
	return decodeOpenAIUsage(usage), true
}

func (g *Gateway) recordUpstreamAttemptWithClass(route modelRoute, ids requestIDs, attempt int, keyID, channel string, anonymous bool, proxy *proxyTransport, resp *http.Response, err error, duration time.Duration, class attemptClassification) {
	if g.monitor == nil {
		return
	}
	status := 0
	if resp != nil {
		status = resp.StatusCode
	}
	success := class.Class == AttemptClassSuccess
	proxyName := "unavailable"
	proxyPool := ""
	if proxy != nil {
		proxyName = redactURL(proxy.name)
		proxyPool = proxy.pool
	}
	g.monitor.RecordAttempt(UpstreamAttempt{
		Time: time.Now().UTC(), RequestID: ids.Request, Model: route.ID, Tier: string(route.Tier), Attempt: attempt,
		KeyID: keyID, Channel: channel, Anonymous: anonymous, Proxy: proxyName, ProxyPool: proxyPool, Status: status,
		DurationMS: max(duration.Milliseconds(), 0), Success: success, Outcome: outcomeFromClass(class.Class, success),
		FailureClass: class.Class, Retryable: class.Retryable, CoolsDown: class.CoolsDown,
	})
}

func newUpstreamRequest(ctx context.Context, baseURL string, protocol Protocol, body []byte, ids requestIDs, key string) (*http.Request, error) {
	endpoint := strings.TrimRight(baseURL, "/") + protocolPath(protocol)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("User-Agent", opencodeUserAgent())
	req.Header.Set("x-opencode-client", "cli")
	req.Header.Set("x-opencode-session", ids.Session)
	// OpenCode 1.18.x sends these correlation headers to preserve provider-side
	// prompt/session affinity. Keep the legacy x-opencode-session header too so
	// older Zen deployments continue to recognize the request.
	req.Header.Set("x-session-affinity", ids.Session)
	req.Header.Set("X-Session-Id", ids.Session)
	req.Header.Set("x-opencode-request", ids.Request)
	req.Header.Set("x-opencode-project", ids.Project)
	if ids.ParentSession != "" {
		req.Header.Set("x-parent-session-id", ids.ParentSession)
	}
	if protocol == ProtocolAnthropic {
		req.Header.Set("x-api-key", key)
		req.Header.Set("anthropic-version", "2023-06-01")
		req.Header.Set("anthropic-beta", "interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14")
	} else {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	return req, nil
}

func isNonRetryableClientResponse(resp *http.Response, err error) bool {
	// Single shared classification: only client_rejected ends the tier.
	// Behavior is unchanged; the mapping lives in classifyAttempt.
	return classifyUpstreamAttempt(resp, err).Class == AttemptClassClientRejected
}

// syncProxyResult updates proxy transport health from refresh traffic.
// Only timeouts and connection refusals mark a proxy unavailable; HTTP
// statuses never change health directly. Other errors and 4xx/5xx responses
// trigger a neutral URL check without being treated as proxy failure.
// Scheduler (credential/target) state is never touched here.
func (g *Gateway) syncProxyResult(ctx context.Context, proxy *proxyTransport, status int, err error) bool {
	if proxy == nil {
		return false
	}
	if isProxyFailure(err) {
		g.markProxyUnavailable(proxy)
		return true
	}
	if status >= 200 && status < 400 {
		wasHealthy := proxy.healthy.Swap(true)
		if !wasHealthy && g.logger != nil {
			g.logger.Info("proxy connectivity restored", "component", "proxy", "event", "proxy_restored", "proxy", redactURL(proxy.name), "proxy_pool", proxy.pool)
		}
		return false
	}
	if err != nil || status >= 400 && status < 600 {
		g.verifyProxyAfterError(ctx, proxy, status)
	}
	return false
}

func (g *Gateway) verifyProxyAfterError(ctx context.Context, proxy *proxyTransport, status int) {
	if !proxy.checking.CompareAndSwap(false, true) {
		return
	}
	pool := g.poolForProxy(proxy)
	if pool == nil {
		proxy.checking.Store(false)
		return
	}
	// The client request may finish or be cancelled while the verification is
	// running. Keep its values but give the proxy check an independent timeout.
	checkCtx := context.WithoutCancel(ctx)
	go func() {
		result := pool.checkClaimedProxy(checkCtx, proxy, proxyHealthCheckURL, proxyHealthCheckTimeout)
		g.applyProxyHealthResult(result, "upstream HTTP response", status)
	}()
}

func (g *Gateway) markProxyUnavailable(proxy *proxyTransport) {
	if proxy == nil {
		return
	}
	wasHealthy := proxy.healthy.Swap(false)
	if wasHealthy && g.logger != nil {
		g.logger.Warn("proxy became unavailable", "component", "proxy", "event", "proxy_unavailable", "proxy", redactURL(proxy.name), "proxy_pool", proxy.pool)
	}
}

func (g *Gateway) StartProxyHealthChecks(ctx context.Context) {
	check := func() {
		for _, pool := range g.uniquePools() {
			results := pool.CheckHealth(ctx, proxyHealthCheckURL, proxyHealthCheckTimeout)
			for _, result := range results {
				g.applyProxyHealthResult(result, "scheduled health check", 0)
			}
		}
	}
	go func() {
		ticker := time.NewTicker(proxyHealthCheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				check()
			}
		}
	}()
}

func (g *Gateway) applyProxyHealthResult(result proxyHealthResult, source string, upstreamStatus int) {
	if result.err == nil {
		if !result.wasHealthy {
			wasHealthy := result.proxy.healthy.Swap(true)
			if !wasHealthy && g.logger != nil {
				g.logger.Info("proxy connectivity restored", "component", "proxy", "event", "proxy_restored", "proxy", redactURL(result.proxy.name), "proxy_pool", result.proxy.pool)
			}
		}
		if g.logger != nil {
			g.logger.Debug("proxy health check passed", "component", "proxy", "event", "health_check_passed", "source", source, "upstream_status", upstreamStatus, "proxy", redactURL(result.proxy.name))
		}
		return
	}
	if !result.failed {
		if g.logger != nil {
			g.logger.Debug("proxy health check was inconclusive", "component", "proxy", "event", "health_check_inconclusive", "source", source, "upstream_status", upstreamStatus, "proxy", redactURL(result.proxy.name), "proxy_pool", result.proxy.pool, "error", result.err)
		}
		return
	}
	hasHealthy := false
	for _, pool := range g.uniquePools() {
		if pool.hasHealthy() {
			hasHealthy = true
			break
		}
	}
	if hasHealthy {
		g.markProxyUnavailable(result.proxy)
		if g.logger != nil {
			g.logger.Warn("proxy health check failed", "component", "proxy", "event", "health_check_failed", "source", source, "upstream_status", upstreamStatus, "proxy", redactURL(result.proxy.name), "proxy_pool", result.proxy.pool, "error", result.err)
		}
		return
	}
	if g.logger != nil {
		g.logger.Debug("proxy health check is still failing", "component", "proxy", "event", "health_check_still_failing", "source", source, "upstream_status", upstreamStatus, "proxy", redactURL(result.proxy.name), "proxy_pool", result.proxy.pool, "error", result.err)
	}
}

func protocolPath(protocol Protocol) string {
	switch protocol {
	case ProtocolResponses:
		return "/v1/responses"
	case ProtocolAnthropic:
		return "/v1/messages"
	default:
		return "/v1/chat/completions"
	}
}

func (g *Gateway) StartModelRefresh(ctx context.Context) {
	refresh := func() {
		var zen, goModels []string
		var capabilities protocolCapabilities
		var capabilitiesErr error
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); zen = g.refreshZen(ctx) }()
		go func() { defer wg.Done(); goModels = g.refreshTier(ctx, g.cfg.Upstream.Go, TierGo) }()
		go func() {
			defer wg.Done()
			capabilityCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			capabilities, capabilitiesErr = g.refreshProtocolCapabilities(capabilityCtx)
		}()
		wg.Wait()
		if ctx.Err() != nil {
			return
		}
		if capabilitiesErr != nil {
			g.logger.Warn("OpenCode capability catalog refresh failed", "component", "models", "event", "capability_refresh_failed", "error", capabilitiesErr)
		}
		if zen != nil || goModels != nil {
			g.catalog.ReplaceWithCapabilities(zen, goModels, capabilities.Protocols, capabilities.Unsupported, capabilities.Metadata)
			if ctx.Err() == nil {
				if err := g.catalog.SaveCache(); err != nil {
					g.logger.Warn("model catalog cache write failed", "component", "models", "event", "catalog_cache_write_failed", "error", err)
				}
			}
			g.logger.Info("model catalog refreshed", "component", "models", "event", "catalog_refreshed", "models", len(g.catalog.List()))
		}
	}
	go func() {
		refresh()
		ticker := time.NewTicker(time.Duration(g.cfg.Models.RefreshSeconds) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				refresh()
			}
		}
	}()
}

func (g *Gateway) refreshProtocolCapabilities(ctx context.Context) (protocolCapabilities, error) {
	clients := g.healthyClients()
	if len(clients) == 0 {
		if len(g.uniquePools()) == 0 {
			return fetchProtocolCapabilities(ctx, &http.Client{Timeout: 30 * time.Second}, openCodeCapabilitiesURL)
		}
		return protocolCapabilities{}, errors.New("no healthy proxy available for OpenCode capability catalog")
	}
	var lastErr error
	for _, client := range clients {
		capabilities, err := fetchProtocolCapabilities(ctx, client, openCodeCapabilitiesURL)
		if err == nil {
			return capabilities, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("no healthy proxy available for OpenCode capability catalog")
	}
	return protocolCapabilities{}, lastErr
}

func (g *Gateway) refreshZen(ctx context.Context) []string {
	if models := g.refreshTier(ctx, g.cfg.Upstream.Zen, TierZen); models != nil {
		return models
	}
	if !g.cfg.Anonymous {
		return nil
	}
	return g.refreshAnonymousTier(ctx, g.cfg.Upstream.Zen)
}

// refreshAnonymousTier fetches the model list with the shared public
// credential over each currently healthy proxy in the assigned pool.
// It is stateless: foreground credential/target cooldowns are neither read
// nor written; only proxy transport health gates and observes candidates.
func (g *Gateway) refreshAnonymousTier(ctx context.Context, base string) []string {
	pool := g.pools[g.cfg.ProxyRouting.Anonymous]
	if pool == nil {
		return nil
	}
	healthy := healthyProxies(pool)
	for attempt, proxy := range healthy {
		refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		models, status, err := fetchModels(refreshCtx, proxy.client, base, anonymousZenKey)
		g.syncProxyResult(refreshCtx, proxy, status, err)
		cancel()
		if err == nil {
			return models
		}
		if g.logger != nil {
			g.logger.Debug("anonymous model catalog refresh attempt failed", "component", "models", "event", "anonymous_refresh_attempt_failed", "upstream", redactURL(base), "attempt", attempt+1, "proxy", redactURL(proxy.name), "error", err)
		}
	}
	if g.logger != nil {
		g.logger.Warn("anonymous model catalog refresh failed", "component", "models", "event", "anonymous_refresh_failed", "upstream", redactURL(base))
	}
	return nil
}

// refreshTier fetches the model list with a stateless key x healthy-proxy
// traversal bounded by retry.max_attempts. Foreground scheduler state is
// never read or written here.
func (g *Gateway) refreshTier(ctx context.Context, base string, tier Tier) []string {
	var keys []string
	poolName := g.cfg.ProxyRouting.Zen
	if tier == TierGo {
		keys = g.cfg.GoKeys
		poolName = g.cfg.ProxyRouting.Go
	} else {
		keys = g.cfg.ZenKeys
	}
	if len(keys) == 0 {
		return nil
	}
	pool := g.pools[poolName]
	if pool == nil {
		return nil
	}
	healthy := healthyProxies(pool)
	if len(healthy) == 0 {
		return nil
	}
	budget := min(g.cfg.Retry.MaxAttempts, len(keys)*len(healthy))
	if budget < 1 {
		budget = 1
	}
	for attempt := 0; attempt < budget; attempt++ {
		key := keys[attempt%len(keys)]
		proxy := healthy[attempt%len(healthy)]
		refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		models, status, err := fetchModels(refreshCtx, proxy.client, base, key)
		g.syncProxyResult(refreshCtx, proxy, status, err)
		cancel()
		if err == nil {
			return models
		}
		if g.logger != nil {
			g.logger.Debug("model catalog refresh attempt failed", "component", "models", "event", "refresh_attempt_failed", "upstream", redactURL(base), "attempt", attempt+1, "error", err)
		}
	}
	if g.logger != nil {
		g.logger.Warn("model catalog refresh failed", "component", "models", "event", "refresh_failed", "upstream", redactURL(base))
	}
	return nil
}

// healthyProxies returns the currently healthy transports in config order.
func healthyProxies(pool *transportPool) []*proxyTransport {
	if pool == nil {
		return nil
	}
	out := make([]*proxyTransport, 0, len(pool.items))
	for _, proxy := range pool.items {
		if proxy != nil && proxy.healthy.Load() {
			out = append(out, proxy)
		}
	}
	return out
}

func copyErrorResponse(w http.ResponseWriter, protocol Protocol, resp *http.Response, requestID string) {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	message := http.StatusText(resp.StatusCode)
	var value map[string]any
	if json.Unmarshal(body, &value) == nil {
		message = firstString(stringAt(value, "error", "message"), stringAt(value, "message"), message)
	}
	writeAPIError(w, protocol, resp.StatusCode, message, "upstream_error", requestID)
}

func writeAPIError(w http.ResponseWriter, protocol Protocol, status int, message, kind, requestID string) {
	w.Header().Set("Content-Type", "application/json")
	if requestID != "" {
		w.Header().Set("x-request-id", requestID)
	}
	if protocol == ProtocolAnthropic {
		writeJSONStatus(w, status, map[string]any{"type": "error", "error": map[string]any{"type": kind, "message": message}})
		return
	}
	writeJSONStatus(w, status, map[string]any{"error": map[string]any{"message": message, "type": kind, "param": nil, "code": nil}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	writeJSONStatus(w, status, value)
}

func writeJSONStatus(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func recoveryMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if value := recover(); value != nil {
				logger.Error("request handler panicked", "component", "http", "event", "request_panic", "error", value)
				writeAPIError(w, ProtocolChat, http.StatusInternalServerError, "internal server error", "server_error", "")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
