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
	"sync/atomic"
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
	authCreds []credentialRef
	// zenCreds mirrors authCreds for test compat; credentials() prefers
	// authCreds and falls back to zenCreds so direct test assignments to
	// either field remain effective.
	zenCreds []credentialRef
	catalog  *modelCatalog
	monitor  *Monitor
	// customClient serves custom OpenAI-compatible fallback channels directly
	// (no proxy pool). Tests override it to point at local servers.
	customClient *http.Client
	// catalogRefreshMu is the shared manual/scheduled catalog refresh gate.
	// Both paths use TryLock so a busy refresh returns 409 instead of
	// stacking. No new dependency is introduced.
	catalogRefreshMu sync.Mutex
	// bulkMu gates overlapping bulk availability checks (409 busy).
	// bulkSnapshot is the admin-only latest-result projection: fixed
	// bounded, replaced atomically per completed/partial run, never
	// routing authority. Restart/Apply clears via fresh Gateway.
	bulkMu       sync.Mutex
	bulkSnapshot atomic.Pointer[bulkAvailabilitySnapshot]
}

type healthResponse struct {
	Status  string        `json:"status"`
	Ready   bool          `json:"ready"`
	Version string        `json:"version"`
	Models  healthModels  `json:"models"`
	Keys    healthKeys    `json:"keys"`
	Proxies healthProxies `json:"proxies"`
	Routing healthRouting `json:"routing"`
	Issues  []string      `json:"issues,omitempty"`
}

type healthModels struct {
	Status            string     `json:"status"`
	Total             int        `json:"total"`
	Exposed           int        `json:"exposed"`
	Zen               int        `json:"zen"`
	LastRefresh       *time.Time `json:"last_refresh,omitempty"`
	StaleAfterSeconds int        `json:"stale_after_seconds"`
	CacheSource       string     `json:"cache_source,omitempty"`
	Stale             bool       `json:"stale"`
}

type healthKeys struct {
	Authenticated int  `json:"authenticated"`
	Total         int  `json:"total"`
	Anonymous     bool `json:"anonymous"`
}

type healthProxies struct {
	Total     int `json:"total"`
	Healthy   int `json:"healthy"`
	Unhealthy int `json:"unhealthy"`
}

// healthRouting is additive global route availability over exactly two
// channels: anonymous and authenticated (both Zen upstream). It never reads
// per-model target cooldowns: a single model's targets all cooling must not
// degrade global readiness. Anonymous availability is config plus assigned
// pool transport health only. Credential availability counts global 401
// cooldowns; channel availability additionally requires a healthy assigned
// pool, so keys without a healthy proxy do not count as a channel.
type healthRouting struct {
	AnonymousAvailable      bool `json:"anonymous_available"`
	AuthenticatedAvailable  int  `json:"authenticated_available"`
	ZenCredentialsAvailable int  `json:"zen_credentials_available"`
	CredentialsCooling      int  `json:"credentials_cooling"`
	ChannelsAvailable       int  `json:"channels_available"`
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
	authPool := cfg.ProxyRouting.Authenticated
	if authPool == "" {
		authPool = cfg.ProxyRouting.Zen
	}
	if pools[authPool] == nil || pools[cfg.ProxyRouting.Anonymous] == nil {
		return nil, fmt.Errorf("proxy_routing must reference existing pools")
	}
	cooldown := secondsToDuration(cfg.Performance.FailureCooldownSeconds)
	if cooldown <= 0 {
		cooldown = 15 * time.Second
	}
	rateCooldown := secondsToDuration(cfg.Performance.RateLimitCooldownSeconds)
	if rateCooldown <= 0 {
		rateCooldown = defaultRateLimitBaseSeconds * time.Second
	}
	catalog := newModelCatalog("", cfg.Models.Protocols)
	catalog.SetRefreshInterval(time.Duration(cfg.Models.RefreshSeconds) * time.Second)
	authCreds := credentialsForKeys(TierZen, cfg.Keys)
	return &Gateway{
		cfg:       cfg,
		logger:    logger,
		pools:     pools,
		scheduler: newTargetScheduler(cooldown, rateCooldown),
		authCreds: authCreds,
		zenCreds:  authCreds,
		catalog:   catalog,
		monitor:   monitor,
	}, nil
}

// credentials returns the single authenticated lane, preferring authCreds and
// falling back to the legacy zenCreds test alias.
func (g *Gateway) credentials() []credentialRef {
	if g == nil {
		return nil
	}
	if len(g.authCreds) > 0 {
		return g.authCreds
	}
	return g.zenCreds
}

// authPoolName returns the canonical authenticated pool, falling back to the
// legacy zen routing value for in-memory compat.
func (g *Gateway) authPoolName() string {
	if g == nil {
		return ""
	}
	if g.cfg.ProxyRouting.Authenticated != "" {
		return g.cfg.ProxyRouting.Authenticated
	}
	return g.cfg.ProxyRouting.Zen
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

// CloseIdleConnections closes idle connections on every unique transport
// pool. Shared routing references resolve to the same pool pointer and are
// closed exactly once via uniquePools. Only idle connections are closed;
// in-flight active connections are never interrupted.
func (g *Gateway) CloseIdleConnections() {
	if g == nil {
		return
	}
	for _, pool := range g.uniquePools() {
		pool.CloseIdleConnections()
	}
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
	authKeys := len(g.credentials())
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
	if authKeys == 0 && !g.cfg.Anonymous {
		issues = append(issues, "no_upstream_keys")
	}
	if proxyHealthy == 0 {
		issues = append(issues, "no_healthy_proxies")
	}
	routing := g.routingReadiness()
	if routing.ChannelsAvailable == 0 {
		issues = append(issues, "no_available_routes")
	}

	status := "ok"
	if len(issues) > 0 {
		status = "degraded"
	}
	blocking := modelStatus == "pending" || modelStatus == "empty" || proxyHealthy == 0 || routing.ChannelsAvailable == 0
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
			LastRefresh:       lastRefresh,
			StaleAfterSeconds: int(staleAfter / time.Second),
			CacheSource:       models.CacheSource,
			Stale:             models.Stale,
		},
		Keys: healthKeys{Authenticated: authKeys, Total: authKeys, Anonymous: g.cfg.Anonymous},
		Proxies: healthProxies{
			Total:     proxyTotal,
			Healthy:   proxyHealthy,
			Unhealthy: proxyTotal - proxyHealthy,
		},
		Routing: routing,
		Issues:  issues,
	})
}

// routingReadiness reports additive global route availability over the two
// channels. It reads only config, proxy transport health, and global
// credential 401 cooldowns; per-model target cooldowns and global proxy429
// cooldowns are never consulted, so one model's backoff or one proxy's
// rate-limit cooldown cannot trigger a global 503. An expired cooldown counts
// as available immediately.
func (g *Gateway) routingReadiness() healthRouting {
	now := time.Now().UnixNano()
	anonPool := g.pools[g.cfg.ProxyRouting.Anonymous]
	anonymousAvailable := g.cfg.Anonymous && anonPool != nil && anonPool.hasHealthy()
	creds := g.credentials()
	authAvailable := 0
	for _, cred := range creds {
		if g.scheduler == nil || g.scheduler.credentialCoolUntil(cred.id) <= now {
			authAvailable++
		}
	}
	cooling := len(creds) - authAvailable
	channels := 0
	if anonymousAvailable {
		channels++
	}
	authPoolName := g.authPoolName()
	if len(creds) > 0 && authAvailable > 0 {
		if pool := g.pools[authPoolName]; pool != nil && pool.hasHealthy() {
			channels++
		}
	}
	return healthRouting{
		AnonymousAvailable:      anonymousAvailable,
		AuthenticatedAvailable:  authAvailable,
		ZenCredentialsAvailable: authAvailable,
		CredentialsCooling:      cooling,
		ChannelsAvailable:       channels,
	}
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
		if g.cfg.Anonymous && len(g.cfg.Keys) == 0 && !g.catalog.anonymousDecision(model).Allowed {
			continue
		}
		route, err := g.catalog.Route(model, len(g.cfg.Keys) > 0, g.cfg.Anonymous)
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
		route, err := g.catalog.Route(model, len(g.cfg.Keys) > 0, g.cfg.Anonymous)
		if err != nil {
			writeAPIError(w, external, http.StatusBadRequest, err.Error(), "invalid_request_error", "model")
			return
		}
		if meta != nil {
			meta.Tier = string(route.Tier)
			meta.Protocol = string(external)
		}
		bodies, err := g.prepareRouteBodies(external, route, payload)
		if err != nil {
			writeAPIError(w, external, http.StatusBadRequest, err.Error(), "invalid_request_error", "")
			return
		}
		ids := deriveRequestIDs(r, payload)
		if meta != nil {
			meta.Request = ids.Request
			meta.ClientSessionHash = clientSessionHash(ids.Session)
			meta.Protocol = string(external)
		}
		stream := boolAt(payload, "stream")
		requestCtx, cancel := context.WithTimeout(r.Context(), time.Duration(g.cfg.Retry.TimeoutSeconds)*time.Second)
		defer cancel()
		ex := upstreamExtra{External: external, Payload: cloneMap(payload)}
		resp, upstreamRoute, err := g.doUpstream(requestCtx, route, bodies, ids, ex)
		if err != nil {
			finalTier := route.Tier
			finalProtocol := string(external)
			if meta != nil {
				if meta.Tier != "" {
					finalTier = Tier(meta.Tier)
				}
				if meta.Protocol != "" {
					finalProtocol = meta.Protocol
				}
			}
			keyID, channel, anonymous := requestCredential(requestCtx)
			g.logger.Warn("all upstream attempts failed", "component", "upstream", "event", "request_failed", "request_id", ids.Request, "tier", finalTier, "protocol", finalProtocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", keyID, "channel", channel, "anonymous", anonymous, "error", err)
			writeAPIError(w, external, http.StatusBadGateway, "all upstream attempts failed", "upstream_error", ids.Request)
			return
		}
		defer resp.Body.Close()
		if meta != nil {
			meta.Tier = string(upstreamRoute.Tier)
			if upstreamRoute.Protocol != "" {
				meta.Protocol = string(upstreamRoute.Protocol)
			}
			// Custom fallback serves the channel-configured model, not the
			// client model. Zen/Go keep the client model untouched.
			if upstreamRoute.Tier == TierCustom && upstreamRoute.ID != "" {
				meta.Model = upstreamRoute.ID
			}
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
				g.logger.Debug("downstream stream ended with an error", "component", "stream", "event", "stream_failed", "request_id", ids.Request, "model", model, "tier", upstreamRoute.Tier, "protocol", upstreamRoute.Protocol, "client_session_hash", clientSessionHash(ids.Session), "error", err)
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
				g.logger.Warn("response protocol conversion failed", "component", "conversion", "event", "response_conversion_failed", "request_id", ids.Request, "model", model, "client_session_hash", clientSessionHash(ids.Session), "source_protocol", upstreamRoute.Protocol, "target_protocol", external, "error", err)
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
		if tier != TierZen || seen[tier] {
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

func (g *Gateway) doUpstream(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, extra ...upstreamExtra) (*http.Response, modelRoute, error) {
	// Per-candidate 400 session recovery lives inside doUpstreamTiers (fixed
	// same target, one replay with a rotated route session). No outer random
	// session retry remains here: the client session is never rewritten and
	// the frozen candidate order is never re-sorted.
	resp, effectiveRoute, _, err := g.doUpstreamTiers(ctx, route, bodies, ids, 0, extra...)
	return resp, effectiveRoute, err
}

// stripResponsesStaleRefs removes server-issued reasoning references from one
// decoded Responses payload: any previous_response_id chain link and replayed
// reasoning input items. It reports the two categories independently and never
// touches other inexpressible content.
func stripResponsesStaleRefs(payload map[string]any) (droppedPreviousResponseID bool, droppedReasoningRefs bool) {
	if _, ok := payload["previous_response_id"]; ok {
		delete(payload, "previous_response_id")
		droppedPreviousResponseID = true
	}
	if raw, ok := payload["input"].([]any); ok {
		kept := make([]any, 0, len(raw))
		stripped := false
		for _, item := range raw {
			if m, ok := item.(map[string]any); ok && stringAt(m, "type") == "reasoning" {
				stripped = true
				continue
			}
			kept = append(kept, item)
		}
		if stripped {
			payload["input"] = kept
			droppedReasoningRefs = true
		}
	}
	return droppedPreviousResponseID, droppedReasoningRefs
}

// staleCleanup reports which stale Responses categories one replay removed.
// Both flags are true only when actually removed; route_session_replay itself
// is tracked separately and stays true even when neither category existed.
type staleCleanup struct {
	DroppedPreviousResponseID bool
	DroppedReasoningRefs      bool
}

// applyRouteSessionToBody builds one candidate-local body from the frozen
// canonical tier body: it overwrites only already-present session fields
// (top-level conversation_id, metadata.session_id) with the canonical
// OpenCode-shaped wire encoding of the target-bound route session and never
// invents schema-foreign fields except the Responses cache/store defaults
// (prompt_cache_key set to the wire session, store defaulting to false). The
// canonical bytes are never mutated; when nothing changes the canonical slice
// is returned untouched. On replay for a Responses target it additionally
// drops previous_response_id and reasoning input items. Malformed session
// fields fail loudly instead of being silently dropped.
func applyRouteSessionToBody(canonical []byte, routeSession string, protocol Protocol, stripStale bool) ([]byte, error) {
	out, _, err := applyRouteSessionToBodyWithReport(canonical, routeSession, protocol, stripStale)
	return out, err
}

// applyRouteSessionToBodyWithReport is the reporting variant of
// applyRouteSessionToBody: identical body behavior, plus an independent
// per-category stale-ref report for Responses replays. Non-Responses targets
// or non-replay sends never report drops.
func applyRouteSessionToBodyWithReport(canonical []byte, routeSession string, protocol Protocol, stripStale bool) ([]byte, staleCleanup, error) {
	var report staleCleanup
	var payload map[string]any
	if err := json.Unmarshal(canonical, &payload); err != nil {
		return nil, report, fmt.Errorf("route session body rewrite: %w", err)
	}
	// Wire session is the canonical OpenCode-shaped encoding of the internal
	// target-bound rss_* token; headers use the same mapping so body and
	// header stay consistent without leaking internal or raw values.
	wireSession := routeWireSession(routeSession)
	changed := false
	if _, ok := payload["conversation_id"]; ok {
		raw := payload["conversation_id"]
		if _, ok := raw.(string); !ok {
			return nil, report, errors.New("conversation_id must be a string")
		}
		if payload["conversation_id"] != wireSession {
			payload["conversation_id"] = wireSession
			changed = true
		}
	}
	if rawMD, ok := payload["metadata"]; ok && rawMD != nil {
		md, ok := rawMD.(map[string]any)
		if ok {
			if _, ok := md["session_id"]; ok {
				if _, ok := md["session_id"].(string); !ok {
					return nil, report, errors.New("metadata.session_id must be a string")
				}
				if md["session_id"] != wireSession {
					md["session_id"] = wireSession
					changed = true
				}
			}
		}
		// Non-object metadata cannot carry session_id: retain it untouched.
		// Returning an error here would reject legitimate string metadata on
		// same-protocol passthrough, and silently dropping it would lose data.
	}
	if protocol == ProtocolResponses {
		if got, ok := payload["prompt_cache_key"]; !ok || got != wireSession {
			payload["prompt_cache_key"] = wireSession
			changed = true
		}
		if _, ok := payload["store"]; !ok {
			payload["store"] = false
			changed = true
		}
	}
	if stripStale && protocol == ProtocolResponses {
		droppedPrev, droppedReasoning := stripResponsesStaleRefs(payload)
		report.DroppedPreviousResponseID = droppedPrev
		report.DroppedReasoningRefs = droppedReasoning
		if droppedPrev || droppedReasoning {
			changed = true
		}
	}
	if !changed {
		return canonical, report, nil
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, report, errors.New("request contains unsupported JSON values")
	}
	return encoded, report, nil
}

// isRouteTerminalBadRequest reports the single route-terminal status: an
// exact HTTP 400 response (no transport error). It terminates the whole
// route: anonymous stops its remaining proxies and never enters the
// authenticated tiers, and an authenticated tier never falls back to the
// other tier. Ordinary 4xx (404/422, …) end the current channel/tier but may
// still fall back to the next channel/tier; 408/425 are transient and never
// terminal here.
func isRouteTerminalBadRequest(resp *http.Response, err error) bool {
	return err == nil && resp != nil && resp.StatusCode == http.StatusBadRequest
}

// isSameTargetTransient reports the same-target retry set: a headers-before
// transport error (caller guarantees ctx is not cancelled), HTTP 408/425, or
// HTTP 500-599. These share one per-channel transient retry token and keep
// the same route session and the same candidate body on retry.
func isSameTargetTransient(resp *http.Response, err error) bool {
	if err != nil {
		return true
	}
	if resp == nil {
		return false
	}
	status := resp.StatusCode
	if status == http.StatusRequestTimeout || status == 425 {
		return true
	}
	return status >= 500 && status <= 599
}

// isOrdinaryClientRejection reports deterministic request-shape rejections
// that must end the current channel/tier without a same-target retry:
// ordinary 4xx excluding 400 (dedicated recovery), 401/403/429 (cooldown
// fallback), and 408/425 (transient neutral fallback).
func isOrdinaryClientRejection(resp *http.Response, err error) bool {
	if err != nil || resp == nil {
		return false
	}
	status := resp.StatusCode
	if status < 400 || status >= 500 {
		return false
	}
	switch status {
	case http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusTooManyRequests,
		http.StatusRequestTimeout,
		425:
		return false
	}
	return true
}

func isContextCancelled(ctx context.Context) bool {
	return ctx != nil && ctx.Err() != nil
}

// bindSessionPin records the successful target for derived session + model.
// The key uses only the derived client session and model ID; the value is the
// full target identity plus resolvable protocol/authority. Raw client signals
// and pin state never leave the Gateway.
func (g *Gateway) bindSessionPin(session, model string, tier Tier, credID, pool, proxyRaw string, protocol Protocol, authority string) {
	if g == nil || g.scheduler == nil || session == "" || model == "" {
		return
	}
	g.scheduler.pinBind(session, model, sessionPin{
		Tier: tier, CredID: credID, Pool: pool, ProxyRaw: proxyRaw,
		Model: model, Protocol: protocol, Authority: authority,
	})
}

func pinLocalResponse(status int, retryAfterSec int64, message string) *http.Response {
	if status < 100 || status > 599 {
		status = http.StatusBadGateway
	}
	if message == "" {
		message = "upstream temporarily unavailable"
	}
	hdr := make(http.Header)
	if retryAfterSec > 0 {
		hdr.Set("Retry-After", fmt.Sprintf("%d", retryAfterSec))
	}
	encoded, _ := json.Marshal(map[string]any{"error": map[string]any{"message": message}})
	if len(encoded) == 0 {
		encoded = []byte(`{"error":{"message":"upstream temporarily unavailable"}}`)
	}
	return &http.Response{StatusCode: status, Header: hdr, Body: io.NopCloser(bytes.NewReader(encoded))}
}

func pinRetryAfterSeconds(untilUnixNano int64, now time.Time) int64 {
	remaining := untilUnixNano - now.UnixNano()
	if remaining <= 0 {
		return 0
	}
	secs := (remaining + int64(time.Second) - 1) / int64(time.Second)
	if secs < 1 {
		secs = 1
	}
	return secs
}

func (g *Gateway) doUpstreamTiers(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, attemptOffset int, extra ...upstreamExtra) (*http.Response, modelRoute, int, error) {
	if g != nil && g.scheduler != nil && g.scheduler.fallbacks != nil && ids.Session != "" {
		if binding, ok := g.scheduler.fallbacks.get(ids.Session); ok {
			return g.doCustomFallbackPinned(ctx, route, bodies, ids, binding, attemptOffset, extra...)
		}
	}
	if ids.Session != "" && route.ID != "" && g != nil && g.scheduler != nil {
		if pin, ok := g.scheduler.pinGet(ids.Session, route.ID); ok {
			return g.doPinnedUpstream(ctx, route, bodies, ids, pin, attemptOffset, extra...)
		}
		return g.doUnboundEstablishment(ctx, route, bodies, ids, attemptOffset, extra...)
	}
	return g.doUpstreamTiersUnbound(ctx, route, bodies, ids, attemptOffset, extra...)
}

// doCustomFallbackPinned serves a session already taken over by a custom
// fallback channel. It is exclusive: no anonymous/authenticated send is
// attempted. A deleted channel or a full-identity mismatch fails locally with
// 502 and never re-establishes. Custom failures return as-is with no fallback.
func (g *Gateway) doCustomFallbackPinned(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, binding fallbackBinding, attemptOffset int, extra ...upstreamExtra) (*http.Response, modelRoute, int, error) {
	effectiveRoute := route
	effectiveRoute.Tier = TierCustom
	effectiveRoute.Protocol = fallbackChannelProtocol(FallbackChannelConfig{Protocol: binding.Protocol})
	if strings.TrimSpace(binding.Model) != "" {
		effectiveRoute.ID = strings.TrimSpace(binding.Model)
	}
	if strings.TrimSpace(binding.ID) == "" && strings.TrimSpace(binding.Name) == "" {
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	ch, ok := fallbackChannelLookup(g.cfg, binding.ID)
	if !ok && strings.TrimSpace(binding.Name) != "" {
		ch, ok = fallbackChannelLookup(g.cfg, binding.Name)
	}
	if !ok || !binding.matchesChannel(ch) {
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	var ex upstreamExtra
	var hasEx bool
	if v, ok := firstUpstreamExtra(extra); ok {
		ex, hasEx = v, true
	}
	return g.doCustomFallbackRequest(ctx, route, ex, hasEx, bodies, ids, ch, binding, attemptOffset)
}

// maybeTakeoverCustomFallback binds the session to the currently active custom
// channel and immediately retries the current request through it. It returns
// handled=true when the caller must return directly (takeover bound, no
// fallback to the original 429, native tiers, or other custom channels).
// The custom send error is never swallowed: transport/build errors return as
// (nil, route, attempts, handled=true, err) so handleInference converts them
// (context cancel per existing cancel semantics, other transport failures as
// safe 502) without a nil-response panic. When no active fallback exists it
// returns handled=false so the caller keeps the original 429. Capacity
// exhaustion fails closed with 502. First-wins: a concurrent takeover keeps
// the existing binding; a tombstone mismatch fails closed with 502.
func (g *Gateway) maybeTakeoverCustomFallback(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, attemptOffset, attempts int, extra ...upstreamExtra) (*http.Response, modelRoute, int, bool, error) {
	ch, ok := activeFallbackChannel(g.cfg)
	if !ok {
		return nil, route, attemptOffset + attempts, false, nil
	}
	binding := fallbackBindingFor(ch)
	stored, _, full := g.scheduler.fallbacks.bind(ids.Session, binding)
	if full {
		effectiveRoute := route
		effectiveRoute.Tier = TierCustom
		effectiveRoute.Protocol = fallbackChannelProtocol(ch)
		if strings.TrimSpace(binding.Model) != "" {
			effectiveRoute.ID = strings.TrimSpace(binding.Model)
		}
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset + attempts, true, nil
	}
	current, exists := fallbackChannelLookup(g.cfg, stored.ID)
	if !exists && strings.TrimSpace(stored.Name) != "" {
		current, exists = fallbackChannelLookup(g.cfg, stored.Name)
	}
	if !exists || !stored.matchesChannel(current) {
		effectiveRoute := route
		effectiveRoute.Tier = TierCustom
		effectiveRoute.Protocol = fallbackChannelProtocol(FallbackChannelConfig{Protocol: stored.Protocol})
		if strings.TrimSpace(stored.Model) != "" {
			effectiveRoute.ID = strings.TrimSpace(stored.Model)
		}
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset + attempts, true, nil
	}
	var ex upstreamExtra
	var hasEx bool
	if v, ok := firstUpstreamExtra(extra); ok {
		ex, hasEx = v, true
	}
	resp, effectiveRoute, nextAttempts, takeErr := g.doCustomFallbackRequest(ctx, route, ex, hasEx, bodies, ids, current, stored, attemptOffset+attempts)
	if takeErr != nil {
		return resp, effectiveRoute, nextAttempts, true, takeErr
	}
	if resp == nil {
		return nil, effectiveRoute, nextAttempts, true, contextError("custom fallback transport failed")
	}
	return resp, effectiveRoute, nextAttempts, true, nil
}

// doUnboundEstablishment serializes initial pin establishment per
// session+model key. Exactly one owner performs the unbound fallback walk;
// followers wait context-cancellably on the owner's done channel, then adopt
// the resulting pin or contend to become the next owner when no pin was
// bound. Capacity is reserved at claim time: at the pin cap a new unpinned
// session+model fails closed locally with 502 before any upstream send,
// while existing pins keep serving. A failed owner releases its reservation
// so a later request can retry; a successful bind consumes the reserved slot
// exactly once. Different keys never block each other except through the
// shared cap. The scheduler/pin mutex is never held over network I/O; claim
// entries are removed on release.
func (g *Gateway) doUnboundEstablishment(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, attemptOffset int, extra ...upstreamExtra) (*http.Response, modelRoute, int, error) {
	if g == nil || g.scheduler == nil || ids.Session == "" || route.ID == "" {
		return g.doUpstreamTiersUnbound(ctx, route, bodies, ids, attemptOffset, extra...)
	}
	for {
		if pin, ok := g.scheduler.pinGet(ids.Session, route.ID); ok {
			return g.doPinnedUpstream(ctx, route, bodies, ids, pin, attemptOffset, extra...)
		}
		if isContextCancelled(ctx) {
			return nil, route, attemptOffset, ctx.Err()
		}
		claim, owned, okCap := g.scheduler.pinClaim(ids.Session, route.ID)
		if !okCap {
			return pinLocalResponse(http.StatusBadGateway, 0, "session affinity capacity exhausted"), route, attemptOffset, nil
		}
		if !owned {
			if claim == nil {
				continue
			}
			select {
			case <-ctx.Done():
				return nil, route, attemptOffset, ctx.Err()
			case <-claim.done:
				continue
			}
		}
		if pin, ok := g.scheduler.pinGet(ids.Session, route.ID); ok {
			g.scheduler.pinRelease(ids.Session, route.ID, claim)
			return g.doPinnedUpstream(ctx, route, bodies, ids, pin, attemptOffset, extra...)
		}
		resp, effectiveRoute, attempts, err := func() (*http.Response, modelRoute, int, error) {
			defer g.scheduler.pinRelease(ids.Session, route.ID, claim)
			return g.doUpstreamTiersUnbound(ctx, route, bodies, ids, attemptOffset, extra...)
		}()
		return resp, effectiveRoute, attempts, err
	}
}

func (g *Gateway) doUpstreamTiersUnbound(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, attemptOffset int, extra ...upstreamExtra) (*http.Response, modelRoute, int, error) {
	var lastResponse *http.Response
	var lastErr error
	effectiveRoute := route
	attempts := attemptOffset
	if route.Anonymous {
		resp, err, used, recovered, pinHit := g.doAnonymousUpstream(ctx, route, bodies, ids, attempts, extra...)
		attempts += used
		if pinHit {
			if pin, ok := g.scheduler.pinGet(ids.Session, route.ID); ok {
				if resp != nil {
					drainAndClose(resp.Body)
				}
				return g.doPinnedUpstream(ctx, route, bodies, ids, pin, attempts, extra...)
			}
			// Pins never expire or evict, so a miss here is unexpected
			// defense-in-depth: fall through with the preserved anon outcome.
		}
		if err == nil && resp != nil && resp.StatusCode/100 == 2 {
			return resp, route, attempts, nil
		}
		if recovered {
			// 400 session recovery is the route's last recovery action: its
			// replay result goes directly outward without scanning remaining
			// anonymous proxies or entering the authenticated tiers.
			if resp != nil {
				return resp, route, attempts, nil
			}
			if err == nil {
				err = errors.New("no usable upstream route")
			}
			return nil, route, attempts, err
		}
		if !pinHit && isRouteTerminalBadRequest(resp, err) {
			return resp, route, attempts, nil
		}
		if pinHit {
			// A missing pin must not be mistaken for a terminal 400: the
			// preserved anon outcome still falls back to the key tiers.
			lastResponse, lastErr = resp, err
		} else {
			lastResponse, lastErr = resp, err
		}
		// Client cancel or the shared request deadline ends the route here:
		// never enter the authenticated tiers after a cancelled anonymous
		// phase.
		if isContextCancelled(ctx) {
			if lastResponse != nil {
				return lastResponse, route, attempts, nil
			}
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			return nil, route, attempts, lastErr
		}
		// Defense in depth: an external/concurrent success may have bound the
		// pin while the anonymous walk ran. Stop before sending another
		// target and route through the pinned target instead.
		if ids.Session != "" && route.ID != "" {
			if pin, ok := g.scheduler.pinGet(ids.Session, route.ID); ok {
				if lastResponse != nil {
					drainAndClose(lastResponse.Body)
					lastResponse = nil
				}
				return g.doPinnedUpstream(ctx, route, bodies, ids, pin, attempts, extra...)
			}
		}
		if len(route.KeyTiers) > 0 {
			g.logger.Debug("anonymous request did not succeed; entering preferred key tiers", "component", "upstream", "event", "anonymous_fallback", "request_id", ids.Request, "client_session_hash", clientSessionHash(ids.Session), "attempts", attempts, "key_tiers", route.KeyTiers)
		}
	}

	keyTiers := route.KeyTiers
	if !route.Anonymous && len(keyTiers) == 0 && route.Tier == TierZen {
		keyTiers = []Tier{route.Tier}
	}
	for tierIdx, tier := range keyTiers {
		// A cancel observed between channels ends the route before the next
		// tier sends or drains anything further.
		if isContextCancelled(ctx) {
			break
		}
		// Defense in depth before a later fallback tier sends another target.
		if tierIdx > 0 && ids.Session != "" && route.ID != "" {
			if pin, ok := g.scheduler.pinGet(ids.Session, route.ID); ok {
				if lastResponse != nil {
					drainAndClose(lastResponse.Body)
					lastResponse = nil
				}
				return g.doPinnedUpstream(ctx, route, bodies, ids, pin, attempts, extra...)
			}
		}
		if lastResponse != nil {
			drainAndClose(lastResponse.Body)
			lastResponse = nil
		}
		keyRoute := route
		keyRoute.Tier = tier
		keyRoute.Anonymous = false
		keyRoute.Protocol = route.ProtocolFor(tier)
		effectiveRoute = keyRoute
		resp, err, used, recovered, pinHit := g.doKeyUpstream(ctx, keyRoute, bodies, ids, attempts, extra...)
		attempts += used
		if pinHit {
			if pin, ok := g.scheduler.pinGet(ids.Session, route.ID); ok {
				if resp != nil {
					drainAndClose(resp.Body)
				}
				return g.doPinnedUpstream(ctx, route, bodies, ids, pin, attempts, extra...)
			}
			// Missing pin after detection is unexpected (pins never vanish):
			// fall through with the preserved tier outcome.
		}
		if err == nil && resp != nil && resp.StatusCode/100 == 2 {
			return resp, keyRoute, attempts, nil
		}
		if recovered {
			// Same rule inside authenticated tiers: after the one same-target
			// replay, do not walk remaining frozen candidates and do not fall
			// back to the next tier.
			if resp != nil {
				return resp, keyRoute, attempts, nil
			}
			if err == nil {
				err = errors.New("no usable upstream route")
			}
			return nil, keyRoute, attempts, err
		}
		if !pinHit && isRouteTerminalBadRequest(resp, err) {
			return resp, effectiveRoute, attempts, nil
		}
		lastResponse, lastErr = resp, err
		// Cancel after a tier ends the route without trying the next tier.
		if isContextCancelled(ctx) {
			break
		}
	}
	if lastResponse != nil {
		return lastResponse, effectiveRoute, attempts, nil
	}
	if lastErr == nil {
		lastErr = errors.New("no usable upstream route")
	}
	return nil, effectiveRoute, attempts, lastErr
}

// doPinnedUpstream serves a request bound to one exact target. No cross-proxy
// or cross-tier fallback is attempted. The exact-400 same-target one-replay
// and the single same-target transient retry token are preserved. Active
// proxy429 (pool+proxy global), credential, or target cooldowns before send
// fail locally without an upstream send; removed or unhealthy pinned
// resources fail locally with 502.
func (g *Gateway) doPinnedUpstream(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, pin sessionPin, attemptOffset int, extra ...upstreamExtra) (*http.Response, modelRoute, int, error) {
	effectiveRoute := route
	effectiveRoute.Tier = pin.Tier
	effectiveRoute.Protocol = pin.Protocol
	if pin.Tier == TierZen {
		effectiveRoute.Protocol = route.ProtocolFor(pin.Tier)
		if pin.Protocol != effectiveRoute.Protocol {
			return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
		}
	} else {
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	if pin.Model != route.ID || pin.Model == "" {
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	var baseURL string
	var poolName string
	var wantCreds []credentialRef
	isAnonymous := pin.CredID == anonymousSchedulerCredentialID
	switch {
	case isAnonymous:
		if pin.Tier != TierZen {
			return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
		}
		if !g.cfg.Anonymous {
			return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
		}
		baseURL = g.cfg.Upstream.Zen
		poolName = g.cfg.ProxyRouting.Anonymous
		if pin.Pool != poolName || pin.Authority != normalizeRouteAuthority(baseURL) {
			return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
		}
	case pin.Tier == TierZen:
		baseURL = g.cfg.Upstream.Zen
		poolName = g.authPoolName()
		wantCreds = g.credentials()
		if pin.Pool != poolName || pin.Authority != normalizeRouteAuthority(baseURL) {
			return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
		}
	default:
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	protocol := effectiveRoute.Protocol
	var credKey, credDisplay string
	credIndex := -1
	if isAnonymous {
		credKey = anonymousZenKey
		credDisplay = anonymousCredentialID
	} else {
		found := false
		for _, cred := range wantCreds {
			if cred.id == pin.CredID {
				credKey = cred.key
				credDisplay = cred.display
				credIndex = cred.index
				found = true
				break
			}
		}
		if !found {
			return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
		}
	}
	if g.pools[pin.Pool] == nil {
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	now := time.Now()
	if until, status, ok := g.scheduler.credentialCooldownStatus(pin.CredID); ok {
		if status != http.StatusUnauthorized && status != http.StatusForbidden && status != http.StatusTooManyRequests && !(status >= 500 && status <= 599) {
			status = http.StatusBadGateway
		}
		retrySec := int64(0)
		if status == http.StatusTooManyRequests || (status >= 500 && status <= 599) {
			retrySec = pinRetryAfterSeconds(until, now)
		}
		return pinLocalResponse(status, retrySec, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	if !isAnonymous {
		if until, status, ok := g.scheduler.credential429CooldownStatus(pin.CredID); ok {
			if status != http.StatusTooManyRequests {
				status = http.StatusTooManyRequests
			}
			retrySec := pinRetryAfterSeconds(until, now)
			return pinLocalResponse(status, retrySec, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
		}
	}
	if isContextCancelled(ctx) {
		return nil, effectiveRoute, attemptOffset, ctx.Err()
	}
	body := bodies[pin.Tier]
	if len(body) == 0 {
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	if isAnonymous {
		return g.doPinnedAnonymous(ctx, route, bodies, ids, pin, effectiveRoute, baseURL, protocol, body, attemptOffset, extra...)
	}
	return g.doPinnedAuth(ctx, route, bodies, ids, pin, effectiveRoute, baseURL, poolName, protocol, body, credKey, credDisplay, credIndex, attemptOffset, extra...)
}

// doPinnedAnonymous serves an anonymous bound target: exactly proxy-affine,
// full-target pin includes proxy, route-session includes proxy, no
// cross-proxy recovery. Active tier-qualified (Zen) proxy429, channel, credential, or
// target cooldowns fast-fail locally; removed/unhealthy pinned resources fail
// with 502. Same-target transient retry and exact-400 replay remain.
func (g *Gateway) doPinnedAnonymous(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, pin sessionPin, effectiveRoute modelRoute, baseURL string, protocol Protocol, body []byte, attemptOffset int, extra ...upstreamExtra) (*http.Response, modelRoute, int, error) {
	pool := g.pools[pin.Pool]
	if pool == nil {
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	var proxy *proxyTransport
	for _, item := range pool.items {
		if item != nil && item.name == pin.ProxyRaw {
			proxy = item
			break
		}
	}
	if proxy == nil || !proxy.healthy.Load() {
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	now := time.Now()
	if until, status, ok := g.scheduler.proxy429CooldownStatus(TierZen, pin.Pool, pin.ProxyRaw); ok {
		if status != http.StatusTooManyRequests {
			status = http.StatusTooManyRequests
		}
		local := pinLocalResponse(status, pinRetryAfterSeconds(until, now), "upstream temporarily unavailable")
		if ids.Session != "" {
			if resp, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, 0, extra...); handled {
				drainAndClose(local.Body)
				if takeErr != nil {
					return nil, eff, next, takeErr
				}
				if resp == nil {
					return nil, eff, next, contextError("custom fallback transport failed")
				}
				return resp, eff, next, nil
			}
		}
		return local, effectiveRoute, attemptOffset, nil
	}
	if until, status, ok := g.scheduler.channelCooldownStatus(TierZen, pin.Pool, pin.ProxyRaw); ok {
		if status != http.StatusForbidden && !(status >= 500 && status <= 599) {
			status = http.StatusBadGateway
		}
		var retrySec int64
		if status >= 500 && status <= 599 {
			retrySec = pinRetryAfterSeconds(until, now)
		}
		return pinLocalResponse(status, retrySec, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	identity := targetIdentity(pin.Tier, pin.CredID, pin.Pool, pin.ProxyRaw, pin.Model)
	if until, status, ok := g.scheduler.targetCooldownStatus(identity); ok {
		if status != http.StatusForbidden && !(status >= 500 && status <= 599) {
			status = http.StatusBadGateway
		}
		var retrySec int64
		if status >= 500 && status <= 599 {
			retrySec = pinRetryAfterSeconds(until, now)
		}
		return pinLocalResponse(status, retrySec, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	if isContextCancelled(ctx) {
		return nil, effectiveRoute, attemptOffset, ctx.Err()
	}
	cand := targetCandidate{
		Tier: pin.Tier, CredKey: anonymousZenKey, CredID: pin.CredID, CredDisplay: anonymousCredentialID,
		CredIndex: -1, PoolName: pin.Pool, Proxy: proxy,
		ProxyRaw: pin.ProxyRaw, Model: pin.Model, Identity: identity,
	}
	scope := routeScopeForCandidate(baseURL, cand, protocol)
	routeSession := g.scheduler.routeSessionFor(ids.Session, scope)
	candBody, err := applyRouteSessionToBody(body, routeSession, protocol, false)
	if err != nil {
		return nil, effectiveRoute, attemptOffset, err
	}
	attempts := 0
	transientAvailable := true
	attempts++
	syncAttemptMeta(ctx, pin.Tier, protocol, attemptOffset, attempts)
	resp, sendErr, _, _, firstDiag, _, buildErr := g.sendUpstreamOnce(ctx, route, pin.Tier, baseURL, protocol, candBody, ids, cand, routeSession, "anonymous", "anonymous", true, attemptOffset+attempts)
	if buildErr != nil {
		return nil, effectiveRoute, attemptOffset + attempts, buildErr
	}
	if sendErr == nil && resp != nil && resp.StatusCode/100 == 2 {
		return resp, effectiveRoute, attemptOffset + attempts, nil
	}
	if isContextCancelled(ctx) {
		return resp, effectiveRoute, attemptOffset + attempts, sendErr
	}
	if isRouteTerminalBadRequest(resp, sendErr) {
		replayResp, replayErr, replayed := g.replayCandidate400(ctx, route, pin.Tier, baseURL, protocol, body, ids, cand, scope, routeSession, resp, firstDiag, attemptOffset, attempts)
		return replayResp, effectiveRoute, attemptOffset + replayed, replayErr
	}
	// Pinned anonymous owns one same-target transient retry independent of
	// the ordinary authenticated real-send budget, matching the unbound
	// anonymous contract (frozen list never truncated by max_attempts).
	if isSameTargetTransient(resp, sendErr) && transientAvailable {
		transientAvailable = false
		if isContextCancelled(ctx) {
			return resp, effectiveRoute, attemptOffset + attempts, sendErr
		}
		if resp != nil {
			drainAndClose(resp.Body)
		}
		attempts++
		syncAttemptMeta(ctx, pin.Tier, protocol, attemptOffset, attempts)
		retryResp, retryErr, _, _, retryDiag, _, retryBuildErr := g.sendUpstreamOnce(ctx, route, pin.Tier, baseURL, protocol, candBody, ids, cand, routeSession, "anonymous", "anonymous", true, attemptOffset+attempts)
		if retryBuildErr != nil {
			return nil, effectiveRoute, attemptOffset + attempts, retryBuildErr
		}
		if retryErr == nil && retryResp != nil && retryResp.StatusCode/100 == 2 {
			return retryResp, effectiveRoute, attemptOffset + attempts, nil
		}
		if isContextCancelled(ctx) {
			return retryResp, effectiveRoute, attemptOffset + attempts, retryErr
		}
		if isRouteTerminalBadRequest(retryResp, retryErr) {
			replayResp, replayErr, replayed := g.replayCandidate400(ctx, route, pin.Tier, baseURL, protocol, body, ids, cand, scope, routeSession, retryResp, retryDiag, attemptOffset, attempts)
			return replayResp, effectiveRoute, attemptOffset + replayed, replayErr
		}
		if retryErr == nil && retryResp != nil && retryResp.StatusCode == http.StatusTooManyRequests && ids.Session != "" {
			if resp2, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
				drainAndClose(retryResp.Body)
				if takeErr != nil {
					return nil, eff, next, takeErr
				}
				if resp2 == nil {
					return nil, eff, next, contextError("custom fallback transport failed")
				}
				return resp2, eff, next, nil
			}
		}
		return retryResp, effectiveRoute, attemptOffset + attempts, retryErr
	}
	if sendErr == nil && resp != nil && resp.StatusCode == http.StatusTooManyRequests && ids.Session != "" {
		if resp2, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
			drainAndClose(resp.Body)
			if takeErr != nil {
				return nil, eff, next, takeErr
			}
			if resp2 == nil {
				return nil, eff, next, contextError("custom fallback transport failed")
			}
			return resp2, eff, next, nil
		}
	}
	return resp, effectiveRoute, attemptOffset + attempts, sendErr
}

// doPinnedAuth serves an established authenticated binding proxy-independently.
// The durable identity fixes tier, credential, pool, model, protocol, and
// authority; ProxyRaw is the current/preferred selection with generation
// fencing. The route session is proxy-independent so moves preserve the same
// upstream session value and body bytes. Within one request, only transport
// failure (after same-target transient retry) or HTTP 429 may try the next
// eligible healthy proxy in the same pool/tier/credential/model/protocol/
// authority before client bytes, and every such move obeys the authenticated
// tier real-send budget exactly like unbound auth: each first send and each
// same-target transient retry consumes retry.max_attempts, and the next proxy
// is tried only while budget remains. Two distinct eligible-proxy 429s stop
// the walk even with more proxies, within budget, returning the second 429
// with its target-protocol envelope/Retry-After and setting credential429
// (second Retry-After, capped); a single 429 never does. Pre-existing
// cooling proxies are skipped before any send and never count as observed
// evidence. No moves on 400/401/403/408/425/ordinary 4xx/5xx. Success on an
// alternate updates only current/generation via CAS.
func (g *Gateway) doPinnedAuth(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, pin sessionPin, effectiveRoute modelRoute, baseURL, poolName string, protocol Protocol, body []byte, credKey, credDisplay string, credIndex int, attemptOffset int, extra ...upstreamExtra) (*http.Response, modelRoute, int, error) {
	pool := g.pools[poolName]
	if pool == nil || len(pool.items) == 0 {
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	// Current-proxy target cooling (403/5xx) fast-fails without moves.
	currentIdentity := targetIdentity(pin.Tier, pin.CredID, pin.Pool, pin.ProxyRaw, pin.Model)
	if until, status, ok := g.scheduler.targetCooldownStatus(currentIdentity); ok {
		if status != http.StatusForbidden && !(status >= 500 && status <= 599) {
			status = http.StatusBadGateway
		}
		var retrySec int64
		if status >= 500 && status <= 599 {
			retrySec = pinRetryAfterSeconds(until, time.Now())
		}
		return pinLocalResponse(status, retrySec, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	ordered := affinityProxyOrder(pool, pin.CredID, pin.ProxyRaw)
	// Filter to eligible: healthy, not target-cooling, not tier-429-cooling,
	// not tier-channel-cooling. Current proxy429/channel cooling does not
	// fast-fail immediately; it is skipped in favor of the next eligible
	// (429/channel are move triggers for auth). Target cooling on alternates
	// only skips them.
	type eligibleProxy struct {
		proxy *proxyTransport
		raw   string
	}
	eligible := make([]eligibleProxy, 0, len(ordered))
	nowNanos := time.Now().UnixNano()
	for _, proxy := range ordered {
		if proxy == nil || !proxy.healthy.Load() {
			continue
		}
		identity := targetIdentity(pin.Tier, pin.CredID, pin.Pool, proxy.name, pin.Model)
		if until, _, ok := g.scheduler.targetCooldownStatus(identity); ok && until > nowNanos {
			continue
		}
		if until, _, ok := g.scheduler.proxy429CooldownStatus(pin.Tier, pin.Pool, proxy.name); ok && until > nowNanos {
			continue
		}
		if until, _, ok := g.scheduler.channelCooldownStatus(pin.Tier, pin.Pool, proxy.name); ok && until > nowNanos {
			continue
		}
		eligible = append(eligible, eligibleProxy{proxy: proxy, raw: proxy.name})
	}
	if len(eligible) == 0 {
		// No eligible proxy: distinguish 429 exhaustion from 502. If any
		// proxy is under tier-429 cooldown, fast-fail 429 with max remaining;
		// channel cooling alone fast-fails 502 (its 403/5xx status is kept
		// in the channel detail table, not as a pinned envelope); otherwise
		// 502 (unhealthy/removed).
		var latest int64
		for _, proxy := range ordered {
			if proxy == nil {
				continue
			}
			if until, _, ok := g.scheduler.proxy429CooldownStatus(pin.Tier, pin.Pool, proxy.name); ok && until > latest {
				latest = until
			}
		}
		if latest > nowNanos {
			return pinLocalResponse(http.StatusTooManyRequests, pinRetryAfterSeconds(latest, time.Now()), "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
		}
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	// Proxy-independent route session: identical across moves.
	probe := targetCandidate{Tier: pin.Tier, CredID: pin.CredID, CredKey: credKey, CredDisplay: credDisplay, CredIndex: credIndex, PoolName: pin.Pool, ProxyRaw: "", Model: pin.Model}
	scope := routeScopeForCandidate(baseURL, probe, protocol)
	routeSession := g.scheduler.routeSessionFor(ids.Session, scope)
	candBody, err := applyRouteSessionToBody(body, routeSession, protocol, false)
	if err != nil {
		return nil, effectiveRoute, attemptOffset, err
	}
	budget := g.cfg.Retry.MaxAttempts
	if budget < 1 {
		budget = 1
	}
	attempts := 0
	ordinarySends := 0
	observed429 := make(map[string]time.Duration)
	for idx, ep := range eligible {
		// Authenticated tier real-send budget: every first send and every
		// same-target transient retry consumes retry.max_attempts. Stop
		// before the next proxy when the budget is exhausted, exactly like
		// unbound auth. The 429 two-distinct stop below stays within budget
		// because its second send already consumed the last token.
		if ordinarySends >= budget {
			break
		}
		if isContextCancelled(ctx) {
			return nil, effectiveRoute, attemptOffset + attempts, ctx.Err()
		}
		identity := targetIdentity(pin.Tier, pin.CredID, pin.Pool, ep.raw, pin.Model)
		cand := targetCandidate{
			Tier: pin.Tier, CredKey: credKey, CredID: pin.CredID, CredDisplay: credDisplay,
			CredIndex: credIndex, PoolName: pin.Pool, Proxy: ep.proxy,
			ProxyRaw: ep.raw, Model: pin.Model, Identity: identity,
		}
		attempts++
		ordinarySends++
		syncAttemptMeta(ctx, pin.Tier, protocol, attemptOffset, attempts)
		resp, sendErr, _, _, firstDiag, firstStarted, buildErr := g.sendUpstreamOnce(ctx, route, pin.Tier, baseURL, protocol, candBody, ids, cand, routeSession, "key", credDisplay, false, attemptOffset+attempts)
		if buildErr != nil {
			return nil, effectiveRoute, attemptOffset + attempts, buildErr
		}
		if sendErr == nil && resp != nil && resp.StatusCode/100 == 2 {
			if ep.raw != pin.ProxyRaw {
				_, _ = g.scheduler.pinMoveCurrent(ids.Session, route.ID, pin.Generation, ep.raw)
			}
			return resp, effectiveRoute, attemptOffset + attempts, nil
		}
		if isContextCancelled(ctx) {
			return resp, effectiveRoute, attemptOffset + attempts, sendErr
		}
		if isRouteTerminalBadRequest(resp, sendErr) {
			replayResp, replayErr, replayed := g.replayCandidate400(ctx, route, pin.Tier, baseURL, protocol, body, ids, cand, scope, routeSession, resp, firstDiag, attemptOffset, attempts)
			attempts = replayed
			if replayErr == nil && replayResp != nil && replayResp.StatusCode/100 == 2 && ep.raw != pin.ProxyRaw {
				_, _ = g.scheduler.pinMoveCurrent(ids.Session, route.ID, pin.Generation, ep.raw)
			}
			return replayResp, effectiveRoute, attemptOffset + attempts, replayErr
		}
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		if status == http.StatusTooManyRequests && sendErr == nil {
			var retryAfter time.Duration
			if resp != nil {
				retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
			}
			if _, seen := observed429[ep.raw]; !seen {
				observed429[ep.raw] = retryAfter
			}
			if len(observed429) >= 2 {
				_ = g.scheduler.noteCredential429Failure(pin.CredID, AttemptClassRateLimited, status, retryAfter, firstStarted)
				return resp, effectiveRoute, attemptOffset + attempts, nil
			}
			// Single 429 without second-distinct evidence: terminal when no
			// budget or no proxy remains, preserving the target-protocol
			// envelope/Retry-After; otherwise drain and try the next proxy.
			lastProxy := idx == len(eligible)-1
			if ordinarySends >= budget || lastProxy {
				return resp, effectiveRoute, attemptOffset + attempts, nil
			}
			if resp != nil {
				drainAndClose(resp.Body)
			}
			continue
		}
		if sendErr != nil || status == http.StatusRequestTimeout || status == 425 || (status >= 500 && status <= 599) {
			// Same-target transient policy: one retry per proxy while
			// ordinary sends remain within budget.
			if ordinarySends < budget {
				if isContextCancelled(ctx) {
					return resp, effectiveRoute, attemptOffset + attempts, sendErr
				}
				if resp != nil {
					drainAndClose(resp.Body)
				}
				attempts++
				ordinarySends++
				syncAttemptMeta(ctx, pin.Tier, protocol, attemptOffset, attempts)
				retryResp, retryErr, _, _, retryDiag, retryStarted, retryBuildErr := g.sendUpstreamOnce(ctx, route, pin.Tier, baseURL, protocol, candBody, ids, cand, routeSession, "key", credDisplay, false, attemptOffset+attempts)
				if retryBuildErr != nil {
					return nil, effectiveRoute, attemptOffset + attempts, retryBuildErr
				}
				if retryErr == nil && retryResp != nil && retryResp.StatusCode/100 == 2 {
					if ep.raw != pin.ProxyRaw {
						_, _ = g.scheduler.pinMoveCurrent(ids.Session, route.ID, pin.Generation, ep.raw)
					}
					return retryResp, effectiveRoute, attemptOffset + attempts, nil
				}
				if isContextCancelled(ctx) {
					return retryResp, effectiveRoute, attemptOffset + attempts, retryErr
				}
				if isRouteTerminalBadRequest(retryResp, retryErr) {
					replayResp, replayErr, replayed := g.replayCandidate400(ctx, route, pin.Tier, baseURL, protocol, body, ids, cand, scope, routeSession, retryResp, retryDiag, attemptOffset, attempts)
					attempts = replayed
					if replayErr == nil && replayResp != nil && replayResp.StatusCode/100 == 2 && ep.raw != pin.ProxyRaw {
						_, _ = g.scheduler.pinMoveCurrent(ids.Session, route.ID, pin.Generation, ep.raw)
					}
					return replayResp, effectiveRoute, attemptOffset + attempts, replayErr
				}
				retryStatus := 0
				if retryResp != nil {
					retryStatus = retryResp.StatusCode
				}
				if retryErr == nil && retryStatus == http.StatusTooManyRequests {
					var retryAfter time.Duration
					if retryResp != nil {
						retryAfter = parseRetryAfter(retryResp.Header.Get("Retry-After"))
					}
					if _, seen := observed429[ep.raw]; !seen {
						observed429[ep.raw] = retryAfter
					}
					if len(observed429) >= 2 {
						_ = g.scheduler.noteCredential429Failure(pin.CredID, AttemptClassRateLimited, retryStatus, retryAfter, retryStarted)
						return retryResp, effectiveRoute, attemptOffset + attempts, nil
					}
					lastProxy := idx == len(eligible)-1
					if ordinarySends >= budget || lastProxy {
						return retryResp, effectiveRoute, attemptOffset + attempts, nil
					}
					drainAndClose(retryResp.Body)
					continue
				}
				if retryErr != nil {
					// Transport retry still failing: move to next proxy only
					// while budget remains; the top-of-loop guard enforces it.
					if retryResp != nil {
						drainAndClose(retryResp.Body)
					}
					continue
				}
				// Non-transport retry outcome (401/403/408/425/ordinary
				// 4xx/5xx): no cross-proxy moves.
				return retryResp, effectiveRoute, attemptOffset + attempts, retryErr
			}
			// No retry token: transport failure moves directly only while
			// budget remains; otherwise stop without another send.
			if sendErr != nil {
				if ordinarySends >= budget || idx == len(eligible)-1 {
					if resp != nil {
						drainAndClose(resp.Body)
					}
					break
				}
				if resp != nil {
					drainAndClose(resp.Body)
				}
				continue
			}
			return resp, effectiveRoute, attemptOffset + attempts, sendErr
		}
		// 401/403/408/425/ordinary 4xx/5xx: no cross-proxy moves.
		return resp, effectiveRoute, attemptOffset + attempts, sendErr
	}
	// Exhausted eligible proxies or real-send budget after transport moves, or
	// budget stopped the walk before the next proxy. No 429 envelope is owed
	// here (single-429 terminals returned above); report 502 with the exact
	// consumed attempt count.
	return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset + attempts, nil
}

// sendUpstreamOnce performs one real upstream send on a fixed candidate with
// a fixed route session and candidate body. It builds the request, sends it,
// applies the single state-update entry point, and records the attempt with
// the given monitor number. A request-build error returns buildErr without
// any send, state change, or attempt record. For an exact 400 it reads one
// bounded body for the privacy-safe diagnostic and restores the bytes so the
// existing drain/client-envelope flow is unchanged; the diagnostic is
// recorded on the attempt and returned for replay logging. No extra send.
// startedNanos is the true send-start time for stale-fenced state updates;
// callers must use it (not note-time time.Now()) for credential429
// failure comparisons so in-flight ordering matches proxy429/target guards.
func (g *Gateway) sendUpstreamOnce(ctx context.Context, route modelRoute, tier Tier, baseURL string, protocol Protocol, candBody []byte, ids requestIDs, cand targetCandidate, routeSession, channel, credDisplay string, anonymous bool, monitorAttempt int) (resp *http.Response, err error, duration time.Duration, class attemptClassification, diag badRequestDiag, startedNanos int64, buildErr error) {
	req, err := newUpstreamRequest(ctx, baseURL, protocol, candBody, ids, cand.CredKey, routeSession)
	if err != nil {
		return nil, err, 0, attemptClassification{}, badRequestDiag{}, 0, err
	}
	setRequestCredential(ctx, tier, protocol, credDisplay, channel, anonymous, cand.Proxy)
	started := time.Now()
	startedNanos = started.UnixNano()
	resp, err = cand.Proxy.client.Do(req)
	duration = time.Since(started)
	class = g.applyAttemptOutcome(ctx, cand, resp, err, startedNanos)
	if err == nil && resp != nil && resp.StatusCode == http.StatusBadRequest {
		diag = peek400Diag(resp)
	}
	g.recordUpstreamAttemptWithClass(route, protocol, ids, monitorAttempt, credDisplay, channel, anonymous, cand.Proxy, resp, err, duration, class, false, false, false, diag)
	return resp, err, duration, class, diag, startedNanos, nil
}

func syncAttemptMeta(ctx context.Context, tier Tier, protocol Protocol, attemptOffset, attempts int) {
	if meta, _ := ctx.Value(requestMetaKey{}).(*requestMeta); meta != nil {
		meta.Attempts = attemptOffset + attempts
		meta.Tier = string(tier)
		meta.Protocol = string(protocol)
	}
}

// doAnonymousUpstream walks the frozen anonymous target list once: the fixed
// anonymous credential x every currently available proxy. HRW ordering uses
// only the client session; each candidate sends its own target-bound route
// session in headers and in the already-present body session fields. The
// canonical tier body is never mutated. The whole channel owns one shared
// transient retry token: the first transport error, 408/425, or 5xx re-sends
// once on the same target with the same route session and body. The first
// exact HTTP 400 on any candidate instead replays exactly once on the same
// candidate with a rotated route session (same request ID, attempt +1, same
// target/protocol/proxy, always before any client bytes); the replay result
// is final for the whole route and never consumes the transient token.
// Ordinary 4xx ends the anonymous channel (authenticated tiers may still
// run); other retryable outcomes advance to the next proxy. Cancel ends the
// channel immediately without further sends or state changes. The list is
// never truncated by retry.max_attempts.
func (g *Gateway) doAnonymousUpstream(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, attemptOffset int, extra ...upstreamExtra) (*http.Response, error, int, bool, bool) {
	var lastResponse *http.Response
	var lastErr error
	if !g.cfg.Anonymous {
		return nil, errors.New("anonymous channel is disabled"), 0, false, false
	}
	pool := g.pools[g.cfg.ProxyRouting.Anonymous]
	body := bodies[TierZen]
	if len(body) == 0 {
		return nil, errors.New("no prepared Zen request body"), 0, false, false
	}
	now := time.Now().UnixNano()
	cands := g.scheduler.orderCandidates(g.scheduler.buildAnonymousCandidates(pool, route.ID, now), ids.Session)
	attempts := 0
	transientAvailable := true
	for idx, cand := range cands {
		if isContextCancelled(ctx) {
			if lastResponse != nil {
				return lastResponse, nil, attempts, false, false
			}
			if lastErr != nil {
				return nil, lastErr, attempts, false, false
			}
			return nil, ctx.Err(), attempts, false, false
		}
		// Defense in depth: before a later fallback candidate sends another
		// target, stop when a pin appeared and let the outer route through
		// the pinned target. Same-target transient retry/replay below stays
		// on the same candidate and never checks here.
		if idx > 0 && ids.Session != "" && route.ID != "" {
			if _, ok := g.scheduler.pinGet(ids.Session, route.ID); ok {
				return lastResponse, lastErr, attempts, false, true
			}
		}
		if lastResponse != nil {
			drainAndClose(lastResponse.Body)
			lastResponse = nil
		}
		scope := routeScopeForCandidate(g.cfg.Upstream.Zen, cand, route.Protocol)
		routeSession := g.scheduler.routeSessionFor(ids.Session, scope)
		candBody, err := applyRouteSessionToBody(body, routeSession, route.Protocol, false)
		if err != nil {
			attempts++
			syncAttemptMeta(ctx, TierZen, route.Protocol, attemptOffset, attempts)
			return nil, err, attempts, false, false
		}
		attempts++
		syncAttemptMeta(ctx, TierZen, route.Protocol, attemptOffset, attempts)
		resp, err, _, _, firstDiag, _, buildErr := g.sendUpstreamOnce(ctx, route, TierZen, g.cfg.Upstream.Zen, route.Protocol, candBody, ids, cand, routeSession, "anonymous", "anonymous", true, attemptOffset+attempts)
		if buildErr != nil {
			return nil, buildErr, attempts, false, false
		}
		if err == nil && resp != nil && resp.StatusCode/100 == 2 {
			g.logger.Debug("anonymous upstream accepted request", "component", "upstream", "event", "anonymous_attempt_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "status", resp.StatusCode)
			g.bindSessionPin(ids.Session, route.ID, TierZen, anonymousSchedulerCredentialID, cand.PoolName, cand.ProxyRaw, route.Protocol, normalizeRouteAuthority(g.cfg.Upstream.Zen))
			return resp, nil, attempts, false, false
		}
		if isContextCancelled(ctx) {
			return resp, err, attempts, false, false
		}
		if isRouteTerminalBadRequest(resp, err) {
			replayResp, replayErr, replayed := g.replayCandidate400(ctx, route, TierZen, g.cfg.Upstream.Zen, route.Protocol, body, ids, cand, scope, routeSession, resp, firstDiag, attemptOffset, attempts)
			attempts = replayed
			if replayResp != nil || replayErr != nil {
				return replayResp, replayErr, attempts, true, false
			}
			// Replay suppressed (cancelled context): preserve terminal 400.
			termArgs := []any{"component", "upstream", "event", "anonymous_attempt_terminal", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "status", resp.StatusCode}
			termArgs = append(termArgs, diagLogArgs(firstDiag, "")...)
			g.logger.Debug("anonymous upstream returned route-terminal 400; stopping anonymous phase", termArgs...)
			return resp, nil, attempts, false, false
		}
		if isSameTargetTransient(resp, err) && transientAvailable {
			transientAvailable = false
			if isContextCancelled(ctx) {
				return resp, err, attempts, false, false
			}
			if resp != nil {
				drainAndClose(resp.Body)
			}
			attempts++
			syncAttemptMeta(ctx, TierZen, route.Protocol, attemptOffset, attempts)
			retryResp, retryErr, _, _, retryDiag, _, retryBuildErr := g.sendUpstreamOnce(ctx, route, TierZen, g.cfg.Upstream.Zen, route.Protocol, candBody, ids, cand, routeSession, "anonymous", "anonymous", true, attemptOffset+attempts)
			if retryBuildErr != nil {
				return nil, retryBuildErr, attempts, false, false
			}
			if retryErr == nil && retryResp != nil && retryResp.StatusCode/100 == 2 {
				g.logger.Debug("anonymous transient retry succeeded", "component", "upstream", "event", "anonymous_transient_retry_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "status", retryResp.StatusCode)
				g.bindSessionPin(ids.Session, route.ID, TierZen, anonymousSchedulerCredentialID, cand.PoolName, cand.ProxyRaw, route.Protocol, normalizeRouteAuthority(g.cfg.Upstream.Zen))
				return retryResp, nil, attempts, false, false
			}
			if isContextCancelled(ctx) {
				return retryResp, retryErr, attempts, false, false
			}
			if isRouteTerminalBadRequest(retryResp, retryErr) {
				replayResp, replayErr, replayed := g.replayCandidate400(ctx, route, TierZen, g.cfg.Upstream.Zen, route.Protocol, body, ids, cand, scope, routeSession, retryResp, retryDiag, attemptOffset, attempts)
				attempts = replayed
				if replayResp != nil || replayErr != nil {
					return replayResp, replayErr, attempts, true, false
				}
				return retryResp, nil, attempts, false, false
			}
			if isOrdinaryClientRejection(retryResp, retryErr) {
				g.logger.Debug("anonymous transient retry hit ordinary rejection; ending anonymous channel", "component", "upstream", "event", "anonymous_attempt_rejected", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "status", retryResp.StatusCode)
				return retryResp, nil, attempts, false, false
			}
			lastResponse = retryResp
			lastErr = retryErr
			if retryErr != nil {
				g.logger.Debug("anonymous transient retry still failing; trying the next proxy", "component", "upstream", "event", "anonymous_attempt_transport_failed", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "error", retryErr)
			} else {
				g.logger.Debug("anonymous transient retry returned an error response; trying the next proxy", "component", "upstream", "event", "anonymous_attempt_response_failed", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "status", retryResp.StatusCode)
			}
			continue
		}
		if isOrdinaryClientRejection(resp, err) {
			g.logger.Debug("anonymous upstream rejected a non-retryable request; ending anonymous channel", "component", "upstream", "event", "anonymous_attempt_rejected", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "status", resp.StatusCode)
			return resp, nil, attempts, false, false
		}
		lastResponse = resp
		lastErr = err
		if err != nil {
			g.logger.Debug("anonymous transport attempt failed", "component", "upstream", "event", "anonymous_attempt_transport_failed", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "error", err)
		} else {
			g.logger.Debug("anonymous upstream returned an error response; trying the next proxy", "component", "upstream", "event", "anonymous_attempt_response_failed", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "status", resp.StatusCode)
		}
	}
	if lastResponse != nil {
		return lastResponse, nil, attempts, false, false
	}
	if lastErr == nil {
		lastErr = errors.New("no healthy anonymous proxies available")
	}
	return nil, lastErr, attempts, false, false
}

// replayCandidate400 performs the single same-target 400 recovery replay: it
// rotates the route session, rebuilds a fresh candidate body from the frozen
// canonical body (overwriting present session fields and, for Responses,
// dropping stale previous_response_id/reasoning refs), and re-sends on the
// identical credential/proxy/protocol with the same request ID and attempt
// number +1. The first 400 is neutral and its body is drained before the
// replay. The first 400 body was already read once bounded for diagnostics by
// sendUpstreamOnce and restored, so draining here preserves control flow
// without a second network read. The replay 400 body (when 400) is likewise
// read once bounded and restored so the final client envelope keeps the
// substantively same upstream message with no extra send. Recovery never runs
// after client bytes have been written: all callers invoke it before
// returning the upstream response downstream.
// It returns the replay response/error and the updated attempt count; a
// nil/nil pair means replay was suppressed (cancelled context) and the caller
// must preserve the original terminal 400. A body-rewrite error aborts the
// route with that error. Only sanitized hint/type/code/fingerprint are logged.
func (g *Gateway) replayCandidate400(ctx context.Context, route modelRoute, tier Tier, baseURL string, protocol Protocol, canonical []byte, ids requestIDs, cand targetCandidate, scope routeSessionScope, observed string, firstResp *http.Response, firstDiag badRequestDiag, attemptOffset, attempts int) (*http.Response, error, int) {
	if ctx != nil && ctx.Err() != nil {
		return nil, nil, attempts
	}
	channel := "key"
	anonymous := false
	credDisplay := cand.CredDisplay
	if cand.CredID == anonymousSchedulerCredentialID || cand.CredDisplay == anonymousCredentialID {
		channel = "anonymous"
		anonymous = true
		credDisplay = "anonymous"
	}
	newSession := g.scheduler.rotateRouteSession(ids.Session, scope, observed)
	replayBody, cleanup, err := applyRouteSessionToBodyWithReport(canonical, newSession, protocol, true)
	if err != nil {
		if firstResp != nil {
			drainAndClose(firstResp.Body)
		}
		return nil, err, attempts
	}
	if firstResp != nil {
		drainAndClose(firstResp.Body)
	}
	attempts++
	if meta, _ := ctx.Value(requestMetaKey{}).(*requestMeta); meta != nil {
		meta.Attempts = attemptOffset + attempts
		meta.Tier = string(tier)
		meta.Protocol = string(protocol)
	}
	req, err := newUpstreamRequest(ctx, baseURL, protocol, replayBody, ids, cand.CredKey, newSession)
	if err != nil {
		return nil, err, attempts
	}
	setRequestCredential(ctx, tier, protocol, credDisplay, channel, anonymous, cand.Proxy)
	started := time.Now()
	resp, err := cand.Proxy.client.Do(req)
	duration := time.Since(started)
	class := g.applyAttemptOutcome(ctx, cand, resp, err, started.UnixNano())
	var replayDiag badRequestDiag
	if err == nil && resp != nil && resp.StatusCode == http.StatusBadRequest {
		replayDiag = peek400Diag(resp)
	}
	sessionHash := clientSessionHash(ids.Session)
	g.recordUpstreamAttemptWithClass(route, protocol, ids, attemptOffset+attempts, credDisplay, channel, anonymous, cand.Proxy, resp, err, duration, class, true, cleanup.DroppedPreviousResponseID, cleanup.DroppedReasoningRefs, replayDiag)
	if g.logger != nil {
		firstArgs := diagLogArgs(firstDiag, "")
		replayArgs := diagLogArgs(replayDiag, "replay_")
		base := []any{"component", "upstream", "request_id", ids.Request, "tier", tier, "protocol", protocol, "client_session_hash", sessionHash, "key_id", credDisplay, "channel", channel, "proxy", redactURL(cand.Proxy.name), "duration_ms", duration.Milliseconds(), "route_session_replay", true, "dropped_previous_response_id", cleanup.DroppedPreviousResponseID, "dropped_reasoning_refs", cleanup.DroppedReasoningRefs}
		base = append(base, firstArgs...)
		if err == nil && resp != nil && resp.StatusCode/100 == 2 {
			g.logger.Info("upstream 400 session recovery succeeded", append([]any{"event", "route_session_recovery_succeeded", "status", resp.StatusCode}, base...)...)
		} else if err == nil && resp != nil {
			args := append([]any{"event", "route_session_recovery_replayed", "status", resp.StatusCode}, base...)
			args = append(args, replayArgs...)
			g.logger.Info("upstream 400 session recovery replayed", args...)
		} else {
			// Transport error carries no upstream message; log it without raw bodies.
			args := append([]any{"event", "route_session_recovery_replayed"}, base...)
			args = append(args, replayArgs...)
			if err != nil {
				args = append(args, "error", err)
			}
			g.logger.Info("upstream 400 session recovery replayed", args...)
		}
	}
	// The replay result is final for the route: success returns normally, a
	// second exact 400 terminates the route with that 400, and any other
	// non-2xx/transport outcome is classified normally (cooldowns apply) but
	// never walks remaining frozen candidates or the next tier. A replay
	// success remains bound to that same target.
	if err == nil && resp != nil && resp.StatusCode/100 == 2 {
		g.bindSessionPin(ids.Session, route.ID, tier, cand.CredID, cand.PoolName, cand.ProxyRaw, protocol, normalizeRouteAuthority(baseURL))
	}
	return resp, err, attempts
}

func (g *Gateway) doKeyUpstream(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, attemptOffset int, extra ...upstreamExtra) (*http.Response, error, int, bool, bool) {
	var lastResponse *http.Response
	var lastErr error
	creds := g.credentials()
	poolName := g.authPoolName()
	baseURL := g.cfg.Upstream.Zen
	pool := g.pools[poolName]
	if len(creds) == 0 {
		return nil, fmt.Errorf("no %s nodes configured", route.Tier), 0, false, false
	}
	body := bodies[route.Tier]
	if len(body) == 0 {
		return nil, fmt.Errorf("no prepared %s request body", route.Tier), 0, false, false
	}
	// Freeze the candidate order at request start; failover walks the frozen
	// list without dynamic re-sorting. The tier budget counts real ordinary
	// upstream sends (each candidate first send plus at most one same-target
	// transient retry) against retry.max_attempts. The 400 route-session
	// recovery is always allowed once extra and never consumes the transient
	// token or the ordinary budget. Auth ordering is credential-soft-affinity:
	// credential groups by session HRW, proxies within each credential by
	// deterministic affinity (session/model independent). The route session
	// for auth is proxy-independent.
	now := time.Now().UnixNano()
	// Credential429 fast-fail before any send: when every credential for this
	// tier is cooling and at least one is credential-429 cooling, fail locally
	// with 429 + max Retry-After and zero sends.
	if len(creds) > 0 {
		allCooling := true
		var latest429 int64
		for _, cred := range creds {
			if until, _, ok := g.scheduler.credential429CooldownStatus(cred.id); ok && until > now {
				if until > latest429 {
					latest429 = until
				}
				continue
			}
			if until, _, ok := g.scheduler.credentialCooldownStatus(cred.id); ok && until > now {
				continue
			}
			allCooling = false
			break
		}
		if allCooling && latest429 > now {
			return pinLocalResponse(http.StatusTooManyRequests, pinRetryAfterSeconds(latest429, time.Now()), "upstream temporarily unavailable"), nil, 0, false, false
		}
	}
	cands := g.scheduler.orderCandidates(g.scheduler.buildAuthCandidates(route.Tier, creds, pool, route.ID, now), ids.Session)
	// Two-distinct-proxy 429 evidence per credential for this unbound request.
	// Only same-credential 429s on two distinct proxies set credential429.
	cred429Evidence := make(map[string]map[string]time.Duration)
	budget := g.cfg.Retry.MaxAttempts
	if budget < 1 {
		budget = 1
	}
	attempts := 0
	ordinarySends := 0
	transientAvailable := true
	for idx, cand := range cands {
		if isContextCancelled(ctx) {
			if lastResponse != nil {
				return lastResponse, nil, attempts, false, false
			}
			if lastErr != nil {
				return nil, lastErr, attempts, false, false
			}
			return nil, ctx.Err(), attempts, false, false
		}
		if ordinarySends >= budget {
			break
		}
		// Defense in depth: before a later fallback candidate sends another
		// target, stop when a pin appeared and let the outer route through
		// the pinned target. Same-target transient retry/replay below stays
		// on the same candidate and never checks here.
		if idx > 0 && ids.Session != "" && route.ID != "" {
			if _, ok := g.scheduler.pinGet(ids.Session, route.ID); ok {
				return lastResponse, lastErr, attempts, false, true
			}
		}
		if lastResponse != nil {
			drainAndClose(lastResponse.Body)
			lastResponse = nil
		}
		// Mid-request credential429 evidence: skip remaining candidates with
		// a credential already proven rate-limited in this request (two
		// distinct proxies 429). Other credentials still walk normally.
		if ev, ok := cred429Evidence[cand.CredID]; ok && len(ev) >= 2 {
			continue
		}
		scope := routeScopeForCandidate(baseURL, cand, route.Protocol)
		routeSession := g.scheduler.routeSessionFor(ids.Session, scope)
		candBody, err := applyRouteSessionToBody(body, routeSession, route.Protocol, false)
		if err != nil {
			attempts++
			syncAttemptMeta(ctx, route.Tier, route.Protocol, attemptOffset, attempts)
			return nil, err, attempts, false, false
		}
		// Keep the request-level trace synchronized with the attempt that is
		// about to be sent. Only the redacted key suffix is retained.
		attempts++
		syncAttemptMeta(ctx, route.Tier, route.Protocol, attemptOffset, attempts)
		resp, err, duration, _, firstDiag, firstStarted, buildErr := g.sendUpstreamOnce(ctx, route, route.Tier, baseURL, route.Protocol, candBody, ids, cand, routeSession, "key", cand.CredDisplay, false, attemptOffset+attempts)
		if buildErr != nil {
			return nil, buildErr, attempts, false, false
		}
		ordinarySends++
		_ = duration
		if err == nil && resp != nil && resp.StatusCode/100 == 2 {
			g.logger.Debug("upstream accepted request", "component", "upstream", "event", "attempt_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "proxy", redactURL(cand.Proxy.name), "status", resp.StatusCode)
			g.bindSessionPin(ids.Session, route.ID, route.Tier, cand.CredID, cand.PoolName, cand.ProxyRaw, route.Protocol, normalizeRouteAuthority(baseURL))
			return resp, nil, attempts, false, false
		}
		// Unbound two-distinct-proxy 429 evidence: same credential on two
		// distinct proxies both 429 sets credential429 (second Retry-After).
		// Single-proxy 429 never sets it.
		if err == nil && resp != nil && resp.StatusCode == http.StatusTooManyRequests {
			retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
			ev, ok := cred429Evidence[cand.CredID]
			if !ok {
				ev = make(map[string]time.Duration)
				cred429Evidence[cand.CredID] = ev
			}
			if _, seen := ev[cand.ProxyRaw]; !seen {
				ev[cand.ProxyRaw] = retryAfter
				if len(ev) >= 2 {
					_ = g.scheduler.noteCredential429Failure(cand.CredID, AttemptClassRateLimited, resp.StatusCode, retryAfter, firstStarted)
				}
			}
		}
		if isContextCancelled(ctx) {
			return resp, err, attempts, false, false
		}
		if isRouteTerminalBadRequest(resp, err) {
			replayResp, replayErr, replayed := g.replayCandidate400(ctx, route, route.Tier, baseURL, route.Protocol, body, ids, cand, scope, routeSession, resp, firstDiag, attemptOffset, attempts)
			attempts = replayed
			if replayResp != nil || replayErr != nil {
				return replayResp, replayErr, attempts, true, false
			}
			termArgs := []any{"component", "upstream", "event", "attempt_terminal", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "status", resp.StatusCode, "proxy", redactURL(cand.Proxy.name)}
			termArgs = append(termArgs, diagLogArgs(firstDiag, "")...)
			g.logger.Debug("upstream returned route-terminal 400; stopping route", termArgs...)
			return resp, nil, attempts, false, false
		}
		if isSameTargetTransient(resp, err) && transientAvailable && ordinarySends < budget {
			transientAvailable = false
			if isContextCancelled(ctx) {
				return resp, err, attempts, false, false
			}
			if resp != nil {
				drainAndClose(resp.Body)
			}
			attempts++
			syncAttemptMeta(ctx, route.Tier, route.Protocol, attemptOffset, attempts)
			retryResp, retryErr, _, _, retryDiag, _, retryBuildErr := g.sendUpstreamOnce(ctx, route, route.Tier, baseURL, route.Protocol, candBody, ids, cand, routeSession, "key", cand.CredDisplay, false, attemptOffset+attempts)
			if retryBuildErr != nil {
				return nil, retryBuildErr, attempts, false, false
			}
			ordinarySends++
			if retryErr == nil && retryResp != nil && retryResp.StatusCode/100 == 2 {
				g.logger.Debug("upstream transient retry succeeded", "component", "upstream", "event", "transient_retry_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "proxy", redactURL(cand.Proxy.name), "status", retryResp.StatusCode)
				g.bindSessionPin(ids.Session, route.ID, route.Tier, cand.CredID, cand.PoolName, cand.ProxyRaw, route.Protocol, normalizeRouteAuthority(baseURL))
				return retryResp, nil, attempts, false, false
			}
			if isContextCancelled(ctx) {
				return retryResp, retryErr, attempts, false, false
			}
			if isRouteTerminalBadRequest(retryResp, retryErr) {
				replayResp, replayErr, replayed := g.replayCandidate400(ctx, route, route.Tier, baseURL, route.Protocol, body, ids, cand, scope, routeSession, retryResp, retryDiag, attemptOffset, attempts)
				attempts = replayed
				if replayResp != nil || replayErr != nil {
					return replayResp, replayErr, attempts, true, false
				}
				return retryResp, nil, attempts, false, false
			}
			// Request-shape rejections end this tier without rotating through
			// unrelated keys; 408/425 stay transient-neutral and fall through
			// to the next frozen candidate.
			if isOrdinaryClientRejection(retryResp, retryErr) {
				g.logger.Debug("upstream rejected a non-retryable request", "component", "upstream", "event", "attempt_rejected", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "status", retryResp.StatusCode, "proxy", redactURL(cand.Proxy.name))
				return retryResp, nil, attempts, false, false
			}
			lastResponse = retryResp
			lastErr = retryErr
			if retryErr != nil {
				g.logger.Debug("upstream transient retry still failing", "component", "upstream", "event", "attempt_transport_failed", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "proxy", redactURL(cand.Proxy.name), "error", retryErr)
			} else {
				g.logger.Debug("upstream transient retry returned a retryable response", "component", "upstream", "event", "attempt_retryable_response", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "status", retryResp.StatusCode, "proxy", redactURL(cand.Proxy.name))
			}
			continue
		}
		// Request-shape errors are deterministic and must leave this tier
		// without rotating through unrelated keys. 408/425 are transient and
		// never end the tier here. Authentication, throttling, server, and
		// transport failures remain retryable inside this tier via fallback.
		if isOrdinaryClientRejection(resp, err) {
			g.logger.Debug("upstream rejected a non-retryable request", "component", "upstream", "event", "attempt_rejected", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "status", resp.StatusCode, "proxy", redactURL(cand.Proxy.name))
			return resp, nil, attempts, false, false
		}
		lastResponse = resp
		lastErr = err
		if err != nil {
			g.logger.Debug("upstream transport attempt failed", "component", "upstream", "event", "attempt_transport_failed", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "proxy", redactURL(cand.Proxy.name), "error", err)
		} else {
			g.logger.Debug("upstream returned a retryable response", "component", "upstream", "event", "attempt_retryable_response", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "status", resp.StatusCode, "proxy", redactURL(cand.Proxy.name))
		}
	}
	if lastResponse != nil {
		return lastResponse, nil, attempts, false, false
	}
	return nil, lastErr, attempts, false, false
}

// applyAttemptOutcome is the single state-update entry point for every
// upstream attempt. It classifies once, updates the six state layers, and
// returns the classification so recordUpstreamAttempt reuses the same result.
//
//   - Downstream cancellation (request ctx done): no state is touched.
//   - 2xx: clears this target and the credential 401 state, plus the
//     tier-qualified proxy429/channel state and the credential429 state when
//     the send started at or after the latest recorded failure (stale
//     in-flight 2xx never clears a newer cooldown); a healthy response also
//     restores proxy transport health.
//   - 401: global credential cooldown; the target is not cooled twice.
//   - 429: tier-qualified proxy cooldown (Retry-After wins when larger,
//     capped at the configured 429 max); the per-target, credential-401,
//     credential429, and channel layers are untouched here. Credential429 is
//     set only by the two-distinct-proxy evidence rule in pinned/unbound
//     flows, never from a single 429. No same-target retry is implied; it
//     still triggers only the neutral async proxy verification and never
//     flips healthy directly.
//   - 403/5xx: per-target cooldown (Retry-After wins when larger, capped at
//     the generic 5 minutes).
//     The channel layer is never written here; only comparative management
//     probes write it.
//   - Transport error (non-cancelled): neutral, no
//     credential/target/proxy429/channel state; it only triggers the existing
//     async neutral proxy health verification. Only that independent probe may
//     flip proxy healthy on isProxyFailure.
//   - Ordinary 4xx (client_rejected, including exact 400) and transient
//     408/425 (transient_client): neutral no-op; never clears state.
//
// startedNanos is the send-start time of this attempt. It guards the
// proxy429/channel and credential429 clear paths; a zero value falls back to
// now.
func (g *Gateway) applyAttemptOutcome(ctx context.Context, cand targetCandidate, resp *http.Response, err error, startedNanos int64) attemptClassification {
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
		targetCleared := g.scheduler.noteTargetSuccess(cand.Identity)
		credCleared := g.scheduler.noteCredentialSuccess(cand.CredID)
		proxyCleared := g.scheduler.noteProxy429Success(cand.Tier, cand.PoolName, cand.ProxyRaw, startedNanos)
		channelCleared := g.scheduler.noteChannelSuccess(cand.Tier, cand.PoolName, cand.ProxyRaw, startedNanos)
		cred429Cleared := g.scheduler.noteCredential429Success(cand.CredID, startedNanos)
		g.logSchedulerCleared(cand, targetCleared, credCleared, proxyCleared, channelCleared, cred429Cleared)
		if cand.Proxy != nil && !cand.Proxy.healthy.Load() {
			wasHealthy := cand.Proxy.healthy.Swap(true)
			if !wasHealthy && g.logger != nil {
				g.logger.Info("proxy connectivity restored", "component", "proxy", "event", "proxy_restored", "proxy", redactURL(cand.Proxy.name), "proxy_pool", cand.Proxy.pool)
			}
		}
	case err != nil:
		g.verifyProxyAfterError(ctx, cand.Proxy, 0)
		return class
	case status == http.StatusUnauthorized:
		change := g.scheduler.noteCredentialAuthFailure(cand.CredID)
		g.logCredentialCooldownSet(cand, change, status)
	case status == http.StatusTooManyRequests:
		var retryAfter time.Duration
		if resp != nil {
			retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
		}
		change := g.scheduler.noteProxy429Failure(cand.Tier, cand.PoolName, cand.ProxyRaw, class.Class, status, retryAfter, startedNanos)
		g.logProxy429CooldownSet(cand, change)
		g.verifyProxyAfterError(ctx, cand.Proxy, status)
	case status == http.StatusForbidden || status >= 500:
		var retryAfter time.Duration
		if resp != nil {
			retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
		}
		change := g.scheduler.noteTargetFailure(cand.Identity, class.Class, status, retryAfter)
		g.logTargetCooldownSet(cand, change)
		g.verifyProxyAfterError(ctx, cand.Proxy, status)
	default:
		// Ordinary 4xx and other responses: neutral, no state change.
	}
	return class
}

// credentialChannel names the observability channel for one candidate: the
// shared public credential uses the anonymous literal, auth keys use "key".
func credentialChannel(cand targetCandidate) string {
	if cand.CredID == anonymousSchedulerCredentialID || cand.CredDisplay == anonymousCredentialID {
		return anonymousCredentialID
	}
	return "key"
}

// logCredentialCooldownSet emits credential_cooldown_set only when the 401
// actually extended the credential cooldown. The full key and its fingerprint
// never leave the Gateway; only the suffix display is logged.
func (g *Gateway) logCredentialCooldownSet(cand targetCandidate, change credentialChange, status int) {
	if g.logger == nil || !change.Changed {
		return
	}
	remaining := change.CooldownUntil - time.Now().UnixNano()
	if remaining < 0 {
		remaining = 0
	}
	g.logger.Warn("credential cooldown extended",
		"component", "scheduler", "event", "credential_cooldown_set",
		"tier", string(cand.Tier), "key_id", cand.CredDisplay,
		"failures", change.Failures,
		"cooldown_until", time.Unix(0, change.CooldownUntil).UTC(),
		"remaining_ms", time.Duration(remaining).Milliseconds(),
		"status", status)
}

// logTargetCooldownSet emits target_cooldown_set only when the failure
// actually extended the target cooldown. Ordinary 4xx never reaches here.
func (g *Gateway) logTargetCooldownSet(cand targetCandidate, change targetChange) {
	if g.logger == nil || !change.Changed {
		return
	}
	remaining := change.CooldownUntil - time.Now().UnixNano()
	if remaining < 0 {
		remaining = 0
	}
	proxyNode := ""
	poolName := cand.PoolName
	if cand.Proxy != nil {
		proxyNode = redactURL(cand.Proxy.name)
		poolName = cand.Proxy.pool
		if poolName == "" {
			poolName = cand.PoolName
		}
	} else if cand.ProxyRaw != "" {
		proxyNode = redactURL(cand.ProxyRaw)
	}
	g.logger.Debug("target cooldown extended",
		"component", "scheduler", "event", "target_cooldown_set",
		"tier", string(cand.Tier), "key_id", cand.CredDisplay,
		"channel", credentialChannel(cand),
		"proxy_pool", poolName, "proxy_node", proxyNode,
		"model", cand.Model, "failure_class", change.FailureClass,
		"status", change.Status, "failures", change.Failures,
		"cooldown_until", time.Unix(0, change.CooldownUntil).UTC(),
		"remaining_ms", time.Duration(remaining).Milliseconds())
}

// logProxy429CooldownSet emits proxy_rate_limit_cooldown_set only when the
// 429 actually extended the pool-qualified proxy cooldown. The raw proxy URL
// never leaves the Gateway; only the redacted node label is logged.
func (g *Gateway) logProxy429CooldownSet(cand targetCandidate, change proxy429Change) {
	if g.logger == nil || !change.Changed {
		return
	}
	remaining := change.CooldownUntil - time.Now().UnixNano()
	if remaining < 0 {
		remaining = 0
	}
	proxyNode := ""
	poolName := cand.PoolName
	if cand.Proxy != nil {
		proxyNode = redactURL(cand.Proxy.name)
		poolName = cand.Proxy.pool
		if poolName == "" {
			poolName = cand.PoolName
		}
	} else if cand.ProxyRaw != "" {
		proxyNode = redactURL(cand.ProxyRaw)
	}
	g.logger.Debug("proxy rate-limit cooldown extended",
		"component", "scheduler", "event", "proxy_rate_limit_cooldown_set",
		"tier", string(cand.Tier), "key_id", cand.CredDisplay,
		"channel", credentialChannel(cand),
		"proxy_pool", poolName, "proxy_node", proxyNode,
		"failure_class", change.FailureClass,
		"status", change.Status, "failures", change.Failures,
		"cooldown_until", time.Unix(0, change.CooldownUntil).UTC(),
		"remaining_ms", time.Duration(remaining).Milliseconds())
}

// logChannelCooldownSet emits channel_cooldown_set only when the comparative
// probe failure actually extended the tier-qualified channel cooldown.
func (g *Gateway) logChannelCooldownSet(tier Tier, pool, proxyRaw, keyDisplay, failureClass string, status int, change channelChange) {
	if g.logger == nil || !change.Changed {
		return
	}
	remaining := change.CooldownUntil - time.Now().UnixNano()
	if remaining < 0 {
		remaining = 0
	}
	g.logger.Debug("channel availability cooldown extended",
		"component", "scheduler", "event", "channel_cooldown_set",
		"tier", string(tier), "key_id", keyDisplay,
		"proxy_pool", pool, "proxy_node", redactURL(proxyRaw),
		"failure_class", failureClass,
		"status", status, "failures", change.Failures,
		"cooldown_until", time.Unix(0, change.CooldownUntil).UTC(),
		"remaining_ms", time.Duration(remaining).Milliseconds())
}

// logSchedulerCleared emits clear events only when 2xx actually removed
// stored credential/target/proxy429/channel/credential429 state.
func (g *Gateway) logSchedulerCleared(cand targetCandidate, target targetChange, cred credentialChange, proxy proxy429Change, channel channelChange, cred429 credentialChange) {
	if g.logger == nil {
		return
	}
	proxyNode := ""
	poolName := cand.PoolName
	if cand.Proxy != nil {
		proxyNode = redactURL(cand.Proxy.name)
		poolName = cand.Proxy.pool
		if poolName == "" {
			poolName = cand.PoolName
		}
	} else if cand.ProxyRaw != "" {
		proxyNode = redactURL(cand.ProxyRaw)
	}
	if target.Changed {
		g.logger.Debug("target cooldown cleared",
			"component", "scheduler", "event", "target_cooldown_cleared",
			"tier", string(cand.Tier), "key_id", cand.CredDisplay,
			"channel", credentialChannel(cand),
			"proxy_pool", poolName, "proxy_node", proxyNode,
			"model", cand.Model, "failures", target.Failures)
	}
	if cred.Changed {
		g.logger.Debug("credential cooldown cleared",
			"component", "scheduler", "event", "credential_cooldown_cleared",
			"tier", string(cand.Tier), "key_id", cand.CredDisplay,
			"failures", cred.Failures, "remaining_ms", 0)
	}
	if proxy.Changed && proxy.Cleared {
		g.logger.Debug("proxy rate-limit cooldown cleared",
			"component", "scheduler", "event", "proxy_rate_limit_cooldown_cleared",
			"tier", string(cand.Tier), "key_id", cand.CredDisplay,
			"channel", credentialChannel(cand),
			"proxy_pool", poolName, "proxy_node", proxyNode,
			"failures", proxy.Failures)
	}
	if cred429.Changed && cred429.Cleared {
		g.logger.Debug("credential rate-limit cooldown cleared",
			"component", "scheduler", "event", "credential_rate_limit_cooldown_cleared",
			"tier", string(cand.Tier), "key_id", cand.CredDisplay,
			"failures", cred429.Failures, "remaining_ms", 0)
	}
	if channel.Changed && channel.Cleared {
		g.logger.Debug("channel availability cooldown cleared",
			"component", "scheduler", "event", "channel_cooldown_cleared",
			"tier", string(cand.Tier), "key_id", cand.CredDisplay,
			"channel", credentialChannel(cand),
			"proxy_pool", poolName, "proxy_node", proxyNode,
			"failures", channel.Failures)
	}
}

func setRequestCredential(ctx context.Context, tier Tier, protocol Protocol, keyID, channel string, anonymous bool, proxy *proxyTransport) {
	meta, _ := ctx.Value(requestMetaKey{}).(*requestMeta)
	if meta == nil {
		return
	}
	meta.Tier = string(tier)
	meta.Protocol = string(protocol)
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

func setRequestModel(ctx context.Context, model string) {
	meta, _ := ctx.Value(requestMetaKey{}).(*requestMeta)
	if meta == nil {
		return
	}
	if model != "" {
		meta.Model = model
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

func (g *Gateway) recordUpstreamAttemptWithClass(route modelRoute, protocol Protocol, ids requestIDs, attempt int, keyID, channel string, anonymous bool, proxy *proxyTransport, resp *http.Response, err error, duration time.Duration, class attemptClassification, routeSessionReplay, droppedPrev, droppedReasoning bool, diag badRequestDiag) {
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
	tier := string(route.Tier)
	if protocol == "" {
		protocol = route.Protocol
	}
	// Diagnostic hygiene: only exact 400 carries hint/type/code/fingerprint.
	// Non-400 attempts omit all four even if a stale diag was passed.
	var hint, typ, code, fp string
	if status == http.StatusBadRequest && !diag.empty() {
		hint, typ, code, fp = diag.Hint, diag.Type, diag.Code, diag.Fingerprint
	}
	g.monitor.RecordAttempt(UpstreamAttempt{
		Time: time.Now().UTC(), RequestID: ids.Request, Model: route.ID, Tier: tier, Protocol: string(protocol), ClientSessionHash: clientSessionHash(ids.Session), Attempt: attempt,
		KeyID: keyID, Channel: channel, Anonymous: anonymous, Proxy: proxyName, ProxyPool: proxyPool, Status: status,
		DurationMS: max(duration.Milliseconds(), 0), Success: success, Outcome: outcomeFromClass(class.Class, success),
		FailureClass: class.Class, Retryable: class.Retryable, CoolsDown: class.CoolsDown,
		RouteSessionReplay: routeSessionReplay, DroppedPreviousResponseID: droppedPrev, DroppedReasoningRefs: droppedReasoning,
		ErrorHint: hint, ErrorType: typ, ErrorCode: code, ErrorFingerprint: fp,
	})
}

func newUpstreamRequest(ctx context.Context, baseURL string, protocol Protocol, body []byte, ids requestIDs, key, routeSession string) (*http.Request, error) {
	endpoint := strings.TrimRight(baseURL, "/") + protocolPath(protocol)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	// Single canonical OpenCode wire header set shared with custom fallback
	// inference and custom probes (bare UA, client, pseudonymous
	// session/request/project/parent). Auth below stays per-target.
	setOpenCodeWireHeaders(req.Header, ids, routeSession)
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
	// Single shared classification: only ordinary client_rejected ends the
	// channel/tier. 408/425 are transient_client and stay retryable; the
	// mapping lives in classifyAttempt.
	return classifyUpstreamAttempt(resp, err).Class == AttemptClassClientRejected
}

// syncProxyResult updates proxy transport health from non-refresh traffic.
// A single foreground transport error never flips proxy health directly;
// it only triggers the async neutral verification below. Only that
// independent probe may flip healthy on isProxyFailure. HTTP statuses never
// change health directly. Other errors and 4xx/5xx responses trigger a
// neutral URL check without being treated as proxy failure. Scheduler
// (credential/target) state is never touched here. A cancelled or expired
// request context is never a proxy signal: it returns without touching
// healthy/checking. Model refresh paths must not call this helper at all;
// refresh is stateless and observes healthy proxies read-only.
func (g *Gateway) syncProxyResult(ctx context.Context, proxy *proxyTransport, status int, err error) bool {
	if proxy == nil {
		return false
	}
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	if status >= 200 && status < 400 && err == nil {
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
	if proxy == nil {
		return
	}
	if ctx != nil && ctx.Err() != nil {
		return
	}
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
		_, _ = g.refreshCatalogGated(ctx, "scheduled")
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

// refreshCatalogGated runs one catalog refresh under the shared manual /
// scheduled gate. It reports whether the gate was acquired; a busy gate
// returns ran=false without stacking another refresh.
func (g *Gateway) refreshCatalogGated(ctx context.Context, source string) (ran bool, refreshed bool) {
	if g == nil {
		return false, false
	}
	if !g.catalogRefreshMu.TryLock() {
		return false, false
	}
	defer g.catalogRefreshMu.Unlock()
	return true, g.runCatalogRefresh(ctx, source)
}

// runCatalogRefresh performs one Zen/capability refresh pass. It reuses
// the stateless foreground-safe traversal, keeps the previous snapshot on
// failure, and emits a single catalog_refresh_completed event. Callers must
// hold catalogRefreshMu.
func (g *Gateway) runCatalogRefresh(ctx context.Context, source string) (refreshed bool) {
	started := time.Now()
	var zen []string
	var capabilities protocolCapabilities
	var capabilitiesErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); zen = g.refreshZen(ctx) }()
	go func() {
		defer wg.Done()
		capabilityCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		capabilities, capabilitiesErr = g.refreshProtocolCapabilities(capabilityCtx)
	}()
	wg.Wait()
	duration := time.Since(started)
	if ctx.Err() != nil {
		return false
	}
	if capabilitiesErr != nil && g.logger != nil {
		g.logger.Warn("OpenCode capability catalog refresh failed", "component", "models", "event", "capability_refresh_failed", "source", source, "error", capabilitiesErr)
	}
	if zen != nil {
		g.catalog.ReplaceWithCapabilities(zen, nil, capabilities.Protocols, capabilities.Unsupported, capabilities.Metadata)
		if ctx.Err() == nil {
			if err := g.catalog.SaveCache(); err != nil && g.logger != nil {
				g.logger.Warn("model catalog cache write failed", "component", "models", "event", "catalog_cache_write_failed", "source", source, "error", err)
			}
		}
		refreshed = true
	}
	if g.logger != nil {
		if refreshed {
			g.logger.Info("model catalog refresh completed", "component", "models", "event", "catalog_refresh_completed", "source", source, "duration_ms", duration.Milliseconds(), "refreshed", true, "models", len(g.catalog.List()))
		} else {
			g.logger.Warn("model catalog refresh completed", "component", "models", "event", "catalog_refresh_completed", "source", source, "duration_ms", duration.Milliseconds(), "refreshed", false)
		}
	}
	return refreshed
}

func (g *Gateway) refreshProtocolCapabilities(ctx context.Context) (protocolCapabilities, error) {
	clients := g.healthyClients()
	if len(clients) == 0 {
		if len(g.uniquePools()) == 0 {
			return fetchProtocolCapabilities(ctx, &http.Client{Timeout: 30 * time.Second}, openCodeCapabilitiesEndpoint)
		}
		return protocolCapabilities{}, errors.New("no healthy proxy available for OpenCode capability catalog")
	}
	var lastErr error
	for _, client := range clients {
		capabilities, err := fetchProtocolCapabilities(ctx, client, openCodeCapabilitiesEndpoint)
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
// It is stateless: foreground scheduler credential/target state is neither
// read nor written, and proxy healthy/checking is never changed via
// syncProxyResult/verifyProxyAfterError. Healthy proxies are observed
// read-only to build the traversal; success/failure only decides the
// catalog snapshot in the caller, and a context deadline/cancel is only a
// refresh failure, never a proxy signal.
func (g *Gateway) refreshAnonymousTier(ctx context.Context, base string) []string {
	pool := g.pools[g.cfg.ProxyRouting.Anonymous]
	if pool == nil {
		return nil
	}
	healthy := healthyProxies(pool)
	for attempt, proxy := range healthy {
		refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		models, _, err := fetchModels(refreshCtx, proxy.client, base, anonymousZenKey)
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
// traversal bounded by retry.max_attempts. Foreground scheduler
// credential/target state is never read or written here, and proxy
// healthy/checking is never changed via syncProxyResult/verifyProxyAfterError.
// Healthy proxies are observed read-only; success/failure only decides the
// catalog snapshot in the caller, and a context deadline/cancel is only a
// refresh failure, never a proxy signal. Only the Zen lane exists; the tier
// argument is retained for compat and ignored beyond pool selection.
func (g *Gateway) refreshTier(ctx context.Context, base string, tier Tier) []string {
	keys := g.cfg.Keys
	poolName := g.authPoolName()
	_ = tier
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
		models, _, err := fetchModels(refreshCtx, proxy.client, base, key)
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
