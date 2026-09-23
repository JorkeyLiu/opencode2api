package app

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
	// availGate is the availability RW gate: single (per-row/scoped)
	// detections hold read locks (TryRLock) so independent singles run
	// concurrently; batch detections hold the write lock (TryLock) so
	// batches are single-flight with each other and mutually exclusive
	// with singles in both directions. Any failed acquisition returns
	// fail-fast 409 bulk_busy without queueing.
	// bulkSnapshot is the admin-only latest-result projection: fixed
	// bounded, replaced atomically per completed/partial run, never
	// routing authority. Restart/Apply clears via fresh Gateway.
	// bulkSnapMu serializes concurrent single-merge load/copy/store
	// sections only (never network sends) so independent single results
	// are not lost.
	bulkMu       sync.RWMutex
	bulkSnapMu   sync.Mutex
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
	timeout := time.Duration(cfg.Retry.AttemptTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
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
	suspectCooldown := secondsToDuration(cfg.Performance.TransportSuspectCooldownSeconds)
	if suspectCooldown <= 0 {
		suspectCooldown = 15 * time.Second
	}
	catalog := newModelCatalog("", cfg.Models.Protocols)
	catalog.SetRefreshInterval(time.Duration(cfg.Models.RefreshSeconds) * time.Second)
	authCreds := credentialsForKeys(TierZen, cfg.Keys)
	return &Gateway{
		cfg:       cfg,
		logger:    logger,
		pools:     pools,
		scheduler: newTargetScheduler(cooldown, rateCooldown, suspectCooldown),
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
		if route.Anonymous {
			if canonical := bodies[route.Tier]; len(canonical) > 0 {
				if _, shapeErr := shapedAnonymousBody(canonical, route.ProtocolFor(route.Tier)); shapeErr != nil {
					writeAPIError(w, external, http.StatusBadRequest, shapeErr.Error(), "invalid_request_error", "")
					return
				}
			}
		}
		ids := deriveRequestIDs(r, payload)
		if meta != nil {
			meta.Request = ids.Request
			meta.ClientSessionHash = clientSessionHash(ids.Session)
			meta.Protocol = string(external)
		}
		stream := boolAt(payload, "stream")
		if meta != nil {
			meta.Stream = stream
		}
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
		_, _, anonymousLane := requestCredential(requestCtx)
		if !stream && anonymousLane && upstreamRoute.Tier == TierZen {
			collapsed, collapsedUsage, collapsedReported, collapseErr := collapseUpstreamSSE(resp.Body, upstreamRoute.Protocol, external, model)
			if collapseErr != nil {
				g.logger.Warn("anonymous upstream stream collapse failed", "component", "stream", "event", "anonymous_collapse_failed", "request_id", ids.Request, "model", model, "client_session_hash", clientSessionHash(ids.Session), "source_protocol", upstreamRoute.Protocol, "target_protocol", external, "error", collapseErr)
				writeAPIError(w, external, http.StatusBadGateway, "upstream stream failed", "upstream_error", ids.Request)
				return
			}
			if meta != nil {
				meta.Usage, meta.UsageReported = collapsedUsage, collapsedReported
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(resp.StatusCode)
			_, _ = w.Write(collapsed)
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
	// Per-candidate 400 recovery lives inside doUpstreamTiers (fixed
	// same target, one replay with the same route session). No outer random
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

func (g *Gateway) observationAttempts() int {
	if g == nil {
		return 3
	}
	n := g.cfg.Retry.MaxAttempts
	if n < 1 {
		n = 1
	}
	return n
}

func (g *Gateway) transientInterval() time.Duration {
	if g == nil {
		return 3 * time.Second
	}
	d := time.Duration(g.cfg.Retry.TransientRetryIntervalSeconds) * time.Second
	if d < 0 {
		d = 0
	}
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

func (g *Gateway) attemptTimeout() time.Duration {
	if g == nil {
		return 5 * time.Second
	}
	d := secondsToDuration(g.cfg.Retry.AttemptTimeoutSeconds)
	if d <= 0 {
		d = 5 * time.Second
	}
	if d > secondsToDuration(g.cfg.Retry.TimeoutSeconds) {
		d = secondsToDuration(g.cfg.Retry.TimeoutSeconds)
	}
	return d
}

func isStreamContext(ctx context.Context) bool {
	meta, _ := ctx.Value(requestMetaKey{}).(*requestMeta)
	return meta != nil && meta.Stream
}

func isStreamStartupFailureErr(err error) bool {
	return err != nil && err.Error() == "upstream stream startup failure"
}

func isTrueTransportError(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if isContextCancelled(ctx) {
		return false
	}
	if isStreamStartupFailureErr(err) {
		return false
	}
	// 408/425 are HTTP statuses, not transport err, so not here.
	// Context cancellation already excluded.
	return true
}

func streamedAttemptDuration(startedNanos int64) time.Duration {
	if startedNanos <= 0 {
		return 0
	}
	d := time.Duration(time.Now().UnixNano() - startedNanos)
	if d < 0 {
		return 0
	}
	return d
}

// gatedStreamBody is the post-commit stream wrapper for streaming inference.
// After the sseStartupGate reaches commit, the opening frames are buffered
// exactly as raw bytes plus parsed events in order. The remainder is the
// pending incomplete tail plus the original Body. Read first replays the
// buffered raw opening (preserving exact byte order for forward) then the
// pending tail then the remaining stream, so existing forward and transcode
// paths see a complete ordered byte stream without needing separate replay.
type gatedStreamBody struct {
	gate       *sseStartupGate
	buffered   []byte
	bufOffset  int
	pending    []byte
	pendOffset int
	remaining  io.ReadCloser
	parser     *bridgeStreamParser
}

func newGatedStreamBody(gate *sseStartupGate, pending []byte, remaining io.ReadCloser, parser *bridgeStreamParser) *gatedStreamBody {
	var buffered []byte
	if gate != nil {
		buffered = gate.BufferedRaw()
	}
	return &gatedStreamBody{gate: gate, buffered: buffered, pending: pending, remaining: remaining, parser: parser}
}

func (g *gatedStreamBody) Read(p []byte) (int, error) {
	if g == nil {
		return 0, io.EOF
	}
	if g.buffered != nil && g.bufOffset < len(g.buffered) {
		n := copy(p, g.buffered[g.bufOffset:])
		g.bufOffset += n
		if n > 0 {
			return n, nil
		}
	}
	if len(g.pending) > g.pendOffset {
		n := copy(p, g.pending[g.pendOffset:])
		g.pendOffset += n
		if n > 0 {
			return n, nil
		}
	}
	if g.remaining == nil {
		return 0, io.EOF
	}
	return g.remaining.Read(p)
}

func (g *gatedStreamBody) Close() error {
	if g == nil || g.remaining == nil {
		return nil
	}
	return g.remaining.Close()
}

// verifyStreamGate runs the sseStartupGate until commit or startup failure.
// It buffers raw frames and parsed events, preserving exact order, and
// returns the gate, pending bytes (incomplete tail), and parser state.
// The caller must not have called WriteHeader yet. Context cancellation is
// treated as cancellation, not startup failure.
func (g *Gateway) verifyStreamGate(ctx context.Context, body io.Reader, protocol Protocol) (*sseStartupGate, []byte, *bridgeStreamParser, error) {
	gate := newSSEStartupGate()
	parser := &bridgeStreamParser{
		protocol:          protocol,
		tools:             map[string]bool{},
		toolIDs:           map[string]string{},
		toolNames:         map[string]string{},
		responseArgs:      map[string]bool{},
		responseReasoning: map[string]bool{},
	}
	// pending accumulates raw bytes not yet framed
	var pending bytes.Buffer
	tmp := make([]byte, 4096)
	for {
		if gate.ShouldCommit() || gate.HasStartupFailure() {
			break
		}
		if ctx != nil && ctx.Err() != nil {
			return gate, pending.Bytes(), parser, ctx.Err()
		}
		// Drain complete frames from pending before reading more
		drained := false
		for {
			data := pending.Bytes()
			idx, width := nextSSEBoundary(data)
			if idx < 0 {
				break
			}
			frame := make([]byte, idx+width)
			copy(frame, data[:idx+width])
			// remove frame from pending
			remaining := make([]byte, len(data)-idx-width)
			copy(remaining, data[idx+width:])
			pending.Reset()
			pending.Write(remaining)
			gate.AddRawFrame(frame)
			evs, parseErr := parseSSEFrame(frame, parser)
			if parseErr != nil {
				gate.NoteParseError(parseErr)
			} else if len(evs) > 0 {
				gate.AddEvents(evs)
			}
			drained = true
			if gate.ShouldCommit() || gate.HasStartupFailure() {
				break
			}
		}
		if gate.ShouldCommit() || gate.HasStartupFailure() {
			break
		}
		if drained {
			// try again to see if pending still has frames without reading
			continue
		}
		// Need more data
		n, readErr := body.Read(tmp)
		if n > 0 {
			pending.Write(tmp[:n])
			continue
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				gate.NoteReadError(nil)
			} else if errors.Is(readErr, errSSEUnexpectedEOF) {
				gate.NoteReadError(readErr)
			} else if streamClientCancelled(ctx, readErr) {
				return gate, pending.Bytes(), parser, readErr
			} else {
				gate.NoteReadError(readErr)
			}
			break
		}
		// n==0 and readErr==nil: avoid spin
		if ctx != nil {
			select {
			case <-ctx.Done():
				return gate, pending.Bytes(), parser, ctx.Err()
			default:
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if ctx != nil && ctx.Err() != nil && !gate.ShouldCommit() && !gate.HasStartupFailure() {
		return gate, pending.Bytes(), parser, ctx.Err()
	}
	return gate, pending.Bytes(), parser, nil
}

// parseSSEFrame parses a single complete SSE frame into bridge events.
func parseSSEFrame(frame []byte, parser *bridgeStreamParser) ([]bridgeStreamEvent, error) {
	var events []bridgeStreamEvent
	var parseErr error
	err := readSSE(bytes.NewReader(frame), func(eventName, data string) error {
		evs, err := parser.Parse(eventName, data)
		if err != nil {
			parseErr = err
			return err
		}
		events = append(events, evs...)
		return nil
	})
	if parseErr != nil {
		return nil, parseErr
	}
	if err != nil && !errors.Is(err, io.EOF) {
		if errors.Is(err, errSSEUnexpectedEOF) {
			return nil, err
		}
		// handler errors other than parse are not expected for single frame
		return events, err
	}
	return events, nil
}

// applyStreamSuccess marks the scheduler success path for a gate-committed stream.
// It mirrors the 2xx branch of applyAttemptOutcome but is called only after commit.
func (g *Gateway) applyStreamSuccess(cand targetCandidate, startedNanos int64) {
	if g == nil || g.scheduler == nil {
		return
	}
	targetCleared := g.scheduler.noteTargetSuccess(cand.Identity)
	credCleared := g.scheduler.noteCredentialSuccess(cand.CredID)
	proxyCleared := g.scheduler.noteProxy429Success(cand.Tier, cand.PoolName, cand.ProxyRaw, startedNanos)
	channelCleared := g.scheduler.noteChannelSuccess(cand.Tier, cand.PoolName, cand.ProxyRaw, startedNanos)
	cred429Cleared := g.scheduler.noteCredential429Success(cand.CredID, startedNanos)
	suspectCleared := g.scheduler.noteTransportSuspectSuccess(cand.Tier, cand.PoolName, cand.ProxyRaw, startedNanos)
	if suspectCleared.Changed && g.logger != nil {
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
		g.logger.Debug("proxy transport suspect cleared",
			"component", "scheduler", "event", "proxy_transport_suspect_cleared",
			"tier", string(cand.Tier), "key_id", cand.CredDisplay,
			"channel", credentialChannel(cand),
			"proxy_pool", poolName, "proxy_node", proxyNode,
			"failures", suspectCleared.Failures)
	}
	g.logSchedulerCleared(cand, targetCleared, credCleared, proxyCleared, channelCleared, cred429Cleared)
	if cand.Proxy != nil && !cand.Proxy.healthy.Load() {
		wasHealthy := cand.Proxy.healthy.Swap(true)
		if !wasHealthy && g.logger != nil {
			g.logger.Info("proxy connectivity restored", "component", "proxy", "event", "proxy_restored", "proxy", redactURL(cand.Proxy.name), "proxy_pool", cand.Proxy.pool)
		}
	}
}

// noteStreamStartupFailure cools the single target for a pre-commit startup failure
// and records the observability attempt. It never touches proxy health, proxy429,
// credential429, or channel state. A cancelled stream never cools the target:
// cancellation is not an object-unavailable signal. The monitoring record below
// keeps the existing recording policy unchanged (still recorded on cancel).
func (g *Gateway) noteStreamStartupFailure(ctx context.Context, cand targetCandidate, route modelRoute, ids requestIDs, attempt int, startedNanos int64) {
	if g == nil || g.scheduler == nil {
		return
	}
	if !isContextCancelled(ctx) {
		change := g.scheduler.noteTargetFailure(cand.Identity, AttemptClassUpstreamFailure, http.StatusBadGateway, 0)
		g.logTargetCooldownSet(cand, change)
	}
	class := attemptClassification{Class: AttemptClassUpstreamFailure, Retryable: true, CoolsDown: true}
	fakeResp := &http.Response{StatusCode: http.StatusBadGateway, Header: make(http.Header)}
	duration := streamedAttemptDuration(startedNanos)
	g.recordUpstreamAttemptWithClass(route, route.Protocol, ids, attempt, cand.CredDisplay, credentialChannel(cand), cand.CredID == anonymousSchedulerCredentialID, cand.Proxy, fakeResp, nil, duration, class, false, false, false, badRequestDiag{})
	_ = ctx
}

// attemptOutcome is the structured single-send result owned by executeAttempt.
// It carries the upstream response/error, request-build error, diagnostic,
// and send-start time sufficient for the outer loop to decide candidate
// advance, proxy fallback, 400 replay, pin moves, and custom fallback
// without re-implementing send/classification/stream gating. Started is the
// true send-start nanos from sendUpstreamOnce and is required by doPinnedAuth
// for credential429 stale fencing (lastStartedNanos comparison).
type attemptOutcome struct {
	Resp     *http.Response
	Err      error
	BuildErr error
	Diag     badRequestDiag
	Started  int64
}

// executeAttempt is the single-attempt executor. It owns: invoking the existing
// request send/state-classification authority (sendUpstreamOnce), applying the
// streaming startup gate before client commitment, stream-success
// scheduler/monitor recording, startup-failure classification/recording, and
// returning a structured outcome. It does NOT select/advance candidates, decide
// proxy fallback, accumulate credential429 evidence,
// invoke custom fallback, replay exact 400, bind/move pins, or change
// route-session/body construction.
func (g *Gateway) executeAttempt(ctx context.Context, route modelRoute, tier Tier, baseURL string, protocol Protocol, candBody []byte, ids requestIDs, cand targetCandidate, routeSession, channel, credDisplay string, anonymous bool, monitorAttempt int) attemptOutcome {
	resp, err, _, _, diag, started, buildErr := g.sendUpstreamOnce(ctx, route, tier, baseURL, protocol, candBody, ids, cand, routeSession, channel, credDisplay, anonymous, monitorAttempt)
	if buildErr != nil {
		return attemptOutcome{BuildErr: buildErr, Diag: diag, Started: started}
	}
	if err == nil && resp != nil && resp.StatusCode/100 == 2 && isStreamContext(ctx) {
		gate, pending, parser, verifyErr := g.verifyStreamGate(ctx, resp.Body, protocol)
		if verifyErr != nil && isContextCancelled(ctx) {
			drainAndClose(resp.Body)
			return attemptOutcome{Resp: nil, Err: ctx.Err(), Diag: diag, Started: started}
		}
		if gate.ShouldCommit() {
			// A cancelled stream keeps its already-gated committed body and
			// envelope, but never applies scheduler success: cancellation is
			// not a clear signal. The monitoring record below keeps the
			// existing recording policy unchanged (still recorded on cancel),
			// mirroring applyAttemptOutcome's cancel behavior on non-stream
			// paths (early return without state change, record still kept).
			if !isContextCancelled(ctx) {
				g.applyStreamSuccess(cand, started)
			}
			g.recordUpstreamAttemptWithClass(route, protocol, ids, monitorAttempt, credDisplay, channel, anonymous, cand.Proxy, resp, nil, streamedAttemptDuration(started), attemptClassification{Class: AttemptClassSuccess}, false, false, false, badRequestDiag{})
			resp.Body = newGatedStreamBody(gate, pending, resp.Body, parser)
			return attemptOutcome{Resp: resp, Err: nil, Diag: diag, Started: started}
		}
		g.noteStreamStartupFailure(ctx, cand, route, ids, monitorAttempt, started)
		drainAndClose(resp.Body)
		return attemptOutcome{Resp: nil, Err: errors.New("upstream stream startup failure"), Diag: diag, Started: started}
	}
	return attemptOutcome{Resp: resp, Err: err, Diag: diag, Started: started}
}

func transientDelay(interval time.Duration, resp *http.Response) time.Duration {
	d := interval
	if resp != nil {
		if ra := parseRetryAfter(resp.Header.Get("Retry-After")); ra > d {
			d = ra
		}
	}
	return d
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	if ctx == nil {
		time.Sleep(d)
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// transientStopReason is the structured L1 observation stop: why the
// same-target observation loop stopped. Stable means the final outcome is no
// longer isSameTargetTransient (2xx/400/429/ordinary 4xx/BuildErr included) and
// the caller owns all policy for it. Context covers pre-retry cancellation and
// sleep interruption. ObservationLimit means maxObservation (normalized
// retry.max_attempts, including the initial send) was reached while still
// transient.
type transientStopReason int

const (
	transientStopStable transientStopReason = iota
	transientStopContext
	transientStopObservationLimit
)

// transientLoopResult is the L1 observation outcome with history metadata for
// exact HEAD equivalence. Final is the last observed outcome (stable, context,
// or limit). InitialResp/InitialErr preserve the initial send so
// pinned callers can reproduce the old stale-initial return (mixed 503 ->
// transport-nil at the observation limit) and the old stream-startup poisoning
// (initial OR final sentinel => local502, matching old
// isStreamStartupFailureErr(curErr)||isStreamStartupFailureErr(sendErr)).
// Intermediate sentinel-only states never poison: only initial or final counts.
type transientLoopResult struct {
	Final       attemptOutcome
	Stop        transientStopReason
	InitialResp *http.Response
	InitialErr  error
}

// sawStreamSentinel reports the old pinned poisoning condition: either the
// initial send or the final outcome is the stream-startup sentinel.
func (r transientLoopResult) sawStreamSentinel() bool {
	return isStreamStartupFailureErr(r.InitialErr) || isStreamStartupFailureErr(r.Final.Err)
}

// stableCause names the single post-L1 stable reason observed after
// observeSameTargetTransient stops. It only describes why the observation
// stopped and what the final outcome is; it never selects candidates,
// proxy moves, scheduler writes, custom fallback, or return envelopes.
// L1Final covers every remaining live final (L1-final transient 408/425/5xx
// including 503, true transport errors, stream-startup sentinels, and stable
// 401/403); callers keep their existing fine-grained policy inside that
// bucket (sentinel poisoning, stale-initial returns, transport walks,
// 5xx no-move, exhaustion evidence).
type stableCause int

const (
	stableCauseContext stableCause = iota
	stableCauseBuildFailure
	stableCauseSuccess
	stableCauseExact400
	stableCauseLive429
	stableCauseOrdinaryRejection
	stableCauseL1Final
)

// stableCauseResult is the structured post-L1 classification: the cause plus
// the raw L1 outcome passthrough that existing caller policy still needs.
// Final/Stop/InitialResp/InitialErr are loopRes unchanged. StreamSentinel is
// loopRes.sawStreamSentinel(), StillTransient is
// isSameTargetTransient(Final), Cancelled is isContextCancelled(ctx).
type stableCauseResult struct {
	Cause          stableCause
	Final          attemptOutcome
	Stop           transientStopReason
	InitialResp    *http.Response
	InitialErr     error
	StreamSentinel bool
	StillTransient bool
	Cancelled      bool
}

// classifyStableCause is the single post-L1 classification authority shared
// by the four native recovery walkers. Cause describes the observed final
// outcome plus the L1 stop fact only: build failure and 2xx success precede
// everything (a BuildErr/2xx final is returned as-is even under
// cancellation), then the L1 stop-context (Stop == transientStopContext),
// exact 400, live 429, ordinary rejection, and finally the L1-final bucket.
// Late cancellation after observation (Stop == stable/observation-limit with
// a cancelled ctx) is NOT folded into Cause; it is carried in Cancelled for
// each caller's own preserved gate, so an observationLimit+cancelled final
// still classifies as its outcome (e.g. live 429, exact 400, L1-final)
// exactly as the old per-path predicates did. All status predicates reuse
// the existing authorities; no status-code matrix is copied here.
func classifyStableCause(ctx context.Context, loopRes transientLoopResult) stableCauseResult {
	final := loopRes.Final
	cancelled := isContextCancelled(ctx)
	res := stableCauseResult{
		Final:          final,
		Stop:           loopRes.Stop,
		InitialResp:    loopRes.InitialResp,
		InitialErr:     loopRes.InitialErr,
		StreamSentinel: loopRes.sawStreamSentinel(),
		StillTransient: isSameTargetTransient(final.Resp, final.Err),
		Cancelled:      cancelled,
	}
	switch {
	case final.BuildErr != nil:
		res.Cause = stableCauseBuildFailure
	case final.Err == nil && final.Resp != nil && final.Resp.StatusCode/100 == 2:
		res.Cause = stableCauseSuccess
	case loopRes.Stop == transientStopContext:
		res.Cause = stableCauseContext
	case isRouteTerminalBadRequest(final.Resp, final.Err):
		res.Cause = stableCauseExact400
	case final.Err == nil && final.Resp != nil && final.Resp.StatusCode == http.StatusTooManyRequests:
		res.Cause = stableCauseLive429
	case isOrdinaryClientRejection(final.Resp, final.Err):
		res.Cause = stableCauseOrdinaryRejection
	default:
		res.Cause = stableCauseL1Final
	}
	return res
}

// pinnedAuthContextEarlyReturn is the exact HEAD doPinnedAuth context
// early-return gate kept verbatim: only an L1 stop-context or a stable final
// with a cancelled ctx returns early. An observationLimit final with a ctx
// cancelled after observation stays on the old L1-final path (sentinel,
// stale-initial, transport-walk, 5xx no-move policy); it must not be folded
// into the broad stop==context-or-cancelled gates the unbound and
// pinned-anonymous paths keep.
func pinnedAuthContextEarlyReturn(r stableCauseResult) bool {
	return r.Stop == transientStopContext || (r.Stop == transientStopStable && r.Cancelled)
}

// observeSameTargetTransient owns only the L1 same-target observation mechanism
// starting from an initial transient attemptOutcome: while it remains
// isSameTargetTransient it drains the retried outcome, sleeps with
// Retry-After/interval and context, increments attempts, syncs attempt meta,
// and executes the next attempt via exec. It never drains the returned stable
// or limit final response; the caller drains or returns it per policy. The
// caller owns every policy/action: BuildErr return, 2xx bind/move, exact-400
// replay, 429 evidence/Started/custom/candidate walk, ordinary 4xx,
// transport cross-proxy rules, stream startup local502, and scheduler/pin
// state. The loop has no candidate-send budget: maxObservation (normalized
// retry.max_attempts, including the initial send) is the unique L1 stop, and
// candidate traversal is bounded only by the frozen eligible/candidate slice.
// The stream startup sentinel is returned unchanged so pinned callers produce
// local502 while unbound callers retain candidate behavior. Sleep interruption
// keeps context behavior.
func (g *Gateway) observeSameTargetTransient(ctx context.Context, initial attemptOutcome, tier Tier, protocol Protocol, attempts *int, attemptOffset int, maxObservation int, interval time.Duration, exec func(monitorAttempt int) attemptOutcome) transientLoopResult {
	cur := initial
	initResp := initial.Resp
	initErr := initial.Err
	sends := 1
	if maxObservation < 1 {
		maxObservation = 1
	}
	mk := func(final attemptOutcome, stop transientStopReason) transientLoopResult {
		return transientLoopResult{Final: final, Stop: stop, InitialResp: initResp, InitialErr: initErr}
	}
	for {
		if cur.BuildErr != nil {
			return mk(cur, transientStopStable)
		}
		if !isSameTargetTransient(cur.Resp, cur.Err) {
			return mk(cur, transientStopStable)
		}
		if isContextCancelled(ctx) {
			return mk(cur, transientStopContext)
		}
		if sends >= maxObservation {
			return mk(cur, transientStopObservationLimit)
		}
		d := transientDelay(interval, cur.Resp)
		if cur.Resp != nil {
			drainAndClose(cur.Resp.Body)
		}
		if !sleepWithContext(ctx, d) {
			ctxErr := context.Canceled
			if ctx != nil && ctx.Err() != nil {
				ctxErr = ctx.Err()
			}
			return mk(attemptOutcome{Err: ctxErr}, transientStopContext)
		}
		*attempts++
		syncAttemptMeta(ctx, tier, protocol, attemptOffset, *attempts)
		cur = exec(attemptOffset + *attempts)
		sends++
	}
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

// bindSessionPinCtx is the context-aware bind entry for post-send paths. The
// outer pre-check is a fast path only; the store-level lock-held recheck
// inside pinBindCtx defines the local linearization point, so a ctx that
// cancels between send and write never binds. It never touches pin claims,
// waiters, the fallback store, first-wins, or generation fencing.
func (g *Gateway) bindSessionPinCtx(ctx context.Context, session, model string, tier Tier, credID, pool, proxyRaw string, protocol Protocol, authority string) {
	if g == nil || g.scheduler == nil || session == "" || model == "" {
		return
	}
	if isContextCancelled(ctx) {
		return
	}
	g.scheduler.pinBindCtx(ctx, session, model, sessionPin{
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

// lookupCustomFallbackBinding centralizes the repeated nil-safe session
// fallback lookup shared by the three ADR-required recheck windows. It
// performs lookup only and never serves, owns, or releases pin claims, so the
// post-claim window can release before network I/O. Callers keep the exact
// window placement and order: fallback lookup before pin lookup, serving via
// doCustomFallbackPinned on hit.
func (g *Gateway) lookupCustomFallbackBinding(ids requestIDs) (fallbackBinding, bool) {
	if g == nil || g.scheduler == nil || g.scheduler.fallbacks == nil || ids.Session == "" {
		return fallbackBinding{}, false
	}
	return g.scheduler.fallbacks.get(ids.Session)
}

func (g *Gateway) doUpstreamTiers(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, attemptOffset int, extra ...upstreamExtra) (*http.Response, modelRoute, int, error) {
	if binding, ok := g.lookupCustomFallbackBinding(ids); ok {
		return g.doCustomFallbackPinned(ctx, route, bodies, ids, binding, attemptOffset, extra...)
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
// Pending bindings (no HTTP 2xx yet) keep crossing the authority: the send
// strips provider-bound Responses refs so a failed first custom attempt never
// forwards native-issued previous_response_id/reasoning items. Only
// established bindings (first 2xx observed) preserve refs issued by the custom
// channel itself. The first 2xx on this path flips the binding to established.
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
	crossing := !binding.Established
	resp, effectiveRoute, nextAttempts, sendErr := g.doCustomFallbackRequestCrossingAuthority(ctx, route, ex, hasEx, bodies, ids, ch, binding, attemptOffset, crossing)
	if isCustomFallbackSuccess(resp, sendErr) && g != nil && g.scheduler != nil && g.scheduler.fallbacks != nil {
		g.scheduler.fallbacks.markEstablished(ids.Session)
	}
	return resp, effectiveRoute, nextAttempts, sendErr
}

// customTakeoverQualification is the explicit L2 429-only custom takeover
// proof (Bounded Increment 6, behavior lock, no new eligibility). The current
// recovery-domain exhaustion is proven only when every currently eligible
// target has supplied distinct live 429 evidence in this request/route:
// pre-cooled skips never enter the frozen eligible set and never count as
// evidence, partial 429 is false, any terminal non-429 outcome is false, a
// 400 corrective-replay final route is false, and cancel/deadline or stream
// committed paths are false. Credential/pool/proxy/session identity, last
// Retry-After selection, Started stale fencing, active-channel lookup,
// capacity fail-closed, and scheduler cooldown writes stay with the callers;
// this helper owns only the shared qualification gate and creates no second
// authority. Generalized L2 fallback remains unimplemented.
type customTakeoverQualification struct {
	ObservedLive429 int
	Eligible        int
	TerminalStatus  int
	Recovered400    bool
	Cancelled       bool
	Committed       bool
}

// customTakeoverEligible reports strict 429-only takeover eligibility for one
// recovery domain step. TerminalStatus is the final HTTP status (0 for a
// transport error / no response). Eligible must be >0: the pre-cooled
// local-429 fast path (zero live-eligible with an actively cooling proxy429)
// stays a separate caller-owned branch and never qualifies here.
func customTakeoverEligible(q customTakeoverQualification) bool {
	if q.Cancelled || q.Committed || q.Recovered400 {
		return false
	}
	if q.TerminalStatus != http.StatusTooManyRequests {
		return false
	}
	if q.Eligible <= 0 {
		return false
	}
	return q.ObservedLive429 >= q.Eligible
}

// finalNon429ObjectUnavailable is the single leaf authority for the
// overlapping non-429 final-unavailable classification shared by the unbound
// and pinned exhaustion wrappers. It reports whether one live final outcome
// proves its object unavailable excluding 429: 401/403, L1-final 408/425/5xx
// (including 503), true transport failure, and stream-startup failure. Any
// non-nil err counts as unavailable here; the L1-final, cancel/deadline,
// BuildErr/no-send, and committed-stream gating stays with the callers, which
// only pass the L1-final outcome of a real send via the frozen-walk paths.
// Exact 400 (same-target corrective replay, route-terminal), ordinary client
// 4xx, 2xx and other statuses, nil/nil, and 429 never count here; 429
// ownership stays with the wrappers (unbound counts live 429, pinned excludes
// it via the separate full-live-429 gate). No scheduler writes, attempts
// accounting, Started fencing, response draining, proxy moves, or
// route-session behavior lives here.
func finalNon429ObjectUnavailable(resp *http.Response, err error) bool {
	if err != nil {
		return true
	}
	if resp == nil {
		return false
	}
	status := resp.StatusCode
	if status == http.StatusTooManyRequests {
		return false
	}
	if status == http.StatusBadRequest {
		return false
	}
	if isOrdinaryClientRejection(resp, nil) {
		return false
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusRequestTimeout || status == 425 {
		return true
	}
	return status >= 500 && status <= 599
}

// pinnedConsumptionAllowCustom is the bounded pinned L2 consumption
// exhaustion gate (pinned native only). It never replaces the 429-only
// compat gate above: 429 finals return false here and stay owned by
// customTakeoverEligible (with credential429 writes). Consumption means a
// real switch/send to the next frozen eligible proxy already happened in
// this request (429 walk or pinned-auth transport next-proxy); the initial
// same-target L1 observation alone never counts, so single-proxy first
// non-429 failures (consumed=false) never qualify. Exhaustion requires
// every frozen eligible proxy to have a real live send in this request
// (attempted >= eligible); pre-cooled skips never enter eligible and
// BuildErr/no-send never counts. The final must prove its object
// unavailable: live 401/403, L1-final transport/408/425/5xx, or L1-final
// stream startup failure (only after a consume action). Exact-400,
// ordinary 4xx, cancel/deadline, and committed streams never qualify;
// without an active custom the caller keeps the native faithful envelope.
func pinnedConsumptionAllowCustom(eligible, attempted int, consumed, cancelled bool, resp *http.Response, err error) bool {
	if cancelled {
		return false
	}
	if eligible <= 0 || attempted < eligible {
		return false
	}
	if !consumed {
		return false
	}
	if err == nil && resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		return false
	}
	return finalNon429ObjectUnavailable(resp, err)
}

// unboundDomainEvidence is the per-domain object-unavailable exhaustion proof
// for one frozen unbound recovery domain: the anonymous lane or one
// authenticated credential. Domains are never summed: the outer unbound custom
// decision requires every collected domain to independently satisfy the
// state-agnostic object-exhaustion gate. Entered reports whether the domain
// actually sent at least one live upstream attempt in this request;
// empty/pre-cooled domains stay Entered=false/Frozen=0 and can never prove
// exhaustion. Frozen is the frozen candidate count; Unavailable counts the
// distinct frozen candidates with live unavailable evidence in this request
// (live 429, 401/403 terminal, L1-final transport/408/425/5xx, or L1-final
// stream startup failure; each frozen candidate counts at most once,
// intermediate L1 retries never count separately). The unbound gate reads
// only Entered/Frozen/Unavailable. Recovered400 marks a 400
// corrective-replay final for that domain (never counts as unavailable).
type unboundDomainEvidence struct {
	Domain       string
	Entered      bool
	Frozen       int
	Unavailable  int
	Recovered400 bool
}

// unboundObjectUnavailable reports whether one live final outcome proves its
// object unavailable for unbound exhaustion: live 429, 401/403, L1-final
// transport/408/425/5xx, or L1-final stream startup failure. 400 corrective
// replays (any terminal), ordinary client 4xx, cancel/deadline
// (caller-gated), committed streams, and build errors never count. The
// stream-startup sentinel counts only when the caller passes the L1-final
// outcome of a real send; BuildErr/unsent, cancel/deadline, committed, and
// 400/ordinary-4xx finals never reach it via the frozen-walk early returns.
func unboundObjectUnavailable(resp *http.Response, err error) bool {
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		return true
	}
	return finalNon429ObjectUnavailable(resp, err)
}

// unboundDomainExhausted reports state-agnostic object exhaustion for one
// unbound domain: every frozen candidate has live unavailable evidence in this
// request. Pre-cooled/empty domains (Entered=false/Frozen<=0) and 400-replay
// finals never qualify.
func unboundDomainExhausted(d unboundDomainEvidence) bool {
	if d.Recovered400 {
		return false
	}
	if !d.Entered || d.Frozen <= 0 {
		return false
	}
	return d.Unavailable >= d.Frozen
}

// unboundDomainsExhaustedAllowCustom is the single unbound custom authority:
// state-agnostic per-domain object exhaustion. Every collected domain
// (anonymous lane plus each authenticated credential, never summed) must
// independently satisfy unboundDomainExhausted, and the route must not be
// recovered/cancelled/committed. It is state-agnostic: no terminal or
// live-429 equality requirement. Pinned paths must not use it; they keep
// customTakeoverEligible (429-only).
func unboundDomainsExhaustedAllowCustom(domains []unboundDomainEvidence, recovered400, cancelled, committed bool) bool {
	if recovered400 || cancelled || committed {
		return false
	}
	if len(domains) == 0 {
		return false
	}
	for _, d := range domains {
		if !unboundDomainExhausted(d) {
			return false
		}
	}
	return true
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
// the existing binding; a tombstone mismatch fails closed with 502. This is
// the only native->custom authority crossing: the first send strips
// provider-bound Responses refs (native-issued previous_response_id/reasoning
// items) via the crossing-authority custom send while preserving ordinary
// history; the binding stays pending until its first HTTP 2xx, so every later
// attempt on a still-pending binding (via doCustomFallbackPinned) keeps
// crossing until success flips it to established. Bound established follow-ups
// keep custom-issued refs. No custom 400 replay is added, no custom retry is
// added, and custom errors (including 429/400/5xx/transport) return as-is
// without re-entering native. No error-text sniffing is used anywhere.
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
	resp, effectiveRoute, nextAttempts, takeErr := g.doCustomFallbackRequestCrossingAuthority(ctx, route, ex, hasEx, bodies, ids, current, stored, attemptOffset+attempts, true)
	if takeErr != nil {
		return resp, effectiveRoute, nextAttempts, true, takeErr
	}
	if resp == nil {
		return nil, effectiveRoute, nextAttempts, true, contextError("custom fallback transport failed")
	}
	if isCustomFallbackSuccess(resp, nil) && g != nil && g.scheduler != nil && g.scheduler.fallbacks != nil {
		g.scheduler.fallbacks.markEstablished(ids.Session)
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
		// Fallback recheck at loop start (defensive): a session already bound
		// to custom must not continue to native even when this call missed the
		// doUpstreamTiers entry fallback.get. Covers follower wakeup and the
		// deterministic missed-entry path exercised by tests via
		// doUnboundEstablishment. No pinClaim is held, so prepared
		// route/crossing/metadata are preserved via doCustomFallbackPinned.
		if binding, ok := g.lookupCustomFallbackBinding(ids); ok {
			return g.doCustomFallbackPinned(ctx, route, bodies, ids, binding, attemptOffset, extra...)
		}
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
		// Owner double-check before first native send (defensive): if a
		// concurrent session binding appeared between loop-start fallback
		// recheck and acquiring ownership (e.g. different-model owner for the
		// same session), avoid a stray native send by rechecking fallback now.
		// No cross-store atomicity is promised; on hit release the reservation
		// before serving custom exclusively. Kept as defense; no deterministic
		// repro is added for this racy window.
		if binding, ok := g.lookupCustomFallbackBinding(ids); ok {
			g.scheduler.pinRelease(ids.Session, route.ID, claim)
			return g.doCustomFallbackPinned(ctx, route, bodies, ids, binding, attemptOffset, extra...)
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
	var unboundDomains []unboundDomainEvidence
	var unboundRecovered bool
	if route.Anonymous {
		resp, err, used, recovered, pinHit, anonEvidence := g.doAnonymousUpstream(ctx, route, bodies, ids, attempts, extra...)
		attempts += used
		unboundDomains = append(unboundDomains, anonEvidence)
		if recovered {
			unboundRecovered = true
		}
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
		resp, err, used, recovered, pinHit, keyDomains := g.doKeyUpstream(ctx, keyRoute, bodies, ids, attempts, extra...)
		attempts += used
		unboundDomains = append(unboundDomains, keyDomains...)
		if recovered {
			unboundRecovered = true
		}
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
		// Native final fallback: only full unbound-domain object exhaustion
		// reaches the custom channel. Each frozen candidate must have live
		// unavailable evidence in this request (live 429, 401/403, L1-final
		// transport/408/425/5xx, or L1-final stream startup failure; 400
		// replays, ordinary 4xx, cancel/deadline, and committed streams never
		// count). The per-domain proof lives in
		// the frozen walks (doAnonymousUpstream/doKeyUpstream); this outer
		// step requires every collected domain (anonymous lane plus each
		// authenticated credential, never summed) to independently satisfy
		// the state-agnostic object-exhaustion gate. Pinned paths keep the
		// separate 429-only gate and never read this predicate.
		if ids.Session != "" && unboundDomainsExhaustedAllowCustom(unboundDomains, unboundRecovered, isContextCancelled(ctx), false) {
			if resp, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
				drainAndClose(lastResponse.Body)
				if takeErr != nil {
					return nil, eff, next, takeErr
				}
				if resp == nil {
					return nil, eff, next, contextError("custom fallback transport failed")
				}
				return resp, eff, next, nil
			}
		}
		return lastResponse, effectiveRoute, attempts, nil
	}
	// Transport-only exhaustion (all frozen objects failed with no response)
	// carries no lastResponse but the same per-domain proof still applies.
	// 400 replays return above, ordinary 4xx returns above with a response,
	// and cancel/deadline denies via the gate.
	if ids.Session != "" && len(unboundDomains) > 0 && unboundDomainsExhaustedAllowCustom(unboundDomains, unboundRecovered, isContextCancelled(ctx), false) {
		if resp, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
			if takeErr != nil {
				return nil, eff, next, takeErr
			}
			if resp == nil {
				return nil, eff, next, contextError("custom fallback transport failed")
			}
			return resp, eff, next, nil
		}
	}
	if lastErr == nil {
		lastErr = errors.New("no usable upstream route")
	}
	return nil, effectiveRoute, attempts, lastErr
}

// doPinnedUpstream serves a request bound to one binding. No cross-credential,
// cross-pool, or cross-channel fallback is attempted: the walk stays within
// the same credential+pool. The exact-400 same-target one-replay and the
// same-target transient rules are preserved. Active credential or target
// cooldowns before send fail locally without an upstream send; removed or
// unhealthy pinned resources fail locally with 502. Full 429 exhaustion of
// the binding's sendable proxies tries the custom final fallback.
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

// doPinnedAnonymous serves an established anonymous binding
// proxy-independently: the durable identity fixes tier, credential, pool,
// model, protocol, and authority; ProxyRaw is the generation-fenced mutable
// current selection. The walk covers all currently sendable proxies in the
// same pool with the same credential/model/protocol/authority, current first,
// in stable affinity order, sharing one proxy-independent route session and
// identical body bytes. Each proxy gets at most one 429 send (429 never
// retries same-target). Local proxy429 cooldown skips without new evidence.
// Only 429 walks to the next proxy; transport keeps the existing same-target
// transient retry only, and 400/401/403/408/425/ordinary 4xx/5xx never move.
// Any 2xx clears state and CAS-updates pin current. Full live 429
// exhaustion tries the custom final fallback; pre-cooled zero-send keeps
// native 429 with Retry-After and never tries custom; other terminals
// return as-is.
func (g *Gateway) doPinnedAnonymous(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, pin sessionPin, effectiveRoute modelRoute, baseURL string, protocol Protocol, body []byte, attemptOffset int, extra ...upstreamExtra) (*http.Response, modelRoute, int, error) {
	pool := g.pools[pin.Pool]
	if pool == nil || len(pool.items) == 0 {
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	ordered := affinityProxyOrder(pool, pin.CredID, pin.ProxyRaw)
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
		if until, _, ok := g.scheduler.suspectCooldownStatus(pin.Tier, pin.Pool, proxy.name); ok && until > nowNanos {
			continue
		}
		eligible = append(eligible, eligibleProxy{proxy: proxy, raw: proxy.name})
	}
	if len(eligible) == 0 {
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
		// Suspect exhaustion is 502, not 429: do not trigger custom fallback.
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	probe := targetCandidate{Tier: pin.Tier, CredID: pin.CredID, CredKey: anonymousZenKey, CredDisplay: anonymousCredentialID, CredIndex: -1, PoolName: pin.Pool, ProxyRaw: "", Model: pin.Model}
	scope := routeScopeForCandidate(baseURL, probe, protocol)
	routeSession := g.scheduler.routeSessionFor(ids.Session, scope)
	shaped, err := shapedAnonymousBody(body, protocol)
	if err != nil {
		return nil, effectiveRoute, attemptOffset, err
	}
	candBody, err := applyRouteSessionToBody(shaped, routeSession, protocol, false)
	if err != nil {
		return nil, effectiveRoute, attemptOffset, err
	}
	attempts := 0
	maxObservation := g.observationAttempts()
	interval := g.transientInterval()
	var last429 *http.Response
	live429 := 0
	consumed := false
	for idx, ep := range eligible {
		if isContextCancelled(ctx) {
			if last429 != nil {
				drainAndClose(last429.Body)
			}
			return nil, effectiveRoute, attemptOffset + attempts, ctx.Err()
		}
		identity := targetIdentity(pin.Tier, pin.CredID, pin.Pool, ep.raw, pin.Model)
		cand := targetCandidate{
			Tier: pin.Tier, CredKey: anonymousZenKey, CredID: pin.CredID, CredDisplay: anonymousCredentialID,
			CredIndex: -1, PoolName: pin.Pool, Proxy: ep.proxy,
			ProxyRaw: ep.raw, Model: pin.Model, Identity: identity,
		}
		attempts++
		syncAttemptMeta(ctx, pin.Tier, protocol, attemptOffset, attempts)
		out := g.executeAttempt(ctx, route, pin.Tier, baseURL, protocol, candBody, ids, cand, routeSession, "anonymous", "anonymous", true, attemptOffset+attempts)
		if out.BuildErr != nil {
			if last429 != nil {
				drainAndClose(last429.Body)
			}
			return nil, effectiveRoute, attemptOffset + attempts, out.BuildErr
		}
		resp := out.Resp
		sendErr := out.Err
		firstDiag := out.Diag
		if sendErr == nil && resp != nil && resp.StatusCode/100 == 2 {
			if last429 != nil {
				drainAndClose(last429.Body)
			}
			if ep.raw != pin.ProxyRaw && !isContextCancelled(ctx) {
				_, _ = g.scheduler.pinMoveCurrentCtx(ctx, ids.Session, route.ID, pin.Generation, ep.raw)
			}
			return resp, effectiveRoute, attemptOffset + attempts, nil
		}
		if isContextCancelled(ctx) {
			if last429 != nil {
				drainAndClose(last429.Body)
			}
			return resp, effectiveRoute, attemptOffset + attempts, sendErr
		}
		if handled, replayResp, replayErr, replayed := g.maybeReplayCandidate400(ctx, resp, sendErr, firstDiag, route, pin.Tier, baseURL, protocol, body, ids, cand, scope, routeSession, attemptOffset, attempts, func() {
			if last429 != nil {
				drainAndClose(last429.Body)
			}
		}); handled {
			if replayErr == nil && replayResp != nil && replayResp.StatusCode/100 == 2 && ep.raw != pin.ProxyRaw && !isContextCancelled(ctx) {
				_, _ = g.scheduler.pinMoveCurrentCtx(ctx, ids.Session, route.ID, pin.Generation, ep.raw)
			}
			return replayResp, effectiveRoute, attemptOffset + replayed, replayErr
		}
		if isSameTargetTransient(resp, sendErr) {
			loopRes := g.observeSameTargetTransient(ctx, attemptOutcome{Resp: resp, Err: sendErr, Diag: firstDiag}, pin.Tier, protocol, &attempts, attemptOffset, maxObservation, interval, func(monitorAttempt int) attemptOutcome {
				return g.executeAttempt(ctx, route, pin.Tier, baseURL, protocol, candBody, ids, cand, routeSession, "anonymous", "anonymous", true, monitorAttempt)
			})
			classified := classifyStableCause(ctx, loopRes)
			final := classified.Final
			stop := classified.Stop
			_ = stop
			if classified.Cause == stableCauseBuildFailure {
				if last429 != nil {
					drainAndClose(last429.Body)
				}
				return nil, effectiveRoute, attemptOffset + attempts, final.BuildErr
			}
			if classified.Cause == stableCauseSuccess {
				if last429 != nil {
					drainAndClose(last429.Body)
				}
				if ep.raw != pin.ProxyRaw && !isContextCancelled(ctx) {
					_, _ = g.scheduler.pinMoveCurrentCtx(ctx, ids.Session, route.ID, pin.Generation, ep.raw)
				}
				return final.Resp, effectiveRoute, attemptOffset + attempts, nil
			}
			if classified.Cause == stableCauseContext || classified.Cancelled {
				if stop == transientStopContext {
					if last429 != nil {
						drainAndClose(last429.Body)
					}
					// Exact HEAD pre-retry-cancel ownership: an intermediate cur
					// (cur != initial) was drained before return; the initial
					// itself was returned undrained. Sleep interruption returns
					// nil + ctx err with no body.
					if final.Resp != nil && final.Resp != classified.InitialResp {
						drainAndClose(final.Resp.Body)
					}
					return final.Resp, effectiveRoute, attemptOffset + attempts, final.Err
				}
				if last429 != nil {
					drainAndClose(last429.Body)
				}
				return final.Resp, effectiveRoute, attemptOffset + attempts, final.Err
			}
			if classified.Cause == stableCauseExact400 {
				if handled, replayResp, replayErr, replayed := g.maybeReplayCandidate400(ctx, final.Resp, final.Err, final.Diag, route, pin.Tier, baseURL, protocol, body, ids, cand, scope, routeSession, attemptOffset, attempts, func() {
					if last429 != nil {
						drainAndClose(last429.Body)
					}
				}); handled {
					if replayErr == nil && replayResp != nil && replayResp.StatusCode/100 == 2 && ep.raw != pin.ProxyRaw && !isContextCancelled(ctx) {
						_, _ = g.scheduler.pinMoveCurrentCtx(ctx, ids.Session, route.ID, pin.Generation, ep.raw)
					}
					return replayResp, effectiveRoute, attemptOffset + replayed, replayErr
				}
			}
			if classified.Cause == stableCauseLive429 {
				if last429 != nil {
					drainAndClose(last429.Body)
				}
				last429 = final.Resp
				live429++
				consumed = true
				continue
			}
			if isStreamStartupFailureErr(final.Err) {
				if last429 != nil {
					drainAndClose(last429.Body)
					last429 = nil
				}
				if ids.Session != "" && pinnedConsumptionAllowCustom(len(eligible), idx+1, consumed, isContextCancelled(ctx), nil, final.Err) {
					if resp2, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
						if takeErr != nil {
							return nil, eff, next, takeErr
						}
						if resp2 == nil {
							return nil, eff, next, contextError("custom fallback transport failed")
						}
						return resp2, eff, next, nil
					}
				}
				return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset + attempts, nil
			}
			if pinnedConsumptionAllowCustom(len(eligible), idx+1, consumed, isContextCancelled(ctx), final.Resp, final.Err) {
				if ids.Session != "" {
					if resp2, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
						if last429 != nil {
							drainAndClose(last429.Body)
							last429 = nil
						}
						if final.Resp != nil {
							drainAndClose(final.Resp.Body)
						}
						if takeErr != nil {
							return nil, eff, next, takeErr
						}
						if resp2 == nil {
							return nil, eff, next, contextError("custom fallback transport failed")
						}
						return resp2, eff, next, nil
					}
				}
			}
			if last429 != nil {
				drainAndClose(last429.Body)
				last429 = nil
			}
			// Exact HEAD transport ownership: a retry transport error carrying a
			// non-nil response was drained before return (old drained retryResp
			// before setting cur/return). An initial transport with no retry
			// (final == initial, e.g. maxObservation==1) was returned undrained.
			// Stable finals are never drained here; the caller returns them
			// for the envelope.
			if final.Err != nil && final.Resp != nil && final.Resp != classified.InitialResp {
				drainAndClose(final.Resp.Body)
			}
			return final.Resp, effectiveRoute, attemptOffset + attempts, final.Err
		}
		if sendErr == nil && resp != nil && resp.StatusCode == http.StatusTooManyRequests {
			if last429 != nil {
				drainAndClose(last429.Body)
			}
			last429 = resp
			live429++
			consumed = true
			continue
		}
		if pinnedConsumptionAllowCustom(len(eligible), idx+1, consumed, isContextCancelled(ctx), resp, sendErr) {
			if ids.Session != "" {
				if resp2, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
					if last429 != nil {
						drainAndClose(last429.Body)
					}
					if resp != nil {
						drainAndClose(resp.Body)
					}
					if takeErr != nil {
						return nil, eff, next, takeErr
					}
					if resp2 == nil {
						return nil, eff, next, contextError("custom fallback transport failed")
					}
					return resp2, eff, next, nil
				}
			}
		}
		if last429 != nil {
			drainAndClose(last429.Body)
		}
		return resp, effectiveRoute, attemptOffset + attempts, sendErr
	}
	// Pinned-anonymous live exhaustion: the loop above returns on every
	// non-429 outcome, so a non-nil last429 here means every frozen eligible
	// proxy supplied live 429 in this request (live429 == len(eligible)).
	// The pure count+terminal half owns the native 429 envelope; the full
	// shared gate owns only the custom attempt, so a full exhaustion under
	// cancel still keeps its 429 envelope without a custom send.
	terminalAnon := 0
	if last429 != nil {
		terminalAnon = last429.StatusCode
	}
	anonExhausted := last429 != nil && customTakeoverEligible(customTakeoverQualification{ObservedLive429: live429, Eligible: len(eligible), TerminalStatus: terminalAnon, Recovered400: false, Cancelled: false, Committed: false})
	if anonExhausted {
		if ids.Session != "" && customTakeoverEligible(customTakeoverQualification{ObservedLive429: live429, Eligible: len(eligible), TerminalStatus: terminalAnon, Recovered400: false, Cancelled: isContextCancelled(ctx), Committed: false}) {
			// Preserve the native 429 body for the custom path decision: the
			// takeover helper re-derives its own request, so drain here only
			// when actually handing off.
			if resp2, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
				drainAndClose(last429.Body)
				if takeErr != nil {
					return nil, eff, next, takeErr
				}
				if resp2 == nil {
					return nil, eff, next, contextError("custom fallback transport failed")
				}
				return resp2, eff, next, nil
			}
		}
		return last429, effectiveRoute, attemptOffset + attempts, nil
	}
	return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset + attempts, nil
}

// doPinnedAuth serves an established authenticated binding proxy-independently.
// The durable identity fixes tier, credential, pool, model, protocol, and
// authority; ProxyRaw is the current/preferred selection with generation
// fencing. The route session is proxy-independent so moves preserve the same
// upstream session value and body bytes. Within one request, only transport
// failure (after same-target L1 observation) or HTTP 429 may try the next
// eligible healthy proxy in the same pool/tier/credential/model/protocol/
// authority before client bytes. The L1 observation limit (normalized
// retry.max_attempts, including the first send) bounds only same-target
// stability observation; candidate traversal is bounded by the frozen eligible
// slice. 429 walks all currently sendable proxies until exhaustion.
// Credential429 is written only after every eligible proxy has
// returned live 429 in this request (last Retry-After); partial 429 never
// writes it and pre-cooled skips never count. Full live 429 exhaustion
// tries the custom final fallback; pre-cooled zero-send keeps native 429
// with Retry-After and never tries custom. Pre-existing cooling proxies
// are skipped before any
// send and never count as observed evidence. No moves on
// 400/401/403/408/425/ordinary 4xx/5xx. Success on an alternate updates only
// current/generation via CAS.
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
		if until, _, ok := g.scheduler.suspectCooldownStatus(pin.Tier, pin.Pool, proxy.name); ok && until > nowNanos {
			continue
		}
		eligible = append(eligible, eligibleProxy{proxy: proxy, raw: proxy.name})
	}
	if len(eligible) == 0 {
		// No eligible proxy: distinguish 429 exhaustion from 502. If any
		// proxy is under tier-429 cooldown, fast-fail 429 with max remaining
		// and keep the native envelope (zero-send pre-cooled 429 never
		// triggers custom: no native send, no proxyPosts, no fallback bind);
		// channel/suspect cooling alone fast-fails 502 (its 403/5xx status
		// is kept in the channel detail table, not as a pinned envelope);
		// otherwise 502 (unhealthy/removed).
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
	attempts := 0
	observed429 := make(map[string]time.Duration)
	var last429 *http.Response
	consumed := false
	discardLast429 := func() {
		if last429 != nil {
			drainAndClose(last429.Body)
			last429 = nil
		}
	}
	for idx, ep := range eligible {
		if isContextCancelled(ctx) {
			discardLast429()
			return nil, effectiveRoute, attemptOffset + attempts, ctx.Err()
		}
		identity := targetIdentity(pin.Tier, pin.CredID, pin.Pool, ep.raw, pin.Model)
		cand := targetCandidate{
			Tier: pin.Tier, CredKey: credKey, CredID: pin.CredID, CredDisplay: credDisplay,
			CredIndex: credIndex, PoolName: pin.Pool, Proxy: ep.proxy,
			ProxyRaw: ep.raw, Model: pin.Model, Identity: identity,
		}
		attempts++
		syncAttemptMeta(ctx, pin.Tier, protocol, attemptOffset, attempts)
		out := g.executeAttempt(ctx, route, pin.Tier, baseURL, protocol, candBody, ids, cand, routeSession, "key", credDisplay, false, attemptOffset+attempts)
		if out.BuildErr != nil {
			discardLast429()
			return nil, effectiveRoute, attemptOffset + attempts, out.BuildErr
		}
		resp := out.Resp
		sendErr := out.Err
		firstDiag := out.Diag
		firstStarted := out.Started
		if sendErr == nil && resp != nil && resp.StatusCode/100 == 2 {
			discardLast429()
			if ep.raw != pin.ProxyRaw && !isContextCancelled(ctx) {
				_, _ = g.scheduler.pinMoveCurrentCtx(ctx, ids.Session, route.ID, pin.Generation, ep.raw)
			}
			return resp, effectiveRoute, attemptOffset + attempts, nil
		}
		if isContextCancelled(ctx) {
			discardLast429()
			return resp, effectiveRoute, attemptOffset + attempts, sendErr
		}
		if handled, replayResp, replayErr, replayed := g.maybeReplayCandidate400(ctx, resp, sendErr, firstDiag, route, pin.Tier, baseURL, protocol, body, ids, cand, scope, routeSession, attemptOffset, attempts, discardLast429); handled {
			attempts = replayed
			if replayErr == nil && replayResp != nil && replayResp.StatusCode/100 == 2 && ep.raw != pin.ProxyRaw && !isContextCancelled(ctx) {
				_, _ = g.scheduler.pinMoveCurrentCtx(ctx, ids.Session, route.ID, pin.Generation, ep.raw)
			}
			return replayResp, effectiveRoute, attemptOffset + attempts, replayErr
		}
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		if status == http.StatusTooManyRequests && sendErr == nil {
			// 429 never consumes the L1 observation limit: the chain walks
			// all currently sendable proxies of this binding until
			// exhaustion.
			var retryAfter time.Duration
			if resp != nil {
				retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
			}
			if _, seen := observed429[ep.raw]; !seen {
				observed429[ep.raw] = retryAfter
			}
			// Full exhaustion of this binding's eligible set writes
			// credential429 (last Retry-After) and then tries custom;
			// partial exhaustion drains and continues. The shared L2
			// 429-only gate owns the exhaustion proof; identity/Started
			// fencing stays with the callers.
			if customTakeoverEligible(customTakeoverQualification{ObservedLive429: len(observed429), Eligible: len(eligible), TerminalStatus: status, Recovered400: false, Cancelled: isContextCancelled(ctx), Committed: false}) {
				_ = g.scheduler.noteCredential429Failure(pin.CredID, AttemptClassRateLimited, status, retryAfter, firstStarted)
				if ids.Session != "" {
					if resp2, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
						drainAndClose(resp.Body)
						discardLast429()
						if takeErr != nil {
							return nil, eff, next, takeErr
						}
						if resp2 == nil {
							return nil, eff, next, contextError("custom fallback transport failed")
						}
						return resp2, eff, next, nil
					}
				}
				discardLast429()
				return resp, effectiveRoute, attemptOffset + attempts, nil
			}
			if resp != nil {
				if last429 != nil {
					drainAndClose(last429.Body)
				}
				last429 = resp
			}
			consumed = true
			continue
		}
		if sendErr != nil || status == http.StatusRequestTimeout || status == 425 || (status >= 500 && status <= 599) {
			loopRes := g.observeSameTargetTransient(ctx, attemptOutcome{Resp: resp, Err: sendErr, Diag: firstDiag, Started: firstStarted}, pin.Tier, protocol, &attempts, attemptOffset, g.observationAttempts(), g.transientInterval(), func(monitorAttempt int) attemptOutcome {
				return g.executeAttempt(ctx, route, pin.Tier, baseURL, protocol, candBody, ids, cand, routeSession, "key", credDisplay, false, monitorAttempt)
			})
			classified := classifyStableCause(ctx, loopRes)
			final := classified.Final
			stop := classified.Stop
			_ = stop
			if classified.Cause == stableCauseBuildFailure {
				discardLast429()
				return nil, effectiveRoute, attemptOffset + attempts, final.BuildErr
			}
			if classified.Cause == stableCauseSuccess {
				discardLast429()
				if ep.raw != pin.ProxyRaw && !isContextCancelled(ctx) {
					_, _ = g.scheduler.pinMoveCurrentCtx(ctx, ids.Session, route.ID, pin.Generation, ep.raw)
				}
				return final.Resp, effectiveRoute, attemptOffset + attempts, nil
			}
			if pinnedAuthContextEarlyReturn(classified) {
				discardLast429()
				return final.Resp, effectiveRoute, attemptOffset + attempts, final.Err
			}
			if classified.Cause == stableCauseExact400 {
				if handled, replayResp, replayErr, replayed := g.maybeReplayCandidate400(ctx, final.Resp, final.Err, final.Diag, route, pin.Tier, baseURL, protocol, body, ids, cand, scope, routeSession, attemptOffset, attempts, discardLast429); handled {
					attempts = replayed
					if replayErr == nil && replayResp != nil && replayResp.StatusCode/100 == 2 && ep.raw != pin.ProxyRaw && !isContextCancelled(ctx) {
						_, _ = g.scheduler.pinMoveCurrentCtx(ctx, ids.Session, route.ID, pin.Generation, ep.raw)
					}
					return replayResp, effectiveRoute, attemptOffset + attempts, replayErr
				}
			}
			if classified.Cause == stableCauseLive429 {
				retryAfter := parseRetryAfter(final.Resp.Header.Get("Retry-After"))
				if _, seen := observed429[ep.raw]; !seen {
					observed429[ep.raw] = retryAfter
				}
				finalStatus := 0
				if final.Resp != nil {
					finalStatus = final.Resp.StatusCode
				}
				if customTakeoverEligible(customTakeoverQualification{ObservedLive429: len(observed429), Eligible: len(eligible), TerminalStatus: finalStatus, Recovered400: false, Cancelled: isContextCancelled(ctx), Committed: false}) {
					_ = g.scheduler.noteCredential429Failure(pin.CredID, AttemptClassRateLimited, final.Resp.StatusCode, retryAfter, final.Started)
					if ids.Session != "" {
						if resp2, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
							drainAndClose(final.Resp.Body)
							discardLast429()
							if takeErr != nil {
								return nil, eff, next, takeErr
							}
							if resp2 == nil {
								return nil, eff, next, contextError("custom fallback transport failed")
							}
							return resp2, eff, next, nil
						}
					}
					discardLast429()
					return final.Resp, effectiveRoute, attemptOffset + attempts, nil
				}
				// Exact HEAD order (HEAD gateway.go:2372 before cur==nil continue):
				// full exhaustion above precedes poisoning; only the partial path
				// with an initial stream-startup sentinel poisons. Final is 429
				// here, so sawStreamSentinel() reduces to initial-sentinel only;
				// intermediate-only sentinels never poison. Preserve the
				// observed429 write, drain both final and prior last429, and
				// return pin-local 502 with no next-proxy/custom/credential429.
				if classified.StreamSentinel {
					drainAndClose(final.Resp.Body)
					discardLast429()
					return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset + attempts, nil
				}
				if last429 != nil {
					drainAndClose(last429.Body)
				}
				last429 = final.Resp
				consumed = true
				continue
			}
			// Exact HEAD stream-startup poisoning: either the initial send or
			// the final outcome being the sentinel yields local502, matching
			// old isStreamStartupFailureErr(curErr)||isStreamStartupFailureErr(sendErr).
			// Intermediate-only sentinels never poison. No last-wins, no
			// de-poisoning.
			if classified.StreamSentinel {
				if ids.Session != "" && pinnedConsumptionAllowCustom(len(eligible), idx+1, consumed, isContextCancelled(ctx), nil, final.Err) {
					if resp2, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
						discardLast429()
						if takeErr != nil {
							return nil, eff, next, takeErr
						}
						if resp2 == nil {
							return nil, eff, next, contextError("custom fallback transport failed")
						}
						return resp2, eff, next, nil
					}
				}
				discardLast429()
				return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset + attempts, nil
			}
			// Exact HEAD mixed 503->transport-nil at the observation limit:
			// old returned the stale initial response (initial 503, nil err)
			// where the final is a nil-body transport error, not nil+transport.
			if stop == transientStopObservationLimit && final.Err != nil && final.Resp == nil && classified.InitialErr == nil && classified.InitialResp != nil {
				if ids.Session != "" && pinnedConsumptionAllowCustom(len(eligible), idx+1, consumed, isContextCancelled(ctx), classified.InitialResp, classified.InitialErr) {
					if resp2, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
						discardLast429()
						drainAndClose(classified.InitialResp.Body)
						if takeErr != nil {
							return nil, eff, next, takeErr
						}
						if resp2 == nil {
							return nil, eff, next, contextError("custom fallback transport failed")
						}
						return resp2, eff, next, nil
					}
				}
				discardLast429()
				return classified.InitialResp, effectiveRoute, attemptOffset + attempts, classified.InitialErr
			}
			if final.Resp != nil && classified.StillTransient {
				if final.Err != nil {
					if idx != len(eligible)-1 {
						if final.Resp != nil {
							drainAndClose(final.Resp.Body)
						}
						discardLast429()
						consumed = true
						continue
					}
					if ids.Session != "" && pinnedConsumptionAllowCustom(len(eligible), idx+1, consumed, isContextCancelled(ctx), final.Resp, final.Err) {
						if resp2, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
							if final.Resp != nil {
								drainAndClose(final.Resp.Body)
							}
							discardLast429()
							if takeErr != nil {
								return nil, eff, next, takeErr
							}
							if resp2 == nil {
								return nil, eff, next, contextError("custom fallback transport failed")
							}
							return resp2, eff, next, nil
						}
					}
					if final.Resp != nil {
						drainAndClose(final.Resp.Body)
					}
					discardLast429()
					break
				}
				// 5xx/408/425 never move to another proxy: return as-is.
				if ids.Session != "" && pinnedConsumptionAllowCustom(len(eligible), idx+1, consumed, isContextCancelled(ctx), final.Resp, final.Err) {
					if resp2, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
						discardLast429()
						drainAndClose(final.Resp.Body)
						if takeErr != nil {
							return nil, eff, next, takeErr
						}
						if resp2 == nil {
							return nil, eff, next, contextError("custom fallback transport failed")
						}
						return resp2, eff, next, nil
					}
				}
				discardLast429()
				return final.Resp, effectiveRoute, attemptOffset + attempts, final.Err
			}
			if final.Resp != nil {
				if ids.Session != "" && pinnedConsumptionAllowCustom(len(eligible), idx+1, consumed, isContextCancelled(ctx), final.Resp, final.Err) {
					if resp2, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
						discardLast429()
						drainAndClose(final.Resp.Body)
						if takeErr != nil {
							return nil, eff, next, takeErr
						}
						if resp2 == nil {
							return nil, eff, next, contextError("custom fallback transport failed")
						}
						return resp2, eff, next, nil
					}
				}
				discardLast429()
				return final.Resp, effectiveRoute, attemptOffset + attempts, final.Err
			}
			if sendErr != nil && !isStreamStartupFailureErr(sendErr) {
				if idx == len(eligible)-1 {
					if ids.Session != "" && pinnedConsumptionAllowCustom(len(eligible), idx+1, consumed, isContextCancelled(ctx), nil, sendErr) {
						if resp2, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
							if resp != nil {
								drainAndClose(resp.Body)
							}
							discardLast429()
							if takeErr != nil {
								return nil, eff, next, takeErr
							}
							if resp2 == nil {
								return nil, eff, next, contextError("custom fallback transport failed")
							}
							return resp2, eff, next, nil
						}
					}
					if resp != nil {
						drainAndClose(resp.Body)
					}
					discardLast429()
					break
				}
				if resp != nil {
					drainAndClose(resp.Body)
				}
				discardLast429()
				consumed = true
				continue
			}
			discardLast429()
			return final.Resp, effectiveRoute, attemptOffset + attempts, final.Err
		}
		// 401/403/ordinary 4xx: no cross-proxy moves. Only 401/403 after a
		// consume action on the last eligible may take over custom; ordinary
		// 4xx and single/unconsumed finals stay faithful.
		if ids.Session != "" && pinnedConsumptionAllowCustom(len(eligible), idx+1, consumed, isContextCancelled(ctx), resp, sendErr) {
			if resp2, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
				if last429 != nil {
					drainAndClose(last429.Body)
				}
				if resp != nil {
					drainAndClose(resp.Body)
				}
				if takeErr != nil {
					return nil, eff, next, takeErr
				}
				if resp2 == nil {
					return nil, eff, next, contextError("custom fallback transport failed")
				}
				return resp2, eff, next, nil
			}
		}
		if last429 != nil {
			drainAndClose(last429.Body)
		}
		return resp, effectiveRoute, attemptOffset + attempts, sendErr
	}
	terminalPinned := 0
	if last429 != nil {
		terminalPinned = last429.StatusCode
	}
	// Strict 429 exhaustion: every proxy of the frozen eligible set
	// returned a distinct live 429 in this request. Partial 429 mixed
	// with any non-429 transport/4xx/5xx outcome never reaches here
	// with a full set (non-429 paths discard last429 and return or
	// continue without a stale envelope). The pure count+terminal half
	// owns the native 429 envelope; the full gate (plus cancel/deadline,
	// committed, replay-final route state) owns only the custom attempt,
	// so a full exhaustion under cancel still keeps its 429 envelope
	// without a custom send.
	exhaustionOwed := customTakeoverEligible(customTakeoverQualification{ObservedLive429: len(observed429), Eligible: len(eligible), TerminalStatus: terminalPinned, Recovered400: false, Cancelled: false, Committed: false})
	if exhaustionOwed {
		if ids.Session != "" && customTakeoverEligible(customTakeoverQualification{ObservedLive429: len(observed429), Eligible: len(eligible), TerminalStatus: terminalPinned, Recovered400: false, Cancelled: isContextCancelled(ctx), Committed: false}) {
			if resp2, eff, next, handled, takeErr := g.maybeTakeoverCustomFallback(ctx, route, bodies, ids, attemptOffset, attempts, extra...); handled {
				drainAndClose(last429.Body)
				if takeErr != nil {
					return nil, eff, next, takeErr
				}
				if resp2 == nil {
					return nil, eff, next, contextError("custom fallback transport failed")
				}
				return resp2, eff, next, nil
			}
		}
		return last429, effectiveRoute, attemptOffset + attempts, nil
	}
	if last429 != nil {
		// Stale partial 429 after a non-429 transport/terminal event: never a
		// 429 exhaustion, never custom. Drop the stale envelope and report a
		// transport-neutral 502 with the exact consumed attempt count instead
		// of forging a 429.
		drainAndClose(last429.Body)
		last429 = nil
	}
	// Exhausted eligible proxies after transport moves. No 429 envelope is owed
	// here (429 exhaustion returned above); report 502 with the exact
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
	// Streaming inference defers success commitment until the startup gate
	// reaches commit. For non-stream or non-2xx, handle immediately.
	if isStreamContext(ctx) && err == nil && resp != nil && resp.StatusCode/100 == 2 {
		class = attemptClassification{Class: AttemptClassSuccess}
		// Defer scheduler success and observability until gate commit; the
		// caller will invoke applyStreamSuccess and record on commit, or
		// noteStreamStartupFailure on pre-commit failure.
		if resp.StatusCode == http.StatusBadRequest {
			diag = peek400Diag(resp)
		}
		return resp, err, duration, class, diag, startedNanos, nil
	}
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
// only the client session; the route session is proxy-independent and shared
// across candidates, stamped into the already-present body session fields.
// The canonical tier body is never mutated. Each same-target transient
// (transport error, 408/425, 500-599, stream startup failure) is observed up
// to the unique L1 limit (normalized retry.max_attempts, including the first
// send) on the same target with the same route session and body. The first
// exact HTTP 400 on any candidate instead replays exactly once on the same
// candidate with the same route session (same request ID, attempt +1, same
// target/protocol/proxy, always before any client bytes); the replay result
// is final for the whole route. Ordinary 4xx ends the anonymous channel
// (authenticated tiers may still run); other retryable outcomes advance to
// the next proxy. Cancel ends the channel immediately without further sends
// or state changes. The list is never truncated by a candidate-send budget;
// traversal is bounded by the frozen candidate slice.
func (g *Gateway) doAnonymousUpstream(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, attemptOffset int, extra ...upstreamExtra) (*http.Response, error, int, bool, bool, unboundDomainEvidence) {
	var lastResponse *http.Response
	var lastErr error
	anonUnavailable := map[string]struct{}{}
	anonEvidence := func(entered bool, frozen int, recovered bool) unboundDomainEvidence {
		return unboundDomainEvidence{Domain: "anonymous", Entered: entered, Frozen: frozen, Unavailable: len(anonUnavailable), Recovered400: recovered}
	}
	anonEntered := false
	markAnonUnavailable := func(proxyRaw string, resp *http.Response, err error) {
		if proxyRaw == "" {
			return
		}
		if isContextCancelled(ctx) {
			return
		}
		if _, ok := anonUnavailable[proxyRaw]; ok {
			return
		}
		if unboundObjectUnavailable(resp, err) {
			anonUnavailable[proxyRaw] = struct{}{}
		}
	}
	if !g.cfg.Anonymous {
		return nil, errors.New("anonymous channel is disabled"), 0, false, false, anonEvidence(false, 0, false)
	}
	pool := g.pools[g.cfg.ProxyRouting.Anonymous]
	body := bodies[TierZen]
	if len(body) == 0 {
		return nil, errors.New("no prepared Zen request body"), 0, false, false, anonEvidence(false, 0, false)
	}
	now := time.Now().UnixNano()
	cands := g.scheduler.orderCandidates(g.scheduler.buildAnonymousCandidates(pool, route.ID, now), ids.Session)
	if len(cands) == 0 {
		// Empty anonymous candidate set: distinguish transport-suspect-only
		// exhaustion (local 502, never custom) from proxy429 exhaustion
		// (local 429 with Retry-After and existing custom fallback
		// semantics). Channel-only or health/unknown-model emptiness also
		// maps to 502. Do not let a stale historic 429 mask a later
		// transport/non-429 outcome: only an actively cooling proxy429 with a
		// future deadline triggers 429.
		var latest int64
		if pool != nil {
			for _, p := range pool.items {
				if p == nil {
					continue
				}
				if until, _, ok := g.scheduler.proxy429CooldownStatus(TierZen, pool.name, p.name); ok && until > latest {
					latest = until
				}
			}
		}
		if latest > time.Now().UnixNano() {
			return pinLocalResponse(http.StatusTooManyRequests, pinRetryAfterSeconds(latest, time.Now()), "upstream temporarily unavailable"), nil, 0, false, false, anonEvidence(false, 0, false)
		}
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), nil, 0, false, false, anonEvidence(false, 0, false)
	}
	anonFrozen := len(cands)
	attempts := 0
	maxObservation := g.observationAttempts()
	interval := g.transientInterval()
	for idx, cand := range cands {
		if isContextCancelled(ctx) {
			if lastResponse != nil {
				return lastResponse, nil, attempts, false, false, anonEvidence(anonEntered, anonFrozen, false)
			}
			if lastErr != nil {
				return nil, lastErr, attempts, false, false, anonEvidence(anonEntered, anonFrozen, false)
			}
			return nil, ctx.Err(), attempts, false, false, anonEvidence(anonEntered, anonFrozen, false)
		}
		// Defense in depth: before a later fallback candidate sends another
		// target, stop when a pin appeared and let the outer route through
		// the pinned target. Same-target L1 observation/replay below stays
		// on the same candidate and never checks here.
		if idx > 0 && ids.Session != "" && route.ID != "" {
			if _, ok := g.scheduler.pinGet(ids.Session, route.ID); ok {
				return lastResponse, lastErr, attempts, false, true, anonEvidence(anonEntered, anonFrozen, false)
			}
		}
		if lastResponse != nil {
			drainAndClose(lastResponse.Body)
			lastResponse = nil
		}
		scope := routeScopeForCandidate(g.cfg.Upstream.Zen, cand, route.Protocol)
		routeSession := g.scheduler.routeSessionFor(ids.Session, scope)
		shaped, err := shapedAnonymousBody(body, route.Protocol)
		if err != nil {
			attempts++
			syncAttemptMeta(ctx, TierZen, route.Protocol, attemptOffset, attempts)
			return nil, err, attempts, false, false, anonEvidence(anonEntered, anonFrozen, false)
		}
		candBody, err := applyRouteSessionToBody(shaped, routeSession, route.Protocol, false)
		if err != nil {
			attempts++
			syncAttemptMeta(ctx, TierZen, route.Protocol, attemptOffset, attempts)
			return nil, err, attempts, false, false, anonEvidence(anonEntered, anonFrozen, false)
		}
		attempts++
		syncAttemptMeta(ctx, TierZen, route.Protocol, attemptOffset, attempts)
		out := g.executeAttempt(ctx, route, TierZen, g.cfg.Upstream.Zen, route.Protocol, candBody, ids, cand, routeSession, "anonymous", "anonymous", true, attemptOffset+attempts)
		if out.BuildErr != nil {
			return nil, out.BuildErr, attempts, false, false, anonEvidence(anonEntered, anonFrozen, false)
		}
		anonEntered = true
		resp := out.Resp
		err = out.Err
		firstDiag := out.Diag
		if err == nil && resp != nil && resp.StatusCode == http.StatusTooManyRequests {
			markAnonUnavailable(cand.ProxyRaw, resp, nil)
		}
		if err == nil && resp != nil && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) {
			markAnonUnavailable(cand.ProxyRaw, resp, nil)
		}
		if err == nil && resp != nil && resp.StatusCode/100 == 2 {
			if isStreamContext(ctx) {
				g.logger.Debug("anonymous upstream stream committed", "component", "upstream", "event", "anonymous_stream_committed", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "status", resp.StatusCode)
			} else {
				g.logger.Debug("anonymous upstream accepted request", "component", "upstream", "event", "anonymous_attempt_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "status", resp.StatusCode)
			}
			g.bindSessionPinCtx(ctx, ids.Session, route.ID, TierZen, anonymousSchedulerCredentialID, cand.PoolName, cand.ProxyRaw, route.Protocol, normalizeRouteAuthority(g.cfg.Upstream.Zen))
			return resp, nil, attempts, false, false, anonEvidence(anonEntered, anonFrozen, false)
		}
		if isContextCancelled(ctx) {
			return resp, err, attempts, false, false, anonEvidence(anonEntered, anonFrozen, false)
		}
		if handled, replayResp, replayErr, replayed := g.maybeReplayCandidate400(ctx, resp, err, firstDiag, route, TierZen, g.cfg.Upstream.Zen, route.Protocol, body, ids, cand, scope, routeSession, attemptOffset, attempts, nil); handled {
			attempts = replayed
			if replayResp != nil || replayErr != nil {
				return replayResp, replayErr, attempts, true, false, anonEvidence(anonEntered, anonFrozen, true)
			}
			// Replay suppressed (cancelled context): preserve terminal 400.
			termArgs := []any{"component", "upstream", "event", "anonymous_attempt_terminal", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "status", resp.StatusCode}
			termArgs = append(termArgs, diagLogArgs(firstDiag, "")...)
			g.logger.Debug("anonymous upstream returned route-terminal 400; stopping anonymous phase", termArgs...)
			return resp, nil, attempts, false, false, anonEvidence(anonEntered, anonFrozen, false)
		}
		if isSameTargetTransient(resp, err) {
			loopRes := g.observeSameTargetTransient(ctx, attemptOutcome{Resp: resp, Err: err, Diag: firstDiag}, TierZen, route.Protocol, &attempts, attemptOffset, maxObservation, interval, func(monitorAttempt int) attemptOutcome {
				return g.executeAttempt(ctx, route, TierZen, g.cfg.Upstream.Zen, route.Protocol, candBody, ids, cand, routeSession, "anonymous", "anonymous", true, monitorAttempt)
			})
			classified := classifyStableCause(ctx, loopRes)
			final := classified.Final
			stop := classified.Stop
			_ = stop
			if classified.Cause == stableCauseBuildFailure {
				return nil, final.BuildErr, attempts, false, false, anonEvidence(anonEntered, anonFrozen, false)
			}
			if classified.Cause == stableCauseSuccess {
				if isStreamContext(ctx) {
					g.logger.Debug("anonymous transient retry stream committed", "component", "upstream", "event", "anonymous_transient_retry_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "status", final.Resp.StatusCode)
				} else {
					g.logger.Debug("anonymous transient retry succeeded", "component", "upstream", "event", "anonymous_transient_retry_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "status", final.Resp.StatusCode)
				}
				g.bindSessionPinCtx(ctx, ids.Session, route.ID, TierZen, anonymousSchedulerCredentialID, cand.PoolName, cand.ProxyRaw, route.Protocol, normalizeRouteAuthority(g.cfg.Upstream.Zen))
				return final.Resp, nil, attempts, false, false, anonEvidence(anonEntered, anonFrozen, false)
			}
			if classified.Cause == stableCauseContext || classified.Cancelled {
				return final.Resp, final.Err, attempts, false, false, anonEvidence(anonEntered, anonFrozen, false)
			}
			if classified.Cause == stableCauseExact400 {
				if handled, replayResp, replayErr, replayed := g.maybeReplayCandidate400(ctx, final.Resp, final.Err, final.Diag, route, TierZen, g.cfg.Upstream.Zen, route.Protocol, body, ids, cand, scope, routeSession, attemptOffset, attempts, nil); handled {
					attempts = replayed
					if replayResp != nil || replayErr != nil {
						return replayResp, replayErr, attempts, true, false, anonEvidence(anonEntered, anonFrozen, true)
					}
					return final.Resp, nil, attempts, false, false, anonEvidence(anonEntered, anonFrozen, false)
				}
			}
			if classified.Cause == stableCauseOrdinaryRejection {
				g.logger.Debug("anonymous transient retry hit ordinary rejection; ending anonymous channel", "component", "upstream", "event", "anonymous_attempt_rejected", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "status", final.Resp.StatusCode)
				return final.Resp, nil, attempts, false, false, anonEvidence(anonEntered, anonFrozen, false)
			}
			if classified.Cause == stableCauseLive429 {
				markAnonUnavailable(cand.ProxyRaw, final.Resp, nil)
			} else if stop == transientStopObservationLimit || stop == transientStopStable {
				// L1-final transport/408/425/5xx, stable 401/403, or L1-final
				// stream startup failure after observation proves the object
				// unavailable (each frozen candidate once; ordinary 4xx/400
				// already returned above, cancel already returned,
				// BuildErr/committed never reach here).
				markAnonUnavailable(cand.ProxyRaw, final.Resp, final.Err)
			}
			lastResponse = final.Resp
			lastErr = final.Err
			if final.Err != nil {
				g.logger.Debug("anonymous transient retry still failing; trying the next proxy", "component", "upstream", "event", "anonymous_attempt_transport_failed", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "error", final.Err)
			} else if final.Resp != nil {
				g.logger.Debug("anonymous transient retry returned an error response; trying the next proxy", "component", "upstream", "event", "anonymous_attempt_response_failed", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "status", final.Resp.StatusCode)
			} else {
				g.logger.Debug("anonymous transient retry still failing; trying the next proxy", "component", "upstream", "event", "anonymous_attempt_transport_failed", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "error", final.Err)
			}
			continue
		}
		if isOrdinaryClientRejection(resp, err) {
			g.logger.Debug("anonymous upstream rejected a non-retryable request; ending anonymous channel", "component", "upstream", "event", "anonymous_attempt_rejected", "request_id", ids.Request, "attempt", attempts, "tier", TierZen, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", "anonymous", "channel", "anonymous", "anonymous", true, "proxy", redactURL(cand.Proxy.name), "status", resp.StatusCode)
			return resp, nil, attempts, false, false, anonEvidence(anonEntered, anonFrozen, false)
		}
		if resp != nil && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) {
			markAnonUnavailable(cand.ProxyRaw, resp, nil)
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
		return lastResponse, nil, attempts, false, false, anonEvidence(anonEntered, anonFrozen, false)
	}
	if lastErr == nil {
		lastErr = errors.New("no healthy anonymous proxies available")
	}
	return nil, lastErr, attempts, false, false, anonEvidence(anonEntered, anonFrozen, false)
}

// maybeReplayCandidate400 centralizes the repeated mechanical exact-400 entry
// condition and call without absorbing caller-owned recovery policy. It takes
// the observed attempt outcome (response/error/diag) plus the existing
// replayCandidate400 arguments, determines exact HTTP 400 via
// isRouteTerminalBadRequest, runs the caller-owned preReplay action (if any),
// and only then calls replayCandidate400. preReplay exists so pinned callers
// can execute their last429/discardLast429 draining before the replay send
// performs replay I/O, exactly as HEAD a733a20 ordered drain-before-replay.
// It returns handled plus the replay response/error/updated attempt count;
// when not an exact 400 it returns handled=false with the input attempt count
// and never runs preReplay. All other caller-owned policy stays outside:
// pinMoveCurrent after replay 2xx on a pinned alternate proxy, unbound
// Recovered400 evidence and credRecovered mutations,
// suppressed nil/nil preservation of the original 400, route-terminal returns,
// and attemptOffset return shapes.
func (g *Gateway) maybeReplayCandidate400(ctx context.Context, resp *http.Response, err error, diag badRequestDiag, route modelRoute, tier Tier, baseURL string, protocol Protocol, canonical []byte, ids requestIDs, cand targetCandidate, scope routeSessionScope, routeSession string, attemptOffset, attempts int, preReplay func()) (handled bool, replayResp *http.Response, replayErr error, replayed int) {
	if !isRouteTerminalBadRequest(resp, err) {
		return false, nil, nil, attempts
	}
	if preReplay != nil {
		preReplay()
	}
	replayResp, replayErr, replayed = g.replayCandidate400(ctx, route, tier, baseURL, protocol, canonical, ids, cand, scope, routeSession, resp, diag, attemptOffset, attempts)
	return true, replayResp, replayErr, replayed
}

// replayCandidate400 performs the single same-target 400 recovery replay: it
// keeps the same route session, rebuilds a fresh candidate body from the
// frozen canonical body (overwriting present session fields with the same
// wire session and, for Responses, dropping stale previous_response_id /
// reasoning refs), and re-sends on the identical credential/proxy/protocol
// with the same request ID and attempt number +1. The first 400 is neutral
// and its body is drained before the replay. The first 400 body was already
// read once bounded for diagnostics by sendUpstreamOnce and restored, so
// draining here preserves control flow without a second network read. The
// replay 400 body (when 400) is likewise read once bounded and restored so
// the final client envelope keeps the substantively same upstream message
// with no extra send. Recovery never runs after client bytes have been
// written: all callers invoke it before returning the upstream response
// downstream. No route-session override is created; the wire session is
// byte-identical across the replay.
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
	newSession := observed
	_ = scope
	shapedCanonical := canonical
	if anonymous {
		shaped, shapeErr := shapedAnonymousBody(canonical, protocol)
		if shapeErr != nil {
			if firstResp != nil {
				drainAndClose(firstResp.Body)
			}
			return nil, shapeErr, attempts
		}
		shapedCanonical = shaped
	}
	replayBody, cleanup, err := applyRouteSessionToBodyWithReport(shapedCanonical, newSession, protocol, true)
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
		g.bindSessionPinCtx(ctx, ids.Session, route.ID, tier, cand.CredID, cand.PoolName, cand.ProxyRaw, protocol, normalizeRouteAuthority(baseURL))
	}
	return resp, err, attempts
}

func (g *Gateway) doKeyUpstream(ctx context.Context, route modelRoute, bodies map[Tier][]byte, ids requestIDs, attemptOffset int, extra ...upstreamExtra) (*http.Response, error, int, bool, bool, []unboundDomainEvidence) {
	var lastResponse *http.Response
	var lastErr error
	creds := g.credentials()
	poolName := g.authPoolName()
	baseURL := g.cfg.Upstream.Zen
	pool := g.pools[poolName]
	buildEmptyKeyDomains := func() []unboundDomainEvidence {
		domains := make([]unboundDomainEvidence, 0, len(creds))
		for _, cred := range creds {
			domains = append(domains, unboundDomainEvidence{Domain: cred.id, Entered: false, Frozen: 0, Recovered400: false})
		}
		return domains
	}
	if len(creds) == 0 {
		return nil, fmt.Errorf("no %s nodes configured", route.Tier), 0, false, false, nil
	}
	body := bodies[route.Tier]
	if len(body) == 0 {
		return nil, fmt.Errorf("no prepared %s request body", route.Tier), 0, false, false, buildEmptyKeyDomains()
	}
	// Freeze the candidate order at request start; failover walks the frozen
	// list without dynamic re-sorting. The L1 observation limit (normalized
	// retry.max_attempts, including the first send) bounds only same-target
	// stability observation; candidate traversal is bounded by the frozen
	// credential x proxy slice. 429 walks all currently sendable candidates
	// until exhaustion. Credential429 is written only after every
	// eligible proxy for one credential has returned live 429 (last
	// Retry-After); partial 429 never writes it and pre-cooled skips never
	// count. The 400 recovery is always allowed once extra on the same target.
	// Auth ordering is credential-soft-affinity: credential groups by session
	// HRW, proxies within each credential by deterministic affinity
	// (session/model independent). The route session is proxy-independent.
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
			return pinLocalResponse(http.StatusTooManyRequests, pinRetryAfterSeconds(latest429, time.Now()), "upstream temporarily unavailable"), nil, 0, false, false, buildEmptyKeyDomains()
		}
	}
	cands := g.scheduler.orderCandidates(g.scheduler.buildAuthCandidates(route.Tier, creds, pool, route.ID, now), ids.Session)
	if len(cands) == 0 {
		// Empty authenticated candidate set: same distinction as anonymous
		// — transport-suspect-only exhaustion maps to local 502 (never
		// custom), while an active proxy429 deadline maps to local 429 with
		// Retry-After and existing custom fallback semantics. Preserve the
		// credential429 pre-check above and all-suspect 502 behavior; do not
		// let a stale historic 429 mask a later transport/non-429 outcome.
		var latest int64
		if pool != nil {
			for _, p := range pool.items {
				if p == nil {
					continue
				}
				if until, _, ok := g.scheduler.proxy429CooldownStatus(route.Tier, pool.name, p.name); ok && until > latest {
					latest = until
				}
			}
		}
		if latest > time.Now().UnixNano() {
			return pinLocalResponse(http.StatusTooManyRequests, pinRetryAfterSeconds(latest, time.Now()), "upstream temporarily unavailable"), nil, 0, false, false, buildEmptyKeyDomains()
		}
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), nil, 0, false, false, buildEmptyKeyDomains()
	}
	// Exhaustion evidence per credential for this unbound request: live 429
	// proxies observed plus the last Retry-After/started for the eventual
	// credential429 write. Only same-credential live 429s on the frozen
	// eligible set count; pre-cooled skips never enter the frozen list.
	cred429Evidence := make(map[string]map[string]time.Duration)
	cred429LastRetry := make(map[string]time.Duration)
	cred429LastStarted := make(map[string]int64)
	credEligibleCount := make(map[string]int)
	for _, cand := range cands {
		credEligibleCount[cand.CredID]++
	}
	credEntered := make(map[string]bool)
	credRecovered := make(map[string]bool)
	credUnavailable := make(map[string]map[string]struct{})
	markCredUnavailable := func(credID, proxyRaw string, resp *http.Response, err error) {
		if credID == "" || proxyRaw == "" {
			return
		}
		if isContextCancelled(ctx) {
			return
		}
		set, ok := credUnavailable[credID]
		if !ok {
			set = make(map[string]struct{})
			credUnavailable[credID] = set
		}
		if _, done := set[proxyRaw]; done {
			return
		}
		if unboundObjectUnavailable(resp, err) {
			set[proxyRaw] = struct{}{}
		}
	}
	buildKeyDomains := func() []unboundDomainEvidence {
		domains := make([]unboundDomainEvidence, 0, len(creds))
		for _, cred := range creds {
			frozen := credEligibleCount[cred.id]
			entered := credEntered[cred.id]
			domains = append(domains, unboundDomainEvidence{Domain: cred.id, Entered: entered, Frozen: frozen, Unavailable: len(credUnavailable[cred.id]), Recovered400: credRecovered[cred.id]})
		}
		return domains
	}
	attempts := 0
	maxObservation := g.observationAttempts()
	interval := g.transientInterval()
	for idx, cand := range cands {
		if isContextCancelled(ctx) {
			if lastResponse != nil {
				return lastResponse, nil, attempts, false, false, buildKeyDomains()
			}
			if lastErr != nil {
				return nil, lastErr, attempts, false, false, buildKeyDomains()
			}
			return nil, ctx.Err(), attempts, false, false, buildKeyDomains()
		}
		// Defense in depth: before a later fallback candidate sends another
		// target, stop when a pin appeared and let the outer route through
		// the pinned target. Same-target L1 observation/replay below stays
		// on the same candidate and never checks here.
		if idx > 0 && ids.Session != "" && route.ID != "" {
			if _, ok := g.scheduler.pinGet(ids.Session, route.ID); ok {
				return lastResponse, lastErr, attempts, false, true, buildKeyDomains()
			}
		}
		if lastResponse != nil {
			drainAndClose(lastResponse.Body)
			lastResponse = nil
		}
		scope := routeScopeForCandidate(baseURL, cand, route.Protocol)
		routeSession := g.scheduler.routeSessionFor(ids.Session, scope)
		candBody, err := applyRouteSessionToBody(body, routeSession, route.Protocol, false)
		if err != nil {
			attempts++
			syncAttemptMeta(ctx, route.Tier, route.Protocol, attemptOffset, attempts)
			return nil, err, attempts, false, false, buildKeyDomains()
		}
		// Keep the request-level trace synchronized with the attempt that is
		// about to be sent. Only the redacted key suffix is retained.
		attempts++
		syncAttemptMeta(ctx, route.Tier, route.Protocol, attemptOffset, attempts)
		out := g.executeAttempt(ctx, route, route.Tier, baseURL, route.Protocol, candBody, ids, cand, routeSession, "key", cand.CredDisplay, false, attemptOffset+attempts)
		if out.BuildErr != nil {
			return nil, out.BuildErr, attempts, false, false, buildKeyDomains()
		}
		credEntered[cand.CredID] = true
		resp := out.Resp
		err = out.Err
		firstDiag := out.Diag
		firstStarted := out.Started
		if err == nil && resp != nil && resp.StatusCode/100 == 2 {
			if isStreamContext(ctx) {
				g.logger.Debug("upstream stream committed", "component", "upstream", "event", "attempt_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "proxy", redactURL(cand.Proxy.name), "status", resp.StatusCode)
			} else {
				g.logger.Debug("upstream accepted request", "component", "upstream", "event", "attempt_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "proxy", redactURL(cand.Proxy.name), "status", resp.StatusCode)
			}
			g.bindSessionPinCtx(ctx, ids.Session, route.ID, route.Tier, cand.CredID, cand.PoolName, cand.ProxyRaw, route.Protocol, normalizeRouteAuthority(baseURL))
			return resp, nil, attempts, false, false, buildKeyDomains()
		}
		// Unbound exhaustion evidence: same credential live 429s accumulate;
		// credential429 is written only when the credential's full frozen
		// eligible set has 429ed (last Retry-After). Single/progress 429
		// never sets it. Candidate traversal is bounded only by the frozen slice.
		if err == nil && resp != nil && resp.StatusCode == http.StatusTooManyRequests {
			markCredUnavailable(cand.CredID, cand.ProxyRaw, resp, nil)
			retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
			ev, ok := cred429Evidence[cand.CredID]
			if !ok {
				ev = make(map[string]time.Duration)
				cred429Evidence[cand.CredID] = ev
			}
			if _, seen := ev[cand.ProxyRaw]; !seen {
				ev[cand.ProxyRaw] = retryAfter
				cred429LastRetry[cand.CredID] = retryAfter
				cred429LastStarted[cand.CredID] = firstStarted
				// Same strict proof as the L2 custom gate (frozen eligible
				// set fully live-429ed, pre-cooled excluded); route state
				// stays false here so the scheduler write is unchanged.
				if customTakeoverEligible(customTakeoverQualification{ObservedLive429: len(ev), Eligible: credEligibleCount[cand.CredID], TerminalStatus: resp.StatusCode, Recovered400: false, Cancelled: false, Committed: false}) {
					_ = g.scheduler.noteCredential429Failure(cand.CredID, AttemptClassRateLimited, resp.StatusCode, retryAfter, firstStarted)
				}
			} else {
				cred429LastRetry[cand.CredID] = retryAfter
				cred429LastStarted[cand.CredID] = firstStarted
			}
		}
		if err == nil && resp != nil && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) {
			markCredUnavailable(cand.CredID, cand.ProxyRaw, resp, nil)
		}
		if isContextCancelled(ctx) {
			return resp, err, attempts, false, false, buildKeyDomains()
		}
		if handled, replayResp, replayErr, replayed := g.maybeReplayCandidate400(ctx, resp, err, firstDiag, route, route.Tier, baseURL, route.Protocol, body, ids, cand, scope, routeSession, attemptOffset, attempts, nil); handled {
			attempts = replayed
			if replayResp != nil || replayErr != nil {
				credRecovered[cand.CredID] = true
				return replayResp, replayErr, attempts, true, false, buildKeyDomains()
			}
			termArgs := []any{"component", "upstream", "event", "attempt_terminal", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "status", resp.StatusCode, "proxy", redactURL(cand.Proxy.name)}
			termArgs = append(termArgs, diagLogArgs(firstDiag, "")...)
			g.logger.Debug("upstream returned route-terminal 400; stopping route", termArgs...)
			return resp, nil, attempts, false, false, buildKeyDomains()
		}
		if isSameTargetTransient(resp, err) {
			loopRes := g.observeSameTargetTransient(ctx, attemptOutcome{Resp: resp, Err: err, Diag: firstDiag, Started: firstStarted}, route.Tier, route.Protocol, &attempts, attemptOffset, maxObservation, interval, func(monitorAttempt int) attemptOutcome {
				return g.executeAttempt(ctx, route, route.Tier, baseURL, route.Protocol, candBody, ids, cand, routeSession, "key", cand.CredDisplay, false, monitorAttempt)
			})
			classified := classifyStableCause(ctx, loopRes)
			final := classified.Final
			stop := classified.Stop
			_ = stop
			if classified.Cause == stableCauseBuildFailure {
				return nil, final.BuildErr, attempts, false, false, buildKeyDomains()
			}
			if classified.Cause == stableCauseSuccess {
				if isStreamContext(ctx) {
					g.logger.Debug("upstream transient retry stream committed", "component", "upstream", "event", "transient_retry_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "proxy", redactURL(cand.Proxy.name), "status", final.Resp.StatusCode)
				} else {
					g.logger.Debug("upstream transient retry succeeded", "component", "upstream", "event", "transient_retry_succeeded", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "proxy", redactURL(cand.Proxy.name), "status", final.Resp.StatusCode)
				}
				g.bindSessionPinCtx(ctx, ids.Session, route.ID, route.Tier, cand.CredID, cand.PoolName, cand.ProxyRaw, route.Protocol, normalizeRouteAuthority(baseURL))
				return final.Resp, nil, attempts, false, false, buildKeyDomains()
			}
			if classified.Cancelled {
				return final.Resp, final.Err, attempts, false, false, buildKeyDomains()
			}
			if classified.Cause == stableCauseExact400 {
				if handled, replayResp, replayErr, replayed := g.maybeReplayCandidate400(ctx, final.Resp, final.Err, final.Diag, route, route.Tier, baseURL, route.Protocol, body, ids, cand, scope, routeSession, attemptOffset, attempts, nil); handled {
					attempts = replayed
					if replayResp != nil || replayErr != nil {
						credRecovered[cand.CredID] = true
						return replayResp, replayErr, attempts, true, false, buildKeyDomains()
					}
					return final.Resp, nil, attempts, false, false, buildKeyDomains()
				}
			}
			if classified.Cause == stableCauseOrdinaryRejection {
				g.logger.Debug("upstream rejected a non-retryable request", "component", "upstream", "event", "attempt_rejected", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "status", final.Resp.StatusCode, "proxy", redactURL(cand.Proxy.name))
				return final.Resp, nil, attempts, false, false, buildKeyDomains()
			}
			if classified.Cause == stableCauseLive429 {
				markCredUnavailable(cand.CredID, cand.ProxyRaw, final.Resp, nil)
				retryAfter := parseRetryAfter(final.Resp.Header.Get("Retry-After"))
				ev, ok := cred429Evidence[cand.CredID]
				if !ok {
					ev = make(map[string]time.Duration)
					cred429Evidence[cand.CredID] = ev
				}
				if _, seen := ev[cand.ProxyRaw]; !seen {
					ev[cand.ProxyRaw] = retryAfter
					cred429LastRetry[cand.CredID] = retryAfter
					cred429LastStarted[cand.CredID] = final.Started
					if customTakeoverEligible(customTakeoverQualification{ObservedLive429: len(ev), Eligible: credEligibleCount[cand.CredID], TerminalStatus: final.Resp.StatusCode, Recovered400: false, Cancelled: false, Committed: false}) {
						_ = g.scheduler.noteCredential429Failure(cand.CredID, AttemptClassRateLimited, final.Resp.StatusCode, retryAfter, final.Started)
					}
				}
				lastResponse = final.Resp
				lastErr = final.Err
				continue
			}
			// L1-final transport/408/425/5xx, stable 401/403, or L1-final
			// stream startup failure after observation proves the object
			// unavailable (each frozen candidate once; ordinary 4xx/400
			// already returned, cancel already returned, BuildErr/committed
			// never reach here).
			if stop == transientStopObservationLimit || stop == transientStopStable {
				markCredUnavailable(cand.CredID, cand.ProxyRaw, final.Resp, final.Err)
			}
			lastResponse = final.Resp
			lastErr = final.Err
			if final.Err != nil {
				g.logger.Debug("upstream transient retry still failing", "component", "upstream", "event", "attempt_transport_failed", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "proxy", redactURL(cand.Proxy.name), "error", final.Err)
			} else if final.Resp != nil {
				g.logger.Debug("upstream transient retry returned a retryable response", "component", "upstream", "event", "attempt_retryable_response", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "status", final.Resp.StatusCode, "proxy", redactURL(cand.Proxy.name))
			} else {
				g.logger.Debug("upstream transient retry still failing", "component", "upstream", "event", "attempt_transport_failed", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "proxy", redactURL(cand.Proxy.name), "error", final.Err)
			}
			continue
		}
		// Request-shape errors are deterministic and must leave this tier
		// without rotating through unrelated keys. 408/425 are transient and
		// never end the tier here. Authentication, throttling, server, and
		// transport failures remain retryable inside this tier via fallback.
		if isOrdinaryClientRejection(resp, err) {
			g.logger.Debug("upstream rejected a non-retryable request", "component", "upstream", "event", "attempt_rejected", "request_id", ids.Request, "attempt", attempts, "tier", route.Tier, "protocol", route.Protocol, "client_session_hash", clientSessionHash(ids.Session), "key_id", cand.CredDisplay, "status", resp.StatusCode, "proxy", redactURL(cand.Proxy.name))
			return resp, nil, attempts, false, false, buildKeyDomains()
		}
		if resp != nil && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) {
			markCredUnavailable(cand.CredID, cand.ProxyRaw, resp, nil)
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
		return lastResponse, nil, attempts, false, false, buildKeyDomains()
	}
	return nil, lastErr, attempts, false, false, buildKeyDomains()
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
//     set only by the exhaustion rule in pinned/unbound flows (every eligible
//     proxy for one credential live-429ed), never from a partial 429. No
//     same-target retry is implied; it still triggers only the neutral async
//     proxy verification and never flips healthy directly.
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
		suspectCleared := g.scheduler.noteTransportSuspectSuccess(cand.Tier, cand.PoolName, cand.ProxyRaw, startedNanos)
		if suspectCleared.Changed && g.logger != nil {
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
			g.logger.Debug("proxy transport suspect cleared",
				"component", "scheduler", "event", "proxy_transport_suspect_cleared",
				"tier", string(cand.Tier), "key_id", cand.CredDisplay,
				"channel", credentialChannel(cand),
				"proxy_pool", poolName, "proxy_node", proxyNode,
				"failures", suspectCleared.Failures)
		}
		g.logSchedulerCleared(cand, targetCleared, credCleared, proxyCleared, channelCleared, cred429Cleared)
		if cand.Proxy != nil && !cand.Proxy.healthy.Load() {
			wasHealthy := cand.Proxy.healthy.Swap(true)
			if !wasHealthy && g.logger != nil {
				g.logger.Info("proxy connectivity restored", "component", "proxy", "event", "proxy_restored", "proxy", redactURL(cand.Proxy.name), "proxy_pool", cand.Proxy.pool)
			}
		}
	case err != nil:
		g.verifyProxyAfterError(ctx, cand.Proxy, 0)
		if isTrueTransportError(ctx, err) {
			ch := g.scheduler.noteTransportSuspect(cand.Tier, cand.PoolName, cand.ProxyRaw, class.Class, 0, startedNanos)
			g.logSuspectCooldownSet(cand, ch)
		}
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

func (g *Gateway) logSuspectCooldownSet(cand targetCandidate, change transportSuspectChange) {
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
	g.logger.Debug("proxy transport suspect cooldown extended",
		"component", "scheduler", "event", "proxy_transport_suspect_set",
		"tier", string(cand.Tier), "key_id", cand.CredDisplay,
		"channel", credentialChannel(cand),
		"proxy_pool", poolName, "proxy_node", proxyNode,
		"failure_class", change.FailureClass,
		"status", change.Status, "failures", change.Failures,
		"cooldown_until", time.Unix(0, change.CooldownUntil).UTC(),
		"remaining_ms", time.Duration(remaining).Milliseconds())
}

// logSchedulerCleared emits clear events only when 2xx actually removed
// stored credential/target/proxy429/channel/credential429/suspect state.
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
// traversal over all frozen candidates: the slice length itself is the
// independent, naturally finite upper bound and never reuses the inference L1
// retry.max_attempts. Foreground scheduler credential/target state is never
// read or written here, and proxy healthy/checking is never changed via
// syncProxyResult/verifyProxyAfterError. Healthy proxies are observed
// read-only; success/failure only decides the catalog snapshot in the caller,
// and a context deadline/cancel is only a refresh failure, never a proxy
// signal. Only the Zen lane exists; the tier argument is retained for compat
// and ignored beyond pool selection.
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
	type refreshCandidate struct {
		key   string
		proxy *proxyTransport
	}
	candidates := make([]refreshCandidate, 0, len(keys)*len(healthy))
	for _, key := range keys {
		for _, proxy := range healthy {
			candidates = append(candidates, refreshCandidate{key: key, proxy: proxy})
		}
	}
	for attempt, cand := range candidates {
		refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		models, _, err := fetchModels(refreshCtx, cand.proxy.client, base, cand.key)
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
