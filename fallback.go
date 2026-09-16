package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// TierCustom is the observability tier for custom OpenAI-compatible fallback
// channels. It never participates in Zen/Go scheduler state, health readiness,
// or the public model catalog.
const TierCustom Tier = "custom"

// FallbackChannelConfig is one operator-defined OpenAI-compatible channel.
// Protocol selects the upstream inference endpoint: "chat" (Chat Completions)
// or "responses" (Responses). Empty/legacy configs normalize to "chat".
// ReasoningEffort is an optional per-channel thinking-strength override:
// "" (supplier default), "low", "medium", or "high". Anything else is
// strictly rejected. It never participates in the takeover binding identity.
type FallbackChannelConfig struct {
	Name            string   `json:"name"`
	BaseURL         string   `json:"base_url"`
	APIKey          string   `json:"api_key"`
	Model           string   `json:"model"`
	Protocol        Protocol `json:"protocol"`
	ReasoningEffort string   `json:"reasoning_effort,omitempty"`
}

// FallbackConfig is the strict fallback object: {"active":"name","channels":[...]}.
type FallbackConfig struct {
	Active   string                  `json:"active"`
	Channels []FallbackChannelConfig `json:"channels"`
}

func normalizeFallbackBaseURL(raw string) string {
	return strings.TrimRight(strings.TrimSpace(raw), "/")
}

func fallbackKeyHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// fallbackNormalizeProtocol canonicalizes a channel protocol value. Empty
// normalizes to chat; chat/responses (case-insensitive, trimmed) are accepted;
// anything else reports invalid.
func fallbackNormalizeProtocol(raw Protocol) (Protocol, error) {
	trimmed := strings.ToLower(strings.TrimSpace(string(raw)))
	if trimmed == "" {
		return ProtocolChat, nil
	}
	switch Protocol(trimmed) {
	case ProtocolChat, ProtocolResponses:
		return Protocol(trimmed), nil
	default:
		return "", fmt.Errorf("fallback channel protocol %q must be \"chat\" or \"responses\"", string(raw))
	}
}

// fallbackChannelProtocol returns the effective inference protocol for a
// channel. Normalized configs always carry chat or responses; empty or
// unexpected values defensively fall back to chat without failing.
func fallbackChannelProtocol(ch FallbackChannelConfig) Protocol {
	if p, err := fallbackNormalizeProtocol(ch.Protocol); err == nil {
		return p
	}
	return ProtocolChat
}

// fallbackProtocolDisplay is the operator-facing name for a channel protocol.
func fallbackProtocolDisplay(p Protocol) string {
	if fallbackChannelProtocol(FallbackChannelConfig{Protocol: p}) == ProtocolResponses {
		return "Responses"
	}
	return "Chat Completions"
}

// fallbackAPIRoot strips any known API suffix so host, host/v1, and either
// full inference endpoint (or /v1/models) all resolve to the same host root.
// A base already carrying the other protocol's full endpoint therefore never
// produces a doubled bad path; the caller re-appends the selected protocol.
func fallbackAPIRoot(normalized string) string {
	if normalized == "" {
		return ""
	}
	if strings.HasSuffix(normalized, "/v1/models") {
		return strings.TrimSuffix(normalized, "/v1/models")
	}
	if strings.HasSuffix(normalized, "/v1/chat/completions") {
		return strings.TrimSuffix(normalized, "/v1/chat/completions")
	}
	if strings.HasSuffix(normalized, "/v1/responses") {
		return strings.TrimSuffix(normalized, "/v1/responses")
	}
	if strings.HasSuffix(normalized, "/chat/completions") {
		return strings.TrimSuffix(normalized, "/chat/completions")
	}
	if strings.HasSuffix(normalized, "/responses") {
		return strings.TrimSuffix(normalized, "/responses")
	}
	if strings.HasSuffix(normalized, "/v1") {
		return strings.TrimSuffix(normalized, "/v1")
	}
	return normalized
}

// fallbackEndpointURL maps a configured base_url to the inference endpoint
// for the selected protocol using the unified API-root rule.
func fallbackEndpointURL(baseURL string, protocol Protocol) string {
	normalized := normalizeFallbackBaseURL(baseURL)
	if normalized == "" {
		return ""
	}
	root := fallbackAPIRoot(normalized)
	if root == "" {
		return ""
	}
	if fallbackChannelProtocol(FallbackChannelConfig{Protocol: protocol}) == ProtocolResponses {
		return root + "/v1/responses"
	}
	return root + "/v1/chat/completions"
}

// fallbackChatURL maps a configured base_url to the chat completions endpoint.
// It is the chat specialization of fallbackEndpointURL, kept for existing
// callers and WebUI URL-preview parity.
func fallbackChatURL(baseURL string) string {
	return fallbackEndpointURL(baseURL, ProtocolChat)
}

// fallbackResponsesURL maps a configured base_url to the responses endpoint.
func fallbackResponsesURL(baseURL string) string {
	return fallbackEndpointURL(baseURL, ProtocolResponses)
}

// fallbackModelsURL maps a configured base_url to the model discovery endpoint
// with the same unified root tolerance: host, /v1, /v1/chat/completions, and
// /v1/responses all resolve to /v1/models.
func fallbackModelsURL(baseURL string) string {
	normalized := normalizeFallbackBaseURL(baseURL)
	if normalized == "" {
		return ""
	}
	root := fallbackAPIRoot(normalized)
	if root == "" {
		return ""
	}
	if strings.HasSuffix(normalized, "/v1/models") {
		return normalized
	}
	return root + "/v1/models"
}

func validateFallbackChannelName(name string) error {
	if err := validatePoolName(strings.TrimSpace(name)); err != nil {
		return fmt.Errorf("invalid fallback channel name %q: %w", name, err)
	}
	return nil
}

func validateFallbackBaseURL(raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return errors.New("fallback channel base_url must not be empty")
	}
	u, err := url.Parse(trimmed)
	if err != nil || u.Host == "" {
		return fmt.Errorf("fallback channel base_url %q must be an http or https URL", redactURL(trimmed))
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		return nil
	default:
		return fmt.Errorf("fallback channel base_url %q must be an http or https URL", redactURL(trimmed))
	}
}

// fallbackNormalizeReasoningEffort canonicalizes the per-channel thinking
// strength. Empty normalizes to "" (supplier default); low/medium/high
// (case-insensitive, trimmed) normalize to lowercase; anything else is
// strictly rejected.
func fallbackNormalizeReasoningEffort(raw string) (string, error) {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	if trimmed == "" {
		return "", nil
	}
	switch trimmed {
	case "low", "medium", "high":
		return trimmed, nil
	default:
		return "", fmt.Errorf("fallback channel reasoning_effort %q must be \"\", \"low\", \"medium\" or \"high\"", raw)
	}
}

// fallbackChannelEffort returns the effective reasoning effort for a channel.
// Normalized configs always carry ""/low/medium/high; unexpected values
// defensively read as "" without failing.
func fallbackChannelEffort(ch FallbackChannelConfig) string {
	if effort, err := fallbackNormalizeReasoningEffort(ch.ReasoningEffort); err == nil {
		return effort
	}
	return ""
}

// applyFallbackReasoningEffort overlays the channel effort on a converted
// request body built by prepareUpstreamRequest. Chat sets the
// reasoning_effort string; Responses merges/creates reasoning:{effort} while
// preserving other reasoning map keys. Empty effort never injects or
// overwrites, so existing client fields or the supplier default survive.
func applyFallbackReasoningEffort(converted map[string]any, effort string, target Protocol) {
	effort = strings.ToLower(strings.TrimSpace(effort))
	if effort == "" || converted == nil {
		return
	}
	if fallbackChannelProtocol(FallbackChannelConfig{Protocol: target}) == ProtocolResponses {
		if existing, ok := converted["reasoning"].(map[string]any); ok && existing != nil {
			clone := make(map[string]any, len(existing)+1)
			for k, v := range existing {
				clone[k] = v
			}
			clone["effort"] = effort
			converted["reasoning"] = clone
			return
		}
		converted["reasoning"] = map[string]any{"effort": effort}
		return
	}
	converted["reasoning_effort"] = effort
}

// validateFallbackConfig normalizes in place and validates the fallback object.
// Names are unique; active may be empty but when non-empty must reference an
// existing channel; every channel requires name/base_url/api_key/model.
// Empty/missing protocol normalizes to chat; any other value besides chat or
// responses is strictly rejected. Empty/missing reasoning_effort normalizes
// to "" (supplier default); only low/medium/high are accepted otherwise.
func validateFallbackConfig(fb *FallbackConfig) error {
	if fb == nil {
		return nil
	}
	fb.Active = strings.TrimSpace(fb.Active)
	if fb.Channels == nil {
		fb.Channels = []FallbackChannelConfig{}
	}
	seen := map[string]bool{}
	for i := range fb.Channels {
		ch := &fb.Channels[i]
		ch.Name = strings.TrimSpace(ch.Name)
		ch.BaseURL = strings.TrimSpace(ch.BaseURL)
		ch.APIKey = strings.TrimSpace(ch.APIKey)
		ch.Model = strings.TrimSpace(ch.Model)
		proto, err := fallbackNormalizeProtocol(ch.Protocol)
		if err != nil {
			return fmt.Errorf("fallback channel %q: %w", ch.Name, err)
		}
		ch.Protocol = proto
		effort, err := fallbackNormalizeReasoningEffort(ch.ReasoningEffort)
		if err != nil {
			return fmt.Errorf("fallback channel %q: %w", ch.Name, err)
		}
		ch.ReasoningEffort = effort
		if ch.Name == "" {
			return errors.New("fallback channel name must not be empty")
		}
		if err := validateFallbackChannelName(ch.Name); err != nil {
			return err
		}
		if seen[ch.Name] {
			return fmt.Errorf("duplicate fallback channel name %q", ch.Name)
		}
		seen[ch.Name] = true
		if err := validateFallbackBaseURL(ch.BaseURL); err != nil {
			return err
		}
		if ch.APIKey == "" {
			return fmt.Errorf("fallback channel %q api_key must not be empty", ch.Name)
		}
		if ch.Model == "" {
			return fmt.Errorf("fallback channel %q model must not be empty", ch.Name)
		}
	}
	if fb.Active != "" && !seen[fb.Active] {
		return fmt.Errorf("fallback active channel %q does not exist", fb.Active)
	}
	return nil
}

func fallbackChannelByName(cfg Config, name string) (FallbackChannelConfig, bool) {
	name = strings.TrimSpace(name)
	for _, ch := range cfg.Fallback.Channels {
		if ch.Name == name {
			return ch, true
		}
	}
	return FallbackChannelConfig{}, false
}

func activeFallbackChannel(cfg Config) (FallbackChannelConfig, bool) {
	if strings.TrimSpace(cfg.Fallback.Active) == "" {
		return FallbackChannelConfig{}, false
	}
	return fallbackChannelByName(cfg, cfg.Fallback.Active)
}

// fallbackBinding is the session-level takeover value. It freezes the complete
// channel identity (including protocol) so a later rename, credential, model,
// URL, or protocol change cannot silently drift an old session.
type fallbackBinding struct {
	Name     string
	BaseURL  string // normalized base URL (no trailing slash)
	KeyHash  string // full SHA-256 hex of the api_key
	KeyFP    string // 10-char fingerprint for redacted diagnostics
	Model    string // configured channel model
	Protocol Protocol
}

// fallbackBindingFor freezes the takeover identity. ReasoningEffort is
// intentionally excluded: an effort change never drifts or invalidates a
// bound session and never affects hot-Apply tombstone migration.
func fallbackBindingFor(ch FallbackChannelConfig) fallbackBinding {
	return fallbackBinding{
		Name:     strings.TrimSpace(ch.Name),
		BaseURL:  normalizeFallbackBaseURL(ch.BaseURL),
		KeyHash:  fallbackKeyHash(strings.TrimSpace(ch.APIKey)),
		KeyFP:    secretFingerprint(strings.TrimSpace(ch.APIKey)),
		Model:    strings.TrimSpace(ch.Model),
		Protocol: fallbackChannelProtocol(ch),
	}
}

func (b fallbackBinding) matchesChannel(ch FallbackChannelConfig) bool {
	if b.Name != strings.TrimSpace(ch.Name) {
		return false
	}
	if b.BaseURL != normalizeFallbackBaseURL(ch.BaseURL) {
		return false
	}
	if b.KeyHash != fallbackKeyHash(strings.TrimSpace(ch.APIKey)) {
		return false
	}
	if b.Model != strings.TrimSpace(ch.Model) {
		return false
	}
	// Empty binding protocol (pre-protocol tombstones) reads as chat so old
	// chat sessions do not spuriously 502; an explicit protocol change still
	// mismatches and fails closed with 502.
	if fallbackChannelProtocol(FallbackChannelConfig{Protocol: b.Protocol}) != fallbackChannelProtocol(ch) {
		return false
	}
	return true
}

// fallbackTakeoverStoreCap bounds the session->channel takeover map.
const fallbackTakeoverStoreCap = 4096

type fallbackTakeoverStore struct {
	mu      sync.Mutex
	entries map[string]fallbackBinding
}

func newFallbackTakeoverStore() *fallbackTakeoverStore {
	return &fallbackTakeoverStore{entries: make(map[string]fallbackBinding)}
}

func (st *fallbackTakeoverStore) get(session string) (fallbackBinding, bool) {
	if st == nil || session == "" {
		return fallbackBinding{}, false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	b, ok := st.entries[session]
	return b, ok
}

func (st *fallbackTakeoverStore) count() int {
	if st == nil {
		return 0
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.entries)
}

// bind inserts first-wins. It returns the stored binding, whether this call
// inserted, and whether the store is at capacity (insert refused).
func (st *fallbackTakeoverStore) bind(session string, b fallbackBinding) (fallbackBinding, bool, bool) {
	if st == nil || session == "" {
		return b, false, false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if existing, ok := st.entries[session]; ok {
		return existing, false, false
	}
	if len(st.entries) >= fallbackTakeoverStoreCap {
		return b, false, true
	}
	if st.entries == nil {
		st.entries = make(map[string]fallbackBinding)
	}
	st.entries[session] = b
	return b, true, false
}

// migrateFallbackFrom copies all takeover identities without validity
// filtering (tombstone semantics). Removed or changed channels still resolve
// to the pinned fallback path and fail locally with 502. Deterministic key
// order, stops at the cap without evicting.
func (st *fallbackTakeoverStore) migrateFallbackFrom(old *fallbackTakeoverStore) int {
	if st == nil || old == nil || st == old {
		return 0
	}
	old.mu.Lock()
	type copied struct {
		key string
		val fallbackBinding
	}
	staged := make([]copied, 0, len(old.entries))
	for k, v := range old.entries {
		if k == "" || v.Name == "" {
			continue
		}
		staged = append(staged, copied{key: k, val: v})
	}
	old.mu.Unlock()
	sort.Slice(staged, func(i, j int) bool { return staged[i].key < staged[j].key })
	migrated := 0
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, item := range staged {
		if _, ok := st.entries[item.key]; ok {
			continue
		}
		if len(st.entries) >= fallbackTakeoverStoreCap {
			break
		}
		if st.entries == nil {
			st.entries = make(map[string]fallbackBinding)
		}
		st.entries[item.key] = item.val
		migrated++
	}
	return migrated
}

// upstreamExtra carries the client entry context needed to build the custom
// chat body without changing the historical doUpstream signatures used by
// existing tests (variadic, backward compatible).
type upstreamExtra struct {
	External Protocol
	Payload  map[string]any
}

func firstUpstreamExtra(extra []upstreamExtra) (upstreamExtra, bool) {
	if len(extra) == 0 {
		return upstreamExtra{}, false
	}
	return extra[0], true
}

// buildFallbackChatBody converts the client request to the channel protocol
// (chat or responses) and rewrites model to the channel configured model. The
// strict client-payload path is the only supported path: ex.Payload carries
// the original client entry with its protocol, and prepareUpstreamRequest
// performs the strict three-protocol bridge. Without that context the prepared
// tier bodies cannot be reliably attributed to chat/responses/anthropic (chat
// and anthropic both use "messages" with different block shapes), so rewriting
// only "model" would silently send a responses/anthropic body to the wrong
// endpoint. Callers without upstreamExtra therefore fail loudly instead of
// sending. The name is historical; the target is the channel protocol.
func buildFallbackChatBody(ex upstreamExtra, hasEx bool, bodies map[Tier][]byte, route modelRoute, ch FallbackChannelConfig) ([]byte, error) {
	return buildFallbackRequestBody(ex, hasEx, bodies, route, ch)
}

// buildFallbackRequestBody is the protocol-aware fallback body builder.
func buildFallbackRequestBody(ex upstreamExtra, hasEx bool, bodies map[Tier][]byte, route modelRoute, ch FallbackChannelConfig) ([]byte, error) {
	model := strings.TrimSpace(ch.Model)
	if model == "" {
		return nil, errors.New("fallback channel model must not be empty")
	}
	if !hasEx || ex.Payload == nil {
		return nil, errors.New("fallback requires client request context")
	}
	target := fallbackChannelProtocol(ch)
	converted, err := prepareUpstreamRequest(ex.External, target, cloneMap(ex.Payload), ch.BaseURL)
	if err != nil {
		return nil, err
	}
	converted["model"] = model
	applyFallbackReasoningEffort(converted, fallbackChannelEffort(ch), target)
	encoded, err := json.Marshal(converted)
	if err != nil {
		return nil, errors.New("request contains unsupported JSON values")
	}
	return encoded, nil
}

// Structured model-discovery failure reasons. Only reliably distinguishable
// transport classes are enumerated; everything else collapses to
// transport_error. No reason ever carries key material, body text, or the raw
// error string.
const (
	fallbackDiscoverDNS            = "dns_error"
	fallbackDiscoverConnRefused    = "connect_refused"
	fallbackDiscoverTimeout        = "timeout"
	fallbackDiscoverTLS            = "tls_error"
	fallbackDiscoverTransport      = "transport_error"
	fallbackDiscoverNon2xx         = "non_2xx"
	fallbackDiscoverInvalidJSON    = "invalid_json"
	fallbackDiscoverEmptyList      = "empty_list"
	fallbackDiscoverInvalidRequest = "invalid_request"
)

// fallbackDiscoverError is the safe structured discovery failure. Endpoint is
// the redacted models URL (never key material); HTTPStatus is set only when an
// HTTP response was received; ElapsedMS measures the attempt.
type fallbackDiscoverError struct {
	Reason     string
	Endpoint   string
	HTTPStatus int
	ElapsedMS  int64
}

func (e *fallbackDiscoverError) Error() string {
	if e == nil {
		return "model discovery failed"
	}
	return "model discovery failed: " + e.Reason
}

func asFallbackDiscoverError(err error) (*fallbackDiscoverError, bool) {
	var target *fallbackDiscoverError
	if err == nil {
		return nil, false
	}
	if errors.As(err, &target) && target != nil {
		return target, true
	}
	return nil, false
}

// fallbackDiscoverTransportReason classifies a client.Do transport error into
// the stable reason enum. Classification is conservative: only positively
// identified classes return their enum, everything else is transport_error.
func fallbackDiscoverTransportReason(err error) string {
	if err == nil {
		return fallbackDiscoverTransport
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fallbackDiscoverTimeout
	}
	var netErr interface{ Timeout() bool }
	if errors.As(err, &netErr) && netErr.Timeout() {
		return fallbackDiscoverTimeout
	}
	if isFallbackDiscoverTimeoutString(err) {
		return fallbackDiscoverTimeout
	}
	var dnsErr interface{ IsNotFound() bool }
	_ = dnsErr
	if isFallbackDiscoverDNSError(err) {
		return fallbackDiscoverDNS
	}
	if errors.Is(err, syscall.ECONNREFUSED) || isFallbackDiscoverConnRefusedString(err) {
		return fallbackDiscoverConnRefused
	}
	if isFallbackDiscoverTLSError(err) {
		return fallbackDiscoverTLS
	}
	return fallbackDiscoverTransport
}

func isFallbackDiscoverTimeoutString(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline exceeded")
}

func isFallbackDiscoverDNSError(err error) bool {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr != nil {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such host") || strings.Contains(msg, "dns")
}

func isFallbackDiscoverConnRefusedString(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection refused")
}

func isFallbackDiscoverTLSError(err error) bool {
	var unknownAuthority x509.UnknownAuthorityError
	if errors.As(err, &unknownAuthority) {
		return true
	}
	var hostnameErr x509.HostnameError
	if errors.As(err, &hostnameErr) {
		return true
	}
	var invalidErr x509.CertificateInvalidError
	if errors.As(err, &invalidErr) {
		return true
	}
	var constraintErr x509.ConstraintViolationError
	if errors.As(err, &constraintErr) {
		return true
	}
	var recordHeader tls.RecordHeaderError
	if errors.As(err, &recordHeader) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "certificate") || strings.Contains(msg, "x509") || strings.Contains(msg, "tls")
}

// fetchFallbackModels lists models from a custom OpenAI-compatible base_url.
// It is configuration-assist only and never an availability verdict.
// Failures after validation return *fallbackDiscoverError with a stable reason
// enum, redacted endpoint, HTTP status (when available), and elapsed time.
// The error text never contains key material, upstream body, Authorization
// values, or the raw transport error.
func fetchFallbackModels(ctx context.Context, client *http.Client, baseURL, apiKey string) ([]string, error) {
	trimmedBase := strings.TrimSpace(baseURL)
	if err := validateFallbackBaseURL(trimmedBase); err != nil {
		return nil, err
	}
	if strings.TrimSpace(apiKey) == "" {
		return nil, errors.New("api_key must not be empty")
	}
	endpoint := fallbackModelsURL(trimmedBase)
	redactedEndpoint := redactURL(endpoint)
	started := time.Now()
	elapsedMS := func() int64 { return max(time.Since(started).Milliseconds(), 0) }
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, &fallbackDiscoverError{Reason: fallbackDiscoverTransport, Endpoint: redactedEndpoint, ElapsedMS: elapsedMS()}
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(apiKey))
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", opencodeUserAgent())
	resp, err := client.Do(req)
	if err != nil {
		return nil, &fallbackDiscoverError{Reason: fallbackDiscoverTransportReason(err), Endpoint: redactedEndpoint, ElapsedMS: elapsedMS()}
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return nil, &fallbackDiscoverError{Reason: fallbackDiscoverNon2xx, Endpoint: redactedEndpoint, HTTPStatus: resp.StatusCode, ElapsedMS: elapsedMS()}
	}
	var payload modelsResponse
	dec := json.NewDecoder(io.LimitReader(resp.Body, 2<<20))
	if err := dec.Decode(&payload); err != nil {
		return nil, &fallbackDiscoverError{Reason: fallbackDiscoverInvalidJSON, Endpoint: redactedEndpoint, HTTPStatus: resp.StatusCode, ElapsedMS: elapsedMS()}
	}
	seen := map[string]bool{}
	out := []string{}
	for _, item := range payload.Data {
		id := strings.TrimSpace(item.ID)
		if id == "" || len(id) > 256 || strings.ContainsRune(id, 0) {
			continue
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
		if len(out) >= 512 {
			break
		}
	}
	if len(out) == 0 {
		return nil, &fallbackDiscoverError{Reason: fallbackDiscoverEmptyList, Endpoint: redactedEndpoint, HTTPStatus: resp.StatusCode, ElapsedMS: elapsedMS()}
	}
	sort.Strings(out)
	return out, nil
}

// fallbackCustomClient returns the Gateway custom-channel HTTP client,
// defaulting to a direct client when unset (tests may override).
func (g *Gateway) fallbackCustomClient() *http.Client {
	if g != nil && g.customClient != nil {
		return g.customClient
	}
	return &http.Client{Timeout: 30 * time.Second}
}

// doCustomFallbackRequest sends one request to the bound custom channel using
// the channel protocol (chat or responses). It never touches Zen/Go scheduler
// cooldowns or proxy transport health. The returned route is TierCustom plus
// the channel protocol so the caller transcodes back to the client protocol
// with the existing cross-protocol paths.
func (g *Gateway) doCustomFallbackRequest(ctx context.Context, route modelRoute, ex upstreamExtra, hasEx bool, bodies map[Tier][]byte, ids requestIDs, ch FallbackChannelConfig, binding fallbackBinding, attemptOffset int) (*http.Response, modelRoute, int, error) {
	channelProtocol := fallbackChannelProtocol(ch)
	channelModel := strings.TrimSpace(ch.Model)
	effectiveRoute := route
	effectiveRoute.Tier = TierCustom
	effectiveRoute.Protocol = channelProtocol
	if channelModel != "" {
		effectiveRoute.ID = channelModel
	}
	if isContextCancelled(ctx) {
		return nil, effectiveRoute, attemptOffset, ctx.Err()
	}
	chatBody, err := buildFallbackRequestBody(ex, hasEx, bodies, route, ch)
	if err != nil {
		return nil, effectiveRoute, attemptOffset, err
	}
	endpoint := fallbackEndpointURL(ch.BaseURL, channelProtocol)
	if endpoint == "" {
		return pinLocalResponse(http.StatusBadGateway, 0, "upstream temporarily unavailable"), effectiveRoute, attemptOffset, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(chatBody))
	if err != nil {
		return nil, effectiveRoute, attemptOffset, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("User-Agent", opencodeUserAgent())
	req.Header.Set("x-opencode-client", "cli")
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(ch.APIKey))
	// No supplier session affinity headers; every request carries the full
	// client-provided history in the chat body.
	fakeProxy := &proxyTransport{name: normalizeFallbackBaseURL(ch.BaseURL), pool: "fallback"}
	display := "custom:" + strings.TrimSpace(ch.Name)
	setRequestCredential(ctx, TierCustom, channelProtocol, display, "custom", false, fakeProxy)
	setRequestModel(ctx, channelModel)
	syncAttemptMeta(ctx, TierCustom, channelProtocol, attemptOffset, 1)
	started := time.Now()
	resp, sendErr := g.fallbackCustomClient().Do(req)
	duration := time.Since(started)
	// Defensive: an http.Client must return either resp or err, but a
	// misbehaving RoundTripper returning (nil, nil) must never surface as
	// nil response + nil error (handleInference would panic on defer
	// resp.Body.Close). Synthesize a transport error instead.
	if resp == nil && sendErr == nil {
		sendErr = errors.New("custom fallback transport failed")
	}
	class := classifyUpstreamAttempt(resp, sendErr)
	customRoute := route
	customRoute.Tier = TierCustom
	customRoute.Protocol = channelProtocol
	if channelModel != "" {
		customRoute.ID = channelModel
	}
	g.recordUpstreamAttemptWithClass(customRoute, channelProtocol, ids, attemptOffset+1, display, "custom", false, fakeProxy, resp, sendErr, duration, class, false, false, false, badRequestDiag{})
	_ = binding
	if sendErr != nil {
		// Custom transport errors stay on the bound channel: return the
		// error so handleInference converts it (context cancel per existing
		// cancel semantics, other transport failures as safe 502) with no
		// fallback to the original 429, native tiers, or other channels.
		return resp, effectiveRoute, attemptOffset + 1, sendErr
	}
	if resp == nil {
		return nil, effectiveRoute, attemptOffset + 1, errors.New("custom fallback transport failed")
	}
	return resp, effectiveRoute, attemptOffset + 1, nil
}
