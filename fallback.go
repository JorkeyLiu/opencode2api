package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// TierCustom is the observability tier for custom OpenAI-compatible fallback
// channels. It never participates in Zen/Go scheduler state, health readiness,
// or the public model catalog.
const TierCustom Tier = "custom"

// FallbackChannelConfig is one operator-defined OpenAI-compatible channel.
type FallbackChannelConfig struct {
	Name    string `json:"name"`
	BaseURL string `json:"base_url"`
	APIKey  string `json:"api_key"`
	Model   string `json:"model"`
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

// fallbackChatURL maps a configured base_url to the chat completions endpoint.
// It tolerates a trailing slash and a base that already contains a reasonable
// API root: a base ending in /chat/completions is used as-is, a base ending
// in /v1 appends /chat/completions, otherwise /v1/chat/completions is appended.
func fallbackChatURL(baseURL string) string {
	normalized := normalizeFallbackBaseURL(baseURL)
	if normalized == "" {
		return ""
	}
	if strings.HasSuffix(normalized, "/chat/completions") {
		return normalized
	}
	if strings.HasSuffix(normalized, "/v1") {
		return normalized + "/chat/completions"
	}
	return normalized + "/v1/chat/completions"
}

// fallbackModelsURL maps a configured base_url to the model discovery endpoint
// with the same tolerance as fallbackChatURL.
func fallbackModelsURL(baseURL string) string {
	normalized := normalizeFallbackBaseURL(baseURL)
	if normalized == "" {
		return ""
	}
	if strings.HasSuffix(normalized, "/v1/models") {
		return normalized
	}
	if strings.HasSuffix(normalized, "/v1") {
		return normalized + "/models"
	}
	// A base already ending with the chat endpoint root cannot serve discovery;
	// fall back to the conventional mapping anyway (caller validates).
	if strings.HasSuffix(normalized, "/chat/completions") {
		return strings.TrimSuffix(normalized, "/chat/completions") + "/models"
	}
	return normalized + "/v1/models"
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

// validateFallbackConfig normalizes in place and validates the fallback object.
// Names are unique; active may be empty but when non-empty must reference an
// existing channel; every channel requires name/base_url/api_key/model.
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
// channel identity so a later rename or credential/model/URL change cannot
// silently drift an old session.
type fallbackBinding struct {
	Name    string
	BaseURL string // normalized base URL (no trailing slash)
	KeyHash string // full SHA-256 hex of the api_key
	KeyFP   string // 10-char fingerprint for redacted diagnostics
	Model   string // configured channel model
}

func fallbackBindingFor(ch FallbackChannelConfig) fallbackBinding {
	return fallbackBinding{
		Name:    strings.TrimSpace(ch.Name),
		BaseURL: normalizeFallbackBaseURL(ch.BaseURL),
		KeyHash: fallbackKeyHash(strings.TrimSpace(ch.APIKey)),
		KeyFP:   secretFingerprint(strings.TrimSpace(ch.APIKey)),
		Model:   strings.TrimSpace(ch.Model),
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

// buildFallbackChatBody converts the client request to OpenAI chat and rewrites
// model to the channel configured model. The strict client-payload path is
// the only supported path: ex.Payload carries the original client entry with
// its protocol, and prepareUpstreamRequest performs the strict three-protocol
// bridge. Without that context the prepared tier bodies cannot be reliably
// attributed to chat/responses/anthropic (chat and anthropic both use
// "messages" with different block shapes), so rewriting only "model" would
// silently send a responses/anthropic body to a chat endpoint. Callers without
// upstreamExtra therefore fail loudly instead of sending.
func buildFallbackChatBody(ex upstreamExtra, hasEx bool, bodies map[Tier][]byte, route modelRoute, ch FallbackChannelConfig) ([]byte, error) {
	model := strings.TrimSpace(ch.Model)
	if model == "" {
		return nil, errors.New("fallback channel model must not be empty")
	}
	if !hasEx || ex.Payload == nil {
		return nil, errors.New("fallback requires client request context")
	}
	converted, err := prepareUpstreamRequest(ex.External, ProtocolChat, cloneMap(ex.Payload), ch.BaseURL)
	if err != nil {
		return nil, err
	}
	converted["model"] = model
	encoded, err := json.Marshal(converted)
	if err != nil {
		return nil, errors.New("request contains unsupported JSON values")
	}
	return encoded, nil
}

// fetchFallbackModels lists models from a custom OpenAI-compatible base_url.
// It is configuration-assist only and never an availability verdict.
func fetchFallbackModels(ctx context.Context, client *http.Client, baseURL, apiKey string) ([]string, error) {
	trimmedBase := strings.TrimSpace(baseURL)
	if err := validateFallbackBaseURL(trimmedBase); err != nil {
		return nil, err
	}
	if strings.TrimSpace(apiKey) == "" {
		return nil, errors.New("api_key must not be empty")
	}
	endpoint := fallbackModelsURL(trimmedBase)
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(apiKey))
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", opencodeUserAgent())
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return nil, fmt.Errorf("models endpoint returned HTTP %d", resp.StatusCode)
	}
	var payload modelsResponse
	dec := json.NewDecoder(io.LimitReader(resp.Body, 2<<20))
	if err := dec.Decode(&payload); err != nil {
		return nil, errors.New("models endpoint returned invalid JSON")
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
		return nil, errors.New("models endpoint returned an empty list")
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

// doCustomFallbackRequest sends one chat request to the bound custom channel.
// It never touches Zen/Go scheduler cooldowns or proxy transport health. The
// returned route is TierCustom/ProtocolChat so the caller transcodes back to
// the client protocol with the existing cross-protocol paths.
func (g *Gateway) doCustomFallbackRequest(ctx context.Context, route modelRoute, ex upstreamExtra, hasEx bool, bodies map[Tier][]byte, ids requestIDs, ch FallbackChannelConfig, binding fallbackBinding, attemptOffset int) (*http.Response, modelRoute, int, error) {
	effectiveRoute := route
	effectiveRoute.Tier = TierCustom
	effectiveRoute.Protocol = ProtocolChat
	if isContextCancelled(ctx) {
		return nil, effectiveRoute, attemptOffset, ctx.Err()
	}
	chatBody, err := buildFallbackChatBody(ex, hasEx, bodies, route, ch)
	if err != nil {
		return nil, effectiveRoute, attemptOffset, err
	}
	endpoint := fallbackChatURL(ch.BaseURL)
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
	setRequestCredential(ctx, TierCustom, ProtocolChat, display, "custom", false, fakeProxy)
	syncAttemptMeta(ctx, TierCustom, ProtocolChat, attemptOffset, 1)
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
	customRoute.Protocol = ProtocolChat
	g.recordUpstreamAttemptWithClass(customRoute, ProtocolChat, ids, attemptOffset+1, display, "custom", false, fakeProxy, resp, sendErr, duration, class, false, false, false, badRequestDiag{})
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
