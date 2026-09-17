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
// ID is the stable machine identity (persisted, unique, strict syntax) used
// by active selection, takeover binding, secret resolution, discovery/check,
// and availability keys. Name is free-form operator display text and never
// participates in identity matching: a name-only edit preserves bindings.
// Protocol selects the upstream inference endpoint: "chat" (Chat Completions)
// or "responses" (Responses). Empty/legacy configs normalize to "chat".
// ReasoningEffort is an optional per-channel thinking-strength control:
// "" (supplier default: strip target strength after conversion),
// "inherit" (preserve the converted request strength), "low", "medium", or
// "high". Anything else is strictly rejected. It never participates in the
// takeover binding identity.
type FallbackChannelConfig struct {
	ID              string   `json:"id"`
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

func validateFallbackChannelID(id string) error {
	trimmed := strings.TrimSpace(id)
	if trimmed == "" {
		return errors.New("fallback channel id must not be empty")
	}
	if len(trimmed) > 64 {
		return fmt.Errorf("invalid fallback channel id %q: must be 1-64 characters", id)
	}
	for _, r := range trimmed {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
			continue
		}
		return fmt.Errorf("invalid fallback channel id %q: use letters, digits, '_', '-' or '.'", id)
	}
	return nil
}

// validateFallbackDisplayName permits trimmed free-form UTF-8 display text
// (spaces such as "OpenCode Go" are allowed). Only empty values,
// unreasonable length, or control characters are rejected.
func validateFallbackDisplayName(name string) error {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return errors.New("fallback channel display name must not be empty")
	}
	if len([]rune(trimmed)) > 128 {
		return fmt.Errorf("invalid fallback channel display name %q: must be 1-128 characters", name)
	}
	for _, r := range trimmed {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("invalid fallback channel display name %q: must not contain control characters", name)
		}
	}
	return nil
}

// fallbackDeriveChannelID deterministically derives a stable ID for legacy
// name-only configs. Names that already satisfy the ID syntax are preserved
// as-is (legacy valid names keep their identity); anything else is slugified
// (lowercased, invalid runs become "-"). Empty slugs fall back to a stable
// hash so derivation never yields an empty ID. Collisions are reported by the
// caller as duplicate IDs rather than silently renamed.
func fallbackDeriveChannelID(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return ""
	}
	if err := validateFallbackChannelID(trimmed); err == nil {
		return trimmed
	}
	lower := strings.ToLower(trimmed)
	var b strings.Builder
	prevDash := false
	for _, r := range lower {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
			b.WriteRune(r)
			prevDash = r == '-'
			continue
		}
		if !prevDash && b.Len() > 0 {
			b.WriteRune('-')
			prevDash = true
		}
	}
	slug := strings.Trim(b.String(), "-.")
	if slug == "" {
		slug = "channel-" + fallbackKeyHash(trimmed)[:8]
	}
	if len(slug) > 64 {
		slug = strings.Trim(slug[:64], "-.")
	}
	if slug == "" {
		slug = "channel"
	}
	return slug
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
// strength. Empty normalizes to "" (supplier default: strip target strength
// after conversion); "inherit" preserves the converted request strength;
// low/medium/high (case-insensitive, trimmed) normalize to lowercase;
// anything else is strictly rejected.
func fallbackNormalizeReasoningEffort(raw string) (string, error) {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	if trimmed == "" {
		return "", nil
	}
	switch trimmed {
	case "inherit", "low", "medium", "high":
		return trimmed, nil
	default:
		return "", fmt.Errorf("fallback channel reasoning_effort %q must be \"\", \"inherit\", \"low\", \"medium\" or \"high\"", raw)
	}
}

// fallbackChannelEffort returns the effective reasoning effort for a channel.
// Normalized configs always carry ""/inherit/low/medium/high; unexpected
// values defensively read as "" without failing.
func fallbackChannelEffort(ch FallbackChannelConfig) string {
	if effort, err := fallbackNormalizeReasoningEffort(ch.ReasoningEffort); err == nil {
		return effort
	}
	return ""
}

// stripFallbackReasoningStrength removes target reasoning-strength controls
// after conversion so the custom supplier chooses its default. Chat removes
// top-level strength fields; Responses removes the target reasoning control.
// Reasoning history/content (reasoning_content, input reasoning items,
// summaries) is never touched.
func stripFallbackReasoningStrength(converted map[string]any, target Protocol) {
	if converted == nil {
		return
	}
	if fallbackChannelProtocol(FallbackChannelConfig{Protocol: target}) == ProtocolResponses {
		delete(converted, "reasoning")
		delete(converted, "reasoning_effort")
		delete(converted, "effort")
		return
	}
	delete(converted, "reasoning_effort")
	delete(converted, "reasoning")
	delete(converted, "effort")
}

// applyFallbackReasoningEffort overlays the channel effort on a converted
// request body built by prepareUpstreamRequest. "inherit" preserves the
// converted strength (injecting nothing when absent); "" (supplier default)
// strips target strength controls; low/medium/high override. Chat sets the
// reasoning_effort string; Responses merges/creates reasoning:{effort} while
// preserving other reasoning map keys.
func applyFallbackReasoningEffort(converted map[string]any, effort string, target Protocol) {
	effort = strings.ToLower(strings.TrimSpace(effort))
	if converted == nil {
		return
	}
	if effort == "" {
		stripFallbackReasoningStrength(converted, target)
		return
	}
	if effort == "inherit" {
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
// IDs are the stable identity (unique, strict); names are free-form display
// text (unique for unambiguous UI/migration). Legacy name-only channels
// derive their ID deterministically; legacy active values referencing a
// display name normalize to the derived ID. Active may be empty but when
// non-empty must reference an existing channel ID; every channel requires
// id/name/base_url/api_key/model. Empty/missing protocol normalizes to chat;
// any other value besides chat or responses is strictly rejected.
// Empty/missing reasoning_effort normalizes to "" (supplier default); only
// inherit/low/medium/high are accepted otherwise.
func validateFallbackConfig(fb *FallbackConfig) error {
	if fb == nil {
		return nil
	}
	fb.Active = strings.TrimSpace(fb.Active)
	if fb.Channels == nil {
		fb.Channels = []FallbackChannelConfig{}
	}
	for i := range fb.Channels {
		ch := &fb.Channels[i]
		ch.ID = strings.TrimSpace(ch.ID)
		ch.Name = strings.TrimSpace(ch.Name)
		ch.BaseURL = strings.TrimSpace(ch.BaseURL)
		ch.APIKey = strings.TrimSpace(ch.APIKey)
		ch.Model = strings.TrimSpace(ch.Model)
		if ch.ID == "" {
			ch.ID = fallbackDeriveChannelID(ch.Name)
		}
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
		if err := validateFallbackChannelID(ch.ID); err != nil {
			return err
		}
		if err := validateFallbackDisplayName(ch.Name); err != nil {
			return err
		}
		if err := validateFallbackBaseURL(ch.BaseURL); err != nil {
			return err
		}
		if ch.APIKey == "" {
			return fmt.Errorf("fallback channel %q api_key must not be empty", ch.ID)
		}
		if ch.Model == "" {
			return fmt.Errorf("fallback channel %q model must not be empty", ch.ID)
		}
	}
	seenID := map[string]bool{}
	seenName := map[string]bool{}
	for _, ch := range fb.Channels {
		if seenID[ch.ID] {
			return fmt.Errorf("duplicate fallback channel id %q", ch.ID)
		}
		seenID[ch.ID] = true
		if seenName[ch.Name] {
			return fmt.Errorf("duplicate fallback channel display name %q", ch.Name)
		}
		seenName[ch.Name] = true
	}
	// Cross-channel namespace ambiguity: one channel's stable ID must not
	// equal another channel's trimmed display name, otherwise ID-first
	// lookup with legacy-name fallback could silently select the wrong
	// channel. Same-channel ID==name stays valid for legacy compatibility.
	for i := range fb.Channels {
		for j := range fb.Channels {
			if i == j {
				continue
			}
			if fb.Channels[i].ID == fb.Channels[j].Name {
				return fmt.Errorf("fallback channel id %q conflicts with display name %q of another channel", fb.Channels[i].ID, fb.Channels[j].Name)
			}
		}
	}
	if fb.Active != "" {
		if seenID[fb.Active] {
			// Already a stable ID.
		} else {
			// Legacy active referencing a display name: normalize to the ID.
			matched := ""
			for _, ch := range fb.Channels {
				if ch.Name == fb.Active {
					matched = ch.ID
					break
				}
			}
			if matched == "" {
				return fmt.Errorf("fallback active channel %q does not exist", fb.Active)
			}
			fb.Active = matched
		}
	}
	return nil
}

func fallbackChannelByID(cfg Config, id string) (FallbackChannelConfig, bool) {
	id = strings.TrimSpace(id)
	if id == "" {
		return FallbackChannelConfig{}, false
	}
	for _, ch := range cfg.Fallback.Channels {
		if ch.ID == id {
			return ch, true
		}
	}
	return FallbackChannelConfig{}, false
}

// fallbackChannelByName resolves by display name (legacy compat). New code
// should use fallbackChannelByID; this helper stays for legacy API payloads
// and tests where ID equals name.
func fallbackChannelByName(cfg Config, name string) (FallbackChannelConfig, bool) {
	name = strings.TrimSpace(name)
	for _, ch := range cfg.Fallback.Channels {
		if ch.Name == name {
			return ch, true
		}
	}
	return FallbackChannelConfig{}, false
}

// fallbackChannelLookup resolves a channel by stable ID first, then by legacy
// display name, so old callers sending a name keep working while new callers
// send IDs. A name-only rename never changes the ID match.
func fallbackChannelLookup(cfg Config, idOrName string) (FallbackChannelConfig, bool) {
	if ch, ok := fallbackChannelByID(cfg, idOrName); ok {
		return ch, true
	}
	return fallbackChannelByName(cfg, idOrName)
}

func activeFallbackChannel(cfg Config) (FallbackChannelConfig, bool) {
	if strings.TrimSpace(cfg.Fallback.Active) == "" {
		return FallbackChannelConfig{}, false
	}
	return fallbackChannelLookup(cfg, cfg.Fallback.Active)
}

// fallbackBinding is the session-level takeover value. It freezes the stable
// channel identity (ID plus normalized base URL/authority, key hash, model,
// protocol) so a later display-name rename, credential, model, URL, or
// protocol change cannot silently drift an old session. Name is carried as
// display-only snapshot and never participates in matching: a name-only edit
// preserves the binding.
type fallbackBinding struct {
	ID       string
	Name     string // display snapshot only, never matched
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
		ID:       strings.TrimSpace(ch.ID),
		Name:     strings.TrimSpace(ch.Name),
		BaseURL:  normalizeFallbackBaseURL(ch.BaseURL),
		KeyHash:  fallbackKeyHash(strings.TrimSpace(ch.APIKey)),
		KeyFP:    secretFingerprint(strings.TrimSpace(ch.APIKey)),
		Model:    strings.TrimSpace(ch.Model),
		Protocol: fallbackChannelProtocol(ch),
	}
}

func (b fallbackBinding) matchesChannel(ch FallbackChannelConfig) bool {
	// Stable path: match by ID; display name is ignored so renames preserve.
	if strings.TrimSpace(b.ID) != "" {
		if b.ID != strings.TrimSpace(ch.ID) {
			return false
		}
	} else {
		// Legacy tombstone (pre-ID binding carries Name only): match the
		// legacy name against the current ID or display name so derived IDs
		// (valid legacy names derive to themselves) keep serving.
		legacy := strings.TrimSpace(b.Name)
		if legacy == "" {
			return false
		}
		if legacy != strings.TrimSpace(ch.ID) && legacy != strings.TrimSpace(ch.Name) {
			return false
		}
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
		if k == "" || (strings.TrimSpace(v.ID) == "" && strings.TrimSpace(v.Name) == "") {
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

// fallbackObservabilityChannel names the observability channel for one custom
// fallback channel. Tier stays "custom"; Channel carries the stable ID
// ("custom:<channelID>") so per-channel usage/attempts split on the machine
// identity while the display name is resolved separately in operator copy.
// Old "custom" records keep their merged row and are never rewritten.
func fallbackObservabilityChannel(id string) string {
	trimmed := strings.TrimSpace(id)
	if trimmed == "" {
		return string(TierCustom)
	}
	return string(TierCustom) + ":" + trimmed
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
	display := "custom:" + strings.TrimSpace(ch.ID)
	channel := fallbackObservabilityChannel(ch.ID)
	setRequestCredential(ctx, TierCustom, channelProtocol, display, channel, false, fakeProxy)
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
	g.recordUpstreamAttemptWithClass(customRoute, channelProtocol, ids, attemptOffset+1, display, channel, false, fakeProxy, resp, sendErr, duration, class, false, false, false, badRequestDiag{})
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
