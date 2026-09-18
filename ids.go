package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"runtime"
	"strings"
)

type requestIDs struct {
	Session       string
	Request       string
	Project       string
	ParentSession string
}

func deriveRequestIDs(r *http.Request, body map[string]any) requestIDs {
	signal := firstString(
		r.Header.Get("x-opencode-session"),
		r.Header.Get("x-session-affinity"),
		r.Header.Get("X-Session-Id"),
		r.Header.Get("x-session-id"),
		r.Header.Get("conversation-id"),
		stringAt(body, "conversation_id"),
		stringAt(body, "metadata", "session_id"),
	)
	if signal == "" {
		// Using the first user turn keeps a multi-turn conversation stable as its
		// history grows while separating conversations with different beginnings.
		signal = conversationSeed(body)
	}
	if signal == "" {
		signal = stringAt(body, "previous_response_id")
	}
	if signal == "" || signal == `{}` {
		signal = randomID("fallback", 16)
	}
	session := stableID("ses", signal)
	projectSignal := firstString(r.Header.Get("x-opencode-project"), stringAt(body, "metadata", "project_id"))
	if projectSignal == "" {
		projectSignal = "opencode2api:default-project"
	}
	parentSession := firstString(
		r.Header.Get("x-parent-session-id"),
		stringAt(body, "metadata", "parent_session_id"),
	)
	return requestIDs{
		Session:       session,
		Request:       randomID("req", 16),
		Project:       stableID("prj", projectSignal),
		ParentSession: parentSession,
	}
}

func conversationSeed(body map[string]any) string {
	if input, ok := body["input"].(string); ok && input != "" {
		return input
	}
	for _, field := range []string{"messages", "input"} {
		for _, raw := range sliceAt(body, field) {
			item, ok := raw.(map[string]any)
			if !ok || stringAt(item, "role") != "user" {
				continue
			}
			encoded, _ := json.Marshal(item["content"])
			if len(encoded) > 0 && string(encoded) != "null" {
				return string(encoded)
			}
		}
	}
	return ""
}

func stableID(prefix, value string) string {
	sum := sha256.Sum256([]byte(prefix + "\x00" + value))
	return prefix + "_" + hex.EncodeToString(sum[:12])
}

// clientSessionHash is the bounded, privacy-safe diagnostic correlator for
// the internally derived client session (ids.Session). It is domain-separated
// (csh namespace, disjoint from ses_/rss_) and derived only from ids.Session,
// never from raw client signals, bodies, or secrets. The output makes clear
// it is a hash, not the raw session; empty input stays empty so missing
// sessions are omitted from diagnostics.
func clientSessionHash(session string) string {
	if session == "" {
		return ""
	}
	return stableID("csh", session)
}

func randomID(prefix string, size int) string {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return prefix + "_" + hex.EncodeToString(buf)
}

func firstString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

// genericFetchUserAgent is the generic fetch UA for non-upstream public
// reads only (capability directory, endpoint docs, models.dev, proxy
// transport probes). It is explicitly NOT the canonical upstream OpenCode
// wire identity: only opencodeWireUserAgent defines that standard and only
// the shared OpenCode request authorities may present it upstream.
func genericFetchUserAgent() string {
	return fmt.Sprintf("opencode/1.18.21 (%s %s; %s)", runtime.GOOS, runtime.GOARCH, runtime.Version())
}

// setOpenCodeWireHeaders applies the single canonical OpenCode wire header set
// shared by native Zen inference, custom fallback inference, and custom
// minimal-inference probes: bare official UA, x-opencode-client, canonical
// pseudonymous session/request/project derived from the internal IDs plus the
// target-bound internal route token, and the optional canonical parent.
// Callers set Content-Type/Accept/auth separately per target rules. The raw
// client session, secrets, and body content never leave through these headers:
// session/request/project are stable pseudonymous mappings only.
func setOpenCodeWireHeaders(h http.Header, ids requestIDs, routeSession string) {
	if h == nil {
		return
	}
	h.Set("User-Agent", opencodeWireUserAgent())
	h.Set("x-opencode-client", "cli")
	h.Set("x-opencode-session", routeWireSession(routeSession))
	h.Set("x-opencode-request", requestWireID(ids.Request))
	h.Set("x-opencode-project", projectWireID(ids.Project))
	if parent := parentWireSession(ids.ParentSession); parent != "" {
		h.Set("x-parent-session-id", parent)
	}
}

// setOpenCodePublicHeaders applies the sessionless public subset for model
// discovery GETs: bare official UA plus x-opencode-client only. Discovery must
// not forge an inference session requirement.
func setOpenCodePublicHeaders(h http.Header) {
	if h == nil {
		return
	}
	h.Set("User-Agent", opencodeWireUserAgent())
	h.Set("x-opencode-client", "cli")
}

// newOpenCodeDiscoveryRequest is the single shared centralized authority for
// all upstream OpenCode model-discovery GETs (Zen and custom fallback). It
// owns the discovery category policy: sessionless public identity subset
// (bare canonical wire UA plus x-opencode-client, never session/request/
// project/parent) plus the target Bearer auth. Non-OpenCode data-directory
// fetches (models.opencode.ai, models.dev, GitHub docs) and proxy transport
// probes are not OpenCode discovery and stay outside this authority.
func newOpenCodeDiscoveryRequest(ctx context.Context, endpoint, apiKey string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	setOpenCodePublicHeaders(req.Header)
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(apiKey))
	return req, nil
}

// newZenDiscoveryRequest builds the Zen model-discovery GET through the
// shared discovery authority. The endpoint derives from the Zen base URL with
// the plain /v1/models suffix.
func newZenDiscoveryRequest(ctx context.Context, baseURL, apiKey string) (*http.Request, error) {
	endpoint := strings.TrimRight(strings.TrimSpace(baseURL), "/") + "/v1/models"
	return newOpenCodeDiscoveryRequest(ctx, endpoint, apiKey)
}

// newFallbackDiscoveryRequest builds a custom fallback channel
// model-discovery GET through the shared discovery authority. The endpoint
// derives from the channel base URL with the unified API-root rule so host,
// /v1, and full inference endpoints all resolve to /v1/models.
func newFallbackDiscoveryRequest(ctx context.Context, baseURL, apiKey string) (*http.Request, error) {
	endpoint := fallbackModelsURL(strings.TrimSpace(baseURL))
	if endpoint == "" {
		return nil, fmt.Errorf("fallback channel base_url must not be empty")
	}
	return newOpenCodeDiscoveryRequest(ctx, endpoint, apiKey)
}

// openCodeWireVersion is the current official OpenCode release tracked for the
// canonical upstream OpenCode wire identity. Every upstream OpenCode egress
// presents this bare version through the single shared construction authority;
// per-category header differences are category policies inside that authority.
// Non-upstream public reads use the independent generic fetch UA instead.
const openCodeWireVersion = "1.18.31"

// opencodeWireUserAgent returns the bare official OpenCode wire identity
// (e.g. "opencode/1.18.31"). Official v1.18.31 sends no platform suffix.
func opencodeWireUserAgent() string {
	return "opencode/" + openCodeWireVersion
}

// Canonical OpenCode wire identity shapes (official v1.18.31 Zen path):
// ses_<12 lowercase hex><14 base62> and msg_ with the same shape, plus a
// 40 lowercase hex project hash. The 12-hex prefix is an opaque stable hash
// fragment, not a real timestamp; the base62 tail is fixed-width over
// alphabet 0-9A-Za-z with leading zeroes preserved.
const (
	wireSessionPrefix = "ses_"
	wireRequestPrefix = "msg_"
	wireHexLen        = 12
	wireB62Len        = 14
	wireProjectHexLen = 40
)

const base62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func isLowerHex(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func isBase62(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z') {
			return false
		}
	}
	return true
}

// isCanonicalWireSession reports the exact official session shape:
// ses_ + 12 lowercase hex + 14 base62 (total 30 chars).
func isCanonicalWireSession(s string) bool {
	if len(s) != len(wireSessionPrefix)+wireHexLen+wireB62Len {
		return false
	}
	if !strings.HasPrefix(s, wireSessionPrefix) {
		return false
	}
	rest := s[len(wireSessionPrefix):]
	return isLowerHex(rest[:wireHexLen]) && isBase62(rest[wireHexLen:])
}

// isCanonicalWireRequest reports the exact official request shape:
// msg_ + 12 lowercase hex + 14 base62 (total 30 chars).
func isCanonicalWireRequest(s string) bool {
	if len(s) != len(wireRequestPrefix)+wireHexLen+wireB62Len {
		return false
	}
	if !strings.HasPrefix(s, wireRequestPrefix) {
		return false
	}
	rest := s[len(wireRequestPrefix):]
	return isLowerHex(rest[:wireHexLen]) && isBase62(rest[wireHexLen:])
}

// isCanonicalWireProject reports the official-shaped project hash:
// 40 lowercase hex.
func isCanonicalWireProject(s string) bool {
	return len(s) == wireProjectHexLen && isLowerHex(s)
}

func wireHash(domain, signal string) [32]byte {
	return sha256.Sum256([]byte(domain + "\x00" + signal))
}

// base62Fixed encodes data (big-endian) as fixed-width base62 over
// 0-9A-Za-z, left-padded with '0' to preserve leading zeroes. Inputs wider
// than the width keep the least-significant width chars so output length is
// always exactly width.
func base62Fixed(data []byte, width int) string {
	if width <= 0 {
		return ""
	}
	n := new(big.Int).SetBytes(data)
	if n.Sign() == 0 {
		return strings.Repeat("0", width)
	}
	base := big.NewInt(62)
	mod := new(big.Int)
	var digits []byte
	for n.Sign() > 0 {
		n.DivMod(n, base, mod)
		digits = append(digits, base62Alphabet[mod.Int64()])
	}
	for i, j := 0, len(digits)-1; i < j; i, j = i+1, j-1 {
		digits[i], digits[j] = digits[j], digits[i]
	}
	if len(digits) > width {
		digits = digits[len(digits)-width:]
	}
	if len(digits) < width {
		pad := make([]byte, width-len(digits))
		for i := range pad {
			pad[i] = '0'
		}
		digits = append(pad, digits...)
	}
	return string(digits)
}

func buildWireSession(signal, domain string) string {
	sum := wireHash(domain, signal)
	hexPart := hex.EncodeToString(sum[:6])
	b62 := base62Fixed(sum[6:16], wireB62Len)
	return wireSessionPrefix + hexPart + b62
}

func buildWireRequest(signal, domain string) string {
	sum := wireHash(domain, signal)
	hexPart := hex.EncodeToString(sum[:6])
	b62 := base62Fixed(sum[6:16], wireB62Len)
	return wireRequestPrefix + hexPart + b62
}

func buildWireProject(signal, domain string) string {
	sum := wireHash(domain, signal)
	return hex.EncodeToString(sum[:20])
}

// routeWireSession maps the internal target-bound route token (rss_*) to the
// canonical OpenCode-shaped pseudonymous wire session. It always derives via
// a dedicated domain so the wire value stays target-bound and never passes a
// raw client session through, even when the input already looks canonical.
func routeWireSession(routeToken string) string {
	return buildWireSession(routeToken, "route-wire-session-v1")
}

// requestWireID maps the internal request ID to the canonical msg_* wire ID.
func requestWireID(internalReqID string) string {
	return buildWireRequest(internalReqID, "wire-request-v1")
}

// projectWireID maps the internal project token to the official-shaped
// 40-hex pseudonymous project value.
func projectWireID(internalProject string) string {
	return buildWireProject(internalProject, "wire-project-v1")
}

// parentWireSession maps the raw parent signal to a canonical pseudonymous
// session. Empty input stays empty so missing parents are omitted upstream.
func parentWireSession(rawParent string) string {
	trimmed := strings.TrimSpace(rawParent)
	if trimmed == "" {
		return ""
	}
	return buildWireSession(trimmed, "wire-parent-v1")
}

// Deterministic bulk-probe internal identity: fixed signals, stateless, never
// touching scheduler route sessions or pins. Probes reuse the normal gateway
// construction path: the internal IDs below are mapped to wire values only
// through requestWireID/projectWireID, and the upstream route session is the
// stateless first generation deriveFirstRouteSession for the probe client
// session plus the target-bound scope, encoded via routeWireSession. Header
// and body therefore share one internally consistent canonical triple.
const (
	bulkProbeClientSessionSignal = "opencode2api:bulk-probe:session:v1"
	bulkProbeRequestSignal       = "opencode2api:bulk-probe:request:v1"
	bulkProbeProjectSignal       = "opencode2api:bulk-probe:project:v1"
)

// bulkProbeIDs returns the fixed scheduler-neutral internal identity for
// availability minimal-inference probes.
func bulkProbeIDs() requestIDs {
	return requestIDs{
		Session: bulkProbeClientSessionSignal,
		Request: bulkProbeRequestSignal,
		Project: bulkProbeProjectSignal,
	}
}
