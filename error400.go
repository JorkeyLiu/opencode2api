package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
)

// Bounded 400 diagnostic: privacy-safe classification retained before the
// first 400 body is drained, and for a replay result that is also 400.
//
// Only structured metadata is retained: sanitized error type/code, a fixed
// error_hint enum, and a short domain-separated fingerprint for grouping.
// Raw message, response body, param, quoted IDs, stack traces, headers,
// request body, raw sessions, keys, and proxy credentials are never stored,
// logged, or displayed. Type/code use a strict short allowlist; unsafe values
// are omitted. Fingerprint is diagnostic only (e400_ namespace, disjoint from
// ses_/rss_/csh_/req_) and normalizes away embedded IDs/numbers so the same
// error class groups without exposing content.

const (
	// max400DiagBodyBytes matches the existing error-body convention in
	// copyErrorResponse (4MB). Diagnostics read at most this much once per
	// exact 400 response, then restore the bytes for the existing control
	// flow (drain or final client envelope). No extra upstream send occurs.
	max400DiagBodyBytes = 4 << 20
	// errorAttrMaxLen bounds sanitized type/code display values.
	errorAttrMaxLen = 64
)

const (
	ErrorHintContextLength          = "context_length"
	ErrorHintStaleResponseReference = "stale_response_reference"
	ErrorHintSessionRejected        = "session_rejected"
	ErrorHintInvalidRequest         = "invalid_request"
	ErrorHintUnknown                = "unknown"
)

// badRequestDiag is the in-memory diagnostic for one exact 400 attempt.
// Empty means non-400 (all fields omitted on the wire via omitempty).
type badRequestDiag struct {
	Hint        string
	Type        string
	Code        string
	Fingerprint string
}

func (d badRequestDiag) empty() bool {
	return d.Hint == "" && d.Type == "" && d.Code == "" && d.Fingerprint == ""
}

// sanitizeErrorAttr keeps only short allowlist type/code values. Anything
// else (empty, oversized, charset violation) is omitted as "".
func sanitizeErrorAttr(v string) string {
	v = strings.TrimSpace(v)
	if v == "" || len(v) > errorAttrMaxLen {
		return ""
	}
	for _, r := range v {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-' {
			continue
		}
		return ""
	}
	return v
}

// validErrorHint reports whether h is one of the fixed enum values.
func validErrorHint(h string) bool {
	switch h {
	case ErrorHintContextLength, ErrorHintStaleResponseReference, ErrorHintSessionRejected, ErrorHintInvalidRequest, ErrorHintUnknown:
		return true
	default:
		return false
	}
}

var (
	re400SingleQuoted = regexp.MustCompile(`'[^']*'`)
	re400DoubleQuoted = regexp.MustCompile(`"[^"]*"`)
	re400PrefixedID   = regexp.MustCompile(`\b(rs|resp|req|ses|rss|csh|conv|msg|item|call|tool|run|file|emb|cmpl|chatcmpl)_[a-z0-9_-]+\b`)
	re400UUID         = regexp.MustCompile(`\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	re400LongHex      = regexp.MustCompile(`\b[0-9a-f]{12,}\b`)
	re400Numbers      = regexp.MustCompile(`[0-9]+`)
	re400Spaces       = regexp.MustCompile(`\s+`)
)

// normalize400Message reduces a raw upstream message to a grouping-stable
// form without exposing content: lowercase, quoted segments collapsed,
// prefixed IDs/UUIDs/long hex collapsed, all numbers collapsed, whitespace
// collapsed, bounded length. The raw message itself is never retained.
func normalize400Message(msg string) string {
	s := strings.ToLower(msg)
	if len(s) > 2000 {
		s = s[:2000]
	}
	s = re400SingleQuoted.ReplaceAllString(s, "'<q>'")
	s = re400DoubleQuoted.ReplaceAllString(s, `"<q>"`)
	s = re400PrefixedID.ReplaceAllString(s, "<id>")
	s = re400UUID.ReplaceAllString(s, "<id>")
	s = re400LongHex.ReplaceAllString(s, "<id>")
	s = re400Numbers.ReplaceAllString(s, "<n>")
	s = re400Spaces.ReplaceAllString(s, " ")
	s = strings.TrimSpace(s)
	if len(s) > 500 {
		s = s[:500]
	}
	return s
}

// extract400Fields pulls type/code/message from representative upstream JSON
// envelopes without retaining raw bodies. Supported shapes:
//   - OpenAI/Responses: {"error": {"message","type","code"}}
//   - String error: {"error": "bad"}
//   - Anthropic-like: {"type":"error","error":{"type","message"}}
//   - Top-level {"message": "..."} fallback.
//
// Non-string scalars are treated as absent (stringAt returns "").
func extract400Fields(body []byte) (typ, code, message string) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", "", ""
	}
	if s, ok := payload["error"].(string); ok {
		return "", "", s
	}
	errObj := mapAt(payload, "error")
	inner := mapAt(errObj, "error")
	msg := firstString(
		stringAt(errObj, "message"),
		stringAt(inner, "message"),
		stringAt(payload, "message"),
	)
	typRaw := firstString(
		stringAt(errObj, "type"),
		stringAt(inner, "type"),
	)
	codeRaw := firstString(
		stringAt(errObj, "code"),
		stringAt(inner, "code"),
	)
	return typRaw, codeRaw, msg
}

// classify400Hint maps sanitized type/code plus raw message (matched
// case-insensitively, never retained) to the fixed enum. Order is most
// specific first; anything without a conservative match is unknown.
func classify400Hint(typ, code, message string) string {
	t := strings.ToLower(typ)
	c := strings.ToLower(code)
	m := strings.ToLower(message)
	// Context length: explicit token or structured marker, or context plus a
	// capacity signal. Checked first so capacity errors with session words
	// still group as context_length.
	if strings.Contains(t, "context_length") || strings.Contains(c, "context_length") ||
		strings.Contains(t, "context-length") || strings.Contains(c, "context-length") {
		return ErrorHintContextLength
	}
	if strings.Contains(m, "context_length") || strings.Contains(m, "context-length") {
		return ErrorHintContextLength
	}
	if strings.Contains(m, "context") && (strings.Contains(m, "exceed") || strings.Contains(m, "limit") || strings.Contains(m, "maximum") || strings.Contains(m, "too many") || strings.Contains(m, "too long") || strings.Contains(m, "window")) {
		return ErrorHintContextLength
	}
	if strings.Contains(m, "maximum context") || strings.Contains(m, "context window") || strings.Contains(m, "too many tokens") || strings.Contains(m, "token limit") || strings.Contains(m, "reduce the length") || strings.Contains(m, "max_tokens") || strings.Contains(m, "max tokens") {
		return ErrorHintContextLength
	}
	// Stale response reference: previous_response chain link or reasoning
	// items that the replay drops. Quoted IDs are matched structurally here
	// but never retained.
	if strings.Contains(m, "previous_response") || strings.Contains(m, "previous response") ||
		strings.Contains(t, "previous_response") || strings.Contains(c, "previous_response") {
		return ErrorHintStaleResponseReference
	}
	if strings.Contains(t, "reasoning") || strings.Contains(c, "reasoning") {
		return ErrorHintStaleResponseReference
	}
	if strings.Contains(m, "reasoning") && (strings.Contains(m, "not found") || strings.Contains(m, "expired") || strings.Contains(m, "does not exist") || strings.Contains(m, "invalid") || strings.Contains(m, "unknown")) {
		return ErrorHintStaleResponseReference
	}
	// Session rejected: route-session / conversation affinity signals.
	if strings.Contains(t, "session") || strings.Contains(c, "session") {
		return ErrorHintSessionRejected
	}
	if (strings.Contains(m, "session") || strings.Contains(m, "conversation_id") || strings.Contains(m, "session_id") || strings.Contains(m, "affinity")) &&
		(strings.Contains(m, "invalid") || strings.Contains(m, "expired") || strings.Contains(m, "not found") || strings.Contains(m, "unknown") || strings.Contains(m, "rejected") || strings.Contains(m, "mismatch") || strings.Contains(m, "does not exist")) {
		return ErrorHintSessionRejected
	}
	// Invalid request: explicit invalid/validation markers only. Generic
	// 400s without such markers stay unknown to avoid over-classification.
	if strings.Contains(t, "invalid_request") || strings.Contains(c, "invalid_request") ||
		strings.Contains(t, "invalid-request") || strings.Contains(c, "invalid-request") ||
		strings.Contains(t, "validation") || strings.Contains(c, "validation") {
		return ErrorHintInvalidRequest
	}
	if strings.Contains(m, "invalid_request") || strings.Contains(m, "invalid request") || strings.Contains(m, "validation") || strings.Contains(m, "malformed") {
		return ErrorHintInvalidRequest
	}
	return ErrorHintUnknown
}

// classify400Body builds the full diagnostic from one bounded body. It never
// returns raw content: only sanitized type/code, fixed hint, and the grouped
// fingerprint. Malformed/non-JSON bodies yield unknown with a stable empty
// fingerprint and no raw leakage.
func classify400Body(body []byte) badRequestDiag {
	typRaw, codeRaw, msg := extract400Fields(body)
	typ := sanitizeErrorAttr(typRaw)
	code := sanitizeErrorAttr(codeRaw)
	hint := classify400Hint(typ, code, msg)
	msgNorm := normalize400Message(msg)
	fp := stableID("e400", strings.ToLower(typ)+"\x00"+strings.ToLower(code)+"\x00"+msgNorm)
	return badRequestDiag{Hint: hint, Type: typ, Code: code, Fingerprint: fp}
}

// diagLogArgs renders one diagnostic as structured log args with an optional
// prefix ("" for the triggering 400, "replay_" for the replay 400). Empty
// diagnostics yield no args; raw message/body never appears. Type/code are
// already sanitized allowlist values, hint is the fixed enum, fingerprint is
// the short e400_ hash.
func diagLogArgs(d badRequestDiag, prefix string) []any {
	if d.empty() {
		return nil
	}
	args := []any{}
	if d.Hint != "" {
		args = append(args, prefix+"error_hint", d.Hint)
	}
	if d.Type != "" {
		args = append(args, prefix+"error_type", d.Type)
	}
	if d.Code != "" {
		args = append(args, prefix+"error_code", d.Code)
	}
	if d.Fingerprint != "" {
		args = append(args, prefix+"error_fingerprint", d.Fingerprint)
	}
	return args
}

// isErrorFingerprint validates the short e400_ domain hash shape
// (e400_ + 24 lowercase hex from stableID). It keeps the ingestion point
// from persisting malformed fingerprints.
func isErrorFingerprint(v string) bool {
	if len(v) != len("e400_")+24 {
		return false
	}
	if !strings.HasPrefix(v, "e400_") {
		return false
	}
	for _, r := range v[len("e400_"):] {
		if r >= '0' && r <= '9' || r >= 'a' && r <= 'f' {
			continue
		}
		return false
	}
	return true
}

// peek400Diag reads one bounded exact-400 body and restores it for the
// existing control flow. On non-400 (or nil/transport error) it returns empty
// without touching the body. The restored body preserves the substantively
// same bytes (up to the 4MB error convention) so the final client envelope
// and drain behavior are unchanged. No extra upstream send occurs.
func peek400Diag(resp *http.Response) badRequestDiag {
	if resp == nil || resp.Body == nil || resp.StatusCode != http.StatusBadRequest {
		return badRequestDiag{}
	}
	data, _ := io.ReadAll(io.LimitReader(resp.Body, max400DiagBodyBytes))
	_ = resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(data))
	return classify400Body(data)
}
