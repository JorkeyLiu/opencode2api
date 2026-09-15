package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func mustDiag(t *testing.T, body string) badRequestDiag {
	t.Helper()
	return classify400Body([]byte(body))
}

func TestError400Envelopes(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		wantHint string
		wantType string
		wantCode string
	}{
		{"openai_chat", `{"error":{"message":"This model's maximum context length is 128000 tokens","type":"invalid_request_error","code":"context_length_exceeded"}}`, ErrorHintContextLength, "invalid_request_error", "context_length_exceeded"},
		{"responses_stale", `{"error":{"message":"Referenced reasoning item 'rs_abc123' was not found or has expired","type":"invalid_request_error"}}`, ErrorHintStaleResponseReference, "invalid_request_error", ""},
		{"anthropic_session", `{"type":"error","error":{"type":"invalid_request_error","message":"Invalid session_id 'ses_xyz': expired"}}`, ErrorHintSessionRejected, "invalid_request_error", ""},
		{"invalid_generic", `{"error":{"message":"Invalid request: missing required field","type":"invalid_request_error","code":"invalid_request"}}`, ErrorHintInvalidRequest, "invalid_request_error", "invalid_request"},
		{"string_error_unknown", `{"error":"bad"}`, ErrorHintUnknown, "", ""},
		{"top_message_unknown", `{"message":"something broke"}`, ErrorHintUnknown, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := mustDiag(t, tc.body)
			if d.Hint != tc.wantHint {
				t.Fatalf("hint=%q want %q (diag=%+v)", d.Hint, tc.wantHint, d)
			}
			if d.Type != tc.wantType || d.Code != tc.wantCode {
				t.Fatalf("type/code=%q/%q want %q/%q", d.Type, d.Code, tc.wantType, tc.wantCode)
			}
			if !isErrorFingerprint(d.Fingerprint) {
				t.Fatalf("bad fingerprint %q", d.Fingerprint)
			}
			// Wire omission: non-empty hint/fingerprint present, empty type/code omitted.
			data, _ := json.Marshal(UpstreamAttempt{RequestID: "r", Model: "m", Tier: "zen", Attempt: 1, Channel: "key", Status: 400, FailureClass: AttemptClassClientRejected, ErrorHint: d.Hint, ErrorType: d.Type, ErrorCode: d.Code, ErrorFingerprint: d.Fingerprint})
			s := string(data)
			if !strings.Contains(s, `"error_hint"`) || !strings.Contains(s, `"error_fingerprint"`) {
				t.Fatalf("400 must serialize hint/fingerprint: %s", s)
			}
			if tc.wantType == "" && strings.Contains(s, `"error_type"`) {
				t.Fatalf("empty type must omit: %s", s)
			}
			if tc.wantCode == "" && strings.Contains(s, `"error_code"`) {
				t.Fatalf("empty code must omit: %s", s)
			}
		})
	}
}

func TestError400QuotedIDsGroupedNoLeak(t *testing.T) {
	a := mustDiag(t, `{"error":{"message":"Referenced reasoning item 'rs_111' was not found or has expired","type":"invalid_request_error"}}`)
	b := mustDiag(t, `{"error":{"message":"Referenced reasoning item 'rs_999_xyz' was not found or has expired","type":"invalid_request_error"}}`)
	if a.Hint != ErrorHintStaleResponseReference || b.Hint != ErrorHintStaleResponseReference {
		t.Fatalf("hints=%q/%q", a.Hint, b.Hint)
	}
	if a.Fingerprint != b.Fingerprint {
		t.Fatalf("same class must group: %q vs %q", a.Fingerprint, b.Fingerprint)
	}
	// Numbers collapse too: token counts differ but group.
	c := mustDiag(t, `{"error":{"message":"maximum context length is 128000 tokens, you requested 150000","code":"context_length_exceeded"}}`)
	d := mustDiag(t, `{"error":{"message":"maximum context length is 200000 tokens, you requested 250000","code":"context_length_exceeded"}}`)
	if c.Fingerprint != d.Fingerprint {
		t.Fatalf("counts must group: %q vs %q", c.Fingerprint, d.Fingerprint)
	}
	for _, diag := range []badRequestDiag{a, b, c, d} {
		for _, needle := range []string{"rs_111", "rs_999_xyz", "128000", "150000", "reasoning item 'rs"} {
			// Fingerprint/type/code/hint must not embed raw IDs/numbers.
			blob := diag.Hint + "|" + diag.Type + "|" + diag.Code + "|" + diag.Fingerprint
			if strings.Contains(strings.ToLower(blob), strings.ToLower(needle)) {
				t.Fatalf("raw leaked %q in %q", needle, blob)
			}
		}
	}
	// Attempt JSON must not leak quoted IDs either.
	att := UpstreamAttempt{RequestID: "r", Model: "m", Tier: "zen", Attempt: 1, Channel: "key", Status: 400, FailureClass: AttemptClassClientRejected, ErrorHint: a.Hint, ErrorType: a.Type, ErrorCode: a.Code, ErrorFingerprint: a.Fingerprint}
	data, _ := json.Marshal(att)
	if strings.Contains(string(data), "rs_111") || strings.Contains(string(data), "rs_999") {
		t.Fatalf("attempt leaks quoted ID: %s", data)
	}
}

func TestError400UnsafeOversizedOmitted(t *testing.T) {
	d := mustDiag(t, `{"error":{"message":"bad","type":"bad;type<script>","code":"`+strings.Repeat("x", 100)+`"}}`)
	if d.Type != "" || d.Code != "" {
		t.Fatalf("unsafe must omit, got %q/%q", d.Type, d.Code)
	}
	if d.Hint == "" || !isErrorFingerprint(d.Fingerprint) {
		t.Fatalf("hint/fingerprint must remain: %+v", d)
	}
	// Numeric scalars omitted.
	d2 := mustDiag(t, `{"error":{"message":"bad","type":123,"code":true}}`)
	if d2.Type != "" || d2.Code != "" {
		t.Fatalf("non-string must omit: %+v", d2)
	}
	data, _ := json.Marshal(UpstreamAttempt{RequestID: "r", Model: "m", Tier: "zen", Attempt: 1, Channel: "key", Status: 400, FailureClass: AttemptClassClientRejected, ErrorHint: d.Hint, ErrorType: d.Type, ErrorCode: d.Code, ErrorFingerprint: d.Fingerprint})
	if strings.Contains(string(data), "error_type") || strings.Contains(string(data), "error_code") {
		t.Fatalf("unsafe must omit on wire: %s", data)
	}
}

func TestError400FirstThen200OnlyFirst(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	calls := 0
	stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return responseWithBody(400, `{"error":{"message":"maximum context length exceeded","type":"invalid_request_error","code":"context_length_exceeded"}}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})
	route := authOnlyRoute()
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp, _, err := gateway.doUpstream(ctx, route, routeBodies(), clientSessionIDs("ses_diag_400_200"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	drainAndClose(resp.Body)
	if calls != 2 {
		t.Fatalf("calls=%d want 2 (no extra send)", calls)
	}
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) != 2 {
		t.Fatalf("attempts=%d", len(recent))
	}
	first, replay := recent[0], recent[1]
	if first.ErrorHint != ErrorHintContextLength || !isErrorFingerprint(first.ErrorFingerprint) {
		t.Fatalf("first must carry diag: %+v", first)
	}
	if replay.ErrorHint != "" || replay.ErrorFingerprint != "" || replay.ErrorType != "" || replay.ErrorCode != "" {
		t.Fatalf("replay 200 must omit: %+v", replay)
	}
	// Request rows never carry raw error metadata.
	m := NewMonitor()
	m.Record("/v1/chat/completions", 200, time.Millisecond, &requestMeta{Model: "m", Tier: "zen", Protocol: "chat", Request: "req-400-200", Channel: "key", KeyID: "KKKKK", Attempts: 2})
	for _, r := range m.Snapshot().Upstream.Requests {
		if r.RequestID == "req-400-200" {
			data, _ := json.Marshal(r)
			if strings.Contains(string(data), "error_hint") || strings.Contains(string(data), "error_fingerprint") {
				t.Fatalf("request must not carry diag: %s", data)
			}
		}
	}
}

func TestError400Then400DistinctFinalUnchanged(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	firstBody := `{"error":{"message":"Referenced reasoning item 'rs_first' was not found","type":"invalid_request_error"}}`
	replayBody := `{"error":{"message":"Invalid session_id 'abc': expired","type":"invalid_request_error"}}`
	calls := 0
	stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return responseWithBody(400, firstBody), nil
		}
		return responseWithBody(400, replayBody), nil
	})
	route := authOnlyRoute()
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp, _, err := gateway.doUpstream(ctx, route, routeBodies(), clientSessionIDs("ses_diag_400_400"))
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.StatusCode != 400 {
		t.Fatalf("status=%v", resp)
	}
	if calls != 2 {
		t.Fatalf("calls=%d want exactly 2", calls)
	}
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) != 2 {
		t.Fatalf("attempts=%d", len(recent))
	}
	first, replay := recent[0], recent[1]
	if first.ErrorHint != ErrorHintStaleResponseReference {
		t.Fatalf("first hint=%q", first.ErrorHint)
	}
	if replay.ErrorHint != ErrorHintSessionRejected {
		t.Fatalf("replay hint=%q", replay.ErrorHint)
	}
	if first.ErrorFingerprint == "" || replay.ErrorFingerprint == "" || first.ErrorFingerprint == replay.ErrorFingerprint {
		t.Fatalf("distinct per-attempt fingerprints required: %q vs %q", first.ErrorFingerprint, replay.ErrorFingerprint)
	}
	// Final client envelope substantively same as upstream replay message.
	raw, _ := io.ReadAll(resp.Body)
	drainAndClose(resp.Body)
	if len(raw) == 0 {
		t.Fatalf("final 400 body must remain returnable")
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("final body must be valid JSON for envelope check: %v", err)
	}
	// Simulate copyErrorResponse message extraction on the restored body.
	msg := firstString(stringAt(payload, "error", "message"), stringAt(payload, "message"), "")
	if !strings.Contains(strings.ToLower(msg), "session") {
		t.Fatalf("final message must be replay's session error, got %q", msg)
	}
	if strings.Contains(string(raw), "rs_first") {
		t.Fatalf("final body must not be first body")
	}
	// No raw leakage into attempt JSON beyond sanitized fields.
	for _, a := range recent {
		data, _ := json.Marshal(a)
		if strings.Contains(string(data), "rs_first") || strings.Contains(string(data), "Referenced reasoning") {
			t.Fatalf("attempt leaks raw message: %s", data)
		}
	}
}

func TestError400MalformedNoLeak(t *testing.T) {
	for _, body := range []string{"not json {{{", "", `{"error":123}`, `{"weird":[]}`} {
		d := classify400Body([]byte(body))
		if d.Hint != ErrorHintUnknown {
			t.Fatalf("body %q hint=%q want unknown", body, d.Hint)
		}
		if !isErrorFingerprint(d.Fingerprint) {
			t.Fatalf("body %q bad fp %q", body, d.Fingerprint)
		}
		att := UpstreamAttempt{RequestID: "r", Model: "m", Tier: "zen", Attempt: 1, Channel: "key", Status: 400, FailureClass: AttemptClassClientRejected, ErrorHint: d.Hint, ErrorType: d.Type, ErrorCode: d.Code, ErrorFingerprint: d.Fingerprint}
		data, _ := json.Marshal(att)
		if body != "" && strings.Contains(string(data), body) {
			t.Fatalf("raw body leaked: %q in %s", body, data)
		}
	}
	// Live malformed 400 still records unknown without leak and restores body.
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
		return responseWithBody(400, `not json {{{`), nil
	})
	// Force no replay success: second also malformed 400.
	calls := 0
	_ = calls
	route := modelRoute{ID: "m", Tier: TierZen, Protocol: ProtocolChat, Protocols: map[Tier]Protocol{TierZen: ProtocolChat}, Anonymous: false, KeyTiers: []Tier{TierZen}}
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp, _, err := gateway.doUpstream(ctx, route, routeBodies(), clientSessionIDs("ses_diag_malformed"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 400 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	drainAndClose(resp.Body)
	if string(raw) != `not json {{{` {
		t.Fatalf("malformed body must round-trip, got %q", raw)
	}
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) != 2 {
		t.Fatalf("attempts=%d", len(recent))
	}
	if recent[0].ErrorHint != ErrorHintUnknown || recent[1].ErrorHint != ErrorHintUnknown {
		t.Fatalf("malformed must be unknown: %+v", recent)
	}
}

func TestError400Non400Omit(t *testing.T) {
	m := NewMonitor()
	m.RecordAttempt(UpstreamAttempt{Time: time.Now().UTC(), RequestID: "r404", Model: "m", Tier: "zen", Attempt: 1, KeyID: "K", Channel: "key", Proxy: "direct", Status: 404, Success: false, FailureClass: AttemptClassClientRejected, ErrorHint: ErrorHintUnknown, ErrorFingerprint: stableID("e400", "x")})
	got := m.Snapshot().Upstream.Recent[len(m.Snapshot().Upstream.Recent)-1]
	if got.ErrorHint != "" || got.ErrorFingerprint != "" || got.ErrorType != "" || got.ErrorCode != "" {
		t.Fatalf("non-400 must omit: %+v", got)
	}
	data, _ := json.Marshal(got)
	if strings.Contains(string(data), "error_hint") || strings.Contains(string(data), "error_fingerprint") {
		t.Fatalf("non-400 wire must omit: %s", data)
	}
}

func TestError400HistoryWebUICompat(t *testing.T) {
	now := time.Now().UTC()
	d := mustDiag(t, `{"error":{"message":"maximum context length exceeded","type":"invalid_request_error","code":"context_length_exceeded"}}`)
	// New line round-trips.
	line := historyAttemptLine{V: historySchemaV, Kind: string(historyKindAttempt), Time: now.UTC().Format(time.RFC3339Nano), RequestID: "h400", Model: "m", Tier: "zen", Protocol: "chat", ClientSessionHash: "csh_x", Attempt: 1, Channel: "key", Status: 400, DurationMS: 1, Success: false, FailureClass: AttemptClassClientRejected, ErrorHint: d.Hint, ErrorType: d.Type, ErrorCode: d.Code, ErrorFingerprint: d.Fingerprint}
	data, _ := json.Marshal(line)
	var decoded historyAttemptLine
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ErrorHint != ErrorHintContextLength || decoded.ErrorFingerprint != d.Fingerprint {
		t.Fatalf("history round-trip failed: %+v", decoded)
	}
	// Old lines decode with zero values.
	var old historyAttemptLine
	if err := json.Unmarshal([]byte(`{"v":1,"kind":"attempt","time":"`+now.UTC().Format(time.RFC3339Nano)+`","request_id":"old400","model":"m","attempt":1,"channel":"key","duration_ms":1,"success":false}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.ErrorHint != "" || old.ErrorFingerprint != "" || old.ErrorType != "" || old.ErrorCode != "" {
		t.Fatalf("old must zero-fill: %+v", old)
	}
	// Store path preserves diag without raw leakage.
	dir := t.TempDir()
	cfgPath := dir + "/config.json"
	_ = os.WriteFile(cfgPath, []byte("{}"), 0600)
	store := OpenHistoryStore(cfgPath, HistoryConfig{Enabled: true, Directory: "h", RetentionDays: 7, MaxBytesMB: 128}, nil, NewSecretRedactor())
	defer store.Close()
	store.EnqueueAttempt(UpstreamAttempt{Time: now, RequestID: "h400", Model: "m", Tier: "zen", Protocol: "chat", ClientSessionHash: clientSessionHash("ses_hist_400"), Attempt: 1, KeyID: "K", Channel: "key", Proxy: "direct", Status: 400, DurationMS: 1, Success: false, FailureClass: AttemptClassClientRejected, ErrorHint: d.Hint, ErrorType: d.Type, ErrorCode: d.Code, ErrorFingerprint: d.Fingerprint})
	// WebUI contract: backend keeps error_hint/type/code/fingerprint fields, but
	// the static bundle no longer renders Request ID / replay / HTTP400-diagnosis
	// columns. Tables share the compact 11-column header and status cell.
	htmlBytes, err := os.ReadFile("webui/index.html")
	if err != nil {
		t.Fatal(err)
	}
	html := string(htmlBytes)
	for _, stale := range []string{"<th>Request ID</th>", "<th>重放</th>", "HTTP 400 诊断", "400_diag", "diag400Cell", "diag400Text", "error_hint", "error_fingerprint", "分组指纹"} {
		if strings.Contains(html, stale) {
			t.Fatalf("removed HTTP400 diagnosis UI must stay removed: %q", stale)
		}
	}
	if strings.Count(html, "<th>时间</th><th>模型</th><th>上游</th>") < 2 {
		t.Fatal("live and persisted tables must share the header without HTTP400 diagnosis")
	}
	for _, needle := range []string{"attemptRowCells", "pillForFailureClass(fc)", "failureLabel(fc)", "失败分类：", "HTTP 状态 ", `+" ms"`} {
		if !strings.Contains(html, needle) {
			t.Fatalf("shared row must keep compact status with title detail, missing %q", needle)
		}
	}
	if strings.Contains(html, "毫秒") {
		t.Fatal("stale duration unit 毫秒 must be removed")
	}
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write"} {
		if strings.Contains(html, sink) {
			t.Fatalf("forbidden sink %q", sink)
		}
	}
}
