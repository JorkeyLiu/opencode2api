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

func TestClientSessionHashStabilitySeparation(t *testing.T) {
	a := clientSessionHash("ses_client_abc")
	b := clientSessionHash("ses_client_abc")
	if a == "" || a != b {
		t.Fatalf("hash must be stable non-empty: %q vs %q", a, b)
	}
	if !strings.HasPrefix(a, "csh_") {
		t.Fatalf("hash must use diagnostic csh_ namespace, got %q", a)
	}
	other := clientSessionHash("ses_client_other")
	if a == other {
		t.Fatalf("different sessions must differ")
	}
	if got := clientSessionHash(""); got != "" {
		t.Fatalf("empty session must stay empty, got %q", got)
	}
	// Domain separation from ses_/rss_ namespaces.
	if strings.HasPrefix(a, "ses_") || strings.HasPrefix(a, "rss_") {
		t.Fatalf("hash must not reuse ses_/rss_ prefix: %q", a)
	}
	// No raw leakage: hash must not contain the raw signal or raw ids.
	raw := "super-secret-client-signal-123"
	ses := stableID("ses", raw)
	h := clientSessionHash(ses)
	for _, needle := range []string{raw, ses, "rss_"} {
		if needle != "" && strings.Contains(h, needle) {
			t.Fatalf("hash leaks raw material %q in %q", needle, h)
		}
	}
	if strings.Contains(h, raw) {
		t.Fatalf("hash leaks raw signal")
	}
}

func TestStaleCleanupReportsIndependently(t *testing.T) {
	// previous_response_id only.
	p1 := map[string]any{"model": "m", "previous_response_id": "resp_old"}
	dPrev, dRsn := stripResponsesStaleRefs(p1)
	if !dPrev || dRsn {
		t.Fatalf("prev-only got prev=%v rsn=%v", dPrev, dRsn)
	}
	if _, ok := p1["previous_response_id"]; ok {
		t.Fatalf("prev must be removed")
	}
	// reasoning only.
	p2 := map[string]any{"model": "m", "input": []any{map[string]any{"type": "reasoning"}, map[string]any{"type": "message"}}}
	dPrev, dRsn = stripResponsesStaleRefs(p2)
	if dPrev || !dRsn {
		t.Fatalf("reasoning-only got prev=%v rsn=%v", dPrev, dRsn)
	}
	// both.
	p3 := map[string]any{"model": "m", "previous_response_id": "x", "input": []any{map[string]any{"type": "reasoning"}}}
	dPrev, dRsn = stripResponsesStaleRefs(p3)
	if !dPrev || !dRsn {
		t.Fatalf("both got prev=%v rsn=%v", dPrev, dRsn)
	}
	// neither.
	p4 := map[string]any{"model": "m"}
	dPrev, dRsn = stripResponsesStaleRefs(p4)
	if dPrev || dRsn {
		t.Fatalf("neither got prev=%v rsn=%v", dPrev, dRsn)
	}
	// Non-Responses protocol never reports drops even with stale content.
	raw, _ := json.Marshal(map[string]any{"model": "m", "previous_response_id": "x", "input": []any{map[string]any{"type": "reasoning"}}})
	out, report, err := applyRouteSessionToBodyWithReport(raw, "rss_new", ProtocolChat, true)
	if err != nil {
		t.Fatal(err)
	}
	if report.DroppedPreviousResponseID || report.DroppedReasoningRefs {
		t.Fatalf("chat must not report drops: %+v", report)
	}
	if string(out) != string(raw) {
		t.Fatalf("chat replay must preserve stale refs byte-identically")
	}
	// Responses with no stale refs: report both false, but Responses
	// cache/store defaults still apply.
	raw2, _ := json.Marshal(map[string]any{"model": "m"})
	out2, report2, err := applyRouteSessionToBodyWithReport(raw2, "rss_new", ProtocolResponses, true)
	if err != nil {
		t.Fatal(err)
	}
	if report2.DroppedPreviousResponseID || report2.DroppedReasoningRefs {
		t.Fatalf("no-stale report must be false/false: %+v", report2)
	}
	var decoded map[string]any
	if err := json.Unmarshal(out2, &decoded); err != nil {
		t.Fatal(err)
	}
	if got := stringAt(decoded, "prompt_cache_key"); got != routeWireSession("rss_new") {
		t.Fatalf("responses must set prompt_cache_key to wire session, got %q", got)
	}
	if got, ok := decoded["store"]; !ok || got != false {
		t.Fatalf("responses must default store:false, got %v", decoded["store"])
	}
	// Legacy wrapper preserves exact body behavior.
	legacyOut, err := applyRouteSessionToBody(raw, "rss_new", ProtocolChat, true)
	if err != nil {
		t.Fatal(err)
	}
	if string(legacyOut) != string(out) {
		t.Fatalf("legacy wrapper diverged")
	}
}

func TestReplayFlagsNoStaleRefBothFalse(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	calls := 0
	stubProxy(t, gateway, "z", 0, func(r *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return responseWithBody(400, `{"error":"bad"}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})
	route := modelRoute{
		ID: "m", Tier: TierZen, Protocol: ProtocolResponses,
		Protocols: map[Tier]Protocol{TierZen: ProtocolResponses},
		Anonymous: false, KeyTiers: []Tier{TierZen},
	}
	// Canonical Responses body without any stale refs.
	encoded, _ := json.Marshal(map[string]any{"model": "m", "input": "hi"})
	bodies := map[Tier][]byte{TierZen: encoded}
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ids := clientSessionIDs("ses_diag_nostale_1")
	resp, _, err := gateway.doUpstream(ctx, route, bodies, ids)
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("status=%v", resp)
	}
	drainAndClose(resp.Body)
	if calls != 2 {
		t.Fatalf("calls=%d want 2", calls)
	}
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) != 2 {
		t.Fatalf("attempts=%d want 2", len(recent))
	}
	first, replay := recent[0], recent[1]
	if first.RouteSessionReplay || first.DroppedPreviousResponseID || first.DroppedReasoningRefs {
		t.Fatalf("first attempt must be non-replay: %+v", first)
	}
	if !replay.RouteSessionReplay {
		t.Fatalf("replay must set route_session_replay: %+v", replay)
	}
	if replay.DroppedPreviousResponseID || replay.DroppedReasoningRefs {
		t.Fatalf("no-stale replay must be false/false: %+v", replay)
	}
	if replay.Protocol != string(ProtocolResponses) {
		t.Fatalf("replay protocol=%q want responses", replay.Protocol)
	}
	wantHash := clientSessionHash(ids.Session)
	if replay.ClientSessionHash != wantHash || first.ClientSessionHash != wantHash {
		t.Fatalf("session hash mismatch: %+v", recent)
	}
	// Canonical body immutability.
	var canonical map[string]any
	if err := json.Unmarshal(bodies[TierZen], &canonical); err != nil {
		t.Fatal(err)
	}
	if _, ok := canonical["previous_response_id"]; ok {
		t.Fatalf("canonical polluted")
	}
}

func TestReplayFlagsDistinguishCategories(t *testing.T) {
	cases := []struct {
		name       string
		payload    map[string]any
		wantPrev   bool
		wantReason bool
	}{
		{"prev_only", map[string]any{"model": "m", "previous_response_id": "resp_old", "input": "hi"}, true, false},
		{"reason_only", map[string]any{"model": "m", "input": []any{map[string]any{"type": "reasoning"}, map[string]any{"type": "message", "role": "user"}}}, false, true},
		{"both", map[string]any{"model": "m", "previous_response_id": "resp_old", "input": []any{map[string]any{"type": "reasoning"}}}, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			monitor := NewMonitor()
			gateway := routing400Gateway(t, monitor)
			calls := 0
			stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
				calls++
				if calls == 1 {
					return responseWithBody(400, `{"error":"bad"}`), nil
				}
				return responseWithBody(200, `{"ok":true}`), nil
			})
			route := modelRoute{
				ID: "m", Tier: TierZen, Protocol: ProtocolResponses,
				Protocols: map[Tier]Protocol{TierZen: ProtocolResponses},
				Anonymous: false, KeyTiers: []Tier{TierZen},
			}
			encoded, _ := json.Marshal(tc.payload)
			bodies := map[Tier][]byte{TierZen: encoded}
			ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
			ids := clientSessionIDs("ses_diag_" + tc.name)
			resp, _, err := gateway.doUpstream(ctx, route, bodies, ids)
			if err != nil {
				t.Fatal(err)
			}
			drainAndClose(resp.Body)
			recent := monitor.Snapshot().Upstream.Recent
			if len(recent) != 2 {
				t.Fatalf("attempts=%d", len(recent))
			}
			replay := recent[1]
			if !replay.RouteSessionReplay {
				t.Fatalf("missing replay flag")
			}
			if replay.DroppedPreviousResponseID != tc.wantPrev || replay.DroppedReasoningRefs != tc.wantReason {
				t.Fatalf("got prev=%v rsn=%v want prev=%v rsn=%v (%+v)", replay.DroppedPreviousResponseID, replay.DroppedReasoningRefs, tc.wantPrev, tc.wantReason, replay)
			}
		})
	}
}

func TestRequestAttemptHistoryPropagationAndCompat(t *testing.T) {
	m := NewMonitor()
	now := time.Now().UTC()
	m.RecordAttempt(UpstreamAttempt{
		Time: now, RequestID: "req-diag-1", Model: "m", Tier: "zen", Protocol: "chat",
		ClientSessionHash: clientSessionHash("ses_diag_prop"), Attempt: 1,
		KeyID: "KKKKK", Channel: "key", Proxy: "direct", Status: 200, DurationMS: 5,
		Success: true, FailureClass: AttemptClassSuccess,
	})
	snap := m.Snapshot()
	if len(snap.Upstream.Recent) != 1 {
		t.Fatalf("recent=%d", len(snap.Upstream.Recent))
	}
	got := snap.Upstream.Recent[0]
	if got.Protocol != "chat" || got.ClientSessionHash != clientSessionHash("ses_diag_prop") {
		t.Fatalf("attempt propagation failed: %+v", got)
	}
	if got.RouteSessionReplay || got.DroppedPreviousResponseID || got.DroppedReasoningRefs {
		t.Fatalf("non-replay must be false: %+v", got)
	}

	// Request-level propagation via meta.
	meta := &requestMeta{Model: "m", Tier: "zen", Protocol: "responses", ClientSessionHash: clientSessionHash("ses_diag_prop"), Request: "req-diag-2", Channel: "key", KeyID: "KKKKK", Attempts: 1}
	m.Record("/v1/responses", 200, 5*time.Millisecond, meta)
	snap2 := m.Snapshot()
	found := false
	for _, r := range snap2.Upstream.Requests {
		if r.RequestID == "req-diag-2" {
			found = true
			if r.Protocol != "responses" || r.ClientSessionHash != clientSessionHash("ses_diag_prop") {
				t.Fatalf("request propagation failed: %+v", r)
			}
		}
	}
	if !found {
		t.Fatalf("request missing")
	}

	// Persistent history: new fields round-trip; old lines decode with zero values.
	dir := t.TempDir()
	cfgPath := dir + "/config.json"
	_ = os.WriteFile(cfgPath, []byte("{}"), 0600)
	store := OpenHistoryStore(cfgPath, HistoryConfig{Enabled: true, Directory: "h", RetentionDays: 7, MaxBytesMB: 128}, nil, NewSecretRedactor())
	defer store.Close()
	store.EnqueueRequest(UpstreamRequest{Time: now, RequestID: "hr1", Model: "m", Tier: "zen", Protocol: "anthropic", ClientSessionHash: clientSessionHash("ses_hist"), KeyID: "K", Channel: "key", Attempts: 1, Status: 200, DurationMS: 1, Success: true, Outcome: "success"})
	store.EnqueueAttempt(UpstreamAttempt{Time: now, RequestID: "hr1", Model: "m", Tier: "zen", Protocol: "anthropic", ClientSessionHash: clientSessionHash("ses_hist"), Attempt: 2, KeyID: "K", Channel: "key", Proxy: "direct", Status: 400, DurationMS: 1, Success: false, FailureClass: AttemptClassClientRejected, Outcome: "rejected", RouteSessionReplay: true, DroppedPreviousResponseID: true})
	// Old-format lines (no new fields) must decode with zero values.
	var oldReq historyRequestLine
	if err := json.Unmarshal([]byte(`{"v":1,"kind":"request","time":"`+now.UTC().Format(time.RFC3339Nano)+`","request_id":"old1","model":"m","channel":"key","attempts":1,"status":200,"duration_ms":1,"success":true}`), &oldReq); err != nil {
		t.Fatal(err)
	}
	if oldReq.Protocol != "" || oldReq.ClientSessionHash != "" {
		t.Fatalf("old request must zero-fill: %+v", oldReq)
	}
	var oldAtt historyAttemptLine
	if err := json.Unmarshal([]byte(`{"v":1,"kind":"attempt","time":"`+now.UTC().Format(time.RFC3339Nano)+`","request_id":"old1","model":"m","attempt":1,"channel":"key","duration_ms":1,"success":false}`), &oldAtt); err != nil {
		t.Fatal(err)
	}
	if oldAtt.RouteSessionReplay || oldAtt.DroppedPreviousResponseID || oldAtt.DroppedReasoningRefs || oldAtt.Protocol != "" {
		t.Fatalf("old attempt must zero-fill: %+v", oldAtt)
	}
	// New fields ignored by old readers: decode new line into minimal struct.
	type oldAttemptReader struct {
		V         int    `json:"v"`
		RequestID string `json:"request_id"`
		Attempt   int    `json:"attempt"`
	}
	newLine, _ := json.Marshal(historyAttemptLine{V: 1, Kind: "attempt", Time: now.UTC().Format(time.RFC3339Nano), RequestID: "new1", Model: "m", Protocol: "chat", ClientSessionHash: "csh_abc", Attempt: 2, Channel: "key", RouteSessionReplay: true, DroppedPreviousResponseID: true, DroppedReasoningRefs: true})
	var minimal oldAttemptReader
	if err := json.Unmarshal(newLine, &minimal); err != nil {
		t.Fatal(err)
	}
	if minimal.RequestID != "new1" || minimal.Attempt != 2 {
		t.Fatalf("old reader failed: %+v", minimal)
	}
	// Redaction: hash/proxy handling retains privacy (no raw session in lines).
	store.EnqueueAttempt(UpstreamAttempt{Time: now, RequestID: "sec1", Model: "m", Tier: "zen", Protocol: "chat", ClientSessionHash: clientSessionHash("ses_secret_raw_session_value"), Attempt: 1, KeyID: "sk-live-99999", Channel: "key", Proxy: "http://user:hunter2@127.0.0.1:8080", Status: 200, DurationMS: 1, Success: true, FailureClass: AttemptClassSuccess})
	_ = io.Discard
}

func TestDiagnosticsWebUIStatic(t *testing.T) {
	data, err := os.ReadFile("webui/index.html")
	if err != nil {
		t.Fatal(err)
	}
	html := string(data)
	for _, needle := range []string{"client_session_hash", "route_session_replay", "dropped_previous_response_id", "dropped_reasoning_refs", "replay", "cache_hit_tokens", "usage_reported"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("webui missing %q", needle)
		}
	}
	for _, proto := range []string{"chat", "responses", "anthropic"} {
		if !strings.Contains(html, proto) {
			t.Fatalf("webui missing protocol %q", proto)
		}
	}
	// New contract: realtime keeps the 11-column header; the duplicate
	// persisted request-history table is replaced by the proxy-stats aggregate.
	if strings.Count(html, "<th>时间</th><th>模型</th><th>上游</th>") < 1 {
		t.Fatal("realtime table must keep the 11-column header without Request ID")
	}
	for _, stale := range []string{"<th>Request ID</th>", "<th>重放</th>", "HTTP 400 诊断", "400_diag", "diag400Cell", "diag400Text", "error_hint", "error_fingerprint", "分组指纹", "HTTP 400 同目标重放一次", "单次尝试一行", "回退请求行", "请求级回退行"} {
		if strings.Contains(html, stale) {
			t.Fatalf("removed Request ID/replay/HTTP400 diagnosis UI must stay removed: %q", stale)
		}
	}
	if !strings.Contains(html, "<th>代理</th>") {
		t.Fatal("flat attempt rows must carry a 代理 column")
	}
	if !strings.Contains(html, "<th>上游</th>") || !strings.Contains(html, "<th>密钥</th>") || !strings.Contains(html, "<th>状态</th>") || !strings.Contains(html, "<th>耗时</th>") {
		t.Fatal("flat rows must carry localized 上游/密钥/状态/耗时 columns")
	}
	for _, stale := range []string{"<th>客户端会话hash</th>", "<th>会话</th>", "<th>通道</th>", "<th>缓存未命中</th>", "cache_miss_tokens", "loadHistAttempts(", "expandedRequest", "histExpanded"} {
		if strings.Contains(html, stale) {
			t.Fatalf("removed expandable/column state must stay removed: %q", stale)
		}
	}
	if !strings.Contains(html, "attempt-row") {
		t.Fatal("webui must render flat attempt rows")
	}
	if !strings.Contains(html, "attemptRowCells") {
		t.Fatal("realtime table must keep attemptRowCells")
	}
	if !strings.Contains(html, `String(a.request_id||"")`) || !strings.Contains(html, `String(a.client_session_hash||"")`) {
		t.Fatal("search must match both session hash and Request ID")
	}
	// Old records without a hash degrade to a dash, never a raw ID.
	if strings.Contains(html, "— 无hash") {
		t.Fatal("stale no-hash copy must be removed")
	}
	// Shared row: compact status code with title detail; duration uses `ms` without space.
	// Realtime 代理 column is display-only node/dash: native "代理节点：",
	// custom "自定义渠道不使用代理池节点" (never pool/node internals).
	for _, needle := range []string{"pillForFailureClass(fc)", "failureLabel(fc)", "失败分类：", "HTTP 状态 ", "上游：", "目标协议：", "代理节点：", "自定义渠道不使用代理池节点", "密钥尾码："} {
		if !strings.Contains(html, needle) {
			t.Fatalf("shared row must keep compact status with title detail, missing %q", needle)
		}
	}
	if !strings.Contains(html, `+"ms"`) {
		t.Fatal("duration must use `+\"ms\"`")
	}
	if strings.Contains(html, `+" ms"`) {
		t.Fatal("duration must not contain spaced `+\" ms\"`")
	}
	if strings.Contains(html, "毫秒") {
		t.Fatal("stale duration unit 毫秒 must be removed")
	}
	// Cache labels: hit only, unknown stays a dash with neutral wording.
	for _, needle := range []string{"缓存命中", "非最终尝试显示"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("webui missing cache label %q", needle)
		}
	}
	for _, stale := range []string{"上游已上报", "非最终尝试或上游未上报"} {
		if strings.Contains(html, stale) {
			t.Fatalf("repetitive usage caveat must stay removed: %q", stale)
		}
	}
	// Dependency-free text-node rendering still holds.
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write"} {
		if strings.Contains(html, sink) {
			t.Fatalf("forbidden sink %q", sink)
		}
	}
	// Column counts stay consistent with headers (11 realtime columns + 15 proxy-stats columns).
	if strings.Count(html, "emptyRow(tb,11") < 1 {
		t.Fatal("realtime flat table must cover 11 columns")
	}
	if strings.Count(html, "emptyRow(tb,15") < 1 {
		t.Fatal("proxy stats table must cover 15 columns")
	}
	if strings.Contains(html, "emptyRow(tb,14") {
		t.Fatal("stale 14-column empty rows must be removed")
	}
	// Localization: protocol/tier/failure/hint/log/routing/cache mappings present, proper casing kept.
	for _, needle := range []string{"protoLabel", "tierLabel", "failureLabel", "hintLabel", "Chat Completions", "上下文长度", "陈旧响应引用", "会话被拒绝", "无效请求", "传输失败", "认证失败", "限流", "上游失败", "客户端拒绝", "调试", "信息", "警告", "错误", "最近一小时", "进程累计", "上游", "代理", "密钥", "凭证", "目标", "元数据", "尝试", "回退", "Token"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing localized mapping/label %q", needle)
		}
	}
	// Unknown component/event: primary 未知 (event keeps raw as 未知（raw）), raw only in title/secondary; hidden WebUI label stays simple.
	for _, needle := range []string{"WebUI 是否启用", "未知（", `if(!c)return "核心"`} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing localized mapping/label %q", needle)
		}
	}
	for _, stale := range []string{"<th>Tier</th>", "<th>Proxy</th>", "<th>Key</th>", ">debug<", ">info<", ">warn<", ">error<", "dropped / gap", "\"last_error\"", "flat attempt-row", "UpstreamAttempt", ">anonymous<", "<th>回退</th>", "已回退", "无回退", "同目标回退", "后端保留值", "表单隐藏", `return c||"核心"`, `return e||"—"`} {
		if strings.Contains(html, stale) {
			t.Fatalf("stale visible string must be localized: %q", stale)
		}
	}
}

func TestNonReplayAttemptsOmitReplayFlagsJSON(t *testing.T) {
	a := UpstreamAttempt{RequestID: "r", Model: "m", Tier: "zen", Protocol: "chat", Attempt: 1, Channel: "key", Success: true, FailureClass: AttemptClassSuccess}
	data, _ := json.Marshal(a)
	if strings.Contains(string(data), "route_session_replay") || strings.Contains(string(data), "dropped_") {
		t.Fatalf("non-replay must omit replay flags: %s", data)
	}
	r := UpstreamAttempt{RequestID: "r", Model: "m", Tier: "zen", Protocol: "responses", Attempt: 2, Channel: "key", Success: true, FailureClass: AttemptClassSuccess, RouteSessionReplay: true}
	data2, _ := json.Marshal(r)
	if !strings.Contains(string(data2), `"route_session_replay":true`) {
		t.Fatalf("replay must serialize: %s", data2)
	}
	if strings.Contains(string(data2), "dropped_previous_response_id") || strings.Contains(string(data2), "dropped_reasoning_refs") {
		t.Fatalf("false drops must be omitted: %s", data2)
	}
	// Request without protocol/hash omits them (additive).
	req := UpstreamRequest{RequestID: "r", Model: "m", Channel: "not_routed", Attempts: 0, Status: 400, Success: false, Outcome: "not_routed"}
	data3, _ := json.Marshal(req)
	if strings.Contains(string(data3), "client_session_hash") || strings.Contains(string(data3), `"protocol"`) {
		t.Fatalf("empty diagnostics must be omitted: %s", data3)
	}
}

func TestAttemptProtocolFollowsTier(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	route := authOnlyRoute()
	route.Protocols = map[Tier]Protocol{TierZen: ProtocolChat, TierGo: ProtocolAnthropic}
	route.Protocol = ProtocolChat
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{ClientSessionHash: clientSessionHash("ses_proto_tier")})
	ids := clientSessionIDs("ses_proto_tier")
	resp, eff, _, err := gateway.doUpstreamTiers(ctx, route, routeBodies(), ids, 0)
	if err != nil {
		t.Fatal(err)
	}
	drainAndClose(resp.Body)
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) == 0 {
		t.Fatalf("no attempts")
	}
	if recent[0].Protocol != string(eff.Protocol) {
		t.Fatalf("attempt protocol=%q want %q", recent[0].Protocol, eff.Protocol)
	}
	_ = http.StatusOK
}
