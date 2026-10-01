package app

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func nativeFreeGateway(t *testing.T, pools map[string][]string, anonymous bool, models []string, protos map[Tier]map[string]Protocol) *Gateway {
	t.Helper()
	cfg := testGatewayConfig(pools, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Anonymous = anonymous
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	gw, err := NewGateway(cfg, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	native := map[Tier]map[string]Protocol{TierZen: {}, TierGo: {}}
	for _, m := range models {
		p := ProtocolChat
		if protos != nil {
			if v, ok := protos[TierZen][m]; ok && v != "" {
				p = v
			}
		}
		native[TierZen][m] = p
	}
	gw.catalog.ReplaceWithCapabilities(models, nil, native,
		map[Tier]map[string]bool{TierZen: {}, TierGo: {}},
		nil)
	return gw
}

func setZeroCostFree(t *testing.T, gw *Gateway, model string) {
	t.Helper()
	if gw.catalog.metadata == nil {
		gw.catalog.metadata = newModelMetadataStore("", nil)
	}
	zero := 0.0
	gw.catalog.metadata.mu.Lock()
	if gw.catalog.metadata.models == nil {
		gw.catalog.metadata.models = map[string]ModelPrice{}
	}
	gw.catalog.metadata.models[model] = ModelPrice{Input: &zero, Output: &zero}
	gw.catalog.metadata.updatedAt = time.Now()
	gw.catalog.metadata.mu.Unlock()
	if !gw.nativeFreeAgentShape(model) {
		t.Fatalf("%q must be free via zero-cost metadata", model)
	}
}

func decodeNativeBody(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func assertNativeShaped(t *testing.T, proto Protocol, payload map[string]any) {
	t.Helper()
	if payload["stream"] != true {
		t.Fatalf("%s: shaped stream must be true, got %v", proto, payload["stream"])
	}
	tools, ok := payload["tools"].([]any)
	if !ok || len(tools) < 5 {
		t.Fatalf("%s: shaped tools must have >=5, got %v", proto, payload["tools"])
	}
	if proto == ProtocolChat {
		opts, ok := payload["stream_options"].(map[string]any)
		if !ok || opts["include_usage"] != true {
			t.Fatalf("chat must preserve include_usage=true, got %v", payload["stream_options"])
		}
	}
}

func TestNativeFreeAuthUnboundShapedAnonymousDisabled(t *testing.T) {
	gw := nativeFreeGateway(t, map[string][]string{"shared": {"direct"}}, false,
		[]string{"free-model"}, nil)
	var seen []byte
	stubProxy(t, gw, "shared", 0, func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		seen = raw
		return responseWithBody(200, bulkChatSuccessBody("free-model")), nil
	})
	route, err := gw.catalog.Route("free-model", true, false)
	if err != nil {
		t.Fatal(err)
	}
	if route.Anonymous {
		t.Fatalf("anonymous disabled must route authenticated-only")
	}
	bodies, shaped, err := gw.prepareRouteBodies(ProtocolChat, route,
		map[string]any{"model": "free-model", "messages": []any{map[string]any{"role": "user", "content": "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !shaped {
		t.Fatalf("auth free must freeze shaped=true")
	}
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, bodies, clientSessionIDs("ses-native-auth-unbound"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	payload := decodeNativeBody(t, seen)
	assertNativeShaped(t, ProtocolChat, payload)
}

func TestNativeFreeAuthPinnedStable(t *testing.T) {
	gw := nativeFreeGateway(t, map[string][]string{"shared": {"direct"}}, false,
		[]string{"free-model"}, nil)
	var bodies [][]byte
	stubProxy(t, gw, "shared", 0, func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		bodies = append(bodies, append([]byte(nil), raw...))
		return responseWithBody(200, bulkChatSuccessBody("free-model")), nil
	})
	route, _ := gw.catalog.Route("free-model", true, false)
	prep, shaped, err := gw.prepareRouteBodies(ProtocolChat, route,
		map[string]any{"model": "free-model", "messages": []any{map[string]any{"role": "user", "content": "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !shaped {
		t.Fatalf("must be shaped")
	}
	ses := "ses-native-auth-pinned"
	ctx1 := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp1, _, _, err := gw.doUpstreamTiers(ctx1, route, prep, clientSessionIDs(ses), 0)
	if err != nil {
		t.Fatal(err)
	}
	resp1.Body.Close()
	if _, ok := gw.scheduler.pinGet(ses, "free-model"); !ok {
		t.Fatalf("first 2xx must pin")
	}
	ctx2 := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp2, _, _, err := gw.doUpstreamTiers(ctx2, route, prep, clientSessionIDs(ses), 0)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if len(bodies) != 2 {
		t.Fatalf("want 2 sends, got %d", len(bodies))
	}
	if string(bodies[0]) != string(bodies[1]) {
		t.Fatalf("pinned must reuse identical body:\n%s\n%s", bodies[0], bodies[1])
	}
	assertNativeShaped(t, ProtocolChat, decodeNativeBody(t, bodies[1]))
}

func TestNativeFreeAuthExact400ReplayStableTerminal(t *testing.T) {
	gw := nativeFreeGateway(t, map[string][]string{"shared": {"direct"}}, false,
		[]string{"free-model"}, nil)
	var bodies [][]byte
	calls := 0
	stubProxy(t, gw, "shared", 0, func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		bodies = append(bodies, append([]byte(nil), raw...))
		calls++
		if calls == 1 {
			return responseWithBody(400, `{"error":{"message":"bad","type":"invalid_request_error"}}`), nil
		}
		return responseWithBody(400, `{"error":{"message":"still bad","type":"invalid_request_error"}}`), nil
	})
	route, _ := gw.catalog.Route("free-model", true, false)
	prep, _, err := gw.prepareRouteBodies(ProtocolChat, route,
		map[string]any{"model": "free-model", "messages": []any{map[string]any{"role": "user", "content": "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, prep, clientSessionIDs("ses-native-400"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("second 400 must terminate route, got %d", resp.StatusCode)
	}
	if len(bodies) != 2 {
		t.Fatalf("want first+replay, got %d", len(bodies))
	}
	for i, raw := range bodies {
		assertNativeShaped(t, ProtocolChat, decodeNativeBody(t, raw))
		_ = i
	}
	// Replay keeps identical shaped bytes apart from route-session stamping;
	// both must be shaped with same core tools.
	first := decodeNativeBody(t, bodies[0])
	second := decodeNativeBody(t, bodies[1])
	if len(first["tools"].([]any)) != len(second["tools"].([]any)) {
		t.Fatalf("replay must keep identical core shape")
	}
}

func TestNativeFreeAuthRepeatAndMoveBodyStable(t *testing.T) {
	gw := nativeFreeGateway(t, map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}}, false,
		[]string{"free-model"}, nil)
	var bodies [][]byte
	// First proxy 429s, second succeeds. Both sends must be identically shaped.
	stubProxy(t, gw, "shared", 0, func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		bodies = append(bodies, append([]byte(nil), raw...))
		resp := responseWithBody(429, `{"error":{"message":"ratelimited"}}`)
		resp.Header.Set("Retry-After", "0")
		return resp, nil
	})
	stubProxy(t, gw, "shared", 1, func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		bodies = append(bodies, append([]byte(nil), raw...))
		return responseWithBody(200, bulkChatSuccessBody("free-model")), nil
	})
	route, _ := gw.catalog.Route("free-model", true, false)
	prep, _, err := gw.prepareRouteBodies(ProtocolChat, route,
		map[string]any{"model": "free-model", "messages": []any{map[string]any{"role": "user", "content": "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, prep, clientSessionIDs("ses-native-move"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if len(bodies) != 2 {
		t.Fatalf("want 429+success sends, got %d", len(bodies))
	}
	for _, raw := range bodies {
		assertNativeShaped(t, ProtocolChat, decodeNativeBody(t, raw))
	}
	if string(bodies[0]) != string(bodies[1]) {
		t.Fatalf("within-pool 429 move must keep identical body bytes")
	}
}

func TestNativeFreeNonStreamCollapseAllProtocols(t *testing.T) {
	cases := []struct {
		name     string
		proto    Protocol
		path     string
		reqBody  string
		upstream func() string
		check    func(t *testing.T, raw []byte)
	}{
		{
			name: "chat", proto: ProtocolChat, path: "/v1/chat/completions",
			reqBody: `{"model":"free-model","messages":[{"role":"user","content":"hi"}]}`,
			upstream: func() string {
				return chatSSEBody("chatcmpl-9", "hello", "bash", `{"cmd":"ls"}`, `{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}`)
			},
			check: func(t *testing.T, raw []byte) {
				var p map[string]any
				if err := json.Unmarshal(raw, &p); err != nil {
					t.Fatal(err)
				}
				choices := p["choices"].([]any)
				msg := choices[0].(map[string]any)["message"].(map[string]any)
				if msg["content"] != "hello" {
					t.Fatalf("content=%v", msg["content"])
				}
				usage := p["usage"].(map[string]any)
				if int(usage["prompt_tokens"].(float64)) != 10 {
					t.Fatalf("usage=%v", usage)
				}
			},
		},
		{
			name: "responses", proto: ProtocolResponses, path: "/v1/responses",
			reqBody: `{"model":"free-model","input":"hi"}`,
			upstream: func() string {
				return "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-1\",\"model\":\"free-model\"}}\n\n" +
					"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n" +
					"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"model\":\"free-model\",\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":4,\"total_tokens\":7}}}\n\n"
			},
			check: func(t *testing.T, raw []byte) {
				var p map[string]any
				if err := json.Unmarshal(raw, &p); err != nil {
					t.Fatal(err)
				}
				if p["status"] != "completed" {
					t.Fatalf("status=%v", p["status"])
				}
			},
		},
		{
			name: "anthropic", proto: ProtocolAnthropic, path: "/v1/messages",
			reqBody: `{"model":"free-model","messages":[{"role":"user","content":"hi"}],"max_tokens":16}`,
			upstream: func() string {
				return "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg-1\",\"model\":\"free-model\",\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}}\n\n" +
					"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
					"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n" +
					"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":2,\"output_tokens\":6}}\n\n" +
					"data: {\"type\":\"message_stop\"}\n\n"
			},
			check: func(t *testing.T, raw []byte) {
				var p map[string]any
				if err := json.Unmarshal(raw, &p); err != nil {
					t.Fatal(err)
				}
				if p["stop_reason"] != "end_turn" {
					t.Fatalf("stop=%v", p["stop_reason"])
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			protos := map[Tier]map[string]Protocol{TierZen: {"free-model": tc.proto}}
			gw := nativeFreeGateway(t, map[string][]string{"shared": {"direct"}}, false,
				[]string{"free-model"}, protos)
			sse := tc.upstream()
			var seen map[string]any
			stubProxy(t, gw, "shared", 0, func(r *http.Request) (*http.Response, error) {
				raw, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(raw, &seen)
				if seen["stream"] != true {
					return responseWithBody(403, `{"error":{"message":"forbidden"}}`), nil
				}
				resp := responseWithBody(200, sse)
				resp.Header.Set("Content-Type", "text/event-stream")
				return resp, nil
			})
			handler := gw.authenticate(gw.handleInference(tc.proto))
			req, _ := http.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.reqBody))
			req.Header.Set("Authorization", "Bearer local-key")
			req = req.WithContext(context.WithValue(req.Context(), requestMetaKey{}, &requestMeta{}))
			rec := httptest.NewRecorder()
			handler(rec, req)
			res := rec.Result()
			raw, _ := io.ReadAll(res.Body)
			if res.StatusCode != 200 {
				t.Fatalf("status=%d body=%s", res.StatusCode, raw)
			}
			if ct := res.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
				t.Fatalf("non-stream must collapse to JSON, ct=%q", ct)
			}
			tc.check(t, raw)
			// Stream stays SSE.
			stubProxy(t, gw, "shared", 0, func(r *http.Request) (*http.Response, error) {
				resp := responseWithBody(200, sse)
				resp.Header.Set("Content-Type", "text/event-stream")
				return resp, nil
			})
			var streamBody map[string]any
			_ = json.Unmarshal([]byte(tc.reqBody), &streamBody)
			streamBody["stream"] = true
			encoded, _ := json.Marshal(streamBody)
			req2, _ := http.NewRequest(http.MethodPost, tc.path, strings.NewReader(string(encoded)))
			req2.Header.Set("Authorization", "Bearer local-key")
			req2 = req2.WithContext(context.WithValue(req2.Context(), requestMetaKey{}, &requestMeta{}))
			rec2 := httptest.NewRecorder()
			handler(rec2, req2)
			if ct := rec2.Result().Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
				t.Fatalf("stream must stay SSE, ct=%q", ct)
			}
		})
	}
}

func TestNativeFreePaidNonStreamStaysJSON(t *testing.T) {
	gw := nativeFreeGateway(t, map[string][]string{"shared": {"direct"}}, false,
		[]string{"paid-model"}, nil)
	var seen map[string]any
	stubProxy(t, gw, "shared", 0, func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &seen)
		return responseWithBody(200, bulkChatSuccessBody("paid-model")), nil
	})
	handler := gw.authenticate(gw.handleInference(ProtocolChat))
	req, _ := http.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"paid-model","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer local-key")
	req = req.WithContext(context.WithValue(req.Context(), requestMetaKey{}, &requestMeta{}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	res := rec.Result()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		t.Fatalf("status=%d body=%s", res.StatusCode, raw)
	}
	if _, ok := seen["tools"]; ok {
		t.Fatalf("paid must not inject tools: %v", seen)
	}
	if seen["stream"] == true {
		t.Fatalf("paid non-stream must not force stream: %v", seen)
	}
}

func TestNativeFreeZeroCostNoFreeID(t *testing.T) {
	gw := nativeFreeGateway(t, map[string][]string{"shared": {"direct"}}, false,
		[]string{"paid-zero"}, nil)
	setZeroCostFree(t, gw, "paid-zero")
	route, err := gw.catalog.Route("paid-zero", true, false)
	if err != nil {
		t.Fatal(err)
	}
	bodies, shaped, err := gw.prepareRouteBodies(ProtocolChat, route,
		map[string]any{"model": "paid-zero", "messages": []any{map[string]any{"role": "user", "content": "hi"}}})
	if err != nil {
		t.Fatal(err)
	}
	if !shaped {
		t.Fatalf("zero-cost no-free-ID must shape")
	}
	payload := decodeNativeBody(t, bodies[TierZen])
	assertNativeShaped(t, ProtocolChat, payload)
}

func TestNativeFreeAuthAvailabilityShapedAndCollapse(t *testing.T) {
	gw := nativeFreeGateway(t, map[string][]string{"shared": {"direct"}}, true,
		[]string{"free-model", "paid-model"},
		map[Tier]map[string]Protocol{TierZen: {"free-model": ProtocolChat, "paid-model": ProtocolChat}})
	// Auth free probe must be shaped.
	freeTgt := bulkSendTarget{PoolName: "shared", Tier: TierZen, CredKey: "k", CredID: "c1", CredDisp: "x", Raw: "direct", FreeModel: true, ProbeModel: "free-model", ProbeProtocol: ProtocolChat}
	_, _, _, body, err := buildBulkProbeRequest(context.Background(), gw.cfg.Upstream.Zen, freeTgt, ProtocolChat, "free-model")
	if err != nil {
		t.Fatal(err)
	}
	assertNativeShaped(t, ProtocolChat, decodeNativeBody(t, body))
	// Paid probe stays minimal unshaped.
	paidTgt := bulkSendTarget{PoolName: "shared", Tier: TierZen, CredKey: "k", CredID: "c1", CredDisp: "x", Raw: "direct", ProbeModel: "paid-model", ProbeProtocol: ProtocolChat}
	_, _, _, paidBody, err := buildBulkProbeRequest(context.Background(), gw.cfg.Upstream.Zen, paidTgt, ProtocolChat, "paid-model")
	if err != nil {
		t.Fatal(err)
	}
	paid := decodeNativeBody(t, paidBody)
	if paid["stream"] == true {
		t.Fatalf("paid probe must stay non-stream")
	}
	if _, ok := paid["tools"]; ok {
		t.Fatalf("paid probe must not inject tools")
	}
	// Free SSE collapses; paid validates plain only via bulkProbeSuccessBody.
	sse := chatSSEBody("c", "hi", "", "", `{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}`)
	if _, ok := collapseNativeAgentProbeBody(ProtocolChat, "free-model", []byte(sse)); !ok {
		t.Fatalf("free SSE must validate via collapse")
	}
	// Scoped credential path: bulkProbeOnce with FreeModel sends shaped body.
	sends := 0
	pool := gw.pools["shared"]
	pool.items[0].client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		sends++
		raw, _ := io.ReadAll(r.Body)
		p := decodeNativeBody(t, raw)
		assertNativeShaped(t, ProtocolChat, p)
		resp := responseWithBody(200, sse)
		resp.Header.Set("Content-Type", "text/event-stream")
		return resp, nil
	})}
	credTgt := bulkSendTarget{PoolName: "shared", Index: pool.items[0].index, Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: "zen-key-12345", CredID: "cred-1", CredDisp: "12345", FreeModel: true, ProbeModel: "free-model", ProbeProtocol: ProtocolChat}
	res := gw.bulkProbeOnce(context.Background(), credTgt)
	if sends != 1 || !res.Success {
		t.Fatalf("auth free probe must succeed: %+v sends=%d", res, sends)
	}
}

func TestNativeCustomUnchanged(t *testing.T) {
	// Custom minimal bodies never gain native core tools, even for free-named models.
	body, err := bulkProbeRequestBody("free-model", ProtocolChat, "")
	if err != nil {
		t.Fatal(err)
	}
	var p map[string]any
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	if _, ok := p["tools"]; ok {
		t.Fatalf("custom probe must not inject tools: %s", body)
	}
	if p["stream"] == true {
		t.Fatalf("custom probe must stay non-stream: %s", body)
	}
}
