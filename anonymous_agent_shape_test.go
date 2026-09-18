package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func shapedPayload(t *testing.T, protocol Protocol, payload map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	shaped, err := shapedAnonymousBody(raw, protocol)
	if err != nil {
		t.Fatalf("shape: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(shaped, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func toolNames(t *testing.T, protocol Protocol, payload map[string]any) []string {
	t.Helper()
	raw, ok := payload["tools"].([]any)
	if !ok {
		t.Fatalf("tools missing")
	}
	var names []string
	for _, entry := range raw {
		item := entry.(map[string]any)
		name, ok := anonymousToolName(protocol, item)
		if !ok {
			t.Fatalf("unrecognized tool: %v", item)
		}
		names = append(names, name)
	}
	return names
}

func TestAnonymousShapeForcesStreamAndCoreTools(t *testing.T) {
	for _, proto := range []Protocol{ProtocolChat, ProtocolResponses, ProtocolAnthropic} {
		var payload map[string]any
		switch proto {
		case ProtocolResponses:
			payload = map[string]any{"model": "m", "input": "hi"}
		case ProtocolAnthropic:
			payload = map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "max_tokens": 1}
		default:
			payload = map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}
		}
		out := shapedPayload(t, proto, payload)
		if out["stream"] != true {
			t.Fatalf("%s: stream must be true", proto)
		}
		names := toolNames(t, proto, out)
		if len(names) != 5 {
			t.Fatalf("%s: want 5 tools, got %v", proto, names)
		}
		for i, want := range anonymousCoreTools {
			if names[i] != want {
				t.Fatalf("%s: order got %v want core order", proto, names)
			}
		}
		// Idempotence.
		raw, _ := json.Marshal(out)
		reshaped, err := shapedAnonymousBody(raw, proto)
		if err != nil {
			t.Fatal(err)
		}
		var second map[string]any
		if err := json.Unmarshal(reshaped, &second); err != nil {
			t.Fatal(err)
		}
		if len(second["tools"].([]any)) != 5 {
			t.Fatalf("%s: idempotence must stay 5", proto)
		}
	}
}

func TestAnonymousShapePreservesExtraTools(t *testing.T) {
	payload := map[string]any{
		"model":    "m",
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
		"tools": []any{map[string]any{
			"type": "function",
			"function": map[string]any{
				"name": "custom", "description": "c",
				"parameters": map[string]any{"type": "object", "properties": map[string]any{}},
			},
		}},
	}
	out := shapedPayload(t, ProtocolChat, payload)
	names := toolNames(t, ProtocolChat, out)
	if names[0] != "custom" {
		t.Fatalf("extra tool must stay first, got %v", names)
	}
	if len(names) != 6 {
		t.Fatalf("want 6 tools, got %v", names)
	}
}

func TestAnonymousShapeMissingOnlyAppend(t *testing.T) {
	payload := map[string]any{
		"model":    "m",
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
		"tools": []any{map[string]any{
			"type": "function",
			"function": map[string]any{
				"name": "bash", "description": "mine",
				"parameters": map[string]any{"type": "object"},
			},
		}},
	}
	out := shapedPayload(t, ProtocolChat, payload)
	raw := out["tools"].([]any)
	if len(raw) != 5 {
		t.Fatalf("want 5, got %d", len(raw))
	}
	first := raw[0].(map[string]any)["function"].(map[string]any)
	if first["description"] != "mine" {
		t.Fatalf("caller bash must not be overwritten")
	}
}

func TestAnonymousShapeMalformedLoud(t *testing.T) {
	for _, proto := range []Protocol{ProtocolChat, ProtocolResponses, ProtocolAnthropic} {
		payload := map[string]any{"model": "m", "tools": "nope"}
		raw, _ := json.Marshal(payload)
		if _, err := shapedAnonymousBody(raw, proto); err == nil {
			t.Fatalf("%s: non-array tools must fail", proto)
		}
		payload2 := map[string]any{"model": "m", "tools": []any{"nope"}}
		raw2, _ := json.Marshal(payload2)
		if _, err := shapedAnonymousBody(raw2, proto); err == nil {
			t.Fatalf("%s: non-object tool must fail", proto)
		}
	}
	// Chat wrong type must fail.
	payload := map[string]any{"model": "m", "tools": []any{map[string]any{"type": "other"}}}
	raw, _ := json.Marshal(payload)
	if _, err := shapedAnonymousBody(raw, ProtocolChat); err == nil {
		t.Fatalf("wrong chat tool type must fail")
	}
}

func TestAnonymousShapeProtocolDefs(t *testing.T) {
	out := shapedPayload(t, ProtocolResponses, map[string]any{"model": "m", "input": "hi"})
	for _, entry := range out["tools"].([]any) {
		item := entry.(map[string]any)
		if item["strict"] != false {
			t.Fatalf("responses must carry strict:false")
		}
	}
	chatOut := shapedPayload(t, ProtocolChat, map[string]any{"model": "m"})
	for _, entry := range chatOut["tools"].([]any) {
		item := entry.(map[string]any)
		if _, ok := item["function"].(map[string]any)["strict"]; ok {
			t.Fatalf("chat must not inject strict")
		}
	}
	anthOut := shapedPayload(t, ProtocolAnthropic, map[string]any{"model": "m"})
	for _, entry := range anthOut["tools"].([]any) {
		item := entry.(map[string]any)
		if _, ok := item["input_schema"]; !ok {
			t.Fatalf("anthropic must carry input_schema")
		}
	}
}

func chatSSEBody(id, text, toolName, toolArgs string, usage string) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("data: {\"id\":\"%s\",\"model\":\"m\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":%q}}]}\n\n", id, text))
	if toolName != "" {
		b.WriteString(fmt.Sprintf("data: {\"id\":\"%s\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":%q,\"arguments\":%q}}]}}]}\n\n", id, toolName, toolArgs))
	}
	b.WriteString(fmt.Sprintf("data: {\"id\":\"%s\",\"choices\":[{\"finish_reason\":\"tool_calls\"}],\"usage\":%s}\n\n", id, usage))
	b.WriteString("data: [DONE]\n\n")
	return b.String()
}

func TestAnonymousCollapseChat(t *testing.T) {
	usage := `{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}`
	raw := chatSSEBody("chatcmpl-1", "hello", "bash", `{"cmd":"ls"}`, usage)
	body, gotUsage, reported, err := collapseUpstreamSSE(strings.NewReader(raw), ProtocolChat, ProtocolChat, "m")
	if err != nil {
		t.Fatal(err)
	}
	if !reported || gotUsage.Input != 10 || gotUsage.Output != 5 || gotUsage.Total != 15 {
		t.Fatalf("usage got %+v reported=%v", gotUsage, reported)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	choices := payload["choices"].([]any)
	msg := choices[0].(map[string]any)["message"].(map[string]any)
	if msg["content"] != "hello" {
		t.Fatalf("content=%v", msg["content"])
	}
	calls := msg["tool_calls"].([]any)
	fn := calls[0].(map[string]any)["function"].(map[string]any)
	if fn["name"] != "bash" || fn["arguments"] != `{"cmd":"ls"}` {
		t.Fatalf("tool=%v", fn)
	}
	if choices[0].(map[string]any)["finish_reason"] != "tool_calls" {
		t.Fatalf("finish=%v", choices[0])
	}
}

func TestAnonymousCollapseResponses(t *testing.T) {
	raw := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-1\",\"model\":\"m\"}}\n\n" +
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n" +
		"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"fc-1\",\"type\":\"function_call\",\"call_id\":\"call-1\",\"name\":\"read\"}}\n\n" +
		"data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"item_id\":\"fc-1\",\"delta\":\"{}\"}\n\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"model\":\"m\",\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":4,\"total_tokens\":7}}}\n\n"
	body, usage, reported, err := collapseUpstreamSSE(strings.NewReader(raw), ProtocolResponses, ProtocolResponses, "m")
	if err != nil {
		t.Fatal(err)
	}
	if !reported || usage.Input != 3 || usage.Output != 4 {
		t.Fatalf("usage %+v", usage)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["status"] != "completed" {
		t.Fatalf("status=%v", payload["status"])
	}
	foundText, foundTool := false, false
	for _, item := range payload["output"].([]any) {
		m := item.(map[string]any)
		if m["type"] == "message" {
			foundText = true
		}
		if m["type"] == "function_call" && m["name"] == "read" {
			foundTool = true
		}
	}
	if !foundText || !foundTool {
		t.Fatalf("output missing text/tool: %s", body)
	}
}

func TestAnonymousCollapseAnthropic(t *testing.T) {
	raw := "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg-1\",\"model\":\"m\",\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}}\n\n" +
		"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\",\"signature\":\"\"}}\n\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"plan\"}}\n\n" +
		"data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n" +
		"data: {\"type\":\"content_block_start\",\"index\":2,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu-1\",\"name\":\"grep\",\"input\":{}}}\n\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":2,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{}\" }}\n\n" +
		"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"input_tokens\":2,\"output_tokens\":6}}\n\n" +
		"data: {\"type\":\"message_stop\"}\n\n"
	body, usage, reported, err := collapseUpstreamSSE(strings.NewReader(raw), ProtocolAnthropic, ProtocolAnthropic, "m")
	if err != nil {
		t.Fatal(err)
	}
	if !reported || usage.Output != 6 {
		t.Fatalf("usage %+v", usage)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["stop_reason"] != "tool_use" {
		t.Fatalf("stop=%v", payload["stop_reason"])
	}
	content := payload["content"].([]any)
	var hasThinking, hasText, hasTool bool
	for _, c := range content {
		m := c.(map[string]any)
		switch m["type"] {
		case "thinking":
			hasThinking = true
		case "text":
			hasText = true
		case "tool_use":
			hasTool = true
		}
	}
	if !hasThinking || !hasText || !hasTool {
		t.Fatalf("content=%s", body)
	}
}

func TestAnonymousCollapseErrorsNotSuccess(t *testing.T) {
	errSSE := "data: {\"error\":{\"message\":\"boom\",\"type\":\"upstream_error\"}}\n\n"
	if _, _, _, err := collapseUpstreamSSE(strings.NewReader(errSSE), ProtocolChat, ProtocolChat, "m"); err == nil {
		t.Fatalf("error SSE must fail")
	}
	truncated := "data: {\"id\":\"x\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"
	if _, _, _, err := collapseUpstreamSSE(strings.NewReader(truncated), ProtocolChat, ProtocolChat, "m"); err == nil {
		t.Fatalf("truncated must fail")
	}
	if _, _, _, err := collapseUpstreamSSE(strings.NewReader("data: {oops\n\n"), ProtocolChat, ProtocolChat, "m"); err == nil {
		t.Fatalf("malformed must fail")
	}
}

func TestAnonymousGatewayNonStreamCollapses(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Anonymous = true
	cfg.Keys = nil
	gw, err := NewGateway(cfg, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	gw.catalog.ReplaceWithCapabilities([]string{"free-model"}, nil,
		map[Tier]map[string]Protocol{TierZen: {"free-model": ProtocolChat}, TierGo: {}},
		map[Tier]map[string]bool{TierZen: {}, TierGo: {}},
		nil)
	usageJSON := `{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}`
	sse := chatSSEBody("chatcmpl-9", "hi", "bash", `{"cmd":"ls"}`, usageJSON)
	stubProxy(t, gw, "shared", 0, func(r *http.Request) (*http.Response, error) {
		var payload map[string]any
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &payload)
		if payload["stream"] != true {
			return responseWithBody(403, `{"error":{"message":"forbidden"}}`), nil
		}
		tools, ok := payload["tools"].([]any)
		if !ok || len(tools) < 5 {
			return responseWithBody(403, `{"error":{"message":"forbidden"}}`), nil
		}
		resp := responseWithBody(200, sse)
		resp.Header.Set("Content-Type", "text/event-stream")
		return resp, nil
	})
	handler := gw.authenticate(gw.handleInference(ProtocolChat))
	reqBody := `{"model":"free-model","messages":[{"role":"user","content":"hi"}]}`
	req, _ := http.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
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
		t.Fatalf("content-type=%q", ct)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	choices, ok := payload["choices"].([]any)
	if !ok || len(choices) == 0 {
		t.Fatalf("missing choices: %s", raw)
	}
	msg := choices[0].(map[string]any)["message"].(map[string]any)
	if msg["content"] != "hi" {
		t.Fatalf("content=%v", msg["content"])
	}
}

func TestAnonymousGatewayStreamingStaysSSE(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Anonymous = true
	cfg.Keys = nil
	gw, err := NewGateway(cfg, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	gw.catalog.ReplaceWithCapabilities([]string{"free-model"}, nil,
		map[Tier]map[string]Protocol{TierZen: {"free-model": ProtocolChat}, TierGo: {}},
		map[Tier]map[string]bool{TierZen: {}, TierGo: {}},
		nil)
	sse := chatSSEBody("chatcmpl-9", "hi", "", "", `{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}`)
	stubProxy(t, gw, "shared", 0, func(r *http.Request) (*http.Response, error) {
		resp := responseWithBody(200, sse)
		resp.Header.Set("Content-Type", "text/event-stream")
		return resp, nil
	})
	handler := gw.authenticate(gw.handleInference(ProtocolChat))
	reqBody := `{"model":"free-model","messages":[{"role":"user","content":"hi"}],"stream":true}`
	req, _ := http.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer local-key")
	req = req.WithContext(context.WithValue(req.Context(), requestMetaKey{}, &requestMeta{}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	res := rec.Result()
	if ct := res.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("streaming must stay SSE, ct=%q", ct)
	}
}

func TestAnonymousProbeShapedAndCollapses(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	gw, err := NewGateway(cfg, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	tgt := bulkSendTarget{PoolName: "shared", Tier: TierZen, CredKey: "public", CredID: anonymousSchedulerCredentialID, CredDisp: "anonymous", Raw: "direct", IsPublic: true, ProbeModel: "m", ProbeProtocol: ProtocolChat}
	_, _, _, body, err := buildBulkProbeRequest(context.Background(), gw.cfg.Upstream.Zen, tgt, ProtocolChat, "m")
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["stream"] != true || len(payload["tools"].([]any)) != 5 {
		t.Fatalf("probe must be shaped: %s", body)
	}
	// SSE success collapses.
	sse := chatSSEBody("c", "hi", "", "", `{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}`)
	if _, ok := collapseAnonymousProbeBody(ProtocolChat, "m", []byte(sse)); !ok {
		t.Fatalf("valid SSE must validate")
	}
	// Auth probe stays unshaped.
	authTgt := bulkSendTarget{PoolName: "shared", Tier: TierZen, CredKey: "k", CredID: "c1", CredDisp: "x", Raw: "direct", ProbeModel: "m", ProbeProtocol: ProtocolChat}
	_, _, _, authBody, err := buildBulkProbeRequest(context.Background(), gw.cfg.Upstream.Zen, authTgt, ProtocolChat, "m")
	if err != nil {
		t.Fatal(err)
	}
	var authPayload map[string]any
	_ = json.Unmarshal(authBody, &authPayload)
	if authPayload["stream"] != false {
		t.Fatalf("auth probe must stay non-stream")
	}
	if _, ok := authPayload["tools"]; ok {
		t.Fatalf("auth probe must not inject tools")
	}
}

func captureBodies(r *http.Request, out *[][]byte) []byte {
	raw, _ := io.ReadAll(r.Body)
	*out = append(*out, raw)
	return raw
}

func TestAnonymousRetryAndReplayShapingConsistent(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Anonymous = true
	cfg.Keys = nil
	gw, err := NewGateway(cfg, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	// Transient retry: first 500, then SSE success. Both sends must be shaped identically.
	usageJSON := `{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}`
	sse := chatSSEBody("chatcmpl-r", "hi", "", "", usageJSON)
	var bodies [][]byte
	calls := 0
	stubProxy(t, gw, "shared", 0, func(r *http.Request) (*http.Response, error) {
		raw := captureBodies(r, &bodies)
		calls++
		if calls == 1 {
			return responseWithBody(500, `{"error":{"message":"x"}}`), nil
		}
		_ = raw
		resp := responseWithBody(200, sse)
		resp.Header.Set("Content-Type", "text/event-stream")
		return resp, nil
	})
	route := modelRoute{ID: "m", Tier: TierZen, Protocol: ProtocolChat, Protocols: map[Tier]Protocol{TierZen: ProtocolChat}, Anonymous: true}
	prepared, err := prepareUpstreamRequest(ProtocolChat, ProtocolChat, map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}, gw.cfg.Upstream.Zen)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(prepared)
	bodiesMap := map[Tier][]byte{TierZen: encoded}
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, bodiesMap, clientSessionIDs("ses-retry-shape"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if len(bodies) != 2 {
		t.Fatalf("want first+retry sends, got %d", len(bodies))
	}
	for i, raw := range bodies {
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["stream"] != true || len(payload["tools"].([]any)) != 5 {
			t.Fatalf("send %d must be shaped: %s", i, raw)
		}
	}
	if string(bodies[0]) != string(bodies[1]) {
		t.Fatalf("retry must reuse identical shaped body")
	}

	// Exact-400 replay: first 400, then SSE success. Replay must reapply shaping.
	var replayBodies [][]byte
	replayCalls := 0
	stubProxy(t, gw, "shared", 0, func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		replayBodies = append(replayBodies, raw)
		replayCalls++
		if replayCalls == 1 {
			return responseWithBody(400, `{"error":{"message":"bad","type":"invalid_request_error"}}`), nil
		}
		resp := responseWithBody(200, sse)
		resp.Header.Set("Content-Type", "text/event-stream")
		return resp, nil
	})
	ctx2 := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp2, _, _, err := gw.doUpstreamTiers(ctx2, route, bodiesMap, clientSessionIDs("ses-replay-shape"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("replay status=%d", resp2.StatusCode)
	}
	if len(replayBodies) != 2 {
		t.Fatalf("want first+replay sends, got %d", len(replayBodies))
	}
	for i, raw := range replayBodies {
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["stream"] != true || len(payload["tools"].([]any)) != 5 {
			t.Fatalf("replay send %d must be shaped", i)
		}
	}
}

func TestAuthenticatedInferenceUntouched(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Anonymous = true
	gw, err := NewGateway(cfg, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	gw.catalog.ReplaceWithCapabilities([]string{"m"}, nil,
		map[Tier]map[string]Protocol{TierZen: {"m": ProtocolChat}, TierGo: {}},
		map[Tier]map[string]bool{TierZen: {}, TierGo: {}},
		nil)
	var seen []byte
	stubProxy(t, gw, "shared", 0, func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		seen = raw
		return responseWithBody(200, bulkChatSuccessBody("m")), nil
	})
	// Force authenticated-only route (anonymous disabled for this model path
	// is not available, so call the key lane directly with a non-stream body).
	route := modelRoute{ID: "m", Tier: TierZen, Protocol: ProtocolChat, Protocols: map[Tier]Protocol{TierZen: ProtocolChat}, KeyTiers: []Tier{TierZen}}
	prepared, err := prepareUpstreamRequest(ProtocolChat, ProtocolChat, map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}, gw.cfg.Upstream.Zen)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(prepared)
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, map[Tier][]byte{TierZen: encoded}, clientSessionIDs("ses-auth-untouched"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var payload map[string]any
	if err := json.Unmarshal(seen, &payload); err != nil {
		t.Fatal(err)
	}
	if _, hasStream := payload["stream"]; hasStream {
		if payload["stream"] == true {
			t.Fatalf("auth body must respect original non-stream omission, got %s", seen)
		}
	}
	if _, ok := payload["tools"]; ok {
		t.Fatalf("auth must not inject tools: %s", seen)
	}
}

func TestAnonymousProbeNeutrality(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	pool := gw.pools["shared"]
	now := time.Now().UnixNano()
	gw.scheduler.noteProxy429Failure(TierZen, "shared", pool.items[0].name, AttemptClassRateLimited, 429, 0, now)
	sse := chatSSEBody("c", "hi", "", "", `{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}`)
	sends := 0
	pool.items[0].client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		sends++
		var payload map[string]any
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &payload)
		if payload["stream"] != true || len(payload["tools"].([]any)) != 5 {
			return responseWithBody(403, `{"error":{"message":"forbidden"}}`), nil
		}
		resp := responseWithBody(200, sse)
		resp.Header.Set("Content-Type", "text/event-stream")
		return resp, nil
	})}
	pinsBefore := gw.scheduler.pins.count()
	sessBefore := gw.scheduler.routeSessions.count()
	tgt := bulkSendTarget{PoolName: "shared", Index: pool.items[0].index, Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: "public", CredID: anonymousSchedulerCredentialID, CredDisp: "anonymous", IsPublic: true, ProbeModel: "m", ProbeProtocol: ProtocolChat}
	res := gw.bulkProbeOnce(context.Background(), tgt)
	if sends != 1 || !res.Success {
		t.Fatalf("anon SSE probe must succeed: %+v sends=%d", res, sends)
	}
	if gw.scheduler.pins.count() != pinsBefore || gw.scheduler.routeSessions.count() != sessBefore {
		t.Fatalf("probe must stay neutral")
	}
}
