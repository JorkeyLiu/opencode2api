package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// First native->custom takeover must strip provider-bound Responses refs
// (native-issued previous_response_id plus input[] reasoning items) before the
// send, while preserving ordinary message/function_call/function_call_output
// history with the new custom route session stamped.
func TestFallbackTakeoverStripsNativeResponsesRefs(t *testing.T) {
	var gotBody map[string]any
	var gotSes, gotReq, gotPrj, gotUA, gotClient string
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" {
			w.WriteHeader(404)
			return
		}
		gotSes = r.Header.Get("x-opencode-session")
		gotReq = r.Header.Get("x-opencode-request")
		gotPrj = r.Header.Get("x-opencode-project")
		gotUA = r.Header.Get("User-Agent")
		gotClient = r.Header.Get("x-opencode-client")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackResponsesOK("cm-takeover")))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{ID: "c-resp-take", Name: "c-resp-take", BaseURL: custom.URL, APIKey: "k-take", Model: "cm-takeover", Protocol: ProtocolResponses}}, "c-resp-take")
	ses := "ses_takeover_strip_native_1"
	pool := gw.pools["a"]
	if pool == nil || len(pool.items) == 0 {
		t.Fatal("anon pool missing")
	}
	gw.bindSessionPin(ses, "m", TierZen, anonymousSchedulerCredentialID, "a", pool.items[0].name, ProtocolResponses, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	ex := upstreamExtra{External: ProtocolResponses, Payload: map[string]any{
		"model":                "m",
		"previous_response_id": "resp-native-1",
		"input": []any{
			map[string]any{"type": "message", "role": "user", "content": "hi"},
			map[string]any{"type": "reasoning", "id": "rs_native_1", "encrypted_content": "enc-native"},
			map[string]any{"type": "function_call", "call_id": "c1", "name": "fn", "arguments": "{}"},
			map[string]any{"type": "function_call_output", "call_id": "c1", "output": "ok"},
		},
	}}
	route := modelRoute{ID: "m", Tier: TierZen, Protocol: ProtocolResponses, Protocols: map[Tier]Protocol{TierZen: ProtocolResponses}, Anonymous: true, KeyTiers: []Tier{TierZen}}
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r-strip-1"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("takeover status=%d want 200", resp.StatusCode)
	}
	if eff.Tier != TierCustom || eff.Protocol != ProtocolResponses {
		t.Fatalf("effective route must be custom/responses, got %+v", eff)
	}
	if gotBody == nil {
		t.Fatal("custom did not receive a body")
	}
	if _, ok := gotBody["previous_response_id"]; ok {
		t.Fatalf("takeover must drop native previous_response_id, got %v", gotBody["previous_response_id"])
	}
	rawInput, ok := gotBody["input"].([]any)
	if !ok {
		t.Fatalf("takeover body must carry input, got %T", gotBody["input"])
	}
	if len(rawInput) != 3 {
		t.Fatalf("takeover input must keep 3 ordinary items, got %d: %v", len(rawInput), rawInput)
	}
	seen := map[string]bool{}
	for _, item := range rawInput {
		m, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("input item must be an object, got %T", item)
		}
		typ, _ := m["type"].(string)
		if typ == "reasoning" {
			t.Fatalf("takeover must drop all input reasoning items, got %v", m)
		}
		seen[typ] = true
	}
	for _, want := range []string{"message", "function_call", "function_call_output"} {
		if !seen[want] {
			t.Fatalf("takeover must preserve %q history, got types %v", want, seen)
		}
	}
	if gotBody["model"] != "cm-takeover" {
		t.Fatalf("model rewrite missing: %v", gotBody["model"])
	}
	if gotSes == "" || !isCanonicalWireSession(gotSes) {
		t.Fatalf("session header must be canonical wire shape, got %q", gotSes)
	}
	if cacheKey, _ := gotBody["prompt_cache_key"].(string); cacheKey != gotSes {
		t.Fatalf("prompt_cache_key must match new wire session, body=%v header=%q", gotBody["prompt_cache_key"], gotSes)
	}
	if gotBody["store"] != false {
		t.Fatalf("store must default false, got %v", gotBody["store"])
	}
	if gotUA != opencodeWireUserAgent() || gotClient != "cli" {
		t.Fatalf("identity headers mismatch: ua=%q client=%q", gotUA, gotClient)
	}
	if !isCanonicalWireRequest(gotReq) || !isCanonicalWireProject(gotPrj) {
		t.Fatalf("request/project must stay canonical, got %q %q", gotReq, gotPrj)
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatal("takeover must bind the session to the custom channel")
	}
}

// Once the session is bound to the custom channel, follow-up Responses sends
// must preserve refs issued by that custom authority (no repeated cleanup).
func TestFallbackBoundCustomPreservesCustomResponsesRefs(t *testing.T) {
	type captured struct {
		body map[string]any
	}
	var sends []captured
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" {
			w.WriteHeader(404)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		sends = append(sends, captured{body: body})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackResponsesOK("cm-bound")))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{ID: "c-resp-bound", Name: "c-resp-bound", BaseURL: custom.URL, APIKey: "k-bound", Model: "cm-bound", Protocol: ProtocolResponses}}, "c-resp-bound")
	ses := "ses_takeover_bound_keep_1"
	pool := gw.pools["a"]
	if pool == nil || len(pool.items) == 0 {
		t.Fatal("anon pool missing")
	}
	gw.bindSessionPin(ses, "m", TierZen, anonymousSchedulerCredentialID, "a", pool.items[0].name, ProtocolResponses, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	route := modelRoute{ID: "m", Tier: TierZen, Protocol: ProtocolResponses, Protocols: map[Tier]Protocol{TierZen: ProtocolResponses}, Anonymous: true, KeyTiers: []Tier{TierZen}}
	first := upstreamExtra{External: ProtocolResponses, Payload: map[string]any{
		"model":                "m",
		"previous_response_id": "resp-native-9",
		"input": []any{
			map[string]any{"type": "message", "role": "user", "content": "first"},
			map[string]any{"type": "reasoning", "id": "rs_native_9", "encrypted_content": "enc-native-9"},
		},
	}}
	resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r-bound-1"), 0, first)
	if err != nil {
		t.Fatal(err)
	}
	drainResp(resp)
	if resp.StatusCode != 200 || len(sends) != 1 {
		t.Fatalf("takeover must succeed once, status=%d sends=%d", resp.StatusCode, len(sends))
	}
	if _, ok := sends[0].body["previous_response_id"]; ok {
		t.Fatal("first takeover send must strip the native previous_response_id")
	}
	// Follow-up on the now-bound session carries refs issued by the custom
	// channel itself: they must reach the custom channel untouched, with no
	// native send attempted.
	var anonPosts atomic.Int32
	postStub(t, gw, "a", 0, &anonPosts, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	second := upstreamExtra{External: ProtocolResponses, Payload: map[string]any{
		"model":                "m",
		"previous_response_id": "resp-custom-1",
		"input": []any{
			map[string]any{"type": "message", "role": "user", "content": "second"},
			map[string]any{"type": "reasoning", "id": "rs_custom_1", "encrypted_content": "enc-custom-1"},
			map[string]any{"type": "function_call_output", "call_id": "c9", "output": "ok"},
		},
	}}
	resp2, eff2, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r-bound-2"), 0, second)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp2)
	if resp2.StatusCode != 200 || eff2.Tier != TierCustom {
		t.Fatalf("bound follow-up must stay on custom 200, got %d %+v", resp2.StatusCode, eff2)
	}
	if anonPosts.Load() != 0 {
		t.Fatalf("bound follow-up must not touch native, anon posts=%d", anonPosts.Load())
	}
	if len(sends) != 2 {
		t.Fatalf("custom must receive the follow-up send, sends=%d", len(sends))
	}
	follow := sends[1].body
	if follow["previous_response_id"] != "resp-custom-1" {
		t.Fatalf("bound send must keep custom previous_response_id, got %v", follow["previous_response_id"])
	}
	rawInput, ok := follow["input"].([]any)
	if !ok || len(rawInput) != 3 {
		t.Fatalf("bound send must keep all 3 input items, got %v", follow["input"])
	}
	foundReasoning := false
	for _, item := range rawInput {
		m, _ := item.(map[string]any)
		if m == nil {
			t.Fatalf("input item must be an object, got %T", item)
		}
		if m["type"] == "reasoning" {
			foundReasoning = true
			if m["id"] != "rs_custom_1" || m["encrypted_content"] != "enc-custom-1" {
				t.Fatalf("bound reasoning ref must stay byte-identical, got %v", m)
			}
		}
	}
	if !foundReasoning {
		t.Fatal("bound send must preserve the custom-issued reasoning item")
	}
}

// Chat takeover carries no Responses provider-bound refs: the crossing flag
// must leave the chat history path untouched (messages preserved, model
// rewritten, custom error/route semantics unchanged).
func TestFallbackTakeoverChatUnaffected(t *testing.T) {
	var gotBody map[string]any
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			w.WriteHeader(404)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-chat")))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{ID: "c-chat-plain", Name: "c-chat-plain", BaseURL: custom.URL, APIKey: "ck-chat", Model: "cm-chat"}}, "c-chat-plain")
	ses := "ses_takeover_chat_plain_1"
	bindAnonPin(t, gw, ses, "m")
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hello"}}}}
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r-chat-1"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 || eff.Tier != TierCustom || eff.Protocol != ProtocolChat {
		t.Fatalf("chat takeover must stay custom/chat 200, got %d %+v", resp.StatusCode, eff)
	}
	if gotBody == nil {
		t.Fatal("custom did not receive a body")
	}
	if gotBody["model"] != "cm-chat" {
		t.Fatalf("chat model rewrite missing: %v", gotBody["model"])
	}
	msgs, ok := gotBody["messages"].([]any)
	if !ok || len(msgs) != 1 {
		t.Fatalf("chat messages must pass through untouched, got %v", gotBody["messages"])
	}
	first, _ := msgs[0].(map[string]any)
	if first["role"] != "user" || first["content"] != "hello" {
		t.Fatalf("chat message must stay intact, got %v", first)
	}
	if _, ok := gotBody["previous_response_id"]; ok {
		t.Fatalf("chat body must not gain responses refs, got %v", gotBody["previous_response_id"])
	}
}
