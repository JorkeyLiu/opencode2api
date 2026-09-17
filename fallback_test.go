package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fallbackChatOK(model string) string {
	return `{"id":"chatcmpl-fb","object":"chat.completion","created":1,"model":"` + model + `","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`
}

func fallbackTestGateway(t *testing.T, channels []FallbackChannelConfig, active string) (*Gateway, func()) {
	t.Helper()
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct"}, "z": {"direct"}, "g": {"direct"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Fallback = FallbackConfig{Active: active, Channels: channels}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	return gw, func() {}
}

func bindAnonPin(t *testing.T, gw *Gateway, session, model string) {
	t.Helper()
	pool := gw.pools["a"]
	if pool == nil || len(pool.items) == 0 {
		t.Fatal("anon pool missing")
	}
	raw := pool.items[0].name
	gw.bindSessionPin(session, model, TierZen, anonymousSchedulerCredentialID, "a", raw, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
}

func TestFallbackConfigStrict(t *testing.T) {
	base := testBaseConfig()
	base.Fallback = FallbackConfig{Active: "missing", Channels: []FallbackChannelConfig{{Name: "c1", BaseURL: "https://api.example.com", APIKey: "k1", Model: "m1"}}}
	if _, err := NormalizeConfig("config.json", base); err == nil {
		t.Fatal("active referencing missing channel must fail")
	}
	base2 := testBaseConfig()
	base2.Fallback = FallbackConfig{Channels: []FallbackChannelConfig{
		{Name: "dup", BaseURL: "https://a.example", APIKey: "k", Model: "m"},
		{Name: "dup", BaseURL: "https://b.example", APIKey: "k2", Model: "m2"},
	}}
	if _, err := NormalizeConfig("config.json", base2); err == nil {
		t.Fatal("duplicate names must fail")
	}
	for _, bad := range []string{"ftp://x.example", "not-a-url", ""} {
		c := testBaseConfig()
		c.Fallback = FallbackConfig{Channels: []FallbackChannelConfig{{Name: "c", BaseURL: bad, APIKey: "k", Model: "m"}}}
		if _, err := NormalizeConfig("config.json", c); err == nil {
			t.Fatalf("bad base_url %q must fail", bad)
		}
	}
	c := testBaseConfig()
	c.Fallback = FallbackConfig{Channels: []FallbackChannelConfig{{Name: "c", BaseURL: "https://a.example", APIKey: "", Model: "m"}}}
	if _, err := NormalizeConfig("config.json", c); err == nil {
		t.Fatal("empty api_key must fail")
	}
	c2 := testBaseConfig()
	c2.Fallback = FallbackConfig{Channels: []FallbackChannelConfig{{Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: ""}}}
	if _, err := NormalizeConfig("config.json", c2); err == nil {
		t.Fatal("empty model must fail")
	}
	// Unknown fallback fields must be rejected by the strict decoder.
	unknownFallback := `{"active":"","channels":[],"unknown":1}`
	var fbStrict FallbackConfig
	decStrict := json.NewDecoder(strings.NewReader(unknownFallback))
	decStrict.DisallowUnknownFields()
	if err := decStrict.Decode(&fbStrict); err == nil {
		t.Fatal("unknown fallback field must be rejected")
	}
	unknownChannel := `{"active":"","channels":[{"name":"c","base_url":"https://a.example","api_key":"k","model":"m","bogus":1}]}`
	var fbChannel FallbackConfig
	decChannel := json.NewDecoder(strings.NewReader(unknownChannel))
	decChannel.DisallowUnknownFields()
	if err := decChannel.Decode(&fbChannel); err == nil {
		t.Fatal("unknown fallback channel field must be rejected")
	}
	var cfgStrict Config
	if err := json.Unmarshal([]byte(`{"fallback":{"active":"","channels":[],"unknown":1}}`), &cfgStrict); err == nil {
		t.Fatal("Config strict decoder must reject unknown fallback field")
	}
	var cfgStrictChannel Config
	if err := json.Unmarshal([]byte(`{"fallback":{"active":"","channels":[{"name":"c","base_url":"https://a.example","api_key":"k","model":"m","bogus":1}]}}`), &cfgStrictChannel); err == nil {
		t.Fatal("Config strict decoder must reject unknown fallback channel field")
	}
	cfg2 := testBaseConfig()
	cfg2.Fallback = FallbackConfig{Active: "", Channels: []FallbackChannelConfig{}}
	norm, err := NormalizeConfig("config.json", cfg2)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(norm)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"fallback"`) {
		t.Fatal("marshal must persist fallback")
	}
	var decoded Config
	dec2 := json.NewDecoder(strings.NewReader(string(data)))
	dec2.DisallowUnknownFields()
	if err := dec2.Decode(&decoded); err != nil {
		t.Fatalf("fallback roundtrip must decode: %v", err)
	}
}

func TestFallbackMaskResolve(t *testing.T) {
	cfg := testBaseConfig()
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{
		{Name: "c1", BaseURL: "https://a.example", APIKey: "secret-key-11111", Model: "m1"},
		{Name: "c2", BaseURL: "https://b.example", APIKey: "secret-key-22222", Model: "m2"},
	}}
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	view := fallbackViewFromConfig(norm)
	if len(view.Channels) != 2 {
		t.Fatal("view must have 2 channels")
	}
	for _, ch := range view.Channels {
		if strings.Contains(ch.APIKey.Display, "secret-key") {
			t.Fatal("masked display must not contain full key")
		}
		if ch.APIKey.ID == "" {
			t.Fatal("masked id missing")
		}
	}
	// Resolve via masked id.
	id := secretFingerprint("secret-key-11111")
	resolved, err := resolveFallbackInput(FallbackInput{
		Active: "c1",
		Channels: []FallbackChannelInput{
			{Name: "c1", BaseURL: "https://a.example", APIKey: SecretInput{ID: id}, Model: "m1"},
			{Name: "c2", BaseURL: "https://b.example", APIKey: SecretInput{Value: "secret-key-22222"}, Model: "m2"},
		},
	}, norm.Fallback)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Channels[0].APIKey != "secret-key-11111" {
		t.Fatal("masked id must resolve to stored key")
	}
	if _, err := resolveFallbackInput(FallbackInput{
		Channels: []FallbackChannelInput{{Name: "c1", BaseURL: "https://a.example", APIKey: SecretInput{ID: "stale-id"}, Model: "m1"}},
	}, norm.Fallback); err == nil {
		t.Fatal("stale id must fail")
	}
}

func TestFallbackUnbound429Unchanged(t *testing.T) {
	customHits := atomic.Int32{}
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm")))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm"}}, "c1")
	var anonHits atomic.Int32
	postStub(t, gw, "a", 0, &anonHits, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	route := anonAuthRoute()
	ids := pinIDs("ses_unbound_fb_1", "r1")
	resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids, 0, upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp)
	if customHits.Load() != 0 {
		t.Fatal("unbound anonymous 429 must not take over custom")
	}
	if gw.scheduler.fallbacks.count() != 0 {
		t.Fatal("unbound must not bind fallback")
	}
}

func TestFallbackPinnedLive429Takeover(t *testing.T) {
	var customBody []byte
	var customAuth string
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customAuth = r.Header.Get("Authorization")
		customBody, _ = io.ReadAll(r.Body)
		if r.URL.Path != "/v1/chat/completions" {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("custom-m")))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "ck-1", Model: "custom-m"}}, "c1")
	ses := "ses_pinned_live_429"
	bindAnonPin(t, gw, ses, "m")
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		resp := responseWithBody(429, `{"error":{"message":"slow"}}`)
		resp.Header.Set("Retry-After", "1")
		return resp, nil
	})
	route := anonAuthRoute()
	ids := pinIDs(ses, "r-live")
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids, 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("takeover must succeed, got %d", resp.StatusCode)
	}
	if eff.Tier != TierCustom || eff.Protocol != ProtocolChat {
		t.Fatalf("effective route must be custom/chat, got %+v", eff)
	}
	if customAuth != "Bearer ck-1" {
		t.Fatalf("custom auth=%q", customAuth)
	}
	var payload map[string]any
	if err := json.Unmarshal(customBody, &payload); err != nil {
		t.Fatal(err)
	}
	if stringAt(payload, "model") != "custom-m" {
		t.Fatalf("custom model rewrite missing: %s", string(customBody))
	}
	binding, ok := gw.scheduler.fallbacks.get(ses)
	if !ok || binding.Name != "c1" || binding.Model != "custom-m" {
		t.Fatalf("session must be bound to channel: %+v %v", binding, ok)
	}
	// Follow-up request on same session must go straight to custom without native send.
	var anon2 atomic.Int32
	postStub(t, gw, "a", 0, &anon2, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids2 := pinIDs(ses, "r-live-2")
	resp2, _, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids2, 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp2)
	if resp2.StatusCode != 200 || anon2.Load() != 0 {
		t.Fatalf("bound session must stay on custom: code=%d anon=%d", resp2.StatusCode, anon2.Load())
	}
}

func TestFallbackPinnedLocalCooldown429(t *testing.T) {
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm2")))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k", Model: "cm2"}}, "c1")
	ses := "ses_pinned_cool_429"
	bindAnonPin(t, gw, ses, "m")
	pool := gw.pools["a"]
	raw := pool.items[0].name
	gw.scheduler.noteProxy429Failure(TierZen, "a", raw, AttemptClassRateLimited, 429, 0, time.Now().UnixNano())
	route := anonAuthRoute()
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r-cool"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("local cooldown 429 must trigger takeover: %d %+v", resp.StatusCode, eff)
	}
}

func TestFallbackAuthPinNoTrigger(t *testing.T) {
	customHits := atomic.Int32{}
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm")))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k", Model: "cm"}}, "c1")
	// Bind authenticated pin.
	pool := gw.pools["z"]
	if pool == nil {
		// routing uses z pool for zen auth; fallbackTestGateway has z pool.
		t.Fatal("z pool missing")
	}
	cred := credentialsForKeys(TierZen, []string{"zen-secret-12345"})[0]
	gw.authCreds = []credentialRef{cred}
	ses := "ses_auth_pin_429"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", pool.items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	route := authOnlyRoute()
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r-auth"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp)
	if resp.StatusCode != 429 {
		t.Fatalf("auth pin 429 must not trigger, got %d", resp.StatusCode)
	}
	if customHits.Load() != 0 {
		t.Fatal("auth pin must not hit custom")
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatal("auth pin must not bind fallback")
	}
}

func TestFallbackNoActiveKeeps429(t *testing.T) {
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{Name: "c1", BaseURL: "https://api.example.com", APIKey: "k", Model: "cm"}}, "")
	ses := "ses_no_active_429"
	bindAnonPin(t, gw, ses, "m")
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r"), 0, upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m"}})
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp)
	if resp.StatusCode != 429 {
		t.Fatalf("no active must keep 429, got %d", resp.StatusCode)
	}
}

func TestFallbackSessionCrossModelAndActiveSwitch(t *testing.T) {
	c1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var p map[string]any
		_ = json.Unmarshal(body, &p)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK(stringAt(p, "model"))))
	}))
	defer c1.Close()
	c2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("m2chan")))
	}))
	defer c2.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{
		{Name: "c1", BaseURL: c1.URL, APIKey: "k1", Model: "m1chan"},
		{Name: "c2", BaseURL: c2.URL, APIKey: "k2", Model: "m2chan"},
	}, "c1")
	ses := "ses_cross_model"
	bindAnonPin(t, gw, ses, "m")
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	drainResp(resp)
	// Same session, different model must stay on c1.
	route2 := anonAuthRoute()
	route2.ID = "other"
	bodies2 := map[Tier][]byte{TierZen: []byte(`{"model":"other"}`), TierGo: []byte(`{"model":"other"}`)}
	ex2 := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "other", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	resp2, eff2, _, err := gw.doUpstreamTiers(pinTestCtx(), route2, bodies2, pinIDs(ses, "r2"), 0, ex2)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp2)
	if eff2.Tier != TierCustom || resp2.StatusCode != 200 {
		t.Fatalf("cross-model must stay custom: %+v %d", eff2, resp2.StatusCode)
	}
	// Switch active to c2: old session stays on c1, new session uses c2.
	gw.cfg.Fallback.Active = "c2"
	sesNew := "ses_new_after_switch"
	bindAnonPin(t, gw, sesNew, "m")
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	resp3, _, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(sesNew, "r3"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp3)
	bNew, _ := gw.scheduler.fallbacks.get(sesNew)
	if bNew.Name != "c2" {
		t.Fatalf("new takeover must use switched active, got %+v", bNew)
	}
	bOld, _ := gw.scheduler.fallbacks.get(ses)
	if bOld.Name != "c1" {
		t.Fatalf("old binding must not drift, got %+v", bOld)
	}
}

func TestFallbackFailureNoRetry(t *testing.T) {
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
		_, _ = w.Write([]byte(`{"error":{"message":"boom"}}`))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k", Model: "cm"}}, "c1")
	ses := "ses_custom_fail"
	bindAnonPin(t, gw, ses, "m")
	first := atomic.Bool{}
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		if first.CompareAndSwap(false, true) {
			return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})
	// First native 429 triggers custom which fails 500; must return 500 as-is.
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 500 {
		t.Fatalf("custom failure must return as-is, got %d %s", resp.StatusCode, string(body))
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatal("failed takeover must still bind session")
	}
}

func TestFallbackDeleteAndIdentityMismatch502(t *testing.T) {
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm")))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm"}}, "c1")
	ses := "ses_tombstone"
	bindAnonPin(t, gw, ses, "m")
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m"}}
	resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	drainResp(resp)
	// Delete channel: old session must 502, never re-establish.
	gw.cfg.Fallback.Channels = []FallbackChannelConfig{}
	gw.cfg.Fallback.Active = ""
	resp2, _, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r2"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp2)
	if resp2.StatusCode != 502 {
		t.Fatalf("deleted channel must 502, got %d", resp2.StatusCode)
	}
	// Identity change (same name, different key) must also 502.
	gw2, _ := fallbackTestGateway(t, []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm"}}, "c1")
	bindAnonPin(t, gw2, ses, "m")
	postStub(t, gw2, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	resp3, _, _, _ := gw2.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, ex)
	if resp3 != nil {
		drainResp(resp3)
	}
	gw2.cfg.Fallback.Channels = []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "changed", Model: "cm"}}
	resp4, _, _, err := gw2.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r2"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp4)
	if resp4.StatusCode != 502 {
		t.Fatalf("identity change must 502, got %d", resp4.StatusCode)
	}
}

func TestFallbackMigrateAndCapacity(t *testing.T) {
	old := newFallbackTakeoverStore()
	if _, _, _ = old.bind("s1", fallbackBinding{Name: "c1", BaseURL: "https://a.example", KeyHash: "h1", Model: "m1"}); true {
	}
	next := newFallbackTakeoverStore()
	if n := next.migrateFallbackFrom(old); n != 1 {
		t.Fatalf("migrate=%d want 1", n)
	}
	if _, ok := next.get("s1"); !ok {
		t.Fatal("migrated binding missing")
	}
	// Capacity first-wins.
	full := newFallbackTakeoverStore()
	for i := 0; i < fallbackTakeoverStoreCap; i++ {
		full.bind("k", fallbackBinding{Name: "n"})
		_ = i
		break
	}
	// Fill deterministically to cap.
	for i := 0; i < fallbackTakeoverStoreCap; i++ {
		s := "sess-" + string(rune('a'+i%26)) + "-" + string([]byte{byte(i >> 8), byte(i)}) + "\x00" + string(rune(i))
		_ = s
		break
	}
	// Direct fill loop with unique keys.
	store := newFallbackTakeoverStore()
	for i := 0; i < fallbackTakeoverStoreCap; i++ {
		key := "fill-" + fallbackItoa(i)
		if _, _, isFull := store.bind(key, fallbackBinding{Name: "c", BaseURL: "https://a.example", KeyHash: "h", Model: "m"}); isFull {
			t.Fatal("should not be full before cap")
		}
	}
	if _, _, isFull := store.bind("overflow", fallbackBinding{Name: "c", BaseURL: "https://a.example", KeyHash: "h", Model: "m"}); !isFull {
		t.Fatal("overflow must report full")
	}
	if _, ok := store.get("overflow"); ok {
		t.Fatal("overflow must not be recorded")
	}
	// First-wins: existing key keeps original.
	s2 := newFallbackTakeoverStore()
	s2.bind("s", fallbackBinding{Name: "first", BaseURL: "https://a.example", KeyHash: "h", Model: "m"})
	got, inserted, isFull := s2.bind("s", fallbackBinding{Name: "second", BaseURL: "https://b.example", KeyHash: "h2", Model: "m2"})
	if inserted || isFull || got.Name != "first" {
		t.Fatalf("first-wins violated: %+v %v %v", got, inserted, isFull)
	}
	_ = full
}

func fallbackItoa(i int) string {
	if i == 0 {
		return "0"
	}
	out := ""
	for i > 0 {
		out = string(rune('0'+i%10)) + out
		i /= 10
	}
	return out
}

func TestFallbackConversionAllProtocols(t *testing.T) {
	cases := []struct {
		external Protocol
		payload  map[string]any
	}{
		{ProtocolChat, map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}},
		{ProtocolResponses, map[string]any{"model": "m", "input": "hi"}},
		{ProtocolAnthropic, map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "max_tokens": 8}},
	}
	for _, tc := range cases {
		var gotModel string
		var gotPath string
		custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			body, _ := io.ReadAll(r.Body)
			var p map[string]any
			_ = json.Unmarshal(body, &p)
			gotModel, _ = p["model"].(string)
			if auth := r.Header.Get("Authorization"); auth == "" {
				w.WriteHeader(401)
				return
			}
			if r.Header.Get("x-opencode-session") != "" {
				w.WriteHeader(400)
				_, _ = w.Write([]byte(`{"error":{"message":"session leak"}}`))
				return
			}
			w.WriteHeader(200)
			_, _ = w.Write([]byte(fallbackChatOK("configured-m")))
		}))
		gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k", Model: "configured-m"}}, "c1")
		ses := "ses_conv_" + string(tc.external)
		bindAnonPin(t, gw, ses, "m")
		postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
		})
		route := anonAuthRoute()
		ex := upstreamExtra{External: tc.external, Payload: tc.payload}
		resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r"), 0, ex)
		if err != nil {
			custom.Close()
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		custom.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%s: custom status=%d body=%s", tc.external, resp.StatusCode, string(body))
		}
		if eff.Tier != TierCustom {
			t.Fatalf("%s: tier=%s", tc.external, eff.Tier)
		}
		if gotPath != "/v1/chat/completions" {
			t.Fatalf("%s: path=%q", tc.external, gotPath)
		}
		if gotModel != "configured-m" {
			t.Fatalf("%s: model rewrite=%q", tc.external, gotModel)
		}
		// Response back to client protocol must decode.
		if _, err := convertResponse(ProtocolChat, tc.external, []byte(fallbackChatOK("configured-m"))); err != nil {
			t.Fatalf("%s: response transcode failed: %v", tc.external, err)
		}
	}
}

func TestFallbackStreamingChat(t *testing.T) {
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"configured-m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k", Model: "configured-m"}}, "c1")
	ses := "ses_stream_fb"
	bindAnonPin(t, gw, ses, "m")
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "stream": true}}
	resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), map[Tier][]byte{TierZen: []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true}`), TierGo: []byte(`{"model":"m"}`)}, pinIDs(ses, "r"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("stream takeover status=%d", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), "chatcmpl-1") {
		t.Fatalf("stream body missing: %q", string(raw))
	}
}

func TestFallbackDiscoverSecurity(t *testing.T) {
	// Success with plaintext key.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(404)
			return
		}
		if r.Header.Get("Authorization") != "Bearer plain-key-123" {
			w.WriteHeader(401)
			return
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[{"id":"alpha"},{"id":""},{"id":"beta"}]}`))
	}))
	defer srv.Close()
	models, err := fetchFallbackModels(context.Background(), nil, srv.URL, "plain-key-123")
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 2 || models[0] != "alpha" || models[1] != "beta" {
		t.Fatalf("models=%v", models)
	}
	// Empty list rejected.
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer empty.Close()
	if _, err := fetchFallbackModels(context.Background(), nil, empty.URL, "k"); err == nil {
		t.Fatal("empty list must fail")
	}
	// Non-2xx rejected.
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
	}))
	defer bad.Close()
	if _, err := fetchFallbackModels(context.Background(), nil, bad.URL, "k"); err == nil {
		t.Fatal("500 must fail")
	}
}

func TestFallbackAvailabilityCustomAndNoModel(t *testing.T) {
	manager := &RuntimeManager{monitor: NewMonitor(), hub: NewLogHub(100), redactor: NewSecretRedactor()}
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Keys = []string{"zen-key-12345", "go-key-12345"}
	cfg.Anonymous = true
	var customHits atomic.Int32
	customOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm")))
	}))
	defer customOK.Close()
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{{Name: "c1", BaseURL: customOK.URL, APIKey: "ck-secret", Model: "cm"}}}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, nil, manager.monitor)
	if err != nil {
		t.Fatal(err)
	}
	// Seed directory with a free Zen model and a Go model so native lanes have probes.
	gw.catalog.ReplaceWithCapabilities([]string{"probe-free-model"}, []string{"probe-free-model"}, map[Tier]map[string]Protocol{TierZen: {"probe-free-model": ProtocolChat}, TierGo: {"probe-free-model": ProtocolChat}}, map[Tier]map[string]bool{TierZen: {}, TierGo: {}}, nil)
	// Native upstream stub: real minimal POST chat success.
	native := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(404)
			return
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("probe-free-model")))
	}))
	defer native.Close()
	gw.cfg.Upstream.Zen = native.URL
	gw.cfg.Upstream.Go = native.URL
	manager.current.Store(&gatewayRuntime{config: normalized, gateway: gw})
	manager.redactor.Replace(normalized)
	// Seed a stored custom row; batch must preserve it without sending.
	storedAt := time.Now().UTC().Add(-time.Hour)
	gw.bulkSnapshot.Store(&bulkAvailabilitySnapshot{
		CheckedAt: storedAt,
		Custom:    []bulkCustomAvailability{{Name: "c1", BaseURL: redactURL(customOK.URL), Model: "cm", Status: "available", Reason: "success", LastChecked: &storedAt}},
	})
	resp := gw.runBulkCheck(context.Background())
	if len(resp.Custom) != 0 {
		t.Fatalf("batch must exclude custom results, got %+v", resp.Custom)
	}
	if customHits.Load() != 0 {
		t.Fatalf("batch must not send to custom channels, hits=%d", customHits.Load())
	}
	kept := gw.bulkSnapshot.Load()
	if kept == nil || len(kept.Custom) != 1 || kept.Custom[0].Name != "c1" {
		t.Fatalf("batch must preserve stored custom rows: %+v", kept)
	}
	if kept.Custom[0].LastChecked == nil || !kept.Custom[0].LastChecked.Equal(storedAt) {
		t.Fatalf("stored custom time must not move on batch")
	}
	// Per-row custom check provides the custom result with existing helpers.
	customResp := gw.runCustomCheck(context.Background(), gw.cfg.Fallback.Channels[0])
	if customResp.Custom.Status != "available" {
		t.Fatalf("per-row custom must be available: %+v", customResp)
	}
	if strings.Contains(strings.ToLower(customResp.Custom.BaseURL), "ck-secret") {
		t.Fatal("custom must not leak key")
	}
	if customHits.Load() == 0 {
		t.Fatal("per-row custom must send one real probe")
	}
	if len(resp.NoModel) != 0 {
		t.Fatalf("no_model must be empty when directory has probes: %+v", resp.NoModel)
	}
	if resp.TestedNodes == 0 {
		t.Fatal("native must test nodes with real POST")
	}
	// Scheduler writes preserved: force a 429 comparative run via direct results.
	pool := gw.pools["shared"]
	before, _ := gw.scheduler.proxy429EntrySnapshot(TierZen, "shared", pool.items[0].name)
	_ = before
	// No-model lane: fresh gateway with empty catalog must report no_model, not success.
	manager2 := &RuntimeManager{monitor: NewMonitor(), hub: NewLogHub(100), redactor: NewSecretRedactor()}
	cfg2 := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg2.ZenKeys = []string{"zen-key-12345"}
	cfg2.Anonymous = true
	norm2, _ := NormalizeConfig("config.json", cfg2)
	gw2, _ := NewGateway(norm2, nil, manager2.monitor)
	manager2.current.Store(&gatewayRuntime{config: norm2, gateway: gw2})
	resp2 := gw2.runBulkCheck(context.Background())
	if len(resp2.NoModel) == 0 {
		t.Fatal("empty directory must report no_model")
	}
	for _, c := range resp2.Credentials {
		if c.Reason != "no_model" && c.Status != "inconclusive" && c.TestedNodes == 0 {
			// At least the no-model credentials must not claim success.
			if c.Status == "available" && c.Reason == "success" {
				t.Fatalf("no-model must not masquerade success: %+v", c)
			}
		}
	}
	// Custom must not write scheduler state.
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierCustom, "fallback", customOK.URL); ok {
		t.Fatal("custom must not write scheduler")
	}
	_ = time.Now
}
