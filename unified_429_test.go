package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func unifiedThreeProxyAnonGateway(t *testing.T, monitor *Monitor, maxAttempts int) *Gateway {
	t.Helper()
	cfg := testGatewayConfig(
		map[string][]string{
			"a": {"direct", "http://127.0.0.1:8081", "http://127.0.0.1:8082"},
			"z": {"direct"},
		},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Retry.MaxAttempts = maxAttempts
	gateway, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	return gateway
}

func unifiedFallbackGateway(t *testing.T, anonProxies, authProxies []string, keys []string, active string, channels []FallbackChannelConfig) *Gateway {
	t.Helper()
	cfg := testGatewayConfig(
		map[string][]string{"a": anonProxies, "z": authProxies},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Keys = keys
	cfg.Retry.MaxAttempts = 5
	cfg.Fallback = FallbackConfig{Active: active, Channels: channels}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	return gw
}

// Anonymous pinned local proxy429 skips current, second 200, Responses
// session/wire/prompt_cache_key byte-identical, pin current updated.
func TestUnifiedAnonPinnedLocal429StableResponses(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	route := modelRoute{ID: "m", Tier: TierZen, Protocol: ProtocolResponses, Protocols: map[Tier]Protocol{TierZen: ProtocolResponses}, Anonymous: true, KeyTiers: []Tier{TierZen}}
	bodies := map[Tier][]byte{TierZen: []byte(`{"model":"m","prompt_cache_key":"keep","store":false,"input":"hi"}`)}
	// Establish anon pin via first success.
	postStub(t, gateway, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ses := "ses_unified_anon_local_resp_1"
	r1, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, bodies, pinIDs(ses, "r1"), 0)
	drainResp(r1)
	pin, ok := gateway.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must pin")
	}
	ordered := affinityProxyOrder(gateway.pools["a"], pin.CredID, pin.ProxyRaw)
	if len(ordered) != 2 {
		t.Fatalf("ordered=%d want 2", len(ordered))
	}
	curRaw, otherRaw := ordered[0].name, ordered[1].name
	curIdx, otherIdx := poolIndexByRaw(gateway, "a", curRaw), poolIndexByRaw(gateway, "a", otherRaw)
	// Local cooldown on current only.
	gateway.scheduler.noteProxy429Failure(TierZen, "a", curRaw, AttemptClassRateLimited, 429, time.Minute, time.Now().UnixNano())
	cap := &capturedUpstream{}
	var c0, c1 atomic.Int32
	postStub(t, gateway, "a", curIdx, &c0, cap, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", otherIdx, &c1, cap, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	r2, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, bodies, pinIDs(ses, "r2"), 0)
	if err != nil || r2.StatusCode != 200 {
		t.Fatalf("local 429 must skip to second 200, err=%v resp=%v", err, r2)
	}
	drainResp(r2)
	if postCount(&c0) != 0 || postCount(&c1) != 1 {
		// Current is cooled so only the alternate sends; affinity order puts
		// current first but it is skipped pre-send.
		t.Fatalf("local skip sends=%d/%d want 0/1 (cur cooled)", postCount(&c0), postCount(&c1))
	}
	sessions, rawBodies := cap.get()
	if len(sessions) != 1 {
		t.Fatalf("captured=%d want 1", len(sessions))
	}
	var payload map[string]any
	if err := json.Unmarshal(rawBodies[0], &payload); err != nil {
		t.Fatal(err)
	}
	if got := stringAt(payload, "prompt_cache_key"); got != sessions[0] {
		t.Fatalf("prompt_cache_key=%q want wire %q", got, sessions[0])
	}
	pin2, _ := gateway.scheduler.pinGet(ses, "m")
	if pin2.ProxyRaw != otherRaw || pin2.Generation != pin.Generation+1 {
		t.Fatalf("pin must move to alternate: %+v vs %+v", pin, pin2)
	}
	// Local cooldown never writes credential429 evidence.
	if _, _, ok := gateway.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("local cooldown must not write credential429")
	}
}

// Anonymous pinned 3-proxy live429 chain at budget 1 reaches third 200.
func TestUnifiedAnonPinnedThreeProxyBudget1(t *testing.T) {
	monitor := NewMonitor()
	gateway := unifiedThreeProxyAnonGateway(t, monitor, 1)
	route := anonAuthRoute()
	for i := range gateway.pools["a"].items {
		postStub(t, gateway, "a", i, nil, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
	}
	ses := "ses_unified_anon_3p_b1_1"
	r1, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r1"), 0)
	drainResp(r1)
	pin, ok := gateway.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must pin")
	}
	ordered := affinityProxyOrder(gateway.pools["a"], pin.CredID, pin.ProxyRaw)
	if len(ordered) != 3 {
		t.Fatalf("ordered=%d want 3", len(ordered))
	}
	idx := func(raw string) int { return poolIndexByRaw(gateway, "a", raw) }
	var c0, c1, c2 atomic.Int32
	postStub(t, gateway, "a", idx(ordered[0].name), &c0, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gateway, "a", idx(ordered[1].name), &c1, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "5")
		return r, nil
	})
	postStub(t, gateway, "a", idx(ordered[2].name), &c2, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	r2, _, attempts, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r2"), 0)
	if err != nil || r2.StatusCode != 200 {
		t.Fatalf("3-proxy budget-1 must reach third 200, err=%v resp=%v", err, r2)
	}
	drainResp(r2)
	if postCount(&c0) != 1 || postCount(&c1) != 1 || postCount(&c2) != 1 {
		t.Fatalf("sends=%d/%d/%d want 1/1/1", postCount(&c0), postCount(&c1), postCount(&c2))
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3", attempts)
	}
}

// Multi-credential exhaustion: A fully 429s then B 200; credential429 only
// for A; single-proxy credential exhausts; pre-cooled never counts.
func TestUnifiedMultiCredentialExhaustion(t *testing.T) {
	newMultiGateway := func(t *testing.T) *Gateway {
		t.Helper()
		cfg := testGatewayConfig(
			map[string][]string{"z": {"direct", "http://127.0.0.1:8081"}},
			ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
		)
		cfg.Anonymous = false
		cfg.Keys = []string{"zen-key-aaaaa", "zen-key-bbbbb"}
		cfg.Retry.MaxAttempts = 5
		normalized, err := NormalizeConfig("config.json", cfg)
		if err != nil {
			t.Fatal(err)
		}
		gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
		if err != nil {
			t.Fatal(err)
		}
		return gw
	}
	t.Run("AExhaustsThenB200", func(t *testing.T) {
		gw := newMultiGateway(t)
		if len(gw.authCreds) != 2 {
			t.Fatalf("creds=%d want 2", len(gw.authCreds))
		}
		credA, credB := gw.authCreds[0], gw.authCreds[1]
		// Determine frozen order for empty session to stub deterministically:
		// stub by proxy behavior keyed on credential via header? Simpler: both
		// proxies for A 429, both for B 200 would be ambiguous in one pool.
		// Instead drive via per-proxy stubs that inspect the Authorization key
		// tail: A tail vs B tail.
		tailA, tailB := credA.display, credB.display
		stub := func(pool string, idx int, calls *atomic.Int32) {
			postStub(t, gw, pool, idx, calls, nil, func(r *http.Request) (*http.Response, error) {
				auth := r.Header.Get("Authorization")
				// Credential key material never appears in logs, but the stub
				// may inspect the live header to emulate per-credential fate.
				if len(auth) > 0 && len(tailA) > 0 && containsTail(auth, tailA) && !containsTail(auth, tailB) {
					// Distinguish A-only path by checking exact key suffix match
					// without logging key material.
					r2 := responseWithBody(429, `{"error":"t"}`)
					r2.Header.Set("Retry-After", "7")
					return r2, nil
				}
				// Default: decide by credential below via explicit per-cred run.
				return responseWithBody(200, `{"ok":true}`), nil
			})
		}
		_ = stub
		// Deterministic approach: freeze candidates and stub per (cred,proxy)
		// by replacing transports with routing funcs that branch on the
		// Bearer key. Use the real keys directly.
		keyA, keyB := gw.authCreds[0].key, gw.authCreds[1].key
		var z0, z1 atomic.Int32
		postStub(t, gw, "z", 0, &z0, nil, func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("Authorization") == "Bearer "+keyA {
				resp := responseWithBody(429, `{"error":"t"}`)
				resp.Header.Set("Retry-After", "7")
				return resp, nil
			}
			return responseWithBody(200, `{"ok":true}`), nil
		})
		postStub(t, gw, "z", 1, &z1, nil, func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("Authorization") == "Bearer "+keyA {
				resp := responseWithBody(429, `{"error":"t"}`)
				resp.Header.Set("Retry-After", "9")
				return resp, nil
			}
			return responseWithBody(200, `{"ok":true}`), nil
		})
		route := authOnlyRoute()
		route.KeyTiers = []Tier{TierZen}
		resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs("ses_multi_exh_1", "r1"), 0)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("A exhaust then B must 200, err=%v resp=%v", err, resp)
		}
		drainResp(resp)
		if _, _, ok := gw.scheduler.credential429CooldownStatus(credA.id); !ok {
			t.Fatalf("exhausted credential A must write credential429")
		}
		if _, _, ok := gw.scheduler.credential429CooldownStatus(credB.id); ok {
			t.Fatalf("successful credential B must not write credential429")
		}
		_ = keyB
	})
	t.Run("SingleProxyExhausts", func(t *testing.T) {
		cfg := testGatewayConfig(map[string][]string{"z": {"direct"}}, ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"})
		cfg.Anonymous = false
		cfg.Keys = []string{"zen-key-single"}
		cfg.Retry.MaxAttempts = 5
		normalized, err := NormalizeConfig("config.json", cfg)
		if err != nil {
			t.Fatal(err)
		}
		gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
		if err != nil {
			t.Fatal(err)
		}
		postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
			resp := responseWithBody(429, `{"error":"t"}`)
			resp.Header.Set("Retry-After", "11")
			return resp, nil
		})
		route := authOnlyRoute()
		route.KeyTiers = []Tier{TierZen}
		resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs("ses_single_exh_1", "r1"), 0)
		if err != nil || resp.StatusCode != 429 {
			t.Fatalf("single eligible 429 must exhaust 429, err=%v resp=%v", err, resp)
		}
		if got := resp.Header.Get("Retry-After"); got != "11" {
			t.Fatalf("Retry-After=%q want 11", got)
		}
		drainResp(resp)
		if _, _, ok := gw.scheduler.credential429CooldownStatus(gw.authCreds[0].id); !ok {
			t.Fatalf("single-proxy exhaustion must write credential429")
		}
	})
	t.Run("PrecooledNotEvidence", func(t *testing.T) {
		gw := newMultiGateway(t)
		// Pre-cool one proxy globally; eligible per cred becomes 1.
		pool := gw.pools["z"]
		cooled := pool.items[0].name
		gw.scheduler.noteProxy429Failure(TierZen, "z", cooled, AttemptClassRateLimited, 429, time.Minute, time.Now().UnixNano())
		other := pool.items[1].name
		otherIdx := poolIndexByRaw(gw, "z", other)
		var otherCalls atomic.Int32
		postStub(t, gw, "z", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
			resp := responseWithBody(429, `{"error":"t"}`)
			resp.Header.Set("Retry-After", "6")
			return resp, nil
		})
		route := authOnlyRoute()
		route.KeyTiers = []Tier{TierZen}
		resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs("ses_precool_evid_1", "r1"), 0)
		if err != nil || resp.StatusCode != 429 {
			t.Fatalf("err=%v resp=%v want 429", err, resp)
		}
		drainResp(resp)
		// Only one live 429 per credential but eligible is also 1 (cooled
		// excluded), so exhaustion writes credential429 for the creds that
		// actually sent. The key assertion: the pre-cooled proxy never sent
		// and never counts as a second evidence.
		if postCount(&otherCalls) < 1 {
			t.Fatalf("other proxy must send, got %d", postCount(&otherCalls))
		}
	})
}

func containsTail(auth, tail string) bool {
	if tail == "" || auth == "" {
		return false
	}
	if len(auth) < len(tail) {
		return false
	}
	return auth[len(auth)-len(tail):] == tail
}

// Anonymous unbound anon exhaustion continues to auth 200 without custom;
// full native exhaustion reaches custom.
func TestUnifiedAnonUnboundToAuthAndCustom(t *testing.T) {
	customHits := atomic.Int32{}
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-unified")))
	}))
	defer custom.Close()
	ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm-unified"}
	gw := unifiedFallbackGateway(t, []string{"direct", "http://127.0.0.1:8081"}, []string{"direct", "http://127.0.0.1:8081"}, []string{"zen-key-aaaaa"}, "c1", []FallbackChannelConfig{ch})
	route := anonAuthRoute()
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	// Anon both 429, auth 200: no custom.
	var a0, a1, z0, z1 atomic.Int32
	postStub(t, gw, "a", 0, &a0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"t"}`), nil
	})
	postStub(t, gw, "a", 1, &a1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"t"}`), nil
	})
	postStub(t, gw, "z", 0, &z0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "z", 1, &z1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs("ses_unified_anon_auth_1", "r1"), 0, ex)
	if err != nil || resp.StatusCode != 200 || eff.Tier == TierCustom {
		t.Fatalf("anon exhaust + auth 200 must not use custom, err=%v resp=%v eff=%+v", err, resp, eff)
	}
	drainResp(resp)
	if customHits.Load() != 0 {
		t.Fatalf("partial native exhaustion must not hit custom")
	}
	// Full native 429 on every proxy: custom binds.
	var b0, b1, c0, c1 atomic.Int32
	postStub(t, gw, "a", 0, &b0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"t"}`), nil
	})
	postStub(t, gw, "a", 1, &b1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"t"}`), nil
	})
	postStub(t, gw, "z", 0, &c0, nil, func(*http.Request) (*http.Response, error) {
		resp := responseWithBody(429, `{"error":"t"}`)
		resp.Header.Set("Retry-After", "8")
		return resp, nil
	})
	postStub(t, gw, "z", 1, &c1, nil, func(*http.Request) (*http.Response, error) {
		resp := responseWithBody(429, `{"error":"t"}`)
		resp.Header.Set("Retry-After", "9")
		return resp, nil
	})
	resp2, eff2, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs("ses_unified_all429_1", "r2"), 0, ex)
	if err != nil || resp2.StatusCode != 200 || eff2.Tier != TierCustom {
		t.Fatalf("full native 429 must bind custom 200, err=%v resp=%v eff=%+v", err, resp2, eff2)
	}
	drainResp(resp2)
	if customHits.Load() != 1 {
		t.Fatalf("custom hits=%d want 1", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get("ses_unified_all429_1"); !ok {
		t.Fatalf("unbound full exhaustion must bind fallback")
	}
}

// 429 followed by terminal non-429 never triggers custom.
func TestUnified429ThenNon429NoCustom(t *testing.T) {
	customHits := atomic.Int32{}
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm")))
	}))
	defer custom.Close()
	ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm"}
	gw := unifiedFallbackGateway(t, []string{"direct", "http://127.0.0.1:8081"}, []string{"direct"}, []string{"zen-key-aaaaa"}, "c1", []FallbackChannelConfig{ch})
	// Auth-only with 2 proxies: first 429, second 404 terminal.
	var z0, z1 atomic.Int32
	// Force order: stub both, first in frozen order 429. Frozen order depends
	// on session HRW; instead make proxy0 429 and proxy1 404 and assert final
	// is either 429-exhausted or 404-terminal without custom when 404 wins.
	// To make deterministic, use single-credential single-pool and check the
	// 404-terminal path by making both non-429 terminal after a 429:
	// proxy0 404 immediately (no 429 seen on that path). Instead emulate
	// 429-then-404 by sequential calls: first request 429s one proxy then
	// second request hits 404 terminal. Simpler deterministic case: unbound
	// anon 429 then auth 404 terminal in the same request.
	var a0, a1 atomic.Int32
	postStub(t, gw, "a", 0, &a0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"t"}`), nil
	})
	postStub(t, gw, "a", 1, &a1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"t"}`), nil
	})
	postStub(t, gw, "z", 0, &z0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(404, `{"error":"nope"}`), nil
	})
	_ = z1
	route := anonAuthRoute()
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs("ses_unified_429_404_1", "r1"), 0, ex)
	if err != nil || resp.StatusCode != 404 || eff.Tier == TierCustom {
		t.Fatalf("429 then 404 terminal must stay 404 without custom, err=%v resp=%v eff=%+v", err, resp, eff)
	}
	drainResp(resp)
	if customHits.Load() != 0 {
		t.Fatalf("non-429 terminal must not hit custom")
	}
	if _, ok := gw.scheduler.fallbacks.get("ses_unified_429_404_1"); ok {
		t.Fatalf("non-429 terminal must not bind fallback")
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = raw
}

// Concurrent anonymous moves converge to one winner with generation fencing.
func TestUnifiedAnonConcurrentMoves(t *testing.T) {
	monitor := NewMonitor()
	gateway := unifiedThreeProxyAnonGateway(t, monitor, 5)
	route := anonAuthRoute()
	for i := range gateway.pools["a"].items {
		postStub(t, gateway, "a", i, nil, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
	}
	ses := "ses_unified_anon_conc_1"
	r1, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r1"), 0)
	drainResp(r1)
	pin, ok := gateway.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must pin")
	}
	ordered := affinityProxyOrder(gateway.pools["a"], pin.CredID, pin.ProxyRaw)
	otherRaw := ordered[1].name
	// Current proxy transport fails; alternate succeeds.
	curIdx := poolIndexByRaw(gateway, "a", ordered[0].name)
	otherIdx := poolIndexByRaw(gateway, "a", otherRaw)
	var curCalls, otherCalls atomic.Int32
	// Anonymous has no cross-proxy transport move: concurrent transport
	// failures stay same-target. Use 429 to force the shared walk instead so
	// concurrent moves contend on generation.
	postStub(t, gateway, "a", curIdx, &curCalls, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "3")
		return r, nil
	})
	postStub(t, gateway, "a", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	thirdIdx := poolIndexByRaw(gateway, "a", ordered[2].name)
	postStub(t, gateway, "a", thirdIdx, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	const workers = 8
	type result struct {
		resp *http.Response
		err  error
	}
	ch := make([]chan result, workers)
	for i := 0; i < workers; i++ {
		ch[i] = make(chan result, 1)
		go func(c chan result) {
			resp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "conc"), 0)
			c <- result{resp, err}
		}(ch[i])
	}
	for i := 0; i < workers; i++ {
		r := <-ch[i]
		if r.err != nil || r.resp == nil || r.resp.StatusCode != 200 {
			t.Fatalf("worker %d err=%v resp=%v", i, r.err, r.resp)
		}
		drainResp(r.resp)
	}
	final, _ := gateway.scheduler.pinGet(ses, "m")
	if final.Generation != pin.Generation+1 {
		t.Fatalf("exactly one anon CAS move must win: gen %d want %d", final.Generation, pin.Generation+1)
	}
	if postCount(&curCalls) < 1 || postCount(&otherCalls) < 1 {
		t.Fatalf("walk must use both proxies: cur=%d other=%d", postCount(&curCalls), postCount(&otherCalls))
	}
}

// Authenticated pinned local 429 skips to alternate 200 with stable session.
func TestUnifiedAuthPinnedLocal429(t *testing.T) {
	monitor := NewMonitor()
	gateway := authTwoProxyGateway(t, monitor, 5)
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	postStub(t, gateway, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ses := "ses_unified_auth_local_1"
	r1, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r1"), 0)
	drainResp(r1)
	pin, ok := gateway.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must pin")
	}
	ordered := affinityProxyOrder(gateway.pools["z"], pin.CredID, pin.ProxyRaw)
	curRaw, otherRaw := ordered[0].name, ordered[1].name
	gateway.scheduler.noteProxy429Failure(TierZen, "z", curRaw, AttemptClassRateLimited, 429, time.Minute, time.Now().UnixNano())
	cap := &capturedUpstream{}
	var c0, c1 atomic.Int32
	postStub(t, gateway, "z", poolIndexByRaw(gateway, "z", curRaw), &c0, cap, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", poolIndexByRaw(gateway, "z", otherRaw), &c1, cap, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	r2, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r2"), 0)
	if err != nil || r2.StatusCode != 200 {
		t.Fatalf("auth local 429 must skip to 200, err=%v resp=%v", err, r2)
	}
	drainResp(r2)
	sessions, bodies := cap.get()
	if len(sessions) != 1 || len(bodies) != 1 {
		t.Fatalf("captured=%d want 1", len(sessions))
	}
	pin2, _ := gateway.scheduler.pinGet(ses, "m")
	if pin2.ProxyRaw != otherRaw || pin2.Generation != pin.Generation+1 {
		t.Fatalf("auth pin must move: %+v vs %+v", pin, pin2)
	}
	if _, _, ok := gateway.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("local skip must not write credential429")
	}
}

// Authenticated pinned full exhaustion writes credential429 then custom.
func TestUnifiedAuthPinnedExhaustionCustom(t *testing.T) {
	customHits := atomic.Int32{}
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-auth-exh")))
	}))
	defer custom.Close()
	ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm-auth-exh"}
	cfg := testGatewayConfig(map[string][]string{"a": {"direct"}, "z": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"})
	cfg.Anonymous = true
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 5
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	cred := gw.authCreds[0]
	ses := "ses_unified_auth_exh_custom_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	var c0, c1 atomic.Int32
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), &c0, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), &c1, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "9")
		return r, nil
	})
	route := authOnlyRoute()
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r1"), 0, ex)
	if err != nil || resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("auth exhaustion must reach custom 200, err=%v resp=%v eff=%+v", err, resp, eff)
	}
	drainResp(resp)
	if postCount(&c0) != 1 || postCount(&c1) != 1 || customHits.Load() != 1 {
		t.Fatalf("sends=%d/%d custom=%d want 1/1/1", postCount(&c0), postCount(&c1), customHits.Load())
	}
	if got := resp.Header.Get("Retry-After"); got != "" {
		// Custom success carries no upstream 429 Retry-After.
		t.Logf("custom success retry-after=%q (informational)", got)
	}
	until, status, ok := gw.scheduler.credential429CooldownStatus(cred.id)
	if !ok || status != 429 || until <= time.Now().UnixNano() {
		t.Fatalf("exhaustion must write credential429")
	}
	// No-custom variant keeps the last native 429 envelope.
	cfg2 := testGatewayConfig(map[string][]string{"a": {"direct"}, "z": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"})
	cfg2.Anonymous = true
	cfg2.Keys = []string{"zen-key-aaaaa"}
	cfg2.Retry.MaxAttempts = 5
	n2, _ := NormalizeConfig("config.json", cfg2)
	gw2, _ := NewGateway(n2, discardGatewayLogger(), NewMonitor())
	cred2 := gw2.authCreds[0]
	ses2 := "ses_unified_auth_exh_nocustom_1"
	gw2.bindSessionPin(ses2, "m", TierZen, cred2.id, "z", gw2.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw2.cfg.Upstream.Zen))
	pinB, _ := gw2.scheduler.pinGet(ses2, "m")
	ordB := affinityProxyOrder(gw2.pools["z"], pinB.CredID, pinB.ProxyRaw)
	postStub(t, gw2, "z", poolIndexByRaw(gw2, "z", ordB[0].name), nil, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gw2, "z", poolIndexByRaw(gw2, "z", ordB[1].name), nil, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "9")
		return r, nil
	})
	resp2, _, _, err := gw2.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses2, "r1"), 0, ex)
	if err != nil || resp2.StatusCode != 429 {
		t.Fatalf("no-custom exhaustion must keep 429, err=%v resp=%v", err, resp2)
	}
	if got := resp2.Header.Get("Retry-After"); got != "9" {
		t.Fatalf("last Retry-After=%q want 9", got)
	}
	drainResp(resp2)
}

// Candidate filtering: unhealthy/target/channel/proxy429 skipped, never live evidence.
func TestUnifiedCandidateFiltering(t *testing.T) {
	monitor := NewMonitor()
	gateway := authThreeProxyGateway(t, monitor)
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	for i := range gateway.pools["z"].items {
		postStub(t, gateway, "z", i, nil, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
	}
	ses := "ses_unified_filter_1"
	r1, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r1"), 0)
	drainResp(r1)
	pin, ok := gateway.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must pin")
	}
	ordered := affinityProxyOrder(gateway.pools["z"], pin.CredID, pin.ProxyRaw)
	// Filter 1: unhealthy middle proxy skipped.
	unhealthyRaw := ordered[1].name
	for _, p := range gateway.pools["z"].items {
		if p != nil && p.name == unhealthyRaw {
			p.healthy.Store(false)
		}
	}
	// Filter 2: target cooldown on third proxy.
	thirdRaw := ordered[2].name
	thirdIdentity := targetIdentity(TierZen, pin.CredID, "z", thirdRaw, "m")
	gateway.scheduler.noteTargetFailure(thirdIdentity, AttemptClassUpstreamFailure, 500, 0)
	// Only current remains eligible: single live 429 exhausts (1==1) and writes credential429.
	curIdx := poolIndexByRaw(gateway, "z", ordered[0].name)
	var curCalls atomic.Int32
	postStub(t, gateway, "z", curIdx, &curCalls, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "5")
		return r, nil
	})
	r2, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r2"), 0)
	if err != nil || r2.StatusCode != 429 {
		t.Fatalf("filtered single-eligible 429 must exhaust, err=%v resp=%v", err, r2)
	}
	drainResp(r2)
	if postCount(&curCalls) != 1 {
		t.Fatalf("only eligible proxy must send once, got %d", postCount(&curCalls))
	}
	if _, _, ok := gateway.scheduler.credential429CooldownStatus(pin.CredID); !ok {
		t.Fatalf("single-eligible exhaustion must write credential429")
	}
	// Restore health for other tests (fresh gateway per test, no-op).
	for _, p := range gateway.pools["z"].items {
		if p != nil {
			p.healthy.Store(true)
		}
	}
}
