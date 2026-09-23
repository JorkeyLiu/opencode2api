package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// cancelPinGateway builds a two-proxy anon/auth gateway with an active custom
// channel backed by an httptest server. customHits proves no custom send.
func cancelPinGateway(t *testing.T, customHits *atomic.Int32) (*Gateway, *httptest.Server) {
	t.Helper()
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-cancel")))
	}))
	t.Cleanup(custom.Close)
	cfg := testGatewayConfig(
		map[string][]string{
			"a": {"direct", "http://127.0.0.1:8081"},
			"z": {"direct", "http://127.0.0.1:8081"},
		},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm-cancel"}}}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	return gw, custom
}

func cancelCtx() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{}))
}

func assertNoPinNoFallback(t *testing.T, gw *Gateway, session, model string, customHits *atomic.Int32) {
	t.Helper()
	if _, ok := gw.scheduler.pinGet(session, model); ok {
		t.Fatalf("cancelled 2xx must not establish session pin")
	}
	if _, ok := gw.scheduler.fallbacks.get(session); ok {
		t.Fatalf("cancelled 2xx must not bind custom fallback")
	}
	if customHits.Load() != 0 {
		t.Fatalf("cancelled 2xx must not hit custom (hits=%d)", customHits.Load())
	}
}

// Unbound anonymous initial 2xx under cancel: no pin, no second proxy, no auth, no custom.
func TestCancelUnboundAnonymousNoPin(t *testing.T) {
	var customHits atomic.Int32
	gw, _ := cancelPinGateway(t, &customHits)
	ctx, cancel := cancelCtx()
	var a0, a1, z0, z1 atomic.Int32
	anonFn := func(*http.Request) (*http.Response, error) {
		cancel()
		return responseWithBody(200, `{"ok":true}`), nil
	}
	postStub(t, gw, "a", 0, &a0, nil, anonFn)
	postStub(t, gw, "a", 1, &a1, nil, anonFn)
	postStub(t, gw, "z", 0, &z0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "z", 1, &z1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := clientSessionIDs("ses_cancel_unbound_anon")
	resp, _, attempts, err := gw.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("cancelled 2xx keeps its envelope: %v", resp)
	}
	drainAndClose(resp.Body)
	if total := postCount(&a0) + postCount(&a1); total != 1 {
		t.Fatalf("must end after first 2xx: anon=%d/%d", postCount(&a0), postCount(&a1))
	}
	if postCount(&z0)+postCount(&z1) != 0 {
		t.Fatalf("must not enter auth after anon 2xx: z=%d/%d", postCount(&z0), postCount(&z1))
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1", attempts)
	}
	assertNoPinNoFallback(t, gw, ids.Session, "m", &customHits)
}

// No-cancel control: same path binds the pin.
func TestNoCancelUnboundAnonymousBinds(t *testing.T) {
	var customHits atomic.Int32
	gw, _ := cancelPinGateway(t, &customHits)
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := clientSessionIDs("ses_nocancel_unbound_anon")
	resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), ids, 0)
	if err != nil {
		t.Fatal(err)
	}
	drainAndClose(resp.Body)
	if _, ok := gw.scheduler.pinGet(ids.Session, "m"); !ok {
		t.Fatalf("no-cancel 2xx must bind session pin")
	}
}

// Unbound auth initial 2xx under cancel: no pin, no second candidate, no custom.
func TestCancelUnboundAuthNoPin(t *testing.T) {
	var customHits atomic.Int32
	gw, _ := cancelPinGateway(t, &customHits)
	ctx, cancel := cancelCtx()
	var z0, z1 atomic.Int32
	// Anonymous lane is skipped for auth-only routes; stub it to catch strays.
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	authFn := func(r *http.Request) (*http.Response, error) {
		// First frozen candidate may be either proxy; cancel on first POST
		// regardless of which proxy the HRW order hits first.
		cancel()
		return responseWithBody(200, `{"ok":true}`), nil
	}
	postStub(t, gw, "z", 0, &z0, nil, authFn)
	postStub(t, gw, "z", 1, &z1, nil, authFn)
	ids := clientSessionIDs("ses_cancel_unbound_auth")
	resp, _, attempts, err := gw.doUpstreamTiers(ctx, authOnlyRoute(), routeBodies(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("cancelled 2xx keeps its envelope: %v", resp)
	}
	drainAndClose(resp.Body)
	if total := postCount(&z0) + postCount(&z1); total != 1 {
		t.Fatalf("must end after first 2xx: z=%d/%d", postCount(&z0), postCount(&z1))
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1", attempts)
	}
	assertNoPinNoFallback(t, gw, ids.Session, "m", &customHits)
}

// Unbound auth L1-after 2xx under cancel: 500 then cancel+200 must not bind.
func TestCancelUnboundAuthL1SuccessNoPin(t *testing.T) {
	var customHits atomic.Int32
	gw, _ := cancelPinGateway(t, &customHits)
	ctx, cancel := cancelCtx()
	var calls atomic.Int32
	var z0, z1 atomic.Int32
	l1Fn := func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return responseWithBody(500, `{"error":"boom"}`), nil
		}
		cancel()
		return responseWithBody(200, `{"ok":true}`), nil
	}
	postStub(t, gw, "z", 0, &z0, nil, l1Fn)
	postStub(t, gw, "z", 1, &z1, nil, l1Fn)
	ids := clientSessionIDs("ses_cancel_unbound_auth_l1")
	resp, _, _, err := gw.doUpstreamTiers(ctx, authOnlyRoute(), routeBodies(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("cancelled L1 2xx keeps its envelope: %v", resp)
	}
	drainAndClose(resp.Body)
	// Same-target L1 stays on the first candidate; the other proxy is untouched.
	if total := postCount(&z0) + postCount(&z1); total != 2 || (postCount(&z0) != 2 && postCount(&z1) != 2) {
		t.Fatalf("L1 must stay same-target: z=%d/%d want 2 on one proxy", postCount(&z0), postCount(&z1))
	}
	assertNoPinNoFallback(t, gw, ids.Session, "m", &customHits)
}

// Pinned anonymous: current 429 then alternate cancel+200 must not move.
func TestCancelPinnedAnonymousNoMove(t *testing.T) {
	var customHits atomic.Int32
	gw, _ := cancelPinGateway(t, &customHits)
	// Establish pin without cancel.
	estab := clientSessionIDs("ses_cancel_pinned_anon")
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), estab, 0)
	if err != nil {
		t.Fatal(err)
	}
	drainAndClose(resp.Body)
	pin, ok := gw.scheduler.pinGet(estab.Session, "m")
	if !ok {
		t.Fatalf("establishment must pin")
	}
	beforeRaw, beforeGen := pin.ProxyRaw, pin.Generation
	pool := gw.pools["a"]
	curIdx, altIdx := -1, -1
	for i, p := range pool.items {
		if p != nil && p.name == beforeRaw {
			curIdx = i
		} else {
			altIdx = i
		}
	}
	if curIdx < 0 || altIdx < 0 {
		t.Fatalf("pinned proxy not found: %+v", pin)
	}
	ctx, cancel := cancelCtx()
	var curCalls, altCalls atomic.Int32
	postStub(t, gw, "a", curIdx, &curCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"slow"}`), nil
	})
	postStub(t, gw, "a", altIdx, &altCalls, nil, func(*http.Request) (*http.Response, error) {
		cancel()
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := requestIDs{Session: estab.Session, Request: "req-cancel-pinned-anon", Project: "prj-test"}
	resp2, _, _, err := gw.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp2 == nil || resp2.StatusCode != 200 {
		t.Fatalf("cancelled pinned 2xx keeps its envelope: %v", resp2)
	}
	drainAndClose(resp2.Body)
	if postCount(&curCalls) != 1 || postCount(&altCalls) != 1 {
		t.Fatalf("pinned walk must try current then alternate once: %d/%d", postCount(&curCalls), postCount(&altCalls))
	}
	after, ok := gw.scheduler.pinGet(estab.Session, "m")
	if !ok {
		t.Fatalf("pin must still exist")
	}
	if after.ProxyRaw != beforeRaw || after.Generation != beforeGen {
		t.Fatalf("cancelled 2xx must not move pin: %q gen %d -> %q gen %d", beforeRaw, beforeGen, after.ProxyRaw, after.Generation)
	}
	if _, ok := gw.scheduler.fallbacks.get(estab.Session); ok {
		t.Fatalf("cancelled pinned 2xx must not bind custom")
	}
	if customHits.Load() != 0 {
		t.Fatalf("cancelled pinned 2xx must not hit custom")
	}
}

// Pinned auth: current 429 then alternate cancel+200 must not move.
func TestCancelPinnedAuthNoMove(t *testing.T) {
	var customHits atomic.Int32
	gw, _ := cancelPinGateway(t, &customHits)
	estab := clientSessionIDs("ses_cancel_pinned_auth")
	postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), estab, 0)
	if err != nil {
		t.Fatal(err)
	}
	drainAndClose(resp.Body)
	pin, ok := gw.scheduler.pinGet(estab.Session, "m")
	if !ok {
		t.Fatalf("establishment must pin")
	}
	beforeRaw, beforeGen := pin.ProxyRaw, pin.Generation
	pool := gw.pools["z"]
	curIdx, altIdx := -1, -1
	for i, p := range pool.items {
		if p != nil && p.name == beforeRaw {
			curIdx = i
		} else {
			altIdx = i
		}
	}
	if curIdx < 0 || altIdx < 0 {
		t.Fatalf("pinned proxy not found: %+v", pin)
	}
	ctx, cancel := cancelCtx()
	var curCalls, altCalls atomic.Int32
	postStub(t, gw, "z", curIdx, &curCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"slow"}`), nil
	})
	postStub(t, gw, "z", altIdx, &altCalls, nil, func(*http.Request) (*http.Response, error) {
		cancel()
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := requestIDs{Session: estab.Session, Request: "req-cancel-pinned-auth", Project: "prj-test"}
	resp2, _, _, err := gw.doUpstreamTiers(ctx, authOnlyRoute(), routeBodies(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp2 == nil || resp2.StatusCode != 200 {
		t.Fatalf("cancelled pinned 2xx keeps its envelope: %v", resp2)
	}
	drainAndClose(resp2.Body)
	if postCount(&curCalls) != 1 || postCount(&altCalls) != 1 {
		t.Fatalf("pinned walk must try current then alternate once: %d/%d", postCount(&curCalls), postCount(&altCalls))
	}
	after, ok := gw.scheduler.pinGet(estab.Session, "m")
	if !ok {
		t.Fatalf("pin must still exist")
	}
	if after.ProxyRaw != beforeRaw || after.Generation != beforeGen {
		t.Fatalf("cancelled 2xx must not move pin: %q gen %d -> %q gen %d", beforeRaw, beforeGen, after.ProxyRaw, after.Generation)
	}
	if _, ok := gw.scheduler.fallbacks.get(estab.Session); ok {
		t.Fatalf("cancelled pinned 2xx must not bind custom")
	}
	if customHits.Load() != 0 {
		t.Fatalf("cancelled pinned 2xx must not hit custom")
	}
}

// No-cancel pinned control: same 429 walk moves to the alternate.
func TestNoCancelPinnedAuthMoves(t *testing.T) {
	var customHits atomic.Int32
	gw, _ := cancelPinGateway(t, &customHits)
	estab := clientSessionIDs("ses_nocancel_pinned_auth")
	postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), estab, 0)
	if err != nil {
		t.Fatal(err)
	}
	drainAndClose(resp.Body)
	pin, _ := gw.scheduler.pinGet(estab.Session, "m")
	pool := gw.pools["z"]
	curIdx, altIdx := -1, -1
	for i, p := range pool.items {
		if p != nil && p.name == pin.ProxyRaw {
			curIdx = i
		} else {
			altIdx = i
		}
	}
	postStub(t, gw, "z", curIdx, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"slow"}`), nil
	})
	postStub(t, gw, "z", altIdx, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := requestIDs{Session: estab.Session, Request: "req-nocancel-move", Project: "prj-test"}
	resp2, _, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), ids, 0)
	if err != nil {
		t.Fatal(err)
	}
	drainAndClose(resp2.Body)
	after, _ := gw.scheduler.pinGet(estab.Session, "m")
	if after.ProxyRaw == pin.ProxyRaw {
		t.Fatalf("no-cancel 2xx on alternate must move pin")
	}
}

// Exact-400 replay 2xx under cancel (unbound auth): first 400, replay cancel+200, no pin.
func TestCancelExact400ReplayNoPin(t *testing.T) {
	var customHits atomic.Int32
	gw, _ := cancelPinGateway(t, &customHits)
	ctx, cancel := cancelCtx()
	var calls atomic.Int32
	var z0, z1 atomic.Int32
	replayFn := func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return responseWithBody(400, `{"error":"bad"}`), nil
		}
		cancel()
		return responseWithBody(200, `{"ok":true}`), nil
	}
	postStub(t, gw, "z", 0, &z0, nil, replayFn)
	postStub(t, gw, "z", 1, &z1, nil, replayFn)
	ids := clientSessionIDs("ses_cancel_400_replay")
	resp, _, attempts, err := gw.doUpstreamTiers(ctx, authOnlyRoute(), routeBodies(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("cancelled replay 2xx keeps its envelope: %v", resp)
	}
	drainAndClose(resp.Body)
	if total := postCount(&z0) + postCount(&z1); total != 2 || (postCount(&z0) != 2 && postCount(&z1) != 2) {
		t.Fatalf("replay must stay same-target: z=%d/%d want 2 on one proxy", postCount(&z0), postCount(&z1))
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
	assertNoPinNoFallback(t, gw, ids.Session, "m", &customHits)
}
