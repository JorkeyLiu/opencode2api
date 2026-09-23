package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Bounded Increment 6: lock on the existing strict 429-only custom
// fallback qualification (429-only centralization + cancel/no-state-change
// boundary fix). The helper centralizes the previously scattered exhaustion
// gates; cancel/deadline/committed/replay-final route state never qualifies.
// The outer unbound 1/1 step is a terminal-only recheck; the real eligible
// exhaustion proof lives in the frozen inner walks.
func TestCustomTakeoverEligible(t *testing.T) {
	cases := []struct {
		name string
		q    customTakeoverQualification
		want bool
	}{
		{name: "all eligible live 429", q: customTakeoverQualification{ObservedLive429: 2, Eligible: 2, TerminalStatus: 429}, want: true},
		{name: "single eligible live 429", q: customTakeoverQualification{ObservedLive429: 1, Eligible: 1, TerminalStatus: 429}, want: true},
		{name: "observed exceeds eligible", q: customTakeoverQualification{ObservedLive429: 3, Eligible: 2, TerminalStatus: 429}, want: true},
		{name: "partial 429", q: customTakeoverQualification{ObservedLive429: 1, Eligible: 2, TerminalStatus: 429}, want: false},
		{name: "no evidence", q: customTakeoverQualification{ObservedLive429: 0, Eligible: 2, TerminalStatus: 429}, want: false},
		{name: "zero eligible never qualifies", q: customTakeoverQualification{ObservedLive429: 0, Eligible: 0, TerminalStatus: 429}, want: false},
		{name: "pre-cooled excluded still partial", q: customTakeoverQualification{ObservedLive429: 1, Eligible: 2, TerminalStatus: 429}, want: false},
		{name: "terminal 400", q: customTakeoverQualification{ObservedLive429: 2, Eligible: 2, TerminalStatus: 400}, want: false},
		{name: "terminal 401", q: customTakeoverQualification{ObservedLive429: 2, Eligible: 2, TerminalStatus: 401}, want: false},
		{name: "terminal 403", q: customTakeoverQualification{ObservedLive429: 2, Eligible: 2, TerminalStatus: 403}, want: false},
		{name: "terminal 404", q: customTakeoverQualification{ObservedLive429: 2, Eligible: 2, TerminalStatus: 404}, want: false},
		{name: "terminal 408", q: customTakeoverQualification{ObservedLive429: 2, Eligible: 2, TerminalStatus: 408}, want: false},
		{name: "terminal 425", q: customTakeoverQualification{ObservedLive429: 2, Eligible: 2, TerminalStatus: 425}, want: false},
		{name: "terminal 500", q: customTakeoverQualification{ObservedLive429: 2, Eligible: 2, TerminalStatus: 500}, want: false},
		{name: "terminal 503", q: customTakeoverQualification{ObservedLive429: 2, Eligible: 2, TerminalStatus: 503}, want: false},
		{name: "terminal transport", q: customTakeoverQualification{ObservedLive429: 2, Eligible: 2, TerminalStatus: 0}, want: false},
		{name: "400 replay final", q: customTakeoverQualification{ObservedLive429: 2, Eligible: 2, TerminalStatus: 429, Recovered400: true}, want: false},
		{name: "cancelled", q: customTakeoverQualification{ObservedLive429: 2, Eligible: 2, TerminalStatus: 429, Cancelled: true}, want: false},
		{name: "committed stream", q: customTakeoverQualification{ObservedLive429: 2, Eligible: 2, TerminalStatus: 429, Committed: true}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := customTakeoverEligible(tc.q); got != tc.want {
				t.Fatalf("eligible=%v want %v (q=%+v)", got, tc.want, tc.q)
			}
		})
	}
}

func customTakeoverTestGateway(t *testing.T, customHits *atomic.Int32, active bool) *Gateway {
	t.Helper()
	var customURL string
	if customHits != nil {
		custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			customHits.Add(1)
			w.WriteHeader(200)
			_, _ = w.Write([]byte(fallbackChatOK("cm-inc6")))
		}))
		t.Cleanup(custom.Close)
		customURL = custom.URL
	}
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct"}, "z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Keys = []string{"zen-key-inc6aaaaa"}
	cfg.Retry.MaxAttempts = 5
	if active {
		ch := FallbackChannelConfig{Name: "c1", BaseURL: customURL, APIKey: "k1", Model: "cm-inc6"}
		cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	}
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

// No active fallback preserves the original native 429 envelope.
func TestCustomTakeoverNoActivePreserves429(t *testing.T) {
	gw := customTakeoverTestGateway(t, nil, false)
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "9")
		return r, nil
	})
	postStub(t, gw, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "9")
		return r, nil
	})
	ses := "ses_inc6_noactive_429_1"
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r1"), 0)
	if err != nil || resp == nil || resp.StatusCode != 429 {
		t.Fatalf("no-active exhaustion must keep 429, err=%v resp=%v", err, resp)
	}
	if got := resp.Header.Get("Retry-After"); got != "9" {
		t.Fatalf("last Retry-After=%q want 9", got)
	}
	drainResp(resp)
	if eff.Tier == TierCustom {
		t.Fatalf("no-active must not bind custom")
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("no-active must not bind fallback")
	}
}

// Earlier 429 followed by a transport terminal never triggers custom;
// the stale 429 envelope becomes a neutral 502.
func TestCustomTakeover429ThenTransportNoCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	cred := gw.authCreds[0]
	ses := "ses_inc6_429_transport_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), nil, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})
	// Single L1 observation keeps the transport terminal deterministic.
	gw.cfg.Retry.MaxAttempts = 1
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0)
	if err != nil || resp == nil || resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("429 then transport must stay 502, err=%v resp=%v", err, resp)
	}
	drainResp(resp)
	if eff.Tier == TierCustom || customHits.Load() != 0 {
		t.Fatalf("transport terminal must not hit custom (hits=%d)", customHits.Load())
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("partial 429 plus transport must not write credential429")
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("transport terminal must not bind fallback")
	}
}

// A 400 corrective-replay final never triggers custom even with an active channel.
func TestCustomTakeover400ReplayFinalNoCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	cred := gw.authCreds[0]
	ses := "ses_inc6_400_final_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	var first atomic.Int32
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), nil, nil, func(*http.Request) (*http.Response, error) {
		if first.Add(1) == 1 {
			return responseWithBody(400, `{"error":"bad"}`), nil
		}
		return responseWithBody(500, `{"error":"after"}`), nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0)
	if err != nil || resp == nil || resp.StatusCode != 500 {
		t.Fatalf("400 replay final must stay 500, err=%v resp=%v", err, resp)
	}
	drainResp(resp)
	if eff.Tier == TierCustom || customHits.Load() != 0 {
		t.Fatalf("400 final must not hit custom (hits=%d)", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("400 final must not bind fallback")
	}
}

// Cancel/deadline never enters custom even with an active channel.
func TestCustomTakeoverCancelledNoCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	ctx, cancel := context.WithCancel(pinTestCtx())
	cancel()
	resp, _, _, _ := gw.doUpstreamTiers(ctx, authOnlyRoute(), routeBodies(), pinIDs("ses_inc6_cancel_1", "r1"), 0)
	if resp != nil {
		drainResp(resp)
	}
	if customHits.Load() != 0 {
		t.Fatalf("cancelled route must not hit custom (hits=%d)", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get("ses_inc6_cancel_1"); ok {
		t.Fatalf("cancelled route must not bind fallback")
	}
}

// Pinned pre-cooled local-429 fast paths keep the native 429 local terminal
// under cancel: no custom send, no fallback bind, no new scheduler state.
// Uses an already-cancelled context (no timing). The pinned inner walks are
// invoked directly to bypass the doPinnedUpstream entry cancel short-circuit
// and reach the pre-cooled fast-path branch itself.
func TestCustomTakeoverPrecooledCancelledNoCustom(t *testing.T) {
	t.Run("PinnedAuth", func(t *testing.T) {
		var customHits atomic.Int32
		gw := customTakeoverTestGateway(t, &customHits, true)
		cred := gw.authCreds[0]
		ses := "ses_inc6_precool_cancel_auth_1"
		gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
		for _, item := range gw.pools["z"].items {
			gw.scheduler.noteProxy429Failure(TierZen, "z", item.name, AttemptClassRateLimited, 429, time.Minute, time.Now().UnixNano())
		}
		pin, ok := gw.scheduler.pinGet(ses, "m")
		if !ok {
			t.Fatalf("must pin")
		}
		ctx, cancel := context.WithCancel(pinTestCtx())
		cancel()
		route := authOnlyRoute()
		body := routeBodies()[TierZen]
		resp, eff, _, err := gw.doPinnedAuth(ctx, route, routeBodies(), pinIDs(ses, "r1"), pin, route, gw.cfg.Upstream.Zen, "z", ProtocolChat, body, cred.key, cred.display, cred.index, 0)
		if err != nil || resp == nil || resp.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("pre-cooled cancel must keep local 429, err=%v resp=%v", err, resp)
		}
		drainResp(resp)
		if eff.Tier == TierCustom || customHits.Load() != 0 {
			t.Fatalf("pre-cooled cancel must not hit custom (hits=%d)", customHits.Load())
		}
		if _, ok := gw.scheduler.fallbacks.get(ses); ok {
			t.Fatalf("pre-cooled cancel must not bind fallback")
		}
	})
	t.Run("PinnedAnonymous", func(t *testing.T) {
		var customHits atomic.Int32
		gw := customTakeoverTestGateway(t, &customHits, true)
		ses := "ses_inc6_precool_cancel_anon_1"
		gw.bindSessionPin(ses, "m", TierZen, anonymousSchedulerCredentialID, "a", gw.pools["a"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
		for _, item := range gw.pools["a"].items {
			gw.scheduler.noteProxy429Failure(TierZen, "a", item.name, AttemptClassRateLimited, 429, time.Minute, time.Now().UnixNano())
		}
		pin, ok := gw.scheduler.pinGet(ses, "m")
		if !ok {
			t.Fatalf("must pin")
		}
		ctx, cancel := context.WithCancel(pinTestCtx())
		cancel()
		route := anonAuthRoute()
		body := routeBodies()[TierZen]
		resp, eff, _, err := gw.doPinnedAnonymous(ctx, route, routeBodies(), pinIDs(ses, "r1"), pin, route, gw.cfg.Upstream.Zen, ProtocolChat, body, 0)
		if err != nil || resp == nil || resp.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("pre-cooled cancel must keep local 429, err=%v resp=%v", err, resp)
		}
		drainResp(resp)
		if eff.Tier == TierCustom || customHits.Load() != 0 {
			t.Fatalf("pre-cooled cancel must not hit custom (hits=%d)", customHits.Load())
		}
		if _, ok := gw.scheduler.fallbacks.get(ses); ok {
			t.Fatalf("pre-cooled cancel must not bind fallback")
		}
	})
}

// A full live-429 candidate set under an already-cancelled context never
// enters custom and never writes credential429. The pinned loop short-circuits
// on cancel before any send, so stubs stay 429-ready but unsent; the boundary
// under test is no custom hit, no fallback bind, and no credential429 write.
func TestCustomTakeoverCancelledLive429NoCustomNoCredential429(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	cred := gw.authCreds[0]
	ses := "ses_inc6_cancel_live429_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	for _, proxy := range ordered {
		postStub(t, gw, "z", poolIndexByRaw(gw, "z", proxy.name), nil, nil, func(*http.Request) (*http.Response, error) {
			r := responseWithBody(429, `{"error":"t"}`)
			r.Header.Set("Retry-After", "7")
			return r, nil
		})
	}
	ctx, cancel := context.WithCancel(pinTestCtx())
	cancel()
	resp, eff, _, _ := gw.doUpstreamTiers(ctx, authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0)
	if resp != nil {
		drainResp(resp)
	}
	if eff.Tier == TierCustom || customHits.Load() != 0 {
		t.Fatalf("cancelled live-429 must not hit custom (hits=%d)", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("cancelled live-429 must not bind fallback")
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("cancelled live-429 must not write credential429")
	}
}
