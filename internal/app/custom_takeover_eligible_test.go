package app

import (
	"context"
	"errors"
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

// Bounded pinned L2 consumption exhaustion: 429 walk followed by an
// L1-final transport terminal on the last eligible exhausts the frozen
// binding and takes over custom (terminal 429 not required). Stale 429 is
// drained, credential429 stays unwritten for non-429 takeovers.
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
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, ex)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("429 then transport exhaustion must take over custom 200, err=%v resp=%v", err, resp)
	}
	drainResp(resp)
	if eff.Tier != TierCustom || customHits.Load() != 1 {
		t.Fatalf("transport exhaustion must hit custom once (hits=%d eff=%+v)", customHits.Load(), eff)
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("partial 429 plus transport must not write credential429")
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("transport exhaustion must bind fallback")
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

func consumptionExtra() upstreamExtra {
	return upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
}

// Bounded pinned L2 consumption: direct 429 -> 403 on the last eligible
// exhausts the frozen auth binding and takes over custom.
func TestPinnedConsumptionAuth429Then403Takeover(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 1
	cred := gw.authCreds[0]
	ses := "ses_pinned_consume_429_403_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"t"}`), nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(403, `{"error":"forbidden"}`), nil
	})
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("429->403 exhaustion must take over custom 200, err=%v resp=%v eff=%+v", err, resp, eff)
	}
	drainResp(resp)
	if customHits.Load() != 1 {
		t.Fatalf("hits=%d want 1", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("must bind fallback")
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("non-429 takeover must not write credential429")
	}
}

// Bounded pinned L2 consumption: 429 -> L1-final 500 on the last eligible
// takes over custom.
func TestPinnedConsumptionAuth429Then500Takeover(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 2
	cred := gw.authCreds[0]
	ses := "ses_pinned_consume_429_500_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"t"}`), nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(500, `{"error":"boom"}`), nil
	})
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("429->500 exhaustion must take over custom 200, err=%v resp=%v eff=%+v", err, resp, eff)
	}
	drainResp(resp)
	if customHits.Load() != 1 {
		t.Fatalf("hits=%d want 1", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("must bind fallback")
	}
}

// Bounded pinned L2 consumption: transport (L1-final) -> 403 on the next
// proxy exhausts and takes over custom.
func TestPinnedConsumptionAuthTransportThen403Takeover(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 1
	cred := gw.authCreds[0]
	ses := "ses_pinned_consume_trans_403_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(403, `{"error":"forbidden"}`), nil
	})
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("transport->403 exhaustion must take over custom 200, err=%v resp=%v eff=%+v", err, resp, eff)
	}
	drainResp(resp)
	if customHits.Load() != 1 {
		t.Fatalf("hits=%d want 1", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("must bind fallback")
	}
}

// Single-proxy first non-429 never takes over: eligible==1 with 403 stays
// faithful without custom.
func TestPinnedConsumptionAuthSingleNon429NoTakeover(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	cred := gw.authCreds[0]
	// Pre-cool the alternate proxy so frozen eligible has exactly one live target.
	pinTmp, _ := func() (sessionPin, bool) {
		ses := "ses_tmp_precool_1"
		gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
		return gw.scheduler.pinGet(ses, "m")
	}()
	orderedTmp := affinityProxyOrder(gw.pools["z"], pinTmp.CredID, pinTmp.ProxyRaw)
	if len(orderedTmp) == 2 {
		gw.scheduler.noteProxy429Failure(TierZen, "z", orderedTmp[1].name, AttemptClassRateLimited, 429, time.Minute, time.Now().UnixNano())
	}
	ses := "ses_pinned_consume_single403_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", orderedTmp[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", orderedTmp[0].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(403, `{"error":"forbidden"}`), nil
	})
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 403 {
		t.Fatalf("single 403 must stay faithful 403, err=%v resp=%v", err, resp)
	}
	drainResp(resp)
	if eff.Tier == TierCustom || customHits.Load() != 0 {
		t.Fatalf("single non-429 must not hit custom")
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("must not bind fallback")
	}
}

// Partial eligible unattempted: first proxy 403 ends the request without
// sending the second eligible, so no consumption and no custom.
func TestPinnedConsumptionAuthPartialUnattemptedNoTakeover(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 1
	cred := gw.authCreds[0]
	ses := "ses_pinned_consume_partial_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	var secondHits atomic.Int32
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(403, `{"error":"forbidden"}`), nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), &secondHits, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 403 {
		t.Fatalf("first 403 must stay 403, err=%v resp=%v", err, resp)
	}
	drainResp(resp)
	if eff.Tier == TierCustom || customHits.Load() != 0 {
		t.Fatalf("unattempted eligible must not hit custom")
	}
	if secondHits.Load() != 0 {
		t.Fatalf("second eligible must stay unsent, hits=%d", secondHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("must not bind fallback")
	}
}

// Ordinary 4xx is a request terminal even after a consume action: 429 ->
// 404 stays faithful 404 without custom.
func TestPinnedConsumptionAuthOrdinary404NoTakeover(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 1
	cred := gw.authCreds[0]
	ses := "ses_pinned_consume_404_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"t"}`), nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(404, `{"error":"nope"}`), nil
	})
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 404 {
		t.Fatalf("429->404 must stay faithful 404, err=%v resp=%v", err, resp)
	}
	drainResp(resp)
	if eff.Tier == TierCustom || customHits.Load() != 0 {
		t.Fatalf("ordinary 4xx must not hit custom")
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("must not bind fallback")
	}
}

// 400 corrective replay is a route terminal even after a consume action:
// 429 -> 400 (replay 400) stays faithful without custom.
func TestPinnedConsumptionAuth400AfterConsumeNoTakeover(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 1
	cred := gw.authCreds[0]
	ses := "ses_pinned_consume_400_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"t"}`), nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(400, `{"error":"bad"}`), nil
	})
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 400 {
		t.Fatalf("429->400 replay must stay faithful 400, err=%v resp=%v", err, resp)
	}
	drainResp(resp)
	if eff.Tier == TierCustom || customHits.Load() != 0 {
		t.Fatalf("400 replay must not hit custom")
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("must not bind fallback")
	}
}

// Without an active custom the native faithful envelope wins and no stale
// 429 is forged: 429 -> 403 stays 403, not 429.
func TestPinnedConsumptionAuthNoActiveFaithful(t *testing.T) {
	gw := customTakeoverTestGateway(t, nil, false)
	gw.cfg.Retry.MaxAttempts = 1
	cred := gw.authCreds[0]
	ses := "ses_pinned_consume_noactive_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"t"}`), nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(403, `{"error":"forbidden"}`), nil
	})
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 403 {
		t.Fatalf("no-active 429->403 must stay faithful 403, err=%v resp=%v", err, resp)
	}
	drainResp(resp)
	if eff.Tier == TierCustom {
		t.Fatalf("no-active must not bind custom")
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("no-active must not bind fallback")
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("non-429 exhaustion must not write credential429")
	}
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

// Focused e2e: pinned authenticated first proxy 429 enters L2 consumption,
// second proxy real send cancels context inside stub. Must keep faithful
// cancel envelope, no custom, no fallback bind, no credential429, both
// proxies sent exactly once. Uses in-stub cancel as deterministic barrier
// (no race with HTTP return) and does not modify production logic.
func TestPinnedConsumptionAuthCancelDuringSecondProxyNoCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 1
	cred := gw.authCreds[0]
	ses := "ses_pinned_consume_cancel_mid_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	if len(ordered) != 2 {
		t.Fatalf("ordered=%d want 2", len(ordered))
	}
	ctx, cancel := context.WithCancel(pinTestCtx())
	var p0Calls, p1Calls atomic.Int32
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), &p0Calls, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), &p1Calls, nil, func(*http.Request) (*http.Response, error) {
		cancel()
		return nil, context.Canceled
	})
	resp, eff, _, err := gw.doUpstreamTiers(ctx, authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if resp != nil {
		drainResp(resp)
	}
	if postCount(&p0Calls) != 1 {
		t.Fatalf("p0 sends=%d want 1 (first 429 must be sent)", postCount(&p0Calls))
	}
	if postCount(&p1Calls) != 1 {
		t.Fatalf("p1 sends=%d want 1 (second proxy must be really sent despite mid-send cancel)", postCount(&p1Calls))
	}
	if customHits.Load() != 0 {
		t.Fatalf("cancel must not hit custom (hits=%d)", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("cancel must not bind fallback")
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("cancel must not write credential429")
	}
	if eff.Tier == TierCustom {
		t.Fatalf("cancel must not enter custom tier, got %+v", eff)
	}
	if ctx.Err() == nil {
		t.Fatalf("ctx must be cancelled")
	}
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		if err == nil && resp != nil && resp.StatusCode == 200 {
			t.Fatalf("cancelled request must not succeed with 200")
		}
		t.Fatalf("cancelled request must preserve context error, err=%v resp=%v", err, resp)
	}
	if postCount(&p0Calls)+postCount(&p1Calls) != 2 {
		t.Fatalf("total sends must be exactly 2, got %d+%d", postCount(&p0Calls), postCount(&p1Calls))
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("ctx.Err=%v want context.Canceled", ctx.Err())
	}
}
