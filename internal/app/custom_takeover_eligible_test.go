package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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

// Bounded pinned L2 consumption: 429 -> 401 on the last eligible exhausts
// the frozen auth binding and takes over custom. Mirrors
// TestPinnedConsumptionAuth429Then403Takeover with 401 terminal to lock the
// pinnedConsumptionAllowCustom 401 whitelist end-to-end.
func TestPinnedConsumptionAuth429Then401Takeover(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 1
	cred := gw.authCreds[0]
	ses := "ses_pinned_consume_429_401_auth_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	if len(ordered) != 2 {
		t.Fatalf("ordered=%d want 2", len(ordered))
	}
	var p0Calls, p1Calls atomic.Int32
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), &p0Calls, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), &p1Calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(401, `{"error":"bad key"}`), nil
	})
	resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("auth 429->401 must take over custom 200, err=%v resp=%v eff=%+v", err, resp, eff)
	}
	drainResp(resp)
	if customHits.Load() != 1 {
		t.Fatalf("hits=%d want 1", customHits.Load())
	}
	if postCount(&p0Calls) != 1 || postCount(&p1Calls) != 1 {
		t.Fatalf("native sends p0=%d p1=%d want 1/1", postCount(&p0Calls), postCount(&p1Calls))
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3 (2 native + 1 custom)", attempts)
	}
	if eff.ID != "cm-inc6" {
		t.Fatalf("effective model=%q want cm-inc6 rewrite", eff.ID)
	}
	binding, ok := gw.scheduler.fallbacks.get(ses)
	if !ok {
		t.Fatalf("must bind fallback")
	}
	if binding.Model != "cm-inc6" || binding.Name != "c1" {
		t.Fatalf("fallback binding wrong: %+v", binding)
	}
	if until, status, ok := gw.scheduler.credentialCooldownStatus(pin.CredID); !ok || status != 401 || until <= time.Now().UnixNano() {
		t.Fatalf("401 must write credential cooldown, ok=%v status=%v until=%d", ok, status, until)
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("401 take over must not write credential429")
	}
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "z", ordered[1].name); ok {
		t.Fatalf("401 must not write proxy429 for the 401 proxy")
	}
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "z", ordered[1].name); ok {
		t.Fatalf("401 must not write channel for the 401 proxy")
	}
	targetID := targetIdentity(TierZen, pin.CredID, "z", ordered[1].name, "m")
	if _, _, ok := gw.scheduler.targetCooldownStatus(targetID); ok {
		t.Fatalf("401 must not write target cooldown")
	}
}

// Bounded pinned L2 consumption: anonymous 429 -> 401 takes over custom.
// Reuses the anon two-proxy pool helper; keeps candidate order deterministic.
func TestPinnedConsumptionAnon429Then401Takeover(t *testing.T) {
	var customHits atomic.Int32
	gw := pinnedAnonTwoProxyGateway(t, &customHits, true)
	ses := "ses_pinned_consume_429_401_anon_1"
	pool := gw.pools["a"]
	gw.bindSessionPin(ses, "m", TierZen, anonymousSchedulerCredentialID, "a", pool.items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(pool, pin.CredID, pin.ProxyRaw)
	if len(ordered) != 2 {
		t.Fatalf("ordered=%d want 2", len(ordered))
	}
	var p0Calls, p1Calls atomic.Int32
	postStub(t, gw, "a", poolIndexByRaw(gw, "a", ordered[0].name), &p0Calls, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gw, "a", poolIndexByRaw(gw, "a", ordered[1].name), &p1Calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(401, `{"error":"unauthorized"}`), nil
	})
	resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, pinnedAnonConsumeExtra())
	if err != nil || resp == nil || resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("anon 429->401 must take over custom 200, err=%v resp=%v eff=%+v", err, resp, eff)
	}
	drainResp(resp)
	if customHits.Load() != 1 {
		t.Fatalf("hits=%d want 1", customHits.Load())
	}
	if postCount(&p0Calls) != 1 || postCount(&p1Calls) != 1 {
		t.Fatalf("native sends p0=%d p1=%d want 1/1", postCount(&p0Calls), postCount(&p1Calls))
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3 (2 native + 1 custom)", attempts)
	}
	if eff.ID != "cm-anon-consume" {
		t.Fatalf("effective model=%q want cm-anon-consume rewrite", eff.ID)
	}
	binding, ok := gw.scheduler.fallbacks.get(ses)
	if !ok {
		t.Fatalf("must bind anon fallback")
	}
	if binding.Model != "cm-anon-consume" {
		t.Fatalf("fallback binding wrong: %+v", binding)
	}
	if until, status, ok := gw.scheduler.credentialCooldownStatus(pin.CredID); !ok || status != 401 || until <= time.Now().UnixNano() {
		t.Fatalf("anon 401 must write credential cooldown, ok=%v status=%v until=%d", ok, status, until)
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("anon 401 take over must not write credential429")
	}
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "a", ordered[1].name); ok {
		t.Fatalf("anon 401 must not write proxy429 for the 401 proxy")
	}
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "a", ordered[1].name); ok {
		t.Fatalf("anon 401 must not write channel for the 401 proxy")
	}
	targetID := targetIdentity(TierZen, pin.CredID, "a", ordered[1].name, "m")
	if _, _, ok := gw.scheduler.targetCooldownStatus(targetID); ok {
		t.Fatalf("anon 401 must not write target cooldown")
	}
}

// Single-proxy 401 stays faithful: consumed=false never triggers pinned L2
// custom takeover. Mirrors the existing 403 single-proxy guard but for 401.
func TestPinnedConsumptionAuthSingle401NoTakeover(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	cred := gw.authCreds[0]
	pinTmp, _ := func() (sessionPin, bool) {
		ses := "ses_tmp_precool_401_single"
		gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
		return gw.scheduler.pinGet(ses, "m")
	}()
	orderedTmp := affinityProxyOrder(gw.pools["z"], pinTmp.CredID, pinTmp.ProxyRaw)
	if len(orderedTmp) == 2 {
		gw.scheduler.noteProxy429Failure(TierZen, "z", orderedTmp[1].name, AttemptClassRateLimited, 429, time.Minute, time.Now().UnixNano())
	}
	ses := "ses_pinned_consume_single401_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", orderedTmp[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", orderedTmp[0].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(401, `{"error":"unauthorized"}`), nil
	})
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 401 {
		t.Fatalf("single 401 must stay faithful 401, err=%v resp=%v", err, resp)
	}
	drainResp(resp)
	if eff.Tier == TierCustom || customHits.Load() != 0 {
		t.Fatalf("single 401 must not hit custom")
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("single 401 must not bind fallback")
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(cred.id); ok {
		t.Fatalf("single 401 must not write credential429")
	}
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "z", orderedTmp[0].name); ok {
		t.Fatalf("single 401 must not write proxy429")
	}
}

func TestPinnedConsumptionAnonSingle401NoTakeover(t *testing.T) {
	var customHits atomic.Int32
	gw := pinnedAnonTwoProxyGateway(t, &customHits, true)
	pool := gw.pools["a"]
	tmpSes := "ses_tmp_precool_401_anon_single"
	gw.bindSessionPin(tmpSes, "m", TierZen, anonymousSchedulerCredentialID, "a", pool.items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	tmpPin, _ := gw.scheduler.pinGet(tmpSes, "m")
	orderedTmp := affinityProxyOrder(pool, tmpPin.CredID, tmpPin.ProxyRaw)
	if len(orderedTmp) == 2 {
		gw.scheduler.noteProxy429Failure(TierZen, "a", orderedTmp[1].name, AttemptClassRateLimited, 429, time.Minute, time.Now().UnixNano())
	}
	ses := "ses_pinned_consume_single401_anon_1"
	gw.bindSessionPin(ses, "m", TierZen, anonymousSchedulerCredentialID, "a", orderedTmp[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	postStub(t, gw, "a", poolIndexByRaw(gw, "a", orderedTmp[0].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(401, `{"error":"unauthorized"}`), nil
	})
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, pinnedAnonConsumeExtra())
	if err != nil || resp == nil || resp.StatusCode != 401 {
		t.Fatalf("single anon 401 must stay faithful 401, err=%v resp=%v", err, resp)
	}
	drainResp(resp)
	if eff.Tier == TierCustom || customHits.Load() != 0 {
		t.Fatalf("single anon 401 must not hit custom")
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("single anon 401 must not bind fallback")
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(anonymousSchedulerCredentialID); ok {
		t.Fatalf("single anon 401 must not write credential429")
	}
}

// Bounded pinned L2 consumption: p0 live 429 then L1-final 408/425 on the
// last eligible exhausts the frozen auth binding and takes over custom.
// MaxAttempts=2 with TransientInterval=0 so p1 is observed exactly twice
// (L1 limit includes the first send): p0 1x, p1 2x, then TierCustom 200.
// Exercises doPinnedAuth -> observeSameTargetTransient -> pinnedConsumptionAllowCustom -> maybeTakeoverCustomFallback.
func TestPinnedConsumptionAuth429ThenL1Final408Takeover(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 2
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0
	cred := gw.authCreds[0]
	ses := "ses_pinned_consume_429_408_l1final_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	if len(ordered) != 2 {
		t.Fatalf("ordered=%d want 2", len(ordered))
	}
	var p0Calls, p1Calls atomic.Int32
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), &p0Calls, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), &p1Calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(408, `{"error":"timeout"}`), nil
	})
	resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("auth 429->L1-final 408 must take over custom 200, err=%v resp=%v eff=%+v", err, resp, eff)
	}
	drainResp(resp)
	if customHits.Load() != 1 {
		t.Fatalf("hits=%d want 1", customHits.Load())
	}
	if postCount(&p0Calls) != 1 {
		t.Fatalf("p0 sends=%d want 1", postCount(&p0Calls))
	}
	if postCount(&p1Calls) != 2 {
		t.Fatalf("p1 sends=%d want 2 (L1 observation limit includes first send)", postCount(&p1Calls))
	}
	if attempts != 4 {
		t.Fatalf("attempts=%d want 4 (1 native 429 + 2 native 408 + 1 custom)", attempts)
	}
	if eff.ID != "cm-inc6" {
		t.Fatalf("effective model=%q want cm-inc6 rewrite", eff.ID)
	}
	binding, ok := gw.scheduler.fallbacks.get(ses)
	if !ok {
		t.Fatalf("must bind fallback")
	}
	if binding.Model != "cm-inc6" || binding.Name != "c1" {
		t.Fatalf("fallback binding wrong: %+v", binding)
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("408 takeover must not write credential429")
	}
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "z", ordered[1].name); ok {
		t.Fatalf("408 must not write proxy429 for the 408 proxy")
	}
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "z", ordered[1].name); ok {
		t.Fatalf("408 must not write channel for the 408 proxy")
	}
	targetID := targetIdentity(TierZen, pin.CredID, "z", ordered[1].name, "m")
	if _, _, ok := gw.scheduler.targetCooldownStatus(targetID); ok {
		t.Fatalf("408 must not write target cooldown")
	}
	if _, _, ok := gw.scheduler.credentialCooldownStatus(pin.CredID); ok {
		t.Fatalf("408 must not write credential 401 cooldown")
	}
}

func TestPinnedConsumptionAuth429ThenL1Final425Takeover(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 2
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0
	cred := gw.authCreds[0]
	ses := "ses_pinned_consume_429_425_l1final_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	if len(ordered) != 2 {
		t.Fatalf("ordered=%d want 2", len(ordered))
	}
	var p0Calls, p1Calls atomic.Int32
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), &p0Calls, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), &p1Calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(425, `{"error":"too early"}`), nil
	})
	resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("auth 429->L1-final 425 must take over custom 200, err=%v resp=%v eff=%+v", err, resp, eff)
	}
	drainResp(resp)
	if customHits.Load() != 1 {
		t.Fatalf("hits=%d want 1", customHits.Load())
	}
	if postCount(&p0Calls) != 1 {
		t.Fatalf("p0 sends=%d want 1", postCount(&p0Calls))
	}
	if postCount(&p1Calls) != 2 {
		t.Fatalf("p1 sends=%d want 2 (L1 observation limit includes first send)", postCount(&p1Calls))
	}
	if attempts != 4 {
		t.Fatalf("attempts=%d want 4 (1 native 429 + 2 native 425 + 1 custom)", attempts)
	}
	if eff.ID != "cm-inc6" {
		t.Fatalf("effective model=%q want cm-inc6 rewrite", eff.ID)
	}
	binding, ok := gw.scheduler.fallbacks.get(ses)
	if !ok {
		t.Fatalf("must bind fallback")
	}
	if binding.Model != "cm-inc6" || binding.Name != "c1" {
		t.Fatalf("fallback binding wrong: %+v", binding)
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("425 takeover must not write credential429")
	}
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "z", ordered[1].name); ok {
		t.Fatalf("425 must not write proxy429 for the 425 proxy")
	}
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "z", ordered[1].name); ok {
		t.Fatalf("425 must not write channel for the 425 proxy")
	}
	targetID := targetIdentity(TierZen, pin.CredID, "z", ordered[1].name, "m")
	if _, _, ok := gw.scheduler.targetCooldownStatus(targetID); ok {
		t.Fatalf("425 must not write target cooldown")
	}
	if _, _, ok := gw.scheduler.credentialCooldownStatus(pin.CredID); ok {
		t.Fatalf("425 must not write credential 401 cooldown")
	}
}

// Bounded pinned L2 consumption: p0 live 429 then L1-final stream-startup
// failure (distinct nil-resp + sentinel err) on the last eligible exhausts
// the frozen auth binding and takes over custom. MaxAttempts=2 with
// TransientInterval=0 so p1 is observed exactly twice via empty SSE
// (EOF-before-commit startup failure): p0 1x, p1 2x, then TierCustom 200.
// Exercises doPinnedAuth -> executeAttempt stream gate -> sentinel ->
// observeSameTargetTransient -> pinnedConsumptionAllowCustom(err!=nil) ->
// maybeTakeoverCustomFallback. Startup cools only its single target; it must
// not write proxy429/channel/credential429/credential401.
func TestPinnedConsumptionAuth429ThenStartupTakeover(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 2
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0
	cred := gw.authCreds[0]
	ses := "ses_pinned_consume_429_startup_l1final_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	if len(ordered) != 2 {
		t.Fatalf("ordered=%d want 2", len(ordered))
	}
	var p0Calls, p1Calls atomic.Int32
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), &p0Calls, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), &p1Calls, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(""), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	resp, eff, attempts, err := gw.doUpstreamTiers(ctx, authOnlyRoute(), streamBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("auth 429->L1-final startup must take over custom 200, err=%v resp=%v eff=%+v", err, resp, eff)
	}
	drainResp(resp)
	if customHits.Load() != 1 {
		t.Fatalf("hits=%d want 1", customHits.Load())
	}
	if postCount(&p0Calls) != 1 {
		t.Fatalf("p0 sends=%d want 1", postCount(&p0Calls))
	}
	if postCount(&p1Calls) != 2 {
		t.Fatalf("p1 sends=%d want 2 (L1 observation limit includes first send)", postCount(&p1Calls))
	}
	if attempts != 4 {
		t.Fatalf("attempts=%d want 4 (1 native 429 + 2 native startup + 1 custom)", attempts)
	}
	if eff.ID != "cm-inc6" {
		t.Fatalf("effective model=%q want cm-inc6 rewrite", eff.ID)
	}
	binding, ok := gw.scheduler.fallbacks.get(ses)
	if !ok {
		t.Fatalf("must bind fallback")
	}
	if binding.Model != "cm-inc6" || binding.Name != "c1" {
		t.Fatalf("fallback binding wrong: %+v", binding)
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("startup takeover must not write credential429")
	}
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "z", ordered[1].name); ok {
		t.Fatalf("startup must not write proxy429 for the startup proxy")
	}
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "z", ordered[1].name); ok {
		t.Fatalf("startup must not write channel for the startup proxy")
	}
	if _, _, ok := gw.scheduler.credentialCooldownStatus(pin.CredID); ok {
		t.Fatalf("startup must not write credential 401 cooldown")
	}
	targetID := targetIdentity(TierZen, pin.CredID, "z", ordered[1].name, "m")
	if until, _, ok := gw.scheduler.targetCooldownStatus(targetID); !ok || until <= time.Now().UnixNano() {
		t.Fatalf("startup must cool its single target, ok=%v until=%d", ok, until)
	}
}

// Bounded pinned L2 consumption across protocols: native Responses 429 ->
// 403 on the last eligible exhausts the frozen auth binding and crosses to a
// Responses custom channel. Locks the correct protocol envelope, model
// rewrite, binding identity, and first-crossing stale native refs removal
// (previous_response_id + input reasoning dropped, ordinary history kept).
func TestPinnedConsumptionResponses429Then403Takeover(t *testing.T) {
	var customHits atomic.Int32
	var gotPath string
	var gotBody map[string]any
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customHits.Add(1)
		gotPath = r.URL.Path
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		if r.URL.Path != "/v1/responses" {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackResponsesOK("cm-resp-consume")))
	}))
	t.Cleanup(custom.Close)
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct"}, "z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Keys = []string{"zen-key-inc6aaaaa"}
	cfg.Retry.MaxAttempts = 1
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm-resp-consume", Protocol: ProtocolResponses}}}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	cred := gw.authCreds[0]
	ses := "ses_pinned_consume_resp_429_403_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolResponses, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	if len(ordered) != 2 {
		t.Fatalf("ordered=%d want 2", len(ordered))
	}
	var p0Calls, p1Calls atomic.Int32
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), &p0Calls, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), &p1Calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(403, `{"error":"forbidden"}`), nil
	})
	route := modelRoute{ID: "m", Tier: TierZen, Protocol: ProtocolResponses, Protocols: map[Tier]Protocol{TierZen: ProtocolResponses}, Anonymous: false, KeyTiers: []Tier{TierZen}}
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
	resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r1"), 0, ex)
	if err != nil || resp == nil || resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("responses 429->403 must take over custom 200, err=%v resp=%v eff=%+v", err, resp, eff)
	}
	drainResp(resp)
	if customHits.Load() != 1 {
		t.Fatalf("hits=%d want 1", customHits.Load())
	}
	if postCount(&p0Calls) != 1 || postCount(&p1Calls) != 1 {
		t.Fatalf("native sends p0=%d p1=%d want 1/1", postCount(&p0Calls), postCount(&p1Calls))
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3 (2 native + 1 custom)", attempts)
	}
	if eff.Protocol != ProtocolResponses {
		t.Fatalf("effective protocol=%q want responses", eff.Protocol)
	}
	if eff.ID != "cm-resp-consume" {
		t.Fatalf("effective model=%q want cm-resp-consume rewrite", eff.ID)
	}
	if gotPath != "/v1/responses" {
		t.Fatalf("custom path=%q want /v1/responses", gotPath)
	}
	if gotBody == nil {
		t.Fatalf("custom did not receive a body")
	}
	if gotBody["model"] != "cm-resp-consume" {
		t.Fatalf("model rewrite missing: %v", gotBody["model"])
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
	binding, ok := gw.scheduler.fallbacks.get(ses)
	if !ok {
		t.Fatalf("must bind fallback")
	}
	if binding.Protocol != ProtocolResponses {
		t.Fatalf("binding protocol=%q want responses", binding.Protocol)
	}
	if binding.Model != "cm-resp-consume" {
		t.Fatalf("fallback binding wrong: %+v", binding)
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("responses 403 takeover must not write credential429")
	}
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "z", ordered[1].name); ok {
		t.Fatalf("responses 403 must not write proxy429 for the 403 proxy")
	}
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "z", ordered[1].name); ok {
		t.Fatalf("responses 403 must not write channel for the 403 proxy")
	}
}

// Pinned consumption deadline stop boundary: an already-expired deadline
// with live-429-ready proxies never enters custom and never writes
// credential429. Mirrors TestCustomTakeoverCancelledLive429NoCustomNoCredential429
// with DeadlineExceeded instead of cancel, using a past deadline so no timing
// is involved and no native send occurs.
func TestPinnedConsumptionAuthDeadlineNoCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	cred := gw.authCreds[0]
	ses := "ses_pinned_consume_deadline_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	var p0Calls, p1Calls atomic.Int32
	for i, proxy := range ordered {
		calls := &p0Calls
		if i == 1 {
			calls = &p1Calls
		}
		postStub(t, gw, "z", poolIndexByRaw(gw, "z", proxy.name), calls, nil, func(*http.Request) (*http.Response, error) {
			r := responseWithBody(429, `{"error":"t"}`)
			r.Header.Set("Retry-After", "7")
			return r, nil
		})
	}
	ctx, cancel := context.WithDeadline(pinTestCtx(), time.Now().Add(-time.Second))
	defer cancel()
	resp, eff, _, err := gw.doUpstreamTiers(ctx, authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0)
	if resp != nil {
		drainResp(resp)
	}
	if eff.Tier == TierCustom || customHits.Load() != 0 {
		t.Fatalf("deadline live-429 must not hit custom (hits=%d)", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("deadline live-429 must not bind fallback")
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("deadline live-429 must not write credential429")
	}
	if postCount(&p0Calls) != 0 || postCount(&p1Calls) != 0 {
		t.Fatalf("deadline must send nothing, p0=%d p1=%d", postCount(&p0Calls), postCount(&p1Calls))
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline must preserve DeadlineExceeded, err=%v ctx=%v", err, ctx.Err())
	}
}

// Bounded pinned L2 consumption deadline during the second-proxy blocked
// send: p0 live 429 enters L2 consumption, p1 blocks on the request context
// until the outer deadline expires. Must keep the faithful deadline error,
// no custom hit/bind, and no credential429. Proxy order is the frozen
// affinity order (no wall-clock sleep decides routing); only the deadline
// duration ends the blocked p1 send via request-context completion.
func TestPinnedConsumptionAuthDeadlineDuringSecondProxyNoCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 1
	cred := gw.authCreds[0]
	ses := "ses_pinned_consume_deadline_mid_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	if len(ordered) != 2 {
		t.Fatalf("ordered=%d want 2", len(ordered))
	}
	var p0Calls, p1Calls atomic.Int32
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), &p0Calls, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), &p1Calls, nil, func(r *http.Request) (*http.Response, error) {
		select {
		case <-r.Context().Done():
			return nil, r.Context().Err()
		case <-time.After(10 * time.Second):
			return nil, context.DeadlineExceeded
		}
	})
	ctx, cancel := context.WithDeadline(pinTestCtx(), time.Now().Add(500*time.Millisecond))
	defer cancel()
	resp, eff, attempts, err := gw.doUpstreamTiers(ctx, authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if resp != nil {
		drainResp(resp)
	}
	if postCount(&p0Calls) != 1 {
		t.Fatalf("p0 sends=%d want 1 (first 429 must be sent before deadline)", postCount(&p0Calls))
	}
	if postCount(&p1Calls) != 1 {
		t.Fatalf("p1 sends=%d want 1 (second proxy must be really attempted)", postCount(&p1Calls))
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2 (p0 429 + p1 blocked send)", attempts)
	}
	if customHits.Load() != 0 {
		t.Fatalf("deadline must not hit custom (hits=%d)", customHits.Load())
	}
	if eff.Tier == TierCustom {
		t.Fatalf("deadline must not enter custom tier, got %+v", eff)
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("deadline must not bind fallback")
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("deadline must not write credential429")
	}
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "z", ordered[1].name); ok {
		t.Fatalf("deadline must not write proxy429 for the deadline proxy")
	}
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "z", ordered[1].name); ok {
		t.Fatalf("deadline must not write channel for the deadline proxy")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline must preserve DeadlineExceeded, err=%v resp=%v", err, resp)
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("ctx.Err=%v want context.DeadlineExceeded", ctx.Err())
	}
}
