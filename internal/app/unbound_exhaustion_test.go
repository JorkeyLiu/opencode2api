package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Bounded unbound recovery-domain exhaustion: the outer unbound custom
// decision requires every frozen unbound domain (anonymous lane plus each
// authenticated credential, never summed) to independently satisfy the
// existing 429-only gate. These tests lock the evidence wiring without
// generalizing fallback beyond 429-only behavior.

func unboundExhaustionGateway(t *testing.T, anonProxies, authProxies []string, keys []string, customHits *atomic.Int32, active bool) *Gateway {
	t.Helper()
	var customURL string
	if customHits != nil {
		custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			customHits.Add(1)
			w.WriteHeader(200)
			_, _ = w.Write([]byte(fallbackChatOK("cm-exhaust")))
		}))
		t.Cleanup(custom.Close)
		customURL = custom.URL
	}
	cfg := testGatewayConfig(
		map[string][]string{"a": anonProxies, "z": authProxies},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Keys = keys
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	if active {
		ch := FallbackChannelConfig{Name: "c1", BaseURL: customURL, APIKey: "k1", Model: "cm-exhaust"}
		cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	} else {
		cfg.Fallback = FallbackConfig{}
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

func unboundExhaustionExtra() upstreamExtra {
	return upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
}

func stub429(t *testing.T, gw *Gateway, pool string, index int) {
	t.Helper()
	postStub(t, gw, pool, index, nil, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"slow"}`)
		r.Header.Set("Retry-After", "9")
		return r, nil
	})
}

// (1) Anonymous partial plus authenticated full exhaustion must not custom.
// State-agnostic exhaustion: ordinary 4xx never counts as unavailable, so
// anon 429 + ordinary 404 stays partial (Unavailable=1/Frozen=2) and denies
// custom even though auth is fully exhausted.
func TestUnboundExhaustionAnonPartialAuthFullNoCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t, []string{"direct", "http://127.0.0.1:8081"}, []string{"direct"}, []string{"zen-key-exhaust-11111"}, &customHits, true)
	// Anon: one 429, one ordinary 404 (ends the channel without counting).
	// Frozen=2, Unavailable=1 either way; partial denies custom.
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"slow"}`)
		r.Header.Set("Retry-After", "9")
		return r, nil
	})
	postStub(t, gw, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(404, `{"error":"not found"}`), nil
	})
	// Auth: single credential x single proxy fully 429.
	stub429(t, gw, "z", 0)
	ses := "ses_unbound_partial_1"
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if customHits.Load() != 0 {
		t.Fatalf("partial anon must not hit custom (hits=%d)", customHits.Load())
	}
	if eff.Tier == TierCustom {
		t.Fatalf("partial anon must not bind custom: %+v", eff)
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("partial anon must not bind fallback")
	}
	// Native behavior preserved: the ordinary 404 ends the anonymous channel
	// and the route keeps its native terminal without custom.
	if resp.StatusCode != 429 && resp.StatusCode != 404 {
		t.Fatalf("partial exhaustion must keep native terminal, got %d", resp.StatusCode)
	}
}

// (2) Anonymous plus every auth credential fully live-429 allows custom.
func TestUnboundExhaustionFullAllowsCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, []string{"zen-key-exhaust-22222"}, &customHits, true)
	stub429(t, gw, "a", 0)
	stub429(t, gw, "z", 0)
	ses := "ses_unbound_full_1"
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("full exhaustion must take over custom, got %d %+v", resp.StatusCode, eff)
	}
	if customHits.Load() != 1 {
		t.Fatalf("full exhaustion must hit custom once, got %d", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("full exhaustion must bind fallback")
	}
}

// (3) Empty/pre-cooled domain prevents custom.
func TestUnboundExhaustionPrecooledEmptyPreventsCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, []string{"zen-key-exhaust-33333"}, &customHits, true)
	// Pre-cool the single anonymous proxy so the anon domain is empty
	// (Entered=false/Frozen=0) while auth fully 429s.
	gw.scheduler.noteProxy429Failure(TierZen, "a", "direct", AttemptClassRateLimited, 429, time.Minute, time.Now().UnixNano())
	stub429(t, gw, "z", 0)
	ses := "ses_unbound_precool_1"
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 429 {
		t.Fatalf("pre-cooled empty must keep native 429, got %d", resp.StatusCode)
	}
	if eff.Tier == TierCustom || customHits.Load() != 0 {
		t.Fatalf("empty domain must not hit custom (hits=%d)", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("empty domain must not bind fallback")
	}
}

// (4) Ordinary 4xx/400/cancel prevents custom. L1-final 5xx/transport/401/403
// now count as unavailable (see TestUnboundExhaustionNon429AllowsCustom), so
// this group uses an ordinary rejection as the non-429 negative.
func TestUnboundExhaustionTerminalInvalidationNoCustom(t *testing.T) {
	t.Run("OrdinaryTerminal", func(t *testing.T) {
		var customHits atomic.Int32
		gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, []string{"zen-key-exhaust-44444"}, &customHits, true)
		stub429(t, gw, "a", 0)
		postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(422, `{"error":"unprocessable"}`), nil
		})
		ses := "ses_unbound_inv_422_1"
		resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
		if err != nil || resp == nil {
			t.Fatalf("err=%v resp=%v", err, resp)
		}
		defer drainResp(resp)
		if resp.StatusCode != 422 {
			t.Fatalf("ordinary terminal must stay 422, got %d", resp.StatusCode)
		}
		if eff.Tier == TierCustom || customHits.Load() != 0 {
			t.Fatalf("ordinary terminal must not hit custom (hits=%d)", customHits.Load())
		}
		if _, ok := gw.scheduler.fallbacks.get(ses); ok {
			t.Fatalf("ordinary terminal must not bind fallback")
		}
	})
	t.Run("Replay429Terminal", func(t *testing.T) {
		var customHits atomic.Int32
		gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, []string{"zen-key-exhaust-55555"}, &customHits, true)
		stub429(t, gw, "a", 0)
		var first atomic.Int32
		postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
			if first.Add(1) == 1 {
				return responseWithBody(400, `{"error":"bad"}`), nil
			}
			r := responseWithBody(429, `{"error":"slow"}`)
			r.Header.Set("Retry-After", "9")
			return r, nil
		})
		ses := "ses_unbound_inv_400_1"
		resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
		if err != nil || resp == nil {
			t.Fatalf("err=%v resp=%v", err, resp)
		}
		defer drainResp(resp)
		// 400 corrective replay is route-terminal even when the replay is 429.
		if resp.StatusCode != 429 {
			t.Fatalf("replay-429 final must stay 429, got %d", resp.StatusCode)
		}
		if eff.Tier == TierCustom || customHits.Load() != 0 {
			t.Fatalf("replay final must not hit custom (hits=%d)", customHits.Load())
		}
		if _, ok := gw.scheduler.fallbacks.get(ses); ok {
			t.Fatalf("replay final must not bind fallback")
		}
	})
	t.Run("Cancelled", func(t *testing.T) {
		var customHits atomic.Int32
		gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, []string{"zen-key-exhaust-66666"}, &customHits, true)
		stub429(t, gw, "a", 0)
		stub429(t, gw, "z", 0)
		ctx, cancel := context.WithCancel(pinTestCtx())
		cancel()
		resp, _, _, _ := gw.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), pinIDs("ses_unbound_inv_cancel_1", "r1"), 0, unboundExhaustionExtra())
		if resp != nil {
			drainResp(resp)
		}
		if customHits.Load() != 0 {
			t.Fatalf("cancelled route must not hit custom (hits=%d)", customHits.Load())
		}
		if _, ok := gw.scheduler.fallbacks.get("ses_unbound_inv_cancel_1"); ok {
			t.Fatalf("cancelled route must not bind fallback")
		}
	})
}

// (5) No active fallback preserves native 429 even under full exhaustion.
func TestUnboundExhaustionNoActivePreserves429(t *testing.T) {
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, []string{"zen-key-exhaust-77777"}, nil, false)
	stub429(t, gw, "a", 0)
	stub429(t, gw, "z", 0)
	ses := "ses_unbound_noactive_1"
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 429 {
		t.Fatalf("no-active full exhaustion must keep 429, got %d", resp.StatusCode)
	}
	if eff.Tier == TierCustom {
		t.Fatalf("no-active must not bind custom")
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("no-active must not bind fallback")
	}
}
