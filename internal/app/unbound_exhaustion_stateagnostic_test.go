package app

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// Bounded change: unbound native->custom qualification is state-agnostic
// object exhaustion (all frozen objects in the current recovery domain have
// live unavailable evidence). Pinned paths keep the 429-only gate.

// Predicate unit coverage: state-agnostic, no terminal requirement.
func TestUnboundDomainsExhaustedAllowCustom(t *testing.T) {
	mk := func(entered bool, frozen, unavailable int, recovered bool) unboundDomainEvidence {
		return unboundDomainEvidence{Domain: "d", Entered: entered, Frozen: frozen, Live429: 0, Unavailable: unavailable, Terminal: 500, Recovered400: recovered}
	}
	if !unboundDomainsExhaustedAllowCustom([]unboundDomainEvidence{mk(true, 1, 1, false)}, false, false, false) {
		t.Fatalf("single exhausted domain with non-429 terminal must allow")
	}
	if !unboundDomainsExhaustedAllowCustom([]unboundDomainEvidence{
		{Domain: "anonymous", Entered: true, Frozen: 2, Live429: 1, Unavailable: 2, Terminal: 403},
		{Domain: "cred", Entered: true, Frozen: 1, Live429: 0, Unavailable: 1, Terminal: 500},
	}, false, false, false) {
		t.Fatalf("mixed 429/non-429 full exhaustion must allow")
	}
	if unboundDomainsExhaustedAllowCustom([]unboundDomainEvidence{mk(true, 2, 1, false)}, false, false, false) {
		t.Fatalf("partial exhaustion must deny")
	}
	if unboundDomainsExhaustedAllowCustom([]unboundDomainEvidence{mk(false, 0, 0, false)}, false, false, false) {
		t.Fatalf("empty/pre-cooled domain must deny")
	}
	if unboundDomainsExhaustedAllowCustom([]unboundDomainEvidence{mk(true, 1, 1, true)}, true, false, false) {
		t.Fatalf("400 replay must deny")
	}
	if unboundDomainsExhaustedAllowCustom([]unboundDomainEvidence{mk(true, 1, 1, false)}, false, true, false) {
		t.Fatalf("cancel must deny")
	}
	if unboundDomainsExhaustedAllowCustom([]unboundDomainEvidence{mk(true, 1, 1, false)}, false, false, true) {
		t.Fatalf("committed must deny")
	}
	if unboundDomainsExhaustedAllowCustom(nil, false, false, false) {
		t.Fatalf("no domains must deny")
	}
	// Per-domain independence: one exhausted plus one partial denies (no
	// cross-domain summing).
	if unboundDomainsExhaustedAllowCustom([]unboundDomainEvidence{mk(true, 1, 1, false), mk(true, 2, 1, false)}, false, false, false) {
		t.Fatalf("credential domains must not merge counts")
	}
}

// Full non-429 exhaustion allows custom: anonymous 403 + auth 403.
func TestUnboundExhaustionNon429AllowsCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, []string{"zen-key-exhaust-88888"}, &customHits, true)
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(403, `{"error":"forbidden"}`), nil
	})
	postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(403, `{"error":"forbidden"}`), nil
	})
	ses := "ses_unbound_non429_403_1"
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("full 403 exhaustion must take over custom, got %d %+v", resp.StatusCode, eff)
	}
	if customHits.Load() != 1 {
		t.Fatalf("must hit custom once, got %d", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("must bind fallback")
	}
}

// Mixed 429 + L1-final 5xx allows custom.
func TestUnboundExhaustionMixed429And5xxAllowsCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, []string{"zen-key-exhaust-99999"}, &customHits, true)
	stub429(t, gw, "a", 0)
	postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(500, `{"error":"boom"}`), nil
	})
	ses := "ses_unbound_mixed_429_5xx_1"
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("mixed 429+5xx exhaustion must take over custom, got %d %+v", resp.StatusCode, eff)
	}
	if customHits.Load() != 1 {
		t.Fatalf("must hit custom once, got %d", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("must bind fallback")
	}
}

// L1-final transport exhaustion allows custom.
func TestUnboundExhaustionTransportAllowsCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, []string{"zen-key-exhaust-10101"}, &customHits, true)
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})
	postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})
	ses := "ses_unbound_transport_1"
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("transport exhaustion must take over custom, got %d %+v", resp.StatusCode, eff)
	}
	if customHits.Load() != 1 {
		t.Fatalf("must hit custom once, got %d", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("must bind fallback")
	}
}

// 401 exhaustion allows custom.
func TestUnboundExhaustion401AllowsCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, []string{"zen-key-exhaust-20202"}, &customHits, true)
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(401, `{"error":"unauthorized"}`), nil
	})
	postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(401, `{"error":"unauthorized"}`), nil
	})
	ses := "ses_unbound_401_1"
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("401 exhaustion must take over custom, got %d %+v", resp.StatusCode, eff)
	}
	if customHits.Load() != 1 {
		t.Fatalf("must hit custom once, got %d", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("must bind fallback")
	}
}

// 400 replay with a non-429 replay outcome never allows custom.
func TestUnboundExhaustion400Replay500NoCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, []string{"zen-key-exhaust-30303"}, &customHits, true)
	stub429(t, gw, "a", 0)
	var first atomic.Int32
	postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		if first.Add(1) == 1 {
			return responseWithBody(400, `{"error":"bad"}`), nil
		}
		return responseWithBody(500, `{"error":"after"}`), nil
	})
	ses := "ses_unbound_400_500_1"
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 500 {
		t.Fatalf("400 replay final must stay 500, got %d", resp.StatusCode)
	}
	if eff.Tier == TierCustom || customHits.Load() != 0 {
		t.Fatalf("400 replay must not hit custom (hits=%d)", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("400 replay must not bind fallback")
	}
}

// Deadline (not just cancel) never allows custom.
func TestUnboundExhaustionDeadlineNoCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, []string{"zen-key-exhaust-40404"}, &customHits, true)
	stub429(t, gw, "a", 0)
	stub429(t, gw, "z", 0)
	ctx, cancel := context.WithDeadline(pinTestCtx(), time.Now().Add(-time.Second))
	defer cancel()
	resp, _, _, _ := gw.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), pinIDs("ses_unbound_deadline_1", "r1"), 0, unboundExhaustionExtra())
	if resp != nil {
		drainResp(resp)
	}
	if customHits.Load() != 0 {
		t.Fatalf("deadline route must not hit custom (hits=%d)", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get("ses_unbound_deadline_1"); ok {
		t.Fatalf("deadline route must not bind fallback")
	}
}

// Credential domains never merge: one exhausted credential plus one
// ordinary-rejection credential denies custom even though anonymous is
// exhausted. Per-credential fate branches on the Bearer key.
func TestUnboundExhaustionCredentialDomainsNotMerged(t *testing.T) {
	var customHits atomic.Int32
	keys := []string{"zen-key-exhaust-50505a", "zen-key-exhaust-50505b"}
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, keys, &customHits, true)
	stub429(t, gw, "a", 0)
	keyA := gw.authCreds[0].key
	postStub(t, gw, "z", 0, nil, nil, func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") == "Bearer "+keyA {
			resp := responseWithBody(429, `{"error":"slow"}`)
			resp.Header.Set("Retry-After", "9")
			return resp, nil
		}
		return responseWithBody(422, `{"error":"unprocessable"}`), nil
	})
	ses := "ses_unbound_cred_isolation_1"
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if eff.Tier == TierCustom || customHits.Load() != 0 {
		t.Fatalf("split credential domains must not hit custom (hits=%d)", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("split credential domains must not bind fallback")
	}
	if resp.StatusCode != 429 && resp.StatusCode != 422 {
		t.Fatalf("split domains must keep native terminal, got %d", resp.StatusCode)
	}
}

// L1-final stream startup failure counts as unbound object-unavailable: two
// frozen candidates both failing startup after same-target L1 observation
// exhaust the single-credential domain and take over custom. Per-candidate
// POST counts prove the existing same-target L1 retry is preserved.
func TestUnboundExhaustionStreamStartupAllowsCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct", "http://127.0.0.1:8081"}, []string{"zen-key-exhaust-startup-1"}, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 2
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0
	var z0, z1 atomic.Int32
	postStub(t, gw, "z", 0, &z0, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(""), nil
	})
	postStub(t, gw, "z", 1, &z1, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(""), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ses := "ses_unbound_startup_1"
	resp, eff, attempts, err := gw.doUpstreamTiers(ctx, authOnlyRoute(), streamBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("startup exhaustion must take over custom, got %d %+v", resp.StatusCode, eff)
	}
	if customHits.Load() != 1 {
		t.Fatalf("must hit custom once, got %d", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("must bind fallback")
	}
	if got := postCount(&z0); got != 2 {
		t.Fatalf("z0=%d want 2 (initial + 1 same-target L1 retry)", got)
	}
	if got := postCount(&z1); got != 2 {
		t.Fatalf("z1=%d want 2 (initial + 1 same-target L1 retry)", got)
	}
	if attempts != 5 {
		t.Fatalf("attempts=%d want 5 (4 native + 1 custom)", attempts)
	}
}

// Cancelled stream startup exhaustion never takes over custom: the cancel
// gate denies even when every frozen candidate would otherwise report
// startup failure. Guards against over-broadening the new sentinel count.
func TestUnboundExhaustionStreamStartupCancelledNoCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct", "http://127.0.0.1:8081"}, []string{"zen-key-exhaust-startup-2"}, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 2
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0
	postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(""), nil
	})
	postStub(t, gw, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(""), nil
	})
	base := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ctx, cancel := context.WithCancel(base)
	cancel()
	ses := "ses_unbound_startup_cancel_1"
	resp, _, _, _ := gw.doUpstreamTiers(ctx, authOnlyRoute(), streamBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if resp != nil {
		drainResp(resp)
	}
	if customHits.Load() != 0 {
		t.Fatalf("cancelled startup must not hit custom (hits=%d)", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("cancelled startup must not bind fallback")
	}
}
