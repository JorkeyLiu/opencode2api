package app

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
		return unboundDomainEvidence{Domain: "d", Entered: entered, Frozen: frozen, Unavailable: unavailable, Recovered400: recovered}
	}
	if !unboundDomainsExhaustedAllowCustom([]unboundDomainEvidence{mk(true, 1, 1, false)}, false, false, false) {
		t.Fatalf("single exhausted domain with non-429 terminal must allow")
	}
	if !unboundDomainsExhaustedAllowCustom([]unboundDomainEvidence{
		{Domain: "anonymous", Entered: true, Frozen: 2, Unavailable: 2},
		{Domain: "cred", Entered: true, Frozen: 1, Unavailable: 1},
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

// Unbound Responses/Anthropic helpers: auth-only native route plus strict
// three-protocol extras/bodies, mirroring TestFallbackConversionAllProtocols
// payload shapes. Bodies carry stream:true; the native stubs below return
// empty SSE so the startup gate reports EOF-before-commit startup failure
// without depending on per-protocol parser event shapes.
func unboundResponsesRoute() modelRoute {
	return modelRoute{
		ID: "m", Tier: TierZen, Protocol: ProtocolResponses,
		Protocols: map[Tier]Protocol{TierZen: ProtocolResponses},
		Anonymous: false, KeyTiers: []Tier{TierZen},
	}
}

func unboundAnthropicRoute() modelRoute {
	return modelRoute{
		ID: "m", Tier: TierZen, Protocol: ProtocolAnthropic,
		Protocols: map[Tier]Protocol{TierZen: ProtocolAnthropic},
		Anonymous: false, KeyTiers: []Tier{TierZen},
	}
}

func unboundResponsesExtra() upstreamExtra {
	return upstreamExtra{External: ProtocolResponses, Payload: map[string]any{"model": "m", "input": "hi"}}
}

func unboundAnthropicExtra() upstreamExtra {
	return upstreamExtra{External: ProtocolAnthropic, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "max_tokens": 8}}
}

func unboundResponsesStreamBodies() map[Tier][]byte {
	return map[Tier][]byte{TierZen: []byte(`{"model":"m","input":"hi","stream":true}`)}
}

func unboundAnthropicStreamBodies() map[Tier][]byte {
	return map[Tier][]byte{TierZen: []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}],"max_tokens":8,"stream":true}`)}
}

func unboundStreamCtx() context.Context {
	return context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
}

// Responses streaming unbound exhaustion: two native candidates each send
// twice (initial + 1 same-target L1 retry with MaxAttempts=2, empty SSE
// startup failure), then one custom fallback succeeds. Real doUpstreamTiers
// unbound path, not the predicate.
func TestUnboundExhaustionResponsesStreamStartupAllowsCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct", "http://127.0.0.1:8081"}, []string{"zen-key-exhaust-resp-startup-1"}, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 2
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0
	var z0, z1 atomic.Int32
	postStub(t, gw, "z", 0, &z0, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(""), nil
	})
	postStub(t, gw, "z", 1, &z1, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(""), nil
	})
	ses := "ses_unbound_resp_startup_1"
	resp, eff, attempts, err := gw.doUpstreamTiers(unboundStreamCtx(), unboundResponsesRoute(), unboundResponsesStreamBodies(), pinIDs(ses, "r1"), 0, unboundResponsesExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	raw, _ := io.ReadAll(resp.Body)
	drainResp(resp)
	if resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("responses startup exhaustion must take over custom, got %d %+v", resp.StatusCode, eff)
	}
	if len(raw) == 0 || !strings.Contains(string(raw), "hello") {
		t.Fatalf("custom response must carry transcoded hello, got %q", raw)
	}
	if customHits.Load() != 1 {
		t.Fatalf("must hit custom once, got %d", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("must bind fallback")
	}
	if _, ok := gw.scheduler.pinGet(ses, "m"); ok {
		t.Fatalf("startup exhaustion must not establish a session pin")
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

// Anthropic streaming unbound exhaustion: same shape as Responses above with
// the Anthropic route/extras/bodies. Empty SSE keeps the startup-failure
// signal parser-independent (clean EOF before commit for every protocol).
func TestUnboundExhaustionAnthropicStreamStartupAllowsCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct", "http://127.0.0.1:8081"}, []string{"zen-key-exhaust-anth-startup-1"}, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 2
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0
	var z0, z1 atomic.Int32
	postStub(t, gw, "z", 0, &z0, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(""), nil
	})
	postStub(t, gw, "z", 1, &z1, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(""), nil
	})
	ses := "ses_unbound_anth_startup_1"
	resp, eff, attempts, err := gw.doUpstreamTiers(unboundStreamCtx(), unboundAnthropicRoute(), unboundAnthropicStreamBodies(), pinIDs(ses, "r1"), 0, unboundAnthropicExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	raw, _ := io.ReadAll(resp.Body)
	drainResp(resp)
	if resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("anthropic startup exhaustion must take over custom, got %d %+v", resp.StatusCode, eff)
	}
	if len(raw) == 0 || !strings.Contains(string(raw), "hello") {
		t.Fatalf("custom response must carry transcoded hello, got %q", raw)
	}
	if customHits.Load() != 1 {
		t.Fatalf("must hit custom once, got %d", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("must bind fallback")
	}
	if _, ok := gw.scheduler.pinGet(ses, "m"); ok {
		t.Fatalf("startup exhaustion must not establish a session pin")
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

// Cross-domain isolation: one exhausted anonymous/authenticated domain plus
// one non-exhausted domain denies custom (AND, never summed). Both directions
// are locked through the real doUpstreamTiers unbound path: the exhausted
// side uses non-429 object-unavailable evidence (403), the live side ends
// with an ordinary 422 that never counts as unavailable. Per-pool POST
// counts prove both domains were entered (no early local return).
func TestUnboundExhaustionAnonAuthDomainsNotMerged(t *testing.T) {
	t.Run("AnonExhaustedAuthOrdinary", func(t *testing.T) {
		var customHits atomic.Int32
		gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, []string{"zen-key-exhaust-60606"}, &customHits, true)
		var anonPosts, authPosts atomic.Int32
		postStub(t, gw, "a", 0, &anonPosts, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(403, `{"error":"forbidden"}`), nil
		})
		postStub(t, gw, "z", 0, &authPosts, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(422, `{"error":"unprocessable"}`), nil
		})
		ses := "ses_unbound_anonauth_iso_1"
		resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
		if err != nil || resp == nil {
			t.Fatalf("err=%v resp=%v", err, resp)
		}
		defer drainResp(resp)
		if resp.StatusCode != 422 {
			t.Fatalf("auth ordinary terminal must stay 422, got %d", resp.StatusCode)
		}
		if eff.Tier == TierCustom || eff.Tier != TierZen {
			t.Fatalf("must keep native auth tier, got %+v", eff)
		}
		if customHits.Load() != 0 {
			t.Fatalf("split anon/auth domains must not hit custom (hits=%d)", customHits.Load())
		}
		if _, ok := gw.scheduler.fallbacks.get(ses); ok {
			t.Fatalf("split anon/auth domains must not bind fallback")
		}
		if _, ok := gw.scheduler.pinGet(ses, "m"); ok {
			t.Fatalf("non-2xx isolation must not establish a session pin")
		}
		if got := postCount(&anonPosts); got != 1 {
			t.Fatalf("anonPosts=%d want 1 (anon domain entered)", got)
		}
		if got := postCount(&authPosts); got != 1 {
			t.Fatalf("authPosts=%d want 1 (auth domain entered)", got)
		}
		if attempts != 2 {
			t.Fatalf("attempts=%d want 2 (1 anon + 1 auth, no custom)", attempts)
		}
	})
	t.Run("AnonOrdinaryAuthExhausted", func(t *testing.T) {
		var customHits atomic.Int32
		gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, []string{"zen-key-exhaust-60707"}, &customHits, true)
		var anonPosts, authPosts atomic.Int32
		postStub(t, gw, "a", 0, &anonPosts, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(422, `{"error":"unprocessable"}`), nil
		})
		postStub(t, gw, "z", 0, &authPosts, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(403, `{"error":"forbidden"}`), nil
		})
		ses := "ses_unbound_anonauth_iso_2"
		resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
		if err != nil || resp == nil {
			t.Fatalf("err=%v resp=%v", err, resp)
		}
		defer drainResp(resp)
		if resp.StatusCode != 403 {
			t.Fatalf("auth exhausted terminal must stay 403, got %d", resp.StatusCode)
		}
		if eff.Tier == TierCustom || eff.Tier != TierZen {
			t.Fatalf("must keep native auth tier, got %+v", eff)
		}
		if customHits.Load() != 0 {
			t.Fatalf("split anon/auth domains must not hit custom (hits=%d)", customHits.Load())
		}
		if _, ok := gw.scheduler.fallbacks.get(ses); ok {
			t.Fatalf("split anon/auth domains must not bind fallback")
		}
		if _, ok := gw.scheduler.pinGet(ses, "m"); ok {
			t.Fatalf("non-2xx isolation must not establish a session pin")
		}
		if got := postCount(&anonPosts); got != 1 {
			t.Fatalf("anonPosts=%d want 1 (anon domain entered)", got)
		}
		if got := postCount(&authPosts); got != 1 {
			t.Fatalf("authPosts=%d want 1 (auth domain entered)", got)
		}
		if attempts != 2 {
			t.Fatalf("attempts=%d want 2 (1 anon + 1 auth, no custom)", attempts)
		}
	})
}

// Cross-credential isolation with non-429 evidence: credential A exhausts via
// 403 (object-unavailable) while credential B ends with an ordinary 422 that
// never counts. Both credential domains are really walked through the
// doUpstreamTiers unbound path (auth-only route, per-credential POST counts
// branched on the Bearer key). The session is chosen so the 403 credential
// orders first; otherwise an ordinary-first walk would stop before entering
// the second credential and the test would not prove two-domain isolation.
func TestUnboundExhaustionCredentialNon429DomainsNotMerged(t *testing.T) {
	var customHits atomic.Int32
	keys := []string{"zen-key-exhaust-60808a", "zen-key-exhaust-60808b"}
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, keys, &customHits, true)
	if len(gw.authCreds) != 2 {
		t.Fatalf("creds=%d want 2", len(gw.authCreds))
	}
	idA, idB := gw.authCreds[0].id, gw.authCreds[1].id
	keyA := gw.authCreds[0].key
	ses := ""
	for i := 0; i < 1000; i++ {
		cand := fmt.Sprintf("ses_unbound_cred_non429_iso_%d", i)
		sa, sb := credOrderScore(cand, idA), credOrderScore(cand, idB)
		firstA := sa > sb || (sa == sb && idA < idB)
		if firstA {
			ses = cand
			break
		}
	}
	if ses == "" {
		t.Fatalf("no session orders credential A first")
	}
	var hitsA, hitsB atomic.Int32
	postStub(t, gw, "z", 0, nil, nil, func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") == "Bearer "+keyA {
			hitsA.Add(1)
			return responseWithBody(403, `{"error":"forbidden"}`), nil
		}
		hitsB.Add(1)
		return responseWithBody(422, `{"error":"unprocessable"}`), nil
	})
	resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 422 {
		t.Fatalf("ordinary credential terminal must stay 422, got %d", resp.StatusCode)
	}
	if eff.Tier == TierCustom || eff.Tier != TierZen {
		t.Fatalf("must keep native auth tier, got %+v", eff)
	}
	if customHits.Load() != 0 {
		t.Fatalf("split credential domains must not hit custom (hits=%d)", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("split credential domains must not bind fallback")
	}
	if _, ok := gw.scheduler.pinGet(ses, "m"); ok {
		t.Fatalf("non-2xx isolation must not establish a session pin")
	}
	if got := int(hitsA.Load()); got != 1 {
		t.Fatalf("credA POSTs=%d want 1 (exhausted 403 domain entered first)", got)
	}
	if got := int(hitsB.Load()); got != 1 {
		t.Fatalf("credB POSTs=%d want 1 (ordinary 422 domain entered second)", got)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2 (1 per credential, no custom)", attempts)
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

// Historically pre-cooled partial freeze still allows custom: one auth proxy
// is filtered before send (historic proxy429), the single remaining frozen
// candidate returns live 403 in this request (Frozen=1/Unavailable=1), so the
// single auth-only domain exhausts and the custom backstop takes over. The
// pre-cooled node must see zero POST. Real doUpstreamTiers unbound path.
func TestUnboundExhaustionPrecooledPartialFrozenAllowsCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct", "http://127.0.0.1:8081"}, []string{"zen-key-exhaust-precool-partial-1"}, &customHits, true)
	if len(gw.pools["z"].items) != 2 {
		t.Fatalf("auth pool items=%d want 2", len(gw.pools["z"].items))
	}
	coldRaw := gw.pools["z"].items[0].name
	// Historic pre-cool: excluded from the frozen set, never counts as live
	// evidence in this request.
	gw.scheduler.noteProxy429Failure(TierZen, "z", coldRaw, AttemptClassRateLimited, 429, time.Minute, time.Now().UnixNano())
	var coldPosts, livePosts atomic.Int32
	postStub(t, gw, "z", 0, &coldPosts, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"slow"}`), nil
	})
	postStub(t, gw, "z", 1, &livePosts, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(403, `{"error":"forbidden"}`), nil
	})
	ses := "ses_unbound_precool_partial_403_1"
	resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("pre-cooled partial freeze with live 403 must take over custom, got %d %+v", resp.StatusCode, eff)
	}
	if customHits.Load() != 1 {
		t.Fatalf("must hit custom once, got %d", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("must bind fallback")
	}
	if _, ok := gw.scheduler.pinGet(ses, "m"); ok {
		t.Fatalf("custom takeover must not establish a session pin")
	}
	if got := postCount(&coldPosts); got != 0 {
		t.Fatalf("coldPosts=%d want 0 (pre-cooled node never sent)", got)
	}
	if got := postCount(&livePosts); got != 1 {
		t.Fatalf("livePosts=%d want 1 (only the sendable frozen candidate sent)", got)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2 (1 native 403 + 1 custom)", attempts)
	}
}

// Shared pool identity isolation: anonymous and authenticated point at the
// same pool. The anonymous live 429 in this request writes the shared
// TierZen+pool+proxyRaw proxy429, so the authenticated phase freezes empty.
// The anonymous live 429 must never be borrowed as the authenticated domain's
// own evidence: custom stays untouched, no fallback binds, and the auth lane
// sends zero POST. Real doUpstreamTiers unbound path with anon->auth entry.
func TestUnboundExhaustionSharedPoolAnon429NotBorrowed(t *testing.T) {
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-exhaust")))
	}))
	t.Cleanup(custom.Close)
	cfg := testGatewayConfig(
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
	)
	cfg.Anonymous = true
	cfg.Keys = []string{"zen-key-exhaust-shared-1"}
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm-exhaust"}}}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	if gw.pools["shared"] == nil || len(gw.pools["shared"].items) != 1 {
		t.Fatalf("shared pool must hold exactly one proxy")
	}
	raw := gw.pools["shared"].items[0].name
	var anonPosts, authPosts atomic.Int32
	postStub(t, gw, "shared", 0, nil, nil, func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") == "Bearer public" {
			anonPosts.Add(1)
			resp := responseWithBody(429, `{"error":"slow"}`)
			resp.Header.Set("Retry-After", "9")
			return resp, nil
		}
		authPosts.Add(1)
		return responseWithBody(403, `{"error":"forbidden"}`), nil
	})
	ses := "ses_unbound_shared_pool_429_1"
	resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 429 {
		t.Fatalf("shared-pool anon429 with empty auth freeze must keep native 429, got %d", resp.StatusCode)
	}
	if eff.Tier == TierCustom || eff.Tier != TierZen {
		t.Fatalf("must keep native zen tier, got %+v", eff)
	}
	if customHits.Load() != 0 {
		t.Fatalf("borrowed anon 429 must not hit custom (hits=%d)", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("borrowed anon 429 must not bind fallback")
	}
	if _, ok := gw.scheduler.pinGet(ses, "m"); ok {
		t.Fatalf("native 429 isolation must not establish a session pin")
	}
	if got := int(anonPosts.Load()); got != 1 {
		t.Fatalf("anonPosts=%d want 1 (anonymous domain entered with live 429)", got)
	}
	if got := int(authPosts.Load()); got != 0 {
		t.Fatalf("authPosts=%d want 0 (authenticated freeze is empty, never sent)", got)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1 (1 anon + 0 auth + 0 custom)", attempts)
	}
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "shared", raw); !ok {
		t.Fatalf("anonymous live 429 must write shared TierZen+pool+proxyRaw proxy429")
	}
}

// 503 closed loop, both domains L1-final: anonymous and authenticated each
// hold one frozen candidate returning live 503. With MaxAttempts=3 each
// candidate is observed exactly 3 times on the same target (unique L1 bound,
// same route session/body), then counts once as unavailable (Frozen=1/
// Unavailable=1 per domain). Both entered domains exhaust independently, so
// the active custom takes over exactly once and binds the session; the
// native 503 never returns. Real doUpstreamTiers unbound path.
func TestUnboundExhaustion503BothDomainsAllowCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, []string{"zen-key-exhaust-503-both-1"}, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 3
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0
	var anonPosts, authPosts atomic.Int32
	postStub(t, gw, "a", 0, &anonPosts, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(503, `{"error":"svc"}`), nil
	})
	postStub(t, gw, "z", 0, &authPosts, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(503, `{"error":"svc"}`), nil
	})
	ses := "ses_unbound_503_both_1"
	resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("both-domain L1-final 503 exhaustion must take over custom, got %d %+v", resp.StatusCode, eff)
	}
	if eff.ID != "cm-exhaust" {
		t.Fatalf("custom response tier model=%q want cm-exhaust", eff.ID)
	}
	if customHits.Load() != 1 {
		t.Fatalf("must hit custom once, got %d", customHits.Load())
	}
	binding, ok := gw.scheduler.fallbacks.get(ses)
	if !ok {
		t.Fatalf("must bind session-keyed fallback")
	}
	if binding.ID == "" && binding.Name == "" {
		t.Fatalf("fallback binding must carry channel identity: %+v", binding)
	}
	if _, ok := gw.scheduler.pinGet(ses, "m"); ok {
		t.Fatalf("custom takeover must not establish a session pin")
	}
	if got := postCount(&anonPosts); got != 3 {
		t.Fatalf("anonPosts=%d want 3 (initial + 2 same-target L1 retries)", got)
	}
	if got := postCount(&authPosts); got != 3 {
		t.Fatalf("authPosts=%d want 3 (initial + 2 same-target L1 retries)", got)
	}
	if attempts != 7 {
		t.Fatalf("attempts=%d want 7 (3 anon + 3 auth + 1 custom)", attempts)
	}
}

// No-active 503 keeps the native envelope: same two-domain L1-final 503
// evidence as above but without an active channel. The route must return the
// faithful protocol 503 with zero custom contact and no fallback binding.
func TestUnboundExhaustion503NoActivePreserves503(t *testing.T) {
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, []string{"zen-key-exhaust-503-noactive-1"}, nil, false)
	gw.cfg.Retry.MaxAttempts = 3
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0
	var anonPosts, authPosts atomic.Int32
	postStub(t, gw, "a", 0, &anonPosts, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(503, `{"error":"svc"}`), nil
	})
	postStub(t, gw, "z", 0, &authPosts, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(503, `{"error":"svc"}`), nil
	})
	ses := "ses_unbound_503_noactive_1"
	resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 503 {
		t.Fatalf("no-active 503 exhaustion must keep faithful 503, got %d", resp.StatusCode)
	}
	if eff.Tier == TierCustom {
		t.Fatalf("no-active must not enter custom tier: %+v", eff)
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("no-active must not bind fallback")
	}
	if _, ok := gw.scheduler.pinGet(ses, "m"); ok {
		t.Fatalf("no-active 503 must not establish a session pin")
	}
	if got := postCount(&anonPosts); got != 3 {
		t.Fatalf("anonPosts=%d want 3 (L1 observation preserved without active)", got)
	}
	if got := postCount(&authPosts); got != 3 {
		t.Fatalf("authPosts=%d want 3 (L1 observation preserved without active)", got)
	}
	if attempts != 6 {
		t.Fatalf("attempts=%d want 6 (3 anon + 3 auth, no custom)", attempts)
	}
}

// Auth single-domain mixed 429 + L1-final 503: one credential with two frozen
// proxies, one live 429 (stable, single send) and one L1-final 503 (3 sends
// under MaxAttempts=3). Each frozen candidate counts once, so the lone
// credential domain exhausts (Frozen=2/Unavailable=2) and the active custom
// takes over. The non-429 takeover must not write credential429.
func TestUnboundExhaustionAuth429And503AllowCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct", "http://127.0.0.1:8081"}, []string{"zen-key-exhaust-503-auth-mix-1"}, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 3
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0
	var z429Posts, z503Posts atomic.Int32
	postStub(t, gw, "z", 0, &z429Posts, nil, func(*http.Request) (*http.Response, error) {
		resp := responseWithBody(429, `{"error":"slow"}`)
		resp.Header.Set("Retry-After", "9")
		return resp, nil
	})
	postStub(t, gw, "z", 1, &z503Posts, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(503, `{"error":"svc"}`), nil
	})
	ses := "ses_unbound_503_auth_mix_1"
	resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("auth 429+L1-final-503 exhaustion must take over custom, got %d %+v", resp.StatusCode, eff)
	}
	if customHits.Load() != 1 {
		t.Fatalf("must hit custom once, got %d", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("must bind session-keyed fallback")
	}
	if _, ok := gw.scheduler.pinGet(ses, "m"); ok {
		t.Fatalf("custom takeover must not establish a session pin")
	}
	// Proxy behavior is pinned to the pool index, so counts hold regardless
	// of the frozen walk order: 429 is stable (1 send), 503 observes L1 (3).
	if got := postCount(&z429Posts); got != 1 {
		t.Fatalf("z429Posts=%d want 1 (live 429 never observes same-target)", got)
	}
	if got := postCount(&z503Posts); got != 3 {
		t.Fatalf("z503Posts=%d want 3 (initial + 2 same-target L1 retries)", got)
	}
	if attempts != 5 {
		t.Fatalf("attempts=%d want 5 (1x429 + 3x503 + 1 custom)", attempts)
	}
	cred := gw.authCreds[0]
	if _, _, ok := gw.scheduler.credential429CooldownStatus(cred.id); ok {
		t.Fatalf("mixed 429+503 custom takeover must not write credential429 (partial live-429 only)")
	}
}

// Partial 503 is not enough: anonymous reaches L1-final 503 (3 sends) but the
// authenticated lane ends with an ordinary 422 that never counts as
// unavailable. The auth domain stays partial (Frozen=1/Unavailable=0), so the
// single observed 503 must never trigger custom directly; the route keeps the
// faithful 422 with no fallback binding. Real doUpstreamTiers unbound path.
func TestUnboundExhaustion503PartialOrdinaryNoCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t, []string{"direct"}, []string{"direct"}, []string{"zen-key-exhaust-503-partial-1"}, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 3
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0
	var anonPosts, authPosts atomic.Int32
	postStub(t, gw, "a", 0, &anonPosts, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(503, `{"error":"svc"}`), nil
	})
	postStub(t, gw, "z", 0, &authPosts, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(422, `{"error":"unprocessable"}`), nil
	})
	ses := "ses_unbound_503_partial_1"
	resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 422 {
		t.Fatalf("partial 503 + ordinary terminal must keep faithful 422, got %d", resp.StatusCode)
	}
	if eff.Tier == TierCustom || eff.Tier != TierZen {
		t.Fatalf("must keep native zen tier, got %+v", eff)
	}
	if customHits.Load() != 0 {
		t.Fatalf("partial exhaustion must not hit custom (hits=%d)", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("partial exhaustion must not bind fallback")
	}
	if _, ok := gw.scheduler.pinGet(ses, "m"); ok {
		t.Fatalf("non-2xx partial must not establish a session pin")
	}
	if got := postCount(&anonPosts); got != 3 {
		t.Fatalf("anonPosts=%d want 3 (L1-final 503 still observed once per candidate)", got)
	}
	if got := postCount(&authPosts); got != 1 {
		t.Fatalf("authPosts=%d want 1 (ordinary 422 single send)", got)
	}
	if attempts != 4 {
		t.Fatalf("attempts=%d want 4 (3 anon + 1 auth, no custom)", attempts)
	}
}
