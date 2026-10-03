package app

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Optimal bad-node recovery: stalled first node (headers, no deliverable)
// hits the bounded candidate attempt_timeout while the parent stays live,
// cools only its target with stream_startup_timeout, observes L1, then walks
// the next frozen proxy and succeeds on the same pinned session with
// identical wire session/body.
func TestOptimalStalledNodeAnonWalkSameSession(t *testing.T) {
	var customHits atomic.Int32
	gw := pinnedAnonTwoProxyGateway(t, &customHits, false)
	gw.cfg.Retry.MaxAttempts = 3
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0
	gw.cfg.Retry.AttemptTimeoutSeconds = 1
	gw.cfg.Retry.TimeoutSeconds = 10
	ses := "ses_opt_stall_anon_1"
	pool := gw.pools["a"]
	gw.bindSessionPin(ses, "m", TierZen, anonymousSchedulerCredentialID, "a", pool.items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(pool, pin.CredID, pin.ProxyRaw)
	pinnedIdx := poolIndexByRaw(gw, "a", ordered[0].name)
	otherIdx := poolIndexByRaw(gw, "a", ordered[1].name)
	var pinnedCalls, otherCalls atomic.Int32
	postStub(t, gw, "a", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		return sseStallResponse(), nil
	})
	postStub(t, gw, "a", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(chatTextSSE("hi") + chatDoneSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := requestIDs{Session: ses, Request: "req-stall-anon", Project: "prj-test"}
	route := anonAuthRoute()
	route.ID = "m"
	start := time.Now()
	resp, eff, attempts, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0, pinnedAnonConsumeExtra())
	elapsed := time.Since(start)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("stalled anon must walk to 200, err=%v resp=%v eff=%v", err, resp, eff)
	}
	drainResp(resp)
	if eff.Tier == TierCustom {
		t.Fatalf("native walk success must not take custom")
	}
	// Bounded: 1s attempt timeout + L1 (3 sends) + walk, well under 10s parent.
	if elapsed > 9*time.Second {
		t.Fatalf("stall must be bounded by attempt_timeout, elapsed=%v", elapsed)
	}
	if got := postCount(&pinnedCalls); got != 3 {
		t.Fatalf("pinned L1=%d want 3", got)
	}
	if got := postCount(&otherCalls); got != 1 {
		t.Fatalf("other=%d want 1", got)
	}
	if attempts != 4 {
		t.Fatalf("attempts=%d want 4", attempts)
	}
	ident := targetIdentity(TierZen, pin.CredID, "a", ordered[0].name, "m")
	if until, _, ok := gw.scheduler.targetCooldownStatus(ident); !ok || until <= time.Now().UnixNano() {
		t.Fatalf("stalled target must be cooled")
	}
	// Diagnosis must be the bounded timeout, never budget/cancel.
	found := false
	for _, a := range gw.monitor.Snapshot().Upstream.Recent {
		if a.FailureReason == FailureReasonStreamStartupTimeout && a.FailureStage == FailureStageStreamStartup {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("must record stream_startup_timeout")
	}
	after, _ := gw.scheduler.pinGet(ses, "m")
	if after.ProxyRaw != ordered[1].name {
		t.Fatalf("pin must move to alternate %q got %q", ordered[1].name, after.ProxyRaw)
	}
}

func TestOptimalStalledNodeAuthWalkSameSession(t *testing.T) {
	gw := customTakeoverTestGateway(t, nil, false)
	gw.cfg.Retry.MaxAttempts = 3
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0
	gw.cfg.Retry.AttemptTimeoutSeconds = 1
	gw.cfg.Retry.TimeoutSeconds = 10
	cred := gw.authCreds[0]
	ses := "ses_opt_stall_auth_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	pinnedIdx := poolIndexByRaw(gw, "z", ordered[0].name)
	otherIdx := poolIndexByRaw(gw, "z", ordered[1].name)
	var pinnedCalls, otherCalls atomic.Int32
	postStub(t, gw, "z", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		return sseStallResponse(), nil
	})
	postStub(t, gw, "z", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(chatTextSSE("hi") + chatDoneSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := requestIDs{Session: ses, Request: "req-stall-auth", Project: "prj-test"}
	route := authOnlyRoute()
	route.ID = "m"
	resp, eff, _, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("stalled auth must walk to 200, err=%v resp=%v eff=%v", err, resp, eff)
	}
	drainResp(resp)
	if eff.Tier == TierCustom {
		t.Fatalf("native walk must not take custom")
	}
	if got := postCount(&pinnedCalls); got != 3 {
		t.Fatalf("pinned L1=%d want 3", got)
	}
	if got := postCount(&otherCalls); got != 1 {
		t.Fatalf("other=%d want 1", got)
	}
	after, _ := gw.scheduler.pinGet(ses, "m")
	if after.ProxyRaw != ordered[1].name {
		t.Fatalf("auth pin must move to alternate")
	}
}

// Single eligible stall with active custom takes over custom (no Consumed
// prerequisite); without active custom the native timeout envelope is kept.
func TestOptimalSingleStallTakesCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 2
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0
	gw.cfg.Retry.AttemptTimeoutSeconds = 1
	gw.cfg.Retry.TimeoutSeconds = 10
	cred := gw.authCreds[0]
	// Pre-cool alternate so frozen eligible is single.
	tmpSes := "ses_opt_single_tmp"
	gw.bindSessionPin(tmpSes, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	tmpPin, _ := gw.scheduler.pinGet(tmpSes, "m")
	orderedTmp := affinityProxyOrder(gw.pools["z"], tmpPin.CredID, tmpPin.ProxyRaw)
	if len(orderedTmp) == 2 {
		gw.scheduler.noteProxy429Failure(TierZen, "z", orderedTmp[1].name, AttemptClassRateLimited, 429, time.Minute, time.Now().UnixNano())
	}
	ses := "ses_opt_single_stall_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", orderedTmp[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", orderedTmp[0].name), nil, nil, func(*http.Request) (*http.Response, error) {
		return sseStallResponse(), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	resp, eff, _, err := gw.doUpstreamTiers(ctx, authOnlyRoute(), streamBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("single stall must take custom 200, err=%v resp=%v eff=%v", err, resp, eff)
	}
	drainResp(resp)
	if customHits.Load() != 1 {
		t.Fatalf("must hit custom once")
	}
}

// Long successful tail past attempt_timeout survives: commit revokes the
// candidate timer and (for client streams) the overall startup budget.
func TestOptimalLongTailSurvivesAttemptBudget(t *testing.T) {
	gw := customTakeoverTestGateway(t, nil, false)
	gw.cfg.Retry.AttemptTimeoutSeconds = 1
	gw.cfg.Retry.TimeoutSeconds = 10
	gw.cfg.Retry.MaxAttempts = 3
	cred := gw.authCreds[0]
	ses := "ses_opt_tail_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	idx := poolIndexByRaw(gw, "z", pin.ProxyRaw)
	// First frame immediately (commit within budget), tail continues 2s past
	// the 1s candidate budget.
	postStub(t, gw, "z", idx, nil, nil, func(*http.Request) (*http.Response, error) {
		pr, pw := io.Pipe()
		go func() {
			_, _ = pw.Write([]byte(chatTextSSE("hi")))
			time.Sleep(2100 * time.Millisecond)
			_, _ = pw.Write([]byte(chatTextSSE("tail") + chatDoneSSE()))
			_ = pw.Close()
		}()
		h := make(http.Header)
		h.Set("Content-Type", "text/event-stream")
		return &http.Response{StatusCode: 200, Header: h, Body: pr}, nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	resp, _, _, err := gw.doUpstreamTiers(ctx, authOnlyRoute(), streamBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("tail must succeed, err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || !strings.Contains(string(body), "tail") {
		t.Fatalf("tail must survive past attempt budget, err=%v body=%q", err, string(body))
	}
}

// Parent cancel/ultimate deadline wins: zero state writes, no later sends,
// no custom, never a timeout cooldown.
func TestOptimalCancelWinsNoStateNoFallback(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	gw.cfg.Retry.AttemptTimeoutSeconds = 1
	gw.cfg.Retry.TimeoutSeconds = 10
	cred := gw.authCreds[0]
	ses := "ses_opt_cancel_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	idx := poolIndexByRaw(gw, "z", pin.ProxyRaw)
	postStub(t, gw, "z", idx, nil, nil, func(*http.Request) (*http.Response, error) {
		return sseStallResponse(), nil
	})
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true}))
	cancel()
	_, _, _, err := gw.doUpstreamTiers(ctx, authOnlyRoute(), streamBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err == nil {
		t.Fatalf("cancelled must return ctx err")
	}
	if customHits.Load() != 0 {
		t.Fatalf("cancel must not hit custom")
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("cancel must not bind fallback")
	}
	ident := targetIdentity(TierZen, pin.CredID, "z", pin.ProxyRaw, "m")
	if until, _, ok := gw.scheduler.targetCooldownStatus(ident); ok && until > time.Now().UnixNano() {
		t.Fatalf("cancel must not cool target")
	}
}

// Anonymous non-stream internal SSE requires the same bounded startup gate
// before pin/success; incomplete responses never count as success and the
// full-request deadline stays armed (not revoked on first event).
func TestOptimalNonstreamInternalSSEBounded(t *testing.T) {
	gw := pinnedAnonTwoProxyGateway(t, nil, false)
	gw.cfg.Retry.MaxAttempts = 2
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0
	gw.cfg.Retry.AttemptTimeoutSeconds = 1
	gw.cfg.Retry.TimeoutSeconds = 10
	ses := "ses_opt_nonstream_sse_1"
	pool := gw.pools["a"]
	gw.bindSessionPin(ses, "m", TierZen, anonymousSchedulerCredentialID, "a", pool.items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(pool, pin.CredID, pin.ProxyRaw)
	pinnedIdx := poolIndexByRaw(gw, "a", ordered[0].name)
	otherIdx := poolIndexByRaw(gw, "a", ordered[1].name)
	var pinnedCalls, otherCalls atomic.Int32
	postStub(t, gw, "a", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		return sseStallResponse(), nil
	})
	postStub(t, gw, "a", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(chatTextSSE("hi") + chatDoneSSE()), nil
	})
	// Non-stream client (Stream false) with native internal SSE upstream.
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: false})
	ids := requestIDs{Session: ses, Request: "req-nonstream-sse", Project: "prj-test"}
	route := anonAuthRoute()
	route.ID = "m"
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, routeBodies(), ids, 0, pinnedAnonConsumeExtra())
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("nonstream SSE stall must walk to 200, err=%v resp=%v", err, resp)
	}
	// The gated body must still carry the deliverable event (not an
	// incomplete success); collapse-equivalent read proves it.
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	drainResp(resp)
	if !strings.Contains(string(body), "hi") {
		t.Fatalf("nonstream SSE must deliver committed event, body=%q", string(body))
	}
	if got := postCount(&pinnedCalls); got != 2 {
		t.Fatalf("pinned L1=%d want 2", got)
	}
	if got := postCount(&otherCalls); got != 1 {
		t.Fatalf("other=%d want 1", got)
	}
}

// 401 is credential-global: no same-credential resend, binding unavailable by
// credential cause; with active custom it takes over, without it stays 401.
func TestOptimal401CredentialGlobalNoResend(t *testing.T) {
	var customHits atomic.Int32
	gw := customTakeoverTestGateway(t, &customHits, true)
	cred := gw.authCreds[0]
	ses := "ses_opt_401_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	var firstHits, secondHits atomic.Int32
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), &firstHits, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(401, `{"error":"unauthorized"}`), nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), &secondHits, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("401 must take custom, err=%v resp=%v eff=%v", err, resp, eff)
	}
	drainResp(resp)
	if postCount(&firstHits) != 1 || postCount(&secondHits) != 0 {
		t.Fatalf("401 must not resend same credential: first=%d second=%d", postCount(&firstHits), postCount(&secondHits))
	}
}
