package app

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Unit coverage for the structured L1 observation loop: stop reasons only,
// no recovery policy. The loop owns attempts/interval/context/drain/execute.
func TestObserveSameTargetTransientStops(t *testing.T) {
	newGW := func(t *testing.T) *Gateway {
		t.Helper()
		cfg := testGatewayConfig(
			map[string][]string{"z": {"direct"}},
			ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
		)
		cfg.Anonymous = false
		cfg.Keys = []string{"zen-key-aaaaa"}
		cfg.Retry.TransientMaxAttempts = 3
		cfg.Retry.TransientRetryIntervalSeconds = 0
		norm, err := NormalizeConfig("config.json", cfg)
		if err != nil {
			t.Fatal(err)
		}
		gw, err := NewGateway(norm, discardGatewayLogger(), NewMonitor())
		if err != nil {
			t.Fatal(err)
		}
		return gw
	}

	t.Run("stable2xx", func(t *testing.T) {
		gw := newGW(t)
		attempts := 1
		var calls atomic.Int32
		initial := attemptOutcome{Resp: responseWithBody(200, `{"ok":true}`)}
		loopRes := gw.observeSameTargetTransient(context.Background(), initial, TierZen, ProtocolChat, &attempts, 0, 3, 0, nil, func(monitorAttempt int) attemptOutcome {
			calls.Add(1)
			return attemptOutcome{Resp: responseWithBody(200, `{"ok":true}`)}
		})
		final, stop := loopRes.Final, loopRes.Stop
		if stop != transientStopStable {
			t.Fatalf("stop=%v want stable", stop)
		}
		if calls.Load() != 0 {
			t.Fatalf("calls=%d want 0 (stable needs no observation)", calls.Load())
		}
		if attempts != 1 {
			t.Fatalf("attempts=%d want 1", attempts)
		}
		if final.Resp == nil || final.Resp.StatusCode != 200 {
			t.Fatalf("final must be initial 200")
		}
		drainResp(final.Resp)
	})

	t.Run("stableBuildErr", func(t *testing.T) {
		gw := newGW(t)
		attempts := 1
		initial := attemptOutcome{BuildErr: errors.New("bad request build")}
		loopRes := gw.observeSameTargetTransient(context.Background(), initial, TierZen, ProtocolChat, &attempts, 0, 3, 0, nil, func(monitorAttempt int) attemptOutcome {
			t.Fatalf("must not execute on BuildErr")
			return attemptOutcome{}
		})
		final, stop := loopRes.Final, loopRes.Stop
		if stop != transientStopStable {
			t.Fatalf("stop=%v want stable", stop)
		}
		if final.BuildErr == nil {
			t.Fatalf("BuildErr must pass through unchanged")
		}
	})

	t.Run("stable429", func(t *testing.T) {
		gw := newGW(t)
		attempts := 1
		initial := attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`)}
		var calls atomic.Int32
		loopRes := gw.observeSameTargetTransient(context.Background(), initial, TierZen, ProtocolChat, &attempts, 0, 5, 0, nil, func(monitorAttempt int) attemptOutcome {
			calls.Add(1)
			return attemptOutcome{Resp: responseWithBody(429, `{"error":"throttled"}`)}
		})
		final, stop := loopRes.Final, loopRes.Stop
		if stop != transientStopStable {
			t.Fatalf("stop=%v want stable (429 is caller policy, not observation)", stop)
		}
		if calls.Load() != 1 {
			t.Fatalf("calls=%d want 1", calls.Load())
		}
		if attempts != 2 {
			t.Fatalf("attempts=%d want 2", attempts)
		}
		if final.Resp == nil || final.Resp.StatusCode != 429 {
			t.Fatalf("final must be 429")
		}
		drainResp(final.Resp)
	})

	t.Run("observationLimit", func(t *testing.T) {
		gw := newGW(t)
		attempts := 1
		initial := attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`)}
		drainResp(initial.Resp)
		initial = attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`)}
		loopRes := gw.observeSameTargetTransient(context.Background(), initial, TierZen, ProtocolChat, &attempts, 0, 2, 0, nil, func(monitorAttempt int) attemptOutcome {
			if monitorAttempt != 2 {
				t.Fatalf("monitorAttempt=%d want 2", monitorAttempt)
			}
			return attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`)}
		})
		final, stop := loopRes.Final, loopRes.Stop
		if stop != transientStopObservationLimit {
			t.Fatalf("stop=%v want observation-limit", stop)
		}
		if attempts != 2 {
			t.Fatalf("attempts=%d want 2 (initial + 1 observation)", attempts)
		}
		if final.Resp == nil || final.Resp.StatusCode != 503 {
			t.Fatalf("final must be 503")
		}
		drainResp(final.Resp)
	})

	t.Run("budget", func(t *testing.T) {
		gw := newGW(t)
		attempts := 1
		initial := attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`)}
		loopRes := gw.observeSameTargetTransient(context.Background(), initial, TierZen, ProtocolChat, &attempts, 0, 5, 0, func() bool { return false }, func(monitorAttempt int) attemptOutcome {
			t.Fatalf("must not execute when budget exhausted")
			return attemptOutcome{}
		})
		final, stop := loopRes.Final, loopRes.Stop
		if stop != transientStopBudget {
			t.Fatalf("stop=%v want budget", stop)
		}
		if attempts != 1 {
			t.Fatalf("attempts=%d want 1 (no observation on budget stop)", attempts)
		}
		if final.Resp == nil || final.Resp.StatusCode != 503 {
			t.Fatalf("final must be initial 503 undrained")
		}
		drainResp(final.Resp)
	})

	t.Run("budgetBeforeContext", func(t *testing.T) {
		gw := newGW(t)
		attempts := 1
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		initial := attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`)}
		loopRes := gw.observeSameTargetTransient(ctx, initial, TierZen, ProtocolChat, &attempts, 0, 5, 0, func() bool { return false }, func(monitorAttempt int) attemptOutcome {
			t.Fatalf("must not execute when budget exhausted even with cancelled context")
			return attemptOutcome{}
		})
		_, stop := loopRes.Final, loopRes.Stop
		if stop != transientStopBudget {
			t.Fatalf("stop=%v want budget (budget precedes context when provided)", stop)
		}
		if attempts != 1 {
			t.Fatalf("attempts=%d want 1", attempts)
		}
		drainResp(loopRes.Final.Resp)
	})

	t.Run("contextPreCancel", func(t *testing.T) {
		gw := newGW(t)
		attempts := 1
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		initial := attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`)}
		loopRes := gw.observeSameTargetTransient(ctx, initial, TierZen, ProtocolChat, &attempts, 0, 3, 0, nil, func(monitorAttempt int) attemptOutcome {
			t.Fatalf("must not execute on cancelled context")
			return attemptOutcome{}
		})
		final, stop := loopRes.Final, loopRes.Stop
		if stop != transientStopContext {
			t.Fatalf("stop=%v want context", stop)
		}
		if final.Resp == nil || final.Resp.StatusCode != 503 {
			t.Fatalf("pre-cancel must return initial transient undrained")
		}
		drainResp(final.Resp)
	})

	t.Run("contextSleepInterrupt", func(t *testing.T) {
		gw := newGW(t)
		attempts := 1
		base := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
		ctx, cancel := context.WithTimeout(base, 30*time.Millisecond)
		defer cancel()
		initial := attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`)}
		start := time.Now()
		loopRes := gw.observeSameTargetTransient(ctx, initial, TierZen, ProtocolChat, &attempts, 0, 5, 5*time.Second, nil, func(monitorAttempt int) attemptOutcome {
			t.Fatalf("sleep interrupt must not execute")
			return attemptOutcome{}
		})
		final, stop := loopRes.Final, loopRes.Stop
		if stop != transientStopContext {
			t.Fatalf("stop=%v want context", stop)
		}
		if attempts != 1 {
			t.Fatalf("attempts=%d want 1 (interrupted before next send)", attempts)
		}
		if final.Resp != nil {
			t.Fatalf("sleep interrupt must not return drained transient body")
		}
		if final.Err == nil || (!errors.Is(final.Err, context.DeadlineExceeded) && !errors.Is(final.Err, context.Canceled)) {
			t.Fatalf("final err=%v want deadline/canceled", final.Err)
		}
		if time.Since(start) > 2*time.Second {
			t.Fatalf("sleep interrupt did not respect context")
		}
	})

	t.Run("streamSentinelPassthrough", func(t *testing.T) {
		gw := newGW(t)
		attempts := 1
		sentinel := errors.New("upstream stream startup failure")
		initial := attemptOutcome{Resp: nil, Err: sentinel}
		loopRes := gw.observeSameTargetTransient(context.Background(), initial, TierZen, ProtocolChat, &attempts, 0, 1, 0, nil, func(monitorAttempt int) attemptOutcome {
			t.Fatalf("maxTransient=1 must not execute")
			return attemptOutcome{}
		})
		final, stop := loopRes.Final, loopRes.Stop
		if stop != transientStopObservationLimit {
			t.Fatalf("stop=%v want observation-limit", stop)
		}
		if !isStreamStartupFailureErr(final.Err) {
			t.Fatalf("sentinel must pass through unchanged, got %v", final.Err)
		}
		if final.Resp != nil {
			t.Fatalf("sentinel must keep nil resp")
		}
	})

	t.Run("historyPreservesInitial", func(t *testing.T) {
		gw := newGW(t)
		attempts := 1
		initialResp := responseWithBody(503, `{"error":"initial"}`)
		initial := attemptOutcome{Resp: initialResp, Err: nil}
		loopRes := gw.observeSameTargetTransient(context.Background(), initial, TierZen, ProtocolChat, &attempts, 0, 5, 0, func() bool { return false }, func(monitorAttempt int) attemptOutcome {
			t.Fatalf("must not execute on budget stop")
			return attemptOutcome{}
		})
		if loopRes.InitialResp != initialResp {
			t.Fatalf("InitialResp must preserve initial pointer")
		}
		if loopRes.InitialErr != nil {
			t.Fatalf("InitialErr=%v want nil", loopRes.InitialErr)
		}
		if loopRes.Stop != transientStopBudget {
			t.Fatalf("stop=%v want budget", loopRes.Stop)
		}
		drainResp(loopRes.Final.Resp)
	})
}

// Integration: pinnedAuth ordinary-budget stop vs observation-limit transport
// behavior, and 5xx never moves. Stream sentinel pinned 502 is covered by
// TestPinnedAuthStreamStartupFailureNoWalkNoFallback; unbound sentinel retry
// by TestUnboundAnonymousStreamStartupSameTargetRetryBinds.
func TestPinnedAuthBudgetVsLimitTransport(t *testing.T) {
	setup := func(t *testing.T, session string, maxAttempts, transientMax int) (*Gateway, int, int) {
		t.Helper()
		cfg := testGatewayConfig(
			map[string][]string{"a": {"direct"}, "z": {"direct", "http://127.0.0.1:8081"}},
			ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
		)
		cfg.Anonymous = false
		cfg.Keys = []string{"zen-key-aaaaa"}
		cfg.Retry.MaxAttempts = maxAttempts
		cfg.Retry.TransientMaxAttempts = transientMax
		cfg.Retry.TransientRetryIntervalSeconds = 0
		norm, err := NormalizeConfig("config.json", cfg)
		if err != nil {
			t.Fatal(err)
		}
		gw, err := NewGateway(norm, discardGatewayLogger(), NewMonitor())
		if err != nil {
			t.Fatal(err)
		}
		cred := gw.authCreds[0]
		raw := gw.pools[gw.authPoolName()].items[0].name
		gw.bindSessionPin(session, "m", TierZen, cred.id, gw.authPoolName(), raw, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
		pin, ok := gw.scheduler.pinGet(session, "m")
		if !ok {
			t.Fatalf("must bind pin")
		}
		ordered := affinityProxyOrder(gw.pools[gw.authPoolName()], pin.CredID, pin.ProxyRaw)
		pinnedIdx := poolIndexByRaw(gw, gw.authPoolName(), ordered[0].name)
		otherIdx := poolIndexByRaw(gw, gw.authPoolName(), ordered[1].name)
		return gw, pinnedIdx, otherIdx
	}

	t.Run("budgetExhaustedTransportNoWalk", func(t *testing.T) {
		ids := pinIDs("ses_budget_vs_limit_TestPinnedAuthBudgetVsLimitTransport/budgetExhaustedTransportNoWalk", "req-budget")
		gw, pinnedIdx, otherIdx := setup(t, ids.Session, 1, 3)
		var pinnedCalls, otherCalls atomic.Int32
		postStub(t, gw, gw.authPoolName(), pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
			return nil, errors.New("dial timeout")
		})
		postStub(t, gw, gw.authPoolName(), otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
		resp, _, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), ids, 0)
		if err != nil {
			t.Fatalf("budget transport must return 502 response, err=%v", err)
		}
		if resp == nil || resp.StatusCode != 502 {
			t.Fatalf("want 502, got %v", resp)
		}
		drainResp(resp)
		if postCount(&pinnedCalls) != 1 {
			t.Fatalf("pinned=%d want 1 (budget blocks retry and walk)", postCount(&pinnedCalls))
		}
		if postCount(&otherCalls) != 0 {
			t.Fatalf("other=%d want 0 (budget stop must not cross proxy)", postCount(&otherCalls))
		}
		if attempts != 1 {
			t.Fatalf("attempts=%d want 1", attempts)
		}
	})

	t.Run("limitTransportWalksWhenBudgetRemains", func(t *testing.T) {
		ids := pinIDs("ses_budget_vs_limit_TestPinnedAuthBudgetVsLimitTransport/limitTransportWalksWhenBudgetRemains", "req-limit-walk")
		gw, pinnedIdx, otherIdx := setup(t, ids.Session, 5, 1)
		var pinnedCalls, otherCalls atomic.Int32
		postStub(t, gw, gw.authPoolName(), pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
			return nil, errors.New("dial timeout")
		})
		postStub(t, gw, gw.authPoolName(), otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
		resp, _, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), ids, 0)
		if err != nil || resp == nil || resp.StatusCode != 200 {
			t.Fatalf("limit transport with budget must walk to other 200, err=%v resp=%v", err, resp)
		}
		drainResp(resp)
		if postCount(&pinnedCalls) != 1 {
			t.Fatalf("pinned=%d want 1", postCount(&pinnedCalls))
		}
		if postCount(&otherCalls) != 1 {
			t.Fatalf("other=%d want 1 (observation-limit transport walks when budget remains)", postCount(&otherCalls))
		}
		if attempts != 2 {
			t.Fatalf("attempts=%d want 2", attempts)
		}
		pinAfter, _ := gw.scheduler.pinGet(ids.Session, "m")
		otherRaw := gw.pools[gw.authPoolName()].items[otherIdx].name
		if pinAfter.ProxyRaw != otherRaw {
			t.Fatalf("pin must move to other on transport walk, got %q want %q", pinAfter.ProxyRaw, otherRaw)
		}
	})

	t.Run("limit5xxNeverMoves", func(t *testing.T) {
		ids := pinIDs("ses_budget_vs_limit_TestPinnedAuthBudgetVsLimitTransport/limit5xxNeverMoves", "req-limit-5xx")
		gw, pinnedIdx, otherIdx := setup(t, ids.Session, 5, 1)
		var pinnedCalls, otherCalls atomic.Int32
		postStub(t, gw, gw.authPoolName(), pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(503, `{"error":"svc"}`), nil
		})
		postStub(t, gw, gw.authPoolName(), otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
		pinBefore, _ := gw.scheduler.pinGet(ids.Session, "m")
		resp, _, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), ids, 0)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if resp == nil || resp.StatusCode != 503 {
			t.Fatalf("want 503, got %v", resp)
		}
		drainResp(resp)
		if postCount(&pinnedCalls) != 1 {
			t.Fatalf("pinned=%d want 1", postCount(&pinnedCalls))
		}
		if postCount(&otherCalls) != 0 {
			t.Fatalf("other=%d want 0 (5xx never moves even at observation-limit)", postCount(&otherCalls))
		}
		if attempts != 1 {
			t.Fatalf("attempts=%d want 1", attempts)
		}
		pinAfter, _ := gw.scheduler.pinGet(ids.Session, "m")
		if pinAfter != pinBefore {
			t.Fatalf("pin must not drift on 5xx: %+v -> %+v", pinBefore, pinAfter)
		}
	})
}

// Audit fix 1: pinnedAuth mixed initial 503 -> retry transport-nil at
// observation limit must return the stale initial 503 (HEAD behavior), not
// nil + transport error. Proves behavior via status/err, not internals.
func TestPinnedAuthMixed503TransportStaleInitial(t *testing.T) {
	newGW := func(t *testing.T, session string) (*Gateway, int, int) {
		t.Helper()
		cfg := testGatewayConfig(
			map[string][]string{"a": {"direct"}, "z": {"direct", "http://127.0.0.1:8081"}},
			ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
		)
		cfg.Anonymous = false
		cfg.Keys = []string{"zen-key-aaaaa"}
		cfg.Retry.MaxAttempts = 5
		cfg.Retry.TransientMaxAttempts = 2
		cfg.Retry.TransientRetryIntervalSeconds = 0
		norm, err := NormalizeConfig("config.json", cfg)
		if err != nil {
			t.Fatal(err)
		}
		gw, err := NewGateway(norm, discardGatewayLogger(), NewMonitor())
		if err != nil {
			t.Fatal(err)
		}
		cred := gw.authCreds[0]
		raw := gw.pools[gw.authPoolName()].items[0].name
		gw.bindSessionPin(session, "m", TierZen, cred.id, gw.authPoolName(), raw, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
		pin, ok := gw.scheduler.pinGet(session, "m")
		if !ok {
			t.Fatalf("must bind pin")
		}
		ordered := affinityProxyOrder(gw.pools[gw.authPoolName()], pin.CredID, pin.ProxyRaw)
		pinnedIdx := poolIndexByRaw(gw, gw.authPoolName(), ordered[0].name)
		otherIdx := poolIndexByRaw(gw, gw.authPoolName(), ordered[1].name)
		return gw, pinnedIdx, otherIdx
	}
	ids := pinIDs("ses_pinned_mixed_503_transport_stale", "req-mixed-stale")
	gw, pinnedIdx, otherIdx := newGW(t, ids.Session)
	var calls atomic.Int32
	postStub(t, gw, gw.authPoolName(), pinnedIdx, &calls, nil, func(*http.Request) (*http.Response, error) {
		if calls.Load() == 1 {
			return responseWithBody(503, `{"error":"svc"}`), nil
		}
		return nil, errors.New("dial timeout")
	})
	var otherCalls atomic.Int32
	postStub(t, gw, gw.authPoolName(), otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	resp, _, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), ids, 0)
	if err != nil {
		t.Fatalf("stale initial must return 503 response with nil err, got err=%v", err)
	}
	if resp == nil || resp.StatusCode != 503 {
		t.Fatalf("want stale initial 503, got %v err=%v", resp, err)
	}
	drainResp(resp)
	if postCount(&calls) != 2 {
		t.Fatalf("pinned calls=%d want 2 (initial 503 + 1 transport retry)", postCount(&calls))
	}
	if postCount(&otherCalls) != 0 {
		t.Fatalf("other=%d want 0 (stale 503 never walks)", postCount(&otherCalls))
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
}

// Audit fix 2: pinnedAuth initial stream-startup sentinel -> later 503 must
// stay poisoned to local502 (HEAD isStreamStartup(cur)||isStreamStartup(send)).
// No last-wins, no de-poisoning. Proves via 502 envelope, not internals.
func TestPinnedAuthSentinelPoisoning503(t *testing.T) {
	cfg := testGatewayConfig(
		map[string][]string{"z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 2
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 5
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(norm, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	ses := "ses_pinned_sentinel_poison_503"
	cred := gw.authCreds[0]
	raw := gw.pools["z"].items[0].name
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", raw, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, ok := gw.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must bind pin")
	}
	pinnedIdx := poolIndexByRaw(gw, "z", pin.ProxyRaw)
	otherIdx := -1
	for i, p := range gw.pools["z"].items {
		if p != nil && p.name != pin.ProxyRaw {
			otherIdx = i
			break
		}
	}
	if otherIdx < 0 {
		t.Fatalf("other proxy missing")
	}
	var pinnedCalls, otherCalls atomic.Int32
	postStub(t, gw, "z", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		if pinnedCalls.Load() == 1 {
			return sseResponse(""), nil
		}
		return responseWithBody(503, `{"error":"svc"}`), nil
	})
	postStub(t, gw, "z", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(chatTextSSE("should-not") + chatDoneSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := requestIDs{Session: ses, Request: "req-sentinel-poison", Project: "prj-test"}
	route := authOnlyRoute()
	route.ID = "m"
	bodies := map[Tier][]byte{TierZen: []byte(`{"model":"m","stream":true}`)}
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, bodies, ids, 0, upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}})
	if err != nil {
		t.Fatalf("poisoned sentinel must return 502 response, err=%v", err)
	}
	if resp == nil || resp.StatusCode != 502 {
		body := ""
		if resp != nil && resp.Body != nil {
			b, _ := io.ReadAll(resp.Body)
			body = strings.TrimSpace(string(b))
		}
		t.Fatalf("status=%d want 502 (initial sentinel poisons later 503), body=%q", resp.StatusCode, body)
	}
	drainResp(resp)
	if got := postCount(&pinnedCalls); got != 2 {
		t.Fatalf("pinnedCalls=%d want 2 (sentinel + 503 observation)", got)
	}
	if got := postCount(&otherCalls); got != 0 {
		t.Fatalf("otherCalls=%d want 0 (poisoned 502 never walks)", got)
	}
}

// Audit fix 4 note: pinnedAnon intermediate-cancel drain (cur!=initial) needs a
// cancel window between same-target retries, and retry transport+non-nil Resp
// is discarded by net/http Client before executeAttempt ever sees it (the
// Transport may return both, but Client.Do drops the response on error). Neither
// is naturally injectable without timing hacks or bypassing http.Client, so no
// integration test is added here. The caller preserves HEAD ownership
// defensively: pre-retry-cancel drains intermediate cur when cur!=initial, and
// retry transport+non-nil (if ever reachable via a direct executor) is drained
// before return without touching stable/final envelopes.

// Audit fix: pinnedAuth initial stream-startup sentinel -> partial 429 must
// poison to pin-local 502 immediately (HEAD gateway.go:2372 order, before the
// cur==nil continue). Full exhaustion stays before poisoning; intermediate-only
// sentinels never poison. History preserved exactly: observed429 write kept,
// final 429 plus prior last429 drained, no next-proxy/custom/credential429 on
// the poisoned partial path.
func TestPinnedAuthInitialSentinelPartial429Poisons502(t *testing.T) {
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-sentinel-429")))
	}))
	t.Cleanup(custom.Close)
	ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm-sentinel-429"}
	cfg := testGatewayConfig(
		map[string][]string{"z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 2
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 5
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(norm, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	ses := "ses_pinned_sentinel_partial429_poison"
	cred := gw.authCreds[0]
	raw := gw.pools["z"].items[0].name
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", raw, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, ok := gw.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must bind pin")
	}
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	if len(ordered) != 2 {
		t.Fatalf("ordered=%d want 2", len(ordered))
	}
	pinnedIdx := poolIndexByRaw(gw, "z", ordered[0].name)
	otherIdx := poolIndexByRaw(gw, "z", ordered[1].name)
	var pinnedCalls, otherCalls atomic.Int32
	var final429Closed atomic.Int32
	postStub(t, gw, "z", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		if pinnedCalls.Load() == 1 {
			return sseResponse(""), nil
		}
		return trackedResponse(429, `{"error":"throttled"}`, &final429Closed), nil
	})
	postStub(t, gw, "z", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(chatTextSSE("should-not") + chatDoneSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := requestIDs{Session: ses, Request: "req-sentinel-partial429", Project: "prj-test"}
	route := authOnlyRoute()
	route.ID = "m"
	bodies := map[Tier][]byte{TierZen: []byte(`{"model":"m","stream":true}`)}
	resp, eff, attempts, err := gw.doUpstreamTiers(ctx, route, bodies, ids, 0, upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}})
	if err != nil {
		t.Fatalf("poisoned partial 429 must return 502 response, err=%v", err)
	}
	if resp == nil || resp.StatusCode != 502 {
		body := ""
		if resp != nil && resp.Body != nil {
			b, _ := io.ReadAll(resp.Body)
			body = strings.TrimSpace(string(b))
		}
		t.Fatalf("status=%d want 502 (initial sentinel poisons partial 429), body=%q", resp.StatusCode, body)
	}
	drainResp(resp)
	if eff.Tier == TierCustom {
		t.Fatalf("poisoned partial 429 must not take over custom")
	}
	if got := postCount(&pinnedCalls); got != 2 {
		t.Fatalf("pinnedCalls=%d want 2 (sentinel + 429 observation)", got)
	}
	if got := postCount(&otherCalls); got != 0 {
		t.Fatalf("otherCalls=%d want 0 (poisoned 502 never walks)", got)
	}
	if customHits.Load() != 0 {
		t.Fatalf("custom hits=%d want 0 (partial poison never reaches custom)", customHits.Load())
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
	if final429Closed.Load() != 1 {
		t.Fatalf("final 429 body closed=%d want 1 (drained before 502)", final429Closed.Load())
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("partial 429 poison must not write credential429")
	}
}

// Same sentinel -> 429 sequence with a single eligible proxy is full
// exhaustion: the pre-poisoning 429 path still notes credential429 and takes
// the active custom fallback.
func TestPinnedAuthInitialSentinelFull429KeepsExhaustion(t *testing.T) {
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-sentinel-429")))
	}))
	t.Cleanup(custom.Close)
	ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm-sentinel-429"}
	cfg := testGatewayConfig(
		map[string][]string{"z": {"direct"}},
		ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 2
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 5
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(norm, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	ses := "ses_pinned_sentinel_full429_custom"
	cred := gw.authCreds[0]
	raw := gw.pools["z"].items[0].name
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", raw, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, ok := gw.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must bind pin")
	}
	pinnedIdx := poolIndexByRaw(gw, "z", pin.ProxyRaw)
	var pinnedCalls atomic.Int32
	postStub(t, gw, "z", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		if pinnedCalls.Load() == 1 {
			return sseResponse(""), nil
		}
		r := responseWithBody(429, `{"error":"throttled"}`)
		r.Header.Set("Retry-After", "9")
		return r, nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := requestIDs{Session: ses, Request: "req-sentinel-full429", Project: "prj-test"}
	route := authOnlyRoute()
	route.ID = "m"
	bodies := map[Tier][]byte{TierZen: []byte(`{"model":"m","stream":true}`)}
	resp, eff, attempts, err := gw.doUpstreamTiers(ctx, route, bodies, ids, 0, upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}})
	if err != nil {
		t.Fatalf("full exhaustion must return response, err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("single-eligible sentinel->429 must exhaust to custom 200, err=%v resp=%v eff=%+v", err, resp, eff)
	}
	drainResp(resp)
	if got := postCount(&pinnedCalls); got != 2 {
		t.Fatalf("pinnedCalls=%d want 2 (sentinel + 429 observation)", got)
	}
	if customHits.Load() != 1 {
		t.Fatalf("custom hits=%d want 1 (full exhaustion reaches custom)", customHits.Load())
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3 (sentinel + 429 + custom takeover)", attempts)
	}
	if _, status, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); !ok || status != 429 {
		t.Fatalf("full exhaustion must write credential429")
	}
}

// Intermediate-only sentinel (initial 503 -> sentinel -> 429) must not poison:
// the partial 429 continues the candidate walk to the next eligible proxy.
func TestPinnedAuthIntermediateSentinelPartial429Continues(t *testing.T) {
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-sentinel-429")))
	}))
	t.Cleanup(custom.Close)
	ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm-sentinel-429"}
	cfg := testGatewayConfig(
		map[string][]string{"z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 5
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(norm, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	ses := "ses_pinned_intermediate_sentinel_429"
	cred := gw.authCreds[0]
	raw := gw.pools["z"].items[0].name
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", raw, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, ok := gw.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must bind pin")
	}
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	if len(ordered) != 2 {
		t.Fatalf("ordered=%d want 2", len(ordered))
	}
	pinnedIdx := poolIndexByRaw(gw, "z", ordered[0].name)
	otherIdx := poolIndexByRaw(gw, "z", ordered[1].name)
	otherRaw := gw.pools["z"].items[otherIdx].name
	var pinnedCalls, otherCalls atomic.Int32
	postStub(t, gw, "z", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		switch pinnedCalls.Load() {
		case 1:
			return responseWithBody(503, `{"error":"svc"}`), nil
		case 2:
			return sseResponse(""), nil
		default:
			return responseWithBody(429, `{"error":"throttled"}`), nil
		}
	})
	postStub(t, gw, "z", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(chatTextSSE("walked") + chatDoneSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := requestIDs{Session: ses, Request: "req-intermediate-sentinel", Project: "prj-test"}
	route := authOnlyRoute()
	route.ID = "m"
	bodies := map[Tier][]byte{TierZen: []byte(`{"model":"m","stream":true}`)}
	resp, eff, attempts, err := gw.doUpstreamTiers(ctx, route, bodies, ids, 0, upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}})
	if err != nil {
		t.Fatalf("intermediate sentinel walk must return response, err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		body := ""
		if resp != nil && resp.Body != nil {
			b, _ := io.ReadAll(resp.Body)
			body = strings.TrimSpace(string(b))
		}
		t.Fatalf("status=%d want 200 (intermediate sentinel never poisons), body=%q", resp.StatusCode, body)
	}
	drainResp(resp)
	if eff.Tier == TierCustom {
		t.Fatalf("partial walk success must not take over custom")
	}
	if got := postCount(&pinnedCalls); got != 3 {
		t.Fatalf("pinnedCalls=%d want 3 (503 + sentinel + 429 observation)", got)
	}
	if got := postCount(&otherCalls); got != 1 {
		t.Fatalf("otherCalls=%d want 1 (partial 429 continues the walk)", got)
	}
	if customHits.Load() != 0 {
		t.Fatalf("custom hits=%d want 0", customHits.Load())
	}
	if attempts != 4 {
		t.Fatalf("attempts=%d want 4 (3 pinned sends + 1 walk)", attempts)
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("partial 429 walk must not write credential429")
	}
	pinAfter, _ := gw.scheduler.pinGet(ses, "m")
	if pinAfter.ProxyRaw != otherRaw {
		t.Fatalf("pin must move to other on walk, got %q want %q", pinAfter.ProxyRaw, otherRaw)
	}
}
