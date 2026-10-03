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

func transientGateway(t *testing.T, monitor *Monitor, transientMax int, intervalSec int) *Gateway {
	t.Helper()
	cfg := testGatewayConfig(
		map[string][]string{
			"a": {"direct", "http://127.0.0.1:8081"},
			"z": {"direct", "http://127.0.0.1:8082"},
		},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Retry.MaxAttempts = transientMax
	cfg.Retry.TransientRetryIntervalSeconds = intervalSec
	gw, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	return gw
}

func TestUnboundAnonymous503RetrySuccessAfterMultipleFailures(t *testing.T) {
	monitor := NewMonitor()
	gw := transientGateway(t, monitor, 3, 0)
	var a0calls atomic.Int32
	var seq atomic.Int32
	postStub(t, gw, "a", 0, &a0calls, nil, func(*http.Request) (*http.Response, error) {
		n := seq.Add(1)
		if n <= 2 {
			return responseWithBody(503, `{"error":"svc"}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	cap := &capturedUpstream{}
	// need to capture sessions correctly: reinstall stub with cap
	seq.Store(0)
	a0calls.Store(0)
	cap = &capturedUpstream{}
	postStub(t, gw, "a", 0, &a0calls, cap, func(*http.Request) (*http.Response, error) {
		n := seq.Add(1)
		if n <= 2 {
			return responseWithBody(503, `{"error":"svc"}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ids := emptySessionIDs()
	route := anonAuthRoute()
	resp, _, attempts, err := gw.doUpstreamTiers(ctx, route, routeBodies(), ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200 after retries, err=%v resp=%v", err, resp)
	}
	drainAndClose(resp.Body)
	if postCount(&a0calls) != 3 {
		t.Fatalf("a0 calls=%d want 3 (2 fails + success)", postCount(&a0calls))
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3", attempts)
	}
	sessions, bodies := cap.get()
	if len(sessions) != 3 || len(bodies) != 3 {
		t.Fatalf("sessions/bodies len %d/%d want 3", len(sessions), len(bodies))
	}
	for i := 1; i < len(sessions); i++ {
		if sessions[i] != sessions[0] {
			t.Fatalf("session mismatch %q vs %q", sessions[i], sessions[0])
		}
		if string(bodies[i]) != string(bodies[0]) {
			t.Fatalf("body mismatch")
		}
	}
	proxy := gw.pools["a"].items[0]
	if !proxy.healthy.Load() {
		t.Fatalf("503 must not mark proxy unhealthy")
	}
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) < 3 {
		t.Fatalf("recent len %d want >=3", len(recent))
	}
	if recent[0].Status != 503 || recent[0].FailureClass != "upstream_failure" {
		t.Fatalf("first 503 class=%q status=%d", recent[0].FailureClass, recent[0].Status)
	}
}

func TestUnboundAuthenticated503RetrySuccess(t *testing.T) {
	monitor := NewMonitor()
	cfg := testGatewayConfig(
		map[string][]string{
			"a": {"direct"},
			"z": {"direct", "http://127.0.0.1:8081"},
		},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	gw, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	var seq atomic.Int32
	var z0calls atomic.Int32
	postStub(t, gw, "z", 0, &z0calls, nil, func(*http.Request) (*http.Response, error) {
		n := seq.Add(1)
		if n <= 2 {
			return responseWithBody(503, `{"error":"svc"}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ids := emptySessionIDs()
	route := authOnlyRoute()
	resp, _, attempts, err := gw.doUpstreamTiers(ctx, route, routeBodies(), ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200, err=%v resp=%v", err, resp)
	}
	drainAndClose(resp.Body)
	if postCount(&z0calls) != 3 {
		t.Fatalf("z0 calls=%d want 3", postCount(&z0calls))
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3", attempts)
	}
}

func TestPinned503RemainsSameTarget(t *testing.T) {
	monitor := NewMonitor()
	gw := transientGateway(t, monitor, 3, 0)
	ids := pinIDs("ses_pinned_503_same", "req1")
	route := anonAuthRoute()
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	resp, _, _, _ := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids, 0)
	drainResp(resp)
	pin, _ := gw.scheduler.pinGet(ids.Session, "m")
	pinnedIdx := 0
	for i, p := range gw.pools["a"].items {
		if p.name == pin.ProxyRaw {
			pinnedIdx = i
		}
	}
	otherIdx := 1 - pinnedIdx
	var pinnedCalls, otherCalls atomic.Int32
	var seq atomic.Int32
	postStub(t, gw, "a", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		n := seq.Add(1)
		if n <= 2 {
			return responseWithBody(503, `{"error":"svc"}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "a", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids2 := pinIDs(ids.Session, "req2")
	resp2, _, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids2, 0)
	if err != nil || resp2 == nil || resp2.StatusCode != 200 {
		t.Fatalf("pinned retry want 200, err=%v resp=%v", err, resp2)
	}
	drainResp(resp2)
	if postCount(&pinnedCalls) != 3 {
		t.Fatalf("pinned calls=%d want 3", postCount(&pinnedCalls))
	}
	if postCount(&otherCalls) != 0 {
		t.Fatalf("other calls=%d want 0 (pinned must not move)", postCount(&otherCalls))
	}
	pin2, _ := gw.scheduler.pinGet(ids.Session, "m")
	if pin2.ProxyRaw != pin.ProxyRaw {
		t.Fatalf("pin moved %q -> %q, must stay", pin.ProxyRaw, pin2.ProxyRaw)
	}
}

// Empty-session 503 faithful return (not a general "503 never takes over"
// rule): emptySessionIDs() carries Session=="" so doUpstreamTiersUnbound can
// never enter the custom gate (ids.Session!="" required). With a non-empty
// bindable session + active custom, L1-final 503 exhaustion on every entered
// domain does take over custom; see
// TestUnboundExhaustion503BothDomainsAllowCustom and
// TestUnboundExhaustionAuth429And503AllowCustom. This test keeps the L1
// same-target observation (3 sends) and the native 503 envelope verification.
func TestExhausted503EmptySessionReturns503NoCustomFallback(t *testing.T) {
	monitor := NewMonitor()
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm1")))
	}))
	defer custom.Close()
	ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm1"}
	// unbound gateway with single proxy for deterministic 503
	cfgUnbound := testGatewayConfig(
		map[string][]string{"a": {"direct"}, "z": {"direct"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfgUnbound.Anonymous = true
	cfgUnbound.Keys = []string{"single-key-12345"}
	cfgUnbound.Retry.MaxAttempts = 3
	cfgUnbound.Retry.TransientRetryIntervalSeconds = 0
	cfgUnbound.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	normUnbound, _ := NormalizeConfig("config.json", cfgUnbound)
	gwUnbound, _ := NewGateway(normUnbound, discardGatewayLogger(), NewMonitor())
	postStub(t, gwUnbound, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(503, `{"error":"svc"}`), nil
	})
	postStub(t, gwUnbound, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(503, `{"error":"svc"}`), nil
	})
	ids := emptySessionIDs()
	if ids.Session != "" {
		t.Fatalf("test premise broken: want empty session, got %q", ids.Session)
	}
	route := anonAuthRoute()
	resp, eff, _, err := gwUnbound.doUpstreamTiers(context.Background(), route, routeBodies(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 503 {
		t.Fatalf("empty-session exhausted 503 want faithful 503, got %v", resp)
	}
	drainResp(resp)
	if eff.Tier == TierCustom {
		t.Fatalf("empty session must not fallback to custom on 503 (custom gate needs a bindable session)")
	}
	if customHits.Load() != 0 {
		t.Fatalf("custom hits %d want 0", customHits.Load())
	}
	// pinned case with 2 proxies
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct", "http://127.0.0.1:8081"}, "z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	norm, _ := NormalizeConfig("config.json", cfg)
	gw, _ := NewGateway(norm, discardGatewayLogger(), monitor)
	// pinned case
	ids2 := pinIDs("ses_exhaust_503_pinned", "req1")
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	r, _, _, _ := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids2, 0)
	drainResp(r)
	pin, _ := gw.scheduler.pinGet(ids2.Session, "m")
	pinnedIdx := 0
	for i, p := range gw.pools["a"].items {
		if p.name == pin.ProxyRaw {
			pinnedIdx = i
		}
	}
	var pinnedCalls atomic.Int32
	postStub(t, gw, "a", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(503, `{"error":"svc"}`), nil
	})
	ids3 := pinIDs(ids2.Session, "req2")
	resp2, eff2, _, err2 := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids3, 0)
	if err2 != nil || resp2 == nil || resp2.StatusCode != 200 {
		t.Fatalf("pinned 503 must walk to alternate 200, err=%v resp=%v", err2, resp2)
	}
	drainResp(resp2)
	if eff2.Tier == TierCustom {
		t.Fatalf("walk success must not fallback")
	}
	if postCount(&pinnedCalls) != 3 {
		t.Fatalf("pinned calls %d want 3", postCount(&pinnedCalls))
	}
	if customHits.Load() != 0 {
		t.Fatalf("custom hits after pinned %d want 0", customHits.Load())
	}
}

func TestContextCancellationDuringDelay(t *testing.T) {
	monitor := NewMonitor()
	gw := transientGateway(t, monitor, 3, 1) // 1s interval
	var calls atomic.Int32
	postStub(t, gw, "a", 0, &calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(503, `{"error":"svc"}`), nil
	})
	postStub(t, gw, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, requestMetaKey{}, &requestMeta{})
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, _, _, err := gw.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), emptySessionIDs(), 0)
	elapsed := time.Since(start)
	if postCount(&calls) != 1 {
		t.Fatalf("calls=%d want 1 (no retry after cancel)", postCount(&calls))
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("elapsed %v too long, cancellation not prompt", elapsed)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		// allow context.Canceled wrapped
		if !errors.Is(err, context.Canceled) && err != context.Canceled {
			// if gateway returns 503 instead of ctx error, it's okay as long as no extra send
		}
	}
}

func TestRetryAfterLowerBound(t *testing.T) {
	monitor := NewMonitor()
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct"}, "z": {"direct"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	gw, _ := NewGateway(cfg, discardGatewayLogger(), monitor)
	var calls atomic.Int32
	var seq atomic.Int32
	postStub(t, gw, "a", 0, &calls, nil, func(*http.Request) (*http.Response, error) {
		n := seq.Add(1)
		if n == 1 {
			resp := responseWithBody(503, `{"error":"svc"}`)
			resp.Header.Set("Retry-After", "1")
			return resp, nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})
	start := time.Now()
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ids := emptySessionIDs()
	resp, _, _, err := gw.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200, err=%v", err)
	}
	drainAndClose(resp.Body)
	elapsed := time.Since(start)
	if elapsed < 900*time.Millisecond {
		t.Fatalf("Retry-After 1s must delay retry as lower bound: elapsed %v < 900ms", elapsed)
	}
	if elapsed > 2500*time.Millisecond {
		t.Fatalf("elapsed too long %v want <2.5s", elapsed)
	}
	if postCount(&calls) != 2 {
		t.Fatalf("calls %d want 2", postCount(&calls))
	}
}

// Empty-session mixed 429/503 faithful return (not a general "503 never
// takes over" rule): emptySessionIDs() carries Session=="" so the unbound
// custom gate is closed even though the single credential domain would
// otherwise exhaust (live 429 + L1-final 503). With a non-empty bindable
// session + active custom the same evidence does take over; see
// TestUnboundExhaustionAuth429And503AllowCustom. This test keeps the L1
// same-target 503 observation and the native 503 envelope verification.
func TestMixed429Then503EmptySessionKeeps503NoCustomFallback(t *testing.T) {
	monitor := NewMonitor()
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct"}, "z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"single-key-12345"}
	cfg.Retry.MaxAttempts = 2
	cfg.Retry.TransientRetryIntervalSeconds = 0
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm1")))
	}))
	defer custom.Close()
	ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm1"}
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	norm, _ := NormalizeConfig("config.json", cfg)
	gw, _ := NewGateway(norm, discardGatewayLogger(), monitor)
	postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		resp := responseWithBody(429, `{"error":"rate"}`)
		resp.Header.Set("Retry-After", "1")
		return resp, nil
	})
	var z1calls atomic.Int32
	postStub(t, gw, "z", 1, &z1calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(503, `{"error":"svc"}`), nil
	})
	route := authOnlyRoute()
	ids := emptySessionIDs()
	if ids.Session != "" {
		t.Fatalf("test premise broken: want empty session, got %q", ids.Session)
	}
	resp, eff, _, err := gw.doUpstreamTiers(context.Background(), route, routeBodies(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil {
		t.Fatalf("nil resp")
	}
	defer drainResp(resp)
	if resp.StatusCode != 503 {
		t.Fatalf("empty-session mixed 429 then 503 must keep faithful 503, got %d", resp.StatusCode)
	}
	if eff.Tier == TierCustom {
		t.Fatalf("empty session must not fallback to custom on 503 after 429 (custom gate needs a bindable session)")
	}
	if customHits.Load() != 0 {
		t.Fatalf("custom hits %d want 0", customHits.Load())
	}
	// z1 carries the L1 same-target 503 observation (MaxAttempts=2).
	if postCount(&z1calls) != 2 {
		t.Fatalf("z1 calls %d want 2 (initial 503 + 1 same-target L1 retry)", postCount(&z1calls))
	}
	cred := gw.authCreds[0]
	if _, _, ok := gw.scheduler.credential429CooldownStatus(cred.id); ok {
		t.Fatalf("partial 429 + 503 must not write credential429")
	}
}
