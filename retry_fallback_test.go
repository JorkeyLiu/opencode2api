package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func authTwoProxyGateway(t *testing.T, monitor *Monitor, maxAttempts int) *Gateway {
	t.Helper()
	cfg := testGatewayConfig(
		map[string][]string{
			"a": {"direct"},
			"z": {"direct", "http://127.0.0.1:8081"},
			"g": {"direct"},
		},
		ProxyRoutingConfig{Anonymous: "a", Zen: "z", Go: "g"},
	)
	cfg.Anonymous = false
	cfg.Retry.MaxAttempts = maxAttempts
	gateway, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	return gateway
}

// postStub installs a probe-aware stub: async neutral health probes (GET)
// return 200 without counting or recording, so assertions observe only real
// upstream POST sends. postCalls counts POSTs; cap records POST sessions/bodies.
func postStub(t *testing.T, gateway *Gateway, pool string, index int, postCalls *atomic.Int32, cap *capturedUpstream, fn func(*http.Request) (*http.Response, error)) {
	t.Helper()
	stubProxy(t, gateway, pool, index, func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet {
			return responseWithBody(200, `{}`), nil
		}
		if postCalls != nil {
			postCalls.Add(1)
		}
		if cap != nil {
			cap.add(r)
		}
		return fn(r)
	})
}

func postCount(c *atomic.Int32) int {
	if c == nil {
		return 0
	}
	return int(c.Load())
}

// 1. Anonymous transport error retries once on the same target and succeeds.
func TestAnonymousTransportRetrySuccess(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	cap := &capturedUpstream{}
	var a0calls, a1calls atomic.Int32
	var calls atomic.Int32
	postStub(t, gateway, "a", 0, &a0calls, cap, func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("dial timeout")
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, &a1calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ids := emptySessionIDs()
	resp, _, attempts, err := gateway.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("status=%v want 200", resp)
	}
	drainAndClose(resp.Body)
	if postCount(&a0calls) != 2 || postCount(&a1calls) != 0 {
		t.Fatalf("same-target retry calls=%d/%d want 2/0", postCount(&a0calls), postCount(&a1calls))
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) != 2 {
		t.Fatalf("recorded=%d want 2", len(recent))
	}
	if recent[0].RequestID != ids.Request || recent[1].RequestID != ids.Request {
		t.Fatalf("request id mismatch: %+v", recent)
	}
	if recent[0].Attempt+1 != recent[1].Attempt {
		t.Fatalf("attempt numbers not continuous: %+v", recent)
	}
	sessions, bodies := cap.get()
	if len(sessions) != 2 || sessions[0] != sessions[1] {
		t.Fatalf("transient retry must keep the same route session: %q", sessions)
	}
	if len(bodies) != 2 || string(bodies[0]) != string(bodies[1]) {
		t.Fatalf("transient retry must keep the same body")
	}
	proxy := gateway.pools["a"].items[0]
	identity := targetIdentity(TierZen, anonymousSchedulerCredentialID, "a", proxy.name, "m")
	if got := gateway.scheduler.targetCoolUntil(identity); got != 0 {
		t.Fatalf("transport retry must not cool target, got %d", got)
	}
	if !proxy.healthy.Load() {
		t.Fatalf("single transport error must not mark proxy unhealthy")
	}
}

// 2. Anonymous transport retry still fails, then falls back; shared token blocks a second retry.
func TestAnonymousTransportRetryThenFallback(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	cap := &capturedUpstream{}
	var a0calls, a1calls atomic.Int32
	postStub(t, gateway, "a", 0, &a0calls, cap, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("connection reset")
	})
	postStub(t, gateway, "a", 1, &a1calls, cap, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("connection reset")
	})
	var zenCalls atomic.Int32
	postStub(t, gateway, "z", 0, &zenCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ids := emptySessionIDs()
	_, _, attempts, err := gateway.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	sessions, _ := cap.get()
	if len(sessions) != 3 {
		t.Fatalf("anon sends=%d want 3 (A,A,B)", len(sessions))
	}
	if sessions[0] != sessions[1] {
		t.Fatalf("first two sends must share the route session: %q", sessions)
	}
	if sessions[2] == sessions[0] {
		t.Fatalf("fallback target must use a different route session: %q", sessions)
	}
	if postCount(&a0calls) != 2 || postCount(&a1calls) != 1 {
		t.Fatalf("sequence must be A,A,B: %d/%d", postCount(&a0calls), postCount(&a1calls))
	}
	if attempts != 4 {
		t.Fatalf("attempts=%d want 4 (3 anon + zen)", attempts)
	}
	if postCount(&zenCalls) != 1 {
		t.Fatalf("auth must be entered after anon exhaustion: zen=%d", postCount(&zenCalls))
	}
}

// 3. Auth real-send budget: max=1 blocks retry and next candidate; max=2 A,A; max=3 A,A,B.
func TestAuthRealSendBudget(t *testing.T) {
	t.Run("max1", func(t *testing.T) {
		monitor := NewMonitor()
		gateway := authTwoProxyGateway(t, monitor, 1)
		var z0calls, z1calls atomic.Int32
		postStub(t, gateway, "z", 0, &z0calls, nil, func(*http.Request) (*http.Response, error) {
			return nil, errors.New("dial timeout")
		})
		postStub(t, gateway, "z", 1, &z1calls, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
		ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
		route := authOnlyRoute()
		route.KeyTiers = []Tier{TierZen}
		_, _, attempts, _ := gateway.doUpstreamTiers(ctx, route, routeBodies(), emptySessionIDs(), 0)
		if postCount(&z0calls) != 1 || postCount(&z1calls) != 0 {
			t.Fatalf("max=1 must send once only: %d/%d", postCount(&z0calls), postCount(&z1calls))
		}
		if attempts != 1 {
			t.Fatalf("attempts=%d want 1", attempts)
		}
		if got := len(monitor.Snapshot().Upstream.Recent); got != 1 {
			t.Fatalf("recorded=%d want 1", got)
		}
	})
	t.Run("max2", func(t *testing.T) {
		monitor := NewMonitor()
		gateway := authTwoProxyGateway(t, monitor, 2)
		var z0calls, z1calls atomic.Int32
		postStub(t, gateway, "z", 0, &z0calls, nil, func(*http.Request) (*http.Response, error) {
			return nil, errors.New("dial timeout")
		})
		postStub(t, gateway, "z", 1, &z1calls, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
		ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
		route := authOnlyRoute()
		route.KeyTiers = []Tier{TierZen}
		_, _, attempts, _ := gateway.doUpstreamTiers(ctx, route, routeBodies(), emptySessionIDs(), 0)
		if postCount(&z0calls)+postCount(&z1calls) != 2 {
			t.Fatalf("max=2 total sends=%d/%d want 2", postCount(&z0calls), postCount(&z1calls))
		}
		if postCount(&z0calls) != 2 {
			t.Fatalf("max=2 must be A,A: %d/%d", postCount(&z0calls), postCount(&z1calls))
		}
		if attempts != 2 {
			t.Fatalf("attempts=%d want 2", attempts)
		}
	})
	t.Run("max3", func(t *testing.T) {
		monitor := NewMonitor()
		gateway := authTwoProxyGateway(t, monitor, 3)
		var z0calls, z1calls atomic.Int32
		postStub(t, gateway, "z", 0, &z0calls, nil, func(*http.Request) (*http.Response, error) {
			return nil, errors.New("dial timeout")
		})
		postStub(t, gateway, "z", 1, &z1calls, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
		ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
		route := authOnlyRoute()
		route.KeyTiers = []Tier{TierZen}
		resp, _, attempts, err := gateway.doUpstreamTiers(ctx, route, routeBodies(), emptySessionIDs(), 0)
		if err != nil || resp == nil || resp.StatusCode != 200 {
			t.Fatalf("err=%v resp=%v want 200 via B", err, resp)
		}
		drainAndClose(resp.Body)
		if postCount(&z0calls) != 2 || postCount(&z1calls) != 1 {
			t.Fatalf("max=3 total=%d/%d want 2/1 (A,A,B)", postCount(&z0calls), postCount(&z1calls))
		}
		if attempts != 3 {
			t.Fatalf("attempts=%d want 3", attempts)
		}
	})
}

// 4. 408/425 retry once same-target, then fallback; state stays neutral.
func TestTransientClient408425(t *testing.T) {
	for _, status := range []int{408, 425} {
		t.Run(http.StatusText(status)+"_retry_success", func(t *testing.T) {
			monitor := NewMonitor()
			gateway := routing400Gateway(t, monitor)
			cap := &capturedUpstream{}
			var a0calls, a1calls atomic.Int32
			var calls atomic.Int32
			postStub(t, gateway, "a", 0, &a0calls, cap, func(*http.Request) (*http.Response, error) {
				if calls.Add(1) == 1 {
					return responseWithBody(status, `{"error":"transient"}`), nil
				}
				return responseWithBody(200, `{"ok":true}`), nil
			})
			postStub(t, gateway, "a", 1, &a1calls, nil, func(*http.Request) (*http.Response, error) {
				return responseWithBody(200, `{"ok":true}`), nil
			})
			ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
			ids := emptySessionIDs()
			resp, _, attempts, err := gateway.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), ids, 0)
			if err != nil || resp == nil || resp.StatusCode != 200 {
				t.Fatalf("status %d err=%v resp=%v", status, err, resp)
			}
			drainAndClose(resp.Body)
			if postCount(&a0calls) != 2 || postCount(&a1calls) != 0 || attempts != 2 {
				t.Fatalf("status %d calls=%d/%d attempts=%d want 2/0/2", status, postCount(&a0calls), postCount(&a1calls), attempts)
			}
			sessions, bodies := cap.get()
			if len(sessions) != 2 || sessions[0] != sessions[1] {
				t.Fatalf("status %d retry must keep session: %q", status, sessions)
			}
			if string(bodies[0]) != string(bodies[1]) {
				t.Fatalf("status %d retry must keep body", status)
			}
			proxy := gateway.pools["a"].items[0]
			identity := targetIdentity(TierZen, anonymousSchedulerCredentialID, "a", proxy.name, "m")
			if got := gateway.scheduler.targetCoolUntil(identity); got != 0 {
				t.Fatalf("status %d must stay neutral, cooldown=%d", status, got)
			}
			recent := monitor.Snapshot().Upstream.Recent
			if len(recent) != 2 || !recent[0].Retryable || recent[0].CoolsDown {
				t.Fatalf("status %d observability must be retryable/neutral: %+v", status, recent)
			}
		})
		t.Run(http.StatusText(status)+"_fallback", func(t *testing.T) {
			monitor := NewMonitor()
			gateway := routing400Gateway(t, monitor)
			var a0calls, a1calls atomic.Int32
			postStub(t, gateway, "a", 0, &a0calls, nil, func(*http.Request) (*http.Response, error) {
				return responseWithBody(status, `{"error":"transient"}`), nil
			})
			postStub(t, gateway, "a", 1, &a1calls, nil, func(*http.Request) (*http.Response, error) {
				return responseWithBody(200, `{"ok":true}`), nil
			})
			ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
			resp, _, attempts, err := gateway.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), emptySessionIDs(), 0)
			if err != nil || resp == nil || resp.StatusCode != 200 {
				t.Fatalf("status %d err=%v", status, err)
			}
			drainAndClose(resp.Body)
			if postCount(&a0calls) != 2 || postCount(&a1calls) != 1 || attempts != 3 {
				t.Fatalf("status %d calls=%d/%d attempts=%d want 2/1/3", status, postCount(&a0calls), postCount(&a1calls), attempts)
			}
		})
	}
}

// 5. 500 retries same-target; 429/403/401 never retry same-target.
func TestServerRetryAndNoRetryClasses(t *testing.T) {
	t.Run("500_retry_success", func(t *testing.T) {
		monitor := NewMonitor()
		gateway := routing400Gateway(t, monitor)
		var a0calls, a1calls atomic.Int32
		var calls atomic.Int32
		postStub(t, gateway, "a", 0, &a0calls, nil, func(*http.Request) (*http.Response, error) {
			if calls.Add(1) == 1 {
				return responseWithBody(500, `{"error":"boom"}`), nil
			}
			return responseWithBody(200, `{"ok":true}`), nil
		})
		postStub(t, gateway, "a", 1, &a1calls, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
		ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
		resp, _, attempts, err := gateway.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), emptySessionIDs(), 0)
		if err != nil || resp == nil || resp.StatusCode != 200 {
			t.Fatalf("err=%v", err)
		}
		drainAndClose(resp.Body)
		if postCount(&a0calls) != 2 || postCount(&a1calls) != 0 || attempts != 2 {
			t.Fatalf("500 must retry same target: %d/%d attempts=%d", postCount(&a0calls), postCount(&a1calls), attempts)
		}
		proxy := gateway.pools["a"].items[0]
		identity := targetIdentity(TierZen, anonymousSchedulerCredentialID, "a", proxy.name, "m")
		if got := gateway.scheduler.targetCoolUntil(identity); got != 0 {
			t.Fatalf("2xx must clear target, got %d", got)
		}
		_ = monitor
	})
	for _, status := range []int{429, 403, 401} {
		t.Run(http.StatusText(status)+"_no_retry", func(t *testing.T) {
			monitor := NewMonitor()
			gateway := routing400Gateway(t, monitor)
			var a0calls, a1calls atomic.Int32
			postStub(t, gateway, "a", 0, &a0calls, nil, func(*http.Request) (*http.Response, error) {
				return responseWithBody(status, `{"error":"x"}`), nil
			})
			postStub(t, gateway, "a", 1, &a1calls, nil, func(*http.Request) (*http.Response, error) {
				return responseWithBody(200, `{"ok":true}`), nil
			})
			ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
			resp, _, _, err := gateway.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), emptySessionIDs(), 0)
			if err != nil || resp == nil || resp.StatusCode != 200 {
				t.Fatalf("status %d err=%v", status, err)
			}
			drainAndClose(resp.Body)
			if postCount(&a0calls) != 1 || postCount(&a1calls) != 1 {
				t.Fatalf("status %d must fallback without same-target retry: %d/%d", status, postCount(&a0calls), postCount(&a1calls))
			}
			_ = monitor
		})
	}
}

// 6. Anonymous ordinary 4xx ends the channel but still enters auth (covered by
// TestNon400KeepsPreviousSemantics); auth ordinary 4xx ends its tier.
// This test pins the auth side explicitly with a real-send budget.
func TestAuthOrdinary4xxEndsTier(t *testing.T) {
	monitor := NewMonitor()
	gateway := authTwoProxyGateway(t, monitor, 5)
	var z0calls, z1calls, goCalls atomic.Int32
	postStub(t, gateway, "z", 0, &z0calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(404, `{"error":"nope"}`), nil
	})
	postStub(t, gateway, "z", 1, &z1calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(404, `{"error":"nope"}`), nil
	})
	postStub(t, gateway, "g", 0, &goCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen, TierGo}
	resp, _, attempts, err := gateway.doUpstreamTiers(ctx, route, routeBodies(), emptySessionIDs(), 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("err=%v", err)
	}
	drainAndClose(resp.Body)
	// Ordinary 404 ends the zen tier after its first candidate; it must not
	// walk the second zen candidate, but must fall back to go.
	if postCount(&z0calls)+postCount(&z1calls) != 1 || postCount(&goCalls) != 1 {
		t.Fatalf("ordinary 4xx must end tier: zen %d/%d go %d", postCount(&z0calls), postCount(&z1calls), postCount(&goCalls))
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
}

// 7. Cancel after the first failed attempt stops retry/fallback/state mutation.
func TestCancelStopsRetryFallback(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	base, cancel := context.WithCancel(context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{}))
	var a0calls, a1calls, zenCalls atomic.Int32
	postStub(t, gateway, "a", 0, &a0calls, nil, func(*http.Request) (*http.Response, error) {
		cancel()
		return responseWithBody(500, `{"error":"boom"}`), nil
	})
	postStub(t, gateway, "a", 1, &a1calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 0, &zenCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := emptySessionIDs()
	resp, _, attempts, _ := gateway.doUpstreamTiers(base, anonAuthRoute(), routeBodies(), ids, 0)
	if resp != nil && resp.Body != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if postCount(&a0calls) != 1 {
		t.Fatalf("first send=%d want 1", postCount(&a0calls))
	}
	if postCount(&a1calls) != 0 || postCount(&zenCalls) != 0 {
		t.Fatalf("cancel must stop fallback: anon1=%d zen=%d", postCount(&a1calls), postCount(&zenCalls))
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1 (no fictitious retry)", attempts)
	}
	if got := len(monitor.Snapshot().Upstream.Recent); got != 1 {
		t.Fatalf("recorded=%d want 1", got)
	}
	proxy := gateway.pools["a"].items[0]
	identity := targetIdentity(TierZen, anonymousSchedulerCredentialID, "a", proxy.name, "m")
	if got := gateway.scheduler.targetCoolUntil(identity); got != 0 {
		t.Fatalf("cancel must not mutate target state, got %d", got)
	}
	if !proxy.healthy.Load() {
		t.Fatalf("cancel must not change proxy health")
	}
	monitor2 := NewMonitor()
	gateway2 := routing400Gateway(t, monitor2)
	base2, cancel2 := context.WithCancel(context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{}))
	var b0calls, b1calls atomic.Int32
	postStub(t, gateway2, "a", 0, &b0calls, nil, func(*http.Request) (*http.Response, error) {
		cancel2()
		return nil, errors.New("dial timeout")
	})
	postStub(t, gateway2, "a", 1, &b1calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	_, _, attempts2, _ := gateway2.doUpstreamTiers(base2, anonAuthRoute(), routeBodies(), ids, 0)
	if attempts2 != 1 || postCount(&b1calls) != 0 {
		t.Fatalf("transport cancel must stop: attempts=%d anon1=%d", attempts2, postCount(&b1calls))
	}
	if got := len(monitor2.Snapshot().Upstream.Recent); got != 1 {
		t.Fatalf("transport cancel recorded=%d want 1", got)
	}
}

// 8. 400 recovery ignores auth max_attempts=1 and never falls back.
func TestAuth400RecoveryIgnoresBudget(t *testing.T) {
	monitor := NewMonitor()
	gateway := authTwoProxyGateway(t, monitor, 1)
	cap := &capturedUpstream{}
	var z0calls, z1calls, goCalls atomic.Int32
	var calls atomic.Int32
	postStub(t, gateway, "z", 0, &z0calls, cap, func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return responseWithBody(400, `{"error":"bad"}`), nil
		}
		return responseWithBody(500, `{"error":"after"}`), nil
	})
	postStub(t, gateway, "z", 1, &z1calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "g", 0, &goCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	resp, _, attempts, err := gateway.doUpstreamTiers(ctx, route, routeBodies(), emptySessionIDs(), 0)
	if err != nil || resp == nil || resp.StatusCode != 500 {
		t.Fatalf("err=%v resp=%v want replay 500 as final", err, resp)
	}
	drainAndClose(resp.Body)
	if postCount(&z0calls) != 2 || postCount(&z1calls) != 0 || postCount(&goCalls) != 0 {
		t.Fatalf("400 recovery must call A,A only: zen0=%d zen1=%d go=%d", postCount(&z0calls), postCount(&z1calls), postCount(&goCalls))
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
	sessions, _ := cap.get()
	if len(sessions) != 2 || sessions[0] == sessions[1] {
		t.Fatalf("400 must rotate session: %q", sessions)
	}
	if got := len(monitor.Snapshot().Upstream.Recent); got != 2 {
		t.Fatalf("recorded=%d want 2", got)
	}
}

// 9. Attempt numbering stays continuous across transient retry and 400 replay.
func TestAttemptNumberingContinuous(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	postStub(t, gateway, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ids := emptySessionIDs()
	pool := gateway.pools["a"]
	step := &atomic.Int32{}
	var firstCalls atomic.Int32
	pool.items[0].client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet {
			return responseWithBody(200, `{}`), nil
		}
		firstCalls.Add(1)
		switch step.Add(1) {
		case 1:
			return nil, errors.New("dial timeout")
		case 2:
			return responseWithBody(400, `{"error":"bad"}`), nil
		default:
			return responseWithBody(200, `{"ok":true}`), nil
		}
	})}
	resp, _, attempts, err := gateway.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), ids, 10)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	drainAndClose(resp.Body)
	if attempts != 13 {
		t.Fatalf("attempts=%d want 13 (offset 10 + 3 sends)", attempts)
	}
	if postCount(&firstCalls) != 3 {
		t.Fatalf("first proxy posts=%d want 3", postCount(&firstCalls))
	}
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) != 3 {
		t.Fatalf("recorded=%d want 3", len(recent))
	}
	for i, rec := range recent {
		if rec.RequestID != ids.Request {
			t.Fatalf("record %d request id %q want %q", i, rec.RequestID, ids.Request)
		}
		if rec.Attempt != 11+i {
			t.Fatalf("record %d attempt=%d want %d", i, rec.Attempt, 11+i)
		}
	}
	if !strings.HasPrefix(recent[0].FailureClass, "transport") {
		t.Fatalf("first class=%q", recent[0].FailureClass)
	}
}
