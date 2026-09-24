package app

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Narrow L1 floor: explicit interval 0 stays valid config but never resends
// immediately. Shared transientDelay raises zero/negative/below-floor input to
// the named 100ms minimum, then maxes with the configured positive interval
// and any parsed Retry-After. Not a broad backoff matrix.
func TestTransientDelayFloor(t *testing.T) {
	if got := transientDelay(0, nil); got != minL1ObservationDelay {
		t.Fatalf("interval 0 no Retry-After must floor to %v, got %v", minL1ObservationDelay, got)
	}
	if got := transientDelay(-5*time.Second, nil); got != minL1ObservationDelay {
		t.Fatalf("negative interval must floor to %v, got %v", minL1ObservationDelay, got)
	}
	if got := transientDelay(50*time.Millisecond, nil); got != minL1ObservationDelay {
		t.Fatalf("below-floor positive interval must floor to %v, got %v", minL1ObservationDelay, got)
	}
	if got := transientDelay(3*time.Second, nil); got != 3*time.Second {
		t.Fatalf("positive interval above floor must stay 3s, got %v", got)
	}
	// Retry-After larger than floor/interval wins.
	resp := responseWithBody(503, `{"error":"svc"}`)
	resp.Header.Set("Retry-After", "1")
	if got := transientDelay(0, resp); got != time.Second {
		t.Fatalf("interval 0 + Retry-After 1s must be 1s, got %v", got)
	}
	drainResp(resp)
	resp2 := responseWithBody(503, `{"error":"svc"}`)
	resp2.Header.Set("Retry-After", "1")
	if got := transientDelay(3*time.Second, resp2); got != 3*time.Second {
		t.Fatalf("interval 3s + Retry-After 1s must stay 3s, got %v", got)
	}
	drainResp(resp2)
	// Retry-After larger than a positive interval wins.
	resp3 := responseWithBody(503, `{"error":"svc"}`)
	resp3.Header.Set("Retry-After", "5")
	if got := transientDelay(3*time.Second, resp3); got != 5*time.Second {
		t.Fatalf("interval 3s + Retry-After 5s must be 5s, got %v", got)
	}
	drainResp(resp3)
}

// Real observation-loop proof: configured interval 0 (transientInterval()==0)
// cannot immediately resend; the shared L1 loop waits the internal floor.
func TestObserveIntervalZeroHasMinimumDelay(t *testing.T) {
	cfg := testGatewayConfig(
		map[string][]string{"z": {"direct"}},
		ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if norm.Retry.TransientRetryIntervalSeconds != 0 {
		t.Fatalf("explicit interval 0 must persist 0, got %d", norm.Retry.TransientRetryIntervalSeconds)
	}
	gw, err := NewGateway(norm, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	interval := gw.transientInterval()
	if interval != 0 {
		t.Fatalf("transientInterval for explicit 0 must stay 0, got %v", interval)
	}
	attempts := 1
	initial := attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`)}
	calls := 0
	start := time.Now()
	loopRes := gw.observeSameTargetTransient(context.Background(), initial, TierZen, ProtocolChat, &attempts, 0, 2, interval, func(monitorAttempt int) attemptOutcome {
		calls++
		return attemptOutcome{Resp: responseWithBody(200, `{"ok":true}`)}
	})
	elapsed := time.Since(start)
	defer drainResp(loopRes.Final.Resp)
	if calls != 1 {
		t.Fatalf("calls=%d want 1", calls)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
	if loopRes.Stop != transientStopStable {
		t.Fatalf("stop=%v want stable", loopRes.Stop)
	}
	if loopRes.Final.Resp == nil || loopRes.Final.Resp.StatusCode != 200 {
		t.Fatalf("final must be 200")
	}
	// Robust lower bound only: immediate resend would be ~0ms, floor is 100ms.
	if elapsed < 50*time.Millisecond {
		t.Fatalf("interval 0 must not immediately resend: elapsed %v < 50ms (floor %v)", elapsed, minL1ObservationDelay)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("elapsed %v unreasonably long", elapsed)
	}
}

// The floor wait stays cancellable: cancel/deadline during the 100ms floor
// performs no further send.
func TestObserveIntervalZeroFloorCancellable(t *testing.T) {
	newGW := func(t *testing.T) *Gateway {
		t.Helper()
		cfg := testGatewayConfig(
			map[string][]string{"z": {"direct"}},
			ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
		)
		cfg.Anonymous = false
		cfg.Keys = []string{"zen-key-aaaaa"}
		cfg.Retry.MaxAttempts = 3
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

	t.Run("cancel", func(t *testing.T) {
		gw := newGW(t)
		interval := gw.transientInterval()
		attempts := 1
		initial := attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`)}
		calls := 0
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()
		start := time.Now()
		loopRes := gw.observeSameTargetTransient(ctx, initial, TierZen, ProtocolChat, &attempts, 0, 3, interval, func(monitorAttempt int) attemptOutcome {
			calls++
			return attemptOutcome{Resp: responseWithBody(200, `{"ok":true}`)}
		})
		elapsed := time.Since(start)
		if calls != 0 {
			t.Fatalf("cancel during floor must not send again: calls=%d", calls)
		}
		if attempts != 1 {
			t.Fatalf("attempts=%d want 1 (no observation send)", attempts)
		}
		if loopRes.Stop != transientStopContext {
			t.Fatalf("stop=%v want context", loopRes.Stop)
		}
		if loopRes.Final.Resp != nil {
			drainResp(loopRes.Final.Resp)
			t.Fatalf("cancelled floor must not return drained transient body")
		}
		if loopRes.Final.Err == nil || (!errors.Is(loopRes.Final.Err, context.Canceled) && !errors.Is(loopRes.Final.Err, context.DeadlineExceeded)) {
			t.Fatalf("final err=%v want cancel/deadline", loopRes.Final.Err)
		}
		if elapsed < 10*time.Millisecond {
			t.Fatalf("cancel test elapsed %v suspiciously short, floor wait not exercised", elapsed)
		}
		if elapsed > 5*time.Second {
			t.Fatalf("elapsed %v unreasonably long", elapsed)
		}
	})

	t.Run("deadline", func(t *testing.T) {
		gw := newGW(t)
		interval := gw.transientInterval()
		attempts := 1
		initial := attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`)}
		calls := 0
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		loopRes := gw.observeSameTargetTransient(ctx, initial, TierZen, ProtocolChat, &attempts, 0, 3, interval, func(monitorAttempt int) attemptOutcome {
			calls++
			return attemptOutcome{Resp: responseWithBody(200, `{"ok":true}`)}
		})
		if calls != 0 {
			t.Fatalf("deadline during floor must not send again: calls=%d", calls)
		}
		if attempts != 1 {
			t.Fatalf("attempts=%d want 1", attempts)
		}
		if loopRes.Stop != transientStopContext {
			t.Fatalf("stop=%v want context", loopRes.Stop)
		}
		if loopRes.Final.Resp != nil {
			drainResp(loopRes.Final.Resp)
			t.Fatalf("deadline floor must not return drained transient body")
		}
	})
}
