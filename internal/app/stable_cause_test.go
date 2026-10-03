package app

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func stableLoopRes(final attemptOutcome, stop transientStopReason, initResp *http.Response, initErr error) transientLoopResult {
	return transientLoopResult{Final: final, Stop: stop, InitialResp: initResp, InitialErr: initErr}
}

func TestClassifyStableCause(t *testing.T) {
	sentinel := errors.New("upstream stream startup failure")
	transportErr := errors.New("dial timeout")

	t.Run("buildFailure", func(t *testing.T) {
		r := classifyStableCause(context.Background(), stableLoopRes(attemptOutcome{BuildErr: errors.New("bad build")}, transientStopStable, nil, nil))
		if r.Cause != stableCauseBuildFailure {
			t.Fatalf("cause=%v want buildFailure", r.Cause)
		}
		if r.Stop != transientStopStable {
			t.Fatalf("stop not preserved")
		}
	})

	t.Run("buildFailurePrecedesCancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r := classifyStableCause(ctx, stableLoopRes(attemptOutcome{BuildErr: errors.New("bad build")}, transientStopStable, nil, nil))
		if r.Cause != stableCauseBuildFailure {
			t.Fatalf("cause=%v want buildFailure (build precedes context)", r.Cause)
		}
		if !r.Cancelled {
			t.Fatalf("cancelled must be true")
		}
	})

	t.Run("success", func(t *testing.T) {
		resp := responseWithBody(200, `{"ok":true}`)
		defer drainResp(resp)
		r := classifyStableCause(context.Background(), stableLoopRes(attemptOutcome{Resp: resp}, transientStopStable, nil, nil))
		if r.Cause != stableCauseSuccess {
			t.Fatalf("cause=%v want success", r.Cause)
		}
	})

	t.Run("successPrecedesCancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		resp := responseWithBody(200, `{"ok":true}`)
		defer drainResp(resp)
		r := classifyStableCause(ctx, stableLoopRes(attemptOutcome{Resp: resp}, transientStopStable, nil, nil))
		if r.Cause != stableCauseSuccess {
			t.Fatalf("cause=%v want success (success precedes context)", r.Cause)
		}
	})

	t.Run("contextStop", func(t *testing.T) {
		resp := responseWithBody(503, `{"error":"svc"}`)
		defer drainResp(resp)
		r := classifyStableCause(context.Background(), stableLoopRes(attemptOutcome{Resp: resp}, transientStopContext, resp, nil))
		if r.Cause != stableCauseContext {
			t.Fatalf("cause=%v want context", r.Cause)
		}
		if r.Stop != transientStopContext {
			t.Fatalf("stop not preserved")
		}
		if r.InitialResp != resp {
			t.Fatalf("initial metadata not preserved")
		}
	})

	t.Run("contextCancelledStableStaysOutcome", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		resp := responseWithBody(503, `{"error":"svc"}`)
		defer drainResp(resp)
		r := classifyStableCause(ctx, stableLoopRes(attemptOutcome{Resp: resp}, transientStopStable, nil, nil))
		if r.Cause != stableCauseL1Final {
			t.Fatalf("cause=%v want l1Final (stable+cancelled keeps outcome; callers gate cancel separately)", r.Cause)
		}
		if !r.Cancelled {
			t.Fatalf("cancelled must be true")
		}
		if !r.StillTransient {
			t.Fatalf("stable 503 final must stay transient fact")
		}
	})

	t.Run("observationLimitCancelledStaysOutcome", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		init := responseWithBody(503, `{"error":"initial"}`)
		final := responseWithBody(503, `{"error":"svc"}`)
		r := classifyStableCause(ctx, stableLoopRes(attemptOutcome{Resp: final}, transientStopObservationLimit, init, nil))
		if r.Cause != stableCauseL1Final {
			drainResp(init)
			drainResp(final)
			t.Fatalf("cause=%v want l1Final (observationLimit+cancelled must not fold into context)", r.Cause)
		}
		if !r.Cancelled {
			drainResp(init)
			drainResp(final)
			t.Fatalf("cancelled must be true")
		}
		if r.Stop != transientStopObservationLimit {
			drainResp(init)
			drainResp(final)
			t.Fatalf("observation-limit stop not preserved")
		}
		drainResp(init)
		drainResp(final)
	})

	t.Run("observationLimitCancelledLive429Keeps429", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		resp := responseWithBody(429, `{"error":"throttled"}`)
		defer drainResp(resp)
		r := classifyStableCause(ctx, stableLoopRes(attemptOutcome{Resp: resp}, transientStopObservationLimit, nil, nil))
		if r.Cause != stableCauseLive429 {
			t.Fatalf("cause=%v want live429 (late cancel must not hide the 429 outcome)", r.Cause)
		}
		if !r.Cancelled {
			t.Fatalf("cancelled must be true")
		}
	})

	t.Run("observationLimitCancelledExact400Keeps400", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		resp := responseWithBody(400, `{"error":"bad"}`)
		defer drainResp(resp)
		r := classifyStableCause(ctx, stableLoopRes(attemptOutcome{Resp: resp}, transientStopObservationLimit, nil, nil))
		if r.Cause != stableCauseExact400 {
			t.Fatalf("cause=%v want exact400 (late cancel must not hide the 400 outcome)", r.Cause)
		}
		if !r.Cancelled {
			t.Fatalf("cancelled must be true")
		}
	})

	t.Run("exact400", func(t *testing.T) {
		resp := responseWithBody(400, `{"error":"bad"}`)
		defer drainResp(resp)
		r := classifyStableCause(context.Background(), stableLoopRes(attemptOutcome{Resp: resp}, transientStopStable, nil, nil))
		if r.Cause != stableCauseExact400 {
			t.Fatalf("cause=%v want exact400", r.Cause)
		}
		if r.StillTransient {
			t.Fatalf("400 must never be transient")
		}
		if isSameTargetTransient(resp, nil) {
			t.Fatalf("400 must never enter L1 transient")
		}
	})

	t.Run("live429", func(t *testing.T) {
		resp := responseWithBody(429, `{"error":"throttled"}`)
		defer drainResp(resp)
		r := classifyStableCause(context.Background(), stableLoopRes(attemptOutcome{Resp: resp}, transientStopStable, nil, nil))
		if r.Cause != stableCauseLive429 {
			t.Fatalf("cause=%v want live429", r.Cause)
		}
		if r.StillTransient {
			t.Fatalf("429 must never be transient")
		}
		if isSameTargetTransient(resp, nil) {
			t.Fatalf("429 must never enter L1 transient")
		}
	})

	t.Run("ordinaryRejection", func(t *testing.T) {
		for _, status := range []int{404, 422} {
			resp := responseWithBody(status, `{"error":"n"}`)
			r := classifyStableCause(context.Background(), stableLoopRes(attemptOutcome{Resp: resp}, transientStopStable, nil, nil))
			if r.Cause != stableCauseOrdinaryRejection {
				drainResp(resp)
				t.Fatalf("status=%d cause=%v want ordinaryRejection", status, r.Cause)
			}
			if r.StillTransient {
				drainResp(resp)
				t.Fatalf("status=%d must never be transient", status)
			}
			if isSameTargetTransient(resp, nil) {
				drainResp(resp)
				t.Fatalf("status=%d must never enter L1 transient", status)
			}
			if !isOrdinaryClientRejection(resp, nil) {
				drainResp(resp)
				t.Fatalf("status=%d must reuse ordinary authority", status)
			}
			drainResp(resp)
		}
	})

	t.Run("l1FinalTransient503Limit", func(t *testing.T) {
		init := responseWithBody(503, `{"error":"initial"}`)
		final := responseWithBody(503, `{"error":"svc"}`)
		r := classifyStableCause(context.Background(), stableLoopRes(attemptOutcome{Resp: final}, transientStopObservationLimit, init, nil))
		if r.Cause != stableCauseL1Final {
			drainResp(init)
			drainResp(final)
			t.Fatalf("cause=%v want l1Final", r.Cause)
		}
		if !r.StillTransient {
			drainResp(init)
			drainResp(final)
			t.Fatalf("L1-final 503 must stay transient")
		}
		if r.Stop != transientStopObservationLimit {
			drainResp(init)
			drainResp(final)
			t.Fatalf("observation-limit stop not preserved")
		}
		if r.InitialResp != init {
			drainResp(init)
			drainResp(final)
			t.Fatalf("initial metadata not preserved")
		}
		drainResp(init)
		drainResp(final)
	})

	t.Run("l1FinalTransport", func(t *testing.T) {
		r := classifyStableCause(context.Background(), stableLoopRes(attemptOutcome{Err: transportErr}, transientStopObservationLimit, nil, transportErr))
		if r.Cause != stableCauseL1Final {
			t.Fatalf("cause=%v want l1Final (transport)", r.Cause)
		}
		if !r.StillTransient {
			t.Fatalf("transport must stay transient")
		}
	})

	t.Run("l1FinalStartupSentinel", func(t *testing.T) {
		r := classifyStableCause(context.Background(), stableLoopRes(attemptOutcome{Err: sentinel}, transientStopObservationLimit, nil, sentinel))
		if r.Cause != stableCauseL1Final {
			t.Fatalf("cause=%v want l1Final (sentinel stays in L1 bucket, caller owns local502)", r.Cause)
		}
		if !r.StreamSentinel {
			t.Fatalf("sentinel flag must be true (initial-or-final)")
		}
	})

	t.Run("l1FinalInitialSentinelOnly", func(t *testing.T) {
		final := responseWithBody(503, `{"error":"svc"}`)
		defer drainResp(final)
		r := classifyStableCause(context.Background(), stableLoopRes(attemptOutcome{Resp: final}, transientStopObservationLimit, nil, sentinel))
		if r.Cause != stableCauseL1Final {
			t.Fatalf("cause=%v want l1Final", r.Cause)
		}
		if !r.StreamSentinel {
			t.Fatalf("initial-only sentinel must still poison via flag")
		}
	})

	t.Run("l1FinalStable401", func(t *testing.T) {
		resp := responseWithBody(401, `{"error":"u"}`)
		defer drainResp(resp)
		r := classifyStableCause(context.Background(), stableLoopRes(attemptOutcome{Resp: resp}, transientStopStable, nil, nil))
		if r.Cause != stableCauseL1Final {
			t.Fatalf("cause=%v want l1Final (stable 401 is caller faithful/exhaustion policy, not ordinary)", r.Cause)
		}
		if r.StillTransient {
			t.Fatalf("401 must not be transient")
		}
		if isOrdinaryClientRejection(resp, nil) {
			t.Fatalf("401 must never reuse ordinary authority")
		}
	})

	t.Run("targetForbiddenInitial403", func(t *testing.T) {
		resp := responseWithBody(403, `{"error":"f"}`)
		defer drainResp(resp)
		r := classifyStableCause(context.Background(), stableLoopRes(attemptOutcome{Resp: resp}, transientStopStable, nil, nil))
		if r.Cause != stableCauseTargetForbidden {
			t.Fatalf("cause=%v want targetForbidden (initial 403 is explicit target-scoped cause, not L1-final)", r.Cause)
		}
		if r.StillTransient {
			t.Fatalf("403 must not be transient")
		}
		if isSameTargetTransient(resp, nil) {
			t.Fatalf("403 must never enter L1 transient")
		}
		if isOrdinaryClientRejection(resp, nil) {
			t.Fatalf("403 must never reuse ordinary authority")
		}
		if !isStableTargetForbidden(resp, nil) {
			t.Fatalf("403 must reuse target-forbidden authority")
		}
		if !finalNon429ObjectUnavailable(resp, nil) || !unboundObjectUnavailable(resp, nil) {
			t.Fatalf("403 must stay object-unavailable for exhaustion")
		}
	})

	t.Run("targetForbiddenTransientAfterFinal403", func(t *testing.T) {
		init := responseWithBody(503, `{"error":"initial"}`)
		final := responseWithBody(403, `{"error":"f"}`)
		r := classifyStableCause(context.Background(), stableLoopRes(attemptOutcome{Resp: final}, transientStopStable, init, nil))
		if r.Cause != stableCauseTargetForbidden {
			drainResp(init)
			drainResp(final)
			t.Fatalf("cause=%v want targetForbidden (transient-then-403 final is explicit cause, not L1-final)", r.Cause)
		}
		if r.StillTransient {
			drainResp(init)
			drainResp(final)
			t.Fatalf("final 403 must not stay transient")
		}
		if r.Stop != transientStopStable {
			drainResp(init)
			drainResp(final)
			t.Fatalf("stop not preserved")
		}
		if r.InitialResp != init {
			drainResp(init)
			drainResp(final)
			t.Fatalf("initial metadata not preserved")
		}
		drainResp(init)
		drainResp(final)
	})

	t.Run("targetForbiddenLateCancelKeepsCause", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		resp := responseWithBody(403, `{"error":"f"}`)
		defer drainResp(resp)
		r := classifyStableCause(ctx, stableLoopRes(attemptOutcome{Resp: resp}, transientStopStable, nil, nil))
		if r.Cause != stableCauseTargetForbidden {
			t.Fatalf("cause=%v want targetForbidden (late cancel must not hide the 403 outcome)", r.Cause)
		}
		if !r.Cancelled {
			t.Fatalf("cancelled must be true")
		}
	})

	t.Run("contextNeverObjectUnavailable", func(t *testing.T) {
		resp := responseWithBody(503, `{"error":"svc"}`)
		defer drainResp(resp)
		r := classifyStableCause(context.Background(), stableLoopRes(attemptOutcome{Resp: resp}, transientStopContext, resp, nil))
		if r.Cause != stableCauseContext {
			t.Fatalf("cause=%v want context", r.Cause)
		}
		// The underlying 503 alone would count as unavailable, but the
		// context cause must gate it: callers return before any
		// markUnavailable/exhaustion accounting.
		if !finalNon429ObjectUnavailable(resp, nil) {
			t.Fatalf("test premise broken: 503 must count at leaf")
		}
	})
}

func TestPinnedAuthContextEarlyReturn(t *testing.T) {
	mk := func(stop transientStopReason, cancelled bool) stableCauseResult {
		ctx := context.Background()
		if cancelled {
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			cancel()
		}
		return classifyStableCause(ctx, stableLoopRes(attemptOutcome{Resp: responseWithBody(503, `x`)}, stop, nil, nil))
	}
	cases := []struct {
		name   string
		stop   transientStopReason
		cancel bool
		want   recoveryAction
	}{
		{name: "stopContext", stop: transientStopContext, cancel: false, want: recoveryReturnContext},
		{name: "stableCancelled", stop: transientStopStable, cancel: true, want: recoveryReturnContext},
		{name: "observationLimitCancelledNoEarlyReturn", stop: transientStopObservationLimit, cancel: true, want: recoveryWalkNext},
		{name: "observationLimitNoCancel", stop: transientStopObservationLimit, cancel: false, want: recoveryWalkNext},
		{name: "stableNoCancel", stop: transientStopStable, cancel: false, want: recoveryWalkNext},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := mk(tc.stop, tc.cancel)
			defer drainResp(r.Final.Resp)
			// Unified bound walk: an observation-limit/stable 503 final
			// walks the next frozen proxy; stop-context or stable+cancelled
			// still returns context via the strict gate.
			if got := decideCandidateRecovery(recoveryLane{Bound: true, Anonymous: false}, r); got.Action != tc.want {
				t.Fatalf("action=%v want %v (stop=%v cancelled=%v)", got.Action, tc.want, tc.stop, tc.cancel)
			}
		})
	}
}
