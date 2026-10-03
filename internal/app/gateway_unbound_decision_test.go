package app

import (
	"errors"
	"testing"
)

// TestDecideCandidateRecovery covers the shared context-aware pure decision
// across the four recovery lanes. Unified bound walk (optimal repair):
// pinned anon+auth walk the next frozen proxy for stable 403 and for
// L1-final transport/startup/408/425/5xx; 401 stays credential-global
// faithful; 429 stashes; ordinary/400/context/build unchanged.
func TestDecideCandidateRecovery(t *testing.T) {
	sentinel := errors.New("upstream stream startup failure")
	timeoutSentinel := errors.New("upstream stream startup timeout")
	transportErr := errors.New("dial timeout")

	cases := []struct {
		name string
		lane recoveryLane
		in   stableCauseResult
		want recoveryAction
		mark bool
	}{
		// Build/success precede cancellation on every lane.
		{name: "build/unbound-anon-cancelled", lane: recoveryLane{Bound: false, Anonymous: true}, in: stableCauseResult{Cause: stableCauseBuildFailure, Stop: transientStopStable, Cancelled: true}, want: recoveryReturnBuild},
		{name: "build/pinned-auth-cancelled", lane: recoveryLane{Bound: true, Anonymous: false}, in: stableCauseResult{Cause: stableCauseBuildFailure, Stop: transientStopStable, Cancelled: true}, want: recoveryReturnBuild},
		{name: "success/unbound-auth-cancelled", lane: recoveryLane{Bound: false, Anonymous: false}, in: stableCauseResult{Cause: stableCauseSuccess, Stop: transientStopStable, Cancelled: true}, want: recoveryReturnSuccess},
		{name: "success/pinned-anon-cancelled", lane: recoveryLane{Bound: true, Anonymous: true}, in: stableCauseResult{Cause: stableCauseSuccess, Stop: transientStopStable, Cancelled: true}, want: recoveryReturnSuccess},

		// Context gates differ by lane.
		{name: "anon-broad/context-cause", lane: recoveryLane{Bound: false, Anonymous: true}, in: stableCauseResult{Cause: stableCauseContext, Stop: transientStopContext}, want: recoveryReturnContext},
		{name: "anon-broad/late-cancel-overrides-live429", lane: recoveryLane{Bound: false, Anonymous: true}, in: stableCauseResult{Cause: stableCauseLive429, Stop: transientStopStable, Cancelled: true}, want: recoveryReturnContext},
		{name: "pinned-anon-broad/late-cancel-overrides-ordinary", lane: recoveryLane{Bound: true, Anonymous: true}, in: stableCauseResult{Cause: stableCauseOrdinaryRejection, Stop: transientStopStable, Cancelled: true}, want: recoveryReturnContext},
		{name: "unbound-auth/cancel-overrides-live429", lane: recoveryLane{Bound: false, Anonymous: false}, in: stableCauseResult{Cause: stableCauseLive429, Stop: transientStopStable, Cancelled: true}, want: recoveryReturnContext},
		{name: "unbound-auth/context-cause-without-cancel-advances", lane: recoveryLane{Bound: false, Anonymous: false}, in: stableCauseResult{Cause: stableCauseContext, Stop: transientStopContext}, want: recoveryAdvanceNext},
		{name: "pinned-auth-strict/stop-context", lane: recoveryLane{Bound: true, Anonymous: false}, in: stableCauseResult{Cause: stableCauseL1Final, Stop: transientStopContext}, want: recoveryReturnContext},
		{name: "pinned-auth-strict/stable-cancelled", lane: recoveryLane{Bound: true, Anonymous: false}, in: stableCauseResult{Cause: stableCauseTargetForbidden, Stop: transientStopStable, Cancelled: true}, want: recoveryReturnContext},
		{name: "pinned-auth-strict/limit-cancelled-no-early-return", lane: recoveryLane{Bound: true, Anonymous: false}, in: stableCauseResult{Cause: stableCauseLive429, Stop: transientStopObservationLimit, Cancelled: true}, want: recoveryLive429},

		// Exact-400 is the corrective-action marker on every lane.
		{name: "replay/unbound-anon", lane: recoveryLane{Bound: false, Anonymous: true}, in: stableCauseResult{Cause: stableCauseExact400, Stop: transientStopStable}, want: recoveryReplayTerminal},
		{name: "replay/pinned-auth", lane: recoveryLane{Bound: true, Anonymous: false}, in: stableCauseResult{Cause: stableCauseExact400, Stop: transientStopStable}, want: recoveryReplayTerminal},
		// Exact-400 + cancelled (legal Stop is stable: 400 never enters L1)
		// returns context on every lane via the preserved gates: anonymous
		// broad, unbound-auth Cancelled-only, pinned-auth strict
		// stable+cancelled. Observation-limit + cancelled is not a legal
		// exact-400 Stop and is never asserted here, preserving the
		// pinned-auth narrow gate below.
		{name: "exact400-cancelled/unbound-anon", lane: recoveryLane{Bound: false, Anonymous: true}, in: stableCauseResult{Cause: stableCauseExact400, Stop: transientStopStable, Cancelled: true}, want: recoveryReturnContext},
		{name: "exact400-cancelled/unbound-auth", lane: recoveryLane{Bound: false, Anonymous: false}, in: stableCauseResult{Cause: stableCauseExact400, Stop: transientStopStable, Cancelled: true}, want: recoveryReturnContext},
		{name: "exact400-cancelled/pinned-anon", lane: recoveryLane{Bound: true, Anonymous: true}, in: stableCauseResult{Cause: stableCauseExact400, Stop: transientStopStable, Cancelled: true}, want: recoveryReturnContext},
		{name: "exact400-cancelled/pinned-auth-stable", lane: recoveryLane{Bound: true, Anonymous: false}, in: stableCauseResult{Cause: stableCauseExact400, Stop: transientStopStable, Cancelled: true}, want: recoveryReturnContext},

		// Ordinary returns faithfully everywhere.
		{name: "ordinary/unbound-auth", lane: recoveryLane{Bound: false, Anonymous: false}, in: stableCauseResult{Cause: stableCauseOrdinaryRejection, Stop: transientStopStable}, want: recoveryReturnOrdinary},
		{name: "ordinary/pinned-anon", lane: recoveryLane{Bound: true, Anonymous: true}, in: stableCauseResult{Cause: stableCauseOrdinaryRejection, Stop: transientStopStable}, want: recoveryReturnOrdinary},
		// Ordinary + cancelled on the unbound lanes (legal Stop is stable:
		// ordinary never enters L1) returns context via the preserved
		// gates; pinned lanes are not asserted here.
		{name: "ordinary-cancelled/unbound-anon", lane: recoveryLane{Bound: false, Anonymous: true}, in: stableCauseResult{Cause: stableCauseOrdinaryRejection, Stop: transientStopStable, Cancelled: true}, want: recoveryReturnContext},
		{name: "ordinary-cancelled/unbound-auth", lane: recoveryLane{Bound: false, Anonymous: false}, in: stableCauseResult{Cause: stableCauseOrdinaryRejection, Stop: transientStopStable, Cancelled: true}, want: recoveryReturnContext},

		// Live 429: unbound advances with mark, pinned stashes.
		{name: "live429/unbound-anon-marks", lane: recoveryLane{Bound: false, Anonymous: true}, in: stableCauseResult{Cause: stableCauseLive429, Stop: transientStopStable}, want: recoveryAdvanceNext, mark: true},
		{name: "live429/unbound-auth-marks", lane: recoveryLane{Bound: false, Anonymous: false}, in: stableCauseResult{Cause: stableCauseLive429, Stop: transientStopStable}, want: recoveryAdvanceNext, mark: true},
		{name: "live429/pinned-anon-stashes", lane: recoveryLane{Bound: true, Anonymous: true}, in: stableCauseResult{Cause: stableCauseLive429, Stop: transientStopStable}, want: recoveryLive429},
		{name: "live429/pinned-auth-stashes", lane: recoveryLane{Bound: true, Anonymous: false}, in: stableCauseResult{Cause: stableCauseLive429, Stop: transientStopStable}, want: recoveryLive429},

		// Target-scoped 403: unbound advances with mark, pinned walks unified.
		{name: "target403/unbound-marks", lane: recoveryLane{Bound: false, Anonymous: false}, in: stableCauseResult{Cause: stableCauseTargetForbidden, Stop: transientStopStable}, want: recoveryAdvanceNext, mark: true},
		{name: "target403/pinned-anon-walks", lane: recoveryLane{Bound: true, Anonymous: true}, in: stableCauseResult{Cause: stableCauseTargetForbidden, Stop: transientStopStable}, want: recoveryWalkNext, mark: true},
		{name: "target403/pinned-auth-walks", lane: recoveryLane{Bound: true, Anonymous: false}, in: stableCauseResult{Cause: stableCauseTargetForbidden, Stop: transientStopStable}, want: recoveryWalkNext, mark: true},

		// Startup sentinel walks unified (generic and bounded timeout).
		{name: "sentinel/pinned-auth-walks", lane: recoveryLane{Bound: true, Anonymous: false}, in: stableCauseResult{Cause: stableCauseL1Final, Stop: transientStopObservationLimit, StreamSentinel: true, StillTransient: true}, want: recoveryWalkNext, mark: true},
		{name: "sentinel/pinned-anon-walks", lane: recoveryLane{Bound: true, Anonymous: true}, in: stableCauseResult{Cause: stableCauseL1Final, Final: attemptOutcome{Resp: responseWithBody(503, `x`)}, Stop: transientStopObservationLimit, StreamSentinel: true, StillTransient: true}, want: recoveryWalkNext, mark: true},
		{name: "sentinel/pinned-anon-final-walks", lane: recoveryLane{Bound: true, Anonymous: true}, in: stableCauseResult{Cause: stableCauseL1Final, Final: attemptOutcome{Err: sentinel}, Stop: transientStopObservationLimit}, want: recoveryWalkNext, mark: true},
		{name: "sentinel-timeout/pinned-anon-walks", lane: recoveryLane{Bound: true, Anonymous: true}, in: stableCauseResult{Cause: stableCauseL1Final, Final: attemptOutcome{Err: timeoutSentinel}, Stop: transientStopObservationLimit}, want: recoveryWalkNext, mark: true},
		{name: "sentinel-timeout/pinned-auth-walks", lane: recoveryLane{Bound: true, Anonymous: false}, in: stableCauseResult{Cause: stableCauseL1Final, Final: attemptOutcome{Err: timeoutSentinel}, Stop: transientStopObservationLimit}, want: recoveryWalkNext, mark: true},
		{name: "sentinel/unbound-advances", lane: recoveryLane{Bound: false, Anonymous: false}, in: stableCauseResult{Cause: stableCauseL1Final, Final: attemptOutcome{Err: sentinel}, Stop: transientStopObservationLimit, StreamSentinel: true}, want: recoveryAdvanceNext, mark: true},

		// L1-final transport walks unified on both pinned lanes.
		{name: "transport/pinned-auth-walks", lane: recoveryLane{Bound: true, Anonymous: false}, in: stableCauseResult{Cause: stableCauseL1Final, Final: attemptOutcome{Err: transportErr}, Stop: transientStopObservationLimit}, want: recoveryWalkNext, mark: true},
		{name: "transport/pinned-anon-walks", lane: recoveryLane{Bound: true, Anonymous: true}, in: stableCauseResult{Cause: stableCauseL1Final, Final: attemptOutcome{Err: transportErr}, Stop: transientStopObservationLimit}, want: recoveryWalkNext, mark: true},
		{name: "transport/unbound-marks", lane: recoveryLane{Bound: false, Anonymous: true}, in: stableCauseResult{Cause: stableCauseL1Final, Final: attemptOutcome{Err: transportErr}, Stop: transientStopObservationLimit}, want: recoveryAdvanceNext, mark: true},
		{name: "l1final/unbound-context-no-mark", lane: recoveryLane{Bound: false, Anonymous: true}, in: stableCauseResult{Cause: stableCauseL1Final, Stop: transientStopContext}, want: recoveryAdvanceNext},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := tc.in
			// The 503-body case builds its response inline; drain after.
			if in.Final.Resp != nil && (tc.name == "sentinel/pinned-anon-walks") {
				defer drainResp(in.Final.Resp)
			}
			got := decideCandidateRecovery(tc.lane, in)
			if got.Action != tc.want || got.MarkUnavailable != tc.mark {
				t.Fatalf("decideCandidateRecovery(%+v, %+v) = %+v, want action=%v mark=%v", tc.lane, in, got, tc.want, tc.mark)
			}
		})
	}
}
