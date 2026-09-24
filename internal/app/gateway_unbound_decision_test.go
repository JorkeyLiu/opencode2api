package app

import "testing"

func TestDecideUnboundPostL1(t *testing.T) {
	cases := []struct {
		name string
		lane unboundLane
		in   stableCauseResult
		want unboundAction
		mark bool
	}{
		{name: "build/anonymous", lane: unboundLaneAnonymous, in: stableCauseResult{Cause: stableCauseBuildFailure, Stop: transientStopStable}, want: unboundActionReturnBuild},
		{name: "build/authenticated", lane: unboundLaneAuthenticated, in: stableCauseResult{Cause: stableCauseBuildFailure, Stop: transientStopStable}, want: unboundActionReturnBuild},
		{name: "success/anonymous", lane: unboundLaneAnonymous, in: stableCauseResult{Cause: stableCauseSuccess, Stop: transientStopStable}, want: unboundActionReturnSuccess},
		{name: "success/authenticated", lane: unboundLaneAuthenticated, in: stableCauseResult{Cause: stableCauseSuccess, Stop: transientStopStable}, want: unboundActionReturnSuccess},
		{name: "success/anonymous-cancelled", lane: unboundLaneAnonymous, in: stableCauseResult{Cause: stableCauseSuccess, Stop: transientStopStable, Cancelled: true}, want: unboundActionReturnSuccess},
		{name: "success/authenticated-cancelled", lane: unboundLaneAuthenticated, in: stableCauseResult{Cause: stableCauseSuccess, Stop: transientStopStable, Cancelled: true}, want: unboundActionReturnSuccess},
		{name: "build/anonymous-cancelled", lane: unboundLaneAnonymous, in: stableCauseResult{Cause: stableCauseBuildFailure, Stop: transientStopStable, Cancelled: true}, want: unboundActionReturnBuild},
		{name: "anon/context-cause", lane: unboundLaneAnonymous, in: stableCauseResult{Cause: stableCauseContext, Stop: transientStopContext}, want: unboundActionReturnContext},
		{name: "anon/late-cancel-overrides-live429", lane: unboundLaneAnonymous, in: stableCauseResult{Cause: stableCauseLive429, Stop: transientStopStable, Cancelled: true}, want: unboundActionReturnContext},
		{name: "auth/cancel-overrides-live429", lane: unboundLaneAuthenticated, in: stableCauseResult{Cause: stableCauseLive429, Stop: transientStopStable, Cancelled: true}, want: unboundActionReturnContext},
		{name: "auth/context-cause-without-cancel-advances", lane: unboundLaneAuthenticated, in: stableCauseResult{Cause: stableCauseContext, Stop: transientStopContext}, want: unboundActionAdvanceNext},
		{name: "replay/anonymous", lane: unboundLaneAnonymous, in: stableCauseResult{Cause: stableCauseExact400, Stop: transientStopStable}, want: unboundActionReplay},
		{name: "replay/authenticated", lane: unboundLaneAuthenticated, in: stableCauseResult{Cause: stableCauseExact400, Stop: transientStopStable}, want: unboundActionReplay},
		{name: "exact400/anonymous-cancelled", lane: unboundLaneAnonymous, in: stableCauseResult{Cause: stableCauseExact400, Stop: transientStopStable, Cancelled: true}, want: unboundActionReturnContext},
		{name: "exact400/authenticated-cancelled", lane: unboundLaneAuthenticated, in: stableCauseResult{Cause: stableCauseExact400, Stop: transientStopStable, Cancelled: true}, want: unboundActionReturnContext},
		{name: "ordinary/anonymous", lane: unboundLaneAnonymous, in: stableCauseResult{Cause: stableCauseOrdinaryRejection, Stop: transientStopStable}, want: unboundActionReturnOrdinary},
		{name: "ordinary/authenticated", lane: unboundLaneAuthenticated, in: stableCauseResult{Cause: stableCauseOrdinaryRejection, Stop: transientStopStable}, want: unboundActionReturnOrdinary},
		{name: "ordinary/anonymous-cancelled", lane: unboundLaneAnonymous, in: stableCauseResult{Cause: stableCauseOrdinaryRejection, Stop: transientStopStable, Cancelled: true}, want: unboundActionReturnContext},
		{name: "ordinary/authenticated-cancelled", lane: unboundLaneAuthenticated, in: stableCauseResult{Cause: stableCauseOrdinaryRejection, Stop: transientStopStable, Cancelled: true}, want: unboundActionReturnContext},
		{name: "live429/anonymous-marks", lane: unboundLaneAnonymous, in: stableCauseResult{Cause: stableCauseLive429, Stop: transientStopStable}, want: unboundActionAdvanceNext, mark: true},
		{name: "live429/authenticated-marks", lane: unboundLaneAuthenticated, in: stableCauseResult{Cause: stableCauseLive429, Stop: transientStopStable}, want: unboundActionAdvanceNext, mark: true},
		{name: "l1final/stable-marks", lane: unboundLaneAnonymous, in: stableCauseResult{Cause: stableCauseL1Final, Stop: transientStopStable}, want: unboundActionAdvanceNext, mark: true},
		{name: "l1final/limit-marks", lane: unboundLaneAuthenticated, in: stableCauseResult{Cause: stableCauseL1Final, Stop: transientStopObservationLimit}, want: unboundActionAdvanceNext, mark: true},
		{name: "l1final/context-no-mark", lane: unboundLaneAnonymous, in: stableCauseResult{Cause: stableCauseL1Final, Stop: transientStopContext}, want: unboundActionAdvanceNext},
		{name: "l1final/anonymous-cancelled", lane: unboundLaneAnonymous, in: stableCauseResult{Cause: stableCauseL1Final, Stop: transientStopStable, Cancelled: true}, want: unboundActionReturnContext},
		{name: "l1final/authenticated-cancelled", lane: unboundLaneAuthenticated, in: stableCauseResult{Cause: stableCauseL1Final, Stop: transientStopStable, Cancelled: true}, want: unboundActionReturnContext},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := decideUnboundPostL1(tc.lane, tc.in)
			if got.Action != tc.want || got.MarkUnavailable != tc.mark {
				t.Fatalf("decideUnboundPostL1(%v, %+v) = %+v, want action=%v mark=%v", tc.lane, tc.in, got, tc.want, tc.mark)
			}
		})
	}
}
