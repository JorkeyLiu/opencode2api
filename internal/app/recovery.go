package app

import (
	"context"
	"net/http"
	"time"
)

// recoveryLane is the context-aware decision input for the shared single
// candidate recovery authority. Bound distinguishes established (pinned)
// bindings from unbound establishment; Anonymous distinguishes the anonymous
// lane from the authenticated lane. The unified pure decision preserves the
// existing per-context differences instead of flattening them; callers keep
// every effect (frozen candidates, fences, scheduler writes, pin fencing,
// route-session/body/attempt metadata, response ownership, custom gates).
type recoveryLane struct {
	Bound     bool
	Anonymous bool
}

// recoveryAction is the typed single-candidate outcome owned by the shared
// authority. Walkers execute the action with their own state; the authority
// never touches scheduler/pin/session stores, response draining, logging,
// or custom fallback.
type recoveryAction int

const (
	recoveryReturnBuild recoveryAction = iota
	recoveryReturnSuccess
	recoveryReturnContext
	recoveryReplayTerminal
	recoveryLive429
	recoveryReturnOrdinary
	recoveryAdvanceNext
	recoverySentinel502
	recoveryWalkNext
	recoveryFaithful
)

// recoveryDecision is the pure per-candidate decision: the typed action plus
// whether an advance proves its object unavailable. It selects no response,
// drains nothing, and touches no runtime state.
type recoveryDecision struct {
	Action          recoveryAction
	MarkUnavailable bool
}

// decideCandidateRecovery is the single pure context-aware decision seam for
// one classified stable candidate outcome. Inputs are lane plus the
// classified cause/stop/cancelled/sentinel facts only; it never reads ctx,
// attempts, Started, Retry-After, or any runtime state.
//
// Preserved context differences (verified against the pre-migration walkers):
//   - Context gate: anonymous lanes (bound or unbound) return context on
//     Cause==context or late Cancelled; unbound authenticated returns
//     context on Cancelled only; pinned authenticated uses the strict
//     stop==context-or-(stable+cancelled) gate so an observation-limit final
//     with a ctx cancelled after observation stays on the L1-final path.
//   - Live 429: unbound advances with mark; pinned stashes for the 429 walk,
//     except pinned-auth with an initial-or-final stream sentinel poisons to
//     local 502 (pinned-anonymous sentinel is final-only, so an
//     initial-only sentinel with a 429 final still stashes).
//   - Stream sentinel: pinned-auth uses initial-or-final identity, pinned
//     anonymous uses final-only identity; unbound has no sentinel branch and
//     advances with mark as ordinary L1-final.
//   - Target-scoped 403: unbound advances with mark; pinned stays faithful
//     with no move (custom gate owned by the caller).
//   - L1-final remainder: unbound advances (mark only for stable/limit stops);
//     pinned anonymous stays faithful; pinned authenticated walks the next
//     eligible proxy only for a non-sentinel transport final (Err != nil),
//     otherwise stays faithful (5xx/408/425/stable HTTP never move).
func decideCandidateRecovery(lane recoveryLane, r stableCauseResult) recoveryDecision {
	switch {
	case r.Cause == stableCauseBuildFailure:
		return recoveryDecision{Action: recoveryReturnBuild}
	case r.Cause == stableCauseSuccess:
		return recoveryDecision{Action: recoveryReturnSuccess}
	case lane.Bound && !lane.Anonymous && (r.Stop == transientStopContext || (r.Stop == transientStopStable && r.Cancelled)):
		return recoveryDecision{Action: recoveryReturnContext}
	case !lane.Bound && lane.Anonymous && (r.Cause == stableCauseContext || r.Cancelled):
		return recoveryDecision{Action: recoveryReturnContext}
	case lane.Bound && lane.Anonymous && (r.Cause == stableCauseContext || r.Cancelled):
		return recoveryDecision{Action: recoveryReturnContext}
	case !lane.Bound && !lane.Anonymous && r.Cancelled:
		return recoveryDecision{Action: recoveryReturnContext}
	case r.Cause == stableCauseExact400:
		// Executed by the runner's single corrective-action authority; never
		// returned to walkers.
		return recoveryDecision{Action: recoveryReplayTerminal}
	case r.Cause == stableCauseOrdinaryRejection:
		return recoveryDecision{Action: recoveryReturnOrdinary}
	case r.Cause == stableCauseLive429:
		// Pinned-auth initial-sentinel poisoning vs full-429 exhaustion
		// order stays walker-owned (exhaustion precedes poisoning): the
		// authority returns stash and the walker checks the 429-only gate
		// first, then the initial-sentinel poison on the partial path.
		if lane.Bound {
			return recoveryDecision{Action: recoveryLive429}
		}
		return recoveryDecision{Action: recoveryAdvanceNext, MarkUnavailable: true}
	case r.Cause == stableCauseTargetForbidden:
		if lane.Bound {
			return recoveryDecision{Action: recoveryFaithful}
		}
		return recoveryDecision{Action: recoveryAdvanceNext, MarkUnavailable: true}
	default:
		// L1-final bucket (408/425/5xx incl. 503, true transport, stable
		// 401, stream-startup sentinels). Sentinel precedes the general
		// mapping with the preserved per-lane identity.
		if lane.Bound && !lane.Anonymous && r.StreamSentinel {
			return recoveryDecision{Action: recoverySentinel502}
		}
		if lane.Bound && lane.Anonymous && isStreamStartupFailureErr(r.Final.Err) {
			return recoveryDecision{Action: recoverySentinel502}
		}
		if !lane.Bound {
			if r.Stop == transientStopObservationLimit || r.Stop == transientStopStable {
				return recoveryDecision{Action: recoveryAdvanceNext, MarkUnavailable: true}
			}
			return recoveryDecision{Action: recoveryAdvanceNext}
		}
		if lane.Bound && !lane.Anonymous && r.Final.Err != nil && !isStreamStartupFailureErr(r.Final.Err) {
			return recoveryDecision{Action: recoveryWalkNext}
		}
		return recoveryDecision{Action: recoveryFaithful}
	}
}

// candidateRecovery is the typed single-candidate lifecycle result. Final is
// the last observed outcome (initial stable or L1 final); ReplayResp/ReplayErr
// carry the route-terminal replay result when Action is recoveryReplayTerminal.
// MarkUnavailable mirrors the pure decision; walkers apply their own
// mark/evidence writes. Started is preserved inside Final for credential429
// stale fencing; final.Started is never dropped.
type candidateRecovery struct {
	Action          recoveryAction
	Final           attemptOutcome
	Stop            transientStopReason
	Cancelled       bool
	Cause           stableCause
	MarkUnavailable bool
	InitialResp     *http.Response
	InitialErr      error
	ReplayResp      *http.Response
	ReplayErr       error
}

// recoverSingleCandidate owns one frozen candidate's full lifecycle with a
// single initial send, a single bounded L1 observation authority, and a
// single exact-400 corrective-action authority. exec performs one real send
// for the given monitor attempt number; replay runs the existing
// same-target same-request same-session corrective replay once and reports
// handled plus the replay outcome. attempts is the caller-owned relative
// counter mutated for every real send including L1 retries; the replay's
// returned count is adopted. The authority creates no scheduler/pin/session
// state, drains no final response, logs nothing, and never touches custom
// fallback; every effect stays with the walker executing the typed action.
//
// Lifecycle: initial send -> build/success/cancelled terminal; exact-400
// corrective replay once (route-terminal, suppressed replay maps to faithful
// preservation of the original 400); non-transient stable classified once via
// the shared pure decision; transient observed to stable/limit/context via
// the single L1 authority (503 included, bounded by maxObservation, never a
// second L2 loop; 429 never enters L1 and never consumes the budget) then
// classified and decided the same way, with a post-L1 exact-400 replaying
// once as well. Cancel/deadline stops perform no further sends.
func (g *Gateway) recoverSingleCandidate(ctx context.Context, lane recoveryLane, tier Tier, protocol Protocol, maxObservation int, interval time.Duration, attempts *int, attemptOffset int, exec func(monitorAttempt int) attemptOutcome, replay func(final attemptOutcome, attempts int) (bool, *http.Response, error, int)) candidateRecovery {
	*attempts++
	syncAttemptMeta(ctx, tier, protocol, attemptOffset, *attempts)
	initial := exec(attemptOffset + *attempts)
	initResp, initErr := initial.Resp, initial.Err
	mk := func(action recoveryAction, final attemptOutcome, stop transientStopReason, cancelled bool, cause stableCause, mark bool, replayResp *http.Response, replayErr error) candidateRecovery {
		return candidateRecovery{Action: action, Final: final, Stop: stop, Cancelled: cancelled, Cause: cause, MarkUnavailable: mark, InitialResp: initResp, InitialErr: initErr, ReplayResp: replayResp, ReplayErr: replayErr}
	}
	if initial.BuildErr != nil {
		return mk(recoveryReturnBuild, initial, transientStopStable, isContextCancelled(ctx), stableCauseBuildFailure, false, nil, nil)
	}
	if initial.Err == nil && initial.Resp != nil && initial.Resp.StatusCode/100 == 2 {
		return mk(recoveryReturnSuccess, initial, transientStopStable, isContextCancelled(ctx), stableCauseSuccess, false, nil, nil)
	}
	if isContextCancelled(ctx) {
		cause := classifyStableCause(ctx, transientLoopResult{Final: initial, Stop: transientStopStable, InitialResp: initResp, InitialErr: initErr}).Cause
		return mk(recoveryReturnContext, initial, transientStopStable, true, cause, false, nil, nil)
	}
	if isRouteTerminalBadRequest(initial.Resp, initial.Err) {
		if handled, replayResp, replayErr, replayed := replay(initial, *attempts); handled {
			*attempts = replayed
			if replayResp != nil || replayErr != nil {
				return mk(recoveryReplayTerminal, initial, transientStopStable, isContextCancelled(ctx), stableCauseExact400, false, replayResp, replayErr)
			}
			return mk(recoveryFaithful, initial, transientStopStable, isContextCancelled(ctx), stableCauseExact400, false, nil, nil)
		}
	}
	if !isSameTargetTransient(initial.Resp, initial.Err) {
		classified := classifyStableCause(ctx, transientLoopResult{Final: initial, Stop: transientStopStable, InitialResp: initResp, InitialErr: initErr})
		decision := decideCandidateRecovery(lane, classified)
		if decision.Action == recoveryReplayTerminal {
			if handled, replayResp, replayErr, replayed := replay(classified.Final, *attempts); handled {
				*attempts = replayed
				if replayResp != nil || replayErr != nil {
					return mk(recoveryReplayTerminal, classified.Final, classified.Stop, classified.Cancelled, classified.Cause, false, replayResp, replayErr)
				}
				return mk(recoveryFaithful, classified.Final, classified.Stop, classified.Cancelled, classified.Cause, false, nil, nil)
			}
			return mk(recoveryFaithful, classified.Final, classified.Stop, classified.Cancelled, classified.Cause, false, nil, nil)
		}
		return mk(decision.Action, classified.Final, classified.Stop, classified.Cancelled, classified.Cause, decision.MarkUnavailable, nil, nil)
	}
	loopRes := g.observeSameTargetTransient(ctx, initial, tier, protocol, attempts, attemptOffset, maxObservation, interval, exec)
	initResp, initErr = loopRes.InitialResp, loopRes.InitialErr
	classified := classifyStableCause(ctx, loopRes)
	switch {
	case classified.Cause == stableCauseBuildFailure:
		return mk(recoveryReturnBuild, classified.Final, classified.Stop, classified.Cancelled, classified.Cause, false, nil, nil)
	case classified.Cause == stableCauseSuccess:
		return mk(recoveryReturnSuccess, classified.Final, classified.Stop, classified.Cancelled, classified.Cause, false, nil, nil)
	}
	if lane.Bound && !lane.Anonymous {
		if classified.Stop == transientStopContext || (classified.Stop == transientStopStable && classified.Cancelled) {
			return mk(recoveryReturnContext, classified.Final, classified.Stop, classified.Cancelled, classified.Cause, false, nil, nil)
		}
	} else if lane.Bound && lane.Anonymous {
		if classified.Cause == stableCauseContext || classified.Cancelled {
			return mk(recoveryReturnContext, classified.Final, classified.Stop, classified.Cancelled, classified.Cause, false, nil, nil)
		}
	} else if !lane.Bound && lane.Anonymous {
		if classified.Cause == stableCauseContext || classified.Cancelled {
			return mk(recoveryReturnContext, classified.Final, classified.Stop, classified.Cancelled, classified.Cause, false, nil, nil)
		}
	} else if classified.Cancelled {
		return mk(recoveryReturnContext, classified.Final, classified.Stop, classified.Cancelled, classified.Cause, false, nil, nil)
	}
	if classified.Cause == stableCauseExact400 {
		if handled, replayResp, replayErr, replayed := replay(classified.Final, *attempts); handled {
			*attempts = replayed
			if replayResp != nil || replayErr != nil {
				return mk(recoveryReplayTerminal, classified.Final, classified.Stop, classified.Cancelled, classified.Cause, false, replayResp, replayErr)
			}
		}
		return mk(recoveryFaithful, classified.Final, classified.Stop, classified.Cancelled, classified.Cause, false, nil, nil)
	}
	decision := decideCandidateRecovery(lane, classified)
	return mk(decision.Action, classified.Final, classified.Stop, classified.Cancelled, classified.Cause, decision.MarkUnavailable, nil, nil)
}
