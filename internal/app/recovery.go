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

// domainExhaustionKind distinguishes the four domain-level outcomes owned by
// the single domain recovery authority. NoExhaustion means no proof satisfied;
// the three exhausted kinds are distinct proofs, not merged counts.
type domainExhaustionKind int

const (
	domainNone domainExhaustionKind = iota
	domainUnboundExhausted
	domainPinnedFullLive429
	domainPinnedConsumption
)

// unboundDomainEvidence is the per-domain object-unavailable exhaustion proof
// for one frozen unbound recovery domain (anonymous lane or one credential).
// It is semantic evidence only: Entered reports whether the domain sent at
// least one live upstream attempt in this request; Frozen is the frozen
// candidate count; Unavailable counts distinct frozen candidates with live
// unavailable evidence in this request (live 429, 401/403 terminal, L1-final
// transport/408/425/5xx, or L1-final stream startup failure); Recovered400
// marks a 400 corrective-replay final for that domain.
type unboundDomainEvidence struct {
	Domain       string
	Entered      bool
	Frozen       int
	Unavailable  int
	Recovered400 bool
}

// pinnedDomainEvidence is the semantic evidence for one pinned binding's
// frozen eligible set. Eligible is the frozen eligible proxy count; ObservedLive429
// counts distinct live 429 evidence; TerminalStatus is the final HTTP status
// (0 for transport); Attempted counts really attempted frozen candidates;
// Consumed reports whether a real switch/send to another frozen eligible
// already occurred in this request; FinalIsObjectUnavailable reports the leaf
// finalNon429ObjectUnavailable classification of the last real send's outcome;
// FinalIsLive429 reports whether the final is an HTTP 429.
type pinnedDomainEvidence struct {
	Eligible                 int
	ObservedLive429          int
	TerminalStatus           int
	Attempted                int
	Consumed                 bool
	FinalIsObjectUnavailable bool
	FinalIsLive429           bool
}

// domainRecoveryInput is the typed semantic evidence for the single domain-
// level recovery decision authority. It contains no runtime stores, only
// evidence counts and stop gates. UnboundDomains non-nil means evaluating the
// unbound per-domain AND exhaustion; Pinned non-nil means evaluating the
// pinned two-proof exhaustion. At most one of the two is evaluated per call.
type domainRecoveryInput struct {
	Recovered400   bool
	Cancelled      bool
	Committed      bool
	UnboundDomains []unboundDomainEvidence
	Pinned         *pinnedDomainEvidence
}

// domainRecoveryResult is the typed domain-level outcome. Kind distinguishes
// no exhaustion from the three distinct exhausted proofs; AllowCustom reports
// whether a custom fallback selection is permitted after the L3 stop gates
// (cancelled/deadline/committed/recovered400). Exhaustion (Kind != domainNone)
// is the pure availability proof; AllowCustom is the stop-gated permission.
type domainRecoveryResult struct {
	Kind        domainExhaustionKind
	AllowCustom bool
}

// decideDomainRecovery is the single pure typed domain-level recovery decision
// authority. It owns all fallback/exhaustion policy split among the superseded
// helpers and returns a typed kind plus the stop-gated custom permission. Leaf
// evidence classifiers (finalNon429ObjectUnavailable etc.) remain in gateway.go;
// this authority never reads runtime stores, attempts, or scheduler state.
func decideDomainRecovery(in domainRecoveryInput) domainRecoveryResult {
	// Unbound per-domain AND exhaustion (state-agnostic, no terminal requirement).
	if in.UnboundDomains != nil {
		if len(in.UnboundDomains) == 0 {
			return domainRecoveryResult{Kind: domainNone, AllowCustom: false}
		}
		allExhausted := true
		for _, d := range in.UnboundDomains {
			if d.Recovered400 {
				allExhausted = false
				break
			}
			if !d.Entered || d.Frozen <= 0 || d.Unavailable < d.Frozen {
				allExhausted = false
				break
			}
		}
		if !allExhausted {
			return domainRecoveryResult{Kind: domainNone, AllowCustom: false}
		}
		allow := !in.Recovered400 && !in.Cancelled && !in.Committed
		return domainRecoveryResult{Kind: domainUnboundExhausted, AllowCustom: allow}
	}
	if in.Pinned != nil {
		p := in.Pinned
		// Full live-429 proof: every frozen eligible proxy supplied distinct live 429.
		full := p.Eligible > 0 && p.ObservedLive429 >= p.Eligible && p.TerminalStatus == 429
		// Bounded consumption proof: a real move/send already occurred, every
		// eligible was really attempted, final is non-429 object-unavailable.
		consumption := p.Consumed && p.Attempted >= p.Eligible && p.Eligible > 0 && !p.FinalIsLive429 && p.FinalIsObjectUnavailable
		var kind domainExhaustionKind
		if full {
			kind = domainPinnedFullLive429
		} else if consumption {
			kind = domainPinnedConsumption
		} else {
			return domainRecoveryResult{Kind: domainNone, AllowCustom: false}
		}
		allow := !in.Recovered400 && !in.Cancelled && !in.Committed
		return domainRecoveryResult{Kind: kind, AllowCustom: allow}
	}
	return domainRecoveryResult{Kind: domainNone, AllowCustom: false}
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
// Lifecycle: pre-send cancel check -> initial send -> build/success/cancelled
// terminal; exact-400 corrective replay once (route-terminal, suppressed
// replay maps to faithful preservation of the original 400); non-transient
// stable classified once via the shared pure decision; transient observed to
// stable/limit/context via the single L1 authority (503 included, bounded by
// maxObservation, never a second L2 loop; 429 never enters L1 and never
// consumes the budget) then classified and decided the same way, with a
// post-L1 exact-400 replaying once as well. Cancel/deadline stops perform no
// further sends. A ctx already cancelled before the initial send performs no
// send, no increment, no meta sync, and no replay: typed return-context with
// ctx.Err, transientStopContext, no fabricated Started/InitialResp, and no
// unavailable evidence. This promises only observable pre-send cancellation;
// it makes no atomicity claim against a racing wire dispatch.
func (g *Gateway) recoverSingleCandidate(ctx context.Context, lane recoveryLane, tier Tier, protocol Protocol, maxObservation int, interval time.Duration, attempts *int, attemptOffset int, exec func(monitorAttempt int) attemptOutcome, replay func(final attemptOutcome, attempts int) (bool, *http.Response, error, int)) candidateRecovery {
	if isContextCancelled(ctx) {
		ctxErr := context.Canceled
		if ctx != nil && ctx.Err() != nil {
			ctxErr = ctx.Err()
		}
		return candidateRecovery{Action: recoveryReturnContext, Final: attemptOutcome{Err: ctxErr}, Stop: transientStopContext, Cancelled: true, Cause: stableCauseContext, MarkUnavailable: false, InitialResp: nil, InitialErr: nil, ReplayResp: nil, ReplayErr: nil}
	}
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
	// Post-L1 context/replay eligibility reuses the unified pure decision:
	// the typed action decides whether the exact-400 corrective replay may
	// run, preserving build/success/context/replay priority and all lane
	// differences (including the pinned-auth strict observationLimit+
	// Cancelled gate). No parallel context gate lives here.
	decision := decideCandidateRecovery(lane, classified)
	if decision.Action == recoveryReplayTerminal {
		if handled, replayResp, replayErr, replayed := replay(classified.Final, *attempts); handled {
			*attempts = replayed
			if replayResp != nil || replayErr != nil {
				return mk(recoveryReplayTerminal, classified.Final, classified.Stop, classified.Cancelled, classified.Cause, false, replayResp, replayErr)
			}
		}
		return mk(recoveryFaithful, classified.Final, classified.Stop, classified.Cancelled, classified.Cause, false, nil, nil)
	}
	return mk(decision.Action, classified.Final, classified.Stop, classified.Cancelled, classified.Cause, decision.MarkUnavailable, nil, nil)
}
