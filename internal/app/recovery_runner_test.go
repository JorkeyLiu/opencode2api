package app

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

// The shared candidate runner owns the single-candidate lifecycle across all
// four production walkers: exactly one initial send, one bounded L1
// observation, and at most one exact-400 corrective replay. These tests prove
// that lifecycle ownership (send/observation/replay counts, Started
// preservation, cancel stop, lane-divergent typed actions) rather than
// re-covering every status enum.
func TestRunnerSingleStableSend(t *testing.T) {
	gw := &Gateway{}
	sends := 0
	exec := func(monitorAttempt int) attemptOutcome {
		sends++
		return attemptOutcome{Resp: responseWithBody(429, `{"error":"t"}`)}
	}
	replay := func(final attemptOutcome, rel int) (bool, *http.Response, error, int) {
		t.Fatalf("stable 429 must never replay")
		return false, nil, nil, rel
	}
	attempts := 0
	rec := gw.recoverSingleCandidate(context.Background(), recoveryLane{Bound: true, Anonymous: false}, TierZen, ProtocolChat, 3, 0, &attempts, 0, exec, replay)
	defer drainResp(rec.Final.Resp)
	if sends != 1 || attempts != 1 {
		t.Fatalf("stable outcome must send once: sends=%d attempts=%d", sends, attempts)
	}
	if rec.Action != recoveryLive429 || rec.Cause != stableCauseLive429 {
		t.Fatalf("pinned 429 must stash: action=%v cause=%v", rec.Action, rec.Cause)
	}
	// Same observation on the unbound lane advances with mark instead.
	attempts = 0
	sends = 0
	exec2 := func(monitorAttempt int) attemptOutcome {
		sends++
		return attemptOutcome{Resp: responseWithBody(429, `{"error":"t"}`)}
	}
	rec2 := gw.recoverSingleCandidate(context.Background(), recoveryLane{Bound: false, Anonymous: false}, TierZen, ProtocolChat, 3, 0, &attempts, 0, exec2, replay)
	defer drainResp(rec2.Final.Resp)
	if sends != 1 || rec2.Action != recoveryAdvanceNext || !rec2.MarkUnavailable {
		t.Fatalf("unbound 429 must advance+mark in one send: sends=%d %+v", sends, rec2)
	}
}

func TestRunnerL1BoundedObservation(t *testing.T) {
	gw := &Gateway{}
	sends := 0
	exec := func(monitorAttempt int) attemptOutcome {
		sends++
		return attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`)}
	}
	replay := func(final attemptOutcome, rel int) (bool, *http.Response, error, int) {
		t.Fatalf("503 must never replay")
		return false, nil, nil, rel
	}
	attempts := 0
	rec := gw.recoverSingleCandidate(context.Background(), recoveryLane{Bound: false, Anonymous: true}, TierZen, ProtocolChat, 3, 0, &attempts, 0, exec, replay)
	defer drainResp(rec.Final.Resp)
	defer drainResp(rec.InitialResp)
	if sends != 3 || attempts != 3 {
		t.Fatalf("L1 must observe exactly maxObservation sends: sends=%d attempts=%d", sends, attempts)
	}
	if rec.Action != recoveryAdvanceNext || !rec.MarkUnavailable || rec.Stop != transientStopObservationLimit {
		t.Fatalf("L1-final 503 must advance+mark at the limit: %+v", rec)
	}
	if rec.InitialResp == nil || rec.Final.Resp == nil {
		t.Fatalf("runner must preserve initial+final for ownership: %+v", rec)
	}
}

func TestRunnerExact400ReplaysOnceTerminal(t *testing.T) {
	gw := &Gateway{}
	sends := 0
	replays := 0
	exec := func(monitorAttempt int) attemptOutcome {
		sends++
		return attemptOutcome{Resp: responseWithBody(400, `{"error":"bad"}`)}
	}
	replay := func(final attemptOutcome, rel int) (bool, *http.Response, error, int) {
		replays++
		if !isRouteTerminalBadRequest(final.Resp, final.Err) {
			t.Fatalf("replay must see the exact 400 final")
		}
		drainResp(final.Resp)
		return true, responseWithBody(200, `{"ok":true}`), nil, rel + 1
	}
	attempts := 0
	rec := gw.recoverSingleCandidate(context.Background(), recoveryLane{Bound: false, Anonymous: true}, TierZen, ProtocolChat, 3, 0, &attempts, 0, exec, replay)
	defer drainResp(rec.ReplayResp)
	if sends != 1 || replays != 1 || attempts != 2 {
		t.Fatalf("exact 400 must replay exactly once: sends=%d replays=%d attempts=%d", sends, replays, attempts)
	}
	if rec.Action != recoveryReplayTerminal || rec.ReplayResp == nil || rec.ReplayResp.StatusCode != 200 {
		t.Fatalf("replay result must be route-terminal: %+v", rec)
	}
}

func TestRunnerLaneDivergenceOnTransportFinal(t *testing.T) {
	gw := &Gateway{}
	transportErr := errors.New("dial timeout")
	mkExec := func(sends *int) func(int) attemptOutcome {
		return func(monitorAttempt int) attemptOutcome {
			*sends++
			if *sends == 1 {
				return attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`)}
			}
			return attemptOutcome{Err: transportErr}
		}
	}
	replay := func(final attemptOutcome, rel int) (bool, *http.Response, error, int) {
		t.Fatalf("transport final must never replay")
		return false, nil, nil, rel
	}
	// Pinned-auth walks the next proxy; pinned-anonymous stays faithful;
	// unbound advances with mark. Same observation, three typed actions.
	var sendsA, sendsB, sendsC int
	attempts := 0
	recA := gw.recoverSingleCandidate(context.Background(), recoveryLane{Bound: true, Anonymous: false}, TierZen, ProtocolChat, 2, 0, &attempts, 0, mkExec(&sendsA), replay)
	if recA.Action != recoveryWalkNext {
		drainResp(recA.InitialResp)
		t.Fatalf("pinned-auth transport final must walk: %+v", recA)
	}
	drainResp(recA.InitialResp)
	attempts = 0
	recB := gw.recoverSingleCandidate(context.Background(), recoveryLane{Bound: true, Anonymous: true}, TierZen, ProtocolChat, 2, 0, &attempts, 0, mkExec(&sendsB), replay)
	if recB.Action != recoveryFaithful {
		t.Fatalf("pinned-anon transport final must stay faithful: %+v", recB)
	}
	attempts = 0
	recC := gw.recoverSingleCandidate(context.Background(), recoveryLane{Bound: false, Anonymous: false}, TierZen, ProtocolChat, 2, 0, &attempts, 0, mkExec(&sendsC), replay)
	if recC.Action != recoveryAdvanceNext || !recC.MarkUnavailable {
		t.Fatalf("unbound transport final must advance+mark: %+v", recC)
	}
	if sendsA != 2 || sendsB != 2 || sendsC != 2 {
		t.Fatalf("each lane must run the same 2-send observation: %d %d %d", sendsA, sendsB, sendsC)
	}
}

func TestRunnerCancelledStableNoMarkOrAdvance(t *testing.T) {
	// Cancelled initial stable failures (429/401/403) stop before any walker
	// mark: one send, no L1, no replay, return-context with no unavailable
	// evidence. Walkers return on this action before mark/fence/custom, so
	// no next candidate, no request-local evidence, and no erroneous custom.
	// Scheduler writes are already cancel-gated inside sendUpstreamOnce via
	// applyAttemptOutcome, so the only HEAD-vs-current delta is the discarded
	// request-local cred429Evidence entry HEAD left for a cancelled 429.
	cases := []struct {
		name   string
		status int
		cause  stableCause
	}{
		{name: "live429", status: 429, cause: stableCauseLive429},
		{name: "stable401", status: 401, cause: stableCauseL1Final},
		{name: "target403", status: 403, cause: stableCauseTargetForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gw := &Gateway{}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			sends := 0
			exec := func(monitorAttempt int) attemptOutcome {
				sends++
				return attemptOutcome{Resp: responseWithBody(tc.status, `{"error":"x"}`)}
			}
			replay := func(final attemptOutcome, rel int) (bool, *http.Response, error, int) {
				t.Fatalf("%s with cancelled ctx must never replay", tc.name)
				return false, nil, nil, rel
			}
			attempts := 0
			rec := gw.recoverSingleCandidate(ctx, recoveryLane{Bound: false, Anonymous: false}, TierZen, ProtocolChat, 5, time.Second, &attempts, 0, exec, replay)
			defer drainResp(rec.Final.Resp)
			if sends != 1 || attempts != 1 {
				t.Fatalf("%s cancelled must send exactly once: sends=%d attempts=%d", tc.name, sends, attempts)
			}
			if rec.Action != recoveryReturnContext || rec.MarkUnavailable {
				t.Fatalf("%s cancelled must return-context with no mark: %+v", tc.name, rec)
			}
			if rec.Cause != tc.cause || !rec.Cancelled {
				t.Fatalf("%s cancelled must keep cause and cancelled flag: %+v", tc.name, rec)
			}
		})
	}
}

func TestRunnerPostL1Exact400UsesUnifiedDecision(t *testing.T) {
	// Post-L1 exact-400 eligibility comes from the unified pure decision:
	// allowed replays once terminal, late-cancelled returns context without
	// replay. Pinned-auth lane also guards the strict stable+cancelled gate.
	gw := &Gateway{}
	lane := recoveryLane{Bound: true, Anonymous: false}
	execAllowed := func() func(int) attemptOutcome {
		sends := 0
		return func(monitorAttempt int) attemptOutcome {
			sends++
			if sends == 1 {
				return attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`)}
			}
			return attemptOutcome{Resp: responseWithBody(400, `{"error":"bad"}`)}
		}
	}()
	replays := 0
	replayAllowed := func(final attemptOutcome, rel int) (bool, *http.Response, error, int) {
		replays++
		if !isRouteTerminalBadRequest(final.Resp, final.Err) {
			t.Fatalf("post-L1 replay must see the exact 400 final")
		}
		drainResp(final.Resp)
		return true, responseWithBody(200, `{"ok":true}`), nil, rel + 1
	}
	attempts := 0
	rec := gw.recoverSingleCandidate(context.Background(), lane, TierZen, ProtocolChat, 3, 0, &attempts, 0, execAllowed, replayAllowed)
	defer drainResp(rec.ReplayResp)
	if replays != 1 || rec.Action != recoveryReplayTerminal {
		t.Fatalf("post-L1 exact 400 must replay once terminal via unified decision: %+v replays=%d", rec, replays)
	}
	// Late-cancelled post-L1 exact 400 stays context via the same decision.
	ctx, cancel := context.WithCancel(context.Background())
	attemptsBlocked := 0
	execBlocked := func(monitorAttempt int) attemptOutcome {
		if attemptsBlocked == 1 {
			cancel()
			return attemptOutcome{Resp: responseWithBody(400, `{"error":"bad"}`)}
		}
		attemptsBlocked++
		return attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`)}
	}
	attempts = 0
	replayBlocked := func(final attemptOutcome, rel int) (bool, *http.Response, error, int) {
		t.Fatalf("late-cancelled post-L1 exact 400 must never replay")
		return false, nil, nil, rel
	}
	recBlocked := gw.recoverSingleCandidate(ctx, lane, TierZen, ProtocolChat, 3, 0, &attempts, 0, execBlocked, replayBlocked)
	defer drainResp(recBlocked.Final.Resp)
	if recBlocked.Action != recoveryReturnContext || !recBlocked.Cancelled {
		t.Fatalf("late-cancelled post-L1 exact 400 must return context: %+v", recBlocked)
	}
	if recBlocked.Cause != stableCauseExact400 {
		t.Fatalf("cancel must not rewrite the classified cause: %+v", recBlocked)
	}
}

func TestRunnerStartedPreservedAndCancelStops(t *testing.T) {
	gw := &Gateway{}
	const started int64 = 123456789
	exec := func(monitorAttempt int) attemptOutcome {
		return attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`), Started: started}
	}
	replay := func(final attemptOutcome, rel int) (bool, *http.Response, error, int) {
		return false, nil, nil, rel
	}
	attempts := 0
	rec := gw.recoverSingleCandidate(context.Background(), recoveryLane{Bound: true, Anonymous: false}, TierZen, ProtocolChat, 1, 0, &attempts, 0, exec, replay)
	defer drainResp(rec.Final.Resp)
	if rec.Final.Started != started {
		t.Fatalf("final.Started must survive the runner for credential429 fencing: %d", rec.Final.Started)
	}
	// Cancelled context stops with a single send and no observation loop.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sends := 0
	execCancel := func(monitorAttempt int) attemptOutcome {
		sends++
		return attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`)}
	}
	attempts = 0
	recCancel := gw.recoverSingleCandidate(ctx, recoveryLane{Bound: false, Anonymous: false}, TierZen, ProtocolChat, 5, time.Second, &attempts, 0, execCancel, replay)
	defer drainResp(recCancel.Final.Resp)
	if sends != 1 || recCancel.Action != recoveryReturnContext {
		t.Fatalf("cancel must stop after the initial send: sends=%d action=%v", sends, recCancel.Action)
	}
}
