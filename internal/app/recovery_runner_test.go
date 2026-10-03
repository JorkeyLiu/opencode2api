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
	// Pinned-auth and pinned-anon both walk the next proxy (unified bound
	// walk); unbound advances with mark. Same observation, two walk actions
	// plus advance.
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
	if recB.Action != recoveryWalkNext {
		t.Fatalf("pinned-anon transport final must walk (unified): %+v", recB)
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
	// Pre-cancelled runner performs zero sends: no increment, no meta sync,
	// no exec, no L1, no replay, typed return-context with ctx.Err and
	// transientStopContext, no fabricated Started/InitialResp, no unavailable
	// evidence. Walkers return on this action before mark/fence/custom, so
	// no next candidate, no request-local evidence, and no erroneous custom.
	cases := []struct {
		name   string
		status int
	}{
		{name: "live429", status: 429},
		{name: "stable401", status: 401},
		{name: "target403", status: 403},
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
			attempts := 7
			rec := gw.recoverSingleCandidate(ctx, recoveryLane{Bound: false, Anonymous: false}, TierZen, ProtocolChat, 5, time.Second, &attempts, 0, exec, replay)
			if sends != 0 {
				t.Fatalf("%s pre-cancelled must send zero times: sends=%d", tc.name, sends)
			}
			if attempts != 7 {
				t.Fatalf("%s pre-cancelled must preserve attempts: attempts=%d want 7", tc.name, attempts)
			}
			if rec.Action != recoveryReturnContext || rec.MarkUnavailable {
				t.Fatalf("%s cancelled must return-context with no mark: %+v", tc.name, rec)
			}
			if rec.Cause != stableCauseContext || !rec.Cancelled {
				t.Fatalf("%s pre-cancelled must report context cause and cancelled flag: %+v", tc.name, rec)
			}
			if rec.Stop != transientStopContext {
				t.Fatalf("%s pre-cancelled must stop with context: %+v", tc.name, rec)
			}
			if rec.Final.Resp != nil || rec.Final.Err != context.Canceled {
				t.Fatalf("%s pre-cancelled must carry ctx err with no response: resp=%v err=%v", tc.name, rec.Final.Resp, rec.Final.Err)
			}
			if rec.Final.Started != 0 || rec.InitialResp != nil || rec.InitialErr != nil {
				t.Fatalf("%s pre-cancelled must not fabricate Started/InitialResp: %+v", tc.name, rec)
			}
			if rec.ReplayResp != nil || rec.ReplayErr != nil {
				t.Fatalf("%s pre-cancelled must not replay: %+v", tc.name, rec)
			}
		})
	}
}

func TestRunnerCancelDuringExecPreservesResponseNoAdvance(t *testing.T) {
	// Cancel-during-exec keeps the real response/Started identity: the send
	// already happened because ctx was live at entry, but no mark/advance/
	// replay follows once cancellation is observable. This retains the
	// pre-send-check coverage that the real outcome is not rewritten.
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
			const started int64 = 987654321
			sends := 0
			exec := func(monitorAttempt int) attemptOutcome {
				sends++
				cancel()
				return attemptOutcome{Resp: responseWithBody(tc.status, `{"error":"x"}`), Started: started}
			}
			replay := func(final attemptOutcome, rel int) (bool, *http.Response, error, int) {
				t.Fatalf("%s cancelled during exec must never replay", tc.name)
				return false, nil, nil, rel
			}
			attempts := 7
			rec := gw.recoverSingleCandidate(ctx, recoveryLane{Bound: false, Anonymous: false}, TierZen, ProtocolChat, 5, time.Second, &attempts, 0, exec, replay)
			defer drainResp(rec.Final.Resp)
			defer drainResp(rec.InitialResp)
			if sends != 1 || attempts != 8 {
				t.Fatalf("%s cancel-during-exec must send once: sends=%d attempts=%d", tc.name, sends, attempts)
			}
			if rec.Action != recoveryReturnContext || rec.MarkUnavailable {
				t.Fatalf("%s cancelled must return-context with no mark: %+v", tc.name, rec)
			}
			if rec.Cause != tc.cause || !rec.Cancelled {
				t.Fatalf("%s cancelled must keep classified cause and cancelled flag: %+v", tc.name, rec)
			}
			if rec.Final.Resp == nil || rec.Final.Resp.StatusCode != tc.status {
				t.Fatalf("%s must preserve the real response identity: %+v", tc.name, rec)
			}
			if rec.Final.Started != started {
				t.Fatalf("%s must preserve Started: %d", tc.name, rec.Final.Started)
			}
			if rec.InitialResp != rec.Final.Resp {
				t.Fatalf("%s InitialResp must match the real send: %+v", tc.name, rec)
			}
		})
	}
}

func TestRunnerPreSendCancelZeroSendFourLanes(t *testing.T) {
	// Pre-cancel and expired deadline across all four lanes with nonzero
	// initial attempts: zero sends, attempts/meta preserved, no replay or
	// side effects, typed return-context with transientStopContext.
	lanes := []struct {
		name string
		lane recoveryLane
	}{
		{name: "unbound-anon", lane: recoveryLane{Bound: false, Anonymous: true}},
		{name: "unbound-auth", lane: recoveryLane{Bound: false, Anonymous: false}},
		{name: "pinned-anon", lane: recoveryLane{Bound: true, Anonymous: true}},
		{name: "pinned-auth", lane: recoveryLane{Bound: true, Anonymous: false}},
	}
	ctxKinds := []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
		want error
	}{
		{name: "pre-cancel", ctx: func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx, cancel
		}, want: context.Canceled},
		{name: "expired-deadline", ctx: func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			return ctx, cancel
		}, want: context.DeadlineExceeded},
	}
	for _, ln := range lanes {
		for _, ck := range ctxKinds {
			t.Run(ln.name+"/"+ck.name, func(t *testing.T) {
				gw := &Gateway{}
				ctx, cancel := ck.ctx()
				defer cancel()
				if ctx.Err() == nil {
					t.Fatalf("test ctx must already be done")
				}
				meta := &requestMeta{Attempts: 99}
				ctx = context.WithValue(ctx, requestMetaKey{}, meta)
				sends := 0
				exec := func(monitorAttempt int) attemptOutcome {
					sends++
					t.Fatalf("pre-send cancelled must never exec")
					return attemptOutcome{}
				}
				replay := func(final attemptOutcome, rel int) (bool, *http.Response, error, int) {
					t.Fatalf("pre-send cancelled must never replay")
					return false, nil, nil, rel
				}
				attempts := 7
				rec := gw.recoverSingleCandidate(ctx, ln.lane, TierZen, ProtocolChat, 5, time.Second, &attempts, 10, exec, replay)
				if sends != 0 {
					t.Fatalf("pre-send cancelled must send zero times: sends=%d", sends)
				}
				if attempts != 7 {
					t.Fatalf("pre-send cancelled must preserve attempts: got %d want 7", attempts)
				}
				if meta.Attempts != 99 {
					t.Fatalf("pre-send cancelled must not sync meta: got %d want 99", meta.Attempts)
				}
				if rec.Action != recoveryReturnContext || rec.MarkUnavailable {
					t.Fatalf("pre-send cancelled must return-context with no mark: %+v", rec)
				}
				if rec.Stop != transientStopContext || !rec.Cancelled || rec.Cause != stableCauseContext {
					t.Fatalf("pre-send cancelled must report context stop/cause: %+v", rec)
				}
				if rec.Final.Resp != nil || rec.Final.Err != ck.want {
					t.Fatalf("pre-send cancelled must carry ctx err with no response: resp=%v err=%v want %v", rec.Final.Resp, rec.Final.Err, ck.want)
				}
				if rec.Final.Started != 0 || rec.InitialResp != nil || rec.InitialErr != nil {
					t.Fatalf("pre-send cancelled must not fabricate Started/InitialResp: %+v", rec)
				}
				if rec.ReplayResp != nil || rec.ReplayErr != nil {
					t.Fatalf("pre-send cancelled must not replay: %+v", rec)
				}
			})
		}
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
	// Pre-cancelled context performs zero sends and no observation loop.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sends := 0
	execCancel := func(monitorAttempt int) attemptOutcome {
		sends++
		return attemptOutcome{Resp: responseWithBody(503, `{"error":"svc"}`)}
	}
	attempts = 7
	recCancel := gw.recoverSingleCandidate(ctx, recoveryLane{Bound: false, Anonymous: false}, TierZen, ProtocolChat, 5, time.Second, &attempts, 0, execCancel, replay)
	if sends != 0 || attempts != 7 {
		t.Fatalf("pre-cancel must send zero times and preserve attempts: sends=%d attempts=%d", sends, attempts)
	}
	if recCancel.Action != recoveryReturnContext || recCancel.Stop != transientStopContext {
		t.Fatalf("pre-cancel must return context: %+v", recCancel)
	}
	if recCancel.Final.Resp != nil || recCancel.Final.Err != context.Canceled {
		t.Fatalf("pre-cancel must carry ctx err with no response: %+v", recCancel)
	}
}
