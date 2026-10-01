package app

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"
)

// streamStartupBudget is the request-local startup-only timeout controller
// for true client streams. The startup budget (retry.timeout_seconds) covers
// waiting for response headers, empty/non-committing 200s, L1 observation and
// candidate/fallback recovery only until the first deliverable stream event
// passes the existing stream startup gate. After commit the budget is revoked
// and the remaining stream is bound only to the client context
// (client cancel / parent deadline), never to the startup deadline.
//
// Linearizability: pending->committed vs pending->expired has exactly one
// winner under the controller mutex. The timer path calls expire() (the same
// function production AfterFunc uses, so tests driving expire() directly
// exercise the identical linearization); the gate path calls tryCommit().
// Commit wins only while pending with a live context and stops all timers
// synchronously, so a winning commit never sees a later timer cancel.
// Expire wins only while pending and cancels once, so a winning expiry never
// creates a gated body, records success, or binds a pin. Parent cancellation
// always propagates via the WithCancel linkage; commit additionally refuses
// an already-cancelled context so a cancelled gate never fabricates success.
// No resurrection: once expired, tryCommit always fails; once committed,
// expire is a no-op. No WithoutCancel is used.
type streamStartupBudget struct {
	mu         sync.Mutex
	state      int // 0 pending, 1 committed, 2 expired
	timer      *time.Timer
	totalTimer *time.Timer
	cancel     context.CancelFunc
}

const (
	streamBudgetPending = iota
	streamBudgetCommitted
	streamBudgetExpired
)

type streamStartupBudgetKey struct{}

// newStreamStartupBudget returns a client-derived context whose cancel fires
// on parent cancel or, once after timeout, only while uncommitted. The caller
// must defer cancel and call stopStreamStartupBudget after establishment.
// The controller is always attached for stream requests (even timeout<=0,
// with no timer) so commit/expire linearize uniformly; direct executeAttempt
// callers without a controller keep the legacy compat path.
func newStreamStartupBudget(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc, func()) {
	ctx, cancel := context.WithCancel(parent)
	b := &streamStartupBudget{cancel: cancel}
	ctx = context.WithValue(ctx, streamStartupBudgetKey{}, b)
	if timeout > 0 {
		b.timer = time.AfterFunc(timeout, func() {
			b.expire()
		})
	}
	stop := func() {
		b.stopTimers()
	}
	return ctx, cancel, stop
}

// expire is the single timer authority. Production timers call it via
// AfterFunc; tests call it directly to exercise the identical linearization
// without wall-clock waits. It wins only from pending, exactly once.
func (b *streamStartupBudget) expire() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	if b.state != streamBudgetPending {
		b.mu.Unlock()
		return false
	}
	b.state = streamBudgetExpired
	cancel := b.cancel
	b.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return true
}

// tryCommit attempts pending->committed. It fails when already decided or
// when the context is already done (budget expiry or parent cancel), so an
// expired gate never creates a body and a cancelled gate never records
// success. On success it stops all timers synchronously, independent of the
// handler's deferred stop.
func (b *streamStartupBudget) tryCommit(ctx context.Context) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	if b.state != streamBudgetPending {
		b.mu.Unlock()
		return false
	}
	if ctx != nil && ctx.Err() != nil {
		b.mu.Unlock()
		return false
	}
	b.state = streamBudgetCommitted
	timer := b.timer
	total := b.totalTimer
	b.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	if total != nil {
		total.Stop()
	}
	return true
}

func (b *streamStartupBudget) stopTimers() {
	if b == nil {
		return
	}
	b.mu.Lock()
	timer := b.timer
	total := b.totalTimer
	b.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	if total != nil {
		total.Stop()
	}
}

func streamStartupBudgetFromCtx(ctx context.Context) *streamStartupBudget {
	if ctx == nil {
		return nil
	}
	b, _ := ctx.Value(streamStartupBudgetKey{}).(*streamStartupBudget)
	return b
}

// markStreamStartupCommitted revokes the startup budget for this request: a
// later startup-timer fire becomes a no-op and the stream continues under the
// client context only. It must be called synchronously at the gate commit
// decision, before any post-commit recording, so deadline-vs-commit
// linearizes on the controller. It reports whether this caller won the
// commit; false means expired/already-decided/cancelled and the caller must
// not create a gated body, record success, or bind a pin. A nil controller
// (legacy direct executeAttempt callers) reports true for compatibility;
// those paths still gate success recording on isContextCancelled.
func markStreamStartupCommitted(ctx context.Context) bool {
	b := streamStartupBudgetFromCtx(ctx)
	if b == nil {
		return true
	}
	return b.tryCommit(ctx)
}

// stopStreamStartupBudget disarms the startup timer(s) without cancelling the
// request. Safe to call multiple times; the handler's deferred cancel still
// owns final cleanup and client-cancel propagation is unaffected.
func stopStreamStartupBudget(ctx context.Context) {
	if b := streamStartupBudgetFromCtx(ctx); b != nil {
		b.stopTimers()
	}
}

// streamStartupExpired reports whether the request-local budget already lost
// the pending->expired race. Used to keep budget-timeout errors distinct
// from client cancellation.
func streamStartupExpired(ctx context.Context) bool {
	b := streamStartupBudgetFromCtx(ctx)
	if b == nil {
		return false
	}
	b.mu.Lock()
	expired := b.state == streamBudgetExpired
	b.mu.Unlock()
	return expired
}

// streamStartupBudgetErr preserves the timeout-vs-cancel distinction: a won
// budget expiry surfaces as DeadlineExceeded (matching non-stream WithTimeout
// semantics), parent/client cancellation surfaces as the context error.
// Callers returning gate/timeout errors must use this instead of ctx.Err()
// directly so a budget timeout is never misreported as a client cancel.
func streamStartupBudgetErr(ctx context.Context) error {
	if streamStartupExpired(ctx) {
		return context.DeadlineExceeded
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if ctx == nil {
		return context.Canceled
	}
	return ctx.Err()
}

// armStreamStartupTotalTimeout adds a request-local total bound on the same
// controller for custom non-SSE compat bodies (streaming requests answered
// with complete JSON/untyped). The startup timer alone cannot enforce the
// pre-existing shared-client total timeout once the Timeout-stripped clone
// has sent; this second AfterFunc reuses the identical pending->expired
// linearization (commit still revokes both, expiry cancels once). No global
// client mutation, no new config/proxy/persisted lifecycle. d<=0 or an
// already-decided controller arms nothing.
func armStreamStartupTotalTimeout(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return false
	}
	b := streamStartupBudgetFromCtx(ctx)
	if b == nil {
		return false
	}
	b.mu.Lock()
	if b.state != streamBudgetPending || b.totalTimer != nil {
		armed := b.totalTimer != nil && b.state == streamBudgetPending
		b.mu.Unlock()
		return armed
	}
	b.totalTimer = time.AfterFunc(d, func() {
		b.expire()
	})
	b.mu.Unlock()
	return true
}

// isCustomSSEStream reports whether a custom fallback response carries a real
// SSE stream. Only such responses enter the stream startup gate; complete
// JSON documents (existing custom compat for streaming requests answered
// with JSON) keep the pre-existing passthrough untouched. The check stays
// Content-Type scoped only; untyped real SSE keeps the compat bypass (with
// its total timeout, see armStreamStartupTotalTimeout) and is reported as a
// residual rather than widening protocol detection here.
func isCustomSSEStream(resp *http.Response) bool {
	if resp == nil {
		return false
	}
	return strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream")
}

// streamCustomClient returns a client for true-client streaming that never
// applies the shared custom channel total timeout: streaming lifetime is
// bounded by the startup budget before commit and by the client context after
// commit, never by http.Client.Timeout. The shared client is never mutated;
// a non-zero Timeout is stripped on a shallow copy preserving Transport and
// other attributes. Non-stream callers must keep the original client.
// Non-SSE compat bodies re-arm the original total via
// armStreamStartupTotalTimeout on the request controller after Do returns.
func streamCustomClient(base *http.Client) *http.Client {
	if base == nil {
		return &http.Client{}
	}
	if base.Timeout == 0 {
		return base
	}
	c := *base
	c.Timeout = 0
	return &c
}
