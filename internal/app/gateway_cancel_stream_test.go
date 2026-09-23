package app

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// cancelStreamGateway is a single-anon-proxy gateway with an active custom
// channel, mirroring cancelPinGateway but for stream-ctx tests. The single
// proxy plus MaxAttempts=1 bounds every path to exactly one native send, so
// cancel timing is deterministic and follow-up sends are observable.
func cancelStreamGateway(t *testing.T, customHits *atomic.Int32) (*Gateway, *Monitor) {
	t.Helper()
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-cancel-stream")))
	}))
	t.Cleanup(custom.Close)
	cfg := testGatewayConfig(
		map[string][]string{
			"a": {"direct"},
			"z": {"direct", "http://127.0.0.1:8081"},
		},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm-cancel-stream"}}}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	monitor := NewMonitor()
	gw, err := NewGateway(normalized, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	return gw, monitor
}

func cancelStreamCtx() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true}))
}

func anonOnlyStreamRoute() modelRoute {
	route := anonAuthRoute()
	route.KeyTiers = nil
	return route
}

// cancelEOFBody cancels ctx on its first Read and then reports EOF, so the
// gate reaches a real pre-commit startup decision (NoteReadError is inline:
// no loop-top ctx recheck intervenes between the EOF and the failure flag)
// while executeAttempt observes the cancellation. A commit decision cannot be
// paired with cancel this way: frames need a next-iteration drain that
// rechecks ctx first, so a read-triggered cancel always wins before commit;
// the post-commit async race is guarded, not simulated (see above).
type cancelEOFBody struct {
	cancel context.CancelFunc
	fired  bool
}

func (b *cancelEOFBody) Read(_ []byte) (int, error) {
	if !b.fired {
		b.fired = true
		if b.cancel != nil {
			b.cancel()
		}
	}
	return 0, io.EOF
}

func (b *cancelEOFBody) Close() error { return nil }

// Gate-covered stream under pre-gate cancel (cancel-in-stub, the same mock
// strategy as the prior non-stream cancel tests): the gate never commits, the
// response is discarded for a cancel error, the single send is never followed
// up, and nothing is written or recorded. A cancel landing after a real
// commit is an async race with no deterministic seam (the gate stops reading
// the moment it commits, so a read-triggered cancel can never fire
// post-commit); that residual window is guarded by the commit-branch skip in
// executeAttempt and reported, not simulated here.
func TestCancelStreamCommitNoSchedulerWriteNoPin(t *testing.T) {
	var customHits atomic.Int32
	gw, monitor := cancelStreamGateway(t, &customHits)
	pool := gw.pools["a"]
	if pool == nil || len(pool.items) != 1 {
		t.Fatalf("anon pool items=%d want 1", len(pool.items))
	}

	ctx, cancel := cancelStreamCtx()
	var calls atomic.Int32
	postStub(t, gw, "a", 0, &calls, nil, func(*http.Request) (*http.Response, error) {
		cancel()
		return sseResponse(chatTextSSE("hi") + chatDoneSSE()), nil
	})
	ids := clientSessionIDs("ses_cancel_stream_commit")
	resp, _, attempts, err := gw.doUpstreamTiers(ctx, anonOnlyStreamRoute(), streamBodies(), ids, 0)
	if !errors.Is(err, context.Canceled) {
		if resp != nil {
			drainAndClose(resp.Body)
		}
		t.Fatalf("pre-gate cancel must return the cancel error: resp=%v err=%v", resp, err)
	}
	if resp != nil {
		drainAndClose(resp.Body)
		t.Fatalf("pre-gate cancel discards the undecided stream: resp=%v", resp)
	}
	if postCount(&calls) != 1 {
		t.Fatalf("cancelled stream must not send follow-ups: calls=%d want 1", postCount(&calls))
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1", attempts)
	}
	proxyRaw := pool.items[0].name
	ident := targetIdentity(TierZen, anonymousSchedulerCredentialID, "a", proxyRaw, "m")
	if _, _, ok := gw.scheduler.targetCooldownStatus(ident); ok {
		t.Fatalf("cancelled stream must not cool its target")
	}
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "a", proxyRaw); ok {
		t.Fatalf("cancelled stream must not write proxy429")
	}
	if _, ok := gw.scheduler.pinGet(ids.Session, "m"); ok {
		t.Fatalf("cancelled stream must not bind pin")
	}
	if _, ok := gw.scheduler.fallbacks.get(ids.Session); ok {
		t.Fatalf("cancelled stream must not bind custom fallback")
	}
	if customHits.Load() != 0 {
		t.Fatalf("cancelled stream must not hit custom (hits=%d)", customHits.Load())
	}
	if got := len(monitor.Snapshot().Upstream.Recent); got != 0 {
		t.Fatalf("cancel-path record policy unchanged (never recorded): recent=%d want 0", got)
	}
}

// executeAttempt-level pairing: with pre-gate cancel, pre-seeded
// target/proxy429 cooldowns (which a live success started after the seed
// would clear) survive and nothing is recorded; the uncancelled control with
// the same seeds commits, clears both, and records. The candidate is
// captured before seeding so eligibility filtering cannot interfere.
func TestCancelStreamCommitAttemptNoClear(t *testing.T) {
	var customHits atomic.Int32
	gw, monitor := cancelStreamGateway(t, &customHits)
	pool := gw.pools["a"]
	if pool == nil || len(pool.items) != 1 {
		t.Fatalf("anon pool items=%d want 1", len(pool.items))
	}
	proxyRaw := pool.items[0].name
	ident := targetIdentity(TierZen, anonymousSchedulerCredentialID, "a", proxyRaw, "m")
	cands := gw.scheduler.orderCandidates(gw.scheduler.buildAnonymousCandidates(pool, "m", time.Now().UnixNano()), "ses_cancel_stream_attempt")
	if len(cands) != 1 {
		t.Fatalf("candidates=%d want 1", len(cands))
	}
	cand := cands[0]
	seed := func() {
		now := time.Now().UnixNano()
		gw.scheduler.noteTargetFailure(ident, AttemptClassUpstreamFailure, http.StatusInternalServerError, 0)
		gw.scheduler.noteProxy429Failure(TierZen, "a", proxyRaw, AttemptClassRateLimited, http.StatusTooManyRequests, 0, now)
	}
	seed()
	if _, _, ok := gw.scheduler.targetCooldownStatus(ident); !ok {
		t.Fatalf("seeded target cooldown must be present")
	}
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "a", proxyRaw); !ok {
		t.Fatalf("seeded proxy429 cooldown must be present")
	}

	route := anonOnlyStreamRoute()
	ids := clientSessionIDs("ses_cancel_stream_attempt")
	ctx, cancel := cancelStreamCtx()
	var calls atomic.Int32
	postStub(t, gw, "a", 0, &calls, nil, func(*http.Request) (*http.Response, error) {
		cancel()
		return sseResponse(chatTextSSE("hi") + chatDoneSSE()), nil
	})
	out := gw.executeAttempt(ctx, route, TierZen, gw.cfg.Upstream.Zen, ProtocolChat, streamBodies()[TierZen], ids, cand, "rs-test", "anonymous", "anonymous", true, 1)
	if !errors.Is(out.Err, context.Canceled) || out.Resp != nil {
		if out.Resp != nil {
			drainAndClose(out.Resp.Body)
		}
		t.Fatalf("pre-gate cancel must return the cancel error: resp=%v err=%v", out.Resp, out.Err)
	}
	if postCount(&calls) != 1 {
		t.Fatalf("cancelled attempt must send exactly once: calls=%d want 1", postCount(&calls))
	}
	if until, _, ok := gw.scheduler.targetCooldownStatus(ident); !ok || until <= time.Now().UnixNano() {
		t.Fatalf("seeded target cooldown must survive cancelled attempt")
	}
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "a", proxyRaw); !ok {
		t.Fatalf("seeded proxy429 cooldown must survive cancelled attempt")
	}
	if _, ok := gw.scheduler.pinGet(ids.Session, "m"); ok {
		t.Fatalf("executeAttempt must never bind pins itself")
	}
	if customHits.Load() != 0 {
		t.Fatalf("executeAttempt must never hit custom (hits=%d)", customHits.Load())
	}
	if got := len(monitor.Snapshot().Upstream.Recent); got != 0 {
		t.Fatalf("cancel-path record policy unchanged (never recorded): recent=%d want 0", got)
	}

	// Sensitivity control: the same seeds with an uncancelled commit clear.
	ctx2 := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(chatTextSSE("hi") + chatDoneSSE()), nil
	})
	out2 := gw.executeAttempt(ctx2, route, TierZen, gw.cfg.Upstream.Zen, ProtocolChat, streamBodies()[TierZen], ids, cand, "rs-test", "anonymous", "anonymous", true, 2)
	if out2.Err != nil || out2.Resp == nil || out2.Resp.StatusCode != 200 {
		t.Fatalf("control commit must succeed: resp=%v err=%v", out2.Resp, out2.Err)
	}
	body, _ := io.ReadAll(out2.Resp.Body)
	drainAndClose(out2.Resp.Body)
	if !strings.Contains(string(body), "hi") {
		t.Fatalf("control commit must preserve bytes, got %q", body)
	}
	if _, _, ok := gw.scheduler.targetCooldownStatus(ident); ok {
		t.Fatalf("control commit must clear the seeded target cooldown")
	}
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "a", proxyRaw); ok {
		t.Fatalf("control commit must clear the seeded proxy429 cooldown")
	}
	if got := len(monitor.Snapshot().Upstream.Recent); got != 1 {
		t.Fatalf("control commit must record exactly its own attempt: recent=%d want 1", got)
	}
}

// Pre-commit startup failure under cancel: no target cooldown is written,
// the single send is never followed up, and no pin/fallback follows. The
// monitoring record keeps the existing policy (still one attempt).
func TestCancelStreamStartupNoCooldownNoFollowup(t *testing.T) {
	var customHits atomic.Int32
	gw, monitor := cancelStreamGateway(t, &customHits)
	pool := gw.pools["a"]
	if pool == nil || len(pool.items) != 1 {
		t.Fatalf("anon pool items=%d want 1", len(pool.items))
	}
	proxyRaw := pool.items[0].name
	ident := targetIdentity(TierZen, anonymousSchedulerCredentialID, "a", proxyRaw, "m")

	ctx, cancel := cancelStreamCtx()
	var calls atomic.Int32
	postStub(t, gw, "a", 0, &calls, nil, func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: &cancelEOFBody{cancel: cancel}}, nil
	})
	ids := clientSessionIDs("ses_cancel_stream_startup")
	resp, _, attempts, err := gw.doUpstreamTiers(ctx, anonOnlyStreamRoute(), streamBodies(), ids, 0)
	if err == nil {
		if resp != nil {
			drainAndClose(resp.Body)
		}
		t.Fatalf("cancelled startup failure must return its error, resp=%v", resp)
	}
	if resp != nil {
		drainAndClose(resp.Body)
	}
	if postCount(&calls) != 1 {
		t.Fatalf("cancelled startup must not send follow-ups: calls=%d want 1", postCount(&calls))
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1", attempts)
	}
	if _, _, ok := gw.scheduler.targetCooldownStatus(ident); ok {
		t.Fatalf("cancelled startup failure must not cool its target")
	}
	if _, ok := gw.scheduler.pinGet(ids.Session, "m"); ok {
		t.Fatalf("cancelled startup failure must not bind pin")
	}
	if _, ok := gw.scheduler.fallbacks.get(ids.Session); ok {
		t.Fatalf("cancelled startup failure must not bind custom fallback")
	}
	if customHits.Load() != 0 {
		t.Fatalf("cancelled startup failure must not hit custom (hits=%d)", customHits.Load())
	}
	if got := len(monitor.Snapshot().Upstream.Recent); got != 1 {
		t.Fatalf("startup record policy unchanged: recent=%d want 1", got)
	}
}
