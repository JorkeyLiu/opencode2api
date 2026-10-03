package app

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// closeObservingBody wraps a reader and records Close calls. It is the
// tail-liveness sensor: a committed winner must never observe a close from
// the losing timer, while an expiry winner must close exactly once to unblock
// the gate read.
type closeObservingBody struct {
	r      io.Reader
	closed atomic.Bool
	closes atomic.Int32
}

func (b *closeObservingBody) Read(p []byte) (int, error) {
	if b == nil || b.r == nil {
		return 0, io.EOF
	}
	return b.r.Read(p)
}

func (b *closeObservingBody) Close() error {
	if b == nil {
		return nil
	}
	b.closed.Store(true)
	b.closes.Add(1)
	if c, ok := b.r.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// Expire-before-commit through the exact shared authority both production
// paths use: expiry wins once, closes the stalled body, and every later
// commit is rejected so no success/pin/body handoff may follow.
func TestCandidateGate_ExpireBeforeCommitClosesRejectsCommit(t *testing.T) {
	t.Parallel()
	pr, pw := io.Pipe()
	body := &closeObservingBody{r: pr}
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	g := newCandidateStartupGate(time.Hour, ctx, body)
	if !g.tryExpire(ctx) {
		t.Fatalf("directed expire must win")
	}
	if !body.closed.Load() {
		t.Fatalf("expiry winner must close the stalled body")
	}
	if g.tryCommit(ctx) {
		t.Fatalf("commit after expiry must fail")
	}
	if !g.expired() {
		t.Fatalf("expired flag must hold")
	}
	if g.tryExpire(ctx) {
		t.Fatalf("second expire must not re-win")
	}
	if body.closes.Load() != 1 {
		t.Fatalf("body closes=%d want 1", body.closes.Load())
	}
	_ = pw.Close()
}

// Commit-before-expiration through the same authority: commit wins once and
// stops the timer; a later (even already-entered) expiry loses and must
// never close the committed tail.
func TestCandidateGate_CommitBeforeExpireKeepsTailOpen(t *testing.T) {
	t.Parallel()
	pr, pw := io.Pipe()
	defer pw.Close()
	body := &closeObservingBody{r: pr}
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	g := newCandidateStartupGate(time.Hour, ctx, body)
	if !g.tryCommit(ctx) {
		t.Fatalf("directed commit must win")
	}
	// Simulate an already-entered timer callback racing a won commit.
	if g.tryExpire(ctx) {
		t.Fatalf("expire after commit must lose")
	}
	if g.expired() {
		t.Fatalf("commit-win must not read expired")
	}
	if body.closed.Load() {
		t.Fatalf("losing expiry must never close the committed tail")
	}
	g.stop()
}

// Simultaneous race hammer through the exact production authority: N
// committers vs M expirers on one gate. Exactly one transition wins;
// body-closed iff expiry won, commit-won iff not expired.
func TestCandidateGate_ConcurrentHammerSingleWinner(t *testing.T) {
	t.Parallel()
	for i := 0; i < 200; i++ {
		pr, pw := io.Pipe()
		body := &closeObservingBody{r: pr}
		ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
		g := newCandidateStartupGate(time.Hour, ctx, body)
		var commitWins atomic.Int32
		var expireWins atomic.Int32
		var wg sync.WaitGroup
		for k := 0; k < 4; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if g.tryCommit(ctx) {
					commitWins.Add(1)
				}
			}()
		}
		for k := 0; k < 4; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if g.tryExpire(ctx) {
					expireWins.Add(1)
				}
			}()
		}
		wg.Wait()
		cw, ew := commitWins.Load(), expireWins.Load()
		if cw > 1 || ew > 1 {
			_ = pw.Close()
			t.Fatalf("iter %d: commitWins=%d expireWins=%d want <=1 each", i, cw, ew)
		}
		if cw == 1 && ew == 1 {
			_ = pw.Close()
			t.Fatalf("iter %d: commit and expire both won", i)
		}
		if cw == 0 && ew == 0 {
			_ = pw.Close()
			t.Fatalf("iter %d: neither side won", i)
		}
		if ew == 1 && !body.closed.Load() {
			_ = pw.Close()
			t.Fatalf("iter %d: expiry winner must close the body", i)
		}
		if cw == 1 && body.closed.Load() {
			_ = pw.Close()
			t.Fatalf("iter %d: losing expiry closed the committed tail", i)
		}
		if cw == 1 && g.expired() {
			_ = pw.Close()
			t.Fatalf("iter %d: commit-win must not read expired", i)
		}
		if ew == 1 && g.tryCommit(ctx) {
			_ = pw.Close()
			t.Fatalf("iter %d: commit after expiry must fail", i)
		}
		_ = pw.Close()
		g.stop()
	}
}

// Parent-done callers lose on both sides so cancellation/final-budget stays
// owner under the callers' existing guard ordering.
func TestCandidateGate_ParentDoneLosesBothSides(t *testing.T) {
	t.Parallel()
	pr, pw := io.Pipe()
	defer pw.Close()
	body := &closeObservingBody{r: pr}
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	g := newCandidateStartupGate(time.Hour, parent, body)
	if g.tryCommit(parent) {
		t.Fatalf("commit on cancelled parent must fail")
	}
	if g.tryExpire(parent) {
		t.Fatalf("expire on cancelled parent must lose")
	}
	if g.expired() {
		t.Fatalf("parent cancel must not read as candidate expiry")
	}
	if body.closed.Load() {
		t.Fatalf("parent-done expiry must not close the body")
	}
	g.stop()
}

// Timer-driven expiry through the production AfterFunc path closes the body
// and rejects commit; a commit that wins first disarms the timer so the tail
// stays open past the deadline.
func TestCandidateGate_TimerPathClosesOnlyOnExpiryWin(t *testing.T) {
	t.Parallel()
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	// Expiry wins: short timer fires while uncommitted.
	pr, _ := io.Pipe()
	body := &closeObservingBody{r: pr}
	g := newCandidateStartupGate(20*time.Millisecond, ctx, body)
	deadline := time.Now().Add(2 * time.Second)
	for !g.expired() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !g.expired() {
		t.Fatalf("short timer must expire while uncommitted")
	}
	if !body.closed.Load() {
		t.Fatalf("timer expiry winner must close the body")
	}
	if g.tryCommit(ctx) {
		t.Fatalf("commit after timer expiry must fail")
	}
	g.stop()

	// Commit wins: immediate commit disarms the short timer.
	pr2, pw2 := io.Pipe()
	defer pw2.Close()
	body2 := &closeObservingBody{r: pr2}
	g2 := newCandidateStartupGate(30*time.Millisecond, ctx, body2)
	if !g2.tryCommit(ctx) {
		t.Fatalf("immediate commit must win")
	}
	time.Sleep(80 * time.Millisecond)
	if g2.expired() {
		t.Fatalf("committed gate must not expire after the deadline")
	}
	if body2.closed.Load() {
		t.Fatalf("timer must not close the committed tail")
	}
	g2.stop()
}

// Native production sequence: immediate commit with a 2.1s tail past the 1s
// candidate budget succeeds end-to-end through executeAttempt/doUpstreamTiers
// (the committed winner's tail is never closed by the candidate timer).
func TestCandidateGate_NativeLongTailSurvivesCandidateBudget(t *testing.T) {
	gw := customTakeoverTestGateway(t, nil, false)
	gw.cfg.Retry.AttemptTimeoutSeconds = 1
	gw.cfg.Retry.TimeoutSeconds = 10
	gw.cfg.Retry.MaxAttempts = 3
	cred := gw.authCreds[0]
	ses := "ses_candgate_native_tail_1"
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", gw.pools["z"].items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	idx := poolIndexByRaw(gw, "z", pin.ProxyRaw)
	postStub(t, gw, "z", idx, nil, nil, func(r *http.Request) (*http.Response, error) {
		pr, pw := io.Pipe()
		go func() {
			_, _ = pw.Write([]byte(chatTextSSE("hi")))
			select {
			case <-time.After(2100 * time.Millisecond):
				_, _ = pw.Write([]byte(chatTextSSE("tail") + chatDoneSSE()))
				_ = pw.Close()
			case <-r.Context().Done():
				_ = pw.CloseWithError(r.Context().Err())
			}
		}()
		h := make(http.Header)
		h.Set("Content-Type", "text/event-stream")
		return &http.Response{StatusCode: 200, Header: h, Body: pr}, nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	begin := time.Now()
	resp, _, _, err := gw.doUpstreamTiers(ctx, authOnlyRoute(), streamBodies(), pinIDs(ses, "r1"), 0, consumptionExtra())
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("committed tail must succeed, err=%v resp=%v", err, resp)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	elapsed := time.Since(begin)
	drainResp(resp)
	if err != nil || !strings.Contains(string(body), "tail") {
		t.Fatalf("tail must survive past candidate budget, err=%v body=%q", err, string(body))
	}
	if elapsed < 2*time.Second {
		t.Fatalf("elapsed=%v must cross the 1s candidate budget", elapsed)
	}
}

// Custom production sequence: same committed-tail survival through the custom
// SSE gate with the candidate budget (1s) well below the overall startup
// budget (10s), so survival proves the candidate timer was revoked on win.
func TestCandidateGate_CustomLongTailSurvivesCandidateBudget(t *testing.T) {
	var customHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customHits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		fl, _ := w.(http.Flusher)
		_, _ = w.Write([]byte(chatTextSSE("hello")))
		if fl != nil {
			fl.Flush()
		}
		select {
		case <-time.After(2100 * time.Millisecond):
			_, _ = w.Write([]byte(chatTextSSE("world") + chatDoneSSE()))
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	ch := FallbackChannelConfig{ID: "c1", Name: "c1", BaseURL: srv.URL, APIKey: "k1", Model: "cm-candgate-tail", Protocol: ProtocolChat}
	cfg := testGatewayConfig(map[string][]string{"z": {"direct"}}, ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"})
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 10
	cfg.Retry.AttemptTimeoutSeconds = 1
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	gw.catalog.ReplaceWithCapabilities([]string{"m"}, nil,
		map[Tier]map[string]Protocol{TierZen: {"m": ProtocolChat}},
		map[Tier]map[string]bool{},
		map[Tier]map[string]ModelMetadata{TierZen: {"m": {ContextWindow: 1000}}})
	gw.cfg.ServerKeys = []string{"test-server-key"}
	var nativeCalls atomic.Int32
	postStub(t, gw, "z", 0, &nativeCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"busy"}`), nil
	})
	req := httptestNewRequest(t, "POST", "/v1/chat/completions",
		`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "test-server-key")
	req.Header.Set("x-opencode-session", "ses-candgate-custom-tail-1")
	rec := newHttptestRecorder()
	begin := time.Now()
	gw.Handler().ServeHTTP(rec, req)
	elapsed := time.Since(begin)
	if rec.Code != 200 {
		t.Fatalf("custom committed tail must stay 200, got %d body=%q", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !strings.Contains(body, "hello") || !strings.Contains(body, "world") {
		t.Fatalf("custom tail truncated across candidate budget, body=%q", body)
	}
	if elapsed < 2*time.Second {
		t.Fatalf("elapsed=%v must cross the 1s candidate budget", elapsed)
	}
	if customHits.Load() != 1 {
		t.Fatalf("custom hits=%d want 1", customHits.Load())
	}
}

// Non-stream anonymous internal SSE: the full-request deadline stays armed
// after the first deliverable (never revoked on commit). A slow tail that
// exceeds the ultimate deadline must not produce a false successful client
// result. The stub models a deadline-observing transport: it releases the
// tail read with the request error once the 1s whole-request deadline fires.
func TestCandidateGate_NonstreamSlowTailExceedsFullDeadline(t *testing.T) {
	gw := budgetStreamGateway(t, true, "free-candgate-ns", ProtocolChat)
	var calls atomic.Int32
	pipeStreamStub(t, gw, "a", &calls, func(r *http.Request, pw *io.PipeWriter) {
		_, _ = pw.Write([]byte(chatTextSSE("hi")))
		select {
		case <-time.After(3 * time.Second):
			_, _ = pw.Write([]byte(chatTextSSE("tail") + chatDoneSSE()))
			_ = pw.Close()
		case <-r.Context().Done():
			_ = pw.CloseWithError(r.Context().Err())
		}
	})
	req := httptestNewRequest(t, "POST", "/v1/chat/completions",
		`{"model":"free-candgate-ns","messages":[{"role":"user","content":"hi"}]}`, "test-server-key")
	rec := newHttptestRecorder()
	begin := time.Now()
	gw.Handler().ServeHTTP(rec, req)
	elapsed := time.Since(begin)
	if rec.Code == 200 {
		t.Fatalf("slow non-stream tail past the full deadline must not return 200 body=%q", rec.Body.String())
	}
	if elapsed < 800*time.Millisecond || elapsed > 6*time.Second {
		t.Fatalf("elapsed=%v want whole-request deadline bound (~1s)", elapsed)
	}
	if postCount(&calls) < 1 {
		t.Fatalf("upstream sends=%d want >=1", postCount(&calls))
	}
}
