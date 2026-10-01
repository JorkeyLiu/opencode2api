package app

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// (a) Linearizability: directed expire-first / commit-first plus a high-repeat
// concurrent hammer driving the production expire() path (the same function
// AfterFunc uses) against tryCommit with no wall-clock dependence. Exactly
// one side wins; commit-win keeps the context live (timer must not cancel),
// expire-win fails every commit and surfaces DeadlineExceeded, never a
// fabricated client cancel.
func TestBudgetLinearize_DirectedAndHammer(t *testing.T) {
	t.Parallel()
	// Expire-first: commit must fail, budget error is a timeout.
	func() {
		ctx, cancel, stop := newStreamStartupBudget(context.Background(), time.Hour)
		defer cancel()
		defer stop()
		b := streamStartupBudgetFromCtx(ctx)
		if b == nil {
			t.Fatalf("controller missing")
		}
		if !b.expire() {
			t.Fatalf("directed expire must win")
		}
		if markStreamStartupCommitted(ctx) {
			t.Fatalf("commit after expire must fail")
		}
		if !streamStartupExpired(ctx) {
			t.Fatalf("expired flag must hold")
		}
		if err := streamStartupBudgetErr(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expire-first err=%v want DeadlineExceeded", err)
		}
		if b.expire() {
			t.Fatalf("second expire must not re-win")
		}
	}()
	// Commit-first: expire must lose, context stays live past the deadline.
	func() {
		ctx, cancel, stop := newStreamStartupBudget(context.Background(), 30*time.Millisecond)
		defer cancel()
		defer stop()
		if !markStreamStartupCommitted(ctx) {
			t.Fatalf("directed commit must win")
		}
		if b := streamStartupBudgetFromCtx(ctx); b != nil && b.expire() {
			t.Fatalf("expire after commit must lose")
		}
		time.Sleep(80 * time.Millisecond)
		if ctx.Err() != nil {
			t.Fatalf("commit-win must survive deadline, err=%v", ctx.Err())
		}
		if streamStartupExpired(ctx) {
			t.Fatalf("commit-win must not read expired")
		}
	}()
	// Concurrent hammer: N committers vs M expirers on one controller.
	for i := 0; i < 200; i++ {
		ctx, cancel, stop := newStreamStartupBudget(context.Background(), time.Hour)
		b := streamStartupBudgetFromCtx(ctx)
		if b == nil {
			cancel()
			stop()
			t.Fatalf("controller missing")
		}
		var wins atomic.Int32
		var wg sync.WaitGroup
		for k := 0; k < 4; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if markStreamStartupCommitted(ctx) {
					wins.Add(1)
				}
			}()
		}
		for k := 0; k < 4; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				b.expire()
			}()
		}
		wg.Wait()
		w := wins.Load()
		expired := streamStartupExpired(ctx)
		if w > 1 {
			cancel()
			stop()
			t.Fatalf("iter %d: %d commit winners, want <=1", i, w)
		}
		if w == 1 && expired {
			cancel()
			stop()
			t.Fatalf("iter %d: commit and expire both won", i)
		}
		if w == 0 && !expired {
			cancel()
			stop()
			t.Fatalf("iter %d: neither side won", i)
		}
		if w == 1 && ctx.Err() != nil {
			cancel()
			stop()
			t.Fatalf("iter %d: commit winner cancelled err=%v", i, ctx.Err())
		}
		if w == 0 && !errors.Is(streamStartupBudgetErr(ctx), context.DeadlineExceeded) {
			cancel()
			stop()
			t.Fatalf("iter %d: expire winner err=%v want DeadlineExceeded", i, streamStartupBudgetErr(ctx))
		}
		cancel()
		stop()
	}
}

// (a) Timeout vs client-cancel distinction is preserved at the controller:
// budget expiry reports DeadlineExceeded while a parent cancel reports
// Canceled, and a parent-cancelled gate refuses commit so no success/pin is
// fabricated. Nil-controller legacy callers keep mark()==true compat.
func TestBudgetLinearize_TimeoutVsCancel(t *testing.T) {
	t.Parallel()
	// Parent cancel without expiry: commit refuses, error stays Canceled.
	parent, parentCancel := context.WithCancel(context.Background())
	ctx, cancel, stop := newStreamStartupBudget(parent, time.Hour)
	defer cancel()
	defer stop()
	defer parentCancel()
	parentCancel()
	<-ctx.Done()
	if markStreamStartupCommitted(ctx) {
		t.Fatalf("commit on parent-cancelled ctx must fail")
	}
	if streamStartupExpired(ctx) {
		t.Fatalf("parent cancel must not read as budget expiry")
	}
	if err := streamStartupBudgetErr(ctx); !errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("parent-cancel err=%v want Canceled, not DeadlineExceeded", err)
	}
	// Legacy nil-controller compat: mark reports true.
	if !markStreamStartupCommitted(context.Background()) {
		t.Fatalf("nil controller mark must stay true for compat")
	}
	cancelled, cancelFn := context.WithCancel(context.Background())
	cancelFn()
	if !markStreamStartupCommitted(cancelled) {
		t.Fatalf("nil controller mark must stay true even when cancelled (success gating stays with isContextCancelled)")
	}
	if !isContextCancelled(cancelled) {
		t.Fatalf("cancelled ctx must still read cancelled so callers skip success")
	}
}

// (a) Real gate-to-body path, expire-wins: a stuck native startup times out
// via DeadlineExceeded with no gated body, no pin, exactly one send.
func TestBudgetLinearize_RealGateExpireNoBodyNoPin(t *testing.T) {
	gw := budgetStreamGateway(t, false, "m-lin-exp", ProtocolChat)
	var calls atomic.Int32
	pipeStreamStub(t, gw, "z", &calls, func(r *http.Request, pw *io.PipeWriter) {
		_, _ = pw.Write([]byte(chatStartSSE()))
		<-r.Context().Done()
		_ = pw.CloseWithError(r.Context().Err())
	})
	ses := "ses_linearize_expire_1"
	req := httptestNewRequest(t, "POST", "/v1/chat/completions",
		`{"model":"m-lin-exp","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "test-server-key")
	req.Header.Set("x-opencode-session", ses)
	rec := newHttptestRecorder()
	begin := time.Now()
	gw.Handler().ServeHTTP(rec, req)
	elapsed := time.Since(begin)
	if rec.Code == 200 {
		t.Fatalf("expired startup must not return 200 body=%q", rec.Body.String())
	}
	if elapsed < 800*time.Millisecond || elapsed > 6*time.Second {
		t.Fatalf("elapsed=%v want startup-timeout bound (~1s)", elapsed)
	}
	if postCount(&calls) != 1 {
		t.Fatalf("sends=%d want 1", postCount(&calls))
	}
	if _, ok := gw.scheduler.pinGet(ses, "m-lin-exp"); ok {
		t.Fatalf("expired startup must not bind a session pin")
	}
}

// (b) Custom 2xx SSE with start/heartbeat only (no deliverable) stays bounded
// by TimeoutSeconds and never establishes the takeover binding nor resends.
func TestCustomSSE_StartOnlyStaysBoundedNoEstablish(t *testing.T) {
	var customHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customHits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		fl, _ := w.(http.Flusher)
		// Start-only: role frame plus comment heartbeat, never committable.
		_, _ = w.Write([]byte(chatStartSSE()))
		_, _ = w.Write([]byte(": heartbeat\n\n"))
		if fl != nil {
			fl.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()
	ch := FallbackChannelConfig{ID: "c1", Name: "c1", BaseURL: srv.URL, APIKey: "k1", Model: "cm-start-only", Protocol: ProtocolChat}
	cfg := testGatewayConfig(map[string][]string{"z": {"direct"}}, ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"})
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 1
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
	gw.customClient = srv.Client()
	var nativeCalls atomic.Int32
	postStub(t, gw, "z", 0, &nativeCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"busy"}`), nil
	})
	ses := "ses-custom-start-only-1"
	req := httptestNewRequest(t, "POST", "/v1/chat/completions",
		`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "test-server-key")
	req.Header.Set("x-opencode-session", ses)
	rec := newHttptestRecorder()
	begin := time.Now()
	gw.Handler().ServeHTTP(rec, req)
	elapsed := time.Since(begin)
	if rec.Code == 200 {
		t.Fatalf("start-only custom SSE must not commit 200 body=%q", rec.Body.String())
	}
	if elapsed < 800*time.Millisecond || elapsed > 6*time.Second {
		t.Fatalf("elapsed=%v want TimeoutSeconds bound (~1s)", elapsed)
	}
	if customHits.Load() != 1 {
		t.Fatalf("custom hits=%d want 1 (no resend on stuck startup)", customHits.Load())
	}
	if binding, ok := gw.scheduler.fallbacks.get(ses); ok && binding.Established {
		t.Fatalf("start-only custom must not establish the takeover binding")
	}
}

// (c) Custom JSON and untyped hangs stay bounded through body end; complete
// JSON keeps the legacy compat passthrough.
func TestCustomJSONCompat_BoundedTailAndCompletePassthrough(t *testing.T) {
	t.Run("json_hang_bounded_by_total", func(t *testing.T) {
		var customHits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			customHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			fl, _ := w.(http.Flusher)
			if fl != nil {
				fl.Flush()
			}
			// Headers-to-tail stall: never write the body.
			<-r.Context().Done()
		}))
		defer srv.Close()
		gw, _ := customJSONBoundedGateway(t, srv)
		custom := srv.Client()
		custom.Timeout = 1 * time.Second
		gw.customClient = custom
		postStub429(t, gw)
		req := customJSONStreamRequest(t, "ses-custom-json-total-1")
		rec := newHttptestRecorder()
		begin := time.Now()
		gw.Handler().ServeHTTP(rec, req)
		elapsed := time.Since(begin)
		// Headers already flushed 200 before the tail stall; the assertion
		// is bounded return, not a status flip.
		if elapsed < 800*time.Millisecond || elapsed > 8*time.Second {
			t.Fatalf("json tail elapsed=%v want total-timeout bound (~1s)", elapsed)
		}
		if customHits.Load() != 1 {
			t.Fatalf("custom hits=%d want 1", customHits.Load())
		}
		if gw.customClient.Timeout != 1*time.Second {
			t.Fatalf("shared custom client mutated: %v", gw.customClient.Timeout)
		}
	})
	t.Run("json_hang_bounded_by_startup_budget", func(t *testing.T) {
		var customHits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			customHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			fl, _ := w.(http.Flusher)
			if fl != nil {
				fl.Flush()
			}
			<-r.Context().Done()
		}))
		defer srv.Close()
		gw, _ := customJSONBoundedGateway(t, srv)
		gw.cfg.Retry.TimeoutSeconds = 1
		custom := srv.Client()
		custom.Timeout = 30 * time.Second
		gw.customClient = custom
		postStub429(t, gw)
		req := customJSONStreamRequest(t, "ses-custom-json-budget-1")
		rec := newHttptestRecorder()
		begin := time.Now()
		gw.Handler().ServeHTTP(rec, req)
		elapsed := time.Since(begin)
		if elapsed < 800*time.Millisecond || elapsed > 8*time.Second {
			t.Fatalf("json tail elapsed=%v want startup-budget bound (~1s)", elapsed)
		}
		if customHits.Load() != 1 {
			t.Fatalf("custom hits=%d want 1", customHits.Load())
		}
	})
	t.Run("untyped_hang_bounded", func(t *testing.T) {
		var customHits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			customHits.Add(1)
			w.WriteHeader(200)
			fl, _ := w.(http.Flusher)
			if fl != nil {
				fl.Flush()
			}
			<-r.Context().Done()
		}))
		defer srv.Close()
		gw, _ := customJSONBoundedGateway(t, srv)
		gw.cfg.Retry.TimeoutSeconds = 1
		custom := srv.Client()
		custom.Timeout = 30 * time.Second
		gw.customClient = custom
		postStub429(t, gw)
		req := customJSONStreamRequest(t, "ses-custom-untyped-1")
		rec := newHttptestRecorder()
		begin := time.Now()
		gw.Handler().ServeHTTP(rec, req)
		elapsed := time.Since(begin)
		if elapsed < 800*time.Millisecond || elapsed > 8*time.Second {
			t.Fatalf("untyped tail elapsed=%v want bound (~1s)", elapsed)
		}
		if customHits.Load() != 1 {
			t.Fatalf("custom hits=%d want 1", customHits.Load())
		}
	})
	t.Run("complete_json_compat", func(t *testing.T) {
		var customHits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			customHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"id":"cm-1","choices":[]}`))
		}))
		defer srv.Close()
		gw, _ := customJSONBoundedGateway(t, srv)
		custom := srv.Client()
		custom.Timeout = 5 * time.Second
		gw.customClient = custom
		postStub429(t, gw)
		// Non-stream legacy exhaustion compat: complete JSON passes through.
		req := httptestNewRequest(t, "POST", "/v1/chat/completions",
			`{"model":"m","messages":[{"role":"user","content":"hi"}]}`, "test-server-key")
		req.Header.Set("x-opencode-session", "ses-custom-json-ok-1")
		rec := newHttptestRecorder()
		gw.Handler().ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("complete custom JSON must stay 200, got %d body=%q", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "cm-1") {
			t.Fatalf("complete custom JSON body lost: %q", rec.Body.String())
		}
		if customHits.Load() != 1 {
			t.Fatalf("custom hits=%d want 1", customHits.Load())
		}
	})
}

func customJSONBoundedGateway(t *testing.T, srv *httptest.Server) (*Gateway, FallbackChannelConfig) {
	t.Helper()
	ch := FallbackChannelConfig{ID: "c1", Name: "c1", BaseURL: srv.URL, APIKey: "k1", Model: "cm-json", Protocol: ProtocolChat}
	cfg := testGatewayConfig(map[string][]string{"z": {"direct"}}, ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"})
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 10
	cfg.Retry.AttemptTimeoutSeconds = 10
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
	return gw, ch
}

func postStub429(t *testing.T, gw *Gateway) {
	t.Helper()
	var nativeCalls atomic.Int32
	postStub(t, gw, "z", 0, &nativeCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"busy"}`), nil
	})
}

func customJSONStreamRequest(t *testing.T, session string) *http.Request {
	t.Helper()
	req := httptestNewRequest(t, "POST", "/v1/chat/completions",
		`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "test-server-key")
	req.Header.Set("x-opencode-session", session)
	return req
}
