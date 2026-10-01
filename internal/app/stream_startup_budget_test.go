package app

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// pipeStreamStub installs a POST stub whose body is an io.Pipe fed by writer.
// The writer must respect r.Context().Done and CloseWithError on cancel so a
// cancelled startup releases the upstream without leaking goroutines.
func pipeStreamStub(t *testing.T, gw *Gateway, pool string, calls *atomic.Int32, writer func(r *http.Request, pw *io.PipeWriter)) {
	t.Helper()
	postStub(t, gw, pool, 0, calls, nil, func(r *http.Request) (*http.Response, error) {
		pr, pw := io.Pipe()
		go writer(r, pw)
		resp := &http.Response{StatusCode: 200, Header: make(http.Header)}
		resp.Header.Set("Content-Type", "text/event-stream")
		resp.Body = pr
		return resp, nil
	})
}

func budgetStreamGateway(t *testing.T, anonymous bool, model string, proto Protocol) *Gateway {
	t.Helper()
	cfg := testGatewayConfig(
		map[string][]string{
			"a": {"direct"},
			"z": {"direct"},
		},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = anonymous
	if anonymous {
		cfg.Keys = nil
	} else {
		cfg.Keys = []string{"zen-key-aaaaa"}
	}
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 1
	cfg.Retry.AttemptTimeoutSeconds = 1
	gw, err := NewGateway(cfg, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	gw.catalog.ReplaceWithCapabilities([]string{model}, nil,
		map[Tier]map[string]Protocol{TierZen: {model: proto}},
		map[Tier]map[string]bool{},
		map[Tier]map[string]ModelMetadata{TierZen: {model: {ContextWindow: 1000}}})
	gw.cfg.ServerKeys = []string{"test-server-key"}
	return gw
}

func budgetCommitFrames(proto Protocol) (start, commit, tail string) {
	switch proto {
	case ProtocolResponses:
		start = responsesCreatedSSE("resp-budget-1")
		commit = responsesTextSSE("hello")
		tail = responsesTextSSE(" world") + streamResponsesCompletedSSE()
	case ProtocolAnthropic:
		start = "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg-1\",\"model\":\"m\",\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}}\n\n"
		commit = "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n"
		tail = "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" world\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n"
	default:
		start = chatStartSSE()
		commit = chatTextSSE("hello")
		tail = chatTextSSE(" world") + chatDoneSSE()
	}
	return start, commit, tail
}

func budgetClientPayload(model string, proto Protocol) (path, payload string) {
	switch proto {
	case ProtocolResponses:
		return "/v1/responses", `{"model":"` + model + `","stream":true,"input":"hi"}`
	case ProtocolAnthropic:
		return "/v1/messages", `{"model":"` + model + `","stream":true,"messages":[{"role":"user","content":"hi"}],"max_tokens":16}`
	default:
		return "/v1/chat/completions", `{"model":"` + model + `","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	}
}

// The startup budget must not truncate a healthy stream: commit arrives fast,
// the tail continues past the original 1s deadline, and the terminal event is
// delivered intact. Table covers all three client protocols across the native
// anonymous and authenticated lanes.
func TestStreamStartupBudget_LongStreamSurvivesTimeout(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		proto     Protocol
		anonymous bool
		model     string
		pool      string
	}{
		{"chat/anon", ProtocolChat, true, "free-budget-chat", "a"},
		{"responses/auth", ProtocolResponses, false, "m-budget-resp", "z"},
		{"anthropic/anon", ProtocolAnthropic, true, "free-budget-anth", "a"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gw := budgetStreamGateway(t, tc.anonymous, tc.model, tc.proto)
			start, commit, tail := budgetCommitFrames(tc.proto)
			var calls atomic.Int32
			pipeStreamStub(t, gw, tc.pool, &calls, func(r *http.Request, pw *io.PipeWriter) {
				_, _ = pw.Write([]byte(start))
				_, _ = pw.Write([]byte(commit))
				select {
				case <-time.After(1200 * time.Millisecond):
					_, _ = pw.Write([]byte(tail))
					_ = pw.Close()
				case <-r.Context().Done():
					_ = pw.CloseWithError(r.Context().Err())
				}
			})
			path, payload := budgetClientPayload(tc.model, tc.proto)
			req := httptestNewRequest(t, "POST", path, payload, "test-server-key")
			rec := newHttptestRecorder()
			begin := time.Now()
			gw.Handler().ServeHTTP(rec, req)
			elapsed := time.Since(begin)
			if rec.Code != 200 {
				t.Fatalf("status=%d want 200 body=%q", rec.Code, rec.Body.String())
			}
			body := rec.Body.String()
			if !strings.Contains(body, "hello") || !strings.Contains(body, "world") {
				t.Fatalf("stream truncated across deadline, body=%q", body)
			}
			if elapsed < 1100*time.Millisecond {
				t.Fatalf("elapsed=%v must cross the 1s startup deadline", elapsed)
			}
			if postCount(&calls) != 1 {
				t.Fatalf("upstream sends=%d want 1 (no retry after commit)", postCount(&calls))
			}
		})
	}
}

// A stream that never delivers a committable event must still time out: 200
// with only start/usage frames followed by silence ends with 502, not an
// unbounded wait past the startup budget.
func TestStreamStartupBudget_StuckStartupStillTimesOut(t *testing.T) {
	gw := budgetStreamGateway(t, false, "m-stuck", ProtocolChat)
	var calls atomic.Int32
	pipeStreamStub(t, gw, "z", &calls, func(r *http.Request, pw *io.PipeWriter) {
		_, _ = pw.Write([]byte(chatStartSSE()))
		<-r.Context().Done()
		_ = pw.CloseWithError(r.Context().Err())
	})
	req := httptestNewRequest(t, "POST", "/v1/chat/completions",
		`{"model":"m-stuck","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "test-server-key")
	rec := newHttptestRecorder()
	begin := time.Now()
	gw.Handler().ServeHTTP(rec, req)
	elapsed := time.Since(begin)
	if rec.Code == 200 {
		t.Fatalf("stuck startup must not return 200, body=%q", rec.Body.String())
	}
	if elapsed < 800*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("elapsed=%v want startup-timeout bound (~1s)", elapsed)
	}
}

// Non-stream semantics are unchanged: the whole-request timeout still applies,
// including the anonymous internal SSE fold path which reads fully before
// collapsing.
func TestStreamStartupBudget_NonStreamDeadlineStillApplies(t *testing.T) {
	gw := budgetStreamGateway(t, false, "m-nonstream", ProtocolChat)
	var calls atomic.Int32
	postStub(t, gw, "z", 0, &calls, nil, func(r *http.Request) (*http.Response, error) {
		select {
		case <-time.After(3 * time.Second):
			return responseWithBody(200, `{"id":"x","choices":[]}`), nil
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
	})
	req := httptestNewRequest(t, "POST", "/v1/chat/completions",
		`{"model":"m-nonstream","messages":[{"role":"user","content":"hi"}]}`, "test-server-key")
	rec := newHttptestRecorder()
	begin := time.Now()
	gw.Handler().ServeHTTP(rec, req)
	elapsed := time.Since(begin)
	if rec.Code == 200 {
		t.Fatalf("slow non-stream must not return 200")
	}
	if elapsed < 800*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("elapsed=%v want whole-request timeout (~1s)", elapsed)
	}
}

// Client cancellation after commit must release the upstream promptly and
// must not trigger further sends.
func TestStreamStartupBudget_ClientCancelAfterCommitReleasesUpstream(t *testing.T) {
	gw := budgetStreamGateway(t, false, "m-cancel", ProtocolChat)
	gw.cfg.Retry.TimeoutSeconds = 5
	var calls atomic.Int32
	var upstreamSawCancel atomic.Bool
	pipeStreamStub(t, gw, "z", &calls, func(r *http.Request, pw *io.PipeWriter) {
		_, _ = pw.Write([]byte(chatTextSSE("hello")))
		select {
		case <-time.After(5 * time.Second):
			_, _ = pw.Write([]byte(chatTextSSE(" world") + chatDoneSSE()))
			_ = pw.Close()
		case <-r.Context().Done():
			upstreamSawCancel.Store(true)
			_ = pw.CloseWithError(r.Context().Err())
		}
	})
	base := httptestNewRequest(t, "POST", "/v1/chat/completions",
		`{"model":"m-cancel","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "test-server-key")
	ctx, cancel := context.WithCancel(base.Context())
	defer cancel()
	req := base.WithContext(ctx)
	rec := newHttptestRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		gw.Handler().ServeHTTP(rec, req)
	}()
	time.Sleep(400 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatalf("handler did not return after client cancel")
	}
	if !upstreamSawCancel.Load() {
		t.Fatalf("upstream did not observe client cancel")
	}
	if postCount(&calls) != 1 {
		t.Fatalf("upstream sends=%d want 1 (no continued send after cancel)", postCount(&calls))
	}
}

// Custom takeover and pinned follow-ups share the same startup-only budget
// and must not be cut by the shared custom http.Client total timeout: the
// client carries a short 300ms Timeout while the committed tail runs 1.2s.
func TestStreamStartupBudget_CustomTakeoverAndPinnedSurvive(t *testing.T) {
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
		case <-time.After(1200 * time.Millisecond):
			_, _ = w.Write([]byte(chatTextSSE(" world") + chatDoneSSE()))
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	ch := FallbackChannelConfig{ID: "c1", Name: "c1", BaseURL: srv.URL, APIKey: "k1", Model: "cm-budget", Protocol: ProtocolChat}
	cfg := testGatewayConfig(
		map[string][]string{"z": {"direct"}},
		ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
	)
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
	// Short total client timeout: only the stream clone (Timeout stripped)
	// lets the 1.2s tail survive; the shared client itself stays 300ms.
	custom := srv.Client()
	custom.Timeout = 300 * time.Millisecond
	gw.customClient = custom
	var nativeCalls atomic.Int32
	postStub(t, gw, "z", 0, &nativeCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"busy"}`), nil
	})
	payload := `{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	serve := func() (int, string, time.Duration) {
		req := httptestNewRequest(t, "POST", "/v1/chat/completions", payload, "test-server-key")
		req.Header.Set("x-opencode-session", "ses-budget-custom-1")
		rec := newHttptestRecorder()
		begin := time.Now()
		gw.Handler().ServeHTTP(rec, req)
		return rec.Code, rec.Body.String(), time.Since(begin)
	}
	code, body, elapsed := serve()
	if code != 200 || !strings.Contains(body, "hello") || !strings.Contains(body, "world") {
		t.Fatalf("takeover stream truncated: code=%d body=%q", code, body)
	}
	if elapsed < 1100*time.Millisecond {
		t.Fatalf("takeover elapsed=%v must cross the startup deadline", elapsed)
	}
	code2, body2, elapsed2 := serve()
	if code2 != 200 || !strings.Contains(body2, "hello") || !strings.Contains(body2, "world") {
		t.Fatalf("pinned stream truncated: code=%d body=%q", code2, body2)
	}
	if elapsed2 < 1100*time.Millisecond {
		t.Fatalf("pinned elapsed=%v must cross the startup deadline", elapsed2)
	}
	if customHits.Load() < 2 {
		t.Fatalf("custom hits=%d want >=2 (takeover+pinned)", customHits.Load())
	}
	if gw.customClient.Timeout != 300*time.Millisecond {
		t.Fatalf("shared custom client mutated: timeout=%v", gw.customClient.Timeout)
	}
}

// Focused deadline-vs-commit check on the budget controller itself: a commit
// marked before the deadline survives, an unmarked startup is cancelled, and
// concurrent mark/fire hammers stay race-free with no timer leak.
func TestStreamStartupBudget_ControllerSemantics(t *testing.T) {
	ctx, cancel, stop := newStreamStartupBudget(context.Background(), 60*time.Millisecond)
	markStreamStartupCommitted(ctx)
	time.Sleep(120 * time.Millisecond)
	if ctx.Err() != nil {
		cancel()
		t.Fatalf("committed budget must survive its deadline, err=%v", ctx.Err())
	}
	stop()
	cancel()

	ctx2, cancel2, stop2 := newStreamStartupBudget(context.Background(), 40*time.Millisecond)
	defer cancel2()
	defer stop2()
	time.Sleep(150 * time.Millisecond)
	if ctx2.Err() == nil {
		t.Fatalf("uncommitted startup must be cancelled at its deadline")
	}

	parent, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()
	for i := 0; i < 50; i++ {
		c, cancelFn, st := newStreamStartupBudget(parent, time.Millisecond)
		markStreamStartupCommitted(c)
		st()
		cancelFn()
	}
	// Client cancel always propagates even after commit.
	p2, cancelP2, stopP2 := newStreamStartupBudget(parent, time.Hour)
	defer stopP2()
	defer cancelP2()
	markStreamStartupCommitted(p2)
	parentCancel()
	select {
	case <-p2.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("parent cancel must propagate through a committed budget")
	}

	if got := streamCustomClient(&http.Client{Timeout: 300 * time.Millisecond}).Timeout; got != 0 {
		t.Fatalf("stream custom client must strip Timeout, got %v", got)
	}
	shared := &http.Client{Timeout: 300 * time.Millisecond}
	_ = streamCustomClient(shared)
	if shared.Timeout != 300*time.Millisecond {
		t.Fatalf("shared custom client mutated: %v", shared.Timeout)
	}
}
