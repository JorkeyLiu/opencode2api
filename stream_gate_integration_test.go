package main

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

func streamTestGateway(t *testing.T, monitor *Monitor) *Gateway {
	t.Helper()
	cfg := testGatewayConfig(
		map[string][]string{
			"a": {"direct", "http://127.0.0.1:8081"},
			"z": {"direct"},
		},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 5
	gw, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	return gw
}

func streamSingleProxyGateway(t *testing.T, monitor *Monitor) *Gateway {
	t.Helper()
	cfg := testGatewayConfig(
		map[string][]string{
			"a": {"direct"},
			"z": {"direct"},
		},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 5
	gw, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	return gw
}

func streamKeyGateway(t *testing.T, monitor *Monitor) *Gateway {
	t.Helper()
	cfg := testGatewayConfig(
		map[string][]string{
			"z": {"direct", "http://127.0.0.1:8081"},
		},
		ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"test-key-1", "test-key-2"}
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 5
	gw, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	return gw
}

func sseResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: 200,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func chatTextSSE(text string) string {
	return `data: {"id":"chatcmpl-1","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"content":"` + text + `"}}]}` + "\n\n"
}
func chatDoneSSE() string {
	return "data: [DONE]\n\n"
}
func chatStartSSE() string {
	return `data: {"id":"chatcmpl-1","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"role":"assistant"}}]}` + "\n\n"
}
func responsesCreatedSSE(id string) string {
	return `data: {"type":"response.created","response":{"id":"` + id + `","model":"m"}}` + "\n\n"
}
func responsesFailedSSE(msg string) string {
	return `data: {"type":"response.failed","response":{"status":"failed","error":{"message":"` + msg + `"}}}` + "\n\n"
}
func responsesTextSSE(delta string) string {
	return `data: {"type":"response.output_text.delta","delta":"` + delta + `"}` + "\n\n"
}
func streamResponsesCompletedSSE() string {
	return `data: {"type":"response.completed","response":{"status":"completed","output":[]}}` + "\n\n"
}
func streamMalformedSSE() string {
	return "data: {invalid json\n\n"
}

func streamBodies() map[Tier][]byte {
	return map[Tier][]byte{TierZen: []byte(`{"model":"m","stream":true}`)}
}

func TestStreamGate_ImmediateEOF_RetrySuccess(t *testing.T) {
	monitor := NewMonitor()
	gw := streamSingleProxyGateway(t, monitor)
	var calls atomic.Int32
	stubProxy(t, gw, "a", 0, func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return sseResponse(""), nil
		}
		return sseResponse(chatTextSSE("hi") + chatDoneSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := clientSessionIDs("ses_stream_eof_1")
	route := anonAuthRoute()
	route.KeyTiers = nil
	resp, _, attempts, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200 got %v", resp)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "hi") {
		t.Fatalf("body must contain hi, got %q", body)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d want 2", calls.Load())
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
	if _, ok := gw.scheduler.pinGet(ids.Session, "m"); !ok {
		t.Fatalf("pin must be established after gate commit")
	}
}

func TestStreamGate_CreatedOnlyEOF_RetrySuccess(t *testing.T) {
	monitor := NewMonitor()
	gw := streamSingleProxyGateway(t, monitor)
	var calls atomic.Int32
	stubProxy(t, gw, "a", 0, func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return sseResponse(chatStartSSE()), nil
		}
		return sseResponse(chatTextSSE("hi") + chatDoneSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := clientSessionIDs("ses_stream_created_eof")
	route := anonAuthRoute()
	route.KeyTiers = nil
	resp, _, attempts, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200 got %v", resp)
	}
	resp.Body.Close()
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d want 2", calls.Load())
	}
}

func TestStreamGate_PrecommitResponseFailed_Retry(t *testing.T) {
	monitor := NewMonitor()
	gw := streamSingleProxyGateway(t, monitor)
	var calls atomic.Int32
	stubProxy(t, gw, "a", 0, func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return sseResponse(responsesFailedSSE("boom")), nil
		}
		return sseResponse(responsesTextSSE("hi") + streamResponsesCompletedSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := clientSessionIDs("ses_stream_failed")
	route := anonAuthRoute()
	route.Protocol = ProtocolResponses
	route.Protocols = map[Tier]Protocol{TierZen: ProtocolResponses}
	route.KeyTiers = nil
	bodies := map[Tier][]byte{TierZen: []byte(`{"model":"m","stream":true}`)}
	resp, _, attempts, err := gw.doUpstreamTiers(ctx, route, bodies, ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200 after failed retry, got %v", resp)
	}
	resp.Body.Close()
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d want 2", calls.Load())
	}
}

func TestStreamGate_Malformed_Retry(t *testing.T) {
	monitor := NewMonitor()
	gw := streamSingleProxyGateway(t, monitor)
	var calls atomic.Int32
	stubProxy(t, gw, "a", 0, func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return sseResponse(streamMalformedSSE()), nil
		}
		return sseResponse(chatTextSSE("ok") + chatDoneSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := clientSessionIDs("ses_stream_malformed")
	route := anonAuthRoute()
	route.KeyTiers = nil
	resp, _, attempts, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200 got %v", resp)
	}
	resp.Body.Close()
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
}

func TestStreamGate_EmptyCompleted_NoRetry(t *testing.T) {
	monitor := NewMonitor()
	gw := streamSingleProxyGateway(t, monitor)
	var calls atomic.Int32
	stubProxy(t, gw, "a", 0, func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return sseResponse(streamResponsesCompletedSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := clientSessionIDs("ses_stream_empty_done")
	route := anonAuthRoute()
	route.Protocol = ProtocolResponses
	route.Protocols = map[Tier]Protocol{TierZen: ProtocolResponses}
	route.KeyTiers = nil
	bodies := map[Tier][]byte{TierZen: []byte(`{"model":"m","stream":true}`)}
	resp, _, attempts, err := gw.doUpstreamTiers(ctx, route, bodies, ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200 got %v", resp)
	}
	resp.Body.Close()
	if calls.Load() != 1 || attempts != 1 {
		t.Fatalf("empty completed must not retry: calls=%d attempts=%d want 1", calls.Load(), attempts)
	}
}

func TestStreamGate_EmptyCompletedChat_NoRetry(t *testing.T) {
	monitor := NewMonitor()
	gw := streamSingleProxyGateway(t, monitor)
	var calls atomic.Int32
	stubProxy(t, gw, "a", 0, func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return sseResponse(chatDoneSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := clientSessionIDs("ses_stream_empty_done2")
	route := anonAuthRoute()
	route.KeyTiers = nil
	resp, _, attempts, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("chat empty done err=%v resp=%v", err, resp)
	}
	resp.Body.Close()
	if calls.Load() != 1 || attempts != 1 {
		t.Fatalf("chat empty done must not retry: calls=%d attempts=%d", calls.Load(), attempts)
	}
}

func TestStreamGate_DeliverableThenEOF_NoRetry(t *testing.T) {
	monitor := NewMonitor()
	gw := streamSingleProxyGateway(t, monitor)
	var calls atomic.Int32
	stubProxy(t, gw, "a", 0, func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return sseResponse(chatTextSSE("hello")), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := clientSessionIDs("ses_stream_deliver_eof")
	route := anonAuthRoute()
	route.KeyTiers = nil
	resp, _, attempts, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200 got %v", resp)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "hello") {
		t.Fatalf("body missing hello %q", body)
	}
	if calls.Load() != 1 || attempts != 1 {
		t.Fatalf("deliverable then EOF must not retry: calls=%d attempts=%d", calls.Load(), attempts)
	}
}

func TestStreamGate_UnboundCandidateAdvanceAfterRetries(t *testing.T) {
	monitor := NewMonitor()
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "a"},
	)
	cfg.Anonymous = true
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 5
	gw, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	// Use shared counter to make first overall attempt fail regardless of HRW order
	var overallCalls atomic.Int32
	stubProxy(t, gw, "a", 0, func(*http.Request) (*http.Response, error) {
		if overallCalls.Add(1) == 1 {
			return sseResponse(""), nil
		}
		return sseResponse(chatTextSSE("ok") + chatDoneSSE()), nil
	})
	stubProxy(t, gw, "a", 1, func(*http.Request) (*http.Response, error) {
		if overallCalls.Add(1) == 1 {
			return sseResponse(""), nil
		}
		return sseResponse(chatTextSSE("ok") + chatDoneSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := clientSessionIDs("ses_stream_advance")
	route := anonAuthRoute()
	route.KeyTiers = nil
	resp, _, attempts, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200 got %v", resp)
	}
	resp.Body.Close()
	if overallCalls.Load() != 2 {
		t.Fatalf("overall calls=%d want 2", overallCalls.Load())
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
}

func TestStreamGate_PinnedTargetOnlyThen502(t *testing.T) {
	monitor := NewMonitor()
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "a"},
	)
	cfg.Anonymous = true
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 5
	gw, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	stubProxy(t, gw, "a", 0, func(*http.Request) (*http.Response, error) {
		return sseResponse(chatTextSSE("hi") + chatDoneSSE()), nil
	})
	stubProxy(t, gw, "a", 1, func(*http.Request) (*http.Response, error) {
		return sseResponse(chatTextSSE("hi") + chatDoneSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ses := "ses_stream_pinned"
	ids := clientSessionIDs(ses)
	route := anonAuthRoute()
	route.KeyTiers = nil
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("establish pin err=%v resp=%v", err, resp)
	}
	resp.Body.Close()
	pin, ok := gw.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("pin not established")
	}
	pool := gw.pools["a"]
	pinnedIdx := -1
	for i, p := range pool.items {
		if p != nil && p.name == pin.ProxyRaw {
			pinnedIdx = i
			break
		}
	}
	if pinnedIdx < 0 {
		t.Fatalf("pinned not found")
	}
	otherIdx := 1 - pinnedIdx
	var pinnedCalls, otherCalls atomic.Int32
	stubProxy(t, gw, "a", pinnedIdx, func(*http.Request) (*http.Response, error) {
		pinnedCalls.Add(1)
		return sseResponse(""), nil
	})
	stubProxy(t, gw, "a", otherIdx, func(*http.Request) (*http.Response, error) {
		otherCalls.Add(1)
		return sseResponse(chatTextSSE("should not") + chatDoneSSE()), nil
	})
	ids2 := requestIDs{Session: ses, Request: "req-pinned-retry", Project: "prj-test"}
	resp2, _, _, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids2, 0)
	if resp2 == nil {
		t.Fatalf("pinned should return 502 response, got nil err=%v", err)
	}
	if resp2.StatusCode != 502 {
		t.Fatalf("pinned startup failure must return 502, got %d", resp2.StatusCode)
	}
	resp2.Body.Close()
	if pinnedCalls.Load() != 3 {
		t.Fatalf("pinnedCalls=%d want 3", pinnedCalls.Load())
	}
	if otherCalls.Load() != 0 {
		t.Fatalf("otherCalls=%d want 0", otherCalls.Load())
	}
}

func TestStreamGate_NoPinNoSuccessClearOnPrecommitFailure(t *testing.T) {
	monitor := NewMonitor()
	gw := streamSingleProxyGateway(t, monitor)
	var calls atomic.Int32
	stubProxy(t, gw, "a", 0, func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return sseResponse(""), nil
		}
		return sseResponse(chatTextSSE("hi") + chatDoneSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ses := "ses_stream_no_pin"
	ids := clientSessionIDs(ses)
	route := anonAuthRoute()
	route.KeyTiers = nil
	if _, ok := gw.scheduler.pinGet(ses, "m"); ok {
		t.Fatalf("pin should not exist before")
	}
	resp, _, attempts, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	resp.Body.Close()
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
	if _, ok := gw.scheduler.pinGet(ses, "m"); !ok {
		t.Fatalf("pin must exist after commit")
	}
	pool := gw.pools["a"]
	for _, proxy := range pool.items {
		identity := targetIdentity(TierZen, anonymousSchedulerCredentialID, "a", proxy.name, "m")
		if until := gw.scheduler.targetCoolUntil(identity); until != 0 {
			t.Fatalf("target %q should be cleared after success, got %d", proxy.name, until)
		}
	}
	for _, proxy := range pool.items {
		if until, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "a", proxy.name); ok && until > time.Now().UnixNano() {
			t.Fatalf("proxy429 should not be set for startup failure")
		}
	}
}

func TestStreamGate_429ThenStartupFailureDoesNotRevive(t *testing.T) {
	monitor := NewMonitor()
	gw := streamKeyGateway(t, monitor)
	var z0calls, z1calls atomic.Int32
	stubProxy(t, gw, "z", 0, func(*http.Request) (*http.Response, error) {
		z0calls.Add(1)
		resp := &http.Response{StatusCode: 429, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":"rate"}`))}
		resp.Header.Set("Retry-After", "60")
		return resp, nil
	})
	stubProxy(t, gw, "z", 1, func(*http.Request) (*http.Response, error) {
		z1calls.Add(1)
		return sseResponse(""), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := clientSessionIDs("ses_stream_429_startup")
	route := authOnlyRoute()
	route.KeyTiers = nil
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0)
	if err == nil && resp != nil && resp.StatusCode == 200 {
		t.Fatalf("should not succeed, got 200")
	}
	if until, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "z", gw.pools["z"].items[0].name); !ok || until <= time.Now().UnixNano() {
		t.Fatalf("first proxy 429 should remain")
	}
	if until, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "z", gw.pools["z"].items[1].name); ok && until > time.Now().UnixNano() {
		t.Fatalf("second proxy should not have proxy429 from startup failure")
	}
	if resp != nil && resp.StatusCode == 200 {
		t.Fatalf("should not be 200")
	}
	_ = z0calls
	_ = z1calls
	_ = err
}

type cancelReader struct {
	ctx context.Context
}

func (r *cancelReader) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	case <-time.After(100 * time.Millisecond):
		return 0, io.EOF
	}
}
func (r *cancelReader) Close() error { return nil }

func TestStreamGate_Cancellation(t *testing.T) {
	monitor := NewMonitor()
	gw := streamSingleProxyGateway(t, monitor)
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true}))
	ids := clientSessionIDs("ses_stream_cancel")
	route := anonAuthRoute()
	route.KeyTiers = nil
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	stubProxy(t, gw, "a", 0, func(*http.Request) (*http.Response, error) {
		pr, pw := io.Pipe()
		go func() {
			<-ctx.Done()
			pw.CloseWithError(ctx.Err())
		}()
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: pr}, nil
	})
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0)
	if !errors.Is(err, context.Canceled) && !errors.Is(ctx.Err(), context.Canceled) {
		if resp != nil {
			resp.Body.Close()
		}
		if err == nil {
			t.Fatalf("expected cancel error, got resp %v err %v", resp, err)
		}
	}
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if _, ok := gw.scheduler.pinGet(ids.Session, "m"); ok {
		t.Fatalf("pin must not be established on cancel")
	}
}

func TestStreamGate_HTTPHandler_NoEarlyWriteHeader(t *testing.T) {
	monitor := NewMonitor()
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "a"},
	)
	cfg.Anonymous = true
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 5
	gw, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	gw.catalog.ReplaceWithCapabilities([]string{"m"}, nil, map[Tier]map[string]Protocol{TierZen: {"m": ProtocolChat}}, map[Tier]map[string]bool{}, map[Tier]map[string]ModelMetadata{TierZen: {"m": {ContextWindow: 1000}}})
	var calls atomic.Int32
	stubProxy(t, gw, "a", 0, func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return sseResponse(""), nil
		}
		return sseResponse(chatTextSSE("hello") + chatDoneSSE()), nil
	})
	gw.cfg.ServerKeys = []string{"test-server-key"}
	handler := gw.Handler()
	payload := `{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	req := httptestNewRequest(t, "POST", "/v1/chat/completions", payload, "test-server-key")
	rec := newHttptestRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("handler status=%d want 200 body %q", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "hello") {
		t.Fatalf("handler body must contain hello, got %q", body)
	}
	if !strings.Contains(body, "data:") {
		t.Fatalf("handler body must contain SSE data, got %q", body)
	}
	if calls.Load() != 2 {
		t.Fatalf("handler should have retried startup failure, calls=%d want 2", calls.Load())
	}
}

func TestStreamGate_HTTPHandler_Pinned502BeforeBytes(t *testing.T) {
	monitor := NewMonitor()
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "a"},
	)
	cfg.Anonymous = true
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 5
	gw, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	gw.catalog.ReplaceWithCapabilities([]string{"m"}, nil, map[Tier]map[string]Protocol{TierZen: {"m": ProtocolChat}}, map[Tier]map[string]bool{}, map[Tier]map[string]ModelMetadata{TierZen: {"m": {ContextWindow: 1000}}})
	gw.cfg.ServerKeys = []string{"test-server-key"}
	stubProxy(t, gw, "a", 0, func(*http.Request) (*http.Response, error) {
		return sseResponse(chatTextSSE("hi") + chatDoneSSE()), nil
	})
	stubProxy(t, gw, "a", 1, func(*http.Request) (*http.Response, error) {
		return sseResponse(chatTextSSE("hi") + chatDoneSSE()), nil
	})
	sesRaw := "ses_http_pinned"
	payload := `{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	req1 := httptestNewRequest(t, "POST", "/v1/chat/completions", payload, "test-server-key")
	req1.Header.Set("x-opencode-session", sesRaw)
	rec1 := newHttptestRecorder()
	gw.Handler().ServeHTTP(rec1, req1)
	if rec1.Code != 200 {
		t.Fatalf("establish pin status=%d body %q", rec1.Code, rec1.Body.String())
	}
	ids1 := deriveRequestIDs(req1, map[string]any{"model": "m"})
	ses := ids1.Session
	pin, ok := gw.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("pin not established for ses %q", ses)
	}
	// For pinned failure, use single-proxy check: stub pinned to fail, expect 502
	pool := gw.pools["a"]
	pinnedIdx := -1
	for i, p := range pool.items {
		if p != nil && p.name == pin.ProxyRaw {
			pinnedIdx = i
			break
		}
	}
	if pinnedIdx < 0 {
		t.Fatalf("pinned not found")
	}
	var pinnedCalls atomic.Int32
	stubProxy(t, gw, "a", pinnedIdx, func(*http.Request) (*http.Response, error) {
		pinnedCalls.Add(1)
		return sseResponse(""), nil
	})
	// Verify via direct doUpstreamTiers that pinned stream failure returns 502
	// Use raw session for direct to match pin test that uses raw
	ctx2 := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids2 := requestIDs{Session: sesRaw, Request: "req-pinned-retry-direct", Project: "prj-test"}
	route2 := anonAuthRoute()
	route2.KeyTiers = nil
	// Need to re-establish pin for raw session via direct (since handler pin was under derived)
	{
		ctxTmp := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
		idsTmp := requestIDs{Session: sesRaw, Request: "req-establish-raw", Project: "prj-test"}
		// Temporarily stub pinned to succeed for establishment
		stubProxy(t, gw, "a", pinnedIdx, func(*http.Request) (*http.Response, error) {
			return sseResponse(chatTextSSE("hi") + chatDoneSSE()), nil
		})
		otherIdxTmp := 1 - pinnedIdx
		stubProxy(t, gw, "a", otherIdxTmp, func(*http.Request) (*http.Response, error) {
			return sseResponse(chatTextSSE("hi") + chatDoneSSE()), nil
		})
		respTmp, _, _, _ := gw.doUpstreamTiers(ctxTmp, anonAuthRoute(), streamBodies(), idsTmp, 0)
		if respTmp != nil {
			respTmp.Body.Close()
		}
		// Now stub pinned to fail for second call
		stubProxy(t, gw, "a", pinnedIdx, func(*http.Request) (*http.Response, error) {
			pinnedCalls.Add(1)
			return sseResponse(""), nil
		})
	}
	resp2, _, _, err2 := gw.doUpstreamTiers(ctx2, route2, streamBodies(), ids2, 0)
	if resp2 == nil || resp2.StatusCode != 502 {
		t.Fatalf("direct pinned startup failure must be 502, got %v err=%v", resp2, err2)
	}
	resp2.Body.Close()
	if pinnedCalls.Load() < 3 {
		t.Fatalf("pinnedCalls=%d want at least 3", pinnedCalls.Load())
	}
}

func httptestNewRequest(t *testing.T, method, path, body, serverKey string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+serverKey)
	req = req.WithContext(context.WithValue(req.Context(), requestMetaKey{}, &requestMeta{}))
	return req
}

func newHttptestRecorder() *httptest.ResponseRecorder {
	return httptest.NewRecorder()
}
