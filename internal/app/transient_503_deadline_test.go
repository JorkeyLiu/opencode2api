package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func Test503TransientObservationRespectsDeadline(t *testing.T) {
	monitor := NewMonitor()
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm1")))
	}))
	defer custom.Close()
	ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm1"}
	cfg := testGatewayConfig(
		map[string][]string{
			"a": {"direct", "http://127.0.0.1:8081"},
			"z": {"direct"},
		},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Keys = []string{"single-key-12345"}
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 1
	cfg.Retry.TimeoutSeconds = 5
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(norm, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	var a0calls, a1calls, z0calls atomic.Int32
	postStub(t, gw, "a", 0, &a0calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(503, `{"error":"svc"}`), nil
	})
	postStub(t, gw, "a", 1, &a1calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "z", 0, &z0calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	base := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ctx, cancel := context.WithTimeout(base, 120*time.Millisecond)
	defer cancel()
	start := time.Now()
	resp, _, attempts, retErr := gw.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), emptySessionIDs(), 0)
	elapsed := time.Since(start)
	if resp != nil && resp.Body != nil {
		drainAndClose(resp.Body)
	}
	if postCount(&a0calls) != 1 {
		t.Fatalf("a0 calls=%d want 1 (no retry after deadline)", postCount(&a0calls))
	}
	if postCount(&a1calls) != 0 {
		t.Fatalf("a1 calls=%d want 0 (must not switch candidate on 503 deadline)", postCount(&a1calls))
	}
	if postCount(&z0calls) != 0 {
		t.Fatalf("z calls=%d want 0 (must not enter authenticated tier)", postCount(&z0calls))
	}
	if customHits.Load() != 0 {
		t.Fatalf("custom hits=%d want 0 (must not enter custom fallback on 503 deadline)", customHits.Load())
	}
	if !errors.Is(retErr, context.DeadlineExceeded) && !errors.Is(retErr, context.Canceled) && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("retErr=%v ctxErr=%v want deadline exceeded", retErr, ctx.Err())
	}
	if elapsed > 800*time.Millisecond {
		t.Fatalf("elapsed %v too long, deadline not respected (delay was 1s but deadline 120ms)", elapsed)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1", attempts)
	}
	_ = monitor
}

func TestPinnedAnonymous503DeadlineStopsTransientObservation(t *testing.T) {
	monitor := NewMonitor()
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm1")))
	}))
	defer custom.Close()
	ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm1"}
	cfg := testGatewayConfig(
		map[string][]string{
			"a": {"direct", "http://127.0.0.1:8081"},
			"z": {"direct"},
		},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Keys = []string{"single-key-12345"}
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 1
	cfg.Retry.TimeoutSeconds = 5
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(norm, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	// Establish anonymous pin for session+model.
	session := "ses_pinned_503_deadline_anon_1"
	estabIDs := pinIDs(session, "req-pin-estab")
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), estabIDs, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("pin establishment must succeed 200, err=%v resp=%v", err, resp)
	}
	drainAndClose(resp.Body)
	pinBefore, ok := gw.scheduler.pinGet(session, "m")
	if !ok {
		t.Fatalf("must pin anon success before deadline probe")
	}
	pinnedIdx := -1
	for i, p := range gw.pools["a"].items {
		if p != nil && p.name == pinBefore.ProxyRaw {
			pinnedIdx = i
		}
	}
	if pinnedIdx < 0 {
		t.Fatalf("pinned proxy not found: %+v", pinBefore)
	}
	otherIdx := 1 - pinnedIdx

	var pinnedCalls, otherCalls, z0calls atomic.Int32
	postStub(t, gw, "a", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(503, `{"error":"svc"}`), nil
	})
	postStub(t, gw, "a", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "z", 0, &z0calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	base := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ctx, cancel := context.WithTimeout(base, 120*time.Millisecond)
	defer cancel()
	start := time.Now()
	resp2, _, attempts, retErr := gw.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), pinIDs(session, "req-pin-503"), 0)
	elapsed := time.Since(start)
	if resp2 != nil && resp2.Body != nil {
		drainAndClose(resp2.Body)
	}
	if postCount(&pinnedCalls) != 1 {
		t.Fatalf("pinned calls=%d want 1 (no retry after deadline)", postCount(&pinnedCalls))
	}
	if postCount(&otherCalls) != 0 {
		t.Fatalf("other anon calls=%d want 0 (pinned must not switch on 503 deadline)", postCount(&otherCalls))
	}
	if postCount(&z0calls) != 0 {
		t.Fatalf("z calls=%d want 0 (pinned anon must not enter authenticated tier)", postCount(&z0calls))
	}
	if customHits.Load() != 0 {
		t.Fatalf("custom hits=%d want 0 (pinned 503 deadline must not enter custom fallback)", customHits.Load())
	}
	if !errors.Is(retErr, context.DeadlineExceeded) && !errors.Is(retErr, context.Canceled) && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("retErr=%v ctxErr=%v want deadline exceeded/canceled", retErr, ctx.Err())
	}
	if elapsed > 800*time.Millisecond {
		t.Fatalf("elapsed %v too long, deadline not respected (delay was 1s but deadline 120ms)", elapsed)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1", attempts)
	}
	pinAfter, ok := gw.scheduler.pinGet(session, "m")
	if !ok {
		t.Fatalf("pin must remain after deadline probe")
	}
	if pinAfter != pinBefore {
		t.Fatalf("pin changed across deadline: before %+v after %+v (channel/credential/pool/proxy/model/protocol/authority/generation must stay)", pinBefore, pinAfter)
	}
	_ = monitor
}
