package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func delayedSSE(delay time.Duration, body string) func(*http.Request) (*http.Response, error) {
	return func(*http.Request) (*http.Response, error) {
		time.Sleep(delay)
		return sseResponse(body), nil
	}
}

func assertStreamDurationNotZero(t *testing.T, monitor *Monitor, wantStatus int) {
	t.Helper()
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) == 0 {
		t.Fatalf("no attempts recorded")
	}
	// Find the last matching attempt for this status.
	var last *UpstreamAttempt
	for i := len(recent) - 1; i >= 0; i-- {
		if recent[i].Status == wantStatus {
			v := recent[i]
			last = &v
			break
		}
	}
	if last == nil && wantStatus == 502 {
		// startup failure uses fake 502 response status
		for i := len(recent) - 1; i >= 0; i-- {
			if recent[i].Status == 502 && !recent[i].Success {
				v := recent[i]
				last = &v
				break
			}
		}
	}
	if last == nil {
		// fallback to last attempt
		v := recent[len(recent)-1]
		last = &v
	}
	if last.DurationMS <= 0 {
		t.Fatalf("streamed attempt duration must be non-zero, got %d ms (status %d success=%v) recent=%+v", last.DurationMS, last.Status, last.Success, *last)
	}
	if last.DurationMS < 0 {
		t.Fatalf("duration must be non-negative, got %d", last.DurationMS)
	}
}

func TestStreamDuration_SuccessNotZero_UnboundAnonymous(t *testing.T) {
	monitor := NewMonitor()
	gw := streamSingleProxyGateway(t, monitor)
	stubProxy(t, gw, "a", 0, delayedSSE(20*time.Millisecond, chatTextSSE("hi")+chatDoneSSE()))
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := clientSessionIDs("ses_dur_unbound_anon")
	route := anonAuthRoute()
	route.KeyTiers = nil
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200 got %v err %v", resp, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	assertStreamDurationNotZero(t, monitor, 200)
}

func TestStreamDuration_SuccessNotZero_UnboundAuth(t *testing.T) {
	monitor := NewMonitor()
	gw := streamKeyGateway(t, monitor)
	stubProxy(t, gw, "z", 0, delayedSSE(20*time.Millisecond, chatTextSSE("hi")+chatDoneSSE()))
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := clientSessionIDs("ses_dur_unbound_auth")
	route := authOnlyRoute()
	route.KeyTiers = nil
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200 got %v err %v", resp, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	assertStreamDurationNotZero(t, monitor, 200)
}

func TestStreamDuration_SuccessNotZero_PinnedAnonymous(t *testing.T) {
	monitor := NewMonitor()
	gw := streamSingleProxyGateway(t, monitor)
	ses := "ses_dur_pinned_anon"
	// establish pin first
	stubProxy(t, gw, "a", 0, delayedSSE(5*time.Millisecond, chatTextSSE("hi")+chatDoneSSE()))
	ctxEst := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	idsEst := clientSessionIDs(ses)
	route := anonAuthRoute()
	route.KeyTiers = nil
	respEst, _, _, err := gw.doUpstreamTiers(ctxEst, route, streamBodies(), idsEst, 0)
	if err != nil || respEst.StatusCode != 200 {
		t.Fatalf("establish pin err=%v", err)
	}
	io.Copy(io.Discard, respEst.Body)
	respEst.Body.Close()
	pin, ok := gw.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("pin not established")
	}
	_ = pin
	// now second request on pinned path with deterministic delay
	monitor2 := NewMonitor()
	// reuse same gateway but swap monitor for clean observation? Use new monitor attached via gateway? Keep original monitor but track second attempt.
	// Instead create fresh monitor and re-attach via new gateway? Simpler reuse same monitor and check last attempt.
	// Sleep ensures duration >0
	pool := gw.pools["a"]
	pinnedIdx := -1
	for i, p := range pool.items {
		if p != nil && p.name == pin.ProxyRaw {
			pinnedIdx = i
			break
		}
	}
	if pinnedIdx < 0 {
		t.Fatalf("pinned idx not found")
	}
	stubProxy(t, gw, "a", pinnedIdx, delayedSSE(20*time.Millisecond, chatTextSSE("again")+chatDoneSSE()))
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := requestIDs{Session: ses, Request: "req-pinned-dur", Project: "prj-test"}
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("pinned second want 200 got %v err %v", resp, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	// monitor is same as before, but pinned success also recorded
	assertStreamDurationNotZero(t, monitor, 200)
	_ = monitor2
}

func TestStreamDuration_SuccessNotZero_PinnedAuth(t *testing.T) {
	monitor := NewMonitor()
	cfg := testGatewayConfig(
		map[string][]string{"z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	gw, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	ses := "ses_dur_pinned_auth"
	cred := gw.authCreds[0]
	raw := gw.pools["z"].items[0].name
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", raw, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, ok := gw.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("pin not bound")
	}
	pinnedIdx := -1
	for i, p := range gw.pools["z"].items {
		if p != nil && p.name == pin.ProxyRaw {
			pinnedIdx = i
			break
		}
	}
	if pinnedIdx < 0 {
		t.Fatalf("pinned idx missing")
	}
	stubProxy(t, gw, "z", pinnedIdx, delayedSSE(20*time.Millisecond, chatTextSSE("hi")+chatDoneSSE()))
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := requestIDs{Session: ses, Request: "req-pinned-auth-dur", Project: "prj-test"}
	route := authOnlyRoute()
	route.ID = "m"
	bodies := map[Tier][]byte{TierZen: []byte(`{"model":"m","stream":true}`)}
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, bodies, ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200 got %v err %v", resp, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	assertStreamDurationNotZero(t, monitor, 200)
}

func TestStreamDuration_SuccessNotZero_TransientRetryAnonymous(t *testing.T) {
	monitor := NewMonitor()
	gw := streamSingleProxyGateway(t, monitor)
	var calls atomic.Int32
	stubProxy(t, gw, "a", 0, func(r *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return nil, io.ErrUnexpectedEOF
		}
		time.Sleep(20 * time.Millisecond)
		return sseResponse(chatTextSSE("retry") + chatDoneSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := clientSessionIDs("ses_dur_retry_anon")
	route := anonAuthRoute()
	route.KeyTiers = nil
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200 retry got %v err %v", resp, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	// The successful retry attempt must have non-zero duration.
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) < 2 {
		t.Fatalf("want at least 2 attempts for retry, got %d", len(recent))
	}
	last := recent[len(recent)-1]
	if !last.Success || last.Status != 200 {
		t.Fatalf("last must be success 200, got %+v", last)
	}
	if last.DurationMS <= 0 {
		t.Fatalf("retry streamed success duration must be non-zero, got %d", last.DurationMS)
	}
}

func TestStreamDuration_SuccessNotZero_TransientRetryAuth(t *testing.T) {
	monitor := NewMonitor()
	gw := streamKeyGateway(t, monitor)
	var calls atomic.Int32
	stubProxy(t, gw, "z", 0, func(r *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return nil, io.ErrUnexpectedEOF
		}
		time.Sleep(20 * time.Millisecond)
		return sseResponse(chatTextSSE("retry") + chatDoneSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := clientSessionIDs("ses_dur_retry_auth")
	route := authOnlyRoute()
	route.KeyTiers = nil
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200 retry auth got %v err %v", resp, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) < 2 {
		t.Fatalf("want at least 2 attempts")
	}
	last := recent[len(recent)-1]
	if last.DurationMS <= 0 {
		t.Fatalf("retry auth success duration must be non-zero, got %d", last.DurationMS)
	}
}

func TestStreamDuration_FailureNotZero(t *testing.T) {
	// Pinned auth where startup failure directly records 502 with duration.
	cfg := testGatewayConfig(
		map[string][]string{"z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 1
	monitor2 := NewMonitor()
	gw2, err := NewGateway(cfg, discardGatewayLogger(), monitor2)
	if err != nil {
		t.Fatal(err)
	}
	ses := "ses_dur_fail_pinned"
	cred := gw2.authCreds[0]
	raw := gw2.pools["z"].items[0].name
	gw2.bindSessionPin(ses, "m", TierZen, cred.id, "z", raw, ProtocolChat, normalizeRouteAuthority(gw2.cfg.Upstream.Zen))
	stubProxy(t, gw2, "z", 0, delayedSSE(20*time.Millisecond, ""))
	ctx2 := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids2 := requestIDs{Session: ses, Request: "req-fail-dur", Project: "prj-test"}
	route2 := authOnlyRoute()
	route2.ID = "m"
	bodies := map[Tier][]byte{TierZen: []byte(`{"model":"m","stream":true}`)}
	resp, _, _, err := gw2.doUpstreamTiers(ctx2, route2, bodies, ids2, 0)
	if resp == nil {
		t.Fatalf("pinned failure must return 502 response, err=%v", err)
	}
	if resp.StatusCode != 502 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("want 502 got %d body %q", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	resp.Body.Close()
	assertStreamDurationNotZero(t, monitor2, 502)
}

func TestStreamDuration_FailureNotZero_UnboundAnonymous(t *testing.T) {
	monitor := NewMonitor()
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "a"},
	)
	cfg.Anonymous = true
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	gw, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	// First proxy returns startup failure after delay, second returns success after delay -> first failure recorded
	var firstCalls atomic.Int32
	stubProxy(t, gw, "a", 0, func(r *http.Request) (*http.Response, error) {
		firstCalls.Add(1)
		time.Sleep(20 * time.Millisecond)
		return sseResponse(""), nil
	})
	stubProxy(t, gw, "a", 1, delayedSSE(5*time.Millisecond, chatTextSSE("ok")+chatDoneSSE()))
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := clientSessionIDs("ses_dur_unbound_fail")
	route := anonAuthRoute()
	route.KeyTiers = nil
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, streamBodies(), ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200 after fallback got %v err %v", resp, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	recent := monitor.Snapshot().Upstream.Recent
	// Find the 502 failure
	found := false
	for _, a := range recent {
		if a.Status == 502 && !a.Success {
			found = true
			if a.DurationMS <= 0 {
				t.Fatalf("unbound startup failure duration must be non-zero, got %d %+v", a.DurationMS, a)
			}
		}
	}
	if !found {
		t.Fatalf("no startup failure 502 found in recent %v", recent)
	}
}
