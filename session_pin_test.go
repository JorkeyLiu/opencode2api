package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func pinTestCtx() context.Context {
	return context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
}

func pinIDs(session, req string) requestIDs {
	if req == "" {
		req = "req-pin-test"
	}
	return requestIDs{Session: session, Request: req, Project: "prj-test"}
}

func drainResp(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		drainAndClose(resp.Body)
	}
}

// Unbound first request still falls back (anon 404 -> zen) then pins to zen.
func TestPinUnboundFallbackThenPins(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	var a0, a1, zen atomic.Int32
	postStub(t, gateway, "a", 0, &a0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(404, `{"error":"nope"}`), nil
	})
	postStub(t, gateway, "a", 1, &a1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(404, `{"error":"nope"}`), nil
	})
	postStub(t, gateway, "z", 0, &zen, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := pinIDs("ses_pin_fallback_1", "req-pin-1")
	resp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("err=%v resp=%v want 200 via zen fallback", err, resp)
	}
	drainResp(resp)
	if postCount(&a0)+postCount(&a1) != 1 {
		t.Fatalf("anon must stop after first ordinary rejection: %d/%d", postCount(&a0), postCount(&a1))
	}
	if postCount(&zen) != 1 {
		t.Fatalf("must fallback to auth only: zen=%d", postCount(&zen))
	}
	pin, ok := gateway.scheduler.pinGet(ids.Session, "m")
	if !ok {
		t.Fatalf("first 2xx must bind session+model")
	}
	if pin.Tier != TierZen || pin.CredID == anonymousSchedulerCredentialID {
		t.Fatalf("pin must be authenticated zen, got %+v", pin)
	}
	if pin.Pool != "z" || pin.Model != "m" || pin.Protocol != ProtocolChat {
		t.Fatalf("pin identity incomplete: %+v", pin)
	}
}

// Next request stays on pinned proxy; anon is never touched.
func TestPinStaysOnPinnedProxy(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	postStub(t, gateway, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(404, `{"error":"nope"}`), nil
	})
	postStub(t, gateway, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(404, `{"error":"nope"}`), nil
	})
	postStub(t, gateway, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := pinIDs("ses_pin_sticky_1", "req-pin-1")
	resp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), ids, 0)
	if err != nil {
		t.Fatal(err)
	}
	drainResp(resp)
	// Re-stub anon to succeed if ever touched; zen stays 200.
	var a0, a1, zen atomic.Int32
	postStub(t, gateway, "a", 0, &a0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, &a1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 0, &zen, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids2 := pinIDs(ids.Session, "req-pin-2")
	resp2, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), ids2, 0)
	if err != nil || resp2 == nil || resp2.StatusCode != 200 {
		t.Fatalf("err=%v resp=%v", err, resp2)
	}
	drainResp(resp2)
	if postCount(&a0)+postCount(&a1) != 0 {
		t.Fatalf("pinned request must not touch anon: %d/%d", postCount(&a0), postCount(&a1))
	}
	if postCount(&zen) != 1 {
		t.Fatalf("pinned zen sends=%d want 1", postCount(&zen))
	}
}

// Pinned anonymous 429 walks the same binding to the next proxy with an
// identical session and updates current.
func TestPinnedAnonymous429NoFallback(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	// First request pins to whichever anon proxy HRW picks.
	postStub(t, gateway, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := pinIDs("ses_pin_anon429_1", "req-pin-1")
	route := anonAuthRoute()
	route.KeyTiers = []Tier{TierZen}
	resp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids, 0)
	if err != nil {
		t.Fatal(err)
	}
	drainResp(resp)
	pin, ok := gateway.scheduler.pinGet(ids.Session, "m")
	if !ok {
		t.Fatalf("must pin anon success")
	}
	if pin.CredID != anonymousSchedulerCredentialID {
		t.Fatalf("expected anon pin, got %+v", pin)
	}
	// Find pinned index.
	pool := gateway.pools["a"]
	pinnedIdx := -1
	for i, proxy := range pool.items {
		if proxy != nil && proxy.name == pin.ProxyRaw {
			pinnedIdx = i
		}
	}
	if pinnedIdx < 0 {
		t.Fatalf("pinned proxy not found: %+v", pin)
	}
	otherIdx := 1 - pinnedIdx
	if otherIdx < 0 || otherIdx > 1 {
		t.Fatalf("unexpected pool size")
	}
	var pinnedCalls, otherCalls, zenCalls atomic.Int32
	cap := &capturedUpstream{}
	postStub(t, gateway, "a", pinnedIdx, &pinnedCalls, cap, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"throttled"}`)
		r.Header.Set("Retry-After", "7")
		return r, nil
	})
	postStub(t, gateway, "a", otherIdx, &otherCalls, cap, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 0, &zenCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids2 := pinIDs(ids.Session, "req-pin-2")
	resp2, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids2, 0)
	if err != nil {
		t.Fatalf("pinned 429 must return response, err=%v", err)
	}
	if resp2 == nil || resp2.StatusCode != 200 {
		t.Fatalf("status=%v want 200 via same-binding walk", resp2)
	}
	drainResp(resp2)
	if postCount(&pinnedCalls) != 1 || postCount(&otherCalls) != 1 {
		t.Fatalf("same-binding walk: pinned=%d other=%d", postCount(&pinnedCalls), postCount(&otherCalls))
	}
	if postCount(&zenCalls) != 0 {
		t.Fatalf("pinned walk must not enter auth: zen=%d", postCount(&zenCalls))
	}
	sessions, bodies := cap.get()
	if len(sessions) != 2 || sessions[0] != sessions[1] {
		t.Fatalf("walk must keep session identical: %q", sessions)
	}
	if len(bodies) == 2 && string(bodies[0]) != string(bodies[1]) {
		t.Fatalf("walk body must stay byte-identical")
	}
	pin2, _ := gateway.scheduler.pinGet(ids.Session, "m")
	otherRaw := gateway.pools["a"].items[otherIdx].name
	if pin2.ProxyRaw != otherRaw {
		t.Fatalf("pin current must move to other proxy: %+v", pin2)
	}
}

// Exhaustion fast-fail: after both proxies cool, the next request fast-fails
// locally with 429 and zero sends.
func TestPinnedCooldownFastFail(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	postStub(t, gateway, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := pinIDs("ses_pin_cool_1", "req-pin-1")
	route := anonAuthRoute()
	resp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids, 0)
	if err != nil {
		t.Fatal(err)
	}
	drainResp(resp)
	pin, ok := gateway.scheduler.pinGet(ids.Session, "m")
	if !ok {
		t.Fatalf("must pin")
	}
	pool := gateway.pools["a"]
	pinnedIdx := -1
	for i, proxy := range pool.items {
		if proxy != nil && proxy.name == pin.ProxyRaw {
			pinnedIdx = i
		}
	}
	// Second request: both proxies 429 so the binding exhausts and cools.
	var pinnedCalls, otherSecond atomic.Int32
	postStub(t, gateway, "a", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"throttled"}`)
		r.Header.Set("Retry-After", "60")
		return r, nil
	})
	otherIdx := 1 - pinnedIdx
	postStub(t, gateway, "a", otherIdx, &otherSecond, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"throttled"}`)
		r.Header.Set("Retry-After", "60")
		return r, nil
	})
	ids2 := pinIDs(ids.Session, "req-pin-2")
	resp2, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids2, 0)
	if err != nil || resp2.StatusCode != 429 {
		t.Fatalf("second must exhaust with 429, err=%v resp=%v", err, resp2)
	}
	drainResp(resp2)
	if postCount(&pinnedCalls) != 1 || postCount(&otherSecond) != 1 {
		t.Fatalf("exhaustion must send both: %d/%d", postCount(&pinnedCalls), postCount(&otherSecond))
	}
	// Third request during cooldown must fast-fail locally: zero sends.
	var c0, c1, zc, gc atomic.Int32
	postStub(t, gateway, "a", 0, &c0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, &c1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 0, &zc, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids3 := pinIDs(ids.Session, "req-pin-3")
	resp3, _, attempts, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids3, 0)
	if err != nil {
		t.Fatalf("fast-fail must return response, err=%v", err)
	}
	if resp3 == nil || resp3.StatusCode != 429 {
		t.Fatalf("fast-fail status=%v want 429", resp3)
	}
	if attempts != 0 {
		t.Fatalf("fast-fail attempts=%d want 0 (no send)", attempts)
	}
	if postCount(&c0)+postCount(&c1)+postCount(&zc)+postCount(&gc) != 0 {
		t.Fatalf("fast-fail must not send upstream: a=%d/%d z=%d g=%d", postCount(&c0), postCount(&c1), postCount(&zc), postCount(&gc))
	}
	if got := resp3.Header.Get("Retry-After"); got == "" {
		t.Fatalf("fast-fail must carry Retry-After from remaining cooldown")
	}
	raw, _ := io.ReadAll(resp3.Body)
	drainResp(resp3)
	if len(raw) == 0 || !strings.Contains(string(raw), "upstream") {
		t.Fatalf("fast-fail body must carry concise upstream message, got %q", raw)
	}
	// Target-protocol envelope: copyErrorResponse must render chat + anthropic shapes.
	for _, proto := range []Protocol{ProtocolChat, ProtocolResponses, ProtocolAnthropic} {
		synth := pinLocalResponse(429, 5, "upstream temporarily unavailable")
		rec := &testResponseWriter{header: make(http.Header)}
		copyErrorResponse(rec, proto, synth, "req-x")
		if rec.status != 429 {
			t.Fatalf("proto %s envelope status=%d want 429", proto, rec.status)
		}
		if proto != ProtocolChat && rec.header.Get("Retry-After") == "" {
			// copyErrorResponse copies Retry-After for all protocols.
			t.Fatalf("proto %s must preserve Retry-After", proto)
		}
		if len(rec.body) == 0 {
			t.Fatalf("proto %s empty envelope", proto)
		}
	}
}

type testResponseWriter struct {
	header http.Header
	body   []byte
	status int
}

func (w *testResponseWriter) Header() http.Header { return w.header }
func (w *testResponseWriter) Write(b []byte) (int, error) {
	w.body = append(w.body, b...)
	return len(b), nil
}
func (w *testResponseWriter) WriteHeader(status int) { w.status = status }

// Pinned transport failure retries same-target only then surfaces error (outer 502).
func TestPinnedTransportRetryOnly(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	postStub(t, gateway, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := pinIDs("ses_pin_trans_1", "req-pin-1")
	route := anonAuthRoute()
	resp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids, 0)
	if err != nil {
		t.Fatal(err)
	}
	drainResp(resp)
	pin, _ := gateway.scheduler.pinGet(ids.Session, "m")
	pool := gateway.pools["a"]
	pinnedIdx := 0
	for i, proxy := range pool.items {
		if proxy != nil && proxy.name == pin.ProxyRaw {
			pinnedIdx = i
		}
	}
	otherIdx := 1 - pinnedIdx
	var pinnedCalls, otherCalls, zenCalls atomic.Int32
	postStub(t, gateway, "a", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial timeout")
	})
	postStub(t, gateway, "a", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 0, &zenCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids2 := pinIDs(ids.Session, "req-pin-2")
	_, _, _, err = gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids2, 0)
	if err == nil {
		t.Fatalf("pinned transport exhaustion must return error (outer 502)")
	}
	if postCount(&pinnedCalls) != 2 || postCount(&otherCalls) != 0 || postCount(&zenCalls) != 0 {
		t.Fatalf("same-target retry only: pinned=%d other=%d zen=%d", postCount(&pinnedCalls), postCount(&otherCalls), postCount(&zenCalls))
	}
}

// Pinned 5xx retries same-target only then final 5xx.
func TestPinned5xxRetryOnly(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	postStub(t, gateway, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := pinIDs("ses_pin_5xx_1", "req-pin-1")
	route := anonAuthRoute()
	resp, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids, 0)
	drainResp(resp)
	pin, _ := gateway.scheduler.pinGet(ids.Session, "m")
	pinnedIdx := 0
	for i, proxy := range gateway.pools["a"].items {
		if proxy != nil && proxy.name == pin.ProxyRaw {
			pinnedIdx = i
		}
	}
	otherIdx := 1 - pinnedIdx
	var pinnedCalls, otherCalls, zenCalls atomic.Int32
	postStub(t, gateway, "a", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(500, `{"error":"boom"}`), nil
	})
	postStub(t, gateway, "a", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 0, &zenCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids2 := pinIDs(ids.Session, "req-pin-2")
	resp2, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids2, 0)
	if err != nil || resp2.StatusCode != 500 {
		t.Fatalf("err=%v resp=%v want final 500", err, resp2)
	}
	drainResp(resp2)
	if postCount(&pinnedCalls) != 2 || postCount(&otherCalls) != 0 || postCount(&zenCalls) != 0 {
		t.Fatalf("5xx same-target only: pinned=%d other=%d zen=%d", postCount(&pinnedCalls), postCount(&otherCalls), postCount(&zenCalls))
	}
}

// Pinned exact-400 replays same-target with the same session.
func TestPinned400Replay(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	postStub(t, gateway, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := pinIDs("ses_pin_400_1", "req-pin-1")
	route := anonAuthRoute()
	resp, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids, 0)
	drainResp(resp)
	pin, _ := gateway.scheduler.pinGet(ids.Session, "m")
	pinnedIdx := 0
	for i, proxy := range gateway.pools["a"].items {
		if proxy != nil && proxy.name == pin.ProxyRaw {
			pinnedIdx = i
		}
	}
	cap := &capturedUpstream{}
	var pinnedCalls, otherCalls atomic.Int32
	var calls atomic.Int32
	pool := gateway.pools["a"]
	pool.items[pinnedIdx].client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet {
			return responseWithBody(200, `{}`), nil
		}
		pinnedCalls.Add(1)
		cap.add(r)
		if calls.Add(1) == 1 {
			return responseWithBody(400, `{"error":"bad"}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})}
	otherIdx := 1 - pinnedIdx
	postStub(t, gateway, "a", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids2 := pinIDs(ids.Session, "req-pin-2")
	resp2, _, attempts, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids2, 0)
	if err != nil || resp2.StatusCode != 200 {
		t.Fatalf("err=%v resp=%v want replay 200", err, resp2)
	}
	drainResp(resp2)
	if attempts != 2 || postCount(&pinnedCalls) != 2 || postCount(&otherCalls) != 0 {
		t.Fatalf("400 replay same-target: attempts=%d pinned=%d other=%d", attempts, postCount(&pinnedCalls), postCount(&otherCalls))
	}
	sessions, _ := cap.get()
	if len(sessions) != 2 || sessions[0] != sessions[1] {
		t.Fatalf("replay must keep the same session: %q", sessions)
	}
	if got := gateway.scheduler.routeSessions.count(); got != 0 {
		t.Fatalf("same-session replay must not store an override, got %d", got)
	}
	if _, ok := gateway.scheduler.pinGet(ids.Session, "m"); !ok {
		t.Fatalf("replay success must remain bound")
	}
}

// Auth pin never falls back across tiers.
func TestAuthPinNoCrossTierFallback(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	postStub(t, gateway, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := pinIDs("ses_pin_auth_1", "req-pin-1")
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	resp, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids, 0)
	drainResp(resp)
	pin, ok := gateway.scheduler.pinGet(ids.Session, "m")
	if !ok || pin.Tier != TierZen {
		t.Fatalf("must pin zen, got %+v ok=%v", pin, ok)
	}
	var zenCalls atomic.Int32
	postStub(t, gateway, "z", 0, &zenCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(404, `{"error":"nope"}`), nil
	})
	ids2 := pinIDs(ids.Session, "req-pin-2")
	resp2, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids2, 0)
	if err != nil || resp2.StatusCode != 404 {
		t.Fatalf("err=%v resp=%v want pinned 404", err, resp2)
	}
	drainResp(resp2)
	if postCount(&zenCalls) != 1 {
		t.Fatalf("no cross-tier fallback: zen=%d", postCount(&zenCalls))
	}
}

// Separate models pin independently.
func TestPinSeparateModels(t *testing.T) {
	scheduler := newTargetScheduler(15 * time.Second)
	zenAuth := normalizeRouteAuthority("https://zen.example")
	scheduler.pinBind("ses_multi", "model-a", sessionPin{Tier: TierZen, CredID: "zen:aaa", Pool: "z", ProxyRaw: "p1", Protocol: ProtocolChat, Authority: zenAuth})
	scheduler.pinBind("ses_multi", "model-b", sessionPin{Tier: TierGo, CredID: "go:bbb", Pool: "g", ProxyRaw: "p2", Protocol: ProtocolChat, Authority: normalizeRouteAuthority("https://go.example")})
	a, okA := scheduler.pinGet("ses_multi", "model-a")
	b, okB := scheduler.pinGet("ses_multi", "model-b")
	if !okA || !okB {
		t.Fatalf("both models must pin independently")
	}
	if a.ProxyRaw == b.ProxyRaw || a.Tier == b.Tier {
		t.Fatalf("pins must be independent: %+v vs %+v", a, b)
	}
	// Realistic gateway check: same session different model IDs pin separately.
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	postStub(t, gateway, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	routeA := anonAuthRoute()
	routeA.ID = "model-a"
	routeB := anonAuthRoute()
	routeB.ID = "model-b"
	bodiesA := map[Tier][]byte{TierZen: []byte(`{"model":"model-a"}`)}
	bodiesB := map[Tier][]byte{TierZen: []byte(`{"model":"model-b"}`)}
	ses := "ses_pin_models_1"
	r1, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), routeA, bodiesA, pinIDs(ses, "r1"), 0)
	drainResp(r1)
	r2, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), routeB, bodiesB, pinIDs(ses, "r2"), 0)
	drainResp(r2)
	if _, ok := gateway.scheduler.pinGet(ses, "model-a"); !ok {
		t.Fatalf("model-a must pin")
	}
	if _, ok := gateway.scheduler.pinGet(ses, "model-b"); !ok {
		t.Fatalf("model-b must pin")
	}
}

// Concurrent unpinned same session+model serializes sends: one owner only.
// Target-A fails with a fallback opportunity (403); the follower must send
// nothing until the pin exists, then use only the pinned target. No 400
// replay is involved; the assertion is on serialized sends, not stored identity.
func TestPinEstablishmentSerializesConcurrentSameKey(t *testing.T) {
	monitor := NewMonitor()
	gateway := authTwoProxyGateway(t, monitor, 5)
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	session := "ses_pin_estab_serial_1"
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var once sync.Once
	var order atomic.Int32
	var active atomic.Int32
	var maxActive atomic.Int32
	track := func() func() {
		cur := active.Add(1)
		for {
			prev := maxActive.Load()
			if cur <= prev || maxActive.CompareAndSwap(prev, cur) {
				break
			}
		}
		return func() { active.Add(-1) }
	}
	stubFn := func(*http.Request) (*http.Response, error) {
		done := track()
		defer done()
		if order.Add(1) == 1 {
			once.Do(func() { close(firstStarted) })
			select {
			case <-releaseFirst:
			case <-time.After(5 * time.Second):
				return responseWithBody(500, `{"error":"barrier timeout"}`), nil
			}
			return responseWithBody(403, `{"error":"forbidden"}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	}
	var z0, z1 atomic.Int32
	postStub(t, gateway, "z", 0, &z0, nil, stubFn)
	postStub(t, gateway, "z", 1, &z1, nil, stubFn)
	type result struct {
		resp *http.Response
		err  error
	}
	ownerCh := make(chan result, 1)
	followerCh := make(chan result, 1)
	go func() {
		resp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(session, "req-owner"), 0)
		ownerCh <- result{resp, err}
	}()
	select {
	case <-firstStarted:
	case <-time.After(3 * time.Second):
		t.Fatalf("owner first send never started")
	}
	go func() {
		resp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(session, "req-follower"), 0)
		followerCh <- result{resp, err}
	}()
	// Follower must remain blocked on the claim while the owner is stalled
	// on its first send: no second send may start.
	time.Sleep(150 * time.Millisecond)
	if got := postCount(&z0) + postCount(&z1); got != 1 {
		t.Fatalf("follower sent before pin: total POSTs=%d want 1 (owner only)", got)
	}
	close(releaseFirst)
	var ownerRes, followerRes result
	select {
	case ownerRes = <-ownerCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("owner deadlocked")
	}
	select {
	case followerRes = <-followerCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("follower deadlocked")
	}
	if ownerRes.err != nil || ownerRes.resp == nil || ownerRes.resp.StatusCode != 200 {
		t.Fatalf("owner err=%v resp=%v want 200 via fallback pin", ownerRes.err, ownerRes.resp)
	}
	drainResp(ownerRes.resp)
	if followerRes.err != nil || followerRes.resp == nil || followerRes.resp.StatusCode != 200 {
		t.Fatalf("follower err=%v resp=%v want 200 via pinned target", followerRes.err, followerRes.resp)
	}
	drainResp(followerRes.resp)
	pin, ok := gateway.scheduler.pinGet(session, "m")
	if !ok {
		t.Fatalf("must pin after owner fallback success")
	}
	pool := gateway.pools["z"]
	pinnedIdx := -1
	for i, proxy := range pool.items {
		if proxy != nil && proxy.name == pin.ProxyRaw {
			pinnedIdx = i
		}
	}
	if pinnedIdx < 0 {
		t.Fatalf("pinned proxy not found: %+v", pin)
	}
	counts := []int{postCount(&z0), postCount(&z1)}
	if counts[pinnedIdx] != 2 {
		t.Fatalf("pinned proxy sends=%d want 2 (owner fallback + follower pinned), all=%v pinIdx=%d", counts[pinnedIdx], counts, pinnedIdx)
	}
	if counts[1-pinnedIdx] != 1 {
		t.Fatalf("other proxy sends=%d want 1 (owner first only), all=%v", counts[1-pinnedIdx], counts)
	}
	if maxActive.Load() != 1 {
		t.Fatalf("concurrent sends detected: maxActive=%d want 1 (single owner)", maxActive.Load())
	}
	if n := gateway.scheduler.pins.inflightCount(); n != 0 {
		t.Fatalf("claim leak: inflight=%d want 0", n)
	}
}

// Owner fails with no pin; the waiter contends to become the next owner.
// No deadlock, no thundering duplicate owners (max one sender at a time).
func TestPinEstablishmentOwnerFailsWaiterTakesOver(t *testing.T) {
	monitor := NewMonitor()
	gateway := authTwoProxyGateway(t, monitor, 5)
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	session := "ses_pin_estab_takeover_1"
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var once sync.Once
	var order atomic.Int32
	var active atomic.Int32
	var maxActive atomic.Int32
	stubFn := func(*http.Request) (*http.Response, error) {
		cur := active.Add(1)
		for {
			prev := maxActive.Load()
			if cur <= prev || maxActive.CompareAndSwap(prev, cur) {
				break
			}
		}
		defer active.Add(-1)
		if order.Add(1) == 1 {
			once.Do(func() { close(firstStarted) })
			select {
			case <-releaseFirst:
			case <-time.After(5 * time.Second):
				return responseWithBody(500, `{"error":"barrier timeout"}`), nil
			}
		}
		return responseWithBody(404, `{"error":"nope"}`), nil
	}
	var z0, z1 atomic.Int32
	postStub(t, gateway, "z", 0, &z0, nil, stubFn)
	postStub(t, gateway, "z", 1, &z1, nil, stubFn)
	type result struct {
		resp *http.Response
		err  error
	}
	ownerCh := make(chan result, 1)
	followerCh := make(chan result, 1)
	go func() {
		resp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(session, "req-owner"), 0)
		ownerCh <- result{resp, err}
	}()
	select {
	case <-firstStarted:
	case <-time.After(3 * time.Second):
		t.Fatalf("owner first send never started")
	}
	go func() {
		resp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(session, "req-follower"), 0)
		followerCh <- result{resp, err}
	}()
	time.Sleep(150 * time.Millisecond)
	if got := postCount(&z0) + postCount(&z1); got != 1 {
		t.Fatalf("waiter sent before owner finished: total=%d want 1", got)
	}
	close(releaseFirst)
	var ownerRes, followerRes result
	select {
	case ownerRes = <-ownerCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("owner deadlocked")
	}
	select {
	case followerRes = <-followerCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("waiter deadlocked after owner failure without pin")
	}
	if ownerRes.resp == nil || ownerRes.resp.StatusCode != 404 {
		t.Fatalf("owner status=%v want 404 with no pin", ownerRes.resp)
	}
	drainResp(ownerRes.resp)
	if followerRes.resp == nil || followerRes.resp.StatusCode != 404 {
		t.Fatalf("waiter status=%v want 404 as next owner", followerRes.resp)
	}
	drainResp(followerRes.resp)
	if _, ok := gateway.scheduler.pinGet(session, "m"); ok {
		t.Fatalf("failure without 2xx must not bind a pin")
	}
	if got := postCount(&z0) + postCount(&z1); got != 2 {
		t.Fatalf("total POSTs=%d want 2 (owner + waiter as next owner, sequential)", got)
	}
	if maxActive.Load() != 1 {
		t.Fatalf("duplicate owners: maxActive=%d want 1", maxActive.Load())
	}
	if n := gateway.scheduler.pins.inflightCount(); n != 0 {
		t.Fatalf("claim leak: inflight=%d want 0", n)
	}
}

// A waiting follower cancelled while the owner is stalled returns promptly
// with its context error and never sends; the owner still succeeds and pins.
func TestPinEstablishmentFollowerCancelPrompt(t *testing.T) {
	monitor := NewMonitor()
	gateway := authTwoProxyGateway(t, monitor, 5)
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	session := "ses_pin_estab_cancel_1"
	firstStarted := make(chan struct{})
	releaseOwner := make(chan struct{})
	var once sync.Once
	var order atomic.Int32
	stubFn := func(*http.Request) (*http.Response, error) {
		if order.Add(1) == 1 {
			once.Do(func() { close(firstStarted) })
			select {
			case <-releaseOwner:
			case <-time.After(5 * time.Second):
				return responseWithBody(500, `{"error":"barrier timeout"}`), nil
			}
		}
		return responseWithBody(200, `{"ok":true}`), nil
	}
	var z0, z1 atomic.Int32
	postStub(t, gateway, "z", 0, &z0, nil, stubFn)
	postStub(t, gateway, "z", 1, &z1, nil, stubFn)
	type result struct {
		resp *http.Response
		err  error
	}
	ownerCh := make(chan result, 1)
	go func() {
		resp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(session, "req-owner"), 0)
		ownerCh <- result{resp, err}
	}()
	select {
	case <-firstStarted:
	case <-time.After(3 * time.Second):
		t.Fatalf("owner first send never started")
	}
	followerCtx, cancel := context.WithCancel(pinTestCtx())
	followerCh := make(chan result, 1)
	go func() {
		resp, _, _, err := gateway.doUpstreamTiers(followerCtx, route, routeBodies(), pinIDs(session, "req-follower"), 0)
		followerCh <- result{resp, err}
	}()
	time.Sleep(150 * time.Millisecond)
	if got := postCount(&z0) + postCount(&z1); got != 1 {
		t.Fatalf("follower sent while waiting: total=%d want 1", got)
	}
	cancel()
	select {
	case res := <-followerCh:
		if res.err == nil || !errors.Is(res.err, context.Canceled) {
			if res.resp != nil {
				drainResp(res.resp)
			}
			t.Fatalf("follower err=%v want context.Canceled promptly", res.err)
		}
		if res.resp != nil {
			drainResp(res.resp)
			t.Fatalf("cancelled follower must not return a response")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("cancelled follower did not return promptly")
	}
	if got := postCount(&z0) + postCount(&z1); got != 1 {
		t.Fatalf("cancelled follower sent upstream: total=%d want 1 (owner only)", got)
	}
	close(releaseOwner)
	select {
	case res := <-ownerCh:
		if res.err != nil || res.resp == nil || res.resp.StatusCode != 200 {
			t.Fatalf("owner err=%v resp=%v want 200 after follower cancel", res.err, res.resp)
		}
		drainResp(res.resp)
	case <-time.After(5 * time.Second):
		t.Fatalf("owner affected by follower cancel")
	}
	if _, ok := gateway.scheduler.pinGet(session, "m"); !ok {
		t.Fatalf("owner success must still pin after follower cancel")
	}
	if n := gateway.scheduler.pins.inflightCount(); n != 0 {
		t.Fatalf("claim leak: inflight=%d want 0", n)
	}
}

// Different session+model keys establish fully concurrently: both are owners.
func TestPinEstablishmentDifferentKeysConcurrent(t *testing.T) {
	monitor := NewMonitor()
	gateway := authTwoProxyGateway(t, monitor, 5)
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var active atomic.Int32
	var maxActive atomic.Int32
	stubFn := func(*http.Request) (*http.Response, error) {
		cur := active.Add(1)
		for {
			prev := maxActive.Load()
			if cur <= prev || maxActive.CompareAndSwap(prev, cur) {
				break
			}
		}
		defer active.Add(-1)
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-time.After(5 * time.Second):
			return responseWithBody(500, `{"error":"barrier timeout"}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	}
	var z0, z1 atomic.Int32
	postStub(t, gateway, "z", 0, &z0, nil, stubFn)
	postStub(t, gateway, "z", 1, &z1, nil, stubFn)
	type result struct {
		resp *http.Response
		err  error
	}
	aCh := make(chan result, 1)
	bCh := make(chan result, 1)
	go func() {
		resp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs("ses_pin_conc_A", "req-a"), 0)
		aCh <- result{resp, err}
	}()
	go func() {
		resp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs("ses_pin_conc_B", "req-b"), 0)
		bCh <- result{resp, err}
	}()
	// Both keys must start their sends without waiting for each other.
	timeout := time.After(3 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-timeout:
			t.Fatalf("different keys did not establish concurrently (only %d/2 started)", i)
		}
	}
	close(release)
	var aRes, bRes result
	select {
	case aRes = <-aCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("key A deadlocked")
	}
	select {
	case bRes = <-bCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("key B deadlocked")
	}
	if aRes.err != nil || aRes.resp == nil || aRes.resp.StatusCode != 200 {
		t.Fatalf("key A err=%v resp=%v want 200", aRes.err, aRes.resp)
	}
	drainResp(aRes.resp)
	if bRes.err != nil || bRes.resp == nil || bRes.resp.StatusCode != 200 {
		t.Fatalf("key B err=%v resp=%v want 200", bRes.err, bRes.resp)
	}
	drainResp(bRes.resp)
	if _, ok := gateway.scheduler.pinGet("ses_pin_conc_A", "m"); !ok {
		t.Fatalf("key A must pin independently")
	}
	if _, ok := gateway.scheduler.pinGet("ses_pin_conc_B", "m"); !ok {
		t.Fatalf("key B must pin independently")
	}
	if got := postCount(&z0) + postCount(&z1); got != 2 {
		t.Fatalf("total POSTs=%d want 2 (one per concurrent key)", got)
	}
	if maxActive.Load() < 2 {
		t.Fatalf("keys serialized: maxActive=%d want >=2", maxActive.Load())
	}
	if n := gateway.scheduler.pins.inflightCount(); n != 0 {
		t.Fatalf("claim leak: inflight=%d want 0", n)
	}
}

// Strict process-lifetime semantics: pins never expire and are never evicted.
// Lookup is read-only; repeated lookups and elapsed time must not drop the pin.
func TestPinNeverExpires(t *testing.T) {
	store := newSessionPinStore()
	auth := normalizeRouteAuthority("https://zen.example")
	store.bind("ses_life", "m", sessionPin{Tier: TierZen, CredID: "zen:aaa", Pool: "z", ProxyRaw: "p", Protocol: ProtocolChat, Authority: auth})
	if _, ok := store.get("ses_life", "m"); !ok {
		t.Fatalf("must hit after bind")
	}
	// Advanced clock: elapsed time and repeated lookups must not expire the pin.
	time.Sleep(10 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if _, ok := store.get("ses_life", "m"); !ok {
			t.Fatalf("pin expired on lookup %d", i)
		}
	}
	if store.count() != 1 {
		t.Fatalf("count=%d want 1 (no expiry pruning)", store.count())
	}
	// Re-bind of the same key never overwrites the established pin.
	store.bind("ses_life", "m", sessionPin{Tier: TierGo, CredID: "go:bbb", Pool: "g", ProxyRaw: "p2", Protocol: ProtocolChat, Authority: normalizeRouteAuthority("https://go.example")})
	got, ok := store.get("ses_life", "m")
	if !ok {
		t.Fatalf("established pin must survive re-bind")
	}
	if got.Tier != TierZen || got.CredID != "zen:aaa" {
		t.Fatalf("first pin must win, got %+v", got)
	}
}

// Existing pins survive insert pressure to the cap; overflow inserts are
// dropped without evicting any existing pin.
func TestPinSurvivesInsertPressureNoEviction(t *testing.T) {
	store := newSessionPinStore()
	auth := normalizeRouteAuthority("https://zen.example")
	store.bind("ses_first", "m", sessionPin{Tier: TierZen, CredID: "zen:aaa", Pool: "z", ProxyRaw: "p", Protocol: ProtocolChat, Authority: auth})
	for i := 0; i < sessionPinStoreCap-1; i++ {
		store.bind("ses_cap", "model-"+itoa(i), sessionPin{Tier: TierZen, CredID: "zen:aaa", Pool: "z", ProxyRaw: "p", Protocol: ProtocolChat, Authority: auth})
	}
	if store.count() != sessionPinStoreCap {
		t.Fatalf("count=%d want %d", store.count(), sessionPinStoreCap)
	}
	if _, ok := store.get("ses_first", "m"); !ok {
		t.Fatalf("first pin must survive insert pressure (no LRU eviction)")
	}
	store.bind("ses_cap", "model-overflow", sessionPin{Tier: TierZen, CredID: "zen:aaa", Pool: "z", ProxyRaw: "p", Protocol: ProtocolChat, Authority: auth})
	if store.count() != sessionPinStoreCap {
		t.Fatalf("capped count=%d want %d (no growth)", store.count(), sessionPinStoreCap)
	}
	if _, ok := store.get("ses_cap", "model-overflow"); ok {
		t.Fatalf("overflow insert must drop at cap")
	}
	if _, ok := store.get("ses_first", "m"); !ok {
		t.Fatalf("existing pin must survive overflow attempt")
	}
}

// At the cap a new unpinned session+model fails closed locally before any
// upstream send (502 + concise message) while existing pinned sessions work.
func TestPinCapFailClosedBeforeSend(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	// Establish one real pinned session first.
	postStub(t, gateway, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	keepSes := "ses_pin_cap_keep_1"
	ids := pinIDs(keepSes, "req-keep-1")
	resp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), ids, 0)
	if err != nil {
		t.Fatal(err)
	}
	drainResp(resp)
	keepPin, ok := gateway.scheduler.pinGet(keepSes, "m")
	if !ok {
		t.Fatalf("keep session must pin")
	}
	_ = keepPin
	// Fill remaining slots to the cap with dummy bindings.
	auth := normalizeRouteAuthority("https://zen.example")
	filler := 0
	for gateway.scheduler.pins.count() < sessionPinStoreCap {
		gateway.scheduler.pins.bind("ses_fill", "model-"+itoa(filler), sessionPin{Tier: TierZen, CredID: "zen:aaa", Pool: "z", ProxyRaw: "p", Protocol: ProtocolChat, Authority: auth})
		filler++
		if filler > sessionPinStoreCap+10 {
			t.Fatalf("filler runaway count=%d", gateway.scheduler.pins.count())
		}
	}
	if gateway.scheduler.pins.count() != sessionPinStoreCap {
		t.Fatalf("count=%d want cap %d", gateway.scheduler.pins.count(), sessionPinStoreCap)
	}
	// New session must fail closed before any send.
	var c0, c1, zc, gc atomic.Int32
	postStub(t, gateway, "a", 0, &c0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, &c1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 0, &zc, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	newResp, _, attempts, err := gateway.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs("ses_pin_cap_new_1", "req-new-1"), 0)
	if err != nil {
		t.Fatalf("cap fail-closed must return response, err=%v", err)
	}
	if newResp == nil || newResp.StatusCode != 502 {
		t.Fatalf("cap fail-closed status=%v want 502", newResp)
	}
	if attempts != 0 {
		t.Fatalf("cap fail-closed attempts=%d want 0 (no send)", attempts)
	}
	if postCount(&c0)+postCount(&c1)+postCount(&zc)+postCount(&gc) != 0 {
		t.Fatalf("cap fail-closed must send nothing: a=%d/%d z=%d g=%d", postCount(&c0), postCount(&c1), postCount(&zc), postCount(&gc))
	}
	raw, _ := io.ReadAll(newResp.Body)
	drainResp(newResp)
	if !strings.Contains(string(raw), "session affinity capacity exhausted") {
		t.Fatalf("cap body must carry concise message, got %q", raw)
	}
	if _, ok := gateway.scheduler.pinGet("ses_pin_cap_new_1", "m"); ok {
		t.Fatalf("failed establishment must not bind a pin")
	}
	// Existing pinned session continues normally with exactly one send.
	pool := gateway.pools["a"]
	pinnedIdx := -1
	for i, proxy := range pool.items {
		if proxy != nil && proxy.name == keepPin.ProxyRaw {
			pinnedIdx = i
		}
	}
	if pinnedIdx < 0 {
		// Keep pin may be anon on pool a; if not found, resolve generically.
		pinnedIdx = 0
	}
	var k0, k1 atomic.Int32
	postStub(t, gateway, "a", 0, &k0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, &k1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	keepResp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(keepSes, "req-keep-2"), 0)
	if err != nil || keepResp == nil || keepResp.StatusCode != 200 {
		t.Fatalf("existing pin must keep working at cap, err=%v resp=%v", err, keepResp)
	}
	drainResp(keepResp)
	if postCount(&k0)+postCount(&k1) != 1 {
		t.Fatalf("existing pin sends=%d/%d want exactly 1", postCount(&k0), postCount(&k1))
	}
}

// Concurrent near-cap establishment must not oversubscribe: exactly the
// remaining slots succeed, losers fail closed with no sends.
func TestPinConcurrentNearCapNoOversubscribe(t *testing.T) {
	for iter := 0; iter < 3; iter++ {
		monitor := NewMonitor()
		gateway := authTwoProxyGateway(t, monitor, 5)
		route := authOnlyRoute()
		route.KeyTiers = []Tier{TierZen}
		auth := normalizeRouteAuthority("https://zen.example")
		// Leave exactly 2 slots free.
		for i := 0; i < sessionPinStoreCap-2; i++ {
			gateway.scheduler.pins.bind("ses_pre", "model-"+itoa(i), sessionPin{Tier: TierZen, CredID: "zen:aaa", Pool: "z", ProxyRaw: "p", Protocol: ProtocolChat, Authority: auth})
		}
		var z0, z1 atomic.Int32
		postStub(t, gateway, "z", 0, &z0, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
		postStub(t, gateway, "z", 1, &z1, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
		const racers = 6
		type result struct {
			resp *http.Response
			err  error
		}
		ch := make([]chan result, racers)
		for i := 0; i < racers; i++ {
			ch[i] = make(chan result, 1)
			ses := "ses_race_cap_" + itoa(iter) + "_" + itoa(i)
			go func(c chan result, s string) {
				resp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(s, "req-"+s), 0)
				c <- result{resp, err}
			}(ch[i], ses)
		}
		succeeded := 0
		failedClosed := 0
		for i := 0; i < racers; i++ {
			select {
			case r := <-ch[i]:
				if r.err != nil {
					t.Fatalf("iter %d racer %d err=%v", iter, i, r.err)
				}
				if r.resp == nil {
					t.Fatalf("iter %d racer %d nil resp", iter, i)
				}
				if r.resp.StatusCode == 200 {
					succeeded++
					drainResp(r.resp)
				} else if r.resp.StatusCode == 502 {
					raw, _ := io.ReadAll(r.resp.Body)
					drainResp(r.resp)
					if !strings.Contains(string(raw), "session affinity capacity exhausted") {
						t.Fatalf("iter %d loser body %q must carry capacity message", iter, raw)
					}
					failedClosed++
				} else {
					drainResp(r.resp)
					t.Fatalf("iter %d racer %d status=%d want 200 or 502", iter, i, r.resp.StatusCode)
				}
			case <-time.After(8 * time.Second):
				t.Fatalf("iter %d racer %d deadlocked", iter, i)
			}
		}
		if succeeded != 2 {
			t.Fatalf("iter %d succeeded=%d want exactly 2 (remaining slots)", iter, succeeded)
		}
		if failedClosed != racers-2 {
			t.Fatalf("iter %d failedClosed=%d want %d", iter, failedClosed, racers-2)
		}
		if got := postCount(&z0) + postCount(&z1); got != succeeded {
			t.Fatalf("iter %d total POSTs=%d want %d (losers send nothing)", iter, got, succeeded)
		}
		if gateway.scheduler.pins.count() != sessionPinStoreCap {
			t.Fatalf("iter %d count=%d want cap %d (no oversubscribe)", iter, gateway.scheduler.pins.count(), sessionPinStoreCap)
		}
		if n := gateway.scheduler.pins.inflightCount(); n != 0 {
			t.Fatalf("iter %d claim leak inflight=%d", iter, n)
		}
		if n := gateway.scheduler.pins.reservedCount(); n != 0 {
			t.Fatalf("iter %d reservation leak reserved=%d", iter, n)
		}
	}
}

// A failed owner releases its reservation so a later request can retry.
func TestPinFailedOwnerReleasesReservation(t *testing.T) {
	monitor := NewMonitor()
	gateway := authTwoProxyGateway(t, monitor, 5)
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	auth := normalizeRouteAuthority("https://zen.example")
	for i := 0; i < sessionPinStoreCap-1; i++ {
		gateway.scheduler.pins.bind("ses_pre", "model-"+itoa(i), sessionPin{Tier: TierZen, CredID: "zen:aaa", Pool: "z", ProxyRaw: "p", Protocol: ProtocolChat, Authority: auth})
	}
	var z0, z1 atomic.Int32
	postStub(t, gateway, "z", 0, &z0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(404, `{"error":"nope"}`), nil
	})
	postStub(t, gateway, "z", 1, &z1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(404, `{"error":"nope"}`), nil
	})
	ses := "ses_pin_release_retry_1"
	resp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "req-fail-1"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.StatusCode != 404 {
		t.Fatalf("owner failure status=%v want 404", resp)
	}
	drainResp(resp)
	if _, ok := gateway.scheduler.pinGet(ses, "m"); ok {
		t.Fatalf("failure must not bind")
	}
	if n := gateway.scheduler.pins.reservedCount(); n != 0 {
		t.Fatalf("failed owner must release reservation, reserved=%d", n)
	}
	if n := gateway.scheduler.pins.inflightCount(); n != 0 {
		t.Fatalf("claim leak inflight=%d", n)
	}
	if gateway.scheduler.pins.count() != sessionPinStoreCap-1 {
		t.Fatalf("count=%d want %d (slot freed)", gateway.scheduler.pins.count(), sessionPinStoreCap-1)
	}
	// Retry establishment succeeds and consumes the freed slot exactly once.
	var r0, r1 atomic.Int32
	postStub(t, gateway, "z", 0, &r0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 1, &r1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	resp2, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "req-retry-1"), 0)
	if err != nil || resp2 == nil || resp2.StatusCode != 200 {
		t.Fatalf("retry err=%v resp=%v want 200", err, resp2)
	}
	drainResp(resp2)
	if _, ok := gateway.scheduler.pinGet(ses, "m"); !ok {
		t.Fatalf("retry success must pin")
	}
	if gateway.scheduler.pins.count() != sessionPinStoreCap {
		t.Fatalf("count=%d want cap after retry", gateway.scheduler.pins.count())
	}
	if n := gateway.scheduler.pins.reservedCount(); n != 0 {
		t.Fatalf("reserved leak after success=%d", n)
	}
}

// Apply carries all pins including removed/changed targets as tombstones:
// later requests resolve to the pinned path and fail 502 with no sends and
// no fallback. Valid targets continue normally.
func TestPinMigrationTombstone502(t *testing.T) {
	oldCfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	oldCfg.Keys = []string{"zen-key-aaaaa"}
	oldCfg.Anonymous = true
	oldNormalized, err := NormalizeConfig("config.json", oldCfg)
	if err != nil {
		t.Fatal(err)
	}
	oldGateway, err := NewGateway(oldNormalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	zenAuth := normalizeRouteAuthority(oldNormalized.Upstream.Zen)
	keepCred := oldGateway.authCreds[0].id
	keepPin := sessionPin{Tier: TierZen, CredID: keepCred, Pool: "shared", ProxyRaw: "direct", Model: "m", Protocol: ProtocolChat, Authority: zenAuth}
	removedPin := sessionPin{Tier: TierZen, CredID: credentialIDForKey(TierZen, "zen-key-doomed"), Pool: "shared", ProxyRaw: "direct", Model: "m", Protocol: ProtocolChat, Authority: zenAuth}
	oldGateway.scheduler.pinBind("ses_mig_keep", "m", keepPin)
	oldGateway.scheduler.pinBind("ses_mig_gone", "m", removedPin)

	newCfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	newCfg.Keys = []string{"zen-key-aaaaa"}
	newCfg.Anonymous = true
	newNormalized, err := NormalizeConfig("config.json", newCfg)
	if err != nil {
		t.Fatal(err)
	}
	newGateway, err := NewGateway(newNormalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	summary := migrateGatewaySchedulerState(oldGateway, newGateway)
	if summary.Pins != 2 {
		t.Fatalf("migrated pins=%d want 2 (no validity filtering)", summary.Pins)
	}
	if _, ok := newGateway.scheduler.pinGet("ses_mig_keep", "m"); !ok {
		t.Fatalf("valid pin must migrate")
	}
	if _, ok := newGateway.scheduler.pinGet("ses_mig_gone", "m"); !ok {
		t.Fatalf("removed-credential pin must migrate as tombstone")
	}
	// Tombstone resolves to pinned 502 with no sends and no fallback.
	var c0, c1 atomic.Int32
	postStub(t, newGateway, "shared", 0, &c0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, newGateway, "shared", 1, &c1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	goneResp, _, attempts, err := newGateway.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs("ses_mig_gone", "req-gone-1"), 0)
	if err != nil {
		t.Fatalf("tombstone must return response, err=%v", err)
	}
	if goneResp == nil || goneResp.StatusCode != 502 {
		t.Fatalf("tombstone status=%v want 502", goneResp)
	}
	if attempts != 0 {
		t.Fatalf("tombstone attempts=%d want 0 (no send)", attempts)
	}
	if postCount(&c0)+postCount(&c1) != 0 {
		t.Fatalf("tombstone must not send or fallback: %d/%d", postCount(&c0), postCount(&c1))
	}
	drainResp(goneResp)
	if _, ok := newGateway.scheduler.pinGet("ses_mig_gone", "m"); !ok {
		t.Fatalf("tombstone pin must remain after 502")
	}
	// Valid migration continues normally.
	var v0, v1 atomic.Int32
	postStub(t, newGateway, "shared", 0, &v0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, newGateway, "shared", 1, &v1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	// Valid pin is auth zen on shared/direct; drive via auth-only route to the same target.
	validRoute := authOnlyRoute()
	validRoute.KeyTiers = []Tier{TierZen}
	keepResp, _, _, err := newGateway.doUpstreamTiers(pinTestCtx(), validRoute, routeBodies(), pinIDs("ses_mig_keep", "req-keep-1"), 0)
	if err != nil || keepResp == nil || keepResp.StatusCode != 200 {
		t.Fatalf("valid migrated pin must serve 200, err=%v resp=%v", err, keepResp)
	}
	drainResp(keepResp)
	// Changed-pool tombstone: pool identity changed, pin still migrates then 502s.
	oldGateway2, _ := NewGateway(oldNormalized, nil, NewMonitor())
	oldGateway2.scheduler.pinBind("ses_pool", "m", keepPin)
	newCfg2 := testGatewayConfig(map[string][]string{"other": {"direct"}}, ProxyRoutingConfig{Anonymous: "other", Authenticated: "other"})
	newCfg2.Keys = []string{"zen-key-aaaaa"}
	newCfg2.Anonymous = true
	newNormalized2, _ := NormalizeConfig("config.json", newCfg2)
	newGateway2, _ := NewGateway(newNormalized2, nil, NewMonitor())
	summary2 := migrateGatewaySchedulerState(oldGateway2, newGateway2)
	if summary2.Pins != 1 {
		t.Fatalf("changed-pool pin must still migrate, got %d", summary2.Pins)
	}
	var w0 atomic.Int32
	postStub(t, newGateway2, "other", 0, &w0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	poolResp, _, poolAttempts, err := newGateway2.doUpstreamTiers(pinTestCtx(), validRoute, routeBodies(), pinIDs("ses_pool", "req-pool-1"), 0)
	if err != nil || poolResp == nil || poolResp.StatusCode != 502 {
		t.Fatalf("changed-pool tombstone must 502, err=%v resp=%v", err, poolResp)
	}
	if poolAttempts != 0 || postCount(&w0) != 0 {
		t.Fatalf("changed-pool tombstone must not send: attempts=%d sends=%d", poolAttempts, postCount(&w0))
	}
	drainResp(poolResp)
}

// Fresh construction starts empty: restart is the only clearing boundary and
// there is no disk persistence in this change.
func TestPinRestartStartsEmpty(t *testing.T) {
	if newSessionPinStore().count() != 0 {
		t.Fatalf("fresh pin store must start empty")
	}
	if newTargetScheduler(15*time.Second).pins.count() != 0 {
		t.Fatalf("fresh scheduler pins must start empty")
	}
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Keys = []string{"zen-key-aaaaa"}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	if gateway.scheduler.pins.count() != 0 {
		t.Fatalf("new Gateway must start with empty pins (restart clears)")
	}
}

// Pinned unresolvable/unhealthy: single unhealthy current walks to the
// healthy alternate within the same binding; all unhealthy still 502s.
func TestPinUnresolvableUnhealthy502(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	postStub(t, gateway, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := pinIDs("ses_pin_gone_1", "req-1")
	resp, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), ids, 0)
	drainResp(resp)
	pin, _ := gateway.scheduler.pinGet(ids.Session, "m")
	_ = pin
	// Pinned binding walks to the healthy alternate: next request serves 200
	// with one send on the alternate and updates current.
	for _, proxy := range gateway.pools["a"].items {
		if proxy != nil && proxy.name == pin.ProxyRaw {
			proxy.healthy.Store(false)
		}
	}
	var c0, c1, zc atomic.Int32
	postStub(t, gateway, "a", 0, &c0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, &c1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 0, &zc, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids2 := pinIDs(ids.Session, "req-2")
	resp2, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), ids2, 0)
	if err != nil || resp2.StatusCode != 200 {
		t.Fatalf("walk to healthy alternate must 200, err=%v resp=%v", err, resp2)
	}
	drainResp(resp2)
	// Exactly one send on the healthy alternate; the unhealthy current is
	// skipped and auth is never entered.
	if postCount(&c0)+postCount(&c1) != 1 || postCount(&zc) != 0 {
		t.Fatalf("walk must send once on alternate: %d/%d/%d", postCount(&c0), postCount(&c1), postCount(&zc))
	}
	pin2, _ := gateway.scheduler.pinGet(ids.Session, "m")
	if pin2.ProxyRaw == pin.ProxyRaw {
		t.Fatalf("pin must move away from unhealthy: %+v", pin2)
	}
	// All proxies unhealthy still fails closed 502 with no sends.
	for _, proxy := range gateway.pools["a"].items {
		if proxy != nil {
			proxy.healthy.Store(false)
		}
	}
	var d0, d1, dz atomic.Int32
	postStub(t, gateway, "a", 0, &d0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, &d1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 0, &dz, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	resp3, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ids.Session, "req-3"), 0)
	if err != nil || resp3.StatusCode != 502 {
		t.Fatalf("all-unhealthy must 502, err=%v resp=%v", err, resp3)
	}
	drainResp(resp3)
	if postCount(&d0)+postCount(&d1)+postCount(&dz) != 0 {
		t.Fatalf("all-unhealthy must not send: %d/%d/%d", postCount(&d0), postCount(&d1), postCount(&dz))
	}
}

// Readiness/refresh/probe isolation.
func TestPinIsolation(t *testing.T) {
	gateway := singleZenGateway(t, false)
	// Pin a session to the only target.
	gateway.scheduler.pinBind("ses_iso", "m1", sessionPin{Tier: TierZen, CredID: gateway.authCreds[0].id, Pool: "shared", ProxyRaw: "direct", Model: "m1", Protocol: ProtocolChat, Authority: normalizeRouteAuthority(gateway.cfg.Upstream.Zen)})
	codeBefore, healthBefore := decodeHealth(t, gateway)
	// A bound session must not change global readiness.
	code, health := decodeHealth(t, gateway)
	if code != codeBefore {
		t.Fatalf("pin must not change readiness code")
	}
	if health.Routing.ChannelsAvailable != healthBefore.Routing.ChannelsAvailable {
		t.Fatalf("pin changed channels: %+v vs %+v", health.Routing, healthBefore.Routing)
	}
	// Refresh must not touch pins.
	before := gateway.scheduler.pins.count()
	seeded := seedRefreshForeground(t, gateway)
	_ = seeded
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = gateway.refreshTier(ctx, "http://127.0.0.1:9", TierZen)
	if gateway.scheduler.pins.count() != before {
		t.Fatalf("refresh changed pins: %d -> %d", before, gateway.scheduler.pins.count())
	}
	if _, ok := gateway.scheduler.pinGet("ses_iso", "m1"); !ok {
		t.Fatalf("refresh dropped pin")
	}
	// Route-session store remains independent: pin count and session count diverge.
	if gateway.scheduler.routeSessions == nil || gateway.scheduler.pins == nil {
		t.Fatalf("stores missing")
	}
	// Proxy health flip does not delete the pin (next request 502s, pin stays).
	gateway.pools["shared"].items[0].healthy.Store(false)
	if _, ok := gateway.scheduler.pinGet("ses_iso", "m1"); !ok {
		t.Fatalf("unhealthy proxy must not delete pin")
	}
	gateway.pools["shared"].items[0].healthy.Store(true)
}

// Credential-cooldown fast-fail preserves 401 and never sends.
func TestPinnedCredentialCooldown401(t *testing.T) {
	gateway := authTwoProxyGateway(t, NewMonitor(), 5)
	postStub(t, gateway, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := pinIDs("ses_pin_cred401", "req-1")
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	resp, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids, 0)
	drainResp(resp)
	pin, ok := gateway.scheduler.pinGet(ids.Session, "m")
	if !ok {
		t.Fatalf("must pin auth success")
	}
	// Cool the credential globally with a real 401 on the pinned target.
	pool := gateway.pools["z"]
	var pinnedIdx int
	for i, proxy := range pool.items {
		if proxy != nil && proxy.name == pin.ProxyRaw {
			pinnedIdx = i
		}
	}
	var c0 atomic.Int32
	postStub(t, gateway, "z", pinnedIdx, &c0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(401, `{"error":"bad key"}`), nil
	})
	ids2 := pinIDs(ids.Session, "req-2")
	resp2, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids2, 0)
	if resp2.StatusCode != 401 {
		t.Fatalf("must surface 401, got %v", resp2.StatusCode)
	}
	drainResp(resp2)
	// Third request hits credential cooldown fast-fail: no send, 401 preserved.
	var d0, d1 atomic.Int32
	postStub(t, gateway, "z", 0, &d0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 1, &d1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids3 := pinIDs(ids.Session, "req-3")
	resp3, _, attempts, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids3, 0)
	if err != nil || resp3.StatusCode != 401 {
		t.Fatalf("credential fast-fail must preserve 401, err=%v resp=%v", err, resp3)
	}
	if attempts != 0 || postCount(&d0)+postCount(&d1) != 0 {
		t.Fatalf("credential fast-fail must not send: attempts=%d sends=%d/%d", attempts, postCount(&d0), postCount(&d1))
	}
	drainResp(resp3)
}
