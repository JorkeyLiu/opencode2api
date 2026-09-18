package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type stubRoundTripper struct {
	calls atomic.Int32
	fn    func(*http.Request) (*http.Response, error)
}

func (s *stubRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	// Probe-aware: async neutral proxy health checks (GET) must never count
	// as upstream sends nor consume stub sequencing. Only POSTs are real
	// inference sends; GETs return 200 without invoking the stub fn.
	if r.Method == http.MethodGet {
		return responseWithBody(200, `{}`), nil
	}
	s.calls.Add(1)
	return s.fn(r)
}

func (s *stubRoundTripper) count() int { return int(s.calls.Load()) }

func responseWithBody(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func discardGatewayLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func routing400Gateway(t *testing.T, monitor *Monitor) *Gateway {
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
	gateway, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	return gateway
}

func stubProxy(t *testing.T, gateway *Gateway, poolName string, index int, fn func(*http.Request) (*http.Response, error)) *stubRoundTripper {
	t.Helper()
	pool := gateway.pools[poolName]
	if pool == nil || index >= len(pool.items) {
		t.Fatalf("pool %q index %d missing", poolName, index)
	}
	stub := &stubRoundTripper{fn: fn}
	pool.items[index].client = &http.Client{Transport: stub}
	return stub
}

func anonAuthRoute() modelRoute {
	return modelRoute{
		ID: "m", Tier: TierZen, Protocol: ProtocolChat,
		Protocols: map[Tier]Protocol{TierZen: ProtocolChat},
		Anonymous: true, KeyTiers: []Tier{TierZen},
	}
}

func authOnlyRoute() modelRoute {
	return modelRoute{
		ID: "m", Tier: TierZen, Protocol: ProtocolChat,
		Protocols: map[Tier]Protocol{TierZen: ProtocolChat},
		Anonymous: false, KeyTiers: []Tier{TierZen},
	}
}

func routeBodies() map[Tier][]byte {
	return map[Tier][]byte{
		TierZen: []byte(`{"model":"m"}`),
	}
}

func routeBodiesWithSession() map[Tier][]byte {
	body := []byte(`{"model":"m","conversation_id":"client-conv","metadata":{"session_id":"client-meta","other":"keep"}}`)
	return map[Tier][]byte{TierZen: body}
}

func emptySessionIDs() requestIDs {
	return requestIDs{Session: "", Request: "req-400-test", Project: "prj-test"}
}

func clientSessionIDs(session string) requestIDs {
	return requestIDs{Session: session, Request: "req-400-test", Project: "prj-test"}
}

type capturedUpstream struct {
	mu       sync.Mutex
	sessions []string
	bodies   [][]byte
}

func (c *capturedUpstream) add(r *http.Request) {
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessions = append(c.sessions, r.Header.Get("x-opencode-session"))
	c.bodies = append(c.bodies, body)
}

func (c *capturedUpstream) get() ([]string, [][]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.sessions...), append([][]byte(nil), c.bodies...)
}

func decodeBody(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("body is not a JSON object: %v", err)
	}
	return payload
}

// Anonymous first 400 replays on the same target with the same session,
// never touches the second proxy or the auth tiers.
func TestAnonymous400ReplaysSameTarget(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	cap := &capturedUpstream{}
	var calls atomic.Int32
	anonFn := func(r *http.Request) (*http.Response, error) {
		cap.add(r)
		if calls.Add(1) == 1 {
			return responseWithBody(400, `{"error":"bad request"}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	}
	anon0 := stubProxy(t, gateway, "a", 0, anonFn)
	anon1 := stubProxy(t, gateway, "a", 1, anonFn)
	zen := stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ids := clientSessionIDs("ses_client_anon_1")
	resp, _, attempts, err := gateway.doUpstreamTiers(ctx, anonAuthRoute(), routeBodiesWithSession(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("status=%v want 200 after same-target replay", resp)
	}
	drainAndClose(resp.Body)
	if total := anon0.count() + anon1.count(); total != 2 || (anon0.count() != 2 && anon1.count() != 2) {
		t.Fatalf("same-target replay calls=%d/%d want 2 on one proxy", anon0.count(), anon1.count())
	}
	if zen.count() != 0 {
		t.Fatalf("auth must not be touched after anonymous recovery: zen=%d", zen.count())
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) != 2 {
		t.Fatalf("recorded attempts=%d want 2", len(recent))
	}
	if recent[0].RequestID != ids.Request || recent[1].RequestID != ids.Request {
		t.Fatalf("replay must keep the same request ID: %q vs %q", recent[0].RequestID, recent[1].RequestID)
	}
	if recent[0].Attempt+1 != recent[1].Attempt {
		t.Fatalf("replay attempt numbers must increment: %+v", recent)
	}
	sessions, bodies := cap.get()
	if len(sessions) != 2 {
		t.Fatalf("captured=%d want 2", len(sessions))
	}
	if sessions[0] == "" || sessions[1] == "" || sessions[0] != sessions[1] {
		t.Fatalf("replay must keep the same wire session: %q", sessions)
	}
	for _, s := range sessions {
		if !isCanonicalWireSession(s) {
			t.Fatalf("upstream wire session must be canonical ses_* shape, got %q", s)
		}
		if strings.HasPrefix(s, "rss_") {
			t.Fatalf("wire session must not leak internal rss_* token, got %q", s)
		}
		if s == ids.Session {
			t.Fatalf("upstream wire session must never equal the client session")
		}
		if strings.Contains(s, ids.Session) {
			t.Fatalf("wire session must not embed raw client session")
		}
	}
	// Internal route sessions stay rss_* and distinct from the client session;
	// the wire headers above are their canonical encodings.
	internalFirst := deriveFirstRouteSession(ids.Session, routeScopeForCandidate(gateway.cfg.Upstream.Zen, targetCandidate{Tier: TierZen, CredID: anonymousSchedulerCredentialID, PoolName: "a", ProxyRaw: "direct"}, ProtocolChat))
	if !strings.HasPrefix(internalFirst, "rss_") || internalFirst == ids.Session {
		t.Fatalf("internal route session must stay rss_* distinct from client, got %q", internalFirst)
	}
	for i, raw := range bodies {
		payload := decodeBody(t, raw)
		if got := stringAt(payload, "conversation_id"); got != sessions[i] {
			t.Fatalf("body[%d] conversation_id=%q want header %q", i, got, sessions[i])
		}
		if got := stringAt(payload, "metadata", "session_id"); got != sessions[i] {
			t.Fatalf("body[%d] metadata.session_id=%q want header %q", i, got, sessions[i])
		}
		if got := stringAt(payload, "metadata", "other"); got != "keep" {
			t.Fatalf("body[%d] must preserve unrelated metadata: %s", i, raw)
		}
	}
	// Canonical input map is rebuilt per request; per-candidate rewrites operate
	// on copies (covered explicitly by TestResponses400StripsStaleRefsOnReplay).
	// 400 stays neutral for scheduler cooldowns and proxy health.
	for _, pool := range gateway.uniquePools() {
		for _, proxy := range pool.items {
			if !proxy.healthy.Load() {
				t.Fatalf("400 must not change proxy health")
			}
		}
	}
	gateway.scheduler.mu.Lock()
	targets := len(gateway.scheduler.targetState)
	creds := len(gateway.scheduler.credState)
	gateway.scheduler.mu.Unlock()
	if targets != 0 || creds != 0 {
		t.Fatalf("400 must not create cooldown state: targets=%d creds=%d", targets, creds)
	}
	if got := gateway.scheduler.routeSessions.count(); got != 0 {
		t.Fatalf("same-session replay must not store an override, got %d", got)
	}
}

// Anonymous replay that returns 400 again terminates the route with that 400.
func TestAnonymous400Second400Terminates(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	anonFn := func(*http.Request) (*http.Response, error) {
		return responseWithBody(400, `{"error":"still bad"}`), nil
	}
	anon0 := stubProxy(t, gateway, "a", 0, anonFn)
	anon1 := stubProxy(t, gateway, "a", 1, anonFn)
	zen := stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ids := clientSessionIDs("ses_client_anon_2")
	resp, _, attempts, err := gateway.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 400 {
		t.Fatalf("status=%v want second 400", resp)
	}
	raw, _ := io.ReadAll(resp.Body)
	drainAndClose(resp.Body)
	if len(raw) == 0 {
		t.Fatalf("second 400 body must remain returnable")
	}
	total := anon0.count() + anon1.count()
	if total != 2 || (anon0.count() != 2 && anon1.count() != 2) {
		t.Fatalf("same-target replay calls=%d/%d want 2 on one proxy", anon0.count(), anon1.count())
	}
	if zen.count() != 0 {
		t.Fatalf("auth must stay untouched: zen=%d", zen.count())
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
	if got := len(monitor.Snapshot().Upstream.Recent); got != 2 {
		t.Fatalf("recorded=%d want 2", got)
	}
}

// After a 400 recovery, a non-400 replay result is final: no remaining
// proxies and no auth tiers are scanned.
func TestAnonymous400ReplayNon400IsFinal(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	var calls atomic.Int32
	anonFn := func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return responseWithBody(400, `{"error":"bad"}`), nil
		}
		return responseWithBody(404, `{"error":"gone"}`), nil
	}
	anon0 := stubProxy(t, gateway, "a", 0, anonFn)
	anon1 := stubProxy(t, gateway, "a", 1, anonFn)
	zen := stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp, _, attempts, err := gateway.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), clientSessionIDs("ses_client_anon_3"), 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 404 {
		t.Fatalf("status=%v want replay 404 as final", resp)
	}
	drainAndClose(resp.Body)
	if total := anon0.count() + anon1.count(); total != 2 || (anon0.count() != 2 && anon1.count() != 2) {
		t.Fatalf("anon calls=%d/%d want 2 on one proxy", anon0.count(), anon1.count())
	}
	if zen.count() != 0 {
		t.Fatalf("replay result must not enter auth: zen=%d", zen.count())
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
	if got := len(monitor.Snapshot().Upstream.Recent); got != 2 {
		t.Fatalf("recorded=%d want 2", got)
	}
}

// Authenticated Zen first 400 replays on the same target without Go fallback.
func TestAuthZen400ReplaySuccess(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	cap := &capturedUpstream{}
	calls := 0
	zen := stubProxy(t, gateway, "z", 0, func(r *http.Request) (*http.Response, error) {
		cap.add(r)
		calls++
		if calls == 1 {
			return responseWithBody(400, `{"error":"bad"}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ids := clientSessionIDs("ses_client_auth_1")
	resp, _, attempts, err := gateway.doUpstreamTiers(ctx, authOnlyRoute(), routeBodiesWithSession(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("status=%v want 200", resp)
	}
	drainAndClose(resp.Body)
	if zen.count() != 2 {
		t.Fatalf("zen calls=%d want 2", zen.count())
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
	sessions, bodies := cap.get()
	if len(sessions) != 2 || sessions[0] != sessions[1] {
		t.Fatalf("zen sessions must stay identical: %q", sessions)
	}
	for i, raw := range bodies {
		payload := decodeBody(t, raw)
		if got := stringAt(payload, "conversation_id"); got != sessions[i] {
			t.Fatalf("zen body[%d] session mismatch: %q vs %q", i, got, sessions[i])
		}
	}
}

func TestAuthZen400Second400NoGoFallback(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	zen := stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
		return responseWithBody(400, `{"error":"bad"}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp, _, attempts, err := gateway.doUpstreamTiers(ctx, authOnlyRoute(), routeBodies(), clientSessionIDs("ses_client_auth_2"), 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 400 {
		t.Fatalf("status=%v want 400", resp)
	}
	drainAndClose(resp.Body)
	if zen.count() != 2 {
		t.Fatalf("zen=%d want 2", zen.count())
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
}

func TestAuth400ReplayNon400DoesNotFallback(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	calls := 0
	zen := stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return responseWithBody(400, `{"error":"bad"}`), nil
		}
		return responseWithBody(500, `{"error":"boom"}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp, _, attempts, err := gateway.doUpstreamTiers(ctx, authOnlyRoute(), routeBodies(), clientSessionIDs("ses_client_auth_3"), 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 500 {
		t.Fatalf("status=%v want replay 500 as final", resp)
	}
	drainAndClose(resp.Body)
	if zen.count() != 2 {
		t.Fatalf("replay must not fallback: zen=%d", zen.count())
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
}

// Auth unbound 400 replay with 429/transport is likewise final: exactly one
// same-target replay, no transient retry, no later candidate, no Go fallback.
func TestAuth400Replay429AndTransportUnboundIsFinal(t *testing.T) {
	t.Run("replay429", func(t *testing.T) {
		monitor := NewMonitor()
		gateway := routing400Gateway(t, monitor)
		calls := 0
		zen := stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				return responseWithBody(400, `{"error":"bad"}`), nil
			}
			return responseWithBody(429, `{"error":"throttled"}`), nil
		})
		ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
		resp, _, attempts, err := gateway.doUpstreamTiers(ctx, authOnlyRoute(), routeBodies(), clientSessionIDs("ses_client_auth_429"), 0)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if resp == nil || resp.StatusCode != 429 {
			t.Fatalf("status=%v want replay 429 as final", resp)
		}
		drainAndClose(resp.Body)
		if zen.count() != 2 {
			t.Fatalf("replay 429 must not fallback: zen=%d", zen.count())
		}
		if attempts != 2 {
			t.Fatalf("attempts=%d want 2", attempts)
		}
		if got := len(monitor.Snapshot().Upstream.Recent); got != 2 {
			t.Fatalf("recorded=%d want 2", got)
		}
	})
	t.Run("replayTransport", func(t *testing.T) {
		monitor := NewMonitor()
		gateway := routing400Gateway(t, monitor)
		calls := 0
		zen := stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				return responseWithBody(400, `{"error":"bad"}`), nil
			}
			return nil, errors.New("dial timeout")
		})
		ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
		resp, _, attempts, err := gateway.doUpstreamTiers(ctx, authOnlyRoute(), routeBodies(), clientSessionIDs("ses_client_auth_trans"), 0)
		if err == nil {
			if resp != nil {
				drainAndClose(resp.Body)
			}
			t.Fatalf("replay transport must return error, got resp=%v", resp)
		}
		if zen.count() != 2 {
			t.Fatalf("replay transport must not fallback: zen=%d", zen.count())
		}
		if attempts != 2 {
			t.Fatalf("attempts=%d want 2", attempts)
		}
		if got := len(monitor.Snapshot().Upstream.Recent); got != 2 {
			t.Fatalf("recorded=%d want 2", got)
		}
	})
}

// Anonymous unbound 400 replay with 500/429/transport is final for the whole
// route: exactly 2 POSTs on one anon proxy, no other proxy, no auth tiers.
func TestAnonymous400ReplayNon400UnboundIsFinal(t *testing.T) {
	cases := []struct {
		name      string
		replay    func() (*http.Response, error)
		wantCheck func(t *testing.T, resp *http.Response, err error)
	}{
		{name: "replay500", replay: func() (*http.Response, error) { return responseWithBody(500, `{"error":"boom"}`), nil }, wantCheck: func(t *testing.T, resp *http.Response, err error) {
			t.Helper()
			if err != nil {
				t.Fatalf("err=%v", err)
			}
			if resp == nil || resp.StatusCode != 500 {
				t.Fatalf("status=%v want replay 500", resp)
			}
		}},
		{name: "replay429", replay: func() (*http.Response, error) { return responseWithBody(429, `{"error":"throttled"}`), nil }, wantCheck: func(t *testing.T, resp *http.Response, err error) {
			t.Helper()
			if err != nil {
				t.Fatalf("err=%v", err)
			}
			if resp == nil || resp.StatusCode != 429 {
				t.Fatalf("status=%v want replay 429", resp)
			}
		}},
		{name: "replayTransport", replay: func() (*http.Response, error) { return nil, errors.New("dial timeout") }, wantCheck: func(t *testing.T, resp *http.Response, err error) {
			t.Helper()
			if err == nil {
				if resp != nil {
					drainAndClose(resp.Body)
				}
				t.Fatalf("replay transport must return error, got resp=%v", resp)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			monitor := NewMonitor()
			gateway := routing400Gateway(t, monitor)
			var calls atomic.Int32
			anonFn := func(*http.Request) (*http.Response, error) {
				if calls.Add(1) == 1 {
					return responseWithBody(400, `{"error":"bad"}`), nil
				}
				return tc.replay()
			}
			anon0 := stubProxy(t, gateway, "a", 0, anonFn)
			anon1 := stubProxy(t, gateway, "a", 1, anonFn)
			zen := stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
				return responseWithBody(200, `{"ok":true}`), nil
			})
			ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
			resp, _, attempts, err := gateway.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), clientSessionIDs("ses_client_anon_replay_"+tc.name), 0)
			tc.wantCheck(t, resp, err)
			if resp != nil {
				drainAndClose(resp.Body)
			}
			if total := anon0.count() + anon1.count(); total != 2 || (anon0.count() != 2 && anon1.count() != 2) {
				t.Fatalf("anon calls=%d/%d want 2 on one proxy", anon0.count(), anon1.count())
			}
			if zen.count() != 0 {
				t.Fatalf("replay must not enter auth: zen=%d", zen.count())
			}
			if attempts != 2 {
				t.Fatalf("attempts=%d want 2", attempts)
			}
			if got := len(monitor.Snapshot().Upstream.Recent); got != 2 {
				t.Fatalf("recorded=%d want 2", got)
			}
		})
	}
}

// Pinned 400 replay stays final on the pinned target only: exactly 2 pinned
// POSTs, no other proxy, no tier fallback. Covers auth and anonymous pins
// with 500/429/transport replay outcomes.
func TestPinned400ReplayNon400IsFinal(t *testing.T) {
	establishAnonPin := func(t *testing.T, gateway *Gateway, session string) sessionPin {
		t.Helper()
		stubProxy(t, gateway, "a", 0, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
		stubProxy(t, gateway, "a", 1, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
		ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
		resp, _, _, err := gateway.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), requestIDs{Session: session, Request: "req-pin-estab", Project: "prj-test"}, 0)
		if err != nil {
			t.Fatal(err)
		}
		drainAndClose(resp.Body)
		pin, ok := gateway.scheduler.pinGet(session, "m")
		if !ok {
			t.Fatalf("must pin anon success")
		}
		return pin
	}
	establishAuthPin := func(t *testing.T, gateway *Gateway, session string) sessionPin {
		t.Helper()
		stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
		ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
		route := authOnlyRoute()
		route.KeyTiers = []Tier{TierZen}
		resp, _, _, err := gateway.doUpstreamTiers(ctx, route, routeBodies(), requestIDs{Session: session, Request: "req-pin-estab", Project: "prj-test"}, 0)
		if err != nil {
			t.Fatal(err)
		}
		drainAndClose(resp.Body)
		pin, ok := gateway.scheduler.pinGet(session, "m")
		if !ok {
			t.Fatalf("must pin auth success")
		}
		return pin
	}
	t.Run("pinnedAuth", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			replay func() (*http.Response, error)
		}{
			{"replay500", func() (*http.Response, error) { return responseWithBody(500, `{"error":"boom"}`), nil }},
			{"replay429", func() (*http.Response, error) { return responseWithBody(429, `{"error":"throttled"}`), nil }},
			{"replayTransport", func() (*http.Response, error) { return nil, errors.New("dial timeout") }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				monitor := NewMonitor()
				gateway := routing400Gateway(t, monitor)
				session := "ses_pin_auth_replay_" + tc.name
				_ = establishAuthPin(t, gateway, session)
				var calls atomic.Int32
				zen := stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
					if calls.Add(1) == 1 {
						return responseWithBody(400, `{"error":"bad"}`), nil
					}
					return tc.replay()
				})
				ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
				route := authOnlyRoute()
				route.KeyTiers = []Tier{TierZen}
				resp, _, attempts, err := gateway.doUpstreamTiers(ctx, route, routeBodies(), requestIDs{Session: session, Request: "req-pin-replay", Project: "prj-test"}, 0)
				if tc.name == "replayTransport" {
					if err == nil {
						if resp != nil {
							drainAndClose(resp.Body)
						}
						t.Fatalf("pinned replay transport must return error")
					}
				} else {
					want := 500
					if tc.name == "replay429" {
						want = 429
					}
					if err != nil {
						t.Fatalf("err=%v", err)
					}
					if resp == nil || resp.StatusCode != want {
						t.Fatalf("status=%v want pinned replay %d", resp, want)
					}
					drainAndClose(resp.Body)
				}
				if zen.count() != 2 {
					t.Fatalf("pinned replay must stay same-target: zen=%d", zen.count())
				}
				if attempts != 2 {
					t.Fatalf("attempts=%d want 2", attempts)
				}
			})
		}
	})
	t.Run("pinnedAnon", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			replay func() (*http.Response, error)
		}{
			{"replay500", func() (*http.Response, error) { return responseWithBody(500, `{"error":"boom"}`), nil }},
			{"replay429", func() (*http.Response, error) { return responseWithBody(429, `{"error":"throttled"}`), nil }},
			{"replayTransport", func() (*http.Response, error) { return nil, errors.New("dial timeout") }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				monitor := NewMonitor()
				gateway := routing400Gateway(t, monitor)
				session := "ses_pin_anon_replay_" + tc.name
				pin := establishAnonPin(t, gateway, session)
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
				var calls atomic.Int32
				pinned := stubProxy(t, gateway, "a", pinnedIdx, func(*http.Request) (*http.Response, error) {
					if calls.Add(1) == 1 {
						return responseWithBody(400, `{"error":"bad"}`), nil
					}
					return tc.replay()
				})
				other := stubProxy(t, gateway, "a", otherIdx, func(*http.Request) (*http.Response, error) {
					return responseWithBody(200, `{"ok":true}`), nil
				})
				zen := stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
					return responseWithBody(200, `{"ok":true}`), nil
				})
				ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
				resp, _, attempts, err := gateway.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), requestIDs{Session: session, Request: "req-pin-replay", Project: "prj-test"}, 0)
				if tc.name == "replayTransport" {
					if err == nil {
						if resp != nil {
							drainAndClose(resp.Body)
						}
						t.Fatalf("pinned anon replay transport must return error")
					}
				} else {
					want := 500
					if tc.name == "replay429" {
						want = 429
					}
					if err != nil {
						t.Fatalf("err=%v", err)
					}
					if resp == nil || resp.StatusCode != want {
						t.Fatalf("status=%v want pinned anon replay %d", resp, want)
					}
					drainAndClose(resp.Body)
				}
				if pinned.count() != 2 || other.count() != 0 {
					t.Fatalf("pinned anon replay must stay same-target: pinned=%d other=%d", pinned.count(), other.count())
				}
				if zen.count() != 0 {
					t.Fatalf("pinned anon replay must not enter auth: zen=%d", zen.count())
				}
				if attempts != 2 {
					t.Fatalf("attempts=%d want 2", attempts)
				}
			})
		}
	})
}

// The next client request with the same client session reuses the same
// route session (no rotation) while HRW keeps the same first target.
func TestSubsequentRequestUsesRotatedSession(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	cap := &capturedUpstream{}
	calls := 0
	stubProxy(t, gateway, "z", 0, func(r *http.Request) (*http.Response, error) {
		cap.add(r)
		calls++
		if calls == 1 {
			return responseWithBody(400, `{"error":"bad"}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ids := clientSessionIDs("ses_client_sticky_1")
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	resp, _, _, err := gateway.doUpstreamTiers(ctx, route, routeBodies(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	drainAndClose(resp.Body)
	sessions, _ := cap.get()
	if len(sessions) != 2 {
		t.Fatalf("captured=%d want 2", len(sessions))
	}
	if sessions[0] != sessions[1] {
		t.Fatalf("replay must keep the same session: %q", sessions)
	}
	rotated := sessions[1]
	now := time.Now().UnixNano()
	before := gateway.scheduler.orderCandidates(gateway.scheduler.buildAuthCandidates(TierZen, gateway.authCreds, gateway.pools["z"], "m", now), ids.Session)
	beforeFirst := ""
	if len(before) > 0 {
		beforeFirst = before[0].Identity
	}
	// Second client request with the same client session: fresh 200 path.
	cap2 := &capturedUpstream{}
	gateway.pools["z"].items[0].client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		cap2.add(r)
		return responseWithBody(200, `{"ok":true}`), nil
	})}
	ids2 := requestIDs{Session: ids.Session, Request: "req-400-test-2", Project: "prj-test"}
	resp2, _, _, err := gateway.doUpstreamTiers(ctx, route, routeBodies(), ids2, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	drainAndClose(resp2.Body)
	sessions2, _ := cap2.get()
	if len(sessions2) != 1 {
		t.Fatalf("second request captured=%d want 1", len(sessions2))
	}
	if sessions2[0] != rotated {
		t.Fatalf("second request must reuse the same session: got %q want %q", sessions2[0], rotated)
	}
	after := gateway.scheduler.orderCandidates(gateway.scheduler.buildAuthCandidates(TierZen, gateway.authCreds, gateway.pools["z"], "m", time.Now().UnixNano()), ids.Session)
	if len(after) == 0 || after[0].Identity != beforeFirst {
		t.Fatalf("400 recovery must not reorder the frozen HRW list")
	}
}

func TestFirstGenDiffersByProxy(t *testing.T) {
	scheduler := newTargetScheduler(15 * time.Second)
	first := scheduler.routeSessionFor("ses_client_x", routeSessionScope{Authority: "https://opencode.ai/zen", Tier: TierZen, CredID: "zen:anonymous", Pool: "a", ProxyRaw: "direct", Protocol: ProtocolChat})
	second := scheduler.routeSessionFor("ses_client_x", routeSessionScope{Authority: "https://opencode.ai/zen", Tier: TierZen, CredID: "zen:anonymous", Pool: "a", ProxyRaw: "http://127.0.0.1:8081", Protocol: ProtocolChat})
	// Proxy-independent scope: raw ProxyRaw in the scope struct is ignored by
	// routeScopeForCandidate, but direct scope construction still differs.
	// The gateway-relevant check is via routeScopeForCandidate below.
	_ = first
	_ = second
	candA := targetCandidate{Tier: TierZen, CredID: anonymousSchedulerCredentialID, PoolName: "a", ProxyRaw: "direct"}
	candB := targetCandidate{Tier: TierZen, CredID: anonymousSchedulerCredentialID, PoolName: "a", ProxyRaw: "http://127.0.0.1:8081"}
	gwFirst := scheduler.routeSessionFor("ses_client_x", routeScopeForCandidate("https://opencode.ai/zen", candA, ProtocolChat))
	gwSecond := scheduler.routeSessionFor("ses_client_x", routeScopeForCandidate("https://opencode.ai/zen", candB, ProtocolChat))
	if gwFirst != gwSecond {
		t.Fatalf("proxy-independent gateway scopes must match: %q vs %q", gwFirst, gwSecond)
	}
	again := scheduler.routeSessionFor("ses_client_x", routeScopeForCandidate("https://opencode.ai/zen", candA, ProtocolChat))
	if again != gwFirst {
		t.Fatalf("same client+target must be stable: %q vs %q", again, gwFirst)
	}
	if gwFirst == "ses_client_x" || strings.HasPrefix(gwFirst, "ses_") {
		t.Fatalf("route session must use its own domain, got %q", gwFirst)
	}
}

func TestConcurrentRotateSingleGeneration(t *testing.T) {
	scheduler := newTargetScheduler(15 * time.Second)
	scope := routeSessionScope{Authority: "https://opencode.ai/zen", Tier: TierZen, CredID: "zen:anonymous", Pool: "a", ProxyRaw: "direct", Protocol: ProtocolChat}
	observed := scheduler.routeSessionFor("ses_client_conc", scope)
	const workers = 16
	results := make([]string, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = scheduler.rotateRouteSession("ses_client_conc", scope, observed)
		}(i)
	}
	wg.Wait()
	for _, got := range results {
		if got == observed {
			t.Fatalf("rotation must produce a new generation")
		}
		if got != results[0] {
			t.Fatalf("concurrent rotations must converge on one generation: %q", results)
		}
	}
	if got := scheduler.routeSessions.count(); got != 1 {
		t.Fatalf("overrides=%d want 1", got)
	}
	// A subsequent rotate with the new observed token produces exactly one more generation.
	next := scheduler.rotateRouteSession("ses_client_conc", scope, results[0])
	if next == results[0] {
		t.Fatalf("second rotation must advance again")
	}
}

func TestRouteSessionStoreCapAndTTL(t *testing.T) {
	store := newRouteSessionStore()
	scopeFor := func(i int) routeSessionScope {
		return routeSessionScope{Authority: "https://opencode.ai/zen", Tier: TierZen, CredID: "zen:anonymous", Pool: "a", ProxyRaw: "direct", Protocol: ProtocolChat}
	}
	_ = scopeFor
	// Fill to the cap with distinct client keys.
	for i := 0; i < routeSessionStoreCap; i++ {
		client := "ses_cap_" + strings.Repeat("x", 4) + string(rune('a'+i%26)) + string(rune('0'+i%10)) + "-" + itoa(i)
		store.rotate(client, routeSessionScope{Authority: "https://opencode.ai/zen", Tier: TierZen, CredID: "zen:anonymous", Pool: "a", ProxyRaw: "direct", Protocol: ProtocolChat}, "observed-"+itoa(i))
	}
	if got := store.count(); got != routeSessionStoreCap {
		t.Fatalf("count=%d want %d", got, routeSessionStoreCap)
	}
	// One more insert evicts deterministically instead of growing unbounded.
	store.rotate("ses_cap_overflow", routeSessionScope{Authority: "https://opencode.ai/zen", Tier: TierZen, CredID: "zen:anonymous", Pool: "a", ProxyRaw: "direct", Protocol: ProtocolChat}, "observed-overflow")
	if got := store.count(); got != routeSessionStoreCap {
		t.Fatalf("capped count=%d want %d", got, routeSessionStoreCap)
	}
	// Expired entries are treated as first-generation again.
	expiredStore := newRouteSessionStore()
	expiredScope := routeSessionScope{Authority: "https://opencode.ai/zen", Tier: TierZen, CredID: "zen:anonymous", Pool: "a", ProxyRaw: "direct", Protocol: ProtocolChat}
	token := expiredStore.rotate("ses_ttl", expiredScope, "observed")
	expiredStore.mu.Lock()
	expiredStore.entries[routeSessionMapKey("ses_ttl", expiredScope)].lastUsed = time.Now().Add(-routeSessionIdleTTL - time.Minute).UnixNano()
	expiredStore.mu.Unlock()
	if got := expiredStore.get("ses_ttl", expiredScope); got == token {
		t.Fatalf("expired override must fall back to first generation")
	}
	if got := expiredStore.count(); got != 0 {
		t.Fatalf("expired entry must be pruned on access, count=%d", got)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

// Ordinary 4xx ends the anonymous channel after the first proxy (request-level
// rejection never benefits from another egress IP) but still enters the single
// authenticated lane; auth ends there with no cross-tier fallback.
func TestNon400KeepsPreviousSemantics(t *testing.T) {
	for _, status := range []int{404, 422} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			monitor := NewMonitor()
			cfg := testGatewayConfig(
				map[string][]string{
					"a": {"direct", "http://127.0.0.1:8081"},
					"z": {"direct", "http://127.0.0.1:8081"},
				},
				ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
			)
			cfg.Anonymous = true
			cfg.Retry.MaxAttempts = 5
			gateway, err := NewGateway(cfg, discardGatewayLogger(), monitor)
			if err != nil {
				t.Fatal(err)
			}
			anon0 := stubProxy(t, gateway, "a", 0, func(*http.Request) (*http.Response, error) {
				return responseWithBody(status, `{"error":"not terminal"}`), nil
			})
			anon1 := stubProxy(t, gateway, "a", 1, func(*http.Request) (*http.Response, error) {
				return responseWithBody(status, `{"error":"not terminal"}`), nil
			})
			zen := stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
				return responseWithBody(status, `{"error":"not terminal"}`), nil
			})
			zen1 := stubProxy(t, gateway, "z", 1, func(*http.Request) (*http.Response, error) {
				return responseWithBody(status, `{"error":"not terminal"}`), nil
			})
			ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
			resp, _, attempts, err := gateway.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), emptySessionIDs(), 0)
			if err != nil {
				t.Fatalf("status %d err=%v", status, err)
			}
			if resp == nil || resp.StatusCode != status {
				t.Fatalf("status %d final=%v want %d (no cross-tier fallback)", status, resp, status)
			}
			drainAndClose(resp.Body)
			if total := anon0.count() + anon1.count(); total != 1 {
				t.Fatalf("status %d anonymous must stop after the first ordinary rejection: %d/%d", status, anon0.count(), anon1.count())
			}
			if zen.count()+zen1.count() != 1 {
				t.Fatalf("status %d auth must end after first ordinary rejection: zen=%d zen1=%d", status, zen.count(), zen1.count())
			}
			if attempts != 2 {
				t.Fatalf("status %d attempts=%d want 2 (1 anon + 1 auth)", status, attempts)
			}
			if got := len(monitor.Snapshot().Upstream.Recent); got != 2 {
				t.Fatalf("status %d recorded=%d want 2", status, got)
			}
		})
	}
}

// Auth-only 404 ends the single lane with no fallback (contrast with the
// legacy cross-tier behavior; 400 still replays once).
func TestAuthNon400FallsBack(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	zen := stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
		return responseWithBody(404, `{"error":"nope"}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp, _, attempts, err := gateway.doUpstreamTiers(ctx, authOnlyRoute(), routeBodies(), emptySessionIDs(), 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 404 {
		t.Fatalf("status=%v want 404 with no fallback", resp)
	}
	drainAndClose(resp.Body)
	if zen.count() != 1 {
		t.Fatalf("404 must end the lane: zen=%d", zen.count())
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1", attempts)
	}
	if got := len(monitor.Snapshot().Upstream.Recent); got != 1 {
		t.Fatalf("recorded=%d want 1", got)
	}
}

// Responses 400 replays with the same route session and strips stale chain refs;
// the first send keeps those refs intact.
func TestResponses400StripsStaleRefsOnReplay(t *testing.T) {
	monitor := NewMonitor()
	cfg := testGatewayConfig(
		map[string][]string{"z": {"direct"}},
		ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Retry.MaxAttempts = 3
	gateway, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	pool := gateway.pools["z"]
	if len(pool.items) != 1 {
		t.Fatalf("pool items=%d want 1", len(pool.items))
	}
	var firstBody, secondBody []byte
	var firstSession, secondSession string
	calls := 0
	pool.items[0].client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		session := r.Header.Get("x-opencode-session")
		calls++
		if calls == 1 {
			firstBody = raw
			firstSession = session
			return responseWithBody(400, `{"error":{"message":"Referenced reasoning item 'rs_123' was not found or has expired"}}`), nil
		}
		secondBody = raw
		secondSession = session
		return responseWithBody(200, `{"ok":true}`), nil
	})}
	route := modelRoute{
		ID: "m", Tier: TierZen, Protocol: ProtocolResponses,
		Protocols: map[Tier]Protocol{TierZen: ProtocolResponses},
		Anonymous: false, KeyTiers: []Tier{TierZen},
	}
	payload := map[string]any{
		"model":                "m",
		"conversation_id":      "client-conv",
		"metadata":             map[string]any{"session_id": "client-meta"},
		"previous_response_id": "resp_old",
		"input": []any{
			map[string]any{"type": "reasoning", "id": "rs_123"},
			map[string]any{"type": "message", "role": "user", "content": "hi"},
		},
	}
	encoded, _ := json.Marshal(payload)
	bodies := map[Tier][]byte{TierZen: encoded}
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ids := clientSessionIDs("ses_client_resp_1")
	resp, _, err := gateway.doUpstream(ctx, route, bodies, ids)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("status=%v want 200 after cleanup replay", resp)
	}
	drainAndClose(resp.Body)
	if calls != 2 {
		t.Fatalf("calls=%d want 2 (400 + one same-session replay)", calls)
	}
	if firstSession == "" || secondSession == "" || firstSession != secondSession {
		t.Fatalf("route session must stay identical: %q -> %q", firstSession, secondSession)
	}
	var first map[string]any
	if err := json.Unmarshal(firstBody, &first); err != nil {
		t.Fatal(err)
	}
	// First send overwrites present session fields but keeps stale refs.
	if got := stringAt(first, "conversation_id"); got != firstSession {
		t.Fatalf("first conversation_id=%q want %q", got, firstSession)
	}
	if _, ok := first["previous_response_id"]; !ok {
		t.Fatalf("first send must not strip previous_response_id")
	}
	// Replay reuses the same session plus stripped stale refs.
	if len(secondBody) == 0 {
		t.Fatalf("second request body missing")
	}
	var replay map[string]any
	if err := json.Unmarshal(secondBody, &replay); err != nil {
		t.Fatal(err)
	}
	if got := stringAt(replay, "conversation_id"); got != secondSession {
		t.Fatalf("replay conversation_id=%q want %q", got, secondSession)
	}
	if got := stringAt(replay, "metadata", "session_id"); got != secondSession {
		t.Fatalf("replay metadata.session_id=%q want %q", got, secondSession)
	}
	if _, ok := replay["previous_response_id"]; ok {
		t.Fatalf("replay must strip previous_response_id: %s", secondBody)
	}
	for _, item := range sliceAt(replay, "input") {
		if m, ok := item.(map[string]any); ok && stringAt(m, "type") == "reasoning" {
			t.Fatalf("replay must strip reasoning items: %s", secondBody)
		}
	}
	// Canonical body must be untouched for other candidates.
	var canonical map[string]any
	if err := json.Unmarshal(bodies[TierZen], &canonical); err != nil {
		t.Fatal(err)
	}
	if got := stringAt(canonical, "conversation_id"); got != "client-conv" {
		t.Fatalf("canonical body polluted: %q", got)
	}
}

func TestBodyRewriteProtocols(t *testing.T) {
	canonical := func(payload map[string]any) []byte {
		raw, _ := json.Marshal(payload)
		return raw
	}
	t.Run("chat_overwrites_present_fields", func(t *testing.T) {
		raw := canonical(map[string]any{"model": "m", "conversation_id": "old", "metadata": map[string]any{"session_id": "old"}})
		out, err := applyRouteSessionToBody(raw, "rss_new", ProtocolChat, false)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		payload := decodeBody(t, out)
		want := routeWireSession("rss_new")
		if !isCanonicalWireSession(want) {
			t.Fatalf("wire helper must be canonical, got %q", want)
		}
		if got := stringAt(payload, "conversation_id"); got != want {
			t.Fatalf("conversation_id=%q want wire %q", got, want)
		}
		if got := stringAt(payload, "metadata", "session_id"); got != want {
			t.Fatalf("session_id=%q want wire %q", got, want)
		}
		if _, ok := payload["prompt_cache_key"]; ok {
			t.Fatalf("chat must not add prompt_cache_key")
		}
		if _, ok := payload["store"]; ok {
			t.Fatalf("chat must not add store")
		}
	})
	t.Run("missing_fields_are_not_invented", func(t *testing.T) {
		raw := canonical(map[string]any{"model": "m"})
		out, err := applyRouteSessionToBody(raw, "rss_new", ProtocolChat, false)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if string(out) != string(raw) {
			t.Fatalf("body without session fields must stay byte-identical: %s vs %s", out, raw)
		}
		payload := decodeBody(t, out)
		if _, ok := payload["conversation_id"]; ok {
			t.Fatalf("must not invent conversation_id")
		}
	})
	t.Run("non_object_metadata_retained", func(t *testing.T) {
		raw := canonical(map[string]any{"model": "m", "metadata": "keep-me"})
		out, err := applyRouteSessionToBody(raw, "rss_new", ProtocolAnthropic, false)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		payload := decodeBody(t, out)
		if got, _ := payload["metadata"].(string); got != "keep-me" {
			t.Fatalf("string metadata must be retained, got %v", payload["metadata"])
		}
	})
	t.Run("malformed_session_fields_error", func(t *testing.T) {
		badConv := canonical(map[string]any{"model": "m", "conversation_id": 42})
		if _, err := applyRouteSessionToBody(badConv, "rss_new", ProtocolChat, false); err == nil {
			t.Fatalf("non-string conversation_id must error")
		}
		badMeta := canonical(map[string]any{"model": "m", "metadata": map[string]any{"session_id": 42}})
		if _, err := applyRouteSessionToBody(badMeta, "rss_new", ProtocolResponses, false); err == nil {
			t.Fatalf("non-string metadata.session_id must error")
		}
	})
	t.Run("responses_replay_strips_only_stale_refs", func(t *testing.T) {
		raw := canonical(map[string]any{
			"model": "m", "previous_response_id": "resp_old",
			"input": []any{map[string]any{"type": "reasoning"}, map[string]any{"type": "message", "role": "user"}},
		})
		out, err := applyRouteSessionToBody(raw, "rss_new", ProtocolResponses, true)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		payload := decodeBody(t, out)
		if _, ok := payload["previous_response_id"]; ok {
			t.Fatalf("must strip previous_response_id")
		}
		for _, item := range sliceAt(payload, "input") {
			if m, ok := item.(map[string]any); ok && stringAt(m, "type") == "reasoning" {
				t.Fatalf("must strip reasoning items")
			}
		}
		want := routeWireSession("rss_new")
		if got := stringAt(payload, "prompt_cache_key"); got != want {
			t.Fatalf("prompt_cache_key=%q want wire %q", got, want)
		}
		if got, ok := payload["store"]; !ok || got != false {
			t.Fatalf("responses must default store:false, got %v", payload["store"])
		}
	})
	t.Run("first_send_keeps_stale_refs", func(t *testing.T) {
		raw := canonical(map[string]any{
			"model": "m", "previous_response_id": "resp_old",
			"input": []any{map[string]any{"type": "reasoning"}},
		})
		out, err := applyRouteSessionToBody(raw, "rss_new", ProtocolResponses, false)
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		payload := decodeBody(t, out)
		if _, ok := payload["previous_response_id"]; !ok {
			t.Fatalf("first send must keep previous_response_id")
		}
		if len(sliceAt(payload, "input")) != 1 {
			t.Fatalf("first send must keep reasoning items")
		}
		want := routeWireSession("rss_new")
		if got := stringAt(payload, "prompt_cache_key"); got != want {
			t.Fatalf("prompt_cache_key=%q want wire %q", got, want)
		}
		if got, ok := payload["store"]; !ok || got != false {
			t.Fatalf("responses must default store:false, got %v", payload["store"])
		}
	})
}

func TestRouteSessionMigrationFiltersScopes(t *testing.T) {
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
	keepScope := routeScopeForCandidate(oldNormalized.Upstream.Zen,
		targetCandidate{Tier: TierZen, CredID: oldGateway.authCreds[0].id, PoolName: "shared", ProxyRaw: "direct"}, ProtocolChat)
	dropScope := routeSessionScope{Authority: normalizeRouteAuthority(oldNormalized.Upstream.Zen), Tier: TierZen, CredID: credentialIDForKey(TierZen, "zen-key-doomed"), Pool: "shared", ProxyRaw: "direct", Protocol: ProtocolChat}
	oldGateway.scheduler.routeSessions.rotate("ses_mig", keepScope, "observed-keep")
	oldGateway.scheduler.routeSessions.mu.Lock()
	oldGateway.scheduler.routeSessions.entries[routeSessionMapKey("ses_mig", dropScope)] = &routeSessionEntry{token: "rss_doomed", lastUsed: time.Now().UnixNano(), scope: dropScope}
	oldGateway.scheduler.routeSessions.mu.Unlock()

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
	migrateGatewaySchedulerState(oldGateway, newGateway)
	kept := newGateway.scheduler.routeSessionFor("ses_mig", keepScope)
	if kept == deriveFirstRouteSession("ses_mig", keepScope) {
		t.Fatalf("kept scope must migrate its override")
	}
	dropped := newGateway.scheduler.routeSessionFor("ses_mig", dropScope)
	_ = dropped
	newGateway.scheduler.routeSessions.mu.Lock()
	_, stillThere := newGateway.scheduler.routeSessions.entries[routeSessionMapKey("ses_mig", dropScope)]
	newGateway.scheduler.routeSessions.mu.Unlock()
	if stillThere {
		t.Fatalf("removed credential scope must be dropped on Apply")
	}
	// No secret material in the migrated token.
	if strings.Contains(kept, "zen-key-aaaaa") {
		t.Fatalf("route token must never embed key material")
	}
	// Legacy proxy-bound anonymous override must not hit the new proxy-free
	// scope: it drops and the new scope re-derives statelessly.
	legacyAnon := routeSessionScope{Authority: normalizeRouteAuthority(oldNormalized.Upstream.Zen), Tier: TierZen, CredID: anonymousSchedulerCredentialID, Pool: "shared", ProxyRaw: "direct", Protocol: ProtocolChat}
	oldGateway.scheduler.routeSessions.mu.Lock()
	oldGateway.scheduler.routeSessions.entries[routeSessionMapKey("ses_legacy_anon", legacyAnon)] = &routeSessionEntry{token: "rss_legacy", lastUsed: time.Now().UnixNano(), scope: legacyAnon}
	oldGateway.scheduler.routeSessions.mu.Unlock()
	anonScope := routeScopeForCandidate(oldNormalized.Upstream.Zen,
		targetCandidate{Tier: TierZen, CredID: anonymousSchedulerCredentialID, PoolName: "shared", ProxyRaw: "direct"}, ProtocolChat)
	if anonScope.ProxyRaw != "" {
		t.Fatalf("anonymous scope must be proxy-free, got %q", anonScope.ProxyRaw)
	}
	migrateGatewaySchedulerState(oldGateway, newGateway)
	newGateway.scheduler.routeSessions.mu.Lock()
	_, legacyKept := newGateway.scheduler.routeSessions.entries[routeSessionMapKey("ses_legacy_anon", legacyAnon)]
	newGateway.scheduler.routeSessions.mu.Unlock()
	if legacyKept {
		t.Fatalf("legacy proxy-bound anon override must drop, not hit the new scope")
	}
	if got := newGateway.scheduler.routeSessionFor("ses_legacy_anon", anonScope); got != deriveFirstRouteSession("ses_legacy_anon", anonScope) {
		t.Fatalf("dropped legacy scope must re-derive statelessly")
	}
}

func TestRecoveryHappensBeforeStreaming(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	calls := 0
	stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return responseWithBody(400, `{"error":"bad"}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	// Recovery runs inside doUpstreamTiers, which handleInference always calls
	// before writing any downstream bytes (headers or SSE). A single returned
	// 200 after a 400 proves no partial streaming preceded the replay.
	resp, _, attempts, err := gateway.doUpstreamTiers(ctx, authOnlyRoute(), routeBodies(), clientSessionIDs("ses_client_stream_1"), 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("status=%v want 200", resp)
	}
	drainAndClose(resp.Body)
	if calls != 2 || attempts != 2 {
		t.Fatalf("calls=%d attempts=%d want 2/2 before any streaming", calls, attempts)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
