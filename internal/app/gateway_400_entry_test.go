package app

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

// closeOrderBody is an observable 429 body: Close records its position in the
// shared order sequence so tests can prove stored-last429 draining happens
// before the replay request reaches upstream.
type closeOrderBody struct {
	data    io.Reader
	onClose func()
}

func (b *closeOrderBody) Read(p []byte) (int, error) { return b.data.Read(p) }

func (b *closeOrderBody) Close() error {
	if b != nil && b.onClose != nil {
		b.onClose()
	}
	return nil
}

// Unit entry gate: non-400 outcomes are never handled and preserve the input
// attempt count; an exact 400 delegates to the authoritative replay.
func TestMaybeReplayCandidate400Entry(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	route := authOnlyRoute()
	ids := clientSessionIDs("ses_unit_entry_1")
	proxy := gateway.pools["z"].items[0]
	creds := gateway.credentials()
	if len(creds) == 0 {
		t.Fatalf("test gateway must have credentials")
	}
	cand := targetCandidate{
		Tier: TierZen, CredKey: creds[0].key, CredID: creds[0].id,
		CredDisplay: creds[0].display, CredIndex: creds[0].index,
		PoolName: "z", Proxy: proxy, ProxyRaw: proxy.name,
		Model: "m", Identity: targetIdentity(TierZen, creds[0].id, "z", proxy.name, "m"),
	}
	scope := routeScopeForCandidate(gateway.cfg.Upstream.Zen, cand, ProtocolChat)
	routeSession := gateway.scheduler.routeSessionFor(ids.Session, scope)
	canonical := routeBodies()[TierZen]
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})

	t.Run("non400NotHandled", func(t *testing.T) {
		cases := []struct {
			name string
			resp *http.Response
			err  error
		}{
			{"503", responseWithBody(503, `{"error":"svc"}`), nil},
			{"429", responseWithBody(429, `{"error":"throttled"}`), nil},
			{"404", responseWithBody(404, `{"error":"gone"}`), nil},
			{"408", responseWithBody(408, `{"error":"timeout"}`), nil},
			{"transport", nil, errors.New("dial timeout")},
			{"success", responseWithBody(200, `{"ok":true}`), nil},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var preReplayCalls atomic.Int32
				handled, replayResp, replayErr, replayed := gateway.maybeReplayCandidate400(ctx, tc.resp, tc.err, badRequestDiag{}, route, TierZen, gateway.cfg.Upstream.Zen, ProtocolChat, canonical, ids, cand, scope, routeSession, 0, 1, func() { preReplayCalls.Add(1) })
				if handled {
					t.Fatalf("%s must not be handled as exact-400", tc.name)
				}
				if replayResp != nil || replayErr != nil {
					t.Fatalf("%s must return nil replay pair, got %v/%v", tc.name, replayResp, replayErr)
				}
				if replayed != 1 {
					t.Fatalf("%s must preserve attempts=1, got %d", tc.name, replayed)
				}
				if postCount(&preReplayCalls) != 0 {
					t.Fatalf("%s must never run preReplay when not handled", tc.name)
				}
				if tc.resp != nil {
					drainAndClose(tc.resp.Body)
				}
			})
		}
	})

	t.Run("exact400Handled", func(t *testing.T) {
		stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
		first := responseWithBody(400, `{"error":"bad"}`)
		handled, replayResp, replayErr, replayed := gateway.maybeReplayCandidate400(ctx, first, nil, badRequestDiag{}, route, TierZen, gateway.cfg.Upstream.Zen, ProtocolChat, canonical, ids, cand, scope, routeSession, 0, 1, nil)
		if !handled {
			t.Fatalf("exact 400 must be handled")
		}
		if replayErr != nil || replayResp == nil || replayResp.StatusCode != 200 {
			t.Fatalf("replay=%v err=%v want 200", replayResp, replayErr)
		}
		drainAndClose(replayResp.Body)
		if replayed != 2 {
			t.Fatalf("replayed=%d want 2", replayed)
		}
	})
}

// Unbound anonymous L1-final transient→400→replay: initial 503 observed once
// (MaxAttempts=2), L1-final 400 replays same-target, replay 200 is
// route-terminal with attempts==3, same target/session/request, no second
// proxy and no authenticated channel.
func TestUnboundAnonymousL1FinalTransient400Replay(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	cap := &capturedUpstream{}
	var seq atomic.Int32
	anonFn := func(r *http.Request) (*http.Response, error) {
		cap.add(r)
		switch seq.Add(1) {
		case 1:
			return responseWithBody(503, `{"error":"svc"}`), nil
		case 2:
			return responseWithBody(400, `{"error":"bad"}`), nil
		default:
			return responseWithBody(200, `{"ok":true}`), nil
		}
	}
	anon0 := stubProxy(t, gateway, "a", 0, anonFn)
	anon1 := stubProxy(t, gateway, "a", 1, anonFn)
	zen := stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ids := clientSessionIDs("ses_client_anon_l1_400_1")
	resp, effRoute, attempts, err := gateway.doUpstreamTiers(ctx, anonAuthRoute(), routeBodiesWithSession(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("status=%v want replay 200", resp)
	}
	drainAndClose(resp.Body)
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3 (503 + 400 + replay)", attempts)
	}
	if total := anon0.count() + anon1.count(); total != 3 || (anon0.count() != 3 && anon1.count() != 3) {
		t.Fatalf("same-target L1+replay calls=%d/%d want 3 on one proxy", anon0.count(), anon1.count())
	}
	if zen.count() != 0 {
		t.Fatalf("replay must stay route-terminal without auth: zen=%d", zen.count())
	}
	if effRoute.Tier == TierCustom {
		t.Fatalf("native replay must not take over custom")
	}
	sessions, bodies := cap.get()
	if len(sessions) != 3 {
		t.Fatalf("captured=%d want 3", len(sessions))
	}
	for i := 1; i < len(sessions); i++ {
		if sessions[i] != sessions[0] || sessions[i] == "" {
			t.Fatalf("replay must keep the same wire session: %q", sessions)
		}
		if sessions[i] == ids.Session {
			t.Fatalf("wire session must never equal the client session")
		}
	}
	for i, raw := range bodies {
		payload := decodeBody(t, raw)
		if got := stringAt(payload, "conversation_id"); got != sessions[i] {
			t.Fatalf("body[%d] conversation_id=%q want header %q", i, got, sessions[i])
		}
	}
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) != 3 {
		t.Fatalf("recorded=%d want 3", len(recent))
	}
	for _, attempt := range recent {
		if attempt.RequestID != ids.Request {
			t.Fatalf("replay must keep the same request ID: %q vs %q", attempt.RequestID, ids.Request)
		}
	}
	if got := gateway.scheduler.routeSessions.count(); got != 0 {
		t.Fatalf("same-session replay must not store an override, got %d", got)
	}
}

// Unbound authenticated L1-final transient→400→replay: same 503→400→200 shape
// on the frozen auth candidates; replay stays same-target and terminal with
// attempts==3 and no walk to the remaining candidate.
func TestUnboundAuthL1FinalTransient400Replay(t *testing.T) {
	monitor := NewMonitor()
	cfg := testGatewayConfig(
		map[string][]string{
			"a": {"direct"},
			"z": {"direct", "http://127.0.0.1:8081"},
		},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Retry.MaxAttempts = 2
	cfg.Retry.TransientRetryIntervalSeconds = 0
	gateway, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	cap := &capturedUpstream{}
	var seq atomic.Int32
	keyFn := func(r *http.Request) (*http.Response, error) {
		cap.add(r)
		switch seq.Add(1) {
		case 1:
			return responseWithBody(503, `{"error":"svc"}`), nil
		case 2:
			return responseWithBody(400, `{"error":"bad"}`), nil
		default:
			return responseWithBody(200, `{"ok":true}`), nil
		}
	}
	z0 := stubProxy(t, gateway, "z", 0, keyFn)
	z1 := stubProxy(t, gateway, "z", 1, keyFn)
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ids := clientSessionIDs("ses_client_auth_l1_400_1")
	resp, effRoute, attempts, err := gateway.doUpstreamTiers(ctx, authOnlyRoute(), routeBodiesWithSession(), ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("status=%v want replay 200", resp)
	}
	drainAndClose(resp.Body)
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3 (503 + 400 + replay)", attempts)
	}
	if total := z0.count() + z1.count(); total != 3 || (z0.count() != 3 && z1.count() != 3) {
		t.Fatalf("same-target L1+replay calls=%d/%d want 3 on one candidate", z0.count(), z1.count())
	}
	if effRoute.Tier == TierCustom {
		t.Fatalf("native replay must not take over custom")
	}
	sessions, _ := cap.get()
	if len(sessions) != 3 {
		t.Fatalf("captured=%d want 3", len(sessions))
	}
	for i := 1; i < len(sessions); i++ {
		if sessions[i] != sessions[0] || sessions[i] == "" {
			t.Fatalf("replay must keep the same wire session: %q", sessions)
		}
	}
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) != 3 {
		t.Fatalf("recorded=%d want 3", len(recent))
	}
	for _, attempt := range recent {
		if attempt.RequestID != ids.Request {
			t.Fatalf("replay must keep the same request ID: %q vs %q", attempt.RequestID, ids.Request)
		}
	}
}

// Pinned anonymous initial 400 with a stored 429 must close the stored 429
// body before the replay request reaches upstream (HEAD a733a20 order).
func TestPinnedAnonymous400DrainsLast429BeforeReplay(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	estabCtx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	session := "ses_pin_anon_order_1"
	postStub(t, gateway, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	estabResp, _, _, err := gateway.doUpstreamTiers(estabCtx, anonAuthRoute(), routeBodies(), requestIDs{Session: session, Request: "req-pin-estab", Project: "prj-test"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	drainAndClose(estabResp.Body)
	pin, ok := gateway.scheduler.pinGet(session, "m")
	if !ok {
		t.Fatalf("must pin anon success")
	}
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
	var order, closeOrder, replaySendOrder atomic.Int32
	var pinnedCalls, otherCalls atomic.Int32
	postStub(t, gateway, "a", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusTooManyRequests, Header: make(http.Header), Body: &closeOrderBody{data: strings.NewReader(`{"error":"throttled"}`), onClose: func() { closeOrder.Store(order.Add(1)) }}}, nil
	})
	var otherSeq atomic.Int32
	postStub(t, gateway, "a", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		if otherSeq.Add(1) == 1 {
			return responseWithBody(400, `{"error":"bad"}`), nil
		}
		replaySendOrder.Store(order.Add(1))
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp, _, attempts, err := gateway.doUpstreamTiers(ctx, anonAuthRoute(), routeBodies(), requestIDs{Session: session, Request: "req-pin-replay", Project: "prj-test"}, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("err=%v resp=%v want replay 200", err, resp)
	}
	drainAndClose(resp.Body)
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3 (429 + 400 + replay)", attempts)
	}
	if postCount(&pinnedCalls) != 1 || postCount(&otherCalls) != 2 {
		t.Fatalf("pinned=%d other=%d want 1/2", postCount(&pinnedCalls), postCount(&otherCalls))
	}
	if got := closeOrder.Load(); got == 0 {
		t.Fatalf("stored 429 body must be closed")
	}
	if got := replaySendOrder.Load(); got == 0 {
		t.Fatalf("replay send must be observed")
	}
	if closeOrder.Load() >= replaySendOrder.Load() {
		t.Fatalf("stored 429 close (%d) must precede replay send (%d)", closeOrder.Load(), replaySendOrder.Load())
	}
}

// Pinned authenticated initial 400 with a stored 429 must discard the stored
// 429 before the replay request reaches upstream (HEAD a733a20 order).
func TestPinnedAuth400DiscardsLast429BeforeReplay(t *testing.T) {
	monitor := NewMonitor()
	cfg := testGatewayConfig(
		map[string][]string{
			"a": {"direct"},
			"z": {"direct", "http://127.0.0.1:8081"},
		},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Retry.MaxAttempts = 2
	cfg.Retry.TransientRetryIntervalSeconds = 0
	gateway, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	estabCtx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	session := "ses_pin_auth_order_1"
	estabRoute := authOnlyRoute()
	postStub(t, gateway, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	estabResp, _, _, err := gateway.doUpstreamTiers(estabCtx, estabRoute, routeBodies(), requestIDs{Session: session, Request: "req-pin-estab", Project: "prj-test"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	drainAndClose(estabResp.Body)
	pin, ok := gateway.scheduler.pinGet(session, "m")
	if !ok {
		t.Fatalf("must pin auth success")
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
	otherIdx := 1 - pinnedIdx
	var order, closeOrder, replaySendOrder atomic.Int32
	var pinnedCalls, otherCalls atomic.Int32
	postStub(t, gateway, "z", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusTooManyRequests, Header: make(http.Header), Body: &closeOrderBody{data: strings.NewReader(`{"error":"throttled"}`), onClose: func() { closeOrder.Store(order.Add(1)) }}}, nil
	})
	var otherSeq atomic.Int32
	postStub(t, gateway, "z", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		if otherSeq.Add(1) == 1 {
			return responseWithBody(400, `{"error":"bad"}`), nil
		}
		replaySendOrder.Store(order.Add(1))
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	resp, _, attempts, err := gateway.doUpstreamTiers(ctx, estabRoute, routeBodies(), requestIDs{Session: session, Request: "req-pin-replay", Project: "prj-test"}, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("err=%v resp=%v want replay 200", err, resp)
	}
	drainAndClose(resp.Body)
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3 (429 + 400 + replay)", attempts)
	}
	if postCount(&pinnedCalls) != 1 || postCount(&otherCalls) != 2 {
		t.Fatalf("pinned=%d other=%d want 1/2", postCount(&pinnedCalls), postCount(&otherCalls))
	}
	if got := closeOrder.Load(); got == 0 {
		t.Fatalf("stored 429 body must be discarded")
	}
	if got := replaySendOrder.Load(); got == 0 {
		t.Fatalf("replay send must be observed")
	}
	if closeOrder.Load() >= replaySendOrder.Load() {
		t.Fatalf("stored 429 discard (%d) must precede replay send (%d)", closeOrder.Load(), replaySendOrder.Load())
	}
}
