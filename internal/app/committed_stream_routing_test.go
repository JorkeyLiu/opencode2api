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

// committedErrReader replays one SSE deliverable then fails with io.ErrUnexpectedEOF
// on the next Read, exercising the committed-stream downstream error path.
type committedErrReader struct {
	data        []byte
	off         int
	errReturned bool
}

func (r *committedErrReader) Read(p []byte) (int, error) {
	if r.off < len(r.data) {
		n := copy(p, r.data[r.off:])
		r.off += n
		return n, nil
	}
	if !r.errReturned {
		r.errReturned = true
		return 0, io.ErrUnexpectedEOF
	}
	return 0, io.EOF
}

func (r *committedErrReader) Close() error { return nil }

func sseCommittedResponse(text string) *http.Response {
	return &http.Response{
		StatusCode: 200,
		Header:     make(http.Header),
		Body:       &committedErrReader{data: []byte(chatTextSSE(text))},
	}
}

func TestCommittedStream_PinnedAuth_Handler(t *testing.T) {
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-pinned-committed")))
	}))
	t.Cleanup(custom.Close)
	ch := FallbackChannelConfig{ID: "c1", Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm-pinned-committed", Protocol: ProtocolChat}
	cfg := testGatewayConfig(
		map[string][]string{"z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 5
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	gw.catalog.ReplaceWithCapabilities([]string{"m"}, nil, map[Tier]map[string]Protocol{TierZen: {"m": ProtocolChat}}, map[Tier]map[string]bool{}, map[Tier]map[string]ModelMetadata{TierZen: {"m": {ContextWindow: 1000}}})
	gw.cfg.ServerKeys = []string{"test-server-key"}

	// Derive the handler session id from a raw header value and bind the pin.
	rawSes := "ses_committed_pinned_auth_1"
	dummyReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	dummyReq.Header.Set("x-opencode-session", rawSes)
	derived := deriveRequestIDs(dummyReq, map[string]any{"model": "m"}).Session
	cred := gw.authCreds[0]
	pool := gw.pools["z"]
	if pool == nil || len(pool.items) < 2 {
		t.Fatal("pool z missing")
	}
	// Deterministic current via affinity helper.
	tmpPin := sessionPin{Tier: TierZen, CredID: cred.id, Pool: "z", ProxyRaw: pool.items[0].name}
	ordered := affinityProxyOrder(pool, cred.id, tmpPin.ProxyRaw)
	// Use first ordered as the pinned current so the stub order is stable.
	pinnedRaw := ordered[0].name
	gw.bindSessionPin(derived, "m", TierZen, cred.id, "z", pinnedRaw, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, ok := gw.scheduler.pinGet(derived, "m")
	if !ok {
		t.Fatalf("pin must be bound")
	}
	ordered = affinityProxyOrder(pool, pin.CredID, pin.ProxyRaw)
	pinnedIdx := poolIndexByRaw(gw, "z", ordered[0].name)
	otherIdx := poolIndexByRaw(gw, "z", ordered[1].name)
	var pinnedCalls, otherCalls atomic.Int32
	postStub(t, gw, "z", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		return sseCommittedResponse("committed-hello"), nil
	})
	postStub(t, gw, "z", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(chatTextSSE("should-not") + chatDoneSSE()), nil
	})

	handler := gw.Handler()
	payload := `{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	req := httptestNewRequest(t, "POST", "/v1/chat/completions", payload, "test-server-key")
	req.Header.Set("x-opencode-session", rawSes)
	rec := newHttptestRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status=%d want 200 body=%q", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "committed-hello") {
		t.Fatalf("body must retain committed-hello, got %q", body)
	}
	if !strings.Contains(body, `data: {"error"`) {
		t.Fatalf("body must contain Chat SSE structured error frame, got %q", body)
	}
	if !strings.Contains(body, "[DONE]") {
		t.Fatalf("body must contain [DONE], got %q", body)
	}
	// DONE must be after the error frame, not a clean success-only stream.
	errIdx := strings.Index(body, `{"error"`)
	doneIdx := strings.Index(body, "[DONE]")
	if errIdx < 0 || doneIdx < 0 || doneIdx < errIdx {
		t.Fatalf("DONE must be after error frame, errIdx=%d doneIdx=%d body=%q", errIdx, doneIdx, body)
	}
	if pinnedCalls.Load() != 1 {
		t.Fatalf("pinnedCalls=%d want 1", pinnedCalls.Load())
	}
	if otherCalls.Load() != 0 {
		t.Fatalf("otherCalls=%d want 0 (no proxy walk)", otherCalls.Load())
	}
	if customHits.Load() != 0 {
		t.Fatalf("customHits=%d want 0 (no fallback)", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(derived); ok {
		t.Fatalf("fallback store must not bind on committed stream")
	}
	// Direct cooldown assertions (best-effort, no production change).
	for _, proxy := range pool.items {
		if until, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "z", proxy.name); ok && until > time.Now().UnixNano() {
			t.Fatalf("proxy429 must not be cooled for committed stream, proxy %q until %d", proxy.name, until)
		}
		if until, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "z", proxy.name); ok && until > time.Now().UnixNano() {
			t.Fatalf("channel must not be cooled for committed stream, proxy %q until %d", proxy.name, until)
		}
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("credential429 must not be written for committed stream")
	}
	ident := targetIdentity(TierZen, pin.CredID, "z", pin.ProxyRaw, "m")
	if until := gw.scheduler.targetCoolUntil(ident); until > time.Now().UnixNano() {
		t.Fatalf("target must not be cooled for committed success, until %d", until)
	}
}

func TestCommittedStream_Unbound_Handler(t *testing.T) {
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-unbound-committed")))
	}))
	t.Cleanup(custom.Close)
	ch := FallbackChannelConfig{ID: "c1", Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm-unbound-committed", Protocol: ProtocolChat}
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct", "http://127.0.0.1:8081"}, "z": {"direct"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 5
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	// Use a free-named model so anonymous lane is selected via name fallback when no directory metadata.
	freeModel := "free-committed-model"
	gw.catalog.ReplaceWithCapabilities([]string{freeModel}, nil, map[Tier]map[string]Protocol{TierZen: {freeModel: ProtocolChat}}, map[Tier]map[string]bool{}, map[Tier]map[string]ModelMetadata{TierZen: {freeModel: {ContextWindow: 1000}}})
	gw.cfg.ServerKeys = []string{"test-server-key"}

	rawSes := "ses_committed_unbound_1"
	// Derive expected session for fallback check.
	dummyReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	dummyReq.Header.Set("x-opencode-session", rawSes)
	derived := deriveRequestIDs(dummyReq, map[string]any{"model": freeModel}).Session

	poolA := gw.pools["a"]
	if poolA == nil || len(poolA.items) < 2 {
		t.Fatal("pool a missing")
	}
	var a0Calls, a1Calls, authCalls atomic.Int32
	// Unbound anonymous ordering is HRW by session; avoid assuming order:
	// both anon proxies return the same committed stream so whichever is
	// chosen shows committed-hello and total sends stays 1.
	postStub(t, gw, "a", 0, &a0Calls, nil, func(*http.Request) (*http.Response, error) {
		return sseCommittedResponse("committed-hello"), nil
	})
	postStub(t, gw, "a", 1, &a1Calls, nil, func(*http.Request) (*http.Response, error) {
		return sseCommittedResponse("committed-hello"), nil
	})
	postStub(t, gw, "z", 0, &authCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})

	handler := gw.Handler()
	payload := `{"model":"` + freeModel + `","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	req := httptestNewRequest(t, "POST", "/v1/chat/completions", payload, "test-server-key")
	req.Header.Set("x-opencode-session", rawSes)
	rec := newHttptestRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status=%d want 200 body=%q", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "committed-hello") {
		t.Fatalf("body must retain committed-hello, got %q", body)
	}
	if !strings.Contains(body, `data: {"error"`) {
		t.Fatalf("body must contain Chat SSE error frame, got %q", body)
	}
	if !strings.Contains(body, "[DONE]") {
		t.Fatalf("body must contain [DONE], got %q", body)
	}
	errIdx := strings.Index(body, `{"error"`)
	doneIdx := strings.Index(body, "[DONE]")
	if errIdx < 0 || doneIdx < 0 || doneIdx < errIdx {
		t.Fatalf("DONE must be after error, errIdx=%d doneIdx=%d body=%q", errIdx, doneIdx, body)
	}
	totalAnon := int(a0Calls.Load() + a1Calls.Load())
	if totalAnon != 1 {
		t.Fatalf("anon total calls=%d want 1 (no candidate walk), a0=%d a1=%d", totalAnon, a0Calls.Load(), a1Calls.Load())
	}
	if authCalls.Load() != 0 {
		t.Fatalf("authCalls=%d want 0 (no tier fallback)", authCalls.Load())
	}
	if customHits.Load() != 0 {
		t.Fatalf("customHits=%d want 0 (no fallback)", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(derived); ok {
		t.Fatalf("fallback store must not bind on unbound committed stream")
	}
	// Optional cooldown check: no proxy429/channel for anon lane.
	for _, proxy := range poolA.items {
		if until, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "a", proxy.name); ok && until > time.Now().UnixNano() {
			t.Fatalf("proxy429 must not be cooled for unbound committed, proxy %q until %d", proxy.name, until)
		}
	}
}

func TestCommittedStream_PinnedAuth_Direct(t *testing.T) {
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-direct")))
	}))
	t.Cleanup(custom.Close)
	ch := FallbackChannelConfig{ID: "c1", Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm-direct", Protocol: ProtocolChat}
	cfg := testGatewayConfig(
		map[string][]string{"z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.TimeoutSeconds = 5
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	ses := "ses_committed_direct_1"
	cred := gw.authCreds[0]
	pool := gw.pools["z"]
	ordered := affinityProxyOrder(pool, cred.id, pool.items[0].name)
	pinnedRaw := ordered[0].name
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", pinnedRaw, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, ok := gw.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("pin must be bound")
	}
	ordered = affinityProxyOrder(pool, pin.CredID, pin.ProxyRaw)
	pinnedIdx := poolIndexByRaw(gw, "z", ordered[0].name)
	otherIdx := poolIndexByRaw(gw, "z", ordered[1].name)
	var pinnedCalls, otherCalls atomic.Int32
	postStub(t, gw, "z", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		return sseCommittedResponse("committed-hello"), nil
	})
	postStub(t, gw, "z", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(chatTextSSE("should-not") + chatDoneSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := requestIDs{Session: ses, Request: "req-direct", Project: "prj-test"}
	route := authOnlyRoute()
	bodies := routeBodies()
	resp, eff, attempts, err := gw.doUpstreamTiers(ctx, route, bodies, ids, 0)
	if err != nil {
		t.Fatalf("doUpstreamTiers err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("status=%v want 200", resp)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want 1", attempts)
	}
	if eff.Tier == TierCustom {
		t.Fatalf("TierCustom must not appear on committed stream, eff=%+v", eff)
	}
	if pinnedCalls.Load() != 1 || otherCalls.Load() != 0 {
		t.Fatalf("pinned=%d other=%d want 1/0", pinnedCalls.Load(), otherCalls.Load())
	}
	if customHits.Load() != 0 {
		t.Fatalf("customHits=%d want 0", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("fallback must not bind")
	}
	// Verify downstream streaming still delivers committed-hello + error + DONE via forwardSSE.
	rec := httptest.NewRecorder()
	rec.Header().Set("Content-Type", "text/event-stream")
	_, _, fwdErr := forwardSSEWithUsageContext(ctx, rec, resp.Body, ProtocolChat, "m")
	_ = fwdErr
	resp.Body.Close()
	body := rec.Body.String()
	if !strings.Contains(body, "committed-hello") {
		t.Fatalf("forward body must retain committed-hello, got %q", body)
	}
	if !strings.Contains(body, `data: {"error"`) {
		t.Fatalf("forward body must contain error frame, got %q", body)
	}
	if !strings.Contains(body, "[DONE]") {
		t.Fatalf("forward body must contain DONE, got %q", body)
	}
	errIdx := strings.Index(body, `{"error"`)
	doneIdx := strings.Index(body, "[DONE]")
	if errIdx < 0 || doneIdx < 0 || doneIdx < errIdx {
		t.Fatalf("DONE after error: errIdx=%d doneIdx=%d body=%q", errIdx, doneIdx, body)
	}
	// Cooldowns remain not set (best-effort).
	for _, proxy := range pool.items {
		if until, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "z", proxy.name); ok && until > time.Now().UnixNano() {
			t.Fatalf("proxy429 cooled %q until %d", proxy.name, until)
		}
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("credential429 must not be set")
	}
}
