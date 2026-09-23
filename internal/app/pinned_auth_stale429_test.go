package app

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type trackCloseBody struct {
	io.Reader
	closed *atomic.Int32
}

func (t *trackCloseBody) Close() error {
	if t.closed != nil {
		t.closed.Add(1)
	}
	return nil
}

func trackedResponse(status int, body string, closed *atomic.Int32) *http.Response {
	resp := &http.Response{StatusCode: status, Header: make(http.Header)}
	resp.Body = &trackCloseBody{Reader: strings.NewReader(body), closed: closed}
	return resp
}

func pinnedAuthCustomGateway(t *testing.T, customHits *atomic.Int32) *Gateway {
	t.Helper()
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if customHits != nil {
			customHits.Add(1)
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-stale429")))
	}))
	t.Cleanup(custom.Close)
	ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm-stale429"}
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct"}, "z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 2
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	return gw
}

func bindPinnedAuth(t *testing.T, gw *Gateway, ses string) sessionPin {
	t.Helper()
	cred := gw.authCreds[0]
	raw := gw.pools["z"].items[0].name
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", raw, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, ok := gw.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must bind pinned auth")
	}
	return pin
}

func stale429Extra() upstreamExtra {
	return upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
}

// 1) Bounded pinned L2 consumption: p0 live 429 -> p1 transport + retry
// transport exhausts the frozen binding and takes over custom. Stale 429
// must not be returned and is drained; non-429 takeover never writes
// credential429.
func TestPinnedAuthStale429TransportExhaustionNoCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := pinnedAuthCustomGateway(t, &customHits)
	ses := "ses_pinned_stale429_transport_1"
	pin := bindPinnedAuth(t, gw, ses)
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	if len(ordered) != 2 {
		t.Fatalf("ordered=%d want 2", len(ordered))
	}
	curIdx := poolIndexByRaw(gw, "z", ordered[0].name)
	otherIdx := poolIndexByRaw(gw, "z", ordered[1].name)
	var p0Closed atomic.Int32
	var p0Calls, p1Calls atomic.Int32
	postStub(t, gw, "z", curIdx, &p0Calls, nil, func(*http.Request) (*http.Response, error) {
		r := trackedResponse(429, `{"error":"throttled"}`, &p0Closed)
		r.Header.Set("Retry-After", "7")
		return r, nil
	})
	postStub(t, gw, "z", otherIdx, &p1Calls, nil, func(*http.Request) (*http.Response, error) {
		return nil, io.ErrUnexpectedEOF
	})
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, stale429Extra())
	if err != nil {
		t.Fatalf("transport exhaustion must return response, err=%v", err)
	}
	if resp == nil {
		t.Fatalf("nil resp")
	}
	defer drainResp(resp)
	if resp.StatusCode == 429 {
		t.Fatalf("stale 429 must not be returned after p1 transport, got 429")
	}
	if resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("transport exhaustion must take over custom 200, got %d %+v", resp.StatusCode, eff)
	}
	if customHits.Load() != 1 {
		t.Fatalf("custom hits=%d want 1", customHits.Load())
	}
	if postCount(&p0Calls) != 1 {
		t.Fatalf("p0 sends=%d want 1", postCount(&p0Calls))
	}
	if postCount(&p1Calls) != 2 {
		t.Fatalf("p1 sends=%d want 2 (first + same-target retry)", postCount(&p1Calls))
	}
	if p0Closed.Load() != 1 {
		t.Fatalf("stale p0 429 body closed=%d want 1 (drained)", p0Closed.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("transport exhaustion must bind fallback")
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("partial 429 + transport must not write credential429")
	}
}

// 2) Bounded pinned L2 consumption: p0 live 429 -> p1 5xx transient retry
// -> 403 exhausts the frozen binding and takes over custom. Stale 429 is
// drained; non-429 takeover never writes credential429. The no-active
// faithful 403 case is covered by the consumption no-active test.
func TestPinnedAuthStale429RetryNon429NoCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := pinnedAuthCustomGateway(t, &customHits)
	ses := "ses_pinned_stale429_retry403_1"
	pin := bindPinnedAuth(t, gw, ses)
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	curIdx := poolIndexByRaw(gw, "z", ordered[0].name)
	otherIdx := poolIndexByRaw(gw, "z", ordered[1].name)
	var p0Closed atomic.Int32
	var p1FirstClosed atomic.Int32
	var p1RetryClosed atomic.Int32
	var p0Calls, p1Calls atomic.Int32
	var p1Seq atomic.Int32
	postStub(t, gw, "z", curIdx, &p0Calls, nil, func(*http.Request) (*http.Response, error) {
		r := trackedResponse(429, `{"error":"throttled"}`, &p0Closed)
		r.Header.Set("Retry-After", "7")
		return r, nil
	})
	postStub(t, gw, "z", otherIdx, &p1Calls, nil, func(*http.Request) (*http.Response, error) {
		if p1Seq.Add(1) == 1 {
			return trackedResponse(500, `{"error":"boom"}`, &p1FirstClosed), nil
		}
		return trackedResponse(403, `{"error":"forbidden-final"}`, &p1RetryClosed), nil
	})
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, stale429Extra())
	if err != nil {
		t.Fatalf("retry 403 must return response, err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("429 then L1 500->403 exhaustion must take over custom 200, got %v %+v", resp, eff)
	}
	drainResp(resp)
	if customHits.Load() != 1 {
		t.Fatalf("custom hits=%d want 1", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("429 then 403 exhaustion must bind fallback")
	}
	if p0Closed.Load() != 1 {
		t.Fatalf("stale p0 429 body closed=%d want 1", p0Closed.Load())
	}
	if p1FirstClosed.Load() != 1 {
		t.Fatalf("p1 first 500 body closed=%d want 1 (drained before retry)", p1FirstClosed.Load())
	}
	if p1RetryClosed.Load() != 1 {
		t.Fatalf("handoff must drain final 403 body, closed=%d want 1", p1RetryClosed.Load())
	}
	_ = strings.Contains
	if postCount(&p0Calls) != 1 || postCount(&p1Calls) != 2 {
		t.Fatalf("sends p0=%d p1=%d want 1/2", postCount(&p0Calls), postCount(&p1Calls))
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("partial 429 + 403 must not write credential429")
	}
}

// 4) Pure full-eligible 429 still exhausts: custom takeover + credential429
// with last Retry-After; no-custom keeps last 429 envelope.
func TestPinnedAuthStartedCredentialEvidence(t *testing.T) {
	// Focused regression for Increment 2: pinned-auth must propagate the live
	// send-start timestamp to credential429 so stale fencing and last
	// Retry-After are preserved without brittle wall-clock assumptions.
	monitor := NewMonitor()
	gw2 := authTwoProxyGateway(t, monitor, 5)
	cred2 := gw2.authCreds[0]
	ses2 := "ses_pinned_started_evidence_1"
	raw2 := gw2.pools["z"].items[0].name
	gw2.bindSessionPin(ses2, "m", TierZen, cred2.id, "z", raw2, ProtocolChat, normalizeRouteAuthority(gw2.cfg.Upstream.Zen))
	pin2, _ := gw2.scheduler.pinGet(ses2, "m")
	ord2 := affinityProxyOrder(gw2.pools["z"], pin2.CredID, pin2.ProxyRaw)
	postStub(t, gw2, "z", poolIndexByRaw(gw2, "z", ord2[0].name), nil, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gw2, "z", poolIndexByRaw(gw2, "z", ord2[1].name), nil, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "9")
		return r, nil
	})
	resp2, _, _, err := gw2.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses2, "r1"), 0)
	if err != nil || resp2 == nil || resp2.StatusCode != 429 {
		t.Fatalf("full 429 must keep 429, err=%v resp=%v", err, resp2)
	}
	if got := resp2.Header.Get("Retry-After"); got != "9" {
		drainResp(resp2)
		t.Fatalf("last Retry-After=%q want 9 (Started must retain last live 429)", got)
	}
	drainResp(resp2)
	until, status, ok := gw2.scheduler.credential429CooldownStatus(pin2.CredID)
	if !ok || status != 429 {
		t.Fatalf("full exhaustion must write credential429")
	}
	if until <= 0 {
		t.Fatalf("credential429 until=%d must be non-zero future deadline", until)
	}
	if until <= time.Now().UnixNano() {
		t.Fatalf("credential429 until=%d must be future (non-zero)", until)
	}
	// Prove real Started was propagated, not the scheduler's startedNanos==0 fallback.
	// Direct white-box read of the credential429 entry (same package) provides
	// deterministic evidence without wall-clock brittleness.
	entry := gw2.scheduler.cred429State[pin2.CredID]
	if entry == nil {
		t.Fatalf("credential429 entry missing")
	}
	if entry.lastStartedNanos == 0 {
		t.Fatalf("credential429 lastStartedNanos must be non-zero (real Started propagated, not fallback)")
	}
	if entry.lastStartedNanos <= 1 {
		t.Fatalf("credential429 lastStartedNanos=%d must be >1 to make stale fencing meaningful", entry.lastStartedNanos)
	}
	if entry.failures == 0 {
		t.Fatalf("credential429 failures must be non-zero")
	}
	// Deterministic snapshot accessor cross-check (existing accessor) stays consistent.
	if failures, snapUntil := gw2.scheduler.credential429Snapshot(pin2.CredID); failures == 0 || snapUntil != until {
		t.Fatalf("credential429 snapshot mismatch failures=%d until=%d want non-zero and %d", failures, snapUntil, until)
	}
	// Stale fencing: a success started far in the past (nanos=1) must NOT clear
	// the just-written credential429. If Started were not propagated, lastStarted
	// would be 0 and this stale check would incorrectly clear.
	if ch := gw2.scheduler.noteCredential429Success(pin2.CredID, 1); ch.Changed {
		t.Fatalf("stale Started=1 must not clear credential429 (fencing)")
	}
	if _, _, ok := gw2.scheduler.credential429CooldownStatus(pin2.CredID); !ok {
		t.Fatalf("credential429 must remain after stale success")
	}
}

func TestPinnedAuthFull429StillCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := pinnedAuthCustomGateway(t, &customHits)
	ses := "ses_pinned_full429_custom_1"
	pin := bindPinnedAuth(t, gw, ses)
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[0].name), nil, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gw, "z", poolIndexByRaw(gw, "z", ordered[1].name), nil, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "9")
		return r, nil
	})
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, stale429Extra())
	if err != nil || resp == nil || resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("full 429 must reach custom 200, err=%v resp=%v eff=%+v", err, resp, eff)
	}
	drainResp(resp)
	if customHits.Load() != 1 {
		t.Fatalf("custom hits=%d want 1", customHits.Load())
	}
	if _, status, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); !ok || status != 429 {
		t.Fatalf("full exhaustion must write credential429")
	}

	// No-custom variant keeps last native 429 with last Retry-After.
	monitor := NewMonitor()
	gw2 := authTwoProxyGateway(t, monitor, 5)
	cred2 := gw2.authCreds[0]
	if len(gw2.authCreds) > 1 {
		cred2 = gw2.authCreds[0]
	}
	ses2 := "ses_pinned_full429_nocustom_1"
	raw2 := gw2.pools["z"].items[0].name
	gw2.bindSessionPin(ses2, "m", TierZen, cred2.id, "z", raw2, ProtocolChat, normalizeRouteAuthority(gw2.cfg.Upstream.Zen))
	pin2, _ := gw2.scheduler.pinGet(ses2, "m")
	ord2 := affinityProxyOrder(gw2.pools["z"], pin2.CredID, pin2.ProxyRaw)
	postStub(t, gw2, "z", poolIndexByRaw(gw2, "z", ord2[0].name), nil, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gw2, "z", poolIndexByRaw(gw2, "z", ord2[1].name), nil, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "9")
		return r, nil
	})
	resp2, _, _, err := gw2.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses2, "r1"), 0)
	if err != nil || resp2 == nil || resp2.StatusCode != 429 {
		t.Fatalf("no-custom full 429 must keep 429, err=%v resp=%v", err, resp2)
	}
	if got := resp2.Header.Get("Retry-After"); got != "9" {
		drainResp(resp2)
		t.Fatalf("last Retry-After=%q want 9", got)
	}
	drainResp(resp2)
	if _, _, ok := gw2.scheduler.credential429CooldownStatus(pin2.CredID); !ok {
		t.Fatalf("no-custom full exhaustion must still write credential429")
	}
}
