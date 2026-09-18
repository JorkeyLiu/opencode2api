package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
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
	cfg.Retry.MaxAttempts = 5
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

// 1) p0 live 429 -> p1 transport + retry transport: active custom must not
// take over, stale 429 must not be returned, stale body must be drained.
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
	if resp.StatusCode != 502 {
		t.Fatalf("transport exhaustion status=%d want 502", resp.StatusCode)
	}
	if eff.Tier == TierCustom {
		t.Fatalf("must not take over custom on transport exhaustion")
	}
	if customHits.Load() != 0 {
		t.Fatalf("custom hits=%d want 0", customHits.Load())
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
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("partial 429 + transport must not write credential429")
	}
}

// 2) p0 live 429 -> p1 5xx transient retry -> 403: final non-429 wins, no
// custom, stale 429 drained, returned 403 body intact.
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
	if resp == nil || resp.StatusCode != 403 {
		t.Fatalf("final status=%v want 403", resp)
	}
	if eff.Tier == TierCustom {
		drainResp(resp)
		t.Fatalf("non-429 terminal must not take over custom")
	}
	if customHits.Load() != 0 {
		drainResp(resp)
		t.Fatalf("custom hits=%d want 0", customHits.Load())
	}
	if p0Closed.Load() != 1 {
		drainResp(resp)
		t.Fatalf("stale p0 429 body closed=%d want 1", p0Closed.Load())
	}
	if p1FirstClosed.Load() != 1 {
		drainResp(resp)
		t.Fatalf("p1 first 500 body closed=%d want 1 (drained before retry)", p1FirstClosed.Load())
	}
	if p1RetryClosed.Load() != 0 {
		drainResp(resp)
		t.Fatalf("returned retry 403 body must not be drained before return, closed=%d", p1RetryClosed.Load())
	}
	raw, _ := io.ReadAll(resp.Body)
	drainResp(resp)
	if p1RetryClosed.Load() != 1 {
		t.Fatalf("returned body must close on drain, closed=%d", p1RetryClosed.Load())
	}
	if !strings.Contains(string(raw), "forbidden-final") {
		t.Fatalf("returned 403 body=%q want forbidden-final", raw)
	}
	if postCount(&p0Calls) != 1 || postCount(&p1Calls) != 2 {
		t.Fatalf("sends p0=%d p1=%d want 1/2", postCount(&p0Calls), postCount(&p1Calls))
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("partial 429 + 403 must not write credential429")
	}
}

// 4) Pure full-eligible 429 still exhausts: custom takeover + credential429
// with last Retry-After; no-custom keeps last 429 envelope.
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
