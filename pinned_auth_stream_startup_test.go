package main

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

func TestPinnedAuthStreamStartupFailureNoWalkNoFallback(t *testing.T) {
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-pinned-startup")))
	}))
	t.Cleanup(custom.Close)
	ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm-pinned-startup"}
	cfg := testGatewayConfig(
		map[string][]string{"z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 3
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
	// Ensure deterministic pool ordering.
	ses := "ses_pinned_stream_startup_auth_1"
	cred := gw.authCreds[0]
	raw := gw.pools["z"].items[0].name
	gw.bindSessionPin(ses, "m", TierZen, cred.id, "z", raw, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, ok := gw.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must bind pinned auth")
	}
	ordered := affinityProxyOrder(gw.pools["z"], pin.CredID, pin.ProxyRaw)
	if len(ordered) != 2 {
		t.Fatalf("ordered=%d want 2", len(ordered))
	}
	pinnedIdx := poolIndexByRaw(gw, "z", pin.ProxyRaw)
	otherIdx := -1
	for i, p := range gw.pools["z"].items {
		if p != nil && p.name != pin.ProxyRaw {
			otherIdx = i
			break
		}
	}
	if otherIdx < 0 {
		t.Fatalf("other proxy not found")
	}
	var pinnedCalls, otherCalls atomic.Int32
	// pinned/current returns immediate EOF pre-commit startup failure (empty SSE)
	postStub(t, gw, "z", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(""), nil
	})
	// second would succeed if called — must not be called for pinned startup failure
	postStub(t, gw, "z", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(chatTextSSE("should-not") + chatDoneSSE()), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := requestIDs{Session: ses, Request: "req-pinned-startup", Project: "prj-test"}
	route := authOnlyRoute()
	route.ID = "m"
	bodies := map[Tier][]byte{TierZen: []byte(`{"model":"m","stream":true}`)}
	resp, eff, attempts, err := gw.doUpstreamTiers(ctx, route, bodies, ids, 0, upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}})
	if err != nil {
		t.Fatalf("pinned startup failure must return response, err=%v", err)
	}
	if resp == nil {
		t.Fatalf("nil resp")
	}
	defer drainResp(resp)
	if resp.StatusCode != 502 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d want 502, body=%q", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if eff.Tier == TierCustom {
		t.Fatalf("must not take over custom on pinned startup failure exhaustion")
	}
	if customHits.Load() != 0 {
		t.Fatalf("custom hits=%d want 0 (no fallback)", customHits.Load())
	}
	// Same-target retry count must be existing transient config (3)
	if got := postCount(&pinnedCalls); got != 3 {
		t.Fatalf("pinnedCalls=%d want 3 (initial + 2 same-target retries)", got)
	}
	if got := postCount(&otherCalls); got != 0 {
		t.Fatalf("otherCalls=%d want 0 (no proxy walk)", got)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3", attempts)
	}
	// Failures stay target-scoped: proxy429/channel/credential429 must not be set
	for _, proxy := range gw.pools["z"].items {
		if until, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "z", proxy.name); ok && until > time.Now().UnixNano() {
			t.Fatalf("proxy429 must not be set for startup failure, proxy %q until %d", proxy.name, until)
		}
		if until, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "z", proxy.name); ok && until > time.Now().UnixNano() {
			t.Fatalf("channel must not be set for startup failure, proxy %q until %d", proxy.name, until)
		}
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("credential429 must not be written for pinned startup failure")
	}
	// Target cooldown for pinned identity should be present (502 target cooling) but proxy walk must not have created other target cooldowns
	identPinned := targetIdentity(TierZen, pin.CredID, "z", pin.ProxyRaw, "m")
	if until, _, ok := gw.scheduler.targetCooldownStatus(identPinned); !ok || until <= time.Now().UnixNano() {
		t.Fatalf("pinned target must be cooled after startup failure")
	}
	identOther := targetIdentity(TierZen, pin.CredID, "z", ordered[1].name, "m")
	// other ordered may be pinned or other; check the non-pinned
	otherRaw := gw.pools["z"].items[otherIdx].name
	identOther2 := targetIdentity(TierZen, pin.CredID, "z", otherRaw, "m")
	if until, _, ok := gw.scheduler.targetCooldownStatus(identOther2); ok && until > time.Now().UnixNano() {
		// If otherRaw equals pinned, skip; otherwise other should not be cooled because never sent
		if otherRaw != pin.ProxyRaw {
			t.Fatalf("other target must not be cooled (never sent), until %d", until)
		}
	}
	_ = identOther
}
