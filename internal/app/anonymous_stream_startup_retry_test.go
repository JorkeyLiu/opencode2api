package app

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestUnboundAnonymousStreamStartupSameTargetRetryBinds(t *testing.T) {
	monitor := NewMonitor()
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
	cfg.Keys = []string{"test-key-anon-retry-1"}
	gw, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	gw.catalog.ReplaceWithCapabilities([]string{"m"}, nil, map[Tier]map[string]Protocol{TierZen: {"m": ProtocolChat}}, map[Tier]map[string]bool{}, map[Tier]map[string]ModelMetadata{TierZen: {"m": {ContextWindow: 1000}}})

	// Determine HRW first proxy for this session.
	ses := "ses_anon_stream_startup_retry_3"
	pool := gw.pools["a"]
	now := time.Now().UnixNano()
	cands := gw.scheduler.orderCandidates(gw.scheduler.buildAnonymousCandidates(pool, "m", now), ses)
	if len(cands) != 2 {
		t.Fatalf("cands=%d want 2", len(cands))
	}
	firstRaw := cands[0].ProxyRaw
	firstIdx := -1
	secondIdx := -1
	for i, p := range pool.items {
		if p != nil && p.name == firstRaw {
			firstIdx = i
		} else {
			secondIdx = i
		}
	}
	if firstIdx < 0 || secondIdx < 0 {
		t.Fatalf("idx not found firstRaw=%q", firstRaw)
	}
	var firstCalls, secondCalls, authCalls atomic.Int32
	var seq atomic.Int32
	stubProxy(t, gw, "a", firstIdx, func(*http.Request) (*http.Response, error) {
		n := seq.Add(1)
		firstCalls.Add(1)
		if n == 1 {
			// empty SSE => startup failure before commit
			return sseResponse(""), nil
		}
		return sseResponse(chatTextSSE("retry-ok") + chatDoneSSE()), nil
	})
	stubProxy(t, gw, "a", secondIdx, func(*http.Request) (*http.Response, error) {
		secondCalls.Add(1)
		return sseResponse(chatTextSSE("should-not") + chatDoneSSE()), nil
	})
	stubProxy(t, gw, "z", 0, func(*http.Request) (*http.Response, error) {
		authCalls.Add(1)
		return responseWithBody(200, `{"ok":true}`), nil
	})

	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true})
	ids := requestIDs{Session: ses, Request: "req-anon-startup-retry", Project: "prj-test"}
	route := anonAuthRoute()
	route.KeyTiers = []Tier{TierZen}
	bodies := map[Tier][]byte{TierZen: []byte(`{"model":"m","stream":true}`)}

	resp, eff, attempts, err := gw.doUpstreamTiers(ctx, route, bodies, ids, 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		body := ""
		if resp != nil && resp.Body != nil {
			b, _ := io.ReadAll(resp.Body)
			body = strings.TrimSpace(string(b))
			resp.Body.Close()
		}
		t.Fatalf("want 200 got %v body %q", resp, body)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "retry-ok") {
		t.Fatalf("body must contain retry-ok, got %q", string(body))
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
	if got := int(firstCalls.Load()); got != 2 {
		t.Fatalf("firstCalls=%d want 2 (initial startup failure + gated retry success)", got)
	}
	if got := int(secondCalls.Load()); got != 0 {
		t.Fatalf("secondCalls=%d want 0 (same-target retry must not advance candidate)", got)
	}
	if got := int(authCalls.Load()); got != 0 {
		t.Fatalf("authCalls=%d want 0 (no tier fallback)", got)
	}
	if eff.Tier == TierCustom {
		t.Fatalf("must not fallback to custom on startup retry success")
	}
	pin, ok := gw.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("pin must be bound after gated retry success")
	}
	if pin.Pool != "a" || pin.ProxyRaw != firstRaw || pin.CredID != anonymousSchedulerCredentialID {
		t.Fatalf("pin must bind to first anon proxy same-target, got %+v firstRaw=%q", pin, firstRaw)
	}
	// Verify startup failure was recorded as 502 target cooldown but cleared on success,
	// and not as proxy429/channel.
	for _, p := range pool.items {
		if until, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "a", p.name); ok && until > time.Now().UnixNano() {
			t.Fatalf("proxy429 must not be set for startup failure, proxy %q until %d", p.name, until)
		}
		if until, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "a", p.name); ok && until > time.Now().UnixNano() {
			t.Fatalf("channel must not be set for startup failure, proxy %q until %d", p.name, until)
		}
	}
}
