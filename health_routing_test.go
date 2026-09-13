package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func healthyCatalogGateway(t *testing.T) *Gateway {
	t.Helper()
	gateway := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	seedHealthyCatalog(t, gateway)
	return gateway
}

func seedHealthyCatalog(t *testing.T, gateway *Gateway) {
	t.Helper()
	gateway.catalog.ReplaceWithCapabilities(
		[]string{"m1"}, []string{"m1"},
		map[Tier]map[string]Protocol{TierZen: {"m1": ProtocolChat}, TierGo: {"m1": ProtocolChat}},
		nil, nil,
	)
}

func decodeHealth(t *testing.T, gateway *Gateway) (int, healthResponse) {
	t.Helper()
	rec := httptest.NewRecorder()
	gateway.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	var health healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &health); err != nil {
		t.Fatal(err)
	}
	return rec.Code, health
}

func requireIssue(t *testing.T, health healthResponse, issue string, want bool) {
	t.Helper()
	found := false
	for _, value := range health.Issues {
		if value == issue {
			found = true
		}
	}
	if found != want {
		t.Fatalf("issue %q present=%v want %v (issues=%v)", issue, found, want, health.Issues)
	}
}

func singleZenGateway(t *testing.T, anonymous bool) *Gateway {
	t.Helper()
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	cfg.ZenKeys = []string{"zen-key-aaaaa"}
	cfg.GoKeys = []string{}
	cfg.Anonymous = anonymous
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	seedHealthyCatalog(t, gateway)
	return gateway
}

func TestHealthRoutingAllCredentialsCoolingBlocks(t *testing.T) {
	gateway := singleZenGateway(t, false)
	// Single zen credential actively cooling, anonymous off.
	gateway.applyAttemptOutcome(t.Context(), authCand(TierZen, gateway.zenCreds[0], gateway.pools["shared"], gateway.pools["shared"].items[0], "m1"), responseWithStatus(401), nil)
	code, health := decodeHealth(t, gateway)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("code=%d want 503", code)
	}
	if health.Ready || health.Status != "degraded" {
		t.Fatalf("ready=%v status=%q want false/degraded", health.Ready, health.Status)
	}
	requireIssue(t, health, "no_available_routes", true)
	if health.Routing.ChannelsAvailable != 0 {
		t.Fatalf("channels=%d want 0", health.Routing.ChannelsAvailable)
	}
	if health.Routing.ZenCredentialsAvailable != 0 || health.Routing.CredentialsCooling != 1 {
		t.Fatalf("routing=%+v", health.Routing)
	}
	if health.Routing.AnonymousAvailable {
		t.Fatalf("anonymous must be false when disabled")
	}
	// Old fields unchanged.
	if health.Models.Status != "ready" || health.Proxies.Healthy == 0 {
		t.Fatalf("old fields regressed: %+v", health)
	}
}

func TestHealthRoutingExpiredCooldownImmediatelyAvailable(t *testing.T) {
	gateway := singleZenGateway(t, false)
	cred := gateway.zenCreds[0]
	gateway.scheduler.noteCredentialAuthFailure(cred.id)
	// Expire the cooldown but keep failure memory.
	gateway.scheduler.mu.Lock()
	gateway.scheduler.credState[cred.id].cooldownUntil = time.Now().Add(-time.Second).UnixNano()
	gateway.scheduler.mu.Unlock()
	code, health := decodeHealth(t, gateway)
	if code != http.StatusOK || !health.Ready {
		t.Fatalf("code=%d ready=%v issues=%v", code, health.Ready, health.Issues)
	}
	if health.Routing.ZenCredentialsAvailable != 1 || health.Routing.ChannelsAvailable != 1 {
		t.Fatalf("routing=%+v", health.Routing)
	}
	requireIssue(t, health, "no_available_routes", false)
}

func TestHealthRoutingAnonymousHealthyReady(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	cfg.ZenKeys = []string{"zen-key-aaaaa"}
	cfg.GoKeys = []string{}
	cfg.Anonymous = true
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	seedHealthyCatalog(t, gateway)
	// Cool the only key; anonymous healthy pool must still provide a channel.
	pool := gateway.pools["shared"]
	gateway.applyAttemptOutcome(t.Context(), authCand(TierZen, gateway.zenCreds[0], pool, pool.items[0], "m1"), responseWithStatus(401), nil)
	code, health := decodeHealth(t, gateway)
	if code != http.StatusOK || !health.Ready {
		t.Fatalf("anonymous healthy must stay ready: code=%d ready=%v issues=%v routing=%+v", code, health.Ready, health.Issues, health.Routing)
	}
	if !health.Routing.AnonymousAvailable || health.Routing.ChannelsAvailable != 1 {
		t.Fatalf("routing=%+v", health.Routing)
	}
}

func TestHealthRoutingAnonymousPoolUnhealthyButKeyTierHealthy(t *testing.T) {
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct"}, "z": {"direct"}},
		ProxyRoutingConfig{Anonymous: "a", Zen: "z", Go: "z"},
	)
	cfg.ZenKeys = []string{"zen-key-aaaaa"}
	cfg.GoKeys = []string{}
	cfg.Anonymous = true
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	seedHealthyCatalog(t, gateway)
	gateway.pools["a"].items[0].healthy.Store(false)
	code, health := decodeHealth(t, gateway)
	if code != http.StatusOK || !health.Ready {
		t.Fatalf("key tier healthy must stay ready: code=%d ready=%v issues=%v routing=%+v", code, health.Ready, health.Issues, health.Routing)
	}
	if health.Routing.AnonymousAvailable {
		t.Fatalf("anonymous must be false with unhealthy pool")
	}
	if health.Routing.ZenCredentialsAvailable != 1 || health.Routing.ChannelsAvailable != 1 {
		t.Fatalf("routing=%+v", health.Routing)
	}
}

func TestHealthRoutingKeysWithoutHealthyProxyNotAChannel(t *testing.T) {
	gateway := singleZenGateway(t, false)
	gateway.pools["shared"].items[0].healthy.Store(false)
	code, health := decodeHealth(t, gateway)
	if code != http.StatusServiceUnavailable || health.Ready {
		t.Fatalf("pool without healthy proxy must block: code=%d ready=%v", code, health.Ready)
	}
	requireIssue(t, health, "no_available_routes", true)
	requireIssue(t, health, "no_healthy_proxies", true)
	if health.Routing.ZenCredentialsAvailable != 1 {
		t.Fatalf("credential itself is still available: %+v", health.Routing)
	}
	if health.Routing.ChannelsAvailable != 0 {
		t.Fatalf("channels=%d want 0", health.Routing.ChannelsAvailable)
	}
}

func TestHealthRoutingModelTargetCoolingDoesNotBlock(t *testing.T) {
	gateway := singleZenGateway(t, false)
	pool := gateway.pools["shared"]
	cand := authCand(TierZen, gateway.zenCreds[0], pool, pool.items[0], "m1")
	gateway.scheduler.noteTargetFailure(cand.Identity, AttemptClassUpstreamFailure, 500, 0)
	code, health := decodeHealth(t, gateway)
	if code != http.StatusOK || !health.Ready {
		t.Fatalf("single-model target cooling must not 503: code=%d ready=%v issues=%v routing=%+v", code, health.Ready, health.Issues, health.Routing)
	}
	requireIssue(t, health, "no_available_routes", false)
	if health.Routing.ChannelsAvailable != 1 {
		t.Fatalf("channels=%d want 1", health.Routing.ChannelsAvailable)
	}
}

func TestHealthRoutingSharedPoolCountsEachChannel(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	cfg.ZenKeys = []string{"zen-key-aaaaa"}
	cfg.GoKeys = []string{"go-key-bbbbb"}
	cfg.Anonymous = true
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	seedHealthyCatalog(t, gateway)
	_, health := decodeHealth(t, gateway)
	if health.Routing.ChannelsAvailable != 3 {
		t.Fatalf("shared pool must serve 3 channels, got %+v", health.Routing)
	}
	if health.Routing.ZenCredentialsAvailable != 1 || health.Routing.GoCredentialsAvailable != 1 {
		t.Fatalf("routing=%+v", health.Routing)
	}
}

func TestHealthRoutingIsolatedPoolsAndAnonymousOff(t *testing.T) {
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct"}, "z": {"direct"}, "g": {"direct"}},
		ProxyRoutingConfig{Anonymous: "a", Zen: "z", Go: "g"},
	)
	cfg.ZenKeys = []string{"zen-key-aaaaa"}
	cfg.GoKeys = []string{}
	cfg.Anonymous = false
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	seedHealthyCatalog(t, gateway)
	_, health := decodeHealth(t, gateway)
	if health.Routing.AnonymousAvailable {
		t.Fatalf("anonymous=false must report unavailable")
	}
	if health.Routing.GoCredentialsAvailable != 0 || health.Routing.ChannelsAvailable != 1 {
		t.Fatalf("routing=%+v want go=0 channels=1", health.Routing)
	}
}

func TestHealthRoutingLegacyBlocksNotRegressed(t *testing.T) {
	// Catalog pending still blocks with starting status.
	gateway := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	code, health := decodeHealth(t, gateway)
	if code != http.StatusServiceUnavailable || health.Ready {
		t.Fatalf("pending must block: code=%d ready=%v", code, health.Ready)
	}
	requireIssue(t, health, "model_catalog_pending", true)
	if health.Status != "starting" {
		t.Fatalf("pending status=%q want starting", health.Status)
	}
	// Catalog empty still blocks.
	gateway.catalog.ReplaceWithCapabilities([]string{"m1"}, []string{"m1"}, map[Tier]map[string]Protocol{TierZen: {}, TierGo: {}}, map[Tier]map[string]bool{TierZen: {"m1": true}, TierGo: {"m1": true}}, nil)
	code, health = decodeHealth(t, gateway)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("empty must block: code=%d issues=%v", code, health.Issues)
	}
	requireIssue(t, health, "model_catalog_empty", true)
}
