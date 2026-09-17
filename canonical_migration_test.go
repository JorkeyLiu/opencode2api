package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Legacy config normalizes to canonical-only and saves without legacy fields.
func TestCanonicalLegacyNormalizesAndSavesCanonicalOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	raw := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["k-new"],"zen_keys":["k-zen"],"go_keys":["k-go","k-new"],"anonymous":true,"prefer":"go","proxy_pools":{"shared":{"proxies":["direct"],"proxyfile":""}},"proxy_routing":{"anonymous":"shared","zen":"shared","go":"shared"},"upstream":{"zen":"https://opencode.ai/zen","go":"https://opencode.ai/zen/go"},"retry":{"max_attempts":1,"timeout_seconds":5},"models":{"refresh_seconds":5,"protocols":{}},"performance":{"max_idle_conns":1,"max_idle_conns_per_host":1,"max_conns_per_host":0,"idle_conn_timeout_seconds":1,"connect_timeout_seconds":1,"failure_cooldown_seconds":10,"rate_limit_cooldown_seconds":300,"rate_limit_cooldown_max_seconds":600},"logging":{"level":"info","ring_size":100},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var cfg Config
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("legacy must parse: %v", err)
	}
	got, err := NormalizeConfig(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Keys) != 3 || got.Keys[0] != "k-new" || got.Keys[1] != "k-zen" || got.Keys[2] != "k-go" {
		t.Fatalf("keys merge order wrong: %v", got.Keys)
	}
	if got.ProxyRouting.Authenticated != "shared" || got.ProxyRouting.Zen != "" || got.ProxyRouting.Go != "" {
		t.Fatalf("routing must be canonical: %+v", got.ProxyRouting)
	}
	if got.Upstream.Go != "" || got.Prefer != "" || got.Performance.RateLimitCooldownMaxSeconds != 0 {
		t.Fatalf("legacy runtime fields must clear: %+v %+v %d", got.Upstream, got.Prefer, got.Performance.RateLimitCooldownMaxSeconds)
	}
	if err := SaveConfigAtomic(path, got); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, needle := range []string{`"zen_keys"`, `"go_keys"`, `"prefer"`, `"upstream"`, `"rate_limit_cooldown_max_seconds"`} {
		_ = needle
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		t.Fatal(err)
	}
	if _, ok := top["prefer"]; ok {
		t.Fatalf("saved must omit prefer: %s", text)
	}
	var perf map[string]json.RawMessage
	if err := json.Unmarshal(top["performance"], &perf); err != nil {
		t.Fatal(err)
	}
	if _, ok := perf["rate_limit_cooldown_max_seconds"]; ok {
		t.Fatalf("saved must omit legacy max: %s", text)
	}
	var upstream map[string]json.RawMessage
	if err := json.Unmarshal(top["upstream"], &upstream); err != nil {
		t.Fatal(err)
	}
	if _, ok := upstream["go"]; ok {
		t.Fatalf("saved must omit upstream.go: %s", text)
	}
	if strings.Contains(text, `"zen"`) && strings.Contains(text, `"proxy_routing"`) {
		// proxy_routing.zen/go must be absent; check decoded shape.
		var routing map[string]json.RawMessage
		if err := json.Unmarshal(top["proxy_routing"], &routing); err != nil {
			t.Fatal(err)
		}
		if _, ok := routing["zen"]; ok {
			t.Fatalf("saved routing must omit zen: %s", text)
		}
		if _, ok := routing["go"]; ok {
			t.Fatalf("saved routing must omit go: %s", text)
		}
	}
	reloaded, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(reloaded.Keys) != 3 || reloaded.ProxyRouting.Authenticated != "shared" {
		t.Fatalf("reloaded=%+v", reloaded)
	}
}

// Auth lane without keys performs no inference send and stores unconfigured.
func TestCanonicalNoAuthSendWithoutKeys(t *testing.T) {
	manager, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		nil, nil)
	_ = admin
	_ = token
	_ = csrf
	gw := manager.current.Load().gateway
	if len(gw.credentials()) != 0 {
		t.Fatalf("want 0 creds, got %d", len(gw.credentials()))
	}
	if _, _, ok := gw.bulkProbeModel(TierZen, false); ok {
		// Probe model may exist from catalog, but the lane must still refuse
		// to send without keys. Seed a catalog model to prove the key gate.
	}
	gw.catalog.ReplaceWithCapabilities([]string{"m-nokey"}, nil,
		map[Tier]map[string]Protocol{TierZen: {"m-nokey": ProtocolChat}}, nil, nil)
	model, _, ok := gw.bulkProbeModel(TierZen, false)
	if !ok || model == "" {
		t.Fatalf("seeded auth model must be selectable")
	}
	before := len(manager.monitor.Snapshot().Upstream.Recent)
	resp := gw.runBulkCheck(context.Background())
	_ = resp
	after := len(manager.monitor.Snapshot().Upstream.Recent)
	if after != before {
		t.Fatalf("no-key bulk must not record inference sends: %d -> %d", before, after)
	}
	foundUnconfigured := false
	for _, n := range resp.Nodes {
		if n.Authenticated == "unconfigured" {
			foundUnconfigured = true
		}
		if n.AuthenticatedHTTPStatus != 0 {
			t.Fatalf("unconfigured lane must not carry HTTP status: %+v", n)
		}
	}
	if !foundUnconfigured {
		t.Fatalf("auth lane must report unconfigured without keys: %+v", resp.Nodes)
	}
}

// Actual 429/other statuses remain as real observations, never generic verdicts.
func TestCanonicalActualStatusRemainsObservation(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"k1"}, []string{"direct", "http://127.0.0.1:8081"})
	pool := gw.pools["shared"]
	cred := gw.authCreds[0]
	now := time.Now().UnixNano()
	results := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: cred.key, CredID: cred.id, CredDisp: cred.display}, StartedNanos: now, Status: 200, Success: true},
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[1], Raw: pool.items[1].name, Tier: TierZen, CredKey: cred.key, CredID: cred.id, CredDisp: cred.display}, StartedNanos: now + 1, Status: 429, RetryAfter: time.Second},
	}
	resp := gw.buildBulkResponse(time.Now().UTC(), 2, 2, 0, false, false, results)
	byNode := map[string]bulkNodeAvailability{}
	for _, n := range resp.Nodes {
		byNode[n.ProxyNode] = n
	}
	r429, ok := byNode[redactURL(pool.items[1].name)]
	if !ok || r429.Authenticated != "rate_limited" || r429.AuthenticatedHTTPStatus != 429 {
		t.Fatalf("429 must stay rate_limited/429: %+v", r429)
	}
	// A lone 503 without comparative success keeps its real status.
	lone := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: cred.key, CredID: cred.id, CredDisp: cred.display}, StartedNanos: now, Status: 503},
	}
	respLone := gw.buildBulkResponse(time.Now().UTC(), 1, 1, 0, false, false, lone)
	if len(respLone.Nodes) == 0 || respLone.Nodes[0].AuthenticatedHTTPStatus != 503 {
		t.Fatalf("503 must keep its status: %+v", respLone.Nodes)
	}
	if respLone.Nodes[0].Authenticated == "available" || respLone.Nodes[0].Authenticated == "untested" {
		t.Fatalf("503 must not collapse to generic verdict: %+v", respLone.Nodes[0])
	}
	// Scheduler writes stay independent: buildBulkResponse never writes.
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "shared", pool.items[1].name); ok {
		t.Fatalf("buildBulkResponse must not write scheduler state")
	}
	gw.applyBulkWrites(context.Background(), results)
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "shared", pool.items[1].name); !ok {
		t.Fatalf("429 node must cool proxy429 after apply with comparative success")
	}
	gw2 := schedulerTestGateway(t, []string{"k1"}, []string{"direct", "http://127.0.0.1:8081"})
	pool2 := gw2.pools["shared"]
	cred2 := gw2.authCreds[0]
	lone2 := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool2.items[0], Raw: pool2.items[0].name, Tier: TierZen, CredKey: cred2.key, CredID: cred2.id, CredDisp: cred2.display}, StartedNanos: now, Status: 503},
	}
	gw2.applyBulkWrites(context.Background(), lone2)
	if _, _, ok := gw2.scheduler.channelCooldownStatus(TierZen, "shared", pool2.items[0].name); ok {
		t.Fatalf("lone 503 without comparative success must stay display-only")
	}
}

// Configured fallback channels always appear, even before any probe.
func TestCanonicalCustomChannelAppearsBeforeProbe(t *testing.T) {
	manager := &RuntimeManager{monitor: NewMonitor(), hub: NewLogHub(100), redactor: NewSecretRedactor()}
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Fallback.Channels = []FallbackChannelConfig{{Name: "c1", BaseURL: "https://example.com", APIKey: "k", Model: "m", Protocol: "chat"}}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, manager.monitor)
	if err != nil {
		t.Fatal(err)
	}
	manager.current.Store(&gatewayRuntime{config: normalized, gateway: gateway})
	snap := manager.Resources()
	if len(snap.Custom) != 1 || snap.Custom[0].Name != "c1" {
		t.Fatalf("custom must project: %+v", snap.Custom)
	}
	if snap.Custom[0].Status != "untested" {
		t.Fatalf("fresh custom must be untested: %+v", snap.Custom[0])
	}
}

// Single 429 field: default 300, range 300..3600, fixed 3600 cap.
func TestCanonicalSingle429Field(t *testing.T) {
	if got := defaultConfig().Performance.RateLimitCooldownSeconds; got != 300 {
		t.Fatalf("default=%d want 300", got)
	}
	cfg := testBaseConfig()
	cfg.Performance.RateLimitCooldownSeconds = 0
	got, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.Performance.RateLimitCooldownSeconds != 300 {
		t.Fatalf("0 must default to 300, got %d", got.Performance.RateLimitCooldownSeconds)
	}
	for _, bad := range []int{299, 3601} {
		c := testBaseConfig()
		c.Performance.RateLimitCooldownSeconds = bad
		if _, err := NormalizeConfig("config.json", c); err == nil {
			t.Fatalf("%d must fail", bad)
		}
	}
	s := newTargetScheduler(time.Second, 300*time.Second)
	if s.rateLimitMax() != 3600*time.Second {
		t.Fatalf("fixed cap=%v want 3600s", s.rateLimitMax())
	}
}

// No Go model/routing exposure: legacy Go cache tolerated, never listed/routed.
func TestCanonicalNoGoExposure(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"k1"}, []string{"direct"})
	gw.catalog.ReplaceWithCapabilities([]string{"m-zen"}, []string{"m-go-only"},
		map[Tier]map[string]Protocol{TierZen: {"m-zen": ProtocolChat}, TierGo: {"m-go-only": ProtocolChat}}, nil, nil)
	for _, m := range gw.catalog.List() {
		if m == "m-go-only" {
			t.Fatalf("Go-only model must never list: %v", gw.catalog.List())
		}
	}
	if _, err := gw.catalog.Route("m-go-only", true, true); err == nil {
		t.Fatalf("Go-only model must not route")
	}
	if _, _, ok := gw.bulkProbeModel(TierGo, false); ok {
		t.Fatalf("legacy Go probe must never yield a model")
	}
	route, err := gw.catalog.Route("m-zen", true, true)
	if err != nil {
		t.Fatal(err)
	}
	if route.Tier == TierGo {
		t.Fatalf("live route must never be Go: %+v", route)
	}
	for _, tier := range route.KeyTiers {
		if tier == TierGo {
			t.Fatalf("KeyTiers must never contain Go: %+v", route)
		}
	}
}
