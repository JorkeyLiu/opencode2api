package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testBaseConfig() Config {
	cfg := defaultConfig()
	cfg.ServerKeys = []string{"local-key"}
	cfg.ZenKeys = []string{"zen-key-12345"}
	cfg.GoKeys = []string{"go-key-12345"}
	return cfg
}

func TestLegacyOnlyMigration(t *testing.T) {
	cfg := testBaseConfig()
	cfg.Proxies = []string{" http://127.0.0.1:8080 ", "direct"}
	cfg.legacyProxiesPresent = true
	got, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	pool, ok := got.ProxyPools["shared"]
	if !ok {
		t.Fatalf("shared pool missing: %+v", got.ProxyPools)
	}
	if len(pool.Proxies) != 2 || pool.Proxies[0] != "http://127.0.0.1:8080" || pool.Proxies[1] != "direct" {
		t.Fatalf("shared proxies=%v", pool.Proxies)
	}
	if got.ProxyRouting.Anonymous != "shared" || got.ProxyRouting.Zen != "shared" || got.ProxyRouting.Go != "shared" {
		t.Fatalf("routing=%+v", got.ProxyRouting)
	}
	if len(got.Proxies) != 0 || got.ProxyFile != "" {
		t.Fatalf("legacy fields not cleared: %v %q", got.Proxies, got.ProxyFile)
	}
	if eff := got.RuntimeProxiesFor("shared"); len(eff) != 2 {
		t.Fatalf("effective=%v", eff)
	}
}

func TestNoFieldsMigration(t *testing.T) {
	cfg := testBaseConfig()
	got, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.ProxyRouting.Anonymous != "shared" || got.ProxyRouting.Zen != "shared" || got.ProxyRouting.Go != "shared" {
		t.Fatalf("routing=%+v", got.ProxyRouting)
	}
	if eff := got.RuntimeProxiesFor("shared"); len(eff) != 1 || eff[0] != "direct" {
		t.Fatalf("effective=%v", eff)
	}
}

func TestNewOnlyValidation(t *testing.T) {
	cfg := testBaseConfig()
	cfg.ProxyPools = map[string]ProxyPoolConfig{
		"a": {Proxies: []string{"direct"}},
		"b": {Proxies: []string{"socks5://127.0.0.1:1080"}},
	}
	cfg.ProxyRouting = ProxyRoutingConfig{Anonymous: "a", Zen: "a", Go: "b"}
	cfg.proxyPoolsPresent = true
	cfg.proxyRoutingPresent = true
	got, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.RuntimeProxiesFor("a")[0] != "direct" || got.RuntimeProxiesFor("b")[0] != "socks5://127.0.0.1:1080" {
		t.Fatalf("effective a=%v b=%v", got.RuntimeProxiesFor("a"), got.RuntimeProxiesFor("b"))
	}
}

func TestLegacyNewConflict(t *testing.T) {
	cfg := testBaseConfig()
	cfg.Proxies = []string{"direct"}
	cfg.legacyProxiesPresent = true
	cfg.ProxyPools = map[string]ProxyPoolConfig{"shared": {Proxies: []string{"direct"}}}
	cfg.ProxyRouting = ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"}
	cfg.proxyPoolsPresent = true
	if _, err := NormalizeConfig("config.json", cfg); err == nil || !strings.Contains(err.Error(), "legacy") {
		t.Fatalf("want legacy conflict error, got %v", err)
	}
	// Struct-literal path without presence flags must also conflict.
	cfg2 := testBaseConfig()
	cfg2.Proxies = []string{"direct"}
	cfg2.ProxyPools = map[string]ProxyPoolConfig{"shared": {Proxies: []string{"direct"}}}
	cfg2.ProxyRouting = ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"}
	if _, err := NormalizeConfig("config.json", cfg2); err == nil {
		t.Fatalf("want conflict for struct literals too")
	}
}

func TestUnknownFieldRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{"listen":"127.0.0.1:8080","server_keys":["k"],"zen_keys":["z"],"proxy_pools":{"shared":{"proxies":["direct"],"proxyfile":""}},"proxy_routing":{"anonymous":"shared","zen":"shared","go":"shared"},"typo_field":1}`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("want unknown field error, got %v", err)
	}
}

func TestPoolRefMissing(t *testing.T) {
	cfg := testBaseConfig()
	cfg.ProxyPools = map[string]ProxyPoolConfig{"a": {Proxies: []string{"direct"}}}
	cfg.ProxyRouting = ProxyRoutingConfig{Anonymous: "a", Zen: "missing", Go: "a"}
	cfg.proxyPoolsPresent = true
	cfg.proxyRoutingPresent = true
	if _, err := NormalizeConfig("config.json", cfg); err == nil || !strings.Contains(err.Error(), "unknown pool") {
		t.Fatalf("want unknown pool error, got %v", err)
	}
}

func TestPoolNameValidation(t *testing.T) {
	for _, bad := range []string{"", "direct", "DIRECT", "http://x", "bad name", "a/b", "a:b", "a@b", "::1", strings.Repeat("x", 65)} {
		cfg := testBaseConfig()
		cfg.ProxyPools = map[string]ProxyPoolConfig{bad: {Proxies: []string{"direct"}}}
		cfg.ProxyRouting = ProxyRoutingConfig{Anonymous: bad, Zen: bad, Go: bad}
		cfg.proxyPoolsPresent = true
		cfg.proxyRoutingPresent = true
		if _, err := NormalizeConfig("config.json", cfg); err == nil {
			t.Fatalf("want error for pool name %q", bad)
		}
	}
	for _, good := range []string{"shared", "pool-1", "a_b.c", "A9", "1.2.3.4"} {
		cfg := testBaseConfig()
		cfg.ProxyPools = map[string]ProxyPoolConfig{good: {Proxies: []string{"direct"}}}
		cfg.ProxyRouting = ProxyRoutingConfig{Anonymous: good, Zen: good, Go: good}
		cfg.proxyPoolsPresent = true
		cfg.proxyRoutingPresent = true
		if _, err := NormalizeConfig("config.json", cfg); err != nil {
			t.Fatalf("good pool name %q rejected: %v", good, err)
		}
	}
}

func TestPoolURLValidation(t *testing.T) {
	cfg := testBaseConfig()
	cfg.ProxyPools = map[string]ProxyPoolConfig{"shared": {Proxies: []string{"ftp://127.0.0.1:21"}}}
	cfg.ProxyRouting = ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"}
	cfg.proxyPoolsPresent = true
	cfg.proxyRoutingPresent = true
	if _, err := NormalizeConfig("config.json", cfg); err == nil {
		t.Fatalf("want scheme error")
	}
	cfg2 := testBaseConfig()
	cfg2.ProxyPools = map[string]ProxyPoolConfig{"shared": {Proxies: []string{"http://"}}}
	cfg2.ProxyRouting = ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"}
	cfg2.proxyPoolsPresent = true
	cfg2.proxyRoutingPresent = true
	if _, err := NormalizeConfig("config.json", cfg2); err == nil {
		t.Fatalf("want host error")
	}
}

func TestPoolDedupRelativeProxyfile(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	proxyContent := "# comment\nhttp://127.0.0.1:8080\n\nsocks5://127.0.0.1:1080  # backup\nhttp://127.0.0.1:8080\n; semicolon\n// slash\n"
	if err := os.WriteFile(filepath.Join(dir, "p.txt"), []byte(proxyContent), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := testBaseConfig()
	cfg.ProxyPools = map[string]ProxyPoolConfig{"shared": {Proxies: []string{"direct", "http://127.0.0.1:8080"}, ProxyFile: "p.txt"}}
	cfg.ProxyRouting = ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"}
	cfg.proxyPoolsPresent = true
	cfg.proxyRoutingPresent = true
	got, err := NormalizeConfig(configPath, cfg)
	if err != nil {
		t.Fatal(err)
	}
	eff := got.RuntimeProxiesFor("shared")
	want := []string{"direct", "http://127.0.0.1:8080", "socks5://127.0.0.1:1080"}
	if len(eff) != len(want) {
		t.Fatalf("effective=%v want %v", eff, want)
	}
	for i := range want {
		if eff[i] != want[i] {
			t.Fatalf("effective=%v want %v", eff, want)
		}
	}
	stored := got.ProxyPools["shared"]
	if stored.ProxyFile != "p.txt" || len(stored.Proxies) != 2 {
		t.Fatalf("stored pool=%+v", stored)
	}
}

func TestSaveLoadOnlyNewFormat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	cfg := testBaseConfig()
	cfg.Proxies = []string{"socks5://127.0.0.1:1080"}
	cfg.legacyProxiesPresent = true
	normalized, err := NormalizeConfig(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveConfigAtomic(path, normalized); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		t.Fatal(err)
	}
	if _, ok := top["proxies"]; ok {
		t.Fatalf("saved file must not contain legacy top-level proxies: %s", data)
	}
	if _, ok := top["proxyfile"]; ok {
		t.Fatalf("saved file must not contain legacy top-level proxyfile: %s", data)
	}
	if _, ok := top["proxy_pools"]; !ok {
		t.Fatalf("saved file must contain proxy_pools: %s", data)
	}
	reloaded, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.ProxyRouting.Zen != "shared" || reloaded.RuntimeProxiesFor("shared")[0] != "socks5://127.0.0.1:1080" {
		t.Fatalf("reloaded=%+v", reloaded)
	}
}

func testGatewayConfig(pools map[string][]string, routing ProxyRoutingConfig) Config {
	cfg := testBaseConfig()
	cfg.Anonymous = true
	cfg.ProxyPools = map[string]ProxyPoolConfig{}
	for name, proxies := range pools {
		cfg.ProxyPools[name] = ProxyPoolConfig{Proxies: proxies}
	}
	cfg.ProxyRouting = routing
	cfg.proxyPoolsPresent = true
	cfg.proxyRoutingPresent = true
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		panic(err)
	}
	return normalized
}

func TestSharedRefsSharePointer(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8080"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	gateway, err := NewGateway(cfg, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	if gateway.pools["shared"] == nil {
		t.Fatalf("shared pool missing")
	}
	if len(gateway.pools) != 1 {
		t.Fatalf("pools=%d want 1", len(gateway.pools))
	}
	// Credential x proxy candidates come from the shared pool for both tiers.
	now := time.Now().UnixNano()
	zen := gateway.scheduler.buildAuthCandidates(TierZen, gateway.zenCreds, gateway.pools["shared"], "m", now)
	goCands := gateway.scheduler.buildAuthCandidates(TierGo, gateway.goCreds, gateway.pools["shared"], "m", now)
	if len(zen) != 2 || len(goCands) != 2 {
		t.Fatalf("shared candidates zen=%d go=%d want 2/2", len(zen), len(goCands))
	}
	anon := gateway.scheduler.buildAnonymousCandidates(gateway.pools["shared"], "m", now)
	if len(anon) != 2 {
		t.Fatalf("anon candidates=%d want 2", len(anon))
	}
	if gateway.pools["shared"].items[0].pool != "shared" {
		t.Fatalf("pool identity=%q", gateway.pools["shared"].items[0].pool)
	}
}

func TestSeparateRefsIsolated(t *testing.T) {
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct"}, "z": {"http://127.0.0.1:8080"}, "g": {"socks5://127.0.0.1:1080"}},
		ProxyRoutingConfig{Anonymous: "a", Zen: "z", Go: "g"},
	)
	gateway, err := NewGateway(cfg, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	if gateway.pools["z"] == gateway.pools["g"] {
		t.Fatalf("isolated pools must not share pointer")
	}
	if len(gateway.pools) != 3 {
		t.Fatalf("pools=%d want 3", len(gateway.pools))
	}
	if gateway.pools["z"].name != "z" || gateway.pools["g"].name != "g" {
		t.Fatalf("pool names zen=%q go=%q", gateway.pools["z"].name, gateway.pools["g"].name)
	}
	now := time.Now().UnixNano()
	anon := gateway.scheduler.buildAnonymousCandidates(gateway.pools["a"], "m", now)
	if len(anon) != 1 || anon[0].PoolName != "a" {
		t.Fatalf("anon pool=%+v", anon)
	}
	// Pool isolation: zen candidates only use pool z, go only pool g.
	zen := gateway.scheduler.buildAuthCandidates(TierZen, gateway.zenCreds, gateway.pools["z"], "m", now)
	for _, cand := range zen {
		if cand.PoolName != "z" {
			t.Fatalf("zen candidate leaked across pools: %+v", cand)
		}
	}
}

func TestTargetIsolationAcrossPools(t *testing.T) {
	cfg := testGatewayConfig(
		map[string][]string{"z": {"direct", "http://127.0.0.1:8081"}, "g": {"direct", "http://127.0.0.1:8082"}, "a": {"direct"}},
		ProxyRoutingConfig{Anonymous: "a", Zen: "z", Go: "g"},
	)
	// Give each tier a key so candidates exist.
	cfg.ZenKeys = []string{"zen-key-12345"}
	cfg.GoKeys = []string{"go-key-12345"}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	// Cooling a zen target must not remove the go candidate on its own pool.
	zenPool := gateway.pools["z"]
	zenProxy := zenPool.items[0]
	identity := targetIdentity(TierZen, gateway.zenCreds[0].id, "z", zenProxy.name, "model-a")
	gateway.scheduler.noteTargetFailure(identity, AttemptClassUpstreamFailure, 500, 0)
	now := time.Now().UnixNano()
	zen := gateway.scheduler.buildAuthCandidates(TierZen, gateway.zenCreds, zenPool, "model-a", now)
	for _, cand := range zen {
		if cand.Identity == identity {
			t.Fatalf("cooling target must be filtered: %s", identity)
		}
	}
	goCands := gateway.scheduler.buildAuthCandidates(TierGo, gateway.goCreds, gateway.pools["g"], "model-a", now)
	if len(goCands) != 2 {
		t.Fatalf("go candidates must be unaffected, got %d", len(goCands))
	}
}

func TestSharedPoolTargetFanout(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8080"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	cfg.ZenKeys = []string{"zen-key-12345"}
	cfg.GoKeys = []string{"go-key-12345"}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	// One key x two proxies per tier over the shared pool.
	now := time.Now().UnixNano()
	zen := gateway.scheduler.buildAuthCandidates(TierZen, gateway.zenCreds, gateway.pools["shared"], "m", now)
	goCands := gateway.scheduler.buildAuthCandidates(TierGo, gateway.goCreds, gateway.pools["shared"], "m", now)
	if len(zen) != 2 || len(goCands) != 2 {
		t.Fatalf("shared fanout zen=%d go=%d want 2/2", len(zen), len(goCands))
	}
}

func TestHealthzUniquePoolCount(t *testing.T) {
	sharedCfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8080"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	shared, err := NewGateway(sharedCfg, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	shared.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	var sharedHealth healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &sharedHealth); err != nil {
		t.Fatal(err)
	}
	if sharedHealth.Proxies.Total != 2 {
		t.Fatalf("shared total=%d want 2", sharedHealth.Proxies.Total)
	}
	isoCfg := testGatewayConfig(
		map[string][]string{"a": {"direct"}, "z": {"direct", "http://127.0.0.1:8080"}, "g": {"socks5://127.0.0.1:1080"}},
		ProxyRoutingConfig{Anonymous: "a", Zen: "z", Go: "g"},
	)
	iso, err := NewGateway(isoCfg, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	rec2 := httptest.NewRecorder()
	iso.handleHealth(rec2, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	var isoHealth healthResponse
	if err := json.Unmarshal(rec2.Body.Bytes(), &isoHealth); err != nil {
		t.Fatal(err)
	}
	if isoHealth.Proxies.Total != 4 {
		t.Fatalf("isolated total=%d want 4", isoHealth.Proxies.Total)
	}
}

func TestResourcesPoolAttribution(t *testing.T) {
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct"}, "z": {"direct"}, "g": {"direct"}},
		ProxyRoutingConfig{Anonymous: "a", Zen: "z", Go: "g"},
	)
	cfg.ZenKeys = []string{"zen-key-12345"}
	cfg.GoKeys = []string{"go-key-12345"}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	manager := &RuntimeManager{logger: nil, monitor: NewMonitor(), hub: NewLogHub(100), redactor: NewSecretRedactor()}
	gateway, err := NewGateway(normalized, nil, manager.monitor)
	if err != nil {
		t.Fatal(err)
	}
	manager.current.Store(&gatewayRuntime{config: normalized, gateway: gateway})
	snap := manager.Resources()
	if len(snap.Proxies) != 3 {
		t.Fatalf("proxies=%d want 3", len(snap.Proxies))
	}
	byPool := map[string]ProxyStatus{}
	for _, p := range snap.Proxies {
		byPool[p.Pool] = p
	}
	// ZenKeys/GoKeys are pool-level routed config counts, not bindings:
	// pool z carries the 1 zen key, pool g the 1 go key, pool a none.
	if byPool["z"].ZenKeys != 1 || byPool["z"].GoKeys != 0 {
		t.Fatalf("pool z routed counts wrong: %+v", byPool["z"])
	}
	if byPool["g"].GoKeys != 1 || byPool["g"].ZenKeys != 0 {
		t.Fatalf("pool g routed counts wrong: %+v", byPool["g"])
	}
	if byPool["a"].ZenKeys != 0 || byPool["a"].GoKeys != 0 {
		t.Fatalf("pool a routed counts wrong: %+v", byPool["a"])
	}
	if byPool["z"].AvailableCredentials != 1 || byPool["g"].AvailableCredentials != 1 || byPool["a"].AvailableCredentials != 1 {
		t.Fatalf("available credentials wrong: a=%+v z=%+v g=%+v", byPool["a"], byPool["z"], byPool["g"])
	}
	if !byPool["a"].Anonymous || byPool["z"].Anonymous {
		t.Fatalf("anonymous attribution a=%v z=%v", byPool["a"].Anonymous, byPool["z"].Anonymous)
	}
	if len(byPool["a"].Routing) != 1 || byPool["a"].Routing[0] != "anonymous" {
		t.Fatalf("routing a=%v", byPool["a"].Routing)
	}
	for _, key := range snap.Keys {
		if key.ProxyPool == "" {
			t.Fatalf("key missing pool: %+v", key)
		}
	}
	if len(snap.AnonymousProxies) != 1 || snap.AnonymousProxies[0].Pool != "a" {
		t.Fatalf("anon proxies=%+v", snap.AnonymousProxies)
	}
}

func TestAttemptPoolQualifiedAggregates(t *testing.T) {
	m := NewMonitor()
	now := time.Now().UTC()
	proxyURL := "http://127.0.0.1:8080"
	m.RecordAttempt(UpstreamAttempt{Time: now, RequestID: "r1", Model: "m", Tier: "zen", KeyID: "AAAAA", Channel: "key", Proxy: proxyURL, ProxyPool: "a", Status: 200, DurationMS: 10, Success: true, FailureClass: AttemptClassSuccess})
	m.RecordAttempt(UpstreamAttempt{Time: now, RequestID: "r2", Model: "m", Tier: "zen", KeyID: "AAAAA", Channel: "key", Proxy: proxyURL, ProxyPool: "b", Status: 200, DurationMS: 10, Success: true, FailureClass: AttemptClassSuccess})
	snap := m.Snapshot()
	if len(snap.AttemptResources.Proxies) != 2 {
		t.Fatalf("proxies must not collide across pools: %+v", snap.AttemptResources.Proxies)
	}
	if _, ok := snap.AttemptResources.Proxies["a @ "+redactURL(proxyURL)]; !ok {
		t.Fatalf("pool-qualified proxy key missing: %+v", snap.AttemptResources.Proxies)
	}
	if _, ok := snap.AttemptResources.Pairs["key:AAAAA @ b @ "+redactURL(proxyURL)]; !ok {
		t.Fatalf("pool-qualified pair key missing: %+v", snap.AttemptResources.Pairs)
	}
	recent := snap.Upstream.Recent
	if len(recent) != 2 || recent[0].ProxyPool == "" {
		t.Fatalf("recent attempts must carry proxy_pool: %+v", recent)
	}
}

func TestClientRejectedNeutral(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	cfg.ZenKeys = []string{"k"}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	proxy := gateway.pools["shared"].items[0]
	cand := targetCandidate{
		Tier: TierZen, CredKey: "k", CredID: credentialIDForKey(TierZen, "k"),
		CredDisplay: "k", PoolName: "shared", Proxy: proxy, ProxyRaw: proxy.name,
		Model: "m", Identity: targetIdentity(TierZen, credentialIDForKey(TierZen, "k"), "shared", proxy.name, "m"),
	}
	before := gateway.scheduler.targetCoolUntil(cand.Identity)
	gateway.applyAttemptOutcome(t.Context(), cand, responseWithStatus(400), nil, time.Now().UnixNano())
	if got := gateway.scheduler.targetCoolUntil(cand.Identity); got != before {
		t.Fatalf("400 must not cool target")
	}
	if gateway.syncProxyResult(t.Context(), proxy, 400, nil) {
		t.Fatalf("400 must not count as proxy failure")
	}
	if !proxy.healthy.Load() {
		t.Fatalf("400 must not change health")
	}
	// 400 must not clear pre-existing target state either.
	gateway.scheduler.noteTargetFailure(cand.Identity, AttemptClassUpstreamFailure, 500, 0)
	cooled := gateway.scheduler.targetCoolUntil(cand.Identity)
	gateway.applyAttemptOutcome(t.Context(), cand, responseWithStatus(400), nil, time.Now().UnixNano())
	if got := gateway.scheduler.targetCoolUntil(cand.Identity); got != cooled {
		t.Fatalf("400 must not clear existing target state")
	}
}

func TestAdminPoolHelpers(t *testing.T) {
	current := map[string]ProxyPoolConfig{"shared": {Proxies: []string{"direct", "http://127.0.0.1:8080"}}}
	fingerprint := secretFingerprint("direct")
	inputs := map[string]ProxyPoolInput{
		"shared": {Proxies: []SecretInput{{ID: fingerprint}, {Value: " socks5://127.0.0.1:1080 "}}, ProxyFile: " p.txt "},
		"new":    {Proxies: []SecretInput{{Value: "direct"}}, ProxyFile: ""},
	}
	resolved, err := resolveProxyPools(inputs, current)
	if err != nil {
		t.Fatal(err)
	}
	if len(resolved["shared"].Proxies) != 2 || resolved["shared"].Proxies[0] != "direct" {
		t.Fatalf("resolved shared=%v", resolved["shared"].Proxies)
	}
	if resolved["shared"].ProxyFile != "p.txt" {
		t.Fatalf("proxyfile=%q", resolved["shared"].ProxyFile)
	}
	// Unknown fingerprint must fail.
	bad := map[string]ProxyPoolInput{"shared": {Proxies: []SecretInput{{ID: "deadbeef"}}}}
	if _, err := resolveProxyPools(bad, current); err == nil {
		t.Fatalf("want stale secret id error")
	}
	// Mask keeps SecretView shape per pool.
	masked := maskSecrets(current["shared"].Proxies, true)
	if len(masked) != 2 || masked[0].ID == "" {
		t.Fatalf("masked=%+v", masked)
	}
	// Admin JSON stays strict: legacy top-level proxies must be rejected.
	var update ConfigUpdate
	dec := json.NewDecoder(strings.NewReader(`{"listen":"x","proxies":[]}`))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&update); err == nil {
		t.Fatalf("legacy proxies must be rejected by ConfigUpdate")
	}
	// New model decodes.
	var good ConfigUpdate
	dec2 := json.NewDecoder(strings.NewReader(`{"listen":"x","proxy_pools":{"shared":{"proxies":[],"proxyfile":""}},"proxy_routing":{"anonymous":"shared","zen":"shared","go":"shared"}}`))
	dec2.DisallowUnknownFields()
	if err := dec2.Decode(&good); err != nil {
		t.Fatalf("new model rejected: %v", err)
	}
	if good.ProxyRouting.Zen != "shared" {
		t.Fatalf("routing=%+v", good.ProxyRouting)
	}
}

func TestPoolNameNoSemanticIPURLCheck(t *testing.T) {
	// IP-like and dot-only host-like names are within the allowed charset and
	// must not face extra semantic rejection. "proxy_node不是IP" is node
	// semantics, not a pool-name content check.
	for _, name := range []string{"1.2.3.4", "10.0.0.1", "example.com", "http", "https"} {
		if err := validatePoolName(name); err != nil {
			t.Fatalf("pool name %q must not be semantically rejected: %v", name, err)
		}
	}
	// URL-like spellings containing characters outside the charset stay
	// rejected by the charset rule only; the charset must not be widened to
	// accommodate "://".
	for _, name := range []string{"http://x", "https://a", "a:b", "a/b", "a@b", "bad name"} {
		err := validatePoolName(name)
		if err == nil {
			t.Fatalf("pool name %q must be rejected", name)
		}
		msg := err.Error()
		if !strings.Contains(msg, "letters, digits") {
			t.Fatalf("pool name %q error must come from charset rule, got %q", name, msg)
		}
		if strings.Contains(msg, "stable identity") || strings.Contains(msg, "not an IP") || strings.Contains(msg, "not a URL") {
			t.Fatalf("pool name %q must not carry IP/URL semantic wording, got %q", name, msg)
		}
	}
}

func TestAdminMaskedPoolRenameRoundTrip(t *testing.T) {
	current := map[string]ProxyPoolConfig{
		"old":   {Proxies: []string{"direct", "http://user:pass@127.0.0.1:8080"}},
		"other": {Proxies: []string{"socks5://127.0.0.1:1080"}},
	}
	masked := maskSecrets(current["old"].Proxies, true)
	inputs := map[string]ProxyPoolInput{}
	kept := make([]SecretInput, 0, len(masked))
	for _, view := range masked {
		if view.ID == "" {
			t.Fatalf("masked view missing id: %+v", view)
		}
		kept = append(kept, SecretInput{ID: view.ID})
	}
	// GET then rename only: values are not refilled, IDs travel under the new key.
	inputs["new"] = ProxyPoolInput{Proxies: kept, ProxyFile: ""}
	inputs["other"] = ProxyPoolInput{Proxies: []SecretInput{{ID: secretFingerprint("socks5://127.0.0.1:1080")}}}
	resolved, err := resolveProxyPools(inputs, current)
	if err != nil {
		t.Fatalf("rename round-trip rejected: %v", err)
	}
	got := resolved["new"].Proxies
	if len(got) != 2 || got[0] != "direct" || got[1] != "http://user:pass@127.0.0.1:8080" {
		t.Fatalf("renamed pool values not restored: %v", got)
	}
	if len(resolved["other"].Proxies) != 1 || resolved["other"].Proxies[0] != "socks5://127.0.0.1:1080" {
		t.Fatalf("untouched pool broken: %+v", resolved)
	}
	// Renamed pools plus remapped routing must normalize cleanly.
	cfg := testBaseConfig()
	cfg.ProxyPools = resolved
	cfg.ProxyRouting = remapProxyRoutingForRename(
		ProxyRoutingConfig{Anonymous: "old", Zen: "old", Go: "other"}, "old", "new")
	cfg.proxyPoolsPresent = true
	cfg.proxyRoutingPresent = true
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatalf("renamed config rejected: %v", err)
	}
	if normalized.ProxyRouting.Anonymous != "new" || normalized.ProxyRouting.Zen != "new" || normalized.ProxyRouting.Go != "other" {
		t.Fatalf("routing=%+v", normalized.ProxyRouting)
	}
	if eff := normalized.RuntimeProxiesFor("new"); len(eff) != 2 || eff[0] != "direct" {
		t.Fatalf("effective new=%v", eff)
	}
}

func TestAdminMaskedPoolRenameUnknownFingerprint(t *testing.T) {
	current := map[string]ProxyPoolConfig{"old": {Proxies: []string{"direct"}}}
	inputs := map[string]ProxyPoolInput{"new": {Proxies: []SecretInput{{ID: "deadbeef01"}}}}
	_, err := resolveProxyPools(inputs, current)
	if err == nil || !strings.Contains(err.Error(), "unknown or stale") {
		t.Fatalf("want unknown/stale rejection, got %v", err)
	}
	if strings.Contains(err.Error(), "direct") {
		t.Fatalf("rejection must not expose values: %v", err)
	}
}

func TestResolvePoolPrefersSameNameOverAmbiguous(t *testing.T) {
	value := "direct"
	fingerprint := secretFingerprint(value)
	// Same-name hit wins even if the fingerprint is marked ambiguous.
	got, err := resolvePoolProxySecrets(
		[]SecretInput{{ID: fingerprint}}, []string{value},
		map[string]string{}, map[string]bool{fingerprint: true})
	if err != nil || len(got) != 1 || got[0] != value {
		t.Fatalf("same-name must win: %v %v", got, err)
	}
	// Without a same-name hit, an ambiguous fingerprint stays rejected.
	if _, err := resolvePoolProxySecrets(
		[]SecretInput{{ID: fingerprint}}, nil,
		map[string]string{fingerprint: value}, map[string]bool{fingerprint: true}); err == nil {
		t.Fatalf("ambiguous fallback must be rejected")
	}
	// Consistent global value resolves under a renamed key.
	got, err = resolvePoolProxySecrets(
		[]SecretInput{{ID: fingerprint}}, nil,
		map[string]string{fingerprint: value}, map[string]bool{})
	if err != nil || len(got) != 1 || got[0] != value {
		t.Fatalf("global fallback failed: %v %v", got, err)
	}
}

func TestRemapProxyRoutingForRename(t *testing.T) {
	routing := ProxyRoutingConfig{Anonymous: "old", Zen: "old", Go: "other"}
	mapped := remapProxyRoutingForRename(routing, "old", "new")
	if mapped.Anonymous != "new" || mapped.Zen != "new" || mapped.Go != "other" {
		t.Fatalf("mapped=%+v", mapped)
	}
	untouched := remapProxyRoutingForRename(routing, "missing", "new")
	if untouched != routing {
		t.Fatalf("unrelated rename must not touch routing: %+v", untouched)
	}
}
