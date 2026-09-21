package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Config strict/default/backcompat/hot apply

func TestAttemptTimeoutConfigStrictAndDefault(t *testing.T) {
	def := defaultConfig()
	if def.Retry.TimeoutSeconds != 300 || def.Retry.AttemptTimeoutSeconds != 5 {
		t.Fatalf("default retry 300/5 want 300/5 got %d/%d", def.Retry.TimeoutSeconds, def.Retry.AttemptTimeoutSeconds)
	}
	if def.Performance.TransportSuspectCooldownSeconds != 15 {
		t.Fatalf("default suspect 15 got %d", def.Performance.TransportSuspectCooldownSeconds)
	}
}

func TestAttemptTimeoutValidation(t *testing.T) {
	cfg := testBaseConfig()
	cfg.Retry.TimeoutSeconds = 5
	cfg.Retry.AttemptTimeoutSeconds = 10
	if _, err := NormalizeConfig("config.json", cfg); err == nil {
		t.Fatalf("attempt > timeout must fail")
	}
	cfg.Retry.AttemptTimeoutSeconds = 0
	// legacy zero via direct struct normalizes to timeout (not 5) when present flag absent.
	cfg2 := testBaseConfig()
	cfg2.Retry.TimeoutSeconds = 300
	cfg2.Retry.AttemptTimeoutSeconds = 0
	norm, err := NormalizeConfig("config.json", cfg2)
	if err != nil {
		t.Fatalf("zero normalize err %v", err)
	}
	if norm.Retry.AttemptTimeoutSeconds != 300 {
		t.Fatalf("zero normalize want 300 got %d", norm.Retry.AttemptTimeoutSeconds)
	}
	// legacy zero normalizes to timeout 30 -> attempt 30
	cfg2b := testBaseConfig()
	cfg2b.Retry.TimeoutSeconds = 30
	cfg2b.Retry.AttemptTimeoutSeconds = 0
	normB, _ := NormalizeConfig("config.json", cfg2b)
	if normB.Retry.AttemptTimeoutSeconds != 30 {
		t.Fatalf("timeout30 zero normalize want 30 got %d", normB.Retry.AttemptTimeoutSeconds)
	}
	// timeout 1 => attempt 1
	cfg3 := testBaseConfig()
	cfg3.Retry.TimeoutSeconds = 1
	cfg3.Retry.AttemptTimeoutSeconds = 0
	norm3, _ := NormalizeConfig("config.json", cfg3)
	if norm3.Retry.AttemptTimeoutSeconds != 1 {
		t.Fatalf("timeout1 normalize want 1 got %d", norm3.Retry.AttemptTimeoutSeconds)
	}
}

func TestAttemptTimeoutUnknownFieldStrict(t *testing.T) {
	// unknown field inside retry must be rejected
	raw := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["k2"],"proxy_pools":{"shared":{"proxies":["direct"]}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"upstream":{"zen":"https://opencode.ai/zen"},"retry":{"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":5,"unknown_retry":1},"models":{"refresh_seconds":300,"protocols":{}},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300,"transport_suspect_cooldown_seconds":15},"logging":{"level":"info","ring_size":2000},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var cfg Config
	if err := json.Unmarshal([]byte(raw), &cfg); err == nil {
		t.Fatalf("retry unknown field must be rejected, got %#v", cfg.Retry)
	}
	// also via raw retry decode path
	cfg2 := testBaseConfig()
	// direct JSON with performance unknown
	rawPerf := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["k2"],"proxy_pools":{"shared":{"proxies":["direct"]}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"upstream":{"zen":"https://opencode.ai/zen"},"retry":{"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":5},"models":{"refresh_seconds":300,"protocols":{}},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300,"transport_suspect_cooldown_seconds":15,"unknown_perf":1},"logging":{"level":"info","ring_size":2000},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var cfg3 Config
	if err := json.Unmarshal([]byte(rawPerf), &cfg3); err == nil {
		t.Fatalf("performance unknown field must be rejected")
	}
	_ = cfg2
}

func TestPerformanceUnknownFieldStrict(t *testing.T) {
	raw := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["k2"],"proxy_pools":{"shared":{"proxies":["direct"]}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"upstream":{"zen":"https://opencode.ai/zen"},"retry":{"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":5},"models":{"refresh_seconds":300,"protocols":{}},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300,"transport_suspect_cooldown_seconds":15,"bogus":1},"logging":{"level":"info","ring_size":2000},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var cfg Config
	if err := json.Unmarshal([]byte(raw), &cfg); err == nil {
		t.Fatalf("performance bogus field must be rejected")
	}
}

func TestAttemptTimeoutLegacyMissingNormalization(t *testing.T) {
	// legacy JSON without attempt_timeout_seconds must normalize to timeout (not 5)
	rawMissing := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["k2"],"proxy_pools":{"shared":{"proxies":["direct"]}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"upstream":{"zen":"https://opencode.ai/zen"},"retry":{"max_attempts":3,"timeout_seconds":300,"transient_max_attempts":3,"transient_retry_interval_seconds":3},"models":{"refresh_seconds":300,"protocols":{}},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300},"logging":{"level":"info","ring_size":2000},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var cfg Config
	if err := json.Unmarshal([]byte(rawMissing), &cfg); err != nil {
		t.Fatalf("unmarshal missing attempt: %v", err)
	}
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatalf("normalize missing attempt: %v", err)
	}
	if norm.Retry.AttemptTimeoutSeconds != 300 {
		t.Fatalf("missing attempt normalize want 300 got %d", norm.Retry.AttemptTimeoutSeconds)
	}
	if norm.Retry.TimeoutSeconds != 300 {
		t.Fatalf("timeout should stay 300 got %d", norm.Retry.TimeoutSeconds)
	}
	// explicit 5 must stay 5
	rawExplicit := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["k2"],"proxy_pools":{"shared":{"proxies":["direct"]}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"upstream":{"zen":"https://opencode.ai/zen"},"retry":{"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":5,"transient_max_attempts":3,"transient_retry_interval_seconds":3},"models":{"refresh_seconds":300,"protocols":{}},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300},"logging":{"level":"info","ring_size":2000},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var cfgE Config
	if err := json.Unmarshal([]byte(rawExplicit), &cfgE); err != nil {
		t.Fatalf("unmarshal explicit: %v", err)
	}
	normE, err := NormalizeConfig("config.json", cfgE)
	if err != nil {
		t.Fatalf("normalize explicit: %v", err)
	}
	if normE.Retry.AttemptTimeoutSeconds != 5 {
		t.Fatalf("explicit 5 must stay 5 got %d", normE.Retry.AttemptTimeoutSeconds)
	}
	// missing with small timeout 2 -> attempt 2 (not 5)
	rawSmall := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["k2"],"proxy_pools":{"shared":{"proxies":["direct"]}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"upstream":{"zen":"https://opencode.ai/zen"},"retry":{"max_attempts":3,"timeout_seconds":2,"transient_max_attempts":3,"transient_retry_interval_seconds":3},"models":{"refresh_seconds":300,"protocols":{}},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300},"logging":{"level":"info","ring_size":2000},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var cfg2 Config
	if err := json.Unmarshal([]byte(rawSmall), &cfg2); err != nil {
		t.Fatalf("unmarshal small: %v", err)
	}
	norm2, err := NormalizeConfig("config.json", cfg2)
	if err != nil {
		t.Fatalf("normalize small: %v", err)
	}
	if norm2.Retry.AttemptTimeoutSeconds != 2 {
		t.Fatalf("small timeout normalize want 2 got %d", norm2.Retry.AttemptTimeoutSeconds)
	}
	// explicit zero must be rejected (distinguish absent vs explicit zero)
	rawZero := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["k2"],"proxy_pools":{"shared":{"proxies":["direct"]}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"upstream":{"zen":"https://opencode.ai/zen"},"retry":{"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":0,"transient_max_attempts":3,"transient_retry_interval_seconds":3},"models":{"refresh_seconds":300,"protocols":{}},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300},"logging":{"level":"info","ring_size":2000},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var cfgZ Config
	if err := json.Unmarshal([]byte(rawZero), &cfgZ); err != nil {
		t.Fatalf("unmarshal zero: %v", err)
	}
	if _, err := NormalizeConfig("config.json", cfgZ); err == nil {
		t.Fatalf("explicit zero must be rejected")
	}
}

func TestPerformanceSuspectValidation(t *testing.T) {
	cfg := testBaseConfig()
	cfg.Performance.TransportSuspectCooldownSeconds = 0
	// direct struct zero with absent flag normalizes to 15
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatalf("suspect zero normalize err %v", err)
	}
	if norm.Performance.TransportSuspectCooldownSeconds != 15 {
		t.Fatalf("suspect zero want 15 got %d", norm.Performance.TransportSuspectCooldownSeconds)
	}
	cfg.Performance.TransportSuspectCooldownSeconds = 400
	cfg.performanceSuspectPresent = true
	if _, err := NormalizeConfig("config.json", cfg); err == nil {
		t.Fatalf("suspect >300 must fail")
	}
	// explicit zero must be rejected
	cfg2 := testBaseConfig()
	cfg2.Performance.TransportSuspectCooldownSeconds = 0
	cfg2.performanceSuspectPresent = true
	if _, err := NormalizeConfig("config.json", cfg2); err == nil {
		t.Fatalf("explicit suspect zero must be rejected")
	}
}

func TestGatewayAttemptTimeoutTransport(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Retry.TimeoutSeconds = 5
	cfg.Retry.AttemptTimeoutSeconds = 1
	cfg.Retry.AttemptTimeoutSeconds = 1
	cfg.Performance.TransportSuspectCooldownSeconds = 1
	norm, _ := NormalizeConfig("config.json", cfg)
	gw, _ := NewGateway(norm, discardGatewayLogger(), NewMonitor())
	// ResponseHeaderTimeout must be attempt timeout, not whole
	if gw.pools["shared"].items[0].client.Timeout != 0 {
		t.Fatalf("client.Timeout must be 0, got %v", gw.pools["shared"].items[0].client.Timeout)
	}
	transport := gw.pools["shared"].items[0].client.Transport.(*http.Transport)
	if transport.ResponseHeaderTimeout != 1*time.Second {
		t.Fatalf("ResponseHeaderTimeout want 1s got %v", transport.ResponseHeaderTimeout)
	}
	// legacy missing should preserve whole timeout: 300 -> attempt 300 => header 300
	cfgLeg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfgLeg.Retry.TimeoutSeconds = 300
	cfgLeg.Retry.AttemptTimeoutSeconds = 0
	cfgLeg.retryAttemptTimeoutPresent = false
	normLeg, _ := NormalizeConfig("config.json", cfgLeg)
	if normLeg.Retry.AttemptTimeoutSeconds != 300 {
		t.Fatalf("legacy missing header want 300 got %d", normLeg.Retry.AttemptTimeoutSeconds)
	}
	gwLeg, _ := NewGateway(normLeg, discardGatewayLogger(), NewMonitor())
	trLeg := gwLeg.pools["shared"].items[0].client.Transport.(*http.Transport)
	if trLeg.ResponseHeaderTimeout != 300*time.Second {
		t.Fatalf("legacy header timeout want 300s got %v", trLeg.ResponseHeaderTimeout)
	}
}

// Suspect write/filter/clear

func TestSuspectWriteFilterClear(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081"})
	pool := gw.pools["shared"]
	proxy0 := pool.items[0]
	// write suspect
	gw.scheduler.noteTransportSuspect(TierZen, "shared", proxy0.name, AttemptClassTransportFailure, 0, time.Now().UnixNano())
	if _, _, ok := gw.scheduler.suspectCooldownStatus(TierZen, "shared", proxy0.name); !ok {
		t.Fatalf("suspect not written")
	}
	// filtered from candidates
	now := time.Now().UnixNano()
	cands := gw.scheduler.buildAuthCandidates(TierZen, gw.authCreds, pool, "m", now)
	for _, c := range cands {
		if c.ProxyRaw == proxy0.name {
			t.Fatalf("suspect proxy not filtered")
		}
	}
	// success clears with stale fencing: old started should not clear
	pastStart := time.Now().Add(-10 * time.Second).UnixNano()
	ch := gw.scheduler.noteTransportSuspectSuccess(TierZen, "shared", proxy0.name, pastStart)
	if ch.Cleared {
		t.Fatalf("stale success must not clear")
	}
	// fresh success clears
	ch2 := gw.scheduler.noteTransportSuspectSuccess(TierZen, "shared", proxy0.name, time.Now().UnixNano())
	if !ch2.Cleared {
		t.Fatalf("fresh success must clear")
	}
	if _, _, ok := gw.scheduler.suspectCooldownStatus(TierZen, "shared", proxy0.name); ok {
		t.Fatalf("suspect not cleared")
	}
}

func TestSuspectPoolIsolation(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	// same raw proxy in different pools isolated
	cfg := testGatewayConfig(map[string][]string{"a": {"direct", "http://127.0.0.1:8081"}, "b": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "a", Authenticated: "b"})
	_ = gw
	gw2, _ := NewGateway(cfg, discardGatewayLogger(), NewMonitor())
	gw2.scheduler.noteTransportSuspect(TierZen, "a", "http://127.0.0.1:8081", AttemptClassTransportFailure, 0, time.Now().UnixNano())
	if _, _, ok := gw2.scheduler.suspectCooldownStatus(TierZen, "b", "http://127.0.0.1:8081"); ok {
		t.Fatalf("pool isolation broken")
	}
	// Zen anonymous and auth share same tier
	gw2.scheduler.noteTransportSuspect(TierZen, "a", "direct", AttemptClassTransportFailure, 0, time.Now().UnixNano())
	if _, _, ok := gw2.scheduler.suspectCooldownStatus(TierZen, "a", "direct"); !ok {
		t.Fatalf("suspect not found")
	}
}

func TestSuspectExpiryAndMigration(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081"})
	pool := gw.pools["shared"]
	gw.scheduler.noteTransportSuspect(TierZen, "shared", pool.items[0].name, AttemptClassTransportFailure, 0, time.Now().UnixNano())
	// expire by manipulating time
	gw.scheduler.mu.Lock()
	for _, e := range gw.scheduler.suspectState {
		e.cooldownUntil = time.Now().Add(-time.Second).UnixNano()
		e.lastFailureAt = time.Now().Add(-10 * time.Minute).UnixNano()
	}
	gw.scheduler.mu.Unlock()
	entries, total := gw.scheduler.snapshotSuspect()
	if total != 0 || len(entries) != 0 {
		t.Fatalf("expired suspect should prune, total %d", total)
	}
	// migration
	gw.scheduler.noteTransportSuspect(TierZen, "shared", pool.items[0].name, AttemptClassTransportFailure, 0, time.Now().UnixNano())
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	gw2, _ := NewGateway(cfg, discardGatewayLogger(), NewMonitor())
	migrated := gw2.scheduler.migrateFrom(gw.scheduler)
	if migrated.Suspect != 1 {
		t.Fatalf("suspect migrate want 1 got %d", migrated.Suspect)
	}
	// bounds: fill to cap and ensure eviction doesn't break active
}

func TestSuspectReadinessUnaffected(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Anonymous = true
	norm, _ := NormalizeConfig("config.json", cfg)
	gw, _ := NewGateway(norm, discardGatewayLogger(), NewMonitor())
	gw.scheduler.noteTransportSuspect(TierZen, "shared", "direct", AttemptClassTransportFailure, 0, time.Now().UnixNano())
	// healthz readiness should still have channels available
	routing := gw.routingReadiness()
	if routing.ChannelsAvailable == 0 {
		t.Fatalf("suspect must not degrade readiness")
	}
}

func TestHealthProbeRefreshIsolation(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	pool := gw.pools["shared"]
	gw.scheduler.noteTransportSuspect(TierZen, "shared", pool.items[0].name, AttemptClassTransportFailure, 0, time.Now().UnixNano())
	// health probe should not clear suspect
	proxy := pool.items[0]
	proxy.healthy.Store(false)
	// simulate health probe success
	gw.applyProxyHealthResult(proxyHealthResult{proxy: proxy, err: nil, failed: false, wasHealthy: false}, "probe", 0)
	if _, _, ok := gw.scheduler.suspectCooldownStatus(TierZen, "shared", pool.items[0].name); !ok {
		t.Fatalf("health probe must not clear suspect")
	}
	// refresh should not touch
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_ = gw.refreshTier(ctx, "http://127.0.0.1:9", TierZen)
	if _, _, ok := gw.scheduler.suspectCooldownStatus(TierZen, "shared", pool.items[0].name); !ok {
		t.Fatalf("refresh must not clear suspect")
	}
}

// Anonymous unbound and pinned transport move

func TestAnonymousUnboundTransportMovesToNextProxy(t *testing.T) {
	monitor := NewMonitor()
	cfg := testGatewayConfig(map[string][]string{"a": {"direct", "http://127.0.0.1:8081"}, "z": {"direct"}}, ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"})
	cfg.Anonymous = true
	cfg.Retry.TransientMaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Retry.MaxAttempts = 5
	norm, _ := NormalizeConfig("config.json", cfg)
	gw, _ := NewGateway(norm, discardGatewayLogger(), monitor)
	var a0calls, a1calls atomic.Int32
	postStub(t, gw, "a", 0, &a0calls, nil, func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})
	postStub(t, gw, "a", 1, &a1calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := emptySessionIDs()
	route := anonAuthRoute()
	resp, _, _, err := gw.doUpstreamTiers(context.Background(), route, routeBodies(), ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200 after transport move, err %v resp %v", err, resp)
	}
	drainAndClose(resp.Body)
	if postCount(&a0calls) != 1 || postCount(&a1calls) != 1 {
		t.Fatalf("a0 %d a1 %d want 1/1", postCount(&a0calls), postCount(&a1calls))
	}
	// 503 must not move for pinned but unbound anonymous follows existing retry semantics
	a0calls.Store(0)
	a1calls.Store(0)
	postStub(t, gw, "a", 0, &a0calls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(503, `{"error":"svc"}`), nil
	})
	// need to clear suspect from previous
	gw.scheduler.mu.Lock()
	gw.scheduler.suspectState = make(map[string]*transportSuspectEntry)
	gw.scheduler.mu.Unlock()
	resp2, _, _, err2 := gw.doUpstreamTiers(context.Background(), route, routeBodies(), emptySessionIDs(), 0)
	if err2 != nil {
		t.Fatalf("err %v", err2)
	}
	_ = resp2
}

func TestPinnedAnonymousTransportDoesNotMove(t *testing.T) {
	monitor := NewMonitor()
	cfg := testGatewayConfig(map[string][]string{"a": {"direct", "http://127.0.0.1:8081"}, "z": {"direct"}}, ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"})
	cfg.Anonymous = true
	cfg.Retry.TransientMaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	norm, _ := NormalizeConfig("config.json", cfg)
	gw, _ := NewGateway(norm, discardGatewayLogger(), monitor)
	ids := pinIDs("ses_pinned_transport_anon", "req1")
	route := anonAuthRoute()
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	r, _, _, _ := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids, 0)
	drainResp(r)
	pin, _ := gw.scheduler.pinGet(ids.Session, "m")
	pinnedIdx := 0
	for i, p := range gw.pools["a"].items {
		if p.name == pin.ProxyRaw {
			pinnedIdx = i
		}
	}
	otherIdx := 1 - pinnedIdx
	var pinnedCalls, otherCalls atomic.Int32
	postStub(t, gw, "a", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})
	postStub(t, gw, "a", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids2 := pinIDs(ids.Session, "req2")
	resp2, _, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids2, 0)
	// pinned anonymous must NOT move to another proxy after transport error;
	// it returns transport error / 502 in the pinned binding and writes suspect.
	if err == nil && resp2 != nil && resp2.StatusCode == 200 {
		t.Fatalf("pinned anon transport must not move, got 200")
	}
	if resp2 != nil {
		drainResp(resp2)
	}
	if postCount(&pinnedCalls) != 1 {
		t.Fatalf("pinned calls %d want 1", postCount(&pinnedCalls))
	}
	if postCount(&otherCalls) != 0 {
		t.Fatalf("other proxy must not be tried for pinned anon transport, got %d", postCount(&otherCalls))
	}
	pin2, _ := gw.scheduler.pinGet(ids.Session, "m")
	if pin2.ProxyRaw != pin.ProxyRaw {
		t.Fatalf("pin must not have moved %q -> %q", pin.ProxyRaw, pin2.ProxyRaw)
	}
	// suspect must be written and future routing must filter it (but pinned still attempts it until expiry? For pinned, eligible filters suspect, so next request would see empty? Check that suspect is set)
	if _, _, ok := gw.scheduler.suspectCooldownStatus(TierZen, "a", pin.ProxyRaw); !ok {
		t.Fatalf("pinned transport suspect not written")
	}
	// 503 must not move (still 503)
	postStub(t, gw, "a", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(503, `{"error":"svc"}`), nil
	})
	// clear suspect for 503 test (503 is not transport, should not be suspect)
	gw.scheduler.mu.Lock()
	gw.scheduler.suspectState = make(map[string]*transportSuspectEntry)
	gw.scheduler.mu.Unlock()
	pin3, _ := gw.scheduler.pinGet(ids.Session, "m")
	curIdx := 0
	for i, p := range gw.pools["a"].items {
		if p.name == pin3.ProxyRaw {
			curIdx = i
		}
	}
	var curCalls atomic.Int32
	postStub(t, gw, "a", curIdx, &curCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(503, `{"error":"svc"}`), nil
	})
	ids3 := pinIDs(ids.Session, "req3")
	resp3, _, _, _ := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids3, 0)
	if resp3 == nil || resp3.StatusCode != 503 {
		t.Fatalf("pinned 503 should return 503 not move, got %v", resp3)
	}
	drainResp(resp3)
	if postCount(&curCalls) != 1 {
		t.Fatalf("503 should not move, calls %d", postCount(&curCalls))
	}
	pin4, _ := gw.scheduler.pinGet(ids.Session, "m")
	if pin4.ProxyRaw != pin3.ProxyRaw {
		t.Fatalf("503 must not move pin %q -> %q", pin3.ProxyRaw, pin4.ProxyRaw)
	}
}

func TestAllSuspectGives502NoCustom(t *testing.T) {
	monitor := NewMonitor()
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm1")))
	}))
	defer custom.Close()
	ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm1"}
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Anonymous = true
	cfg.Keys = []string{"single-key-12345"}
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	norm, _ := NormalizeConfig("config.json", cfg)
	gw, _ := NewGateway(norm, discardGatewayLogger(), monitor)
	// make both proxies suspect
	gw.scheduler.noteTransportSuspect(TierZen, "shared", "direct", AttemptClassTransportFailure, 0, time.Now().UnixNano())
	gw.scheduler.noteTransportSuspect(TierZen, "shared", "http://127.0.0.1:8081", AttemptClassTransportFailure, 0, time.Now().UnixNano())
	route := anonAuthRoute()
	resp, eff, _, _ := gw.doUpstreamTiers(context.Background(), route, routeBodies(), emptySessionIDs(), 0)
	if resp == nil || resp.StatusCode != 502 {
		t.Fatalf("all suspect want 502 got %v", resp)
	}
	drainResp(resp)
	if eff.Tier == TierCustom {
		t.Fatalf("must not fallback on suspect 502")
	}
	if customHits.Load() != 0 {
		t.Fatalf("custom hits %d want 0", customHits.Load())
	}
}

func TestAnonymousUnboundEmptyProxy429Returns429(t *testing.T) {
	monitor := NewMonitor()
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm1")))
	}))
	defer custom.Close()
	ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm1"}
	cfg := testGatewayConfig(map[string][]string{"a": {"direct", "http://127.0.0.1:8081"}, "z": {"direct"}}, ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"})
	cfg.Anonymous = true
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	norm, _ := NormalizeConfig("config.json", cfg)
	gw, _ := NewGateway(norm, discardGatewayLogger(), monitor)
	// put both anonymous proxies into proxy429 active cooldown
	gw.scheduler.noteProxy429Failure(TierZen, "a", "direct", AttemptClassRateLimited, 429, time.Second*60, time.Now().UnixNano())
	gw.scheduler.noteProxy429Failure(TierZen, "a", "http://127.0.0.1:8081", AttemptClassRateLimited, 429, time.Second*60, time.Now().UnixNano())
	route := anonAuthRoute()
	// direct unbound anonymous call should return 429 (not 502) and not be blocked by suspect logic
	resp, err, _, _, _ := gw.doAnonymousUpstream(context.Background(), route, routeBodies(), emptySessionIDs(), 0)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	if resp == nil || resp.StatusCode != 429 {
		t.Fatalf("anonymous empty proxy429 want 429 got %v", resp)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatalf("429 must carry Retry-After")
	}
	drainResp(resp)
	_ = customHits
}

func TestAnonymousUnboundEmptySuspectOnlyReturns502(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"a": {"direct", "http://127.0.0.1:8081"}, "z": {"direct"}}, ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"})
	cfg.Anonymous = true
	norm, _ := NormalizeConfig("config.json", cfg)
	gw, _ := NewGateway(norm, discardGatewayLogger(), NewMonitor())
	gw.scheduler.noteTransportSuspect(TierZen, "a", "direct", AttemptClassTransportFailure, 0, time.Now().UnixNano())
	gw.scheduler.noteTransportSuspect(TierZen, "a", "http://127.0.0.1:8081", AttemptClassTransportFailure, 0, time.Now().UnixNano())
	route := anonAuthRoute()
	resp, _, _, _, _ := gw.doAnonymousUpstream(context.Background(), route, routeBodies(), emptySessionIDs(), 0)
	if resp == nil || resp.StatusCode != 502 {
		t.Fatalf("suspect-only anonymous empty want 502 got %v", resp)
	}
	drainResp(resp)
}

func TestAuthUnboundEmptyProxy429Returns429(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"z": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"})
	cfg.Anonymous = false
	cfg.Keys = []string{"k1-12345", "k2-12345"}
	norm, _ := NormalizeConfig("config.json", cfg)
	gw, _ := NewGateway(norm, discardGatewayLogger(), NewMonitor())
	gw.scheduler.noteProxy429Failure(TierZen, "z", "direct", AttemptClassRateLimited, 429, time.Second*60, time.Now().UnixNano())
	gw.scheduler.noteProxy429Failure(TierZen, "z", "http://127.0.0.1:8081", AttemptClassRateLimited, 429, time.Second*60, time.Now().UnixNano())
	route := anonAuthRoute()
	route.Anonymous = false
	resp, _, _, _, _ := gw.doKeyUpstream(context.Background(), route, routeBodies(), emptySessionIDs(), 0)
	if resp == nil || resp.StatusCode != 429 {
		t.Fatalf("auth empty proxy429 want 429 got %v", resp)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatalf("auth 429 must carry Retry-After")
	}
	drainResp(resp)
}

func TestAuthUnboundEmptySuspectOnlyReturns502(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"z": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"})
	cfg.Anonymous = false
	norm, _ := NormalizeConfig("config.json", cfg)
	gw, _ := NewGateway(norm, discardGatewayLogger(), NewMonitor())
	gw.scheduler.noteTransportSuspect(TierZen, "z", "direct", AttemptClassTransportFailure, 0, time.Now().UnixNano())
	gw.scheduler.noteTransportSuspect(TierZen, "z", "http://127.0.0.1:8081", AttemptClassTransportFailure, 0, time.Now().UnixNano())
	route := anonAuthRoute()
	route.Anonymous = false
	resp, _, _, _, _ := gw.doKeyUpstream(context.Background(), route, routeBodies(), emptySessionIDs(), 0)
	if resp == nil || resp.StatusCode != 502 {
		t.Fatalf("auth suspect-only empty want 502 got %v", resp)
	}
	drainResp(resp)
}

func TestUnboundExpiredProxy429NotOverrideSuspect(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"a": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "a", Authenticated: "a"})
	cfg.Anonymous = true
	norm, _ := NormalizeConfig("config.json", cfg)
	gw, _ := NewGateway(norm, discardGatewayLogger(), NewMonitor())
	// create an expired proxy429 entry manually
	gw.scheduler.noteProxy429Failure(TierZen, "a", "direct", AttemptClassRateLimited, 429, time.Second*1, time.Now().Add(-10*time.Second).UnixNano())
	// expire it by setting cooldownUntil in past
	gw.scheduler.mu.Lock()
	if e := gw.scheduler.proxy429State["zen\x00a\x00direct"]; e != nil {
		e.cooldownUntil = time.Now().Add(-time.Second).UnixNano()
		e.lastFailureAt = time.Now().Add(-10 * time.Minute).UnixNano()
	}
	gw.scheduler.mu.Unlock()
	// also make suspect active on the other proxy, so empty due to suspect only after expired prune
	gw.scheduler.noteTransportSuspect(TierZen, "a", "direct", AttemptClassTransportFailure, 0, time.Now().UnixNano())
	gw.scheduler.noteTransportSuspect(TierZen, "a", "http://127.0.0.1:8081", AttemptClassTransportFailure, 0, time.Now().UnixNano())
	route := anonAuthRoute()
	resp, _, _, _, _ := gw.doAnonymousUpstream(context.Background(), route, routeBodies(), emptySessionIDs(), 0)
	if resp == nil || resp.StatusCode != 502 {
		t.Fatalf("expired 429 must not override suspect 502, got %v", resp)
	}
	drainResp(resp)
}
