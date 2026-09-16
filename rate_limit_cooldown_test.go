package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRateLimitCooldownDefaultAndValidation(t *testing.T) {
	base := defaultConfig()
	if base.Performance.RateLimitCooldownSeconds != 15 {
		t.Fatalf("default rate_limit=%d want 15", base.Performance.RateLimitCooldownSeconds)
	}
	// Missing field keeps default 15 via Unmarshal default seed.
	var cfg Config
	raw := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"zen_keys":["z1"],"prefer":"go","proxy_pools":{"shared":{"proxies":["direct"],"proxyfile":""}},"proxy_routing":{"anonymous":"shared","zen":"shared","go":"shared"},"upstream":{"zen":"https://opencode.ai/zen","go":"https://opencode.ai/zen/go"},"retry":{"max_attempts":1,"timeout_seconds":5},"models":{"refresh_seconds":5,"protocols":{}},"performance":{"max_idle_conns":1,"max_idle_conns_per_host":1,"max_conns_per_host":0,"idle_conn_timeout_seconds":1,"connect_timeout_seconds":1,"failure_cooldown_seconds":10},"logging":{"level":"info","ring_size":100},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Performance.RateLimitCooldownSeconds != 15 {
		t.Fatalf("missing rate_limit=%d want default 15", cfg.Performance.RateLimitCooldownSeconds)
	}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.Performance.RateLimitCooldownSeconds != 15 {
		t.Fatalf("normalized missing rate_limit=%d want 15", normalized.Performance.RateLimitCooldownSeconds)
	}
	// Range checks via Normalize.
	for _, v := range []int{1, 300} {
		c := normalized
		c.Performance.RateLimitCooldownSeconds = v
		if _, err := NormalizeConfig("config.json", c); err != nil {
			t.Fatalf("rate_limit=%d must pass: %v", v, err)
		}
	}
	for _, v := range []int{0, -1, 301, 1000} {
		c := normalized
		c.Performance.RateLimitCooldownSeconds = v
		if v == 0 {
			// Programmatic zero defaults to 15 (admin PUT compat).
			got, err := NormalizeConfig("config.json", c)
			if err != nil {
				t.Fatalf("rate_limit=0 must default, got err %v", err)
			}
			if got.Performance.RateLimitCooldownSeconds != 15 {
				t.Fatalf("rate_limit=0 normalized=%d want 15", got.Performance.RateLimitCooldownSeconds)
			}
			continue
		}
		if _, err := NormalizeConfig("config.json", c); err == nil {
			t.Fatalf("rate_limit=%d must fail", v)
		}
	}
	// Unknown field inside performance is rejected.
	bad := `{"max_idle_conns":1,"max_idle_conns_per_host":1,"max_conns_per_host":0,"idle_conn_timeout_seconds":1,"connect_timeout_seconds":1,"failure_cooldown_seconds":1,"rate_limit_cooldown_seconds":15,"bogus":1}`
	var pc PerformanceConfig
	dec := json.NewDecoder(strings.NewReader(bad))
	// Use strict decode path via Config Unmarshal: embed performance unknown.
	full := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"zen_keys":["z1"],"prefer":"go","proxy_pools":{"shared":{"proxies":["direct"],"proxyfile":""}},"proxy_routing":{"anonymous":"shared","zen":"shared","go":"shared"},"upstream":{"zen":"https://opencode.ai/zen","go":"https://opencode.ai/zen/go"},"retry":{"max_attempts":1,"timeout_seconds":5},"models":{"refresh_seconds":5,"protocols":{}},"performance":` + bad + `,"logging":{"level":"info","ring_size":100},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var cfg2 Config
	if err := json.Unmarshal([]byte(full), &cfg2); err == nil {
		t.Fatal("unknown performance field must fail")
	}
	_ = pc
	_ = dec
}

func TestSchedulerDualBaseSeparation(t *testing.T) {
	s := newTargetScheduler(10*time.Second, 60*time.Second)
	if s.failureBase() != 10*time.Second {
		t.Fatalf("failure base=%v want 10s", s.failureBase())
	}
	if s.rateLimitBase() != 60*time.Second {
		t.Fatalf("rate base=%v want 60s", s.rateLimitBase())
	}
	// Failure paths stay on 10s base: jittered delay in [8,12]s for failures=1.
	dFail := s.backoffDelay(1, "probe-fail", 0)
	if dFail < 8*time.Second || dFail > 12*time.Second {
		t.Fatalf("failure delay=%v want in [8s,12s]", dFail)
	}
	// Rate paths use 60s base: [48,72]s.
	dRate := s.rateLimitBackoffDelay(1, "probe-rate", 0)
	if dRate < 48*time.Second || dRate > 72*time.Second {
		t.Fatalf("rate delay=%v want in [48s,72s]", dRate)
	}
	// Same identity scales exactly with base ratio (deterministic jitter shared).
	a := s.backoffDelay(2, "idem", 0)
	b := s.rateLimitBackoffDelay(2, "idem", 0)
	if b < 5*a || b > 7*a {
		t.Fatalf("rate/fail ratio off: fail=%v rate=%v", a, b)
	}
	// Live note paths: target (failure) vs proxy429 (rate).
	tBefore := time.Now()
	tCh := s.noteTargetFailure("tier\x00cred\x00pool\x00proxy\x00model", "upstream_failure", 500, 0)
	tRem := time.Duration(tCh.CooldownUntil - tBefore.UnixNano())
	if tRem < 7*time.Second || tRem > 13*time.Second {
		t.Fatalf("target cooldown=%v want ~10s", tRem)
	}
	pBefore := time.Now()
	pCh := s.noteProxy429Failure(TierZen, "pool", "proxy-rl", "rate_limited", 429, 0, 0)
	pRem := time.Duration(pCh.CooldownUntil - pBefore.UnixNano())
	if pRem < 47*time.Second || pRem > 73*time.Second {
		t.Fatalf("proxy429 cooldown=%v want ~60s", pRem)
	}
	// Credential 401 (failure) vs credential429 (rate).
	cBefore := time.Now()
	cCh := s.noteCredentialAuthFailure("zen:credA")
	cRem := time.Duration(cCh.CooldownUntil - cBefore.UnixNano())
	if cRem < 7*time.Second || cRem > 13*time.Second {
		t.Fatalf("cred401 cooldown=%v want ~10s", cRem)
	}
	rBefore := time.Now()
	rCh := s.noteCredential429Failure("zen:credB", "rate_limited", 429, 0, 0)
	rRem := time.Duration(rCh.CooldownUntil - rBefore.UnixNano())
	if rRem < 47*time.Second || rRem > 73*time.Second {
		t.Fatalf("cred429 cooldown=%v want ~60s", rRem)
	}
	// Channel stays on failure base.
	chBefore := time.Now()
	chCh := s.noteChannelFailure(TierZen, "pool", "proxy-ch", "upstream_failure", 500, 0, 0)
	chRem := time.Duration(chCh.CooldownUntil - chBefore.UnixNano())
	if chRem < 7*time.Second || chRem > 13*time.Second {
		t.Fatalf("channel cooldown=%v want ~10s", chRem)
	}
	// Retry-After larger wins, still capped at 5m for both bases.
	capped := s.backoffDelay(1, "cap-fail", 400*time.Second)
	if capped != targetBackoffCap {
		t.Fatalf("failure Retry-After cap=%v want 5m", capped)
	}
	cappedR := s.rateLimitBackoffDelay(1, "cap-rate", 400*time.Second)
	if cappedR != targetBackoffCap {
		t.Fatalf("rate Retry-After cap=%v want 5m", cappedR)
	}
	// Single-arg scheduler keeps both bases equal (backward compat).
	legacy := newTargetScheduler(15 * time.Second)
	if legacy.failureBase() != legacy.rateLimitBase() {
		t.Fatalf("legacy bases differ: %v vs %v", legacy.failureBase(), legacy.rateLimitBase())
	}
}

func TestGatewayDualBaseAndMigrationKeepsRemaining(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	cfg.Performance.FailureCooldownSeconds = 10
	cfg.Performance.RateLimitCooldownSeconds = 60
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	if gw.scheduler.failureBase() != 10*time.Second || gw.scheduler.rateLimitBase() != 60*time.Second {
		t.Fatalf("gateway bases=%v/%v want 10s/60s", gw.scheduler.failureBase(), gw.scheduler.rateLimitBase())
	}
	// Old scheduler with different bases records a proxy429; migration must
	// preserve remaining (capped), not recompute with the new base.
	oldS := newTargetScheduler(15*time.Second, 15*time.Second)
	ch := oldS.noteProxy429Failure(TierZen, "shared", "direct", "rate_limited", 429, 0, 0)
	remainingBefore := time.Duration(ch.CooldownUntil - time.Now().UnixNano())
	next := newTargetScheduler(10*time.Second, 60*time.Second)
	next.migrateFrom(oldS)
	got := next.proxy429CoolUntil(TierZen, "shared", "direct")
	remainingAfter := time.Duration(got - time.Now().UnixNano())
	if remainingAfter <= 0 {
		t.Fatal("migrated proxy429 must stay future")
	}
	if remainingAfter > targetBackoffCap {
		t.Fatalf("migrated remaining=%v exceeds cap", remainingAfter)
	}
	diff := remainingBefore - remainingAfter
	if diff < 0 {
		diff = -diff
	}
	if diff > 5*time.Second {
		t.Fatalf("migration must preserve remaining, before=%v after=%v", remainingBefore, remainingAfter)
	}
}

func TestAdminPerformancePropagatesRateLimit(t *testing.T) {
	view := ConfigView{Performance: PerformanceConfig{FailureCooldownSeconds: 10, RateLimitCooldownSeconds: 60}}
	data, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"rate_limit_cooldown_seconds":60`) {
		t.Fatalf("ConfigView must propagate rate_limit: %s", data)
	}
	var update ConfigUpdate
	if err := json.Unmarshal([]byte(`{"performance":{"max_idle_conns":1,"max_idle_conns_per_host":1,"max_conns_per_host":0,"idle_conn_timeout_seconds":1,"connect_timeout_seconds":1,"failure_cooldown_seconds":10,"rate_limit_cooldown_seconds":60}}`), &update); err != nil {
		t.Fatal(err)
	}
	if update.Performance.RateLimitCooldownSeconds != 60 {
		t.Fatalf("ConfigUpdate rate_limit=%d want 60", update.Performance.RateLimitCooldownSeconds)
	}
}

func TestWebUIRateLimitFieldAndAutosave(t *testing.T) {
	html := readConfigWebUI(t)
	for _, needle := range []string{
		`id="c-ratelimit-cooldown"`,
		`429 冷却基准`,
		`rate_limit_cooldown_seconds`,
		`"c-ratelimit-cooldown"`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("webui rate-limit field missing %q", needle)
		}
	}
}
