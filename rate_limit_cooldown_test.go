package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRateLimitCooldownDefaultAndValidation(t *testing.T) {
	base := defaultConfig()
	if base.Performance.RateLimitCooldownSeconds != 300 {
		t.Fatalf("default rate_limit=%d want 300", base.Performance.RateLimitCooldownSeconds)
	}
	// Legacy max is always cleared: the 429 maximum is the fixed 3600s cap.
	if base.Performance.RateLimitCooldownMaxSeconds != 0 {
		t.Fatalf("legacy rate_limit max=%d want 0 (cleared)", base.Performance.RateLimitCooldownMaxSeconds)
	}
	// Missing field keeps default 300 via Unmarshal default seed. Canonical
	// shape uses keys + anonymous/authenticated + Zen-only upstream.
	var cfg Config
	raw := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["z1"],"proxy_pools":{"shared":{"proxies":["direct"],"proxyfile":""}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"upstream":{"zen":"https://opencode.ai/zen"},"retry":{"max_attempts":1,"timeout_seconds":5},"models":{"refresh_seconds":5,"protocols":{}},"performance":{"max_idle_conns":1,"max_idle_conns_per_host":1,"max_conns_per_host":0,"idle_conn_timeout_seconds":1,"connect_timeout_seconds":1,"failure_cooldown_seconds":10},"logging":{"level":"info","ring_size":100},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Performance.RateLimitCooldownSeconds != 300 {
		t.Fatalf("missing rate_limit=%d want default 300", cfg.Performance.RateLimitCooldownSeconds)
	}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.Performance.RateLimitCooldownSeconds != 300 {
		t.Fatalf("normalized missing rate_limit=%d want 300", normalized.Performance.RateLimitCooldownSeconds)
	}
	if normalized.Performance.RateLimitCooldownMaxSeconds != 0 {
		t.Fatalf("normalized legacy max=%d want 0 (cleared)", normalized.Performance.RateLimitCooldownMaxSeconds)
	}
	// Legacy max input is accepted but ignored.
	withLegacyMax := normalized
	withLegacyMax.Performance.RateLimitCooldownMaxSeconds = 600
	gotLegacy, err := NormalizeConfig("config.json", withLegacyMax)
	if err != nil {
		t.Fatal(err)
	}
	if gotLegacy.Performance.RateLimitCooldownMaxSeconds != 0 {
		t.Fatalf("legacy max must be cleared, got %d", gotLegacy.Performance.RateLimitCooldownMaxSeconds)
	}
	// Valid single values pass (300..3600 inclusive).
	for _, v := range []int{300, 301, 600, 3599, 3600} {
		c := normalized
		c.Performance.RateLimitCooldownSeconds = v
		if _, err := NormalizeConfig("config.json", c); err != nil {
			t.Fatalf("rate_limit=%d must pass: %v", v, err)
		}
	}
	// Zero normalizes to default (admin PUT compat).
	zeroMin := normalized
	zeroMin.Performance.RateLimitCooldownSeconds = 0
	gotZero, err := NormalizeConfig("config.json", zeroMin)
	if err != nil {
		t.Fatalf("rate_limit 0 must default, got err %v", err)
	}
	if gotZero.Performance.RateLimitCooldownSeconds != 300 {
		t.Fatalf("rate_limit 0 normalized=%d want 300", gotZero.Performance.RateLimitCooldownSeconds)
	}
	// Out-of-range values fail.
	for _, v := range []int{-1, 1, 299, 3601, 7200} {
		c := normalized
		c.Performance.RateLimitCooldownSeconds = v
		if _, err := NormalizeConfig("config.json", c); err == nil {
			t.Fatalf("rate_limit=%d must fail", v)
		}
	}
	// Unknown field inside performance is rejected; legacy max remains known.
	bad := `{"max_idle_conns":1,"max_idle_conns_per_host":1,"max_conns_per_host":0,"idle_conn_timeout_seconds":1,"connect_timeout_seconds":1,"failure_cooldown_seconds":1,"rate_limit_cooldown_seconds":300,"bogus":1}`
	full := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["z1"],"proxy_pools":{"shared":{"proxies":["direct"],"proxyfile":""}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"upstream":{"zen":"https://opencode.ai/zen"},"retry":{"max_attempts":1,"timeout_seconds":5},"models":{"refresh_seconds":5,"protocols":{}},"performance":` + bad + `,"logging":{"level":"info","ring_size":100},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var cfg2 Config
	if err := json.Unmarshal([]byte(full), &cfg2); err == nil {
		t.Fatal("unknown performance field must fail")
	}
	// Legacy config normalizes to canonical-only on save.
	legacyRaw := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"zen_keys":["z1"],"go_keys":["g1"],"prefer":"go","proxy_pools":{"shared":{"proxies":["direct"],"proxyfile":""}},"proxy_routing":{"anonymous":"shared","zen":"shared","go":"shared"},"upstream":{"zen":"https://opencode.ai/zen","go":"https://opencode.ai/zen/go"},"retry":{"max_attempts":1,"timeout_seconds":5},"models":{"refresh_seconds":5,"protocols":{}},"performance":{"max_idle_conns":1,"max_idle_conns_per_host":1,"max_conns_per_host":0,"idle_conn_timeout_seconds":1,"connect_timeout_seconds":1,"failure_cooldown_seconds":10,"rate_limit_cooldown_seconds":300,"rate_limit_cooldown_max_seconds":600},"logging":{"level":"info","ring_size":100},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var legacyCfg Config
	if err := json.Unmarshal([]byte(legacyRaw), &legacyCfg); err != nil {
		t.Fatalf("legacy config must still parse: %v", err)
	}
	gotCanon, err := NormalizeConfig("config.json", legacyCfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotCanon.Keys) != 2 || gotCanon.Keys[0] != "z1" || gotCanon.Keys[1] != "g1" {
		t.Fatalf("legacy keys must merge keys+zen+go, got %v", gotCanon.Keys)
	}
	if gotCanon.ProxyRouting.Authenticated != "shared" || gotCanon.ProxyRouting.Zen != "" || gotCanon.ProxyRouting.Go != "" {
		t.Fatalf("legacy routing must map to authenticated-only: %+v", gotCanon.ProxyRouting)
	}
	if gotCanon.Upstream.Go != "" {
		t.Fatalf("legacy upstream.go must be cleared, got %q", gotCanon.Upstream.Go)
	}
	if gotCanon.Performance.RateLimitCooldownMaxSeconds != 0 {
		t.Fatalf("legacy max must be cleared, got %d", gotCanon.Performance.RateLimitCooldownMaxSeconds)
	}
}

func TestSchedulerDualBaseSeparation(t *testing.T) {
	// The 429 maximum is fixed at 3600s; the legacy max argument is ignored.
	s := newTargetScheduler(10*time.Second, 60*time.Second, 600*time.Second)
	if s.failureBase() != 10*time.Second {
		t.Fatalf("failure base=%v want 10s", s.failureBase())
	}
	if s.rateLimitBase() != 60*time.Second {
		t.Fatalf("rate base=%v want 60s", s.rateLimitBase())
	}
	if s.rateLimitMax() != 3600*time.Second {
		t.Fatalf("rate max=%v want fixed 3600s", s.rateLimitMax())
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
	// Channel stays on failure base.
	chBefore := time.Now()
	chCh := s.noteChannelFailure(TierZen, "pool", "proxy-ch", "upstream_failure", 500, 0, 0)
	chRem := time.Duration(chCh.CooldownUntil - chBefore.UnixNano())
	if chRem < 7*time.Second || chRem > 13*time.Second {
		t.Fatalf("channel cooldown=%v want ~10s", chRem)
	}
	// Non-429 Retry-After still caps at the generic 5 minutes.
	capped := s.backoffDelay(1, "cap-fail", 400*time.Second)
	if capped != targetBackoffCap {
		t.Fatalf("failure Retry-After cap=%v want 5m", capped)
	}
	// 429 Retry-After clamps at the fixed 3600s max.
	cappedR := s.rateLimitBackoffDelay(1, "cap-rate", 10000*time.Second)
	if cappedR != 3600*time.Second {
		t.Fatalf("rate Retry-After cap=%v want 3600s", cappedR)
	}
	// Consecutive 429 strikes escalate exponentially toward the fixed max.
	second := s.rateLimitBackoffDelay(2, "cap-rate", 0)
	if second < 96*time.Second || second > 144*time.Second {
		t.Fatalf("rate second strike=%v want ~120s", second)
	}
	// Single-arg construction keeps both bases equal (legacy compat).
	legacy := newTargetScheduler(15 * time.Second)
	if legacy.rateLimitBase() != 15*time.Second {
		t.Fatalf("legacy rate base=%v want 15s", legacy.rateLimitBase())
	}
	if legacy.rateLimitMax() != 3600*time.Second {
		t.Fatalf("legacy max=%v want 3600s", legacy.rateLimitMax())
	}
}

func TestSchedulerRateLimitSaturationSafe(t *testing.T) {
	// Absurd config values must saturate, never wrap negative.
	hugeSec := int(1) << 40
	if got := secondsToDuration(hugeSec); got <= 0 {
		t.Fatalf("secondsToDuration(%d)=%v must stay positive", hugeSec, got)
	}
	d := backoffDelayForBaseWithCap(secondsToDuration(hugeSec), secondsToDuration(hugeSec), 4, "huge", 0)
	if d <= 0 {
		t.Fatalf("huge backoff=%v must stay positive", d)
	}
	if d > secondsToDuration(hugeSec) {
		t.Fatalf("huge backoff=%v exceeds cap", d)
	}
	// Absurd Retry-After header clamps to the fixed 3600s max.
	s := newTargetScheduler(10*time.Second, 60*time.Second)
	got := s.rateLimitBackoffDelay(1, "huge-ra", secondsToDuration(hugeSec))
	if got != 3600*time.Second {
		t.Fatalf("huge Retry-After must clamp to 3600s, got %v", got)
	}
	// Retry-After parse saturates instead of overflowing.
	if got := parseRetryAfter("99999999999999999999"); got < 0 {
		t.Fatalf("huge Retry-After parse must not go negative: %v", got)
	}
}

func TestGatewayDualBaseAndMigrationKeepsRemaining(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Performance.FailureCooldownSeconds = 10
	cfg.Performance.RateLimitCooldownSeconds = 60
	// 60s is below the canonical 300s minimum and must be rejected.
	if _, err := NormalizeConfig("config.json", cfg); err == nil {
		t.Fatalf("rate_limit=60 must fail (minimum 300)")
	}
	cfg.Performance.RateLimitCooldownSeconds = 300
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	if gw.scheduler.failureBase() != 10*time.Second || gw.scheduler.rateLimitBase() != 300*time.Second || gw.scheduler.rateLimitMax() != 3600*time.Second {
		t.Fatalf("gateway bases=%v/%v/%v want 10s/300s/3600s", gw.scheduler.failureBase(), gw.scheduler.rateLimitBase(), gw.scheduler.rateLimitMax())
	}
	// Old scheduler records a proxy429; migration preserves remaining capped
	// at the fixed 3600s max, not recomputed.
	oldS := newTargetScheduler(15*time.Second, 300*time.Second)
	ch := oldS.noteProxy429Failure(TierZen, "shared", "direct", "rate_limited", 429, 0, 0)
	remainingBefore := time.Duration(ch.CooldownUntil - time.Now().UnixNano())
	next := newTargetScheduler(10*time.Second, 300*time.Second)
	next.migrateFrom(oldS)
	got := next.proxy429CoolUntil(TierZen, "shared", "direct")
	remainingAfter := time.Duration(got - time.Now().UnixNano())
	if remainingAfter <= 0 {
		t.Fatal("migrated proxy429 must stay future")
	}
	if remainingAfter > 3600*time.Second {
		t.Fatalf("migrated remaining=%v exceeds fixed 3600s max", remainingAfter)
	}
	diff := remainingBefore - remainingAfter
	if diff < 0 {
		diff = -diff
	}
	if diff > 5*time.Second {
		t.Fatalf("migration must preserve remaining, before=%v after=%v", remainingBefore, remainingAfter)
	}
	// Non-429 layers keep the generic 5-minute clamp: inject a far-future
	// target and migrate.
	oldTarget := newTargetScheduler(10*time.Second, 300*time.Second)
	oldTarget.mu.Lock()
	oldTarget.targetState["t\x00c\x00p\x00x\x00m"] = &targetEntry{failures: 1, cooldownUntil: time.Now().Add(1000 * time.Second).UnixNano(), lastFailureAt: time.Now().UnixNano(), lastStatus: 500}
	oldTarget.mu.Unlock()
	newTarget := newTargetScheduler(10*time.Second, 300*time.Second)
	newTarget.migrateFrom(oldTarget)
	remTarget := time.Duration(newTarget.targetCoolUntil("t\x00c\x00p\x00x\x00m") - time.Now().UnixNano())
	if remTarget <= 0 || remTarget > targetBackoffCap {
		t.Fatalf("migrated target=%v must clamp to generic 5m", remTarget)
	}
}

func TestAdminPerformancePropagatesRateLimit(t *testing.T) {
	view := ConfigView{Performance: PerformanceConfig{FailureCooldownSeconds: 10, RateLimitCooldownSeconds: 60, RateLimitCooldownMaxSeconds: 600}}
	data, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"rate_limit_cooldown_seconds":60`) {
		t.Fatalf("ConfigView must propagate rate_limit min: %s", data)
	}
	if !strings.Contains(string(data), `"rate_limit_cooldown_max_seconds":600`) {
		t.Fatalf("ConfigView must propagate rate_limit max: %s", data)
	}
	var update ConfigUpdate
	if err := json.Unmarshal([]byte(`{"performance":{"max_idle_conns":1,"max_idle_conns_per_host":1,"max_conns_per_host":0,"idle_conn_timeout_seconds":1,"connect_timeout_seconds":1,"failure_cooldown_seconds":10,"rate_limit_cooldown_seconds":60,"rate_limit_cooldown_max_seconds":600}}`), &update); err != nil {
		t.Fatal(err)
	}
	if update.Performance.RateLimitCooldownSeconds != 60 {
		t.Fatalf("ConfigUpdate rate_limit min=%d want 60", update.Performance.RateLimitCooldownSeconds)
	}
	if update.Performance.RateLimitCooldownMaxSeconds != 600 {
		t.Fatalf("ConfigUpdate rate_limit max=%d want 600", update.Performance.RateLimitCooldownMaxSeconds)
	}
}

func TestWebUIRateLimitFieldAndAutosave(t *testing.T) {
	html := readConfigWebUI(t)
	for _, needle := range []string{
		`id="c-ratelimit-cooldown"`,
		`429 冷却（秒）`,
		`rate_limit_cooldown_seconds`,
		`"c-ratelimit-cooldown"`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("webui rate-limit field missing %q", needle)
		}
	}
	for _, stale := range []string{`id="c-ratelimit-cooldown-max"`, `429 冷却最小值`, `429 冷却最大值`, `rate_limit_cooldown_max_seconds`, `"c-ratelimit-cooldown-max"`, "429 冷却基准"} {
		if strings.Contains(html, stale) {
			t.Fatalf("legacy 429 max field must stay removed: %q", stale)
		}
	}
	// Single 429 input carries the 300..3600 validation limit (3600 is a cap, not a second field).
	for _, id := range []string{`id="c-ratelimit-cooldown"`} {
		tagIdx := strings.Index(html, id)
		if tagIdx < 0 {
			t.Fatalf("missing %s tag", id)
		}
		tagStart := strings.LastIndex(html[:tagIdx], "<input")
		if tagStart < 0 {
			t.Fatalf("missing %s input tag", id)
		}
		tagEnd := strings.Index(html[tagStart:], ">")
		if tagEnd < 0 {
			t.Fatalf("unterminated %s input tag", id)
		}
		tag := html[tagStart : tagStart+tagEnd+1]
		if !strings.Contains(tag, `type="number"`) {
			t.Fatalf("%s must stay type=number, got %q", id, tag)
		}
		if !strings.Contains(tag, `min="300"`) || !strings.Contains(tag, `max="3600"`) {
			t.Fatalf("%s must carry min=300 max=3600, got %q", id, tag)
		}
	}
	// Numeric commit-only binding: change/blur remain, input must not autosave.
	bindIdx := strings.Index(html, "(function bindConfigAutosave(){")
	if bindIdx < 0 {
		t.Fatal("missing bindConfigAutosave")
	}
	bindEnd := strings.Index(html[bindIdx:], "function revealConfig()")
	if bindEnd < 0 {
		t.Fatal("missing revealConfig boundary")
	}
	bindBlock := html[bindIdx : bindIdx+bindEnd]
	numericList := `["c-session","c-attempts","c-timeout","c-attempt-timeout","c-transient-interval","c-refresh","c-idle","c-idle-host","c-max-host","c-idle-timeout","c-connect","c-cooldown","c-ratelimit-cooldown","c-suspect-cooldown","c-ring","c-hist-retention","c-hist-max"]`
	numIdx := strings.Index(bindBlock, numericList)
	if numIdx < 0 {
		t.Fatal("bind block must carry the numeric commit-only list with the single 429 field")
	}
	seg := bindBlock[numIdx : numIdx+800]
	if !strings.Contains(seg, `on(id,"change"`) {
		t.Fatal("numeric 429 fields must save on change")
	}
	if !strings.Contains(seg, `on(id,"blur"`) {
		t.Fatal("numeric 429 fields must save on blur")
	}
	if strings.Contains(seg, `on(id,"input"`) {
		t.Fatal("numeric 429 fields must not autosave on input keystrokes")
	}
	// Collect/fill round-trip the single value with its 300 default.
	if !strings.Contains(html, `rate_limit_cooldown_seconds:num("c-ratelimit-cooldown")`) {
		t.Fatal("collect must round-trip rate_limit_cooldown_seconds")
	}
	if strings.Contains(html, `rate_limit_cooldown_max_seconds`) {
		t.Fatal("collect must not emit rate_limit_cooldown_max_seconds")
	}
	if !strings.Contains(html, `$("c-ratelimit-cooldown").value=`) {
		t.Fatal("fill must round-trip rate_limit_cooldown_seconds")
	}
	if !strings.Contains(html, `||300`) {
		t.Fatal("fill must default the single 429 field to 300")
	}
}
