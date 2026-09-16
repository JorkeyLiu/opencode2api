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
		t.Fatalf("default rate_limit min=%d want 300", base.Performance.RateLimitCooldownSeconds)
	}
	if base.Performance.RateLimitCooldownMaxSeconds != 3600 {
		t.Fatalf("default rate_limit max=%d want 3600", base.Performance.RateLimitCooldownMaxSeconds)
	}
	// Missing fields keep defaults 300/3600 via Unmarshal default seed.
	var cfg Config
	raw := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"zen_keys":["z1"],"prefer":"go","proxy_pools":{"shared":{"proxies":["direct"],"proxyfile":""}},"proxy_routing":{"anonymous":"shared","zen":"shared","go":"shared"},"upstream":{"zen":"https://opencode.ai/zen","go":"https://opencode.ai/zen/go"},"retry":{"max_attempts":1,"timeout_seconds":5},"models":{"refresh_seconds":5,"protocols":{}},"performance":{"max_idle_conns":1,"max_idle_conns_per_host":1,"max_conns_per_host":0,"idle_conn_timeout_seconds":1,"connect_timeout_seconds":1,"failure_cooldown_seconds":10},"logging":{"level":"info","ring_size":100},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Performance.RateLimitCooldownSeconds != 300 {
		t.Fatalf("missing rate_limit min=%d want default 300", cfg.Performance.RateLimitCooldownSeconds)
	}
	if cfg.Performance.RateLimitCooldownMaxSeconds != 3600 {
		t.Fatalf("missing rate_limit max=%d want default 3600", cfg.Performance.RateLimitCooldownMaxSeconds)
	}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.Performance.RateLimitCooldownSeconds != 300 {
		t.Fatalf("normalized missing rate_limit min=%d want 300", normalized.Performance.RateLimitCooldownSeconds)
	}
	if normalized.Performance.RateLimitCooldownMaxSeconds != 3600 {
		t.Fatalf("normalized missing rate_limit max=%d want 3600", normalized.Performance.RateLimitCooldownMaxSeconds)
	}
	// Existing explicit old rate base values stay respected (not rewritten).
	explicit := normalized
	explicit.Performance.RateLimitCooldownSeconds = 15
	explicit.Performance.RateLimitCooldownMaxSeconds = 0
	gotExplicit, err := NormalizeConfig("config.json", explicit)
	if err != nil {
		t.Fatal(err)
	}
	if gotExplicit.Performance.RateLimitCooldownSeconds != 15 {
		t.Fatalf("explicit old min must stay 15, got %d", gotExplicit.Performance.RateLimitCooldownSeconds)
	}
	if gotExplicit.Performance.RateLimitCooldownMaxSeconds != 3600 {
		t.Fatalf("missing max with explicit min must default 3600, got %d", gotExplicit.Performance.RateLimitCooldownMaxSeconds)
	}
	// Valid min/max pairs pass.
	for _, pair := range [][2]int{{1, 1}, {1, 3600}, {300, 3600}, {60, 600}, {600, 600}, {600, 7200}} {
		c := normalized
		c.Performance.RateLimitCooldownSeconds = pair[0]
		c.Performance.RateLimitCooldownMaxSeconds = pair[1]
		if _, err := NormalizeConfig("config.json", c); err != nil {
			t.Fatalf("rate_limit=(%d,%d) must pass: %v", pair[0], pair[1], err)
		}
	}
	// Zero normalizes to defaults (admin PUT compat).
	zeroMin := normalized
	zeroMin.Performance.RateLimitCooldownSeconds = 0
	zeroMin.Performance.RateLimitCooldownMaxSeconds = 3600
	gotZero, err := NormalizeConfig("config.json", zeroMin)
	if err != nil {
		t.Fatalf("rate_limit min=0 must default, got err %v", err)
	}
	if gotZero.Performance.RateLimitCooldownSeconds != 300 {
		t.Fatalf("rate_limit min=0 normalized=%d want 300", gotZero.Performance.RateLimitCooldownSeconds)
	}
	zeroMax := normalized
	zeroMax.Performance.RateLimitCooldownSeconds = 300
	zeroMax.Performance.RateLimitCooldownMaxSeconds = 0
	gotZeroMax, err := NormalizeConfig("config.json", zeroMax)
	if err != nil {
		t.Fatalf("rate_limit max=0 must default, got err %v", err)
	}
	if gotZeroMax.Performance.RateLimitCooldownMaxSeconds != 3600 {
		t.Fatalf("rate_limit max=0 normalized=%d want 3600", gotZeroMax.Performance.RateLimitCooldownMaxSeconds)
	}
	// Negatives and max<min fail.
	for _, pair := range [][2]int{{-1, 3600}, {300, -5}, {0 - 1, 1}, {600, 599}, {300, 299}} {
		c := normalized
		c.Performance.RateLimitCooldownSeconds = pair[0]
		c.Performance.RateLimitCooldownMaxSeconds = pair[1]
		if _, err := NormalizeConfig("config.json", c); err == nil {
			t.Fatalf("rate_limit=(%d,%d) must fail", pair[0], pair[1])
		}
	}
	// Unknown field inside performance is rejected.
	bad := `{"max_idle_conns":1,"max_idle_conns_per_host":1,"max_conns_per_host":0,"idle_conn_timeout_seconds":1,"connect_timeout_seconds":1,"failure_cooldown_seconds":1,"rate_limit_cooldown_seconds":300,"rate_limit_cooldown_max_seconds":3600,"bogus":1}`
	full := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"zen_keys":["z1"],"prefer":"go","proxy_pools":{"shared":{"proxies":["direct"],"proxyfile":""}},"proxy_routing":{"anonymous":"shared","zen":"shared","go":"shared"},"upstream":{"zen":"https://opencode.ai/zen","go":"https://opencode.ai/zen/go"},"retry":{"max_attempts":1,"timeout_seconds":5},"models":{"refresh_seconds":5,"protocols":{}},"performance":` + bad + `,"logging":{"level":"info","ring_size":100},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var cfg2 Config
	if err := json.Unmarshal([]byte(full), &cfg2); err == nil {
		t.Fatal("unknown performance field must fail")
	}
}

func TestSchedulerDualBaseSeparation(t *testing.T) {
	s := newTargetScheduler(10*time.Second, 60*time.Second, 600*time.Second)
	if s.failureBase() != 10*time.Second {
		t.Fatalf("failure base=%v want 10s", s.failureBase())
	}
	if s.rateLimitBase() != 60*time.Second {
		t.Fatalf("rate min=%v want 60s", s.rateLimitBase())
	}
	if s.rateLimitMax() != 600*time.Second {
		t.Fatalf("rate max=%v want 600s", s.rateLimitMax())
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
	// Non-429 Retry-After still caps at the generic 5 minutes.
	capped := s.backoffDelay(1, "cap-fail", 400*time.Second)
	if capped != targetBackoffCap {
		t.Fatalf("failure Retry-After cap=%v want 5m", capped)
	}
	// 429 Retry-After clamps at the configured 429 max, not the generic cap.
	cappedR := s.rateLimitBackoffDelay(1, "cap-rate", 1000*time.Second)
	if cappedR != 600*time.Second {
		t.Fatalf("rate Retry-After cap=%v want 600s", cappedR)
	}
	// Consecutive 429 strikes escalate exponentially toward the max.
	second := s.rateLimitBackoffDelay(2, "cap-rate", 0)
	if second < 96*time.Second || second > 144*time.Second {
		t.Fatalf("rate second strike=%v want ~120s", second)
	}
	third := s.rateLimitBackoffDelay(3, "cap-rate", 0)
	if third < 192*time.Second || third > 288*time.Second {
		t.Fatalf("rate third strike=%v want ~240s", third)
	}
	clamped := s.rateLimitBackoffDelay(4, "cap-rate", 0)
	if clamped < 384*time.Second || clamped > 600*time.Second {
		t.Fatalf("rate fourth strike=%v want in [384s,600s]", clamped)
	}
	// A small max clamps even the first strike.
	small := newTargetScheduler(10*time.Second, 60*time.Second, 100*time.Second)
	firstSmall := small.rateLimitBackoffDelay(1, "small-rate", 0)
	if firstSmall < 48*time.Second || firstSmall > 72*time.Second {
		t.Fatalf("small-max first=%v want ~60s", firstSmall)
	}
	secondSmall := small.rateLimitBackoffDelay(2, "small-rate", 0)
	if secondSmall < 80*time.Second || secondSmall > 100*time.Second {
		t.Fatalf("small-max second=%v want in [80s,100s]", secondSmall)
	}
	// Single-arg scheduler keeps failure/rate bases equal with default max.
	legacy := newTargetScheduler(15 * time.Second)
	if legacy.failureBase() != legacy.rateLimitBase() {
		t.Fatalf("legacy bases differ: %v vs %v", legacy.failureBase(), legacy.rateLimitBase())
	}
	if legacy.rateLimitMax() != defaultRateLimitMaxSeconds*time.Second {
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
	// Absurd Retry-After header clamps to the configured max.
	s := newTargetScheduler(10*time.Second, 60*time.Second, 600*time.Second)
	got := s.rateLimitBackoffDelay(1, "huge-ra", secondsToDuration(hugeSec))
	if got != 600*time.Second {
		t.Fatalf("huge Retry-After must clamp to 600s, got %v", got)
	}
	// Retry-After parse saturates instead of overflowing.
	if got := parseRetryAfter("99999999999999999999"); got < 0 {
		t.Fatalf("huge Retry-After parse must not go negative: %v", got)
	}
}

func TestGatewayDualBaseAndMigrationKeepsRemaining(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	cfg.Performance.FailureCooldownSeconds = 10
	cfg.Performance.RateLimitCooldownSeconds = 60
	cfg.Performance.RateLimitCooldownMaxSeconds = 600
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	if gw.scheduler.failureBase() != 10*time.Second || gw.scheduler.rateLimitBase() != 60*time.Second || gw.scheduler.rateLimitMax() != 600*time.Second {
		t.Fatalf("gateway bases=%v/%v/%v want 10s/60s/600s", gw.scheduler.failureBase(), gw.scheduler.rateLimitBase(), gw.scheduler.rateLimitMax())
	}
	// Old scheduler with different bases records a proxy429; migration must
	// preserve remaining (capped at the NEW 429 max), not recompute.
	oldS := newTargetScheduler(15*time.Second, 15*time.Second, 3600*time.Second)
	ch := oldS.noteProxy429Failure(TierZen, "shared", "direct", "rate_limited", 429, 0, 0)
	remainingBefore := time.Duration(ch.CooldownUntil - time.Now().UnixNano())
	next := newTargetScheduler(10*time.Second, 60*time.Second, 600*time.Second)
	next.migrateFrom(oldS)
	got := next.proxy429CoolUntil(TierZen, "shared", "direct")
	remainingAfter := time.Duration(got - time.Now().UnixNano())
	if remainingAfter <= 0 {
		t.Fatal("migrated proxy429 must stay future")
	}
	if remainingAfter > 600*time.Second {
		t.Fatalf("migrated remaining=%v exceeds new 429 max", remainingAfter)
	}
	diff := remainingBefore - remainingAfter
	if diff < 0 {
		diff = -diff
	}
	if diff > 5*time.Second {
		t.Fatalf("migration must preserve remaining, before=%v after=%v", remainingBefore, remainingAfter)
	}
	// A 1000s proxy429 remaining clamps to the NEW 429 max (600s), not 5m.
	oldBig := newTargetScheduler(10*time.Second, 60*time.Second, 3600*time.Second)
	oldBig.noteProxy429Failure(TierZen, "shared", "direct", "rate_limited", 429, 1000*time.Second, 0)
	shrunk := newTargetScheduler(10*time.Second, 60*time.Second, 600*time.Second)
	shrunk.migrateFrom(oldBig)
	remBig := time.Duration(shrunk.proxy429CoolUntil(TierZen, "shared", "direct") - time.Now().UnixNano())
	if remBig <= 300*time.Second || remBig > 600*time.Second {
		t.Fatalf("migrated big proxy429=%v want in (5m,600s]", remBig)
	}
	// The same 1000s on a credential429 entry clamps identically.
	oldCred := newTargetScheduler(10*time.Second, 60*time.Second, 3600*time.Second)
	oldCred.noteCredential429Failure("zen:credM", "rate_limited", 429, 1000*time.Second, 0)
	shrunkCred := newTargetScheduler(10*time.Second, 60*time.Second, 600*time.Second)
	shrunkCred.migrateFrom(oldCred)
	if _, _, ok := shrunkCred.credential429CooldownStatus("zen:credM"); !ok {
		t.Fatal("migrated credential429 must stay future")
	}
	// Non-429 layers keep the generic 5-minute clamp even when the 429 max
	// is larger: inject a far-future target and migrate.
	oldTarget := newTargetScheduler(10*time.Second, 60*time.Second, 3600*time.Second)
	oldTarget.mu.Lock()
	oldTarget.targetState["t\x00c\x00p\x00x\x00m"] = &targetEntry{failures: 1, cooldownUntil: time.Now().Add(1000 * time.Second).UnixNano(), lastFailureAt: time.Now().UnixNano(), lastStatus: 500}
	oldTarget.mu.Unlock()
	newTarget := newTargetScheduler(10*time.Second, 60*time.Second, 3600*time.Second)
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
		`id="c-ratelimit-cooldown-max"`,
		`429 冷却最小值`,
		`429 冷却最大值`,
		`rate_limit_cooldown_seconds`,
		`rate_limit_cooldown_max_seconds`,
		`"c-ratelimit-cooldown"`,
		`"c-ratelimit-cooldown-max"`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("webui rate-limit field missing %q", needle)
		}
	}
	if strings.Contains(html, "429 冷却基准") {
		t.Fatal("stale single 429 label must be replaced by min/max labels")
	}
	// Both configured values use minimum-only validation: neither input caps.
	for _, id := range []string{`id="c-ratelimit-cooldown"`, `id="c-ratelimit-cooldown-max"`} {
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
		if strings.Contains(tag, "max=") {
			t.Fatalf("%s must not carry a max cap, got %q", id, tag)
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
	numericList := `["c-session","c-attempts","c-timeout","c-refresh","c-idle","c-idle-host","c-max-host","c-idle-timeout","c-connect","c-cooldown","c-ratelimit-cooldown","c-ratelimit-cooldown-max","c-ring","c-hist-retention","c-hist-max"]`
	numIdx := strings.Index(bindBlock, numericList)
	if numIdx < 0 {
		t.Fatal("bind block must carry the numeric commit-only list with both 429 fields")
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
	// Collect/fill round-trip both values.
	if !strings.Contains(html, `rate_limit_cooldown_seconds:num("c-ratelimit-cooldown")`) {
		t.Fatal("collect must round-trip rate_limit_cooldown_seconds")
	}
	if !strings.Contains(html, `rate_limit_cooldown_max_seconds:num("c-ratelimit-cooldown-max")`) {
		t.Fatal("collect must round-trip rate_limit_cooldown_max_seconds")
	}
	if !strings.Contains(html, `$("c-ratelimit-cooldown").value=v.performance.rate_limit_cooldown_seconds`) {
		t.Fatal("fill must round-trip rate_limit_cooldown_seconds")
	}
	if !strings.Contains(html, `$("c-ratelimit-cooldown-max").value=v.performance.rate_limit_cooldown_max_seconds`) {
		t.Fatal("fill must round-trip rate_limit_cooldown_max_seconds")
	}
}
