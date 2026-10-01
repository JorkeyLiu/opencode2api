package app

import (
	"encoding/json"
	"testing"
	"time"
)

// Above-old-limit acceptance: old business maxima are gone; only minima and
// technical representability bound the six fields. Ring keeps its memory
// protection. No sleeps, no large allocs: NormalizeConfig plus scheduler time
// comparisons only.

func TestUnboundedAboveOldLimitParseNormalizeRoundTrip(t *testing.T) {
	base := testBaseConfig()
	base.WebUI.Enabled = true
	base.WebUI.Listen = "127.0.0.1:1"
	base.WebUI.Username = "u"
	base.WebUI.PasswordHash = "test-hash"
	canon, err := NormalizeConfig("config.json", base)
	if err != nil {
		t.Fatalf("base normalize: %v", err)
	}
	type mut struct {
		name string
		set  func(*Config)
		want func(Config) int
	}
	cases := []mut{
		{"transient31", func(c *Config) { c.Retry.TransientRetryIntervalSeconds = 31; c.retryTransientIntervalPresent = true }, func(c Config) int { return c.Retry.TransientRetryIntervalSeconds }},
		{"transient120", func(c *Config) { c.Retry.TransientRetryIntervalSeconds = 120; c.retryTransientIntervalPresent = true }, func(c Config) int { return c.Retry.TransientRetryIntervalSeconds }},
		{"ratelimit3601", func(c *Config) { c.Performance.RateLimitCooldownSeconds = 3601 }, func(c Config) int { return c.Performance.RateLimitCooldownSeconds }},
		{"ratelimit7200", func(c *Config) { c.Performance.RateLimitCooldownSeconds = 7200 }, func(c Config) int { return c.Performance.RateLimitCooldownSeconds }},
		{"suspect301", func(c *Config) {
			c.Performance.TransportSuspectCooldownSeconds = 301
			c.performanceSuspectPresent = true
		}, func(c Config) int { return c.Performance.TransportSuspectCooldownSeconds }},
		{"suspect900", func(c *Config) {
			c.Performance.TransportSuspectCooldownSeconds = 900
			c.performanceSuspectPresent = true
		}, func(c Config) int { return c.Performance.TransportSuspectCooldownSeconds }},
		{"session10081", func(c *Config) { c.WebUI.SessionTTLMinutes = 10081 }, func(c Config) int { return c.WebUI.SessionTTLMinutes }},
		{"session43200", func(c *Config) { c.WebUI.SessionTTLMinutes = 43200 }, func(c Config) int { return c.WebUI.SessionTTLMinutes }},
		{"retention91", func(c *Config) { c.History.RetentionDays = 91 }, func(c Config) int { return c.History.RetentionDays }},
		{"retention365", func(c *Config) { c.History.RetentionDays = 365 }, func(c Config) int { return c.History.RetentionDays }},
		{"bytes2049", func(c *Config) { c.History.MaxBytesMB = 2049 }, func(c Config) int { return c.History.MaxBytesMB }},
		{"bytes8192", func(c *Config) { c.History.MaxBytesMB = 8192 }, func(c Config) int { return c.History.MaxBytesMB }},
	}
	for _, tc := range cases {
		c := canon
		// Deep-copy maps that NormalizeConfig may touch (avoid cross-case bleed).
		c.Models.Protocols = map[string]string{}
		tc.set(&c)
		got, err := NormalizeConfig("config.json", c)
		if err != nil {
			t.Fatalf("%s must pass, got %v", tc.name, err)
		}
		if tc.want(got) == 0 {
			t.Fatalf("%s normalized to zero", tc.name)
		}
		// Save round-trip: Marshal canonical, Unmarshal strict, Normalize again.
		data, err := json.Marshal(got)
		if err != nil {
			t.Fatalf("%s marshal: %v", tc.name, err)
		}
		var back Config
		if err := json.Unmarshal(data, &back); err != nil {
			t.Fatalf("%s unmarshal round-trip: %v", tc.name, err)
		}
		// Presence flags are lost across JSON; restore the explicit markers
		// that the case relied on so the second normalize keeps the value.
		back.retryTransientIntervalPresent = true
		back.performanceSuspectPresent = true
		got2, err := NormalizeConfig("config.json", back)
		if err != nil {
			t.Fatalf("%s round-trip normalize: %v", tc.name, err)
		}
		if tc.want(got2) != tc.want(got) {
			t.Fatalf("%s round-trip mismatch: %d vs %d", tc.name, tc.want(got2), tc.want(got))
		}
	}
}

func TestUnboundedMinInvalidStillFail(t *testing.T) {
	base := testBaseConfig()
	base.WebUI.Enabled = true
	base.WebUI.Listen = "127.0.0.1:1"
	base.WebUI.Username = "u"
	base.WebUI.PasswordHash = "test-hash"
	canon, err := NormalizeConfig("config.json", base)
	if err != nil {
		t.Fatalf("base normalize: %v", err)
	}
	// Below-minimum cases must still fail.
	c := canon
	c.Retry.TransientRetryIntervalSeconds = -1
	c.retryTransientIntervalPresent = true
	if _, err := NormalizeConfig("config.json", c); err == nil {
		t.Fatalf("transient -1 must fail")
	}
	c = canon
	c.Performance.RateLimitCooldownSeconds = 299
	if _, err := NormalizeConfig("config.json", c); err == nil {
		t.Fatalf("rate_limit 299 must fail")
	}
	c = canon
	c.Performance.TransportSuspectCooldownSeconds = 0
	c.performanceSuspectPresent = true
	if _, err := NormalizeConfig("config.json", c); err == nil {
		t.Fatalf("explicit suspect 0 must fail")
	}
	c = canon
	c.WebUI.SessionTTLMinutes = 4
	if _, err := NormalizeConfig("config.json", c); err == nil {
		t.Fatalf("session 4 must fail")
	}
	c = canon
	c.History.RetentionDays = 0
	if _, err := NormalizeConfig("config.json", c); err == nil {
		t.Fatalf("retention 0 must fail")
	}
	c = canon
	c.History.MaxBytesMB = 15
	if _, err := NormalizeConfig("config.json", c); err == nil {
		t.Fatalf("bytes 15 must fail")
	}
	// Ring memory protection stays.
	c = canon
	c.Logging.RingSize = 50001
	if _, err := NormalizeConfig("config.json", c); err == nil {
		t.Fatalf("ring 50001 must fail")
	}
	c = canon
	c.Logging.RingSize = 99
	if _, err := NormalizeConfig("config.json", c); err == nil {
		t.Fatalf("ring 99 must fail")
	}
	// attempt_timeout relation stays.
	c = canon
	c.Retry.TimeoutSeconds = 5
	c.Retry.AttemptTimeoutSeconds = 10
	c.retryAttemptTimeoutPresent = true
	if _, err := NormalizeConfig("config.json", c); err == nil {
		t.Fatalf("attempt>timeout must fail")
	}
	// Strict unknown field stays.
	raw := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["k2"],"proxy_pools":{"shared":{"proxies":["direct"]}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"upstream":{"zen":"https://opencode.ai/zen"},"retry":{"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":5,"bogus":1},"models":{"refresh_seconds":300,"protocols":{}},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300,"transport_suspect_cooldown_seconds":15},"logging":{"level":"info","ring_size":2000},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var bad Config
	if err := json.Unmarshal([]byte(raw), &bad); err == nil {
		t.Fatalf("unknown field must fail")
	}
}

func TestTransient120NotClamped(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Retry.TransientRetryIntervalSeconds = 120
	cfg.retryTransientIntervalPresent = true
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatalf("normalize 120: %v", err)
	}
	gw, err := NewGateway(norm, nil, NewMonitor())
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	if got := gw.transientInterval(); got != 120*time.Second {
		t.Fatalf("transientInterval=%v want 120s (no 30s clamp)", got)
	}
}

func TestSuspect900MigrationNotTruncated(t *testing.T) {
	// Old scheduler holds a still-future suspect remaining of ~800s; the new
	// scheduler configures base 900s (effective cap 900s), so migration must
	// preserve ~800s instead of truncating to the old 300s business cap.
	oldS := newTargetScheduler(15*time.Second, 300*time.Second, 15*time.Second)
	now := time.Now().UnixNano()
	oldS.mu.Lock()
	if oldS.suspectState == nil {
		oldS.suspectState = make(map[string]*transportSuspectEntry)
	}
	oldS.suspectState[suspectIdentity(TierZen, "shared", "direct")] = &transportSuspectEntry{
		failures: 1, cooldownUntil: now + int64(800*time.Second), lastFailureAt: now, lastStatus: 502,
	}
	oldS.mu.Unlock()
	next := newTargetScheduler(15*time.Second, 300*time.Second, 900*time.Second)
	next.migrateFrom(oldS)
	got := next.suspectCoolUntil(TierZen, "shared", "direct")
	remaining := time.Duration(got - time.Now().UnixNano())
	if remaining <= 300*time.Second {
		t.Fatalf("suspect 900 migration truncated to %v, want ~800s", remaining)
	}
	if remaining > 900*time.Second+5*time.Second || remaining < 790*time.Second {
		t.Fatalf("suspect 900 migration remaining=%v want ~800s within cap 900s", remaining)
	}
}

func TestRate7200AndFailure600BackoffAndMigration(t *testing.T) {
	s := newTargetScheduler(600*time.Second, 7200*time.Second, 15*time.Second)
	if got := s.rateLimitMax(); got != 7200*time.Second {
		t.Fatalf("rateLimitMax=%v want 7200s (max(1h,base))", got)
	}
	if got := s.failureCap(); got != 600*time.Second {
		t.Fatalf("failureCap=%v want 600s (max(5m,base))", got)
	}
	// First-strike numerical assertions only: no sleeps.
	dFail := s.backoffDelay(1, "probe-fail-600", 0)
	if dFail < 480*time.Second || dFail > 720*time.Second {
		t.Fatalf("failure delay=%v want in [480s,720s] for base 600", dFail)
	}
	dRate := s.rateLimitBackoffDelay(1, "probe-rate-7200", 0)
	if dRate < 5760*time.Second || dRate > 8640*time.Second {
		t.Fatalf("rate delay=%v want in [5760s,8640s] for base 7200", dRate)
	}
	// Retry-After beyond the effective cap clamps to the effective cap.
	if got := s.backoffDelay(1, "cap-fail-600", 10000*time.Second); got != 600*time.Second {
		t.Fatalf("failure Retry-After cap=%v want 600s", got)
	}
	if got := s.rateLimitBackoffDelay(1, "cap-rate-7200", 20000*time.Second); got != 7200*time.Second {
		t.Fatalf("rate Retry-After cap=%v want 7200s", got)
	}
	// Migration respects the larger base: inject still-future state and
	// migrate to an identical-base scheduler; remaining must survive the
	// effective caps rather than being pressed to 3600/300.
	oldS := newTargetScheduler(600*time.Second, 7200*time.Second, 15*time.Second)
	now := time.Now().UnixNano()
	oldS.mu.Lock()
	if oldS.proxy429State == nil {
		oldS.proxy429State = make(map[string]*proxy429Entry)
	}
	oldS.proxy429State[proxy429Identity(TierZen, "shared", "direct")] = &proxy429Entry{
		failures: 1, cooldownUntil: now + int64(6000*time.Second), lastFailureAt: now, lastStatus: 429,
	}
	oldS.targetState["t\x00c\x00p\x00x\x00m"] = &targetEntry{failures: 1, cooldownUntil: now + int64(500*time.Second), lastFailureAt: now, lastStatus: 500}
	oldS.mu.Unlock()
	next := newTargetScheduler(600*time.Second, 7200*time.Second, 15*time.Second)
	next.migrateFrom(oldS)
	remRate := time.Duration(next.proxy429CoolUntil(TierZen, "shared", "direct") - time.Now().UnixNano())
	if remRate < 5900*time.Second || remRate > 6000*time.Second+5*time.Second {
		t.Fatalf("migrated proxy429=%v want ~6000s (effective 7200s, not 3600)", remRate)
	}
	remTarget := time.Duration(next.targetCoolUntil("t\x00c\x00p\x00x\x00m") - time.Now().UnixNano())
	if remTarget < 490*time.Second || remTarget > 500*time.Second+5*time.Second {
		t.Fatalf("migrated target=%v want ~500s (effective 600s, not 300)", remTarget)
	}
}

func TestTechnicalRepresentabilityBoundaries(t *testing.T) {
	base := testBaseConfig()
	base.WebUI.Enabled = true
	base.WebUI.Listen = "127.0.0.1:1"
	base.WebUI.Username = "u"
	base.WebUI.PasswordHash = "test-hash"
	canon, err := NormalizeConfig("config.json", base)
	if err != nil {
		t.Fatalf("base normalize: %v", err)
	}
	// Critical safe values pass; max+1 is rejected as unrepresentable.
	// No allocation of huge memory/disk and no waiting on huge durations.
	c := canon
	c.Retry.TimeoutSeconds = 9223372036
	if _, err := NormalizeConfig("config.json", c); err != nil {
		t.Fatalf("timeout max safe must pass, got %v", err)
	}
	c = canon
	c.Retry.TimeoutSeconds = 9223372037
	if _, err := NormalizeConfig("config.json", c); err == nil {
		t.Fatalf("timeout max+1 must fail")
	}
	c = canon
	c.Models.RefreshSeconds = 4611686018
	if _, err := NormalizeConfig("config.json", c); err != nil {
		t.Fatalf("refresh max safe must pass, got %v", err)
	}
	c = canon
	c.Models.RefreshSeconds = 4611686019
	if _, err := NormalizeConfig("config.json", c); err == nil {
		t.Fatalf("refresh max+1 must fail (2x window)")
	}
	c = canon
	c.WebUI.SessionTTLMinutes = 35791394
	if _, err := NormalizeConfig("config.json", c); err != nil {
		t.Fatalf("session max safe must pass, got %v", err)
	}
	c = canon
	c.WebUI.SessionTTLMinutes = 35791395
	if _, err := NormalizeConfig("config.json", c); err == nil {
		t.Fatalf("session max+1 must fail (32-bit cookie)")
	}
	c = canon
	c.History.RetentionDays = 106751
	if _, err := NormalizeConfig("config.json", c); err != nil {
		t.Fatalf("retention max safe must pass, got %v", err)
	}
	c = canon
	c.History.RetentionDays = 106752
	if _, err := NormalizeConfig("config.json", c); err == nil {
		t.Fatalf("retention max+1 must fail")
	}
	c = canon
	c.History.MaxBytesMB = 8796093022207
	if _, err := NormalizeConfig("config.json", c); err != nil {
		t.Fatalf("bytes max safe must pass, got %v", err)
	}
	c = canon
	c.History.MaxBytesMB = 8796093022208
	if _, err := NormalizeConfig("config.json", c); err == nil {
		t.Fatalf("bytes max+1 must fail")
	}
	// Saturated conversions never go negative and cookie helper never
	// overflows Go int.
	hugeSec := int(1) << 40
	if got := secondsToDuration(hugeSec); got <= 0 {
		t.Fatalf("secondsToDuration huge must stay positive, got %v", got)
	}
	if got := minutesToDuration(hugeSec); got <= 0 {
		t.Fatalf("minutesToDuration huge must stay positive, got %v", got)
	}
	if got := daysToDuration(hugeSec); got <= 0 {
		t.Fatalf("daysToDuration huge must stay positive, got %v", got)
	}
	if got := megabytesToBytes(hugeSec); got <= 0 {
		t.Fatalf("megabytesToBytes huge must stay positive, got %d", got)
	}
	if got := sessionCookieMaxAgeSeconds(int(1) << 40); got <= 0 {
		t.Fatalf("cookie MaxAge huge must stay positive, got %d", got)
	}
	if got := historyMaxRange(106751); got <= 0 {
		t.Fatalf("historyMaxRange max safe must stay positive, got %v", got)
	}
	if got := refreshStaleAfter(4611686018); got <= 0 {
		t.Fatalf("refreshStaleAfter max safe must stay positive, got %v", got)
	}
	if got := staleAfterSecondsForAPI(refreshStaleAfter(4611686018)); got <= 0 {
		t.Fatalf("staleAfterSeconds max safe must stay positive, got %d", got)
	}
}

func TestCatalogStaleDirectHugeIntervalNoWrap(t *testing.T) {
	// Directly constructed catalog with the largest Duration must not wrap
	// the doubled staleness threshold negative. No waits: updatedAt is set in
	// the past and Snapshot is evaluated immediately. Directory refresh and
	// availability rules are unchanged; only the arithmetic saturates.
	c := newModelCatalog("", map[string]string{})
	c.SetRefreshInterval(maxDurationValue)
	c.mu.Lock()
	c.updatedAt = time.Now().Add(-2 * time.Minute)
	c.stale = false
	c.mu.Unlock()
	if snap := c.Snapshot(); snap.Stale {
		t.Fatalf("huge refreshAfter must not be stale after 2m (threshold saturates near 292y)")
	}
	// Existing max(2*refreshAfter, 1m) semantics preserved for ordinary values.
	c2 := newModelCatalog("", map[string]string{})
	c2.SetRefreshInterval(300 * time.Second)
	c2.mu.Lock()
	c2.updatedAt = time.Now().Add(-2 * time.Minute)
	c2.stale = false
	c2.mu.Unlock()
	if snap := c2.Snapshot(); snap.Stale {
		t.Fatalf("300s refresh must not be stale after 2m (threshold 10m)")
	}
	c2.mu.Lock()
	c2.updatedAt = time.Now().Add(-11 * time.Minute)
	c2.mu.Unlock()
	if snap := c2.Snapshot(); !snap.Stale {
		t.Fatalf("300s refresh must be stale after 11m (threshold 10m)")
	}
}
