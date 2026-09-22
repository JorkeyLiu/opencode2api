package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// Bounded transport regression fix: legacy missing field with route timeout 60 gets 5.
func TestBoundedLegacyMissing60Gets5(t *testing.T) {
	raw := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["k2"],"proxy_pools":{"shared":{"proxies":["direct"]}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"upstream":{"zen":"https://opencode.ai/zen"},"retry":{"max_attempts":3,"timeout_seconds":60,"transient_max_attempts":3,"transient_retry_interval_seconds":3},"models":{"refresh_seconds":300,"protocols":{}},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300},"logging":{"level":"info","ring_size":2000},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var cfg Config
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if norm.Retry.AttemptTimeoutSeconds != 5 {
		t.Fatalf("legacy missing with timeout 60 want attempt 5 got %d", norm.Retry.AttemptTimeoutSeconds)
	}
	// also test effective gateway timeout
	gw, _ := NewGateway(norm, discardGatewayLogger(), NewMonitor())
	if d := gw.attemptTimeout(); d != 5*time.Second {
		t.Fatalf("gateway attemptTimeout want 5s got %v", d)
	}
	// saved normalized config should persist the field
	data, _ := json.Marshal(norm)
	var saved map[string]any
	_ = json.Unmarshal(data, &saved)
	retry, _ := saved["retry"].(map[string]any)
	if retry == nil {
		t.Fatalf("saved retry missing")
	}
	if v, ok := retry["attempt_timeout_seconds"]; !ok || int(v.(float64)) != 5 {
		t.Fatalf("saved attempt_timeout_seconds want 5 got %v", retry["attempt_timeout_seconds"])
	}
}

func TestBoundedClampingBelow5(t *testing.T) {
	cases := []struct {
		timeout int
		want    int
	}{
		{timeout: 4, want: 4},
		{timeout: 3, want: 3},
		{timeout: 2, want: 2},
		{timeout: 1, want: 1},
		{timeout: 5, want: 5},
	}
	for _, tc := range cases {
		cfg := testBaseConfig()
		cfg.Retry.TimeoutSeconds = tc.timeout
		cfg.Retry.AttemptTimeoutSeconds = 0
		cfg.retryAttemptTimeoutPresent = false
		norm, err := NormalizeConfig("config.json", cfg)
		if err != nil {
			t.Fatalf("timeout %d normalize err %v", tc.timeout, err)
		}
		if norm.Retry.AttemptTimeoutSeconds != tc.want {
			t.Fatalf("timeout %d want %d got %d", tc.timeout, tc.want, norm.Retry.AttemptTimeoutSeconds)
		}
	}
}

func TestBoundedExplicitHonored(t *testing.T) {
	// explicit 10 with timeout 60 stays 10
	raw := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["k2"],"proxy_pools":{"shared":{"proxies":["direct"]}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"upstream":{"zen":"https://opencode.ai/zen"},"retry":{"max_attempts":3,"timeout_seconds":60,"attempt_timeout_seconds":10,"transient_max_attempts":3,"transient_retry_interval_seconds":3},"models":{"refresh_seconds":300,"protocols":{}},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300},"logging":{"level":"info","ring_size":2000},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var cfg Config
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if norm.Retry.AttemptTimeoutSeconds != 10 {
		t.Fatalf("explicit 10 want 10 got %d", norm.Retry.AttemptTimeoutSeconds)
	}
	// explicit zero must be rejected
	rawZero := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["k2"],"proxy_pools":{"shared":{"proxies":["direct"]}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"upstream":{"zen":"https://opencode.ai/zen"},"retry":{"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":0,"transient_max_attempts":3,"transient_retry_interval_seconds":3},"models":{"refresh_seconds":300,"protocols":{}},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300},"logging":{"level":"info","ring_size":2000},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var cfgZ Config
	if err := json.Unmarshal([]byte(rawZero), &cfgZ); err != nil {
		t.Fatalf("unmarshal zero: %v", err)
	}
	if _, err := NormalizeConfig("config.json", cfgZ); err == nil {
		t.Fatalf("explicit zero must be rejected")
	}
	// default remains 5
	if def := defaultConfig(); def.Retry.AttemptTimeoutSeconds != 5 {
		t.Fatalf("default want 5 got %d", def.Retry.AttemptTimeoutSeconds)
	}
}

func TestBoundedSuspectEscalatesAndCaps(t *testing.T) {
	// Use non-standard base to exercise cap.
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Performance.TransportSuspectCooldownSeconds = 15
	// normalized default is 15
	norm, _ := NormalizeConfig("config.json", cfg)
	gw, _ := NewGateway(norm, discardGatewayLogger(), NewMonitor())
	s := gw.scheduler
	pool := "shared"
	proxy := "direct"
	identity := suspectIdentity(TierZen, pool, proxy)

	// First failure
	ch1 := s.noteTransportSuspect(TierZen, pool, proxy, AttemptClassTransportFailure, 0, time.Now().UnixNano())
	if ch1.Failures != 1 {
		t.Fatalf("first failures %d want 1", ch1.Failures)
	}
	delay1 := time.Duration(ch1.CooldownUntil - time.Now().UnixNano())
	// Allow some clock skew but ensure roughly base
	if delay1 < 10*time.Second || delay1 > 20*time.Second {
		t.Fatalf("first delay want ~15s got %v", delay1)
	}

	// Second failure shortly after (within retention) escalates
	time.Sleep(5 * time.Millisecond)
	ch2 := s.noteTransportSuspect(TierZen, pool, proxy, AttemptClassTransportFailure, 0, time.Now().UnixNano())
	if ch2.Failures != 2 {
		t.Fatalf("second failures %d want 2", ch2.Failures)
	}
	delay2 := time.Duration(ch2.CooldownUntil - time.Now().UnixNano())
	if delay2 < 24*time.Second || delay2 > 36*time.Second {
		t.Fatalf("second delay want ~30s got %v", delay2)
	}
	if delay2 <= delay1 {
		t.Fatalf("escalation failed: delay2 %v <= delay1 %v", delay2, delay1)
	}

	// Third escalates to ~60s
	time.Sleep(5 * time.Millisecond)
	ch3 := s.noteTransportSuspect(TierZen, pool, proxy, AttemptClassTransportFailure, 0, time.Now().UnixNano())
	if ch3.Failures != 3 {
		t.Fatalf("third failures %d want 3", ch3.Failures)
	}
	delay3 := time.Duration(ch3.CooldownUntil - time.Now().UnixNano())
	if delay3 < 48*time.Second || delay3 > 72*time.Second {
		t.Fatalf("third delay want ~60s got %v", delay3)
	}

	// Fourth escalates to ~120s
	time.Sleep(5 * time.Millisecond)
	ch4 := s.noteTransportSuspect(TierZen, pool, proxy, AttemptClassTransportFailure, 0, time.Now().UnixNano())
	delay4 := time.Duration(ch4.CooldownUntil - time.Now().UnixNano())
	if delay4 < 96*time.Second || delay4 > 144*time.Second {
		t.Fatalf("fourth delay want ~120s got %v", delay4)
	}

	// Cap test with larger base: 100 base should cap at 300
	cfg2 := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg2.Performance.TransportSuspectCooldownSeconds = 100
	norm2, _ := NormalizeConfig("config.json", cfg2)
	gw2, _ := NewGateway(norm2, discardGatewayLogger(), NewMonitor())
	s2 := gw2.scheduler
	// Use same identity but new scheduler
	_ = identity
	// Fail 4 times to reach cap
	for i := 0; i < 4; i++ {
		s2.noteTransportSuspect(TierZen, "shared", "direct", AttemptClassTransportFailure, 0, time.Now().UnixNano())
		time.Sleep(2 * time.Millisecond)
	}
	// After 4 failures, delay should be capped at 300s with jitter, not exceed 300
	// Next failure should stay at cap
	chCap := s2.noteTransportSuspect(TierZen, "shared", "direct", AttemptClassTransportFailure, 0, time.Now().UnixNano())
	delayCap := time.Duration(chCap.CooldownUntil - time.Now().UnixNano())
	if delayCap > 300*time.Second {
		t.Fatalf("cap delay must not exceed 300s got %v", delayCap)
	}
	if delayCap < 240*time.Second {
		t.Fatalf("cap delay should be near 300s got %v", delayCap)
	}

	// Success clears
	success := s.noteTransportSuspectSuccess(TierZen, pool, proxy, time.Now().UnixNano())
	if !success.Cleared {
		t.Fatalf("success must clear")
	}
	if _, _, ok := s.suspectCooldownStatus(TierZen, pool, proxy); ok {
		t.Fatalf("still cooling after clear")
	}

	// Stale fencing: old started must not clear newer
	s.noteTransportSuspect(TierZen, pool, proxy, AttemptClassTransportFailure, 0, time.Now().UnixNano())
	// record new failure time
	s.mu.Lock()
	latestStarted := s.suspectState[identity].lastStartedNanos
	s.mu.Unlock()
	stale := s.noteTransportSuspectSuccess(TierZen, pool, proxy, latestStarted-10*time.Second.Nanoseconds())
	if stale.Cleared {
		t.Fatalf("stale success must not clear")
	}
	if _, _, ok := s.suspectCooldownStatus(TierZen, pool, proxy); !ok {
		t.Fatalf("should still be cooling after stale clear")
	}
	// Fresh clears
	fresh := s.noteTransportSuspectSuccess(TierZen, pool, proxy, time.Now().UnixNano())
	if !fresh.Cleared {
		t.Fatalf("fresh must clear")
	}
}

func TestBoundedSuspectExpiryRetentionEscalation(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	norm, _ := NormalizeConfig("config.json", cfg)
	gw, _ := NewGateway(norm, discardGatewayLogger(), NewMonitor())
	s := gw.scheduler
	pool := "shared"
	proxy := "direct"
	identity := suspectIdentity(TierZen, pool, proxy)

	s.noteTransportSuspect(TierZen, pool, proxy, AttemptClassTransportFailure, 0, time.Now().UnixNano())
	// Expire cooldown but keep within stale retention (lastFailureAt recent)
	s.mu.Lock()
	if e := s.suspectState[identity]; e != nil {
		e.cooldownUntil = time.Now().Add(-time.Second).UnixNano()
		// lastFailureAt stays recent (now)
	}
	s.mu.Unlock()
	// Next failure should escalate (failures 2) not reset to 1, because stale retention not elapsed
	ch2 := s.noteTransportSuspect(TierZen, pool, proxy, AttemptClassTransportFailure, 0, time.Now().UnixNano())
	if ch2.Failures != 2 {
		t.Fatalf("expiry without stale prune must escalate to 2 got %d", ch2.Failures)
	}
	// Now expire via stale retention (lastFailureAt 10m ago) and prune
	s.mu.Lock()
	if e := s.suspectState[identity]; e != nil {
		e.cooldownUntil = time.Now().Add(-time.Second).UnixNano()
		e.lastFailureAt = time.Now().Add(-10 * time.Minute).UnixNano()
	}
	s.mu.Unlock()
	// snapshot prune should delete
	if _, total := s.snapshotSuspect(); total != 0 {
		t.Fatalf("stale expired must prune")
	}
	// Next failure after prune starts at 1
	ch3 := s.noteTransportSuspect(TierZen, pool, proxy, AttemptClassTransportFailure, 0, time.Now().UnixNano())
	if ch3.Failures != 1 {
		t.Fatalf("after prune new failures should be 1 got %d", ch3.Failures)
	}
}

func TestBoundedCandidateFilteringDuringCooldown(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	norm, _ := NormalizeConfig("config.json", cfg)
	gw, _ := NewGateway(norm, discardGatewayLogger(), NewMonitor())
	pool := gw.pools["shared"]
	// Put first proxy into suspect
	gw.scheduler.noteTransportSuspect(TierZen, "shared", pool.items[0].name, AttemptClassTransportFailure, 0, time.Now().UnixNano())
	now := time.Now().UnixNano()
	cands := gw.scheduler.buildAuthCandidates(TierZen, gw.authCreds, pool, "m", now)
	for _, c := range cands {
		if c.ProxyRaw == pool.items[0].name {
			t.Fatalf("suspect proxy must be filtered")
		}
	}
	if len(cands) == 0 {
		t.Fatalf("other proxy should remain")
	}
	// Also test that cooldown status reports correctly
	if _, _, ok := gw.scheduler.suspectCooldownStatus(TierZen, "shared", pool.items[0].name); !ok {
		t.Fatalf("suspect status missing")
	}
}

func TestBoundedGatewayFallbackBeforeRouteDeadline(t *testing.T) {
	// Legacy config with timeout 60 missing attempt => 5, so first transport failure can fallback before route deadline
	cfg := testGatewayConfig(map[string][]string{"a": {"direct", "http://127.0.0.1:8081"}, "z": {"direct"}}, ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"})
	cfg.Anonymous = true
	cfg.Retry.TimeoutSeconds = 60
	cfg.Retry.AttemptTimeoutSeconds = 0
	cfg.retryAttemptTimeoutPresent = false
	cfg.Retry.MaxAttempts = 5
	cfg.Retry.TransientMaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if norm.Retry.AttemptTimeoutSeconds != 5 {
		t.Fatalf("legacy 60 must normalize to 5 got %d", norm.Retry.AttemptTimeoutSeconds)
	}
	gw, _ := NewGateway(norm, discardGatewayLogger(), NewMonitor())
	if gw.attemptTimeout() != 5*time.Second {
		t.Fatalf("attemptTimeout want 5s got %v", gw.attemptTimeout())
	}
	// Transport pool should have 5s header timeout
	tr := gw.pools["a"].items[0].client.Transport.(*http.Transport)
	if tr.ResponseHeaderTimeout != 5*time.Second {
		t.Fatalf("ResponseHeaderTimeout want 5s got %v", tr.ResponseHeaderTimeout)
	}
	// Stub anonymous pool: first proxy transport error, second succeeds
	var a0, a1 atomic.Int32
	postStub(t, gw, "a", 0, &a0, nil, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial timeout")
	})
	postStub(t, gw, "a", 1, &a1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	// Use route timeout context 60s, first attempt uses 5s, so fallback should happen quickly without wall clock
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, requestMetaKey{}, &requestMeta{})
	ids := emptySessionIDs()
	route := anonAuthRoute()
	resp, _, _, err := gw.doUpstreamTiers(ctx, route, routeBodies(), ids, 0)
	if err != nil {
		t.Fatalf("doUpstream err %v", err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("want 200 after fallback got %v", resp)
	}
	drainAndClose(resp.Body)
	if postCount(&a0) != 1 || postCount(&a1) != 1 {
		t.Fatalf("a0 %d a1 %d want 1/1", postCount(&a0), postCount(&a1))
	}
	// Ensure suspect was written for the transport failure but second proxy still succeeded
	if _, _, ok := gw.scheduler.suspectCooldownStatus(TierZen, "a", "direct"); !ok {
		// direct may not be the first proxy depending on affinity; check both
		if _, _, ok2 := gw.scheduler.suspectCooldownStatus(TierZen, "a", "http://127.0.0.1:8081"); !ok2 {
			t.Fatalf("suspect not written after transport failure")
		}
	}
}
