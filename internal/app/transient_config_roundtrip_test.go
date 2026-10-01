package app

import (
	"encoding/json"
	"strings"
	"testing"
)

// Final L1 migration: retry.max_attempts is the unique L1 same-target
// observation limit; transient_max_attempts is removed with strict
// unknown-field rejection and no value migration.

func TestTransientLegacyMissingDefaults(t *testing.T) {
	raw := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["k2"],"proxy_pools":{"shared":{"proxies":["direct"]}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"upstream":{"zen":"https://opencode.ai/zen"},"retry":{"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":5},"models":{"refresh_seconds":300,"protocols":{}},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300,"transport_suspect_cooldown_seconds":15},"logging":{"level":"info","ring_size":2000},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var cfg Config
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("unmarshal legacy missing transient: %v", err)
	}
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatalf("normalize legacy missing transient: %v", err)
	}
	if norm.Retry.MaxAttempts != 3 {
		t.Fatalf("legacy missing max want 3 got %d", norm.Retry.MaxAttempts)
	}
	if norm.Retry.TransientRetryIntervalSeconds != 3 {
		t.Fatalf("legacy missing transient interval want 3 got %d", norm.Retry.TransientRetryIntervalSeconds)
	}
	if norm.Retry.AttemptTimeoutSeconds != 5 {
		t.Fatalf("legacy missing attempt want 5 got %d", norm.Retry.AttemptTimeoutSeconds)
	}
	if norm.Performance.TransportSuspectCooldownSeconds != 15 {
		t.Fatalf("legacy missing suspect want 15 got %d", norm.Performance.TransportSuspectCooldownSeconds)
	}
	data, _ := json.Marshal(norm)
	if strings.Contains(string(data), "transient_max_attempts") {
		t.Fatalf("normalized must omit transient_max_attempts, got %s", string(data))
	}
	// Missing retry object entirely should default interval to 3 and max to 3 etc (via defaults + presence).
	rawNoRetry := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["k2"],"proxy_pools":{"shared":{"proxies":["direct"]}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"upstream":{"zen":"https://opencode.ai/zen"},"models":{"refresh_seconds":300,"protocols":{}},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300,"transport_suspect_cooldown_seconds":15},"logging":{"level":"info","ring_size":2000},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	var cfg2 Config
	if err := json.Unmarshal([]byte(rawNoRetry), &cfg2); err != nil {
		t.Fatalf("unmarshal no retry: %v", err)
	}
	norm2, err := NormalizeConfig("config.json", cfg2)
	if err != nil {
		t.Fatalf("normalize no retry: %v", err)
	}
	if norm2.Retry.TransientRetryIntervalSeconds != 3 {
		t.Fatalf("no retry interval default 3 got %d", norm2.Retry.TransientRetryIntervalSeconds)
	}
	if norm2.Retry.MaxAttempts != 3 {
		t.Fatalf("no retry max default 3 got %d", norm2.Retry.MaxAttempts)
	}
	if strings.Contains(string(func() string { d, _ := json.Marshal(norm2); return string(d) }()), "transient_max_attempts") {
		t.Fatalf("no retry Marshal must still omit transient_max")
	}
}

func TestTransientMaxAttemptsStrictlyRejected(t *testing.T) {
	baseRaw := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["k2"],"proxy_pools":{"shared":{"proxies":["direct"]}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"upstream":{"zen":"https://opencode.ai/zen"},"models":{"refresh_seconds":300,"protocols":{}},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300,"transport_suspect_cooldown_seconds":15},"logging":{"level":"info","ring_size":2000},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	raw := strings.Replace(baseRaw, `"models"`, `"retry":{"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":5,"transient_max_attempts":3,"transient_retry_interval_seconds":3},"models"`, 1)
	var cfg Config
	if err := json.Unmarshal([]byte(raw), &cfg); err == nil {
		t.Fatalf("transient_max_attempts must be strictly rejected, got no error")
	} else if !strings.Contains(err.Error(), "transient_max_attempts") {
		t.Fatalf("rejection must name transient_max_attempts, got %v", err)
	}
}

func TestTransientExplicitValidAndInvalid(t *testing.T) {
	baseRaw := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"keys":["k2"],"proxy_pools":{"shared":{"proxies":["direct"]}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"upstream":{"zen":"https://opencode.ai/zen"},"models":{"refresh_seconds":300,"protocols":{}},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300,"transport_suspect_cooldown_seconds":15},"logging":{"level":"info","ring_size":2000},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128}}`
	cases := []struct {
		name       string
		retryJSON  string
		shouldFail bool
	}{
		{"max 1 valid", `"max_attempts":1,"timeout_seconds":300,"attempt_timeout_seconds":5,"transient_retry_interval_seconds":3`, false},
		{"max 10 valid", `"max_attempts":10,"timeout_seconds":300,"attempt_timeout_seconds":5,"transient_retry_interval_seconds":3`, false},
		{"max 0 invalid", `"max_attempts":0,"timeout_seconds":300,"attempt_timeout_seconds":5,"transient_retry_interval_seconds":3`, true},
		{"interval 0 valid", `"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":5,"transient_retry_interval_seconds":0`, false},
		{"interval 30 valid", `"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":5,"transient_retry_interval_seconds":30`, false},
		{"interval 31 valid (no business max)", `"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":5,"transient_retry_interval_seconds":31`, false},
		{"interval 120 valid (no business max)", `"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":5,"transient_retry_interval_seconds":120`, false},
		{"interval -1 invalid", `"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":5,"transient_retry_interval_seconds":-1`, true},
		{"interval huge invalid (unrepresentable)", `"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":5,"transient_retry_interval_seconds":1099511627776`, true},
	}
	for _, c := range cases {
		raw := strings.Replace(baseRaw, `"models"`, `"retry":{`+c.retryJSON+`},"models"`, 1)
		var cfg Config
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			t.Fatalf("%s unmarshal failed: %v", c.name, err)
		}
		_, err := NormalizeConfig("config.json", cfg)
		if c.shouldFail && err == nil {
			t.Fatalf("%s must fail", c.name)
		}
		if !c.shouldFail && err != nil {
			t.Fatalf("%s must pass, got %v", c.name, err)
		}
	}
	rawZero := strings.Replace(baseRaw, `"models"`, `"retry":{"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":5,"transient_retry_interval_seconds":0},"models"`, 1)
	var cfgZero Config
	if err := json.Unmarshal([]byte(rawZero), &cfgZero); err != nil {
		t.Fatalf("unmarshal zero interval: %v", err)
	}
	normZero, err := NormalizeConfig("config.json", cfgZero)
	if err != nil {
		t.Fatalf("normalize zero interval: %v", err)
	}
	if normZero.Retry.TransientRetryIntervalSeconds != 0 {
		t.Fatalf("explicit interval 0 must stay 0 got %d", normZero.Retry.TransientRetryIntervalSeconds)
	}
}

func TestAdminPUTPreservesTransientWhenOmitted(t *testing.T) {
	cur := testBaseConfig()
	cur.ServerKeys = []string{"local-key"}
	cur.Keys = []string{"k1-12345"}
	cur.ProxyPools = map[string]ProxyPoolConfig{"shared": {Proxies: []string{"direct"}}}
	cur.ProxyRouting = ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"}
	cur.proxyPoolsPresent = true
	cur.proxyRoutingPresent = true
	cur.Retry.MaxAttempts = 7
	cur.retryMaxAttemptsPresent = true
	cur.Retry.TimeoutSeconds = 300
	cur.Retry.AttemptTimeoutSeconds = 2
	cur.retryAttemptTimeoutPresent = true
	cur.Retry.TransientRetryIntervalSeconds = 0
	cur.retryTransientIntervalPresent = true
	cur.Performance.TransportSuspectCooldownSeconds = 25
	cur.performanceSuspectPresent = true
	normCur, err := NormalizeConfig("config.json", cur)
	if err != nil {
		t.Fatalf("normalize cur: %v", err)
	}
	payload := `{"listen":"127.0.0.1:8080","server_keys":[{"id":"` + secretFingerprint("local-key") + `"}],"keys":[{"id":"` + secretFingerprint("k1-12345") + `"}],"anonymous":false,"proxy_pools":{"shared":{"proxies":[],"proxyfile":""}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"fallback":{"active":"","channels":[]},"upstream":{"zen":"https://opencode.ai/zen"},"retry":{"max_attempts":3,"timeout_seconds":300},"models":{"refresh_seconds":300,"protocols":{}},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300},"logging":{"level":"info","ring_size":2000},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5}}`
	var update ConfigUpdate
	if err := json.Unmarshal([]byte(payload), &update); err != nil {
		t.Fatalf("unmarshal update: %v", err)
	}
	if update.retryAttemptTimeoutPresent || update.retryTransientIntervalPresent || update.performanceSuspectPresent {
		t.Fatalf("omitted fields must be absent, got %+v", update)
	}
	candidate := Config{
		Listen: update.Listen, ServerKeys: []string{"local-key"}, Keys: []string{"k1-12345"}, Anonymous: update.Anonymous,
		ProxyPools: map[string]ProxyPoolConfig{"shared": {Proxies: []string{"direct"}}}, ProxyRouting: update.ProxyRouting, Fallback: FallbackConfig{},
		Upstream: update.Upstream, Retry: update.Retry, Models: update.Models, Performance: update.Performance, Logging: update.Logging,
		History: update.History, WebUI: WebUIConfig{Enabled: update.WebUI.Enabled, Listen: update.WebUI.Listen, Username: "u", SessionTTLMinutes: update.WebUI.SessionTTLMinutes},
	}
	candidate.proxyPoolsPresent = true
	candidate.proxyRoutingPresent = true
	candidate.retryMaxAttemptsPresent = update.retryMaxAttemptsPresent
	if !update.retryMaxAttemptsPresent {
		candidate.Retry.MaxAttempts = normCur.Retry.MaxAttempts
		candidate.retryMaxAttemptsPresent = true
	}
	candidate.retryAttemptTimeoutPresent = update.retryAttemptTimeoutPresent
	if !update.retryAttemptTimeoutPresent {
		candidate.Retry.AttemptTimeoutSeconds = normCur.Retry.AttemptTimeoutSeconds
		candidate.retryAttemptTimeoutPresent = true
	}
	candidate.retryTransientIntervalPresent = update.retryTransientIntervalPresent
	if !update.retryTransientIntervalPresent {
		candidate.Retry.TransientRetryIntervalSeconds = normCur.Retry.TransientRetryIntervalSeconds
		candidate.retryTransientIntervalPresent = true
	}
	candidate.performanceSuspectPresent = update.performanceSuspectPresent
	if !update.performanceSuspectPresent {
		candidate.Performance.TransportSuspectCooldownSeconds = normCur.Performance.TransportSuspectCooldownSeconds
		candidate.performanceSuspectPresent = true
	}
	norm, err := NormalizeConfig("config.json", candidate)
	if err != nil {
		t.Fatalf("normalize preserved: %v", err)
	}
	if norm.Retry.MaxAttempts != 3 {
		t.Fatalf("preserved max want 3 (payload) got %d", norm.Retry.MaxAttempts)
	}
	if norm.Retry.TransientRetryIntervalSeconds != 0 {
		t.Fatalf("preserved interval want 0 got %d", norm.Retry.TransientRetryIntervalSeconds)
	}
	if norm.Retry.AttemptTimeoutSeconds != 2 {
		t.Fatalf("preserved attempt want 2 got %d", norm.Retry.AttemptTimeoutSeconds)
	}
	if norm.Performance.TransportSuspectCooldownSeconds != 25 {
		t.Fatalf("preserved suspect want 25 got %d", norm.Performance.TransportSuspectCooldownSeconds)
	}
	data, _ := json.Marshal(norm)
	if strings.Contains(string(data), "transient_max_attempts") {
		t.Fatalf("normalized save must omit transient_max_attempts, got %s", string(data))
	}
}

func TestAdminPUTRejectsExplicitZero(t *testing.T) {
	testExplicit := func(payload string, shouldFail bool, desc string) {
		var update ConfigUpdate
		if err := json.Unmarshal([]byte(payload), &update); err != nil {
			// Strict rejection at admin decode counts as failure for invalid payloads.
			if !shouldFail {
				t.Fatalf("%s unmarshal: %v", desc, err)
			}
			return
		}
		cur := testBaseConfig()
		cur.Retry.TransientRetryIntervalSeconds = 3
		cur.retryTransientIntervalPresent = true
		cur.Retry.AttemptTimeoutSeconds = 5
		cur.retryAttemptTimeoutPresent = true
		cur.Performance.TransportSuspectCooldownSeconds = 15
		cur.performanceSuspectPresent = true
		normCur, _ := NormalizeConfig("config.json", cur)
		candidate := Config{
			Listen: update.Listen, ServerKeys: []string{"local-key"}, Keys: []string{"k1-12345"}, Anonymous: update.Anonymous,
			ProxyPools: map[string]ProxyPoolConfig{"shared": {Proxies: []string{"direct"}}}, ProxyRouting: ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
			Upstream: update.Upstream, Retry: update.Retry, Models: update.Models, Performance: update.Performance, Logging: update.Logging,
			History: update.History, WebUI: WebUIConfig{Enabled: false, Listen: "127.0.0.1:1", Username: "u", SessionTTLMinutes: 5},
		}
		candidate.proxyPoolsPresent = true
		candidate.proxyRoutingPresent = true
		candidate.retryMaxAttemptsPresent = update.retryMaxAttemptsPresent
		if !update.retryMaxAttemptsPresent {
			candidate.Retry.MaxAttempts = normCur.Retry.MaxAttempts
			candidate.retryMaxAttemptsPresent = true
		}
		candidate.retryAttemptTimeoutPresent = update.retryAttemptTimeoutPresent
		if !update.retryAttemptTimeoutPresent {
			candidate.Retry.AttemptTimeoutSeconds = normCur.Retry.AttemptTimeoutSeconds
			candidate.retryAttemptTimeoutPresent = true
		}
		candidate.retryTransientIntervalPresent = update.retryTransientIntervalPresent
		if !update.retryTransientIntervalPresent {
			candidate.Retry.TransientRetryIntervalSeconds = normCur.Retry.TransientRetryIntervalSeconds
			candidate.retryTransientIntervalPresent = true
		}
		candidate.performanceSuspectPresent = update.performanceSuspectPresent
		if !update.performanceSuspectPresent {
			candidate.Performance.TransportSuspectCooldownSeconds = normCur.Performance.TransportSuspectCooldownSeconds
			candidate.performanceSuspectPresent = true
		}
		_, err := NormalizeConfig("config.json", candidate)
		if shouldFail && err == nil {
			t.Fatalf("%s must fail", desc)
		}
		if !shouldFail && err != nil {
			t.Fatalf("%s must pass, got %v", desc, err)
		}
	}
	base := `{"listen":"127.0.0.1:8080","server_keys":[{"id":"` + secretFingerprint("local-key") + `"}],"keys":[{"id":"` + secretFingerprint("k1-12345") + `"}],"anonymous":false,"proxy_pools":{"shared":{"proxies":[],"proxyfile":""}},"proxy_routing":{"anonymous":"shared","authenticated":"shared"},"fallback":{"active":"","channels":[]},"upstream":{"zen":"https://opencode.ai/zen"},"models":{"refresh_seconds":300,"protocols":{}},"logging":{"level":"info","ring_size":2000},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5}}`
	testExplicit(strings.Replace(base, `"models"`, `"retry":{"max_attempts":0,"timeout_seconds":300,"attempt_timeout_seconds":5,"transient_retry_interval_seconds":3},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300,"transport_suspect_cooldown_seconds":15},"models"`, 1), true, "max 0")
	testExplicit(strings.Replace(base, `"models"`, `"retry":{"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":0,"transient_retry_interval_seconds":3},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300,"transport_suspect_cooldown_seconds":15},"models"`, 1), true, "attempt 0")
	testExplicit(strings.Replace(base, `"models"`, `"retry":{"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":5,"transient_retry_interval_seconds":3},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300,"transport_suspect_cooldown_seconds":0},"models"`, 1), true, "suspect 0")
	testExplicit(strings.Replace(base, `"models"`, `"retry":{"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":5,"transient_retry_interval_seconds":0},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300,"transport_suspect_cooldown_seconds":15},"models"`, 1), false, "interval 0")
	// Removed transient_max_attempts is strictly rejected at admin decode.
	testExplicit(strings.Replace(base, `"models"`, `"retry":{"max_attempts":3,"timeout_seconds":300,"attempt_timeout_seconds":5,"transient_max_attempts":3,"transient_retry_interval_seconds":3},"performance":{"max_idle_conns":2048,"max_idle_conns_per_host":256,"max_conns_per_host":0,"idle_conn_timeout_seconds":120,"connect_timeout_seconds":5,"failure_cooldown_seconds":15,"rate_limit_cooldown_seconds":300,"transport_suspect_cooldown_seconds":15},"models"`, 1), true, "transient max removed")
}

func TestWebUITransientFieldsPresent(t *testing.T) {
	html := readConfigWebUI(t)
	for _, needle := range []string{
		`id="c-transient-interval"`,
		`观测重试间隔`,
		`transient_retry_interval_seconds`,
		`c-transient-interval`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("webui missing transient interval contract %q", needle)
		}
	}
	if strings.Contains(html, `id="c-transient-attempts"`) || strings.Contains(html, `transient_max_attempts`) {
		t.Fatalf("webui must omit legacy transient_max_attempts")
	}
	if !strings.Contains(html, `$("c-transient-interval").value=`) {
		t.Fatalf("fillConfig must set transient interval")
	}
	if !strings.Contains(html, `transient_retry_interval_seconds:num("c-transient-interval")`) {
		t.Fatalf("collect must include transient_retry_interval_seconds")
	}
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
		if strings.Contains(html, sink) {
			t.Fatalf("forbidden DOM sink %q", sink)
		}
	}
}
