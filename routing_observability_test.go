package main

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func responseWithStatus(status int) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header)}
}

func TestClassifyAttemptMatrix(t *testing.T) {
	cases := []struct {
		name         string
		status       int
		transportErr bool
		class        string
		retryable    bool
		coolsDown    bool
		nonRetryable bool
	}{
		{"success_200", 200, false, AttemptClassSuccess, false, false, false},
		{"success_201", 201, false, AttemptClassSuccess, false, false, false},
		{"transport", 0, true, AttemptClassTransportFailure, true, false, false},
		{"transport_with_status", 500, true, AttemptClassTransportFailure, true, false, false},
		{"auth_401", 401, false, AttemptClassAuthFailure, true, true, false},
		{"auth_403", 403, false, AttemptClassAuthFailure, true, true, false},
		{"rate_limited", 429, false, AttemptClassRateLimited, true, true, false},
		{"upstream_500", 500, false, AttemptClassUpstreamFailure, true, true, false},
		{"upstream_503", 503, false, AttemptClassUpstreamFailure, true, true, false},
		{"client_400", 400, false, AttemptClassClientRejected, false, false, true},
		{"client_404", 404, false, AttemptClassClientRejected, false, false, true},
		{"client_422", 422, false, AttemptClassClientRejected, false, false, true},
		{"transient_408", 408, false, AttemptClassTransientClient, true, false, false},
		{"transient_425", 425, false, AttemptClassTransientClient, true, false, false},
		{"other_zero", 0, false, AttemptClassOtherResponse, true, false, false},
		{"other_redirect", 302, false, AttemptClassOtherResponse, true, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyAttempt(tc.status, tc.transportErr)
			if got.Class != tc.class || got.Retryable != tc.retryable || got.CoolsDown != tc.coolsDown {
				t.Fatalf("classifyAttempt(%d,%v)=%+v want class=%s retryable=%v coolsDown=%v",
					tc.status, tc.transportErr, got, tc.class, tc.retryable, tc.coolsDown)
			}
			var resp *http.Response
			if tc.status != 0 || !tc.transportErr {
				// classifyUpstreamAttempt must agree with the pure function.
				resp = responseWithStatus(tc.status)
				if tc.name == "other_zero" {
					resp = nil
				}
			}
			var err error
			if tc.transportErr {
				err = errors.New("dial timeout")
			}
			shared := classifyUpstreamAttempt(resp, err)
			if shared != got {
				t.Fatalf("shared classifier=%+v pure=%+v", shared, got)
			}
			if isNonRetryableClientResponse(resp, err) != tc.nonRetryable {
				t.Fatalf("isNonRetryable=%v want %v", !tc.nonRetryable, tc.nonRetryable)
			}
		})
	}
}

func TestClassifyIgnoresErrorText(t *testing.T) {
	secret := "sk-live-super-secret-value"
	got := classifyUpstreamAttempt(nil, errors.New("proxy "+secret+" refused"))
	if got.Class != AttemptClassTransportFailure {
		t.Fatalf("want transport_failure, got %s", got.Class)
	}
	for _, field := range []string{got.Class, outcomeFromClass(got.Class, false)} {
		if strings.Contains(field, secret) {
			t.Fatalf("secret leaked into classification output")
		}
	}
}

func TestMonitorResourceAggregation(t *testing.T) {
	m := NewMonitor()
	now := time.Now().UTC()
	record := func(proxy, cred, channel string, anonymous bool, status int, success bool, class string, ms int64) {
		retryable := class != AttemptClassSuccess && class != AttemptClassClientRejected
		m.RecordAttempt(UpstreamAttempt{
			Time: now, RequestID: "req-1", Model: "m", Tier: "zen",
			KeyID: cred, Channel: channel, Anonymous: anonymous, Proxy: proxy,
			Status: status, DurationMS: ms, Success: success, Outcome: "x",
			FailureClass: class, Retryable: retryable,
		})
	}
	// proxy-a: 2 success + 1 rate-limited via key channel; proxy-b: transport failure via anonymous.
	record("proxy-a", "AAAAA", "key", false, 200, true, AttemptClassSuccess, 100)
	record("proxy-a", "AAAAA", "key", false, 200, true, AttemptClassSuccess, 200)
	record("proxy-a", "AAAAA", "key", false, 429, false, AttemptClassRateLimited, 300)
	record("proxy-b", anonymousCredentialID, "anonymous", true, 0, false, AttemptClassTransportFailure, 400)

	snap := m.Snapshot()
	proxies := snap.AttemptResources.Proxies
	if proxies["proxy-a"].Attempts != 3 || proxies["proxy-a"].Success != 2 || proxies["proxy-a"].Failed != 1 {
		t.Fatalf("proxy-a counts=%+v", proxies["proxy-a"])
	}
	if proxies["proxy-a"].RateLimited != 1 {
		t.Fatalf("proxy-a rate_limited=%+v", proxies["proxy-a"])
	}
	if got := proxies["proxy-a"].AverageMS; got != 200 {
		t.Fatalf("proxy-a average=%v want 200", got)
	}
	if proxies["proxy-b"].TransportFailures != 1 {
		t.Fatalf("proxy-b transport=%+v", proxies["proxy-b"])
	}
	creds := snap.AttemptResources.Credentials
	if creds["key:AAAAA"].Attempts != 3 {
		t.Fatalf("credential counts=%+v", creds["key:AAAAA"])
	}
	if creds[anonymousCredentialID].Attempts != 1 {
		t.Fatalf("anonymous credential missing: %+v", creds)
	}
	pairs := snap.AttemptResources.Pairs
	if pairs["key:AAAAA @ proxy-a"].Attempts != 3 {
		t.Fatalf("pair counts missing: %+v", pairs)
	}
	if pairs[anonymousCredentialID+" @ proxy-b"].Attempts != 1 {
		t.Fatalf("anonymous pair missing: %+v", pairs)
	}
}

func TestMonitorResourceWindowExcludesOldAttempts(t *testing.T) {
	m := NewMonitor()
	old := time.Now().UTC().Add(-2 * time.Hour)
	m.RecordAttempt(UpstreamAttempt{
		Time: old, Tier: "zen", KeyID: "key:BBBBB", Channel: "key", Proxy: "proxy-old",
		Status: 500, DurationMS: 50, Success: false,
		FailureClass: AttemptClassUpstreamFailure, Retryable: true,
	})
	m.RecordAttempt(UpstreamAttempt{
		Tier: "zen", KeyID: "key:BBBBB", Channel: "key", Proxy: "proxy-new",
		Status: 200, DurationMS: 50, Success: true,
		FailureClass: AttemptClassSuccess,
	})
	snap := m.Snapshot()
	if _, ok := snap.AttemptResources.Proxies["proxy-old"]; ok {
		t.Fatalf("stale proxy leaked into last-hour window: %+v", snap.AttemptResources.Proxies)
	}
	if snap.AttemptResources.Proxies["proxy-new"].Attempts != 1 {
		t.Fatalf("fresh proxy missing: %+v", snap.AttemptResources.Proxies)
	}
}

func TestAnonymousTargetCooldownSnapshot(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8080"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Anonymous = true
	gateway, err := NewGateway(cfg, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	pool := gateway.pools["shared"]
	proxy := pool.items[0]
	cand := targetCandidate{
		Tier: TierZen, CredKey: anonymousZenKey, CredID: anonymousSchedulerCredentialID,
		CredDisplay: anonymousCredentialID, PoolName: "shared", Proxy: proxy,
		ProxyRaw: proxy.name, Model: "m", Identity: targetIdentity(TierZen, anonymousSchedulerCredentialID, "shared", proxy.name, "m"),
	}
	// client_rejected is neutral: no cooldown, no state.
	gateway.applyAttemptOutcome(t.Context(), cand, responseWithStatus(400), nil, time.Now().UnixNano())
	if got := gateway.scheduler.targetCoolUntil(cand.Identity); got != 0 {
		t.Fatalf("client_rejected must not cool down, got %d", got)
	}
	// 403 cools the per-target layer (used here for the anonymous summary);
	// 429 is proxy-global and asserted via snapshotProxy429 below.
	gateway.applyAttemptOutcome(t.Context(), cand, responseWithStatus(403), nil, time.Now().UnixNano())
	if got := gateway.scheduler.targetCoolUntil(cand.Identity); got <= time.Now().UnixNano() {
		t.Fatalf("403 must cool target down")
	}
	// Success clears only this target.
	started := time.Now().UnixNano()
	gateway.applyAttemptOutcome(t.Context(), cand, responseWithStatus(200), nil, started)
	if got := gateway.scheduler.targetCoolUntil(cand.Identity); got != 0 {
		t.Fatalf("success must clear cooldown, got %d", got)
	}
	// Single transport errors are neutral: no target cooldown, no proxy flip.
	// They only trigger the async neutral verification; the classification
	// stays retryable without claiming a cooldown.
	class := gateway.applyAttemptOutcome(t.Context(), cand, nil, errors.New("connection reset by peer"), time.Now().UnixNano())
	if class.Class != AttemptClassTransportFailure || !class.Retryable || class.CoolsDown {
		t.Fatalf("transport class=%+v want retryable=true coolsDown=false", class)
	}
	if got := gateway.scheduler.targetCoolUntil(cand.Identity); got != 0 {
		t.Fatalf("transport failure must not cool target, got %d", got)
	}
	if !proxy.healthy.Load() {
		t.Fatalf("single transport error must not mark proxy unhealthy")
	}
	// Re-cool with 403 so the per-target snapshot below still covers a cooling
	// target. The unified 代理可用性 Zen column stays available: per-model
	// target cooldowns never filter the proxy; only transport, proxy429, and
	// channel layers do. Anonymous Zen 403/5xx context is preserved inside
	// the Zen reason/channel detail, not a separate summary table.
	gateway.applyAttemptOutcome(t.Context(), cand, responseWithStatus(403), nil, time.Now().UnixNano())
	if got := gateway.scheduler.targetCoolUntil(cand.Identity); got <= time.Now().UnixNano() {
		t.Fatalf("403 must cool down")
	}
	active, nextAvailable, lastClass, _ := gateway.scheduler.proxyTargetSummary(pool.name, proxy.name)
	if active != 1 || nextAvailable == nil || lastClass == "" {
		t.Fatalf("anonymous per-proxy target aggregate missing: active=%d class=%q", active, lastClass)
	}
	// 429 uses the dedicated proxy429 snapshot, not a separate anonymous table.
	gateway.applyAttemptOutcome(t.Context(), cand, responseWithStatus(429), nil, time.Now().UnixNano())
	if _, _, ok := gateway.scheduler.proxy429CooldownStatus(TierZen, pool.name, proxy.name); !ok {
		t.Fatalf("rate_limited must cool proxy429")
	}
	limits, total := gateway.scheduler.snapshotProxy429()
	if total != 1 || len(limits) != 1 || !limits[0].Active {
		t.Fatalf("proxy429 snapshot missing active entry: total=%d %+v", total, limits)
	}
}

func TestObservabilityRedaction(t *testing.T) {
	raw := "socks5://user:hunter2@proxy.example:1080"
	if got := redactURL(raw); strings.Contains(got, "hunter2") || strings.Contains(got, "user@") {
		t.Fatalf("proxy URL not redacted: %q", got)
	}
	if got := keyDisplayID("sk-live-1234567890"); got != "67890" {
		t.Fatalf("key suffix=%q", got)
	}
	m := NewMonitor()
	m.RecordAttempt(UpstreamAttempt{
		Tier: "zen", KeyID: "should-be-overwritten", Channel: "anonymous", Anonymous: true,
		Proxy: raw, Status: 200, DurationMS: 5, Success: true, FailureClass: AttemptClassSuccess,
	})
	snap := m.Snapshot()
	if len(snap.Upstream.Recent) == 0 {
		t.Fatalf("recent attempt missing")
	}
	stored := snap.Upstream.Recent[len(snap.Upstream.Recent)-1]
	if stored.KeyID != anonymousCredentialID {
		t.Fatalf("anonymous key=%q", stored.KeyID)
	}
	if strings.Contains(stored.Proxy, "hunter2") {
		t.Fatalf("proxy secret leaked: %q", stored.Proxy)
	}
	if _, ok := snap.AttemptResources.Credentials[anonymousCredentialID]; !ok {
		t.Fatalf("anonymous credential key missing")
	}
	for key := range snap.AttemptResources.Proxies {
		if strings.Contains(key, "hunter2") {
			t.Fatalf("proxy secret leaked into aggregation: %q", key)
		}
	}
}

func TestKeyStatusCredentialAndCooldown(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Keys = []string{"sk-live-1234567890"}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	statuses := keyStatusesForTier(gateway, "zen", gateway.authCreds, "shared", nil)
	if len(statuses) != 1 {
		t.Fatalf("statuses=%d", len(statuses))
	}
	if statuses[0].ID != "67890" {
		t.Fatalf("key id=%q", statuses[0].ID)
	}
	// No static binding: proxy fields stay empty, pool names the assignment.
	if statuses[0].Proxy != "" || statuses[0].ProxyIndex != 0 {
		t.Fatalf("key must not expose a static proxy: %+v", statuses[0])
	}
	if statuses[0].ProxyPool != "shared" {
		t.Fatalf("key pool=%q", statuses[0].ProxyPool)
	}
	if statuses[0].CooldownUntil != nil || statuses[0].CooldownRemainingSeconds != nil {
		t.Fatalf("fresh key must not expose cooldown: %+v", statuses[0])
	}
	if statuses[0].TotalTargets != 1 || statuses[0].AvailableTargets != 1 {
		t.Fatalf("fresh key targets=%+v", statuses[0])
	}
	// Only 401 cools the credential; 500 cools the target, not the credential.
	proxy := gateway.pools["shared"].items[0]
	cand := targetCandidate{
		Tier: TierZen, CredKey: "sk-live-1234567890", CredID: gateway.authCreds[0].id,
		CredDisplay: "67890", PoolName: "shared", Proxy: proxy, ProxyRaw: proxy.name,
		Model: "m", Identity: targetIdentity(TierZen, gateway.authCreds[0].id, "shared", proxy.name, "m"),
	}
	gateway.applyAttemptOutcome(t.Context(), cand, responseWithStatus(500), nil, time.Now().UnixNano())
	statuses = keyStatusesForTier(gateway, "zen", gateway.authCreds, "shared", nil)
	if statuses[0].CooldownUntil != nil {
		t.Fatalf("5xx must not cool the credential: %+v", statuses[0])
	}
	gateway.applyAttemptOutcome(t.Context(), cand, responseWithStatus(401), nil, time.Now().UnixNano())
	statuses = keyStatusesForTier(gateway, "zen", gateway.authCreds, "shared", nil)
	if statuses[0].CooldownUntil == nil || statuses[0].CooldownRemainingSeconds == nil {
		t.Fatalf("401 must cool the credential: %+v", statuses[0])
	}
	if *statuses[0].CooldownRemainingSeconds <= 0 {
		t.Fatalf("cooldown remaining=%d", *statuses[0].CooldownRemainingSeconds)
	}
	if statuses[0].AvailableTargets != 0 {
		t.Fatalf("cooling credential must report zero available targets: %+v", statuses[0])
	}
}
