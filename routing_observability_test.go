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
		{"transport", 0, true, AttemptClassTransportFailure, true, true, false},
		{"transport_with_status", 500, true, AttemptClassTransportFailure, true, true, false},
		{"auth_401", 401, false, AttemptClassAuthFailure, true, true, false},
		{"auth_403", 403, false, AttemptClassAuthFailure, true, true, false},
		{"rate_limited", 429, false, AttemptClassRateLimited, true, true, false},
		{"upstream_500", 500, false, AttemptClassUpstreamFailure, true, true, false},
		{"upstream_503", 503, false, AttemptClassUpstreamFailure, true, true, false},
		{"client_400", 400, false, AttemptClassClientRejected, false, false, true},
		{"client_404", 404, false, AttemptClassClientRejected, false, false, true},
		{"client_422", 422, false, AttemptClassClientRejected, false, false, true},
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

func TestAnonymousCooldownSnapshot(t *testing.T) {
	transports, err := newTransportPool("shared", []string{"direct", "direct"}, PerformanceConfig{
		MaxIdleConns: 1, MaxIdleConnsPerHost: 1, ConnectTimeoutSeconds: 1, IdleConnTimeoutSeconds: 1,
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pool := newAnonymousPool(true, transports, 15*time.Second)
	if pool.Len() != 2 {
		t.Fatalf("pool len=%d", pool.Len())
	}
	node := pool.nodes[0]
	pool.MarkFailure(node, responseWithStatus(400), nil)
	if got := node.cooldownUntil.Load(); got != 0 {
		t.Fatalf("client_rejected must not cool down, got %d", got)
	}
	pool.MarkFailure(node, responseWithStatus(429), nil)
	if got := node.cooldownUntil.Load(); got <= time.Now().UnixNano() {
		t.Fatalf("rate_limited must cool down")
	}
	pool.MarkSuccess(node)
	if got := node.cooldownUntil.Load(); got != 0 {
		t.Fatalf("success must clear cooldown, got %d", got)
	}
	pool.MarkFailure(node, nil, errors.New("dial timeout"))
	if got := node.cooldownUntil.Load(); got <= time.Now().UnixNano() {
		t.Fatalf("transport failure must cool down")
	}
	statuses := anonymousProxyStatuses(pool)
	if len(statuses) != 2 {
		t.Fatalf("statuses=%d", len(statuses))
	}
	found := false
	for _, st := range statuses {
		if st.Failures > 0 {
			found = true
			if st.CooldownUntil == nil || st.CooldownRemainingSeconds == nil {
				t.Fatalf("cooling proxy must expose cooldown: %+v", st)
			}
		} else if st.CooldownUntil != nil {
			t.Fatalf("idle proxy must not expose cooldown: %+v", st)
		}
		if st.Address == "" || strings.Contains(st.Address, "://") && strings.Contains(st.Address, "@") && strings.Contains(st.Address, "***") == false {
			t.Fatalf("proxy address not redacted: %q", st.Address)
		}
	}
	if !found {
		t.Fatalf("expected one cooling anonymous proxy")
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

func TestKeyStatusProxyAndCooldown(t *testing.T) {
	transports, err := newTransportPool("shared", []string{"direct", "direct"}, PerformanceConfig{
		MaxIdleConns: 1, MaxIdleConnsPerHost: 1, ConnectTimeoutSeconds: 1, IdleConnTimeoutSeconds: 1,
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := newNodePool([]string{"sk-live-1234567890"}, transports, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	statuses := keyStatuses("zen", pool)
	if len(statuses) != 1 {
		t.Fatalf("statuses=%d", len(statuses))
	}
	if statuses[0].ID != "67890" {
		t.Fatalf("key id=%q", statuses[0].ID)
	}
	if statuses[0].Proxy == "" {
		t.Fatalf("key proxy missing: %+v", statuses[0])
	}
	if statuses[0].CooldownUntil != nil || statuses[0].CooldownRemainingSeconds != nil {
		t.Fatalf("fresh key must not expose cooldown: %+v", statuses[0])
	}
	pool.MarkFailure(pool.nodes[0], responseWithStatus(500), nil)
	statuses = keyStatuses("zen", pool)
	if statuses[0].CooldownUntil == nil || statuses[0].CooldownRemainingSeconds == nil {
		t.Fatalf("cooling key must expose cooldown: %+v", statuses[0])
	}
	if *statuses[0].CooldownRemainingSeconds <= 0 {
		t.Fatalf("cooldown remaining=%d", *statuses[0].CooldownRemainingSeconds)
	}
}
