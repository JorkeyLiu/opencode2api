package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"
)

type refreshStateSnapshot struct {
	healthy  map[string]bool
	checking map[string]bool
	cred     map[string]int64
	targets  map[string]int64
	total    int
}

func snapshotRefreshState(gateway *Gateway) refreshStateSnapshot {
	snap := refreshStateSnapshot{
		healthy: map[string]bool{}, checking: map[string]bool{},
		cred: map[string]int64{}, targets: map[string]int64{},
	}
	for name, pool := range gateway.pools {
		for _, proxy := range pool.items {
			key := name + "\x00" + proxy.name
			snap.healthy[key] = proxy.healthy.Load()
			snap.checking[key] = proxy.checking.Load()
		}
	}
	for _, cred := range gateway.zenCreds {
		snap.cred[cred.id] = gateway.scheduler.credentialCoolUntil(cred.id)
	}
	for _, cred := range gateway.goCreds {
		snap.cred[cred.id] = gateway.scheduler.credentialCoolUntil(cred.id)
	}
	entries, total := gateway.scheduler.snapshotTargets()
	snap.total = total
	for _, entry := range entries {
		// Re-resolve identity via display is lossy; instead capture via direct map read.
		_ = entry
	}
	// Capture raw target cooldowns without pruning side effects beyond snapshot.
	gateway.scheduler.mu.Lock()
	for identity, entry := range gateway.scheduler.targetState {
		if entry != nil {
			snap.targets[identity] = entry.cooldownUntil
		}
	}
	gateway.scheduler.mu.Unlock()
	return snap
}

func assertRefreshStateUnchanged(t *testing.T, before, after refreshStateSnapshot) {
	t.Helper()
	for key, want := range before.healthy {
		if after.healthy[key] != want {
			t.Fatalf("proxy healthy changed for %q: %v -> %v", key, want, after.healthy[key])
		}
	}
	for key, want := range before.checking {
		if after.checking[key] != want {
			t.Fatalf("proxy checking changed for %q: %v -> %v", key, want, after.checking[key])
		}
	}
	for id, want := range before.cred {
		if after.cred[id] != want {
			t.Fatalf("credential cooldown changed for %q: %d -> %d", id, want, after.cred[id])
		}
	}
	// No new credentials may appear.
	for id := range after.cred {
		if _, ok := before.cred[id]; !ok {
			t.Fatalf("refresh created credential state for %q", id)
		}
	}
	if len(before.targets) != len(after.targets) {
		t.Fatalf("target count changed: %d -> %d", len(before.targets), len(after.targets))
	}
	for identity, want := range before.targets {
		got, ok := after.targets[identity]
		if !ok || got != want {
			t.Fatalf("target cooldown changed for %q", identity)
		}
	}
	if before.total != after.total {
		t.Fatalf("target snapshot total changed: %d -> %d", before.total, after.total)
	}
}

func modelsServer(status int, body string, delay time.Duration) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		if body == "" && (status == 401 || status == 429 || status >= 500) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"upstream"}`))
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

func seedRefreshForeground(t *testing.T, gateway *Gateway) targetCandidate {
	t.Helper()
	pool := gateway.pools[gateway.cfg.ProxyRouting.Zen]
	if pool == nil {
		t.Fatalf("zen pool missing")
	}
	cred := gateway.zenCreds[0]
	proxy := pool.items[0]
	cand := authCand(TierZen, cred, pool, proxy, "some-model")
	gateway.applyAttemptOutcome(context.Background(), cand, responseWithStatus(429), nil)
	if got := gateway.scheduler.targetCoolUntil(cand.Identity); got <= time.Now().UnixNano() {
		t.Fatalf("seed target must cool")
	}
	return cand
}

func TestRefreshFailuresLeaveForegroundUntouched(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"unauthorized", 401, ""},
		{"rate_limited", 429, ""},
		{"server_error", 500, ""},
		{"bad_gateway", 502, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gateway := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
			seeded := seedRefreshForeground(t, gateway)
			before := snapshotRefreshState(gateway)
			server := modelsServer(tc.status, tc.body, 0)
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if got := gateway.refreshTier(ctx, server.URL, TierZen); got != nil {
				t.Fatalf("failing refresh must return nil")
			}
			if got := gateway.refreshAnonymousTier(ctx, server.URL); got != nil {
				t.Fatalf("failing anonymous refresh must return nil")
			}
			// Allow any stray async verification to settle; there must be none.
			time.Sleep(50 * time.Millisecond)
			after := snapshotRefreshState(gateway)
			assertRefreshStateUnchanged(t, before, after)
			if got := gateway.scheduler.targetCoolUntil(seeded.Identity); got <= time.Now().UnixNano() {
				t.Fatalf("seeded target cooldown lost")
			}
		})
	}
}

func TestRefreshTransportErrorLeavesForegroundUntouched(t *testing.T) {
	gateway := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	seedRefreshForeground(t, gateway)
	before := snapshotRefreshState(gateway)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if got := gateway.refreshTier(ctx, "http://127.0.0.1:9", TierZen); got != nil {
		t.Fatalf("transport-error refresh must return nil")
	}
	if got := gateway.refreshAnonymousTier(ctx, "http://127.0.0.1:9"); got != nil {
		t.Fatalf("transport-error anonymous refresh must return nil")
	}
	time.Sleep(50 * time.Millisecond)
	after := snapshotRefreshState(gateway)
	assertRefreshStateUnchanged(t, before, after)
}

func TestRefreshContextDeadlineIsOnlyRefreshFailure(t *testing.T) {
	gateway := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	seedRefreshForeground(t, gateway)
	before := snapshotRefreshState(gateway)
	server := modelsServer(200, `{"data":[{"id":"m1"}]}`, 300*time.Millisecond)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if got := gateway.refreshTier(ctx, server.URL, TierZen); got != nil {
		t.Fatalf("deadline refresh must return nil, got %v", got)
	}
	if got := gateway.refreshAnonymousTier(ctx, server.URL); got != nil {
		t.Fatalf("deadline anonymous refresh must return nil")
	}
	after := snapshotRefreshState(gateway)
	assertRefreshStateUnchanged(t, before, after)
	// A parent cancel is also only a refresh failure.
	cancelled, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if got := gateway.refreshTier(cancelled, server.URL, TierZen); got != nil {
		t.Fatalf("cancelled refresh must return nil")
	}
	final := snapshotRefreshState(gateway)
	assertRefreshStateUnchanged(t, before, final)
}

func TestRefreshContextCancelNeverMarksProxy(t *testing.T) {
	if isProxyFailure(context.Canceled) {
		t.Fatalf("context.Canceled must not be a proxy failure")
	}
	if isProxyFailure(wrapCanceled()) {
		t.Fatalf("wrapped context.Canceled must not be a proxy failure")
	}
	gateway := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	proxy := gateway.pools["shared"].items[0]
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if gateway.syncProxyResult(cancelled, proxy, 0, context.DeadlineExceeded) {
		t.Fatalf("cancelled ctx must not report proxy failure")
	}
	if !proxy.healthy.Load() {
		t.Fatalf("cancelled ctx must not change proxy health")
	}
	if proxy.checking.Load() {
		t.Fatalf("cancelled ctx must not leave checking set")
	}
}

func wrapCanceled() error {
	return &canceledWrapper{err: context.Canceled}
}

type canceledWrapper struct{ err error }

func (e *canceledWrapper) Error() string { return e.err.Error() }
func (e *canceledWrapper) Unwrap() error { return e.err }

func TestRefreshSuccessStillUpdatesCatalog(t *testing.T) {
	gateway := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	seedRefreshForeground(t, gateway)
	before := snapshotRefreshState(gateway)
	server := modelsServer(200, `{"data":[{"id":"model-ok-1"},{"id":"model-ok-2"}]}`, 0)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	models := gateway.refreshTier(ctx, server.URL, TierZen)
	if len(models) != 2 {
		t.Fatalf("success refresh must return models, got %v", models)
	}
	after := snapshotRefreshState(gateway)
	assertRefreshStateUnchanged(t, before, after)
	// Success only decides the catalog snapshot in the caller.
	gateway.catalog.ReplaceWithCapabilities(models, nil, nil, nil, nil)
	snap := gateway.catalog.Snapshot()
	if snap.Zen != 2 || snap.Total != 2 {
		t.Fatalf("catalog must update on success, got %+v", snap)
	}
	// Failure retains the previous snapshot (caller only replaces on non-nil).
	failing := gateway.refreshTier(ctx, "http://127.0.0.1:9", TierZen)
	if failing != nil {
		t.Fatalf("failing refresh must be nil")
	}
	if got := gateway.catalog.Snapshot(); got.Zen != 2 {
		t.Fatalf("failed refresh must retain catalog, got %+v", got)
	}
}

func TestRefreshDoesNotCreateCredentialState(t *testing.T) {
	gateway := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	server := modelsServer(500, "", 0)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = gateway.refreshTier(ctx, server.URL, TierZen)
	for _, cred := range gateway.zenCreds {
		if _, until := gateway.scheduler.credentialSnapshot(cred.id); until != 0 {
			t.Fatalf("refresh must not create credential state")
		}
	}
	// A single foreground transport error stays neutral on the real inference
	// path (no immediate health flip), proving the refresh no-op did not
	// disable the path; only the independent probe result may flip health.
	pool := gateway.pools["shared"]
	cand := authCand(TierZen, gateway.zenCreds[0], pool, pool.items[0], "m")
	gateway.applyAttemptOutcome(context.Background(), cand, nil, syscall.ECONNREFUSED)
	if !pool.items[0].healthy.Load() {
		t.Fatalf("single transport failure must not immediately mark proxy unhealthy")
	}
	if got := gateway.scheduler.targetCoolUntil(cand.Identity); got != 0 {
		t.Fatalf("single transport failure must not cool target")
	}
	gateway.applyProxyHealthResult(proxyHealthResult{proxy: pool.items[0], err: syscall.ECONNREFUSED, failed: true, wasHealthy: true}, "test probe", 0)
	if pool.items[0].healthy.Load() {
		t.Fatalf("failed probe must still mark proxy unhealthy")
	}
}
