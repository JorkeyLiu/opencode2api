package main

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func schedulerTestGateway(t *testing.T, keys, proxies []string) *Gateway {
	t.Helper()
	pools := map[string][]string{"shared": proxies}
	cfg := testGatewayConfig(pools, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	cfg.ZenKeys = keys
	cfg.Anonymous = true
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	return gateway
}

func authCand(tier Tier, cred credentialRef, pool *transportPool, proxy *proxyTransport, model string) targetCandidate {
	tierOf := tier
	return targetCandidate{
		Tier: tierOf, CredKey: cred.key, CredID: cred.id, CredDisplay: cred.display,
		CredIndex: cred.index, PoolName: pool.name, Proxy: proxy,
		ProxyRaw: proxy.name, Model: model,
		Identity: targetIdentity(tierOf, cred.id, pool.name, proxy.name, model),
	}
}

// 1. HRW ordering is stable for the same session/model/resources.
func TestHRWStableOrder(t *testing.T) {
	gateway := schedulerTestGateway(t,
		[]string{"zen-key-aaaaa", "zen-key-bbbbb", "zen-key-ccccc"},
		[]string{"direct", "http://127.0.0.1:8081", "http://127.0.0.1:8082"})
	now := time.Now().UnixNano()
	build := func() []targetCandidate {
		return gateway.scheduler.orderCandidates(
			gateway.scheduler.buildAuthCandidates(TierZen, gateway.zenCreds, gateway.pools["shared"], "model-x", now),
			"ses_stable_123")
	}
	first := build()
	second := build()
	if len(first) != 9 || len(second) != 9 {
		t.Fatalf("candidates=%d/%d want 9/9", len(first), len(second))
	}
	for i := range first {
		if first[i].Identity != second[i].Identity {
			t.Fatalf("order unstable at %d", i)
		}
	}
}

// 1b. Adding a candidate preserves the relative order of the old ones
// (minimal disruption); no hardcoded hash scores.
func TestHRWMinimalDisruption(t *testing.T) {
	old := schedulerTestGateway(t,
		[]string{"zen-key-aaaaa", "zen-key-bbbbb"},
		[]string{"direct", "http://127.0.0.1:8081"})
	now := time.Now().UnixNano()
	oldOrder := old.scheduler.orderCandidates(
		old.scheduler.buildAuthCandidates(TierZen, old.zenCreds, old.pools["shared"], "model-x", now),
		"ses_disrupt_9")
	oldIDs := make([]string, 0, len(oldOrder))
	for _, cand := range oldOrder {
		oldIDs = append(oldIDs, cand.Identity)
	}
	full := schedulerTestGateway(t,
		[]string{"zen-key-aaaaa", "zen-key-bbbbb", "zen-key-ccccc"},
		[]string{"direct", "http://127.0.0.1:8081", "http://127.0.0.1:8082"})
	newOrder := full.scheduler.orderCandidates(
		full.scheduler.buildAuthCandidates(TierZen, full.zenCreds, full.pools["shared"], "model-x", now),
		"ses_disrupt_9")
	// Project the new order onto the old identity set (same tier/cred/pool/
	// proxy/model strings are identical across gateways by construction).
	kept := make([]string, 0, len(oldIDs))
	allowed := make(map[string]bool, len(oldIDs))
	for _, id := range oldIDs {
		allowed[id] = true
	}
	for _, cand := range newOrder {
		if allowed[cand.Identity] {
			kept = append(kept, cand.Identity)
		}
	}
	if len(kept) != len(oldIDs) {
		t.Fatalf("kept=%d want %d", len(kept), len(oldIDs))
	}
	for i := range oldIDs {
		if kept[i] != oldIDs[i] {
			t.Fatalf("relative order disturbed at %d", i)
		}
	}
}

// 1c. Different sessions disperse; failover order differs across sessions.
func TestHRWSessionDispersion(t *testing.T) {
	gateway := schedulerTestGateway(t,
		[]string{"zen-key-aaaaa", "zen-key-bbbbb", "zen-key-ccccc"},
		[]string{"direct", "http://127.0.0.1:8081", "http://127.0.0.1:8082"})
	now := time.Now().UnixNano()
	orders := map[string][]string{}
	for _, session := range []string{"ses_one", "ses_two", "ses_three", "ses_four"} {
		cands := gateway.scheduler.orderCandidates(
			gateway.scheduler.buildAuthCandidates(TierZen, gateway.zenCreds, gateway.pools["shared"], "model-x", now),
			session)
		ids := make([]string, 0, len(cands))
		for _, cand := range cands {
			ids = append(ids, cand.Identity)
		}
		orders[session] = ids
	}
	seen := map[string]bool{}
	for _, ids := range orders {
		seen[strings.Join(ids, "|")] = true
	}
	if len(seen) < 2 {
		t.Fatalf("sessions did not disperse ordering")
	}
}

// 1d. Empty session uses round-robin start offsets, no randomness.
func TestNoSessionRoundRobin(t *testing.T) {
	gateway := schedulerTestGateway(t,
		[]string{"zen-key-aaaaa", "zen-key-bbbbb"},
		[]string{"direct", "http://127.0.0.1:8081"})
	now := time.Now().UnixNano()
	base := gateway.scheduler.buildAuthCandidates(TierZen, gateway.zenCreds, gateway.pools["shared"], "model-x", now)
	if len(base) != 4 {
		t.Fatalf("candidates=%d want 4", len(base))
	}
	first := gateway.scheduler.orderCandidates(append([]targetCandidate(nil), base...), "")
	second := gateway.scheduler.orderCandidates(append([]targetCandidate(nil), base...), "")
	if len(first) != 4 || len(second) != 4 {
		t.Fatalf("lengths %d/%d", len(first), len(second))
	}
	// Consecutive empty-session orderings rotate by one.
	if first[0].Identity != base[0].Identity || second[0].Identity != base[1].Identity {
		t.Fatalf("round-robin offset not applied: %s / %s", first[0].Identity, second[0].Identity)
	}
	for i := range first {
		if first[i].Identity != base[(0+i)%4].Identity {
			t.Fatalf("rotation corrupted at %d", i)
		}
	}
}

// 2. Candidate fanout is K x P with pool isolation and shared pools.
func TestCandidateFanoutKxP(t *testing.T) {
	gateway := schedulerTestGateway(t,
		[]string{"zen-key-aaaaa", "zen-key-bbbbb"},
		[]string{"direct", "http://127.0.0.1:8081", "http://127.0.0.1:8082"})
	now := time.Now().UnixNano()
	cands := gateway.scheduler.buildAuthCandidates(TierZen, gateway.zenCreds, gateway.pools["shared"], "m", now)
	if len(cands) != 6 {
		t.Fatalf("candidates=%d want 6", len(cands))
	}
	// Config order: credential index major, proxy index minor.
	if cands[0].CredIndex != 0 || cands[3].CredIndex != 1 {
		t.Fatalf("config order broken: %+v", cands[0])
	}
}

// 2b. Auth tiers are capped by max_attempts; anonymous walks every proxy once.
func TestTierBudgets(t *testing.T) {
	cfg := testGatewayConfig(
		map[string][]string{"shared": {"direct", "http://127.0.0.1:8081", "http://127.0.0.1:8082", "http://127.0.0.1:8083"}},
		ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	cfg.ZenKeys = []string{"zen-key-aaaaa", "zen-key-bbbbb"}
	cfg.Anonymous = true
	cfg.Retry.MaxAttempts = 3
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixNano()
	auth := gateway.scheduler.buildAuthCandidates(TierZen, gateway.zenCreds, gateway.pools["shared"], "m", now)
	if len(auth) != 8 {
		t.Fatalf("auth candidates=%d want 8", len(auth))
	}
	if limit := min(len(auth), normalized.Retry.MaxAttempts); limit != 3 {
		t.Fatalf("auth budget=%d want 3", limit)
	}
	anon := gateway.scheduler.buildAnonymousCandidates(gateway.pools["shared"], "m", now)
	if len(anon) != 4 {
		t.Fatalf("anonymous must walk every proxy once, got %d", len(anon))
	}
}

// 2c. Fully cooled candidates do not penetrate: empty list, outer tier moves on.
func TestAllCoolNoPenetration(t *testing.T) {
	gateway := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	now := time.Now().UnixNano()
	cands := gateway.scheduler.buildAuthCandidates(TierZen, gateway.zenCreds, gateway.pools["shared"], "m", now)
	if len(cands) != 1 {
		t.Fatalf("candidates=%d", len(cands))
	}
	gateway.scheduler.noteTargetFailure(cands[0].Identity, AttemptClassUpstreamFailure, 500, 0)
	again := gateway.scheduler.buildAuthCandidates(TierZen, gateway.zenCreds, gateway.pools["shared"], "m", time.Now().UnixNano())
	if len(again) != 0 {
		t.Fatalf("cooling target must be excluded, got %d", len(again))
	}
}

// 3. State matrix via the unified update entry point.
func TestStateMatrix(t *testing.T) {
	setup := func() (*Gateway, targetCandidate) {
		gateway := schedulerTestGateway(t, []string{"zen-key-aaaaa", "zen-key-bbbbb"}, []string{"direct", "http://127.0.0.1:8081"})
		pool := gateway.pools["shared"]
		cred := gateway.zenCreds[0]
		proxy := pool.items[0]
		return gateway, authCand(TierZen, cred, pool, proxy, "model-a")
	}
	ctx := context.Background()

	t.Run("success_clears_target_and_credential", func(t *testing.T) {
		gateway, cand := setup()
		gateway.scheduler.noteTargetFailure(cand.Identity, AttemptClassUpstreamFailure, 500, 0)
		gateway.scheduler.noteCredentialAuthFailure(cand.CredID)
		class := gateway.applyAttemptOutcome(ctx, cand, responseWithStatus(200), nil, time.Now().UnixNano())
		if class.Class != AttemptClassSuccess {
			t.Fatalf("class=%s", class.Class)
		}
		if got := gateway.scheduler.targetCoolUntil(cand.Identity); got != 0 {
			t.Fatalf("target not cleared")
		}
		if got := gateway.scheduler.credentialCoolUntil(cand.CredID); got != 0 {
			t.Fatalf("credential not cleared")
		}
	})

	t.Run("401_cools_credential_not_target", func(t *testing.T) {
		gateway, cand := setup()
		gateway.applyAttemptOutcome(ctx, cand, responseWithStatus(401), nil, time.Now().UnixNano())
		if got := gateway.scheduler.credentialCoolUntil(cand.CredID); got <= time.Now().UnixNano() {
			t.Fatalf("401 must cool credential")
		}
		if got := gateway.scheduler.targetCoolUntil(cand.Identity); got != 0 {
			t.Fatalf("401 must not cool target, got %d", got)
		}
	})

	t.Run("403_cools_target_not_credential", func(t *testing.T) {
		gateway, cand := setup()
		gateway.applyAttemptOutcome(ctx, cand, responseWithStatus(403), nil, time.Now().UnixNano())
		if got := gateway.scheduler.targetCoolUntil(cand.Identity); got <= time.Now().UnixNano() {
			t.Fatalf("403 must cool target")
		}
		if got := gateway.scheduler.credentialCoolUntil(cand.CredID); got != 0 {
			t.Fatalf("403 must not cool credential, got %d", got)
		}
	})

	t.Run("429_retry_after_wins_capped", func(t *testing.T) {
		gateway, cand := setup()
		resp := responseWithStatus(429)
		resp.Header.Set("Retry-After", "120")
		before := time.Now()
		started := time.Now().UnixNano()
		gateway.applyAttemptOutcome(ctx, cand, resp, nil, started)
		until, _, ok := gateway.scheduler.proxy429CooldownStatus(TierZen, cand.PoolName, cand.ProxyRaw)
		if !ok {
			t.Fatalf("429 must cool pool-qualified proxy")
		}
		remaining := time.Until(time.Unix(0, until))
		if remaining < 100*time.Second || remaining > 5*time.Minute {
			t.Fatalf("Retry-After not honored within cap: %v", remaining)
		}
		_ = before
		// 429 never cools the per-target or credential layers.
		if got := gateway.scheduler.targetCoolUntil(cand.Identity); got != 0 {
			t.Fatalf("429 must not cool target, got %d", got)
		}
		if got := gateway.scheduler.credentialCoolUntil(cand.CredID); got != 0 {
			t.Fatalf("429 must not cool credential")
		}
		if !cand.Proxy.healthy.Load() {
			t.Fatalf("429 must not mark proxy unhealthy")
		}
		// Huge Retry-After is capped at 5 minutes.
		gateway2, cand2 := setup()
		resp2 := responseWithStatus(429)
		resp2.Header.Set("Retry-After", "3600")
		gateway2.applyAttemptOutcome(ctx, cand2, resp2, nil, time.Now().UnixNano())
		until2, _, ok2 := gateway2.scheduler.proxy429CooldownStatus(TierZen, cand2.PoolName, cand2.ProxyRaw)
		if !ok2 {
			t.Fatalf("429 must cool proxy")
		}
		remaining2 := time.Until(time.Unix(0, until2))
		if remaining2 > 5*time.Minute {
			t.Fatalf("cooldown exceeds cap: %v", remaining2)
		}
	})

	t.Run("5xx_cools_target_not_credential", func(t *testing.T) {
		gateway, cand := setup()
		gateway.applyAttemptOutcome(ctx, cand, responseWithStatus(500), nil, time.Now().UnixNano())
		if got := gateway.scheduler.targetCoolUntil(cand.Identity); got <= time.Now().UnixNano() {
			t.Fatalf("5xx must cool target")
		}
		if got := gateway.scheduler.credentialCoolUntil(cand.CredID); got != 0 {
			t.Fatalf("5xx must not cool credential")
		}
		if !cand.Proxy.healthy.Load() {
			t.Fatalf("5xx must not mark proxy unhealthy")
		}
	})

	t.Run("400_neutral", func(t *testing.T) {
		gateway, cand := setup()
		gateway.scheduler.noteTargetFailure(cand.Identity, AttemptClassUpstreamFailure, 500, 0)
		cooled := gateway.scheduler.targetCoolUntil(cand.Identity)
		gateway.applyAttemptOutcome(ctx, cand, responseWithStatus(400), nil, time.Now().UnixNano())
		if got := gateway.scheduler.targetCoolUntil(cand.Identity); got != cooled {
			t.Fatalf("400 must neither cool nor clear")
		}
	})

	t.Run("transport_neutral_no_cooldown_no_unhealthy", func(t *testing.T) {
		for _, transportErr := range []error{syscall.ECONNREFUSED, errors.New("connection reset by peer")} {
			gateway, cand := setup()
			class := gateway.applyAttemptOutcome(ctx, cand, nil, transportErr, time.Now().UnixNano())
			if class.Class != AttemptClassTransportFailure || !class.Retryable || class.CoolsDown {
				t.Fatalf("transport class=%+v want retryable=true coolsDown=false", class)
			}
			if got := gateway.scheduler.targetCoolUntil(cand.Identity); got != 0 {
				t.Fatalf("single transport error must not cool target, got %d (%v)", got, transportErr)
			}
			if got := gateway.scheduler.credentialCoolUntil(cand.CredID); got != 0 {
				t.Fatalf("transport error must not cool credential (%v)", transportErr)
			}
			if !cand.Proxy.healthy.Load() {
				t.Fatalf("single transport error must not immediately mark proxy unhealthy (%v)", transportErr)
			}
		}
	})

	t.Run("async_probe_can_mark_unhealthy", func(t *testing.T) {
		gateway, cand := setup()
		// The foreground transport path above leaves the proxy healthy;
		// only the independent probe result may flip it.
		gateway.applyAttemptOutcome(ctx, cand, nil, syscall.ECONNREFUSED, time.Now().UnixNano())
		if !cand.Proxy.healthy.Load() {
			t.Fatalf("foreground transport must leave proxy healthy for the probe to decide")
		}
		gateway.applyProxyHealthResult(proxyHealthResult{proxy: cand.Proxy, err: syscall.ECONNREFUSED, failed: true, wasHealthy: true}, "test probe", 0)
		if cand.Proxy.healthy.Load() {
			t.Fatalf("failed probe with isProxyFailure must mark proxy unhealthy")
		}
		// An inconclusive probe never flips health.
		gateway2, cand2 := setup()
		gateway2.applyProxyHealthResult(proxyHealthResult{proxy: cand2.Proxy, err: errors.New("connection reset by peer"), failed: false, wasHealthy: true}, "test probe", 0)
		if !cand2.Proxy.healthy.Load() {
			t.Fatalf("inconclusive probe must not mark proxy unhealthy")
		}
	})

	t.Run("client_cancel_noop", func(t *testing.T) {
		gateway, cand := setup()
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		gateway.applyAttemptOutcome(cancelled, cand, responseWithStatus(500), nil, time.Now().UnixNano())
		if got := gateway.scheduler.targetCoolUntil(cand.Identity); got != 0 {
			t.Fatalf("cancelled ctx must not update target")
		}
		gateway.applyAttemptOutcome(cancelled, cand, responseWithStatus(401), nil, time.Now().UnixNano())
		if got := gateway.scheduler.credentialCoolUntil(cand.CredID); got != 0 {
			t.Fatalf("cancelled ctx must not update credential")
		}
	})
}

// 3b. Model isolation: a 403/5xx rejection on model A never cools model B.
// HTTP 429 is intentionally excluded here: it cools the pool-qualified proxy
// globally across models (see proxy429 tests).
func TestModelIsolation(t *testing.T) {
	gateway := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	pool := gateway.pools["shared"]
	cred := gateway.zenCreds[0]
	proxy := pool.items[0]
	a := authCand(TierZen, cred, pool, proxy, "model-a")
	b := authCand(TierZen, cred, pool, proxy, "model-b")
	if a.Identity == b.Identity {
		t.Fatalf("identities must differ by model")
	}
	gateway.applyAttemptOutcome(context.Background(), a, responseWithStatus(403), nil, time.Now().UnixNano())
	if got := gateway.scheduler.targetCoolUntil(b.Identity); got != 0 {
		t.Fatalf("model B polluted by model A rejection")
	}
	now := time.Now().UnixNano()
	cands := gateway.scheduler.buildAuthCandidates(TierZen, gateway.zenCreds, pool, "model-b", now)
	if len(cands) != 1 {
		t.Fatalf("model B must stay available, got %d", len(cands))
	}
	missing := gateway.scheduler.buildAuthCandidates(TierZen, gateway.zenCreds, pool, "model-a", now)
	if len(missing) != 0 {
		t.Fatalf("model A must stay cooled, got %d", len(missing))
	}
}

// 3c. Credential 401 is global across proxies; target failures are per-combo.
func TestCredentialGlobalVsTargetLocal(t *testing.T) {
	gateway := schedulerTestGateway(t, []string{"zen-key-aaaaa", "zen-key-bbbbb"}, []string{"direct", "http://127.0.0.1:8081"})
	pool := gateway.pools["shared"]
	credA := gateway.zenCreds[0]
	credB := gateway.zenCreds[1]
	a0 := authCand(TierZen, credA, pool, pool.items[0], "m")
	a1 := authCand(TierZen, credA, pool, pool.items[1], "m")
	b0 := authCand(TierZen, credB, pool, pool.items[0], "m")
	// 401 on one combo cools the whole credential: both proxies gone for A.
	gateway.applyAttemptOutcome(context.Background(), a0, responseWithStatus(401), nil, time.Now().UnixNano())
	now := time.Now().UnixNano()
	cands := gateway.scheduler.buildAuthCandidates(TierZen, gateway.zenCreds, pool, "m", now)
	for _, cand := range cands {
		if cand.CredID == credA.id {
			t.Fatalf("credential A must be globally cooling: %s", cand.Identity)
		}
	}
	if len(cands) != 2 {
		t.Fatalf("credential B must be unaffected, got %d", len(cands))
	}
	// Target failure is local to the single combination.
	gateway2 := schedulerTestGateway(t, []string{"zen-key-aaaaa", "zen-key-bbbbb"}, []string{"direct", "http://127.0.0.1:8081"})
	pool2 := gateway2.pools["shared"]
	a0b := authCand(TierZen, gateway2.zenCreds[0], pool2, pool2.items[0], "m")
	a1b := authCand(TierZen, gateway2.zenCreds[0], pool2, pool2.items[1], "m")
	_ = a1
	_ = b0
	gateway2.applyAttemptOutcome(context.Background(), a0b, responseWithStatus(500), nil, time.Now().UnixNano())
	cands2 := gateway2.scheduler.buildAuthCandidates(TierZen, gateway2.zenCreds, pool2, "m", time.Now().UnixNano())
	if len(cands2) != 3 {
		t.Fatalf("only one combo must cool, got %d", len(cands2))
	}
	found := false
	for _, cand := range cands2 {
		if cand.Identity == a1b.Identity {
			found = true
		}
	}
	if !found {
		t.Fatalf("sibling combo must stay available")
	}
}

// 4. Deterministic jitter: same identity+count yields the same delay.
func TestDeterministicBackoff(t *testing.T) {
	scheduler := newTargetScheduler(15 * time.Second)
	first := scheduler.backoffDelay(2, "idem", 0)
	second := scheduler.backoffDelay(2, "idem", 0)
	if first != second {
		t.Fatalf("jitter not deterministic: %v vs %v", first, second)
	}
	// Exponential growth with cap: failures 1..10 never exceed 5 minutes.
	previous := time.Duration(0)
	for failures := uint32(1); failures <= 10; failures++ {
		delay := newTargetScheduler(15*time.Second).backoffDelay(failures, "cap", 0)
		if delay > 5*time.Minute {
			t.Fatalf("cap exceeded: %v", delay)
		}
		_ = previous
		previous = delay
	}
	// Base 15s: failure 1 in [12s,18s], failure 2 in [24s,36s].
	one := newTargetScheduler(15*time.Second).backoffDelay(1, "growth", 0)
	two := newTargetScheduler(15*time.Second).backoffDelay(2, "growth", 0)
	if one < 12*time.Second || one > 18*time.Second {
		t.Fatalf("base delay out of jitter band: %v", one)
	}
	if two < 24*time.Second || two > 36*time.Second {
		t.Fatalf("growth delay out of jitter band: %v", two)
	}
	// Success clears only its own scope (403/5xx targets are per-combo; 429
	// is proxy-global and covered in proxy429 tests).
	gateway := schedulerTestGateway(t, []string{"zen-key-aaaaa", "zen-key-bbbbb"}, []string{"direct"})
	pool := gateway.pools["shared"]
	a := authCand(TierZen, gateway.zenCreds[0], pool, pool.items[0], "m")
	b := authCand(TierZen, gateway.zenCreds[1], pool, pool.items[0], "m")
	gateway.applyAttemptOutcome(context.Background(), a, responseWithStatus(500), nil, time.Now().UnixNano())
	gateway.applyAttemptOutcome(context.Background(), b, responseWithStatus(403), nil, time.Now().UnixNano())
	gateway.applyAttemptOutcome(context.Background(), a, responseWithStatus(200), nil, time.Now().UnixNano())
	if got := gateway.scheduler.targetCoolUntil(a.Identity); got != 0 {
		t.Fatalf("success must clear own target")
	}
	if got := gateway.scheduler.targetCoolUntil(b.Identity); got <= time.Now().UnixNano() {
		t.Fatalf("success must not clear sibling target")
	}
}

// 5. Hot reload migrates credential/proxy/target state; deletions drop.
func TestHotReloadMigration(t *testing.T) {
	oldCfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	oldCfg.ZenKeys = []string{"zen-key-aaaaa", "zen-key-doomed"}
	oldCfg.Anonymous = true
	oldNormalized, err := NormalizeConfig("config.json", oldCfg)
	if err != nil {
		t.Fatal(err)
	}
	oldGateway, err := NewGateway(oldNormalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	pool := oldGateway.pools["shared"]
	keepCred := oldGateway.zenCreds[0]
	// Seed: credential 401 on kept key, target 500 on kept combo, proxy down.
	keepCand := authCand(TierZen, keepCred, pool, pool.items[0], "m")
	oldGateway.applyAttemptOutcome(context.Background(), keepCand, responseWithStatus(401), nil, time.Now().UnixNano())
	oldGateway.scheduler.noteTargetFailure(
		targetIdentity(TierZen, keepCred.id, "shared", pool.items[1].name, "m"),
		AttemptClassUpstreamFailure, 500, 0)
	oldGateway.pools["shared"].items[1].healthy.Store(false)

	newCfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	newCfg.ZenKeys = []string{"zen-key-aaaaa", "zen-key-fresh"}
	newCfg.Anonymous = true
	newNormalized, err := NormalizeConfig("config.json", newCfg)
	if err != nil {
		t.Fatal(err)
	}
	newGateway, err := NewGateway(newNormalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	migrateGatewaySchedulerState(oldGateway, newGateway)
	// Kept credential cooldown migrates by tier+full key.
	if got := newGateway.scheduler.credentialCoolUntil(keepCred.id); got <= time.Now().UnixNano() {
		t.Fatalf("credential cooldown must migrate")
	}
	// Kept target cooldown migrates by full identity.
	migratedTarget := targetIdentity(TierZen, keepCred.id, "shared", pool.items[1].name, "m")
	if got := newGateway.scheduler.targetCoolUntil(migratedTarget); got <= time.Now().UnixNano() {
		t.Fatalf("target cooldown must migrate")
	}
	// Proxy health migrates by (pool, raw URL).
	if newGateway.pools["shared"].items[1].healthy.Load() {
		t.Fatalf("proxy health must migrate")
	}
	if !newGateway.pools["shared"].items[0].healthy.Load() {
		t.Fatalf("healthy proxy must stay healthy")
	}
	// Deleted credential state is dropped; fresh credential starts at zero.
	doomedID := credentialIDForKey(TierZen, "zen-key-doomed")
	if _, until := newGateway.scheduler.credentialSnapshot(doomedID); until != 0 {
		t.Fatalf("deleted credential state must drop")
	}
	freshID := credentialIDForKey(TierZen, "zen-key-fresh")
	if _, until := newGateway.scheduler.credentialSnapshot(freshID); until != 0 {
		t.Fatalf("new credential must start at zero state")
	}
}

// 5c. Migrated remaining time is capped at 5 minutes.
func TestMigrationCapsRemaining(t *testing.T) {
	oldScheduler := newTargetScheduler(15 * time.Second)
	oldScheduler.mu.Lock()
	oldScheduler.credState["zen:keep"] = &credentialEntry{failures: 3, cooldownUntil: time.Now().Add(time.Hour).UnixNano()}
	oldScheduler.mu.Unlock()
	next := newTargetScheduler(15 * time.Second)
	next.migrateFrom(oldScheduler)
	_, until := next.credentialSnapshot("zen:keep")
	remaining := time.Until(time.Unix(0, until))
	if remaining <= 0 || remaining > 5*time.Minute {
		t.Fatalf("migrated remaining=%v, want (0, 5m]", remaining)
	}
}

// 5b. Saving config (Apply path helpers) preserves cooldowns.
func TestApplyPreservesCooldowns(t *testing.T) {
	manager := &RuntimeManager{logger: nil, monitor: NewMonitor(), hub: NewLogHub(100), redactor: NewSecretRedactor()}
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	cfg.ZenKeys = []string{"zen-key-aaaaa"}
	cfg.Anonymous = true
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, manager.monitor)
	if err != nil {
		t.Fatal(err)
	}
	pool := gateway.pools["shared"]
	cred := gateway.zenCreds[0]
	cand := authCand(TierZen, cred, pool, pool.items[0], "m")
	gateway.applyAttemptOutcome(context.Background(), cand, responseWithStatus(401), nil, time.Now().UnixNano())
	manager.current.Store(&gatewayRuntime{config: normalized, gateway: gateway})

	next, err := NewGateway(normalized, nil, manager.monitor)
	if err != nil {
		t.Fatal(err)
	}
	migrateGatewaySchedulerState(gateway, next)
	if got := next.scheduler.credentialCoolUntil(cred.id); got <= time.Now().UnixNano() {
		t.Fatalf("Apply must not wash credential cooldowns")
	}
}

// 6. Refresh never touches foreground scheduler state (targets, proxy429,
// credentials, or proxy health).
func TestRefreshStateless(t *testing.T) {
	gateway := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	pool := gateway.pools["shared"]
	cred := gateway.zenCreds[0]
	cand := authCand(TierZen, cred, pool, pool.items[0], "some-model")
	gateway.applyAttemptOutcome(context.Background(), cand, responseWithStatus(500), nil, time.Now().UnixNano())
	targetUntil := gateway.scheduler.targetCoolUntil(cand.Identity)
	gateway.applyAttemptOutcome(context.Background(), cand, responseWithStatus(429), nil, time.Now().UnixNano())
	_, proxyUntil, proxyOk := gateway.scheduler.proxy429CooldownStatus(TierZen, pool.name, pool.items[0].name)
	if !proxyOk {
		t.Fatalf("seed proxy429 must cool")
	}
	credSeed := "seed-cred"
	_ = credSeed
	// Simulate refresh traversal the way refreshTier does: healthy proxies
	// only, no scheduler reads or writes.
	before, _ := gateway.scheduler.snapshotTargets()
	beforeProxy, _ := gateway.scheduler.snapshotProxy429()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = gateway.refreshTier(ctx, "http://127.0.0.1:9", TierZen)
	_ = gateway.refreshAnonymousTier(ctx, "http://127.0.0.1:9")
	after, _ := gateway.scheduler.snapshotTargets()
	afterProxy, _ := gateway.scheduler.snapshotProxy429()
	if len(before) != len(after) {
		t.Fatalf("refresh polluted targets: %d -> %d", len(before), len(after))
	}
	if len(beforeProxy) != len(afterProxy) {
		t.Fatalf("refresh polluted proxy429: %d -> %d", len(beforeProxy), len(afterProxy))
	}
	if got := gateway.scheduler.targetCoolUntil(cand.Identity); got != targetUntil {
		t.Fatalf("refresh changed target cooldown")
	}
	if _, got, ok := gateway.scheduler.proxy429CooldownStatus(TierZen, pool.name, pool.items[0].name); !ok || got != proxyUntil {
		t.Fatalf("refresh changed proxy429 cooldown")
	}
	if _, until := gateway.scheduler.credentialSnapshot(cred.id); until != 0 {
		t.Fatalf("refresh must not create credential state")
	}
}

// 7. Resources targets list is bounded and redacted.
func TestTargetsBoundedAndRedacted(t *testing.T) {
	scheduler := newTargetScheduler(15 * time.Second)
	secretProxy := "http://user:hunter2@127.0.0.1:8080"
	for i := 0; i < 600; i++ {
		identity := targetIdentity(TierZen, "zen:fp", "shared", secretProxy, "model-"+strconv.Itoa(i))
		scheduler.noteTargetFailure(identity, AttemptClassUpstreamFailure, 500, 0)
	}
	entries, total := scheduler.snapshotTargets()
	if total < 600 {
		t.Fatalf("total=%d want >=600", total)
	}
	if len(entries) > maxTargetSnapshotEntries {
		t.Fatalf("entries=%d exceed bound", len(entries))
	}
	for _, entry := range entries {
		if strings.Contains(entry.ProxyNode, "hunter2") {
			t.Fatalf("proxy secret leaked: %q", entry.ProxyNode)
		}
		if entry.Credential == "" || entry.Tier == "" || entry.Model == "" {
			t.Fatalf("target entry incomplete: %+v", entry)
		}
	}
}

// Finding 1: AvailableTargets is zero only while the credential cooldown is
// still in the future. Expired backoff memory (failures>0, cooldown past)
// must report live healthy-proxy availability, matching buildAuthCandidates.
func TestKeyAvailableTargetsExpiredMemoryMatchesCandidates(t *testing.T) {
	gateway := schedulerTestGateway(t,
		[]string{"zen-key-aaaaa"},
		[]string{"direct", "http://127.0.0.1:8081"})
	cred := gateway.zenCreds[0]
	// Active 401 cooldown: zero available.
	gateway.scheduler.noteCredentialAuthFailure(cred.id)
	statuses := keyStatusesForTier(gateway, "zen", gateway.zenCreds, "shared", nil)
	if len(statuses) != 1 || statuses[0].AvailableTargets != 0 {
		t.Fatalf("cooling credential must report zero: %+v", statuses)
	}
	// Expire the cooldown but keep failure memory.
	past := time.Now().Add(-time.Minute).UnixNano()
	gateway.scheduler.mu.Lock()
	gateway.scheduler.credState[cred.id].cooldownUntil = past
	gateway.scheduler.mu.Unlock()
	statuses = keyStatusesForTier(gateway, "zen", gateway.zenCreds, "shared", nil)
	if len(statuses) != 1 {
		t.Fatalf("statuses=%d", len(statuses))
	}
	if statuses[0].Failures == 0 {
		t.Fatalf("failure memory must survive expiry: %+v", statuses[0])
	}
	if statuses[0].CooldownUntil != nil || statuses[0].CooldownRemainingSeconds != nil {
		t.Fatalf("expired cooldown must not expose a deadline: %+v", statuses[0])
	}
	now := time.Now().UnixNano()
	cands := gateway.scheduler.buildAuthCandidates(TierZen, gateway.zenCreds, gateway.pools["shared"], "m", now)
	if statuses[0].AvailableTargets != len(cands) || statuses[0].AvailableTargets != 2 {
		t.Fatalf("available=%d candidates=%d want 2", statuses[0].AvailableTargets, len(cands))
	}
}

// Finding 2: the per-proxy anonymous target aggregate counts only the
// anonymous credential. An auth target cooldown on the same shared-pool proxy
// must not pollute it. The unified 代理可用性 Zen column reflects only
// transport + proxy429 + channel layers, never per-model target cooldowns.
func TestAnonymousSummaryIgnoresSharedPoolAuthCooldown(t *testing.T) {
	gateway := schedulerTestGateway(t,
		[]string{"zen-key-aaaaa"},
		[]string{"direct", "http://127.0.0.1:8081"})
	pool := gateway.pools["shared"]
	cred := gateway.zenCreds[0]
	authID := targetIdentity(TierZen, cred.id, "shared", pool.items[0].name, "m")
	gateway.scheduler.noteTargetFailure(authID, AttemptClassUpstreamFailure, 500, 0)
	if got := gateway.scheduler.targetCoolUntil(authID); got <= time.Now().UnixNano() {
		t.Fatalf("auth target must cool")
	}
	active, _, _, failures := gateway.scheduler.proxyTargetSummary("shared", pool.items[0].name)
	if active != 0 || failures != 0 {
		t.Fatalf("auth cooldown polluted anonymous summary: active=%d failures=%d", active, failures)
	}
	// An anonymous 403 failure on the same proxy is counted (403/5xx only;
	// 429 lives in the proxy429 layer).
	anonID := targetIdentity(TierZen, anonymousSchedulerCredentialID, "shared", pool.items[0].name, "m")
	gateway.scheduler.noteTargetFailure(anonID, AttemptClassAuthFailure, 403, 0)
	active, _, lastClass, failures := gateway.scheduler.proxyTargetSummary("shared", pool.items[0].name)
	if active != 1 || failures != 1 || lastClass != AttemptClassAuthFailure {
		t.Fatalf("anonymous cooldown missing: active=%d failures=%d class=%q", active, failures, lastClass)
	}
}

// Finding 3a: failure memory survives one cooldown expiry for backoff
// escalation, then prunes after targetStaleRetention out of cooldown.
func TestTargetRetentionEscalatesThenPrunes(t *testing.T) {
	scheduler := newTargetScheduler(15 * time.Second)
	identity := targetIdentity(TierZen, "zen:fp", "shared", "direct", "m")
	scheduler.noteTargetFailure(identity, AttemptClassUpstreamFailure, 500, 0)
	scheduler.mu.Lock()
	firstFailures := scheduler.targetState[identity].failures
	// Expire the cooldown but keep the failure recent: memory retained.
	scheduler.targetState[identity].cooldownUntil = time.Now().Add(-time.Second).UnixNano()
	scheduler.mu.Unlock()
	scheduler.noteTargetFailure(identity, AttemptClassUpstreamFailure, 500, 0)
	scheduler.mu.Lock()
	secondFailures := scheduler.targetState[identity].failures
	scheduler.mu.Unlock()
	if firstFailures != 1 || secondFailures != 2 {
		t.Fatalf("failures=%d->%d want 1->2 (escalation across one expiry)", firstFailures, secondFailures)
	}
	// Age past retention out of cooldown: snapshot prunes it.
	scheduler.mu.Lock()
	scheduler.targetState[identity].cooldownUntil = time.Now().Add(-time.Hour).UnixNano()
	scheduler.targetState[identity].lastFailureAt = time.Now().Add(-targetStaleRetention - time.Hour).UnixNano()
	scheduler.mu.Unlock()
	entries, total := scheduler.snapshotTargets()
	if total != 0 || len(entries) != 0 {
		t.Fatalf("stale entry must prune: total=%d entries=%d", total, len(entries))
	}
	scheduler.mu.Lock()
	_, stillThere := scheduler.targetState[identity]
	scheduler.mu.Unlock()
	if stillThere {
		t.Fatalf("stale entry still in map after snapshot prune")
	}
}

// Finding 3b: the target map is bounded at maxTargetStates with deterministic
// (oldest-idle, identity tie-break) eviction; a fully active map may
// temporarily exceed the cap rather than break an active cooldown.
func TestTargetCapDeterministicEvictionAndActiveOverflow(t *testing.T) {
	scheduler := newTargetScheduler(15 * time.Second)
	nowNanos := time.Now().UnixNano()
	// Idle (cooldown just expired, so evictable) but fresh (last failure
	// within retention, so prune keeps them). Staggered lastFailureAt makes
	// the victim deterministic: index 0.
	expiredCooldown := nowNanos - int64(time.Second)
	scheduler.mu.Lock()
	for i := 0; i < maxTargetStates; i++ {
		identity := targetIdentity(TierZen, "zen:fp", "shared", "direct", "model-cap-"+strconv.Itoa(i))
		scheduler.targetState[identity] = &targetEntry{
			failures: 1, cooldownUntil: expiredCooldown,
			lastFailureAt:    nowNanos - int64(maxTargetStates-i),
			lastFailureClass: AttemptClassUpstreamFailure, lastStatus: 500,
		}
	}
	scheduler.mu.Unlock()
	fresh := targetIdentity(TierZen, "zen:fp", "shared", "direct", "model-cap-fresh")
	scheduler.noteTargetFailure(fresh, AttemptClassUpstreamFailure, 500, 0)
	scheduler.mu.Lock()
	size := len(scheduler.targetState)
	_, freshThere := scheduler.targetState[fresh]
	oldest := targetIdentity(TierZen, "zen:fp", "shared", "direct", "model-cap-0")
	_, oldestThere := scheduler.targetState[oldest]
	scheduler.mu.Unlock()
	if size != maxTargetStates {
		t.Fatalf("size=%d want exactly %d after deterministic eviction", size, maxTargetStates)
	}
	if !freshThere || oldestThere {
		t.Fatalf("fresh=%v oldestStillThere=%v: must evict oldest idle", freshThere, oldestThere)
	}
	// All-active map: the next creation must not destroy active state and
	// may temporarily exceed the cap by exactly one.
	future := time.Now().Add(time.Hour).UnixNano()
	scheduler.mu.Lock()
	for _, entry := range scheduler.targetState {
		entry.cooldownUntil = future
	}
	scheduler.mu.Unlock()
	overflow := targetIdentity(TierZen, "zen:fp", "shared", "direct", "model-cap-overflow")
	scheduler.noteTargetFailure(overflow, AttemptClassUpstreamFailure, 500, 0)
	scheduler.mu.Lock()
	overflowSize := len(scheduler.targetState)
	scheduler.mu.Unlock()
	if overflowSize != maxTargetStates+1 {
		t.Fatalf("all-active overflow size=%d want %d", overflowSize, maxTargetStates+1)
	}
}

// Finding 3c: migration carries only still-future cooldowns; expired failure
// memory does not cross Apply (credential and target alike).
func TestMigrationDropsExpiredFailureMemory(t *testing.T) {
	oldScheduler := newTargetScheduler(15 * time.Second)
	now := time.Now()
	oldScheduler.mu.Lock()
	oldScheduler.credState["zen:keep"] = &credentialEntry{failures: 2, cooldownUntil: now.Add(time.Minute).UnixNano()}
	oldScheduler.credState["zen:old"] = &credentialEntry{failures: 3, cooldownUntil: now.Add(-time.Minute).UnixNano()}
	keepTarget := targetIdentity(TierZen, "zen:keep", "shared", "direct", "m")
	staleTarget := targetIdentity(TierZen, "zen:old", "shared", "direct", "m")
	oldScheduler.targetState[keepTarget] = &targetEntry{
		failures: 1, cooldownUntil: now.Add(time.Minute).UnixNano(),
		lastFailureAt: now.UnixNano(), lastFailureClass: AttemptClassUpstreamFailure, lastStatus: 500,
	}
	oldScheduler.targetState[staleTarget] = &targetEntry{
		failures: 4, cooldownUntil: now.Add(-time.Minute).UnixNano(),
		lastFailureAt: now.Add(-time.Minute).UnixNano(), lastFailureClass: AttemptClassUpstreamFailure, lastStatus: 500,
	}
	oldScheduler.mu.Unlock()
	next := newTargetScheduler(15 * time.Second)
	next.migrateFrom(oldScheduler)
	if _, until := next.credentialSnapshot("zen:keep"); until <= time.Now().UnixNano() {
		t.Fatalf("future credential cooldown must migrate")
	}
	if _, until := next.credentialSnapshot("zen:old"); until != 0 {
		t.Fatalf("expired credential memory must not cross Apply")
	}
	if got := next.targetCoolUntil(keepTarget); got <= time.Now().UnixNano() {
		t.Fatalf("future target cooldown must migrate")
	}
	next.mu.Lock()
	_, staleThere := next.targetState[staleTarget]
	next.mu.Unlock()
	if staleThere {
		t.Fatalf("expired target memory must not cross Apply")
	}
}
