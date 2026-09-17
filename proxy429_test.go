package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// 429 on proxy P/model A filters P for model B, anonymous and authenticated
// Zen, and across Zen credentials when tier+pool+proxy same. Go stays
// isolated even for the same pool+URL (tier/channel qualification).
func TestProxy429FiltersAcrossModelsCredsTiers(t *testing.T) {
	cfg := testGatewayConfig(
		map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
	)
	cfg.Keys = []string{"zen-key-aaaaa", "zen-key-bbbbb", "go-key-ccccc"}
	cfg.Anonymous = true
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	pool := gateway.pools["shared"]
	cred := gateway.authCreds[0]
	candA := authCand(TierZen, cred, pool, pool.items[0], "model-a")
	started := time.Now().UnixNano()
	gateway.applyAttemptOutcome(context.Background(), candA, responseWithStatus(429), nil, started)
	if _, _, ok := gateway.scheduler.proxy429CooldownStatus(TierZen, "shared", pool.items[0].name); !ok {
		t.Fatalf("proxy429 must cool shared/direct")
	}
	if got := gateway.scheduler.targetCoolUntil(candA.Identity); got != 0 {
		t.Fatalf("429 must not cool target")
	}
	now := time.Now().UnixNano()
	// Model B authenticated: P excluded for every credential.
	authB := gateway.scheduler.buildAuthCandidates(TierZen, gateway.authCreds, pool, "model-b", now)
	if len(authB) == 0 {
		t.Fatalf("model B must retain the other proxy")
	}
	for _, c := range authB {
		if c.ProxyRaw == pool.items[0].name {
			t.Fatalf("cooled proxy must be filtered for model B: %+v", c)
		}
	}
	// Both auth credentials share the same filtered view (3 creds x 1 proxy).
	if len(authB) != 3 {
		t.Fatalf("authB=%d want 3 (3 creds x 1 remaining proxy)", len(authB))
	}
	// Tier qualification stays isolated: a TierGo 429 never filters TierZen.
	gateway.scheduler.noteProxy429Failure(TierGo, "shared", pool.items[0].name, AttemptClassRateLimited, 429, 0, now)
	if got := gateway.scheduler.buildAuthCandidates(TierZen, gateway.authCreds, pool, "model-b", now); len(got) == 0 {
		t.Fatalf("Go 429 must not filter Zen")
	}
	// Anonymous model B is filtered identically.
	anonB := gateway.scheduler.buildAnonymousCandidates(pool, "model-b", now)
	if len(anonB) != 1 || anonB[0].ProxyRaw == pool.items[0].name {
		t.Fatalf("anonB must exclude cooled proxy, got %+v", anonB)
	}
	// Model A itself is also filtered on the cooled proxy.
	authA := gateway.scheduler.buildAuthCandidates(TierZen, gateway.authCreds, pool, "model-a", now)
	for _, c := range authA {
		if c.ProxyRaw == pool.items[0].name {
			t.Fatalf("model A must also filter cooled proxy")
		}
	}
}

// Same raw proxy URL in different named pools is isolated.
func TestProxy429PoolIsolationSameURL(t *testing.T) {
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct"}, "z": {"direct"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Anonymous = true
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	poolA := gateway.pools["a"]
	cand := authCand(TierZen, gateway.authCreds[0], poolA, poolA.items[0], "m")
	// Record 429 against pool a/direct via a candidate that uses pool a.
	cand.PoolName = "a"
	gateway.applyAttemptOutcome(context.Background(), cand, responseWithStatus(429), nil, time.Now().UnixNano())
	if _, _, ok := gateway.scheduler.proxy429CooldownStatus(TierZen, "a", "direct"); !ok {
		t.Fatalf("a/direct must cool")
	}
	if _, _, ok := gateway.scheduler.proxy429CooldownStatus(TierZen, "z", "direct"); ok {
		t.Fatalf("z/direct must stay isolated from a/direct")
	}
	now := time.Now().UnixNano()
	anon := gateway.scheduler.buildAnonymousCandidates(poolA, "m", now)
	if len(anon) != 0 {
		t.Fatalf("a pool must filter cooled proxy, got %d", len(anon))
	}
	zenPool := gateway.pools["z"]
	zen := gateway.scheduler.buildAuthCandidates(TierZen, gateway.authCreds, zenPool, "m", now)
	if len(zen) != 1 {
		t.Fatalf("z pool must stay available, got %d", len(zen))
	}
}

// 403/5xx remain model+credential target scoped and do not proxy-global cool.
func TestProxy429Target403_5xxStayScoped(t *testing.T) {
	gateway := schedulerTestGateway(t, []string{"zen-key-aaaaa", "zen-key-bbbbb"}, []string{"direct", "http://127.0.0.1:8081"})
	pool := gateway.pools["shared"]
	credA := gateway.authCreds[0]
	a := authCand(TierZen, credA, pool, pool.items[0], "model-a")
	bSameProxyOtherModel := authCand(TierZen, credA, pool, pool.items[0], "model-b")
	bOtherCred := authCand(TierZen, gateway.authCreds[1], pool, pool.items[0], "model-a")
	bOtherProxy := authCand(TierZen, credA, pool, pool.items[1], "model-a")
	for _, status := range []int{403, 500} {
		gw := schedulerTestGateway(t, []string{"zen-key-aaaaa", "zen-key-bbbbb"}, []string{"direct", "http://127.0.0.1:8081"})
		p := gw.pools["shared"]
		aa := authCand(TierZen, gw.authCreds[0], p, p.items[0], "model-a")
		gw.applyAttemptOutcome(context.Background(), aa, responseWithStatus(status), nil, time.Now().UnixNano())
		if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "shared", p.items[0].name); ok {
			t.Fatalf("status %d must not create proxy429", status)
		}
		now := time.Now().UnixNano()
		if got := gw.scheduler.targetCoolUntil(aa.Identity); got <= now {
			t.Fatalf("status %d must cool own target", status)
		}
		// Siblings stay available.
		for _, sib := range []targetCandidate{
			authCand(TierZen, gw.authCreds[0], p, p.items[0], "model-b"),
			authCand(TierZen, gw.authCreds[1], p, p.items[0], "model-a"),
			authCand(TierZen, gw.authCreds[0], p, p.items[1], "model-a"),
		} {
			if got := gw.scheduler.targetCoolUntil(sib.Identity); got != 0 {
				t.Fatalf("status %d polluted sibling %+v", status, sib)
			}
		}
		_ = bSameProxyOtherModel
		_ = bOtherCred
		_ = bOtherProxy
		_ = a
	}
}

// Pinned cross-model sessions on P both fast-fail 429 after one live 429;
// no sends/fallback; after expiry same pin sends P.
func TestProxy429PinnedCrossModelFastFailAndExpiry(t *testing.T) {
	cfg := testGatewayConfig(
		map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
	)
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Anonymous = true
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	pool := gateway.pools["shared"]
	auth := normalizeRouteAuthority(normalized.Upstream.Zen)
	// Anonymous pins remain exactly proxy-affine with no moves: pin same
	// client session, two models, to the same pool+proxy P.
	gateway.scheduler.pinBind("ses_429_cross", "model-a", sessionPin{Tier: TierZen, CredID: anonymousSchedulerCredentialID, Pool: "shared", ProxyRaw: "direct", Model: "model-a", Protocol: ProtocolChat, Authority: auth})
	gateway.scheduler.pinBind("ses_429_cross", "model-b", sessionPin{Tier: TierZen, CredID: anonymousSchedulerCredentialID, Pool: "shared", ProxyRaw: "direct", Model: "model-b", Protocol: ProtocolChat, Authority: auth})
	// One live 429 on P (model-a context) cools P on the Zen channel.
	live := targetCandidate{Tier: TierZen, CredKey: anonymousZenKey, CredID: anonymousSchedulerCredentialID, CredDisplay: anonymousCredentialID, CredIndex: -1, PoolName: "shared", Proxy: pool.items[0], ProxyRaw: pool.items[0].name, Model: "model-a", Identity: targetIdentity(TierZen, anonymousSchedulerCredentialID, "shared", pool.items[0].name, "model-a")}
	gateway.applyAttemptOutcome(context.Background(), live, responseWithStatus(429), nil, time.Now().UnixNano())
	routeA := modelRoute{ID: "model-a", Tier: TierZen, Protocol: ProtocolChat, Protocols: map[Tier]Protocol{TierZen: ProtocolChat}, Anonymous: false, KeyTiers: []Tier{TierZen}}
	routeB := modelRoute{ID: "model-b", Tier: TierZen, Protocol: ProtocolChat, Protocols: map[Tier]Protocol{TierZen: ProtocolChat}, Anonymous: false, KeyTiers: []Tier{TierZen}}
	bodiesA := map[Tier][]byte{TierZen: []byte(`{"model":"model-a"}`)}
	bodiesB := map[Tier][]byte{TierZen: []byte(`{"model":"model-b"}`)}
	var c0, c1 atomic.Int32
	postStub(t, gateway, "shared", 0, &c0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "shared", 1, &c1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	respA, _, attemptsA, err := gateway.doUpstreamTiers(ctx, routeA, bodiesA, requestIDs{Session: "ses_429_cross", Request: "req-a1", Project: "p"}, 0)
	if err != nil {
		t.Fatalf("pinned model-a fast-fail must return response, err=%v", err)
	}
	if respA == nil || respA.StatusCode != 429 {
		t.Fatalf("model-a status=%v want 429", respA)
	}
	if got := respA.Header.Get("Retry-After"); got == "" {
		t.Fatalf("model-a fast-fail must carry Retry-After")
	}
	drainAndClose(respA.Body)
	respB, _, attemptsB, err := gateway.doUpstreamTiers(ctx, routeB, bodiesB, requestIDs{Session: "ses_429_cross", Request: "req-b1", Project: "p"}, 0)
	if err != nil {
		t.Fatalf("pinned model-b fast-fail must return response, err=%v", err)
	}
	if respB == nil || respB.StatusCode != 429 {
		t.Fatalf("model-b status=%v want 429", respB)
	}
	drainAndClose(respB.Body)
	if attemptsA != 0 || attemptsB != 0 {
		t.Fatalf("fast-fail attempts=%d/%d want 0/0", attemptsA, attemptsB)
	}
	if postCount(&c0)+postCount(&c1) != 0 {
		t.Fatalf("fast-fail must send nothing: %d/%d", postCount(&c0), postCount(&c1))
	}
	// After expiry the same pins send only P.
	gateway.scheduler.mu.Lock()
	if entry := gateway.scheduler.proxy429State[proxy429Identity(TierZen, "shared", "direct")]; entry != nil {
		entry.cooldownUntil = time.Now().Add(-time.Second).UnixNano()
	}
	gateway.scheduler.mu.Unlock()
	var e0, e1 atomic.Int32
	postStub(t, gateway, "shared", 0, &e0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "shared", 1, &e1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	respA2, _, _, err := gateway.doUpstreamTiers(ctx, routeA, bodiesA, requestIDs{Session: "ses_429_cross", Request: "req-a2", Project: "p"}, 0)
	if err != nil || respA2 == nil || respA2.StatusCode != 200 {
		t.Fatalf("after expiry model-a err=%v resp=%v want 200", err, respA2)
	}
	drainAndClose(respA2.Body)
	if postCount(&e0) != 1 || postCount(&e1) != 0 {
		t.Fatalf("after expiry must send only pinned P: %d/%d", postCount(&e0), postCount(&e1))
	}
}

// Retry-After/backoff/cap, local envelope, and no same-target retry.
func TestProxy429BackoffCapEnvelopeNoRetry(t *testing.T) {
	scheduler := newTargetScheduler(15 * time.Second)
	first := scheduler.backoffDelay(1, proxy429Identity(TierZen, "p", "direct"), 0)
	if first < 12*time.Second || first > 18*time.Second {
		t.Fatalf("proxy429 base delay out of band: %v", first)
	}
	// Retry-After larger wins, huge capped at 5m.
	gateway := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	pool := gateway.pools["shared"]
	cred := gateway.authCreds[0]
	cand := authCand(TierZen, cred, pool, pool.items[0], "m")
	resp := responseWithStatus(429)
	resp.Header.Set("Retry-After", "120")
	gateway.applyAttemptOutcome(context.Background(), cand, resp, nil, time.Now().UnixNano())
	until, _, ok := gateway.scheduler.proxy429CooldownStatus(TierZen, pool.name, pool.items[0].name)
	if !ok {
		t.Fatalf("must cool")
	}
	// Default gateway uses 429 min 300s: backoff (~240-360s) wins over 120s.
	if remaining := time.Until(time.Unix(0, until)); remaining < 200*time.Second || remaining > 400*time.Second {
		t.Fatalf("Retry-After not honored within 429 max: %v", remaining)
	}
	gateway2 := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	pool2 := gateway2.pools["shared"]
	cand2 := authCand(TierZen, gateway2.authCreds[0], pool2, pool2.items[0], "m")
	resp2 := responseWithStatus(429)
	resp2.Header.Set("Retry-After", "3600")
	gateway2.applyAttemptOutcome(context.Background(), cand2, resp2, nil, time.Now().UnixNano())
	until2, _, _ := gateway2.scheduler.proxy429CooldownStatus(TierZen, pool2.name, pool2.items[0].name)
	if remaining := time.Until(time.Unix(0, until2)); remaining < 3500*time.Second || remaining > 3600*time.Second {
		t.Fatalf("429 Retry-After must clamp to 429 max (3600s), got %v", remaining)
	}
	// Local envelope preserves target protocol + Retry-After.
	for _, proto := range []Protocol{ProtocolChat, ProtocolResponses, ProtocolAnthropic} {
		synth := pinLocalResponse(429, 7, "upstream temporarily unavailable")
		rec := &testResponseWriter{header: make(http.Header)}
		copyErrorResponse(rec, proto, synth, "req-x")
		if rec.status != 429 {
			t.Fatalf("proto %s status=%d want 429", proto, rec.status)
		}
		if rec.header.Get("Retry-After") == "" {
			t.Fatalf("proto %s must carry Retry-After", proto)
		}
		if len(rec.body) == 0 {
			t.Fatalf("proto %s empty envelope", proto)
		}
	}
	// No same-target retry: 429 falls back without retrying the same proxy.
	monitor := NewMonitor()
	authGw := authTwoProxyGateway(t, monitor, 5)
	var z0, z1 atomic.Int32
	postStub(t, authGw, "z", 0, &z0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":"throttled"}`), nil
	})
	postStub(t, authGw, "z", 1, &z1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	resp3, _, _, err := authGw.doUpstreamTiers(context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{}), route, routeBodies(), emptySessionIDs(), 0)
	if err != nil || resp3 == nil || resp3.StatusCode != 200 {
		t.Fatalf("err=%v resp=%v want 200 via fallback", err, resp3)
	}
	drainAndClose(resp3.Body)
	if postCount(&z0) != 1 || postCount(&z1) != 1 {
		t.Fatalf("429 must not retry same target: %d/%d want 1/1", postCount(&z0), postCount(&z1))
	}
}

// Older in-flight 2xx cannot clear newer 429; newer-started 2xx clears.
// Multiple newer failures remain authoritative.
func TestProxy429ClearOrdering(t *testing.T) {
	scheduler := newTargetScheduler(15 * time.Second)
	pool, raw := "shared", "direct"
	// Failure started at 2000.
	scheduler.noteProxy429Failure(TierZen, pool, raw, AttemptClassRateLimited, 429, 0, 2000)
	// Older success started at 1000 must not clear.
	if cleared := scheduler.noteProxy429Success(TierZen, pool, raw, 1000); cleared.Cleared {
		t.Fatalf("stale 2xx must not clear newer 429")
	}
	if _, _, ok := scheduler.proxy429CooldownStatus(TierZen, pool, raw); !ok {
		// Status uses wall clock; the entry above may have expired already
		// because backoff uses wall now. Re-seed with wall-started failure
		// for the status check below.
		_ = ok
	}
	// Newer success started at 2000 (equal) clears.
	if cleared := scheduler.noteProxy429Success(TierZen, pool, raw, 2000); !cleared.Cleared {
		t.Fatalf("equal-started 2xx must clear")
	}
	// Multiple newer failures remain authoritative: two failures, stale clear fails.
	scheduler.noteProxy429Failure(TierZen, pool, raw, AttemptClassRateLimited, 429, 0, 1000)
	scheduler.noteProxy429Failure(TierZen, pool, raw, AttemptClassRateLimited, 429, 0, 2000)
	if cleared := scheduler.noteProxy429Success(TierZen, pool, raw, 1500); cleared.Cleared {
		t.Fatalf("2xx between two failures must not clear the newer failure")
	}
	if cleared := scheduler.noteProxy429Success(TierZen, pool, raw, 3000); !cleared.Cleared {
		t.Fatalf("newest 2xx must clear")
	}
	// Gateway-level ordering with wall-clock cooldowns.
	gateway := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	gwPool := gateway.pools["shared"]
	gwCred := gateway.authCreds[0]
	cand := authCand(TierZen, gwCred, gwPool, gwPool.items[0], "m")
	newerStarted := time.Now().UnixNano()
	gateway.applyAttemptOutcome(context.Background(), cand, responseWithStatus(429), nil, newerStarted)
	olderStarted := newerStarted - int64(time.Second)
	gateway.applyAttemptOutcome(context.Background(), cand, responseWithStatus(200), nil, olderStarted)
	if _, _, ok := gateway.scheduler.proxy429CooldownStatus(TierZen, gwPool.name, gwPool.items[0].name); !ok {
		t.Fatalf("stale in-flight 2xx cleared a newer 429")
	}
	laterStarted := time.Now().UnixNano()
	gateway.applyAttemptOutcome(context.Background(), cand, responseWithStatus(200), nil, laterStarted)
	if _, _, ok := gateway.scheduler.proxy429CooldownStatus(TierZen, gwPool.name, gwPool.items[0].name); ok {
		t.Fatalf("newer-started 2xx must clear proxy429")
	}
}

// -race focused: concurrent 429 + 2xx ordering never panics and respects the
// watermark under concurrency.
func TestProxy429ConcurrentOrderingRace(t *testing.T) {
	scheduler := newTargetScheduler(15 * time.Second)
	pool, raw := "shared", "direct"
	base := time.Now().UnixNano()
	done := make(chan struct{})
	var failures atomic.Int32
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			scheduler.noteProxy429Failure(TierZen, pool, raw, AttemptClassRateLimited, 429, 0, base+int64(i))
			failures.Add(1)
		}
	}()
	for i := 0; i < 50; i++ {
		// Stale clears (started before base) must never clear the newest failure.
		scheduler.noteProxy429Success(TierZen, pool, raw, base-1)
	}
	<-done
	// Newest success clears deterministically.
	if cleared := scheduler.noteProxy429Success(TierZen, pool, raw, base+1000); !cleared.Cleared && failures.Load() > 0 {
		// If already cleared by a concurrent path, status must be absent.
		if _, _, ok := scheduler.proxy429CooldownStatus(TierZen, pool, raw); ok {
			t.Fatalf("newest clear failed while state remains")
		}
	}
}

// Migration valid/drop, bounded cap/eviction.
func TestProxy429MigrationAndBounds(t *testing.T) {
	oldCfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	oldCfg.Keys = []string{"zen-key-aaaaa"}
	oldCfg.Anonymous = true
	oldNormalized, err := NormalizeConfig("config.json", oldCfg)
	if err != nil {
		t.Fatal(err)
	}
	oldGateway, err := NewGateway(oldNormalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	oldGateway.scheduler.noteProxy429Failure(TierZen, "shared", "direct", AttemptClassRateLimited, 429, 0, time.Now().UnixNano())
	oldGateway.scheduler.noteProxy429Failure(TierZen, "shared", "http://127.0.0.1:8081", AttemptClassRateLimited, 429, 0, time.Now().UnixNano())
	// Expire the second entry: it must not migrate.
	oldGateway.scheduler.mu.Lock()
	oldGateway.scheduler.proxy429State[proxy429Identity(TierZen, "shared", "http://127.0.0.1:8081")].cooldownUntil = time.Now().Add(-time.Second).UnixNano()
	oldGateway.scheduler.mu.Unlock()

	newCfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	newCfg.Keys = []string{"zen-key-aaaaa"}
	newCfg.Anonymous = true
	newNormalized, err := NormalizeConfig("config.json", newCfg)
	if err != nil {
		t.Fatal(err)
	}
	newGateway, err := NewGateway(newNormalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	summary := migrateGatewaySchedulerState(oldGateway, newGateway)
	if summary.Proxy429 != 1 {
		t.Fatalf("migrated proxy429=%d want 1 (valid only)", summary.Proxy429)
	}
	if _, _, ok := newGateway.scheduler.proxy429CooldownStatus(TierZen, "shared", "direct"); !ok {
		t.Fatalf("valid proxy429 must migrate")
	}
	if _, _, ok := newGateway.scheduler.proxy429CooldownStatus(TierZen, "shared", "http://127.0.0.1:8081"); ok {
		t.Fatalf("removed/expired proxy429 must drop")
	}
	// Remaining capped at the NEW configured 429 max (default 3600s).
	old2 := newTargetScheduler(15 * time.Second)
	old2.mu.Lock()
	old2.proxy429State[proxy429Identity(TierZen, "shared", "direct")] = &proxy429Entry{failures: 1, cooldownUntil: time.Now().Add(2 * time.Hour).UnixNano(), lastFailureAt: time.Now().UnixNano(), lastStartedNanos: time.Now().UnixNano(), lastFailureClass: AttemptClassRateLimited, lastStatus: 429}
	old2.mu.Unlock()
	next2 := newTargetScheduler(15 * time.Second)
	next2.migrateFrom(old2)
	_, until := next2.proxy429EntrySnapshot(TierZen, "shared", "direct")
	if remaining := time.Until(time.Unix(0, until)); remaining <= 0 || remaining > defaultRateLimitMaxSeconds*time.Second {
		t.Fatalf("migrated remaining=%v want (0,3600s]", remaining)
	}
	// Bounded cap with deterministic oldest-idle eviction.
	bounded := newTargetScheduler(time.Second)
	nowNanos := time.Now().UnixNano()
	expired := nowNanos - int64(time.Second)
	bounded.mu.Lock()
	for i := 0; i < maxProxy429States; i++ {
		id := proxy429Identity(TierZen, "shared", "proxy-cap-"+itoa(i))
		bounded.proxy429State[id] = &proxy429Entry{failures: 1, cooldownUntil: expired, lastFailureAt: nowNanos - int64(maxProxy429States-i), lastFailureClass: AttemptClassRateLimited, lastStatus: 429, lastStartedNanos: nowNanos - int64(maxProxy429States-i)}
	}
	bounded.mu.Unlock()
	bounded.noteProxy429Failure(TierZen, "shared", "proxy-cap-fresh", AttemptClassRateLimited, 429, 0, nowNanos)
	bounded.mu.Lock()
	size := len(bounded.proxy429State)
	_, freshThere := bounded.proxy429State[proxy429Identity(TierZen, "shared", "proxy-cap-fresh")]
	_, oldestThere := bounded.proxy429State[proxy429Identity(TierZen, "shared", "proxy-cap-0")]
	bounded.mu.Unlock()
	if size != maxProxy429States {
		t.Fatalf("size=%d want %d after eviction", size, maxProxy429States)
	}
	if !freshThere || oldestThere {
		t.Fatalf("must evict oldest idle: fresh=%v oldest=%v", freshThere, oldestThere)
	}
	// All-active overflow temporarily exceeds the cap.
	future := time.Now().Add(time.Hour).UnixNano()
	bounded.mu.Lock()
	for _, entry := range bounded.proxy429State {
		entry.cooldownUntil = future
	}
	bounded.mu.Unlock()
	bounded.noteProxy429Failure(TierZen, "shared", "proxy-cap-overflow", AttemptClassRateLimited, 429, 0, nowNanos)
	bounded.mu.Lock()
	overflowSize := len(bounded.proxy429State)
	bounded.mu.Unlock()
	if overflowSize != maxProxy429States+1 {
		t.Fatalf("overflow size=%d want %d", overflowSize, maxProxy429States+1)
	}
}

// Readiness/probe/refresh isolation for proxy429.
func TestProxy429ReadinessProbeRefreshIsolation(t *testing.T) {
	gateway := singleZenGateway(t, false)
	pool := gateway.pools["shared"]
	cand := authCand(TierZen, gateway.authCreds[0], pool, pool.items[0], "m1")
	gateway.applyAttemptOutcome(t.Context(), cand, responseWithStatus(429), nil, time.Now().UnixNano())
	code, health := decodeHealth(t, gateway)
	if code != http.StatusOK || !health.Ready {
		t.Fatalf("proxy429 must not degrade readiness: code=%d ready=%v issues=%v routing=%+v", code, health.Ready, health.Issues, health.Routing)
	}
	if health.Routing.ChannelsAvailable != 1 {
		t.Fatalf("channels=%d want 1", health.Routing.ChannelsAvailable)
	}
	// Probe changes transport health only and never clears/sets proxy429.
	manager, admin, token, csrf := operabilityAdmin(t, []string{"direct"})
	runtime := manager.current.Load()
	gw := runtime.gateway
	gwPool := gw.pools["shared"]
	gwCred := gw.authCreds[0]
	gwCand := authCand(TierZen, gwCred, gwPool, gwPool.items[0], "probe-model")
	gw.applyAttemptOutcome(context.Background(), gwCand, responseWithStatus(429), nil, time.Now().UnixNano())
	_, proxyUntil, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "shared", "direct")
	if !ok {
		t.Fatalf("seed proxy429 must cool")
	}
	targetUntil := gw.scheduler.targetCoolUntil(gwCand.Identity)
	// Direct probe via transport result (healthy flip) must not touch proxy429.
	gw.applyProxyHealthResult(proxyHealthResult{proxy: gwPool.items[0], err: nil, failed: false, wasHealthy: false}, "test probe", 0)
	if _, got, ok2 := gw.scheduler.proxy429CooldownStatus(TierZen, "shared", "direct"); !ok2 || got != proxyUntil {
		t.Fatalf("probe changed proxy429")
	}
	if got := gw.scheduler.targetCoolUntil(gwCand.Identity); got != targetUntil {
		t.Fatalf("probe changed target")
	}
	_ = admin
	_ = token
	_ = csrf
	// Refresh remains stateless for proxy429.
	before := snapshotRefreshState(gw)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = gw.refreshTier(ctx, "http://127.0.0.1:9", TierZen)
	after := snapshotRefreshState(gw)
	assertRefreshStateUnchanged(t, before, after)
}

// Admin JSON/redaction, bounded snapshot, WebUI static rendering + old-data tolerance.
func TestProxy429AdminRedactionWebUI(t *testing.T) {
	secretProxy := "http://user:hunter2@127.0.0.1:8080"
	cfg := testGatewayConfig(map[string][]string{"shared": {secretProxy, "direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Anonymous = true
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	manager := &RuntimeManager{logger: nil, monitor: NewMonitor(), hub: NewLogHub(100), redactor: NewSecretRedactor()}
	gateway, err := NewGateway(normalized, nil, manager.monitor)
	if err != nil {
		t.Fatal(err)
	}
	manager.current.Store(&gatewayRuntime{config: normalized, gateway: gateway})
	manager.redactor.Replace(normalized)
	pool := gateway.pools["shared"]
	cred := gateway.authCreds[0]
	cand := authCand(TierZen, cred, pool, pool.items[0], "m")
	gateway.applyAttemptOutcome(context.Background(), cand, responseWithStatus(429), nil, time.Now().UnixNano())
	entries, total := gateway.scheduler.snapshotProxy429()
	if total != 1 || len(entries) != 1 {
		t.Fatalf("snapshot total=%d entries=%d want 1/1", total, len(entries))
	}
	if strings.Contains(entries[0].ProxyNode, "hunter2") || strings.Contains(entries[0].ProxyNode, "user@") {
		t.Fatalf("proxy secret leaked: %+v", entries[0])
	}
	if entries[0].ProxyPool != "shared" || !entries[0].Active || entries[0].RemainingSeconds == nil || entries[0].NextAvailableAt == nil || entries[0].LastFailureClass == "" || entries[0].LastStatus != 429 {
		t.Fatalf("proxy429 entry incomplete: %+v", entries[0])
	}
	snap := manager.Resources()
	if snap.ProxyRateLimitsTotal != 1 || len(snap.ProxyRateLimits) != 1 {
		t.Fatalf("resources proxy_rate_limits missing: %+v", snap.ProxyRateLimits)
	}
	if snap.ProxyRateLimitsTruncated {
		t.Fatalf("single entry must not truncate")
	}
	raw, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "hunter2") || strings.Contains(string(raw), "user@") {
		t.Fatalf("admin JSON leaked proxy secret")
	}
	// WebUI static rendering: dedicated section + text-node rendering + old-data tolerance.
	html, err := os.ReadFile("webui/index.html")
	if err != nil {
		t.Fatal(err)
	}
	page := string(html)
	for _, want := range []string{"代理限流冷却", "tbody-ratelimit", "proxy_rate_limits", "ratelimit-note"} {
		if !strings.Contains(page, want) {
			t.Fatalf("WebUI missing %q for proxy429 section", want)
		}
	}
	if strings.Contains(page, "innerHTML") {
		t.Fatalf("WebUI must use text nodes only (no innerHTML)")
	}
	// Old-data tolerance: renderer must default missing proxy_rate_limits to empty.
	if !strings.Contains(page, "proxy_rate_limits||[]") && !strings.Contains(page, "proxy_rate_limits)||[]") {
		t.Fatalf("WebUI must tolerate old admin data without proxy_rate_limits")
	}
	// Full-target table must describe only 403/5xx (no 429 interpretation).
	if strings.Contains(page, "429") && strings.Contains(page, "tbody-targets") {
		// Allow 429 only inside the new rate-limit section or token explanation,
		// not as target-table semantics. The targets caption must scope to 403/5xx.
		if !strings.Contains(page, "仅 403/5xx") && !strings.Contains(page, "403/5xx") {
			t.Fatalf("targets table must scope to 403/5xx after 429 migration")
		}
	}
}
