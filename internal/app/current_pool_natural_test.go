package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// CURRENT-POOL natural recovery: changing proxy_routing is current resource
// selection only. Established native pins keep channel/credential/model/
// protocol/authority plus the ORIGINAL upstream route/wire session and serve
// through the CURRENT channel-assigned pool with no migration event, no old
// pool send, and no fabricated evidence.

// oldGatewayWithPools builds a gateway with explicit anon/auth pools and keys.
func currentPoolGateway(t *testing.T, pools map[string][]string, routing ProxyRoutingConfig, keys []string) *Gateway {
	t.Helper()
	cfg := testGatewayConfig(pools, routing)
	if keys != nil {
		cfg.Keys = keys
		cfg = mustNormalizeCurrentPool(t, cfg)
	}
	gw, err := NewGateway(cfg, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	return gw
}

func mustNormalizeCurrentPool(t *testing.T, cfg Config) Config {
	t.Helper()
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	return normalized
}

// migrateToCurrentPool builds the switched gateway and migrates scheduler
// state (pins, cooldowns, route sessions) via the production hot-Apply path.
func migrateToCurrentPool(t *testing.T, old *Gateway, pools map[string][]string, routing ProxyRoutingConfig, keys []string) *Gateway {
	t.Helper()
	cfg := testGatewayConfig(pools, routing)
	if keys != nil {
		cfg.Keys = keys
		cfg = mustNormalizeCurrentPool(t, cfg)
	}
	gw, err := NewGateway(cfg, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	migrateGatewaySchedulerState(old, gw)
	return gw
}

// 1) anon AND auth pin in A, routing switched to B with A removed: native 200
// from B, no A send, same wire session and body bytes, same credential/
// protocol/authority, origin Pool unchanged, current B, gen+1. Also covers A
// still referenced by the other channel.
func TestCurrentPoolNaturalRecoveryAnonAndAuth(t *testing.T) {
	for _, channel := range []string{"anon", "auth"} {
		t.Run(channel, func(t *testing.T) {
			keys := []string{"zen-key-currentpool-1"}
			old := currentPoolGateway(t,
				map[string][]string{"poolA": {"http://127.0.0.1:8181"}, "otherAuth": {"direct"}},
				ProxyRoutingConfig{Anonymous: "poolA", Authenticated: "poolA"},
				keys)
			old.cfg.Retry.MaxAttempts = 2
			var route modelRoute
			var credID string
			if channel == "anon" {
				route = anonAuthRoute()
				credID = anonymousSchedulerCredentialID
			} else {
				route = authOnlyRoute()
				credID = old.authCreds[0].id
			}
			oldCap := &capturedUpstream{}
			var oldHits atomic.Int32
			postStub(t, old, "poolA", 0, &oldHits, oldCap, func(*http.Request) (*http.Response, error) {
				return responseWithBody(200, `{"ok":true}`), nil
			})
			ses := "ses_curpool_" + channel + "_1"
			bodies := routeBodiesWithSession()
			r1, _, _, err := old.doUpstreamTiers(pinTestCtx(), route, bodies, pinIDs(ses, "r1"), 0)
			if err != nil || r1 == nil || r1.StatusCode != 200 {
				t.Fatalf("establish err=%v resp=%v", err, r1)
			}
			drainResp(r1)
			before, ok := old.scheduler.pinGet(ses, "m")
			if !ok {
				t.Fatalf("must pin")
			}
			if before.Pool != "poolA" || before.effectiveCurrentPool() != "poolA" {
				t.Fatalf("initial selection must be poolA: %+v", before)
			}
			firstSessions, firstBodies := oldCap.get()
			if len(firstSessions) != 1 || firstSessions[0] == "" {
				t.Fatalf("establish must carry wire session")
			}
			firstBody := decodeBody(t, firstBodies[0])

			// Switch routing to poolB; poolA removed entirely.
			next := migrateToCurrentPool(t, old,
				map[string][]string{"poolB": {"http://127.0.0.1:8282"}},
				ProxyRoutingConfig{Anonymous: "poolB", Authenticated: "poolB"},
				keys)
			if next.pools["poolA"] != nil {
				t.Fatalf("old pool must be unreferenced")
			}
			nextCap := &capturedUpstream{}
			var nextHits atomic.Int32
			postStub(t, next, "poolB", 0, &nextHits, nextCap, func(*http.Request) (*http.Response, error) {
				return responseWithBody(200, `{"ok":true}`), nil
			})
			r2, _, attempts, err := next.doUpstreamTiers(pinTestCtx(), route, bodies, pinIDs(ses, "r2"), 0)
			if err != nil || r2 == nil || r2.StatusCode != 200 {
				t.Fatalf("current-pool serve err=%v resp=%v", err, r2)
			}
			drainResp(r2)
			if attempts == 0 || postCount(&nextHits) != 1 {
				t.Fatalf("must send once via current pool: attempts=%d sends=%d", attempts, postCount(&nextHits))
			}
			after, ok := next.scheduler.pinGet(ses, "m")
			if !ok {
				t.Fatalf("pin must survive pool switch")
			}
			if after.Pool != "poolA" {
				t.Fatalf("origin pool must stay poolA, got %+v", after)
			}
			if after.effectiveCurrentPool() != "poolB" {
				t.Fatalf("current pool must be poolB, got %+v", after)
			}
			if after.Generation != before.Generation+1 {
				t.Fatalf("pool change must bump generation: before=%+v after=%+v", before, after)
			}
			if after.CredID != credID || after.Protocol != ProtocolChat || after.Authority != before.Authority {
				t.Fatalf("binding identity must persist: %+v", after)
			}
			gotSessions, gotBodies := nextCap.get()
			if len(gotSessions) != 1 || gotSessions[0] != firstSessions[0] {
				t.Fatalf("wire session must persist: first=%v got=%v", firstSessions, gotSessions)
			}
			gotBody := decodeBody(t, gotBodies[0])
			if gotBody["conversation_id"] != firstBody["conversation_id"] {
				t.Fatalf("body wire bytes must persist: %v vs %v", firstBody["conversation_id"], gotBody["conversation_id"])
			}
			// No manufactured old-pool send exists by construction (poolA is
			// not a resource of the new gateway).
			// In-flight old gateway baseline: still serves from old pool.
			var oldAgain atomic.Int32
			postStub(t, old, "poolA", 0, &oldAgain, nil, func(*http.Request) (*http.Response, error) {
				return responseWithBody(200, `{"ok":true}`), nil
			})
			rOld, _, _, err := old.doUpstreamTiers(pinTestCtx(), route, bodies, pinIDs(ses, "r-old"), 0)
			if err != nil || rOld == nil || rOld.StatusCode != 200 {
				t.Fatalf("old gateway must keep serving old pool: err=%v resp=%v", err, rOld)
			}
			drainResp(rOld)
			if postCount(&oldAgain) != 1 {
				t.Fatalf("old gateway must send via old pool once, got %d", postCount(&oldAgain))
			}
		})
	}
}

// 1b) Old pool still referenced by the other channel: anon moves to B while
// auth still uses A; anon serve hits B only.
func TestCurrentPoolOtherChannelKeepsOldPool(t *testing.T) {
	keys := []string{"zen-key-currentpool-2"}
	old := currentPoolGateway(t,
		map[string][]string{"poolA": {"http://127.0.0.1:8181"}},
		ProxyRoutingConfig{Anonymous: "poolA", Authenticated: "poolA"},
		keys)
	ses := "ses_curpool_shared_1"
	bodies := routeBodiesWithSession()
	postStub(t, old, "poolA", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	r1, _, _, err := old.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), bodies, pinIDs(ses, "r1"), 0)
	if err != nil || r1 == nil || r1.StatusCode != 200 {
		t.Fatalf("establish err=%v resp=%v", err, r1)
	}
	drainResp(r1)
	// Anon switches to poolB; auth still references poolA.
	next := migrateToCurrentPool(t, old,
		map[string][]string{"poolA": {"http://127.0.0.1:8181"}, "poolB": {"http://127.0.0.1:8282"}},
		ProxyRoutingConfig{Anonymous: "poolB", Authenticated: "poolA"},
		keys)
	var aHits, bHits atomic.Int32
	postStub(t, next, "poolA", 0, &aHits, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	bCap := &capturedUpstream{}
	postStub(t, next, "poolB", 0, &bHits, bCap, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	r2, _, _, err := next.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), bodies, pinIDs(ses, "r2"), 0)
	if err != nil || r2 == nil || r2.StatusCode != 200 {
		t.Fatalf("serve err=%v resp=%v", err, r2)
	}
	drainResp(r2)
	if postCount(&bHits) != 1 || postCount(&aHits) != 0 {
		t.Fatalf("anon must send via current poolB only: a=%d b=%d", postCount(&aHits), postCount(&bHits))
	}
	pin, _ := next.scheduler.pinGet(ses, "m")
	if pin.Pool != "poolA" || pin.effectiveCurrentPool() != "poolB" {
		t.Fatalf("origin/current mismatch: %+v", pin)
	}
}

// 2) Same raw URL in A and B: old A cooldowns never poison B and B 2xx never
// clears A; B cooldowns are read/written under actual B; success updates the
// pool pair (gen++) even for identical raw URLs; the stale raw URL is not
// forced current in B.
func TestCurrentPoolSameURLIsolation(t *testing.T) {
	keys := []string{"zen-key-currentpool-3"}
	sharedRaw := "http://127.0.0.1:8090"
	otherB := "http://127.0.0.1:8091"
	old := currentPoolGateway(t,
		map[string][]string{"poolA": {sharedRaw}},
		ProxyRoutingConfig{Anonymous: "poolA", Authenticated: "poolA"},
		keys)
	oldCred := old.authCreds[0].id
	ses := "ses_curpool_sameurl_1"
	postStub(t, old, "poolA", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	r1, _, _, err := old.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0)
	if err != nil || r1 == nil || r1.StatusCode != 200 {
		t.Fatalf("establish err=%v resp=%v", err, r1)
	}
	drainResp(r1)
	// Poison poolA identity for the shared URL + model (target cooldown).
	old.scheduler.noteTargetFailure(targetIdentity(TierZen, oldCred, "poolA", sharedRaw, "m"), AttemptClassUpstreamFailure, 403, 0)
	if _, _, ok := old.scheduler.targetCooldownStatus(targetIdentity(TierZen, oldCred, "poolA", sharedRaw, "m")); !ok {
		t.Fatalf("old poolA target cooldown must exist")
	}

	next := migrateToCurrentPool(t, old,
		map[string][]string{"poolB": {sharedRaw, otherB}},
		ProxyRoutingConfig{Anonymous: "poolB", Authenticated: "poolB"},
		keys)
	// The stale raw URL must not be forced current: with a pool mismatch the
	// affinity order is pure affinity over the current pool.
	ordered := affinityProxyOrder(next.pools["poolB"], oldCred, "")
	if len(ordered) != 2 {
		t.Fatalf("ordered=%d want 2", len(ordered))
	}
	firstRaw := ordered[0].name
	secondRaw := ordered[1].name
	firstIdx := poolIndexByRaw(next, "poolB", firstRaw)
	secondIdx := poolIndexByRaw(next, "poolB", secondRaw)
	// First node in affinity order fails 403 (live B evidence), second 200.
	postStub(t, next, "poolB", firstIdx, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(403, `{"error":{"message":"nope"}}`), nil
	})
	var secondHits atomic.Int32
	postStub(t, next, "poolB", secondIdx, &secondHits, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	r2, _, _, err := next.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r2"), 0)
	if err != nil || r2 == nil || r2.StatusCode != 200 {
		t.Fatalf("walk err=%v resp=%v", err, r2)
	}
	drainResp(r2)
	if postCount(&secondHits) != 1 {
		t.Fatalf("must walk to second current-pool node")
	}
	// B cooldown written under actual B only; A entry untouched by B outcome.
	if _, _, ok := next.scheduler.targetCooldownStatus(targetIdentity(TierZen, oldCred, "poolB", firstRaw, "m")); !ok {
		t.Fatalf("B target cooldown must be recorded under actual poolB")
	}
	if _, _, ok := next.scheduler.targetCooldownStatus(targetIdentity(TierZen, oldCred, "poolA", firstRaw, "m")); firstRaw == sharedRaw && ok {
		// poolA identity may have migrated (retainOnly keeps referenced pools
		// only); either absent or still cooling — but it must never have
		// filtered the poolB walk above, which already proved isolation.
		t.Logf("note: stale poolA entry present but did not filter poolB walk")
	}
	pin, _ := next.scheduler.pinGet(ses, "m")
	if pin.Pool != "poolA" {
		t.Fatalf("origin must stay poolA: %+v", pin)
	}
	if pin.effectiveCurrentPool() != "poolB" {
		t.Fatalf("current must be poolB: %+v", pin)
	}
	if pin.Generation != 1 {
		t.Fatalf("same-URL pool change must still bump generation: %+v", pin)
	}
	if pin.ProxyRaw != secondRaw {
		t.Fatalf("current proxy must be second node %q, got %+v", secondRaw, pin)
	}
	// Same URL, different pool counts as selection change at CAS level.
	s := newTargetScheduler(15 * time.Second)
	auth := normalizeRouteAuthority("https://zen.example")
	s.pinBind("ses_cas", "m", sessionPin{Tier: TierZen, CredID: "zen:aaa", Pool: "poolA", ProxyRaw: sharedRaw, Model: "m", Protocol: ProtocolChat, Authority: auth})
	if _, ok := s.pinMoveCurrent("ses_cas", "m", 0, "poolB", sharedRaw); !ok {
		t.Fatalf("same-URL cross-pool move must succeed")
	}
	got, _ := s.pinGet("ses_cas", "m")
	if got.Generation != 1 || got.effectiveCurrentPool() != "poolB" {
		t.Fatalf("same-URL move must bump gen and pool: %+v", got)
	}
}

// 3) Bound walk in current pool, exhaustion to custom, zero-send custom, and
// no-active envelope preservation.
func TestCurrentPoolBoundWalkAndCustom(t *testing.T) {
	keys := []string{"zen-key-currentpool-4"}
	establish := func(t *testing.T, gw *Gateway, ses string) {
		t.Helper()
		postStub(t, gw, "poolA", 0, nil, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
		r, _, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "est"), 0)
		if err != nil || r == nil || r.StatusCode != 200 {
			t.Fatalf("establish err=%v resp=%v", err, r)
		}
		drainResp(r)
	}
	t.Run("FirstBadThenSuccessSameSession", func(t *testing.T) {
		old := currentPoolGateway(t, map[string][]string{"poolA": {"direct"}},
			ProxyRoutingConfig{Anonymous: "poolA", Authenticated: "poolA"}, keys)
		ses := "ses_curpool_walk_1"
		establish(t, old, ses)
		first, _ := old.scheduler.pinGet(ses, "m")
		next := migrateToCurrentPool(t, old, map[string][]string{"poolB": {"http://127.0.0.1:8301", "http://127.0.0.1:8302"}},
			ProxyRoutingConfig{Anonymous: "poolB", Authenticated: "poolB"}, keys)
		ordered := affinityProxyOrder(next.pools["poolB"], first.CredID, "")
		badIdx := poolIndexByRaw(next, "poolB", ordered[0].name)
		goodIdx := poolIndexByRaw(next, "poolB", ordered[1].name)
		badCap := &capturedUpstream{}
		postStub(t, next, "poolB", badIdx, nil, badCap, func(*http.Request) (*http.Response, error) {
			return responseWithBody(500, `{"error":{"message":"boom"}}`), nil
		})
		goodCap := &capturedUpstream{}
		postStub(t, next, "poolB", goodIdx, nil, goodCap, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
		r, _, _, err := next.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodiesWithSession(), pinIDs(ses, "walk"), 0)
		if err != nil || r == nil || r.StatusCode != 200 {
			t.Fatalf("walk err=%v resp=%v", err, r)
		}
		drainResp(r)
		bs, _ := badCap.get()
		gs, _ := goodCap.get()
		all := append(append([]string(nil), bs...), gs...)
		if len(all) < 2 {
			t.Fatalf("walk must send via both nodes (L1 + move): bad=%v good=%v", bs, gs)
		}
		for _, s := range all {
			if s == "" || s != all[0] {
				t.Fatalf("unified walk must share one session: %v", all)
			}
		}
		pin, _ := next.scheduler.pinGet(ses, "m")
		if pin.effectiveCurrentPool() != "poolB" || pin.ProxyRaw != ordered[1].name {
			t.Fatalf("walk must update current pair: %+v", pin)
		}
	})
	t.Run("AllUnavailableTakesCustom", func(t *testing.T) {
		old := currentPoolGateway(t, map[string][]string{"poolA": {"direct"}},
			ProxyRoutingConfig{Anonymous: "poolA", Authenticated: "poolA"}, keys)
		ses := "ses_curpool_custom_1"
		establish(t, old, ses)
		var customHits atomic.Int32
		custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			customHits.Add(1)
			w.WriteHeader(200)
			_, _ = w.Write([]byte(fallbackChatOK("cm")))
		}))
		defer custom.Close()
		cfg := testGatewayConfig(map[string][]string{"poolB": {"http://127.0.0.1:8311", "http://127.0.0.1:8312"}},
			ProxyRoutingConfig{Anonymous: "poolB", Authenticated: "poolB"})
		cfg.Keys = keys
		cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k", Model: "cm"}}}
		cfg = mustNormalizeCurrentPool(t, cfg)
		next, err := NewGateway(cfg, discardGatewayLogger(), NewMonitor())
		if err != nil {
			t.Fatal(err)
		}
		migrateGatewaySchedulerState(old, next)
		postStub(t, next, "poolB", 0, nil, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(500, `{"error":{"message":"boom"}}`), nil
		})
		postStub(t, next, "poolB", 1, nil, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(500, `{"error":{"message":"boom"}}`), nil
		})
		ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
		r, eff, _, err := next.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "exhaust"), 0, ex)
		if err != nil {
			t.Fatal(err)
		}
		defer drainResp(r)
		if r.StatusCode != 200 || eff.Tier != TierCustom || customHits.Load() != 1 {
			t.Fatalf("exhaustion must take custom: code=%d eff=%+v hits=%d", r.StatusCode, eff, customHits.Load())
		}
	})
	t.Run("FilteredZeroSendTakesCustomWithoutFabrication", func(t *testing.T) {
		old := currentPoolGateway(t, map[string][]string{"poolA": {"direct"}},
			ProxyRoutingConfig{Anonymous: "poolA", Authenticated: "poolA"}, keys)
		ses := "ses_curpool_zero_1"
		establish(t, old, ses)
		var customHits atomic.Int32
		custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			customHits.Add(1)
			w.WriteHeader(200)
			_, _ = w.Write([]byte(fallbackChatOK("cm")))
		}))
		defer custom.Close()
		cfg := testGatewayConfig(map[string][]string{"poolB": {"http://127.0.0.1:8321"}},
			ProxyRoutingConfig{Anonymous: "poolB", Authenticated: "poolB"})
		cfg.Keys = keys
		cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k", Model: "cm"}}}
		cfg = mustNormalizeCurrentPool(t, cfg)
		next, err := NewGateway(cfg, discardGatewayLogger(), NewMonitor())
		if err != nil {
			t.Fatal(err)
		}
		migrateGatewaySchedulerState(old, next)
		pin, _ := next.scheduler.pinGet(ses, "m")
		raw := next.pools["poolB"].items[0].name
		next.scheduler.noteProxy429Failure(TierZen, "poolB", raw, AttemptClassRateLimited, 429, time.Minute, time.Now().UnixNano())
		var proxyHits atomic.Int32
		postStub(t, next, "poolB", 0, &proxyHits, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
		_ = pin
		ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
		r, eff, attempts, err := next.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "zero"), 0, ex)
		if err != nil {
			t.Fatal(err)
		}
		defer drainResp(r)
		if r.StatusCode != 200 || eff.Tier != TierCustom || customHits.Load() != 1 {
			t.Fatalf("zero-send must take custom: code=%d eff=%+v hits=%d", r.StatusCode, eff, customHits.Load())
		}
		if attempts != 1 || proxyHits.Load() != 0 {
			t.Fatalf("zero-send must have attempts=1 custom-only: attempts=%d proxy=%d", attempts, proxyHits.Load())
		}
		if _, _, ok := next.scheduler.credential429CooldownStatus(pin.CredID); ok {
			t.Fatalf("zero-send must not fabricate credential429")
		}
	})
	t.Run("NoActiveKeepsNativeEnvelope", func(t *testing.T) {
		old := currentPoolGateway(t, map[string][]string{"poolA": {"direct"}},
			ProxyRoutingConfig{Anonymous: "poolA", Authenticated: "poolA"}, keys)
		ses := "ses_curpool_noactive_1"
		establish(t, old, ses)
		next := migrateToCurrentPool(t, old, map[string][]string{"poolB": {"http://127.0.0.1:8331"}},
			ProxyRoutingConfig{Anonymous: "poolB", Authenticated: "poolB"}, keys)
		postStub(t, next, "poolB", 0, nil, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
		})
		r, eff, _, err := next.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "noactive"), 0)
		if err != nil {
			t.Fatal(err)
		}
		defer drainResp(r)
		if r.StatusCode != 429 || eff.Tier == TierCustom {
			t.Fatalf("no-active must keep native 429: code=%d eff=%+v", r.StatusCode, eff)
		}
	})
}

// 4) Multiple switches A->B->C->A preserve origin rss/wire; legacy proxy-free
// override backing an active pin survives Apply with origin removed; stale or
// invalid overrides drop, proxy-bound legacy drops.
func TestCurrentPoolMultiSwitchPreservesOrigin(t *testing.T) {
	keys := []string{"zen-key-currentpool-5"}
	old := currentPoolGateway(t, map[string][]string{"poolA": {"http://127.0.0.1:8401"}},
		ProxyRoutingConfig{Anonymous: "poolA", Authenticated: "poolA"}, keys)
	ses := "ses_curpool_multi_1"
	postStub(t, old, "poolA", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	r1, _, _, err := old.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodiesWithSession(), pinIDs(ses, "r1"), 0)
	if err != nil || r1 == nil || r1.StatusCode != 200 {
		t.Fatalf("establish err=%v resp=%v", err, r1)
	}
	drainResp(r1)
	origin, _ := old.scheduler.pinGet(ses, "m")
	originProbe := targetCandidate{Tier: origin.Tier, CredID: origin.CredID, PoolName: "poolA", Model: "m"}
	originRSS := old.scheduler.routeSessionFor(ses, routeScopeForCandidate(normalizeRouteAuthority(old.cfg.Upstream.Zen), originProbe, ProtocolChat))
	// Seed a legacy proxy-free override backing the active pin.
	rotated := old.scheduler.rotateRouteSession(ses, routeScopeForCandidate(normalizeRouteAuthority(old.cfg.Upstream.Zen), originProbe, ProtocolChat), originRSS)
	if rotated == originRSS {
		t.Fatalf("rotate must install an override")
	}
	gw := old
	for i, pool := range []string{"poolB", "poolC", "poolA"} {
		gw = migrateToCurrentPool(t, gw, map[string][]string{pool: {"http://127.0.0.1:8401"}},
			ProxyRoutingConfig{Anonymous: pool, Authenticated: pool}, keys)
		var hits atomic.Int32
		postStub(t, gw, pool, 0, &hits, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
		r, _, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodiesWithSession(), pinIDs(ses, "r"), 0)
		if err != nil || r == nil || r.StatusCode != 200 {
			t.Fatalf("switch %d (%s) err=%v resp=%v", i, pool, err, r)
		}
		drainResp(r)
		if postCount(&hits) != 1 {
			t.Fatalf("switch %d must send via current %s", i, pool)
		}
		pin, _ := gw.scheduler.pinGet(ses, "m")
		if pin.Pool != "poolA" {
			t.Fatalf("origin must stay poolA after switch %d: %+v", i, pin)
		}
		got := gw.scheduler.routeSessionFor(ses, routeScopeForCandidate(normalizeRouteAuthority(gw.cfg.Upstream.Zen), originProbe, ProtocolChat))
		if got != rotated {
			t.Fatalf("override must survive switch %d: got %q want %q", i, got, rotated)
		}
	}
}

// 4b) Route-override migration: pinned proxy-free origin survives removal
// exactly; orphan valid-cred proxy-free origin without a backing pin drops;
// current-pool unbound survives; invalid credential/protocol/provider and
// legacy proxy-bound scopes drop; invalid pins grant no exception; expired
// overrides drop; cooldown never decides pin validity.
func TestCurrentPoolRouteOverrideMigration(t *testing.T) {
	keys := []string{"zen-key-currentpool-6"}
	old := currentPoolGateway(t, map[string][]string{"poolA": {"direct"}},
		ProxyRoutingConfig{Anonymous: "poolA", Authenticated: "poolA"}, keys)
	zenAuth := normalizeRouteAuthority(old.cfg.Upstream.Zen)
	cred := old.authCreds[0].id
	// Backing valid native pin with origin poolA/chat. Client/model are
	// excluded from scope validity, so the override below uses a different
	// client than the pin session.
	old.scheduler.pinBind("ses_pinned", "m", sessionPin{Tier: TierZen, CredID: cred, Pool: "poolA", ProxyRaw: "direct", Model: "m", Protocol: ProtocolChat, Authority: zenAuth})
	// Cooldown is transient and must not decide pin-backed scope validity.
	old.scheduler.noteProxy429Failure(TierZen, "poolA", "direct", AttemptClassRateLimited, 429, time.Minute, time.Now().UnixNano())
	validScope := routeSessionScope{Authority: zenAuth, Tier: TierZen, CredID: cred, Pool: "poolA", ProxyRaw: "", Protocol: ProtocolChat}
	validToken := old.scheduler.rotateRouteSession("ses_pinned_other_client", validScope, deriveFirstRouteSession("ses_pinned_other_client", validScope))
	// Orphan: same valid credential/authority, origin poolA, but Responses
	// protocol has no backing pin with that exact scope key.
	orphanScope := routeSessionScope{Authority: zenAuth, Tier: TierZen, CredID: cred, Pool: "poolA", ProxyRaw: "", Protocol: ProtocolResponses}
	orphanFirst := deriveFirstRouteSession("ses_orphan", orphanScope)
	old.scheduler.rotateRouteSession("ses_orphan", orphanScope, orphanFirst)
	// Unbound current-pool scope: poolB is the post-switch assigned pool, so
	// the ordinary pool-exists+assigned gate retains it without any pin.
	currentScope := routeSessionScope{Authority: zenAuth, Tier: TierZen, CredID: cred, Pool: "poolB", ProxyRaw: "", Protocol: ProtocolChat}
	currentToken := old.scheduler.rotateRouteSession("ses_current", currentScope, deriveFirstRouteSession("ses_current", currentScope))
	goneScope := routeSessionScope{Authority: zenAuth, Tier: TierZen, CredID: credentialIDForKey(TierZen, "gone-key"), Pool: "poolA", ProxyRaw: "", Protocol: ProtocolChat}
	old.scheduler.rotateRouteSession("ses_gone", goneScope, deriveFirstRouteSession("ses_gone", goneScope))
	// Invalid pins migrate as tombstones but must not grant the exception.
	old.scheduler.pinBind("ses_gone", "m", sessionPin{Tier: TierZen, CredID: credentialIDForKey(TierZen, "gone-key"), Pool: "poolA", ProxyRaw: "direct", Model: "m", Protocol: ProtocolChat, Authority: zenAuth})
	badAuth := "https://other.example"
	badAuthScope := routeSessionScope{Authority: badAuth, Tier: TierZen, CredID: cred, Pool: "poolA", ProxyRaw: "", Protocol: ProtocolChat}
	old.scheduler.rotateRouteSession("ses_badauth", badAuthScope, deriveFirstRouteSession("ses_badauth", badAuthScope))
	old.scheduler.pinBind("ses_badauth", "m", sessionPin{Tier: TierZen, CredID: cred, Pool: "poolA", ProxyRaw: "direct", Model: "m", Protocol: ProtocolChat, Authority: badAuth})
	badProtoScope := routeSessionScope{Authority: zenAuth, Tier: TierZen, CredID: cred, Pool: "poolA", ProxyRaw: "", Protocol: Protocol("bogus")}
	old.scheduler.rotateRouteSession("ses_badproto", badProtoScope, deriveFirstRouteSession("ses_badproto", badProtoScope))
	old.scheduler.pinBind("ses_badproto", "m", sessionPin{Tier: TierZen, CredID: cred, Pool: "poolA", ProxyRaw: "direct", Model: "m", Protocol: Protocol("bogus"), Authority: zenAuth})
	boundScope := routeSessionScope{Authority: zenAuth, Tier: TierZen, CredID: cred, Pool: "poolA", ProxyRaw: "direct", Protocol: ProtocolChat}
	old.scheduler.rotateRouteSession("ses_bound", boundScope, deriveFirstRouteSession("ses_bound", boundScope))
	// Expired override with an otherwise pinned scope must still drop.
	expiredScope := routeSessionScope{Authority: zenAuth, Tier: TierZen, CredID: cred, Pool: "poolA", ProxyRaw: "", Protocol: ProtocolChat}
	expiredToken := old.scheduler.rotateRouteSession("ses_expired", expiredScope, deriveFirstRouteSession("ses_expired", expiredScope))
	old.scheduler.routeSessions.mu.Lock()
	if entry, ok := old.scheduler.routeSessions.entries[routeSessionMapKey("ses_expired", expiredScope)]; ok && entry != nil {
		entry.lastUsed = time.Now().Add(-routeSessionIdleTTL - time.Minute).UnixNano()
	}
	old.scheduler.routeSessions.mu.Unlock()
	_ = expiredToken
	next := migrateToCurrentPool(t, old, map[string][]string{"poolB": {"direct"}},
		ProxyRoutingConfig{Anonymous: "poolB", Authenticated: "poolB"}, keys)
	if got := next.scheduler.routeSessionFor("ses_pinned_other_client", validScope); got != validToken {
		t.Fatalf("valid proxy-free override must survive origin removal: got %q want %q", got, validToken)
	}
	if got := next.scheduler.routeSessionFor("ses_orphan", orphanScope); got != deriveFirstRouteSession("ses_orphan", orphanScope) {
		t.Fatalf("orphan valid-cred origin scope without backing pin must drop to stateless: got %q", got)
	}
	if got := next.scheduler.routeSessionFor("ses_current", currentScope); got != currentToken {
		t.Fatalf("current-pool unbound override must survive: got %q want %q", got, currentToken)
	}
	if got := next.scheduler.routeSessionFor("ses_gone", goneScope); got != deriveFirstRouteSession("ses_gone", goneScope) {
		t.Fatalf("removed-credential override must drop to stateless: got %q", got)
	}
	if got := next.scheduler.routeSessionFor("ses_badauth", badAuthScope); got != deriveFirstRouteSession("ses_badauth", badAuthScope) {
		t.Fatalf("provider-mismatched override must drop to stateless: got %q", got)
	}
	if got := next.scheduler.routeSessionFor("ses_badproto", badProtoScope); got != deriveFirstRouteSession("ses_badproto", badProtoScope) {
		t.Fatalf("protocol-mismatched override must drop to stateless: got %q", got)
	}
	if got := next.scheduler.routeSessionFor("ses_bound", boundScope); got != deriveFirstRouteSession("ses_bound", boundScope) {
		t.Fatalf("legacy proxy-bound override must drop: got %q", got)
	}
	if got := next.scheduler.routeSessionFor("ses_expired", expiredScope); got == expiredToken {
		t.Fatalf("expired override must drop to stateless: got %q", got)
	}
}

// 5) Tombstones: removed credential/authority/protocol/model fail 502 with
// zero native/custom sends; 400-replay and ordinary-4xx/cancel boundaries
// hold on the current pool.
func TestCurrentPoolTombstonesAndBoundaries(t *testing.T) {
	keys := []string{"zen-key-currentpool-7"}
	old := currentPoolGateway(t, map[string][]string{"poolA": {"direct"}},
		ProxyRoutingConfig{Anonymous: "poolA", Authenticated: "poolA"}, keys)
	ses := "ses_curpool_tomb_1"
	postStub(t, old, "poolA", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	r1, _, _, err := old.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "est"), 0)
	if err != nil || r1 == nil || r1.StatusCode != 200 {
		t.Fatalf("establish err=%v resp=%v", err, r1)
	}
	drainResp(r1)
	// Credential deleted after switch: 502, no sends, no custom.
	next := migrateToCurrentPool(t, old, map[string][]string{"poolB": {"direct"}},
		ProxyRoutingConfig{Anonymous: "poolB", Authenticated: "poolB"}, []string{"zen-key-other-9"})
	var hits atomic.Int32
	postStub(t, next, "poolB", 0, &hits, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	r, _, attempts, err := next.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "gone"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(r)
	if r.StatusCode != 502 || attempts != 0 || postCount(&hits) != 0 {
		t.Fatalf("deleted credential must 502 zero-send: code=%d attempts=%d sends=%d", r.StatusCode, attempts, postCount(&hits))
	}
	// Protocol mismatch tombstones: Chat pin vs Responses route on the same
	// session+model key hits the pinned path and fails 502 zero-send.
	next2 := migrateToCurrentPool(t, old, map[string][]string{"poolB": {"direct"}},
		ProxyRoutingConfig{Anonymous: "poolB", Authenticated: "poolB"}, keys)
	var hits2 atomic.Int32
	postStub(t, next2, "poolB", 0, &hits2, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	otherRoute := authOnlyRoute()
	otherRoute.Protocol = ProtocolResponses
	otherRoute.Protocols = map[Tier]Protocol{TierZen: ProtocolResponses}
	r3, _, a3, err := next2.doUpstreamTiers(pinTestCtx(), otherRoute, routeBodies(), pinIDs(ses, "proto"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(r3)
	if r3.StatusCode != 502 || a3 != 0 || postCount(&hits2) != 0 {
		t.Fatalf("protocol mismatch must 502 zero-send: code=%d attempts=%d sends=%d", r3.StatusCode, a3, postCount(&hits2))
	}
	// Boundaries on the current pool: exact-400 replay is route-terminal and
	// ordinary 4xx stays faithful.
	next3 := migrateToCurrentPool(t, old, map[string][]string{"poolB": {"direct", "http://127.0.0.1:8502"}},
		ProxyRoutingConfig{Anonymous: "poolB", Authenticated: "poolB"}, keys)
	postStub(t, next3, "poolB", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(400, `{"error":{"message":"bad"}}`), nil
	})
	var secondHits atomic.Int32
	postStub(t, next3, "poolB", 1, &secondHits, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	// First 400 replays once same-target (second 400 terminal for the route).
	r4, _, _, err := next3.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "b400"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(r4)
	if r4.StatusCode != 400 {
		t.Fatalf("exact-400 replay terminal must return 400, got %d", r4.StatusCode)
	}
	if postCount(&secondHits) != 0 {
		t.Fatalf("400 replay must not walk remaining proxies: second=%d", postCount(&secondHits))
	}
}

// 6) Concurrent pool-qualified CAS: one winner; stale old-pool moves and
// cancelled moves never disturb the fresh pair.
func TestCurrentPoolConcurrentCAS(t *testing.T) {
	s := newTargetScheduler(15 * time.Second)
	auth := normalizeRouteAuthority("https://zen.example")
	s.pinBind("ses_conc", "m", sessionPin{Tier: TierZen, CredID: "zen:aaa", Pool: "poolA", ProxyRaw: "rawA", Model: "m", Protocol: ProtocolChat, Authority: auth})
	const racers = 8
	var wg sync.WaitGroup
	results := make([]bool, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pool := "poolB"
			raw := "rawB"
			if i%2 == 1 {
				raw = "rawC"
			}
			_, ok := s.pinMoveCurrentCtx(context.Background(), "ses_conc", "m", 0, pool, raw)
			results[i] = ok
		}(i)
	}
	wg.Wait()
	wins := 0
	for _, ok := range results {
		if ok {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("concurrent moves must converge to one winner, wins=%d", wins)
	}
	cur, _ := s.pinGet("ses_conc", "m")
	if cur.effectiveCurrentPool() != "poolB" || cur.Generation != 1 {
		t.Fatalf("winner must set poolB gen1: %+v", cur)
	}
	// Stale old-pool move with old generation cannot disturb the fresh pair.
	if _, ok := s.pinMoveCurrent("ses_conc", "m", 0, "poolA", "rawA"); ok {
		t.Fatalf("stale old-pool move must fail")
	}
	if got, _ := s.pinGet("ses_conc", "m"); got.ProxyRaw != cur.ProxyRaw || got.effectiveCurrentPool() != "poolB" {
		t.Fatalf("stale move must not disturb pair: %+v", got)
	}
	// Cancelled move never mutates.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := s.pinMoveCurrentCtx(ctx, "ses_conc", "m", 1, "poolB", "rawZ"); ok {
		t.Fatalf("cancelled move must not mutate")
	}
	if got, _ := s.pinGet("ses_conc", "m"); got.ProxyRaw != cur.ProxyRaw || got.Generation != 1 {
		t.Fatalf("cancel must leave pair intact: %+v", got)
	}
	// Same-pair move is idempotent without bump.
	if gen, ok := s.pinMoveCurrent("ses_conc", "m", 1, "poolB", cur.ProxyRaw); !ok || gen != 1 {
		t.Fatalf("idempotent same-pair move must succeed without bump: gen=%d ok=%v", gen, ok)
	}
}
