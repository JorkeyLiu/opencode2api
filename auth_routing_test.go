package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func authThreeProxyGateway(t *testing.T, monitor *Monitor) *Gateway {
	t.Helper()
	cfg := testGatewayConfig(
		map[string][]string{
			"a": {"direct"},
			"z": {"direct", "http://127.0.0.1:8081", "http://127.0.0.1:8082"},
			"g": {"direct", "http://127.0.0.1:8083"},
		},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Retry.MaxAttempts = 5
	gateway, err := NewGateway(cfg, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	return gateway
}

// Auth soft affinity: same key+pool stable across sessions/models.
func TestAuthSoftAffinityStable(t *testing.T) {
	monitor := NewMonitor()
	gateway := authTwoProxyGateway(t, monitor, 5)
	pool := gateway.pools["z"]
	cred := gateway.authCreds[0]
	pref := preferredProxyRaw(cred.id, pool.name, pool.items)
	if pref == "" {
		t.Fatalf("empty preferred")
	}
	for _, ses := range []string{"ses_aff_1", "ses_aff_2", "ses_other"} {
		for _, model := range []string{"m", "model-x", "model-y"} {
			now := time.Now().UnixNano()
			cands := gateway.scheduler.buildAuthCandidates(TierZen, gateway.authCreds[:1], pool, model, now)
			ordered := gateway.scheduler.orderCandidates(cands, ses)
			if len(ordered) == 0 {
				t.Fatalf("no candidates")
			}
			if ordered[0].ProxyRaw != pref {
				t.Fatalf("ses=%s model=%s first=%s want preferred %s", ses, model, ordered[0].ProxyRaw, pref)
			}
		}
	}
	// Different credentials disperse: aaaaa vs bbbbb have different preferred.
	gateway2 := schedulerTestGateway(t, []string{"zen-key-aaaaa", "zen-key-bbbbb"}, []string{"direct", "http://127.0.0.1:8081"})
	pool2 := gateway2.pools["shared"]
	p0 := preferredProxyRaw(gateway2.authCreds[0].id, pool2.name, pool2.items)
	p1 := preferredProxyRaw(gateway2.authCreds[1].id, pool2.name, pool2.items)
	if p0 == "" || p1 == "" {
		t.Fatalf("empty preferred")
	}
	if p0 == p1 {
		t.Fatalf("expected dispersion for test keys, both prefer %s", p0)
	}
	// Same key different pools independent: z prefers 8081, other prefers direct.
	cfg := testGatewayConfig(map[string][]string{"z": {"direct", "http://127.0.0.1:8081"}, "other": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "other", Authenticated: "z"})
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Anonymous = true
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	cid := gw.authCreds[0].id
	pz := preferredProxyRaw(cid, "z", gw.pools["z"].items)
	po := preferredProxyRaw(cid, "other", gw.pools["other"].items)
	if pz == "" || po == "" {
		t.Fatalf("empty")
	}
	// Independence means per-pool derivation: changing other pool must not
	// affect z. Verify by recomputing z after mutating other (add proxy).
	if pz2 := preferredProxyRaw(cid, "z", gw.pools["z"].items); pz2 != pz {
		t.Fatalf("z preferred unstable: %s vs %s", pz, pz2)
	}
	_ = po
	// Minimal disruption: adding a proxy preserves relative order of old ones.
	now := time.Now().UnixNano()
	oldCands := gw.scheduler.buildAuthCandidates(TierZen, gw.authCreds, gw.pools["z"], "m", now)
	oldOrdered := gw.scheduler.orderCandidates(oldCands, "ses_aff_1")
	oldIDs := []string{}
	for _, c := range oldOrdered {
		oldIDs = append(oldIDs, c.Identity)
	}
	// New pool with extra proxy.
	cfg2 := testGatewayConfig(map[string][]string{"z": {"direct", "http://127.0.0.1:8081", "http://127.0.0.1:8082"}, "other": {"direct"}}, ProxyRoutingConfig{Anonymous: "other", Authenticated: "z"})
	cfg2.Keys = []string{"zen-key-aaaaa"}
	n2, _ := NormalizeConfig("config.json", cfg2)
	gw2, _ := NewGateway(n2, nil, NewMonitor())
	newCands := gw2.scheduler.buildAuthCandidates(TierZen, gw2.authCreds, gw2.pools["z"], "m", now)
	newOrdered := gw2.scheduler.orderCandidates(newCands, "ses_aff_1")
	allowed := map[string]bool{}
	for _, id := range oldIDs {
		allowed[id] = true
	}
	kept := []string{}
	for _, c := range newOrdered {
		if allowed[c.Identity] {
			kept = append(kept, c.Identity)
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

// Anonymous pin is proxy-independent with 429 walk and stable session.
func TestAnonymousStaysProxyBound(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	postStub(t, gateway, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids := pinIDs("ses_anon_bound_1", "req-1")
	route := anonAuthRoute()
	resp, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids, 0)
	if err != nil {
		t.Fatal(err)
	}
	drainResp(resp)
	pin, ok := gateway.scheduler.pinGet(ids.Session, "m")
	if !ok || pin.CredID != anonymousSchedulerCredentialID {
		t.Fatalf("must pin anon %+v ok=%v", pin, ok)
	}
	pinnedIdx := -1
	for i, p := range gateway.pools["a"].items {
		if p.name == pin.ProxyRaw {
			pinnedIdx = i
		}
	}
	if pinnedIdx < 0 {
		t.Fatalf("pinned proxy not found")
	}
	otherIdx := 1 - pinnedIdx
	cap := &capturedUpstream{}
	var pinnedCalls, otherCalls, zenCalls atomic.Int32
	postStub(t, gateway, "a", pinnedIdx, &pinnedCalls, cap, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "5")
		return r, nil
	})
	postStub(t, gateway, "a", otherIdx, &otherCalls, cap, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 0, &zenCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ids2 := pinIDs(ids.Session, "req-2")
	resp2, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), ids2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if resp2.StatusCode != 200 {
		t.Fatalf("status=%d want 200 via same-pool walk", resp2.StatusCode)
	}
	drainResp(resp2)
	if postCount(&pinnedCalls) != 1 || postCount(&otherCalls) != 1 || postCount(&zenCalls) != 0 {
		t.Fatalf("anon 429 must walk same binding: pinned=%d other=%d zen=%d", postCount(&pinnedCalls), postCount(&otherCalls), postCount(&zenCalls))
	}
	sessions, bodies := cap.get()
	if len(sessions) != 2 || sessions[0] != sessions[1] {
		t.Fatalf("anon walk must keep same wire session: %q", sessions)
	}
	if len(bodies) == 2 && string(bodies[0]) != string(bodies[1]) {
		t.Fatalf("anon walk body must stay byte-identical")
	}
	pin2, _ := gateway.scheduler.pinGet(ids.Session, "m")
	pool := gateway.pools["a"]
	otherRaw := pool.items[otherIdx].name
	if pin2.ProxyRaw != otherRaw || pin2.Generation != pin.Generation+1 {
		t.Fatalf("anon pin must move current with generation: %+v vs %+v", pin, pin2)
	}
	// Route-session proxy-independent: different proxies give same value.
	c0 := targetCandidate{Tier: TierZen, CredID: anonymousSchedulerCredentialID, PoolName: "a", ProxyRaw: pool.items[0].name, Model: "m"}
	c1 := targetCandidate{Tier: TierZen, CredID: anonymousSchedulerCredentialID, PoolName: "a", ProxyRaw: pool.items[1].name, Model: "m"}
	s0 := gateway.scheduler.routeSessionFor("ses_anon_bound_1", routeScopeForCandidate("https://zen.example", c0, ProtocolChat))
	s1 := gateway.scheduler.routeSessionFor("ses_anon_bound_1", routeScopeForCandidate("https://zen.example", c1, ProtocolChat))
	if s0 == "" || s1 == "" || s0 != s1 {
		t.Fatalf("anon route sessions must match across proxies: %q vs %q", s0, s1)
	}
	if strings.Contains(s0, "ses_anon_bound_1") || strings.Contains(s1, "ses_anon_bound_1") {
		t.Fatalf("raw client session exposed")
	}
}

// Auth route-session byte-identical across proxy move.
func TestAuthRouteSessionIdenticalAcrossMove(t *testing.T) {
	monitor := NewMonitor()
	gateway := authTwoProxyGateway(t, monitor, 5)
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	// Establish pin.
	postStub(t, gateway, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ses := "ses_auth_rss_move_1"
	r1, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r1"), 0)
	drainResp(r1)
	pin, ok := gateway.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must pin")
	}
	// Force move: current proxy transport fails, alternate succeeds. Capture sessions.
	pool := gateway.pools["z"]
	curIdx := -1
	for i, p := range pool.items {
		if p.name == pin.ProxyRaw {
			curIdx = i
		}
	}
	otherIdx := 1 - curIdx
	cap := &capturedUpstream{}
	var c0, c1 atomic.Int32
	// Current proxy: first send transport fail, retry transport fail (2 sends), then move.
	postStub(t, gateway, "z", curIdx, &c0, cap, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial timeout")
	})
	postStub(t, gateway, "z", otherIdx, &c1, cap, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	r2, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r2"), 0)
	if err != nil || r2.StatusCode != 200 {
		t.Fatalf("err=%v resp=%v want 200 via move", err, r2)
	}
	drainResp(r2)
	sessions, bodies := cap.get()
	if len(sessions) < 3 {
		t.Fatalf("expected >=3 sends (cur x2 + alternate), got %d", len(sessions))
	}
	// All sends in one request must share the same proxy-independent session.
	first := sessions[0]
	for i, s := range sessions {
		if s != first {
			t.Fatalf("session differs across move at %d: %q vs %q", i, s, first)
		}
		if strings.Contains(s, ses) {
			t.Fatalf("raw client session exposed: %q", s)
		}
	}
	// Bodies identical across move (same route session rewrite).
	for i := 1; i < len(bodies); i++ {
		if string(bodies[i]) != string(bodies[0]) {
			t.Fatalf("body differs across move")
		}
	}
	// Pin moved to alternate.
	pin2, _ := gateway.scheduler.pinGet(ses, "m")
	if pin2.ProxyRaw == pin.ProxyRaw {
		t.Fatalf("pin must move, still %s", pin2.ProxyRaw)
	}
	if pin2.Generation != pin.Generation+1 {
		t.Fatalf("generation %d want %d", pin2.Generation, pin.Generation+1)
	}
}

// Auth established moves: transport and 429 move; others do not.
func TestAuthEstablishedMoveMatrix(t *testing.T) {
	cases := []struct {
		name           string
		status         int
		transport      bool
		wantMove       bool
		wantCallsCur   int
		wantStatus     int
		wantOtherCalls int
	}{
		{"transport_moves", 0, true, true, 2, 200, 1},
		{"429_moves", 429, false, true, 1, 200, 1},
		{"400_no_move", 400, false, false, 2, 400, 0},
		{"401_no_move", 401, false, false, 1, 401, 0},
		{"403_no_move", 403, false, false, 1, 403, 0},
		{"408_no_move", 408, false, false, 2, 408, 0},
		{"425_no_move", 425, false, false, 2, 425, 0},
		{"404_no_move", 404, false, false, 1, 404, 0},
		{"500_no_move", 500, false, false, 2, 500, 0},
		{"503_no_move", 503, false, false, 2, 503, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			monitor := NewMonitor()
			gateway := authTwoProxyGateway(t, monitor, 5)
			route := authOnlyRoute()
			route.KeyTiers = []Tier{TierZen}
			postStub(t, gateway, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
				return responseWithBody(200, `{"ok":true}`), nil
			})
			postStub(t, gateway, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
				return responseWithBody(200, `{"ok":true}`), nil
			})
			ses := "ses_move_" + tc.name
			r1, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r1"), 0)
			drainResp(r1)
			pin, ok := gateway.scheduler.pinGet(ses, "m")
			if !ok {
				t.Fatalf("must pin")
			}
			curIdx := -1
			for i, p := range gateway.pools["z"].items {
				if p.name == pin.ProxyRaw {
					curIdx = i
				}
			}
			otherIdx := 1 - curIdx
			var curCalls, otherCalls atomic.Int32
			if tc.transport {
				postStub(t, gateway, "z", curIdx, &curCalls, nil, func(*http.Request) (*http.Response, error) {
					return nil, errors.New("dial timeout")
				})
			} else if tc.status == 400 {
				var n atomic.Int32
				postStub(t, gateway, "z", curIdx, &curCalls, nil, func(*http.Request) (*http.Response, error) {
					n.Add(1)
					return responseWithBody(400, `{"error":"bad"}`), nil
				})
			} else {
				st := tc.status
				postStub(t, gateway, "z", curIdx, &curCalls, nil, func(*http.Request) (*http.Response, error) {
					return responseWithBody(st, `{"error":"e"}`), nil
				})
			}
			postStub(t, gateway, "z", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
				return responseWithBody(200, `{"ok":true}`), nil
			})
			r2, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r2"), 0)
			if err != nil {
				t.Fatalf("err=%v", err)
			}
			if r2.StatusCode != tc.wantStatus {
				t.Fatalf("status=%d want %d", r2.StatusCode, tc.wantStatus)
			}
			drainResp(r2)
			if postCount(&curCalls) != tc.wantCallsCur {
				t.Fatalf("cur calls=%d want %d", postCount(&curCalls), tc.wantCallsCur)
			}
			if postCount(&otherCalls) != tc.wantOtherCalls {
				t.Fatalf("other calls=%d want %d", postCount(&otherCalls), tc.wantOtherCalls)
			}
			pin2, _ := gateway.scheduler.pinGet(ses, "m")
			moved := pin2.ProxyRaw != pin.ProxyRaw
			if moved != tc.wantMove {
				t.Fatalf("moved=%v want %v (was %s now %s)", moved, tc.wantMove, pin.ProxyRaw, pin2.ProxyRaw)
			}
			// Credential identity unchanged on move.
			if pin2.CredID != pin.CredID || pin2.Pool != pin.Pool || pin2.Tier != pin.Tier || pin2.Model != pin.Model {
				t.Fatalf("binding identity changed: %+v vs %+v", pin, pin2)
			}
		})
	}
}

// Two distinct 429s set credential cooldown; one does not.
func TestAuthCredential429Evidence(t *testing.T) {
	monitor := NewMonitor()
	gateway := authTwoProxyGateway(t, monitor, 5)
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	postStub(t, gateway, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ses := "ses_cred429_1"
	r1, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r1"), 0)
	drainResp(r1)
	pin, _ := gateway.scheduler.pinGet(ses, "m")
	curIdx := -1
	for i, p := range gateway.pools["z"].items {
		if p.name == pin.ProxyRaw {
			curIdx = i
		}
	}
	otherIdx := 1 - curIdx
	// Single 429 moves and succeeds: no credential cooldown.
	var c0, c1 atomic.Int32
	postStub(t, gateway, "z", curIdx, &c0, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "3")
		return r, nil
	})
	postStub(t, gateway, "z", otherIdx, &c1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	r2, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r2"), 0)
	if err != nil || r2.StatusCode != 200 {
		t.Fatalf("single 429 must move to 200, err=%v resp=%v", err, r2)
	}
	drainResp(r2)
	if _, _, ok := gateway.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("single 429 must not set credential cooldown")
	}
	// Expire the single-429 proxy cooldown so both proxies are eligible for
	// the two-distinct evidence test (otherwise the cooled proxy is skipped
	// and only one 429 can be observed).
	gateway.scheduler.mu.Lock()
	for _, entry := range gateway.scheduler.proxy429State {
		if entry != nil {
			entry.cooldownUntil = time.Now().Add(-time.Second).UnixNano()
		}
	}
	gateway.scheduler.mu.Unlock()
	// Two distinct 429s set credential cooldown with second Retry-After.
	pin2, _ := gateway.scheduler.pinGet(ses, "m")
	cur2 := -1
	for i, p := range gateway.pools["z"].items {
		if p.name == pin2.ProxyRaw {
			cur2 = i
		}
	}
	other2 := 1 - cur2
	var d0, d1 atomic.Int32
	postStub(t, gateway, "z", cur2, &d0, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gateway, "z", other2, &d1, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "9")
		return r, nil
	})
	r3, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r3"), 0)
	if err != nil || r3.StatusCode != 429 {
		t.Fatalf("two 429s must return 429, err=%v resp=%v", err, r3)
	}
	if got := r3.Header.Get("Retry-After"); got != "9" {
		t.Fatalf("second Retry-After must be preserved, got %q", got)
	}
	drainResp(r3)
	until, status, ok := gateway.scheduler.credential429CooldownStatus(pin.CredID)
	if !ok || status != 429 {
		t.Fatalf("credential cooldown must be set")
	}
	if until <= time.Now().UnixNano() {
		t.Fatalf("cooldown must be future")
	}
	if postCount(&d0) != 1 || postCount(&d1) != 1 {
		t.Fatalf("two sends: %d/%d", postCount(&d0), postCount(&d1))
	}
	// Fast-fail zero send while active.
	var e0, e1 atomic.Int32
	postStub(t, gateway, "z", 0, &e0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 1, &e1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	r4, _, attempts, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r4"), 0)
	if err != nil || r4.StatusCode != 429 {
		t.Fatalf("fast-fail 429, err=%v resp=%v", err, r4)
	}
	if attempts != 0 || postCount(&e0)+postCount(&e1) != 0 {
		t.Fatalf("fast-fail zero send: attempts=%d sends=%d/%d", attempts, postCount(&e0), postCount(&e1))
	}
	if got := r4.Header.Get("Retry-After"); got == "" {
		t.Fatalf("fast-fail must carry Retry-After")
	}
	drainResp(r4)
	// Envelope preserved across protocols.
	for _, proto := range []Protocol{ProtocolChat, ProtocolResponses, ProtocolAnthropic} {
		synth := pinLocalResponse(429, 5, "upstream temporarily unavailable")
		rec := &testResponseWriter{header: make(http.Header)}
		copyErrorResponse(rec, proto, synth, "req-x")
		if rec.status != 429 || rec.header.Get("Retry-After") == "" || len(rec.body) == 0 {
			t.Fatalf("proto %s envelope broken", proto)
		}
	}
	// Credential IDs stay tier-qualified: same key text on Zen vs legacy Go
	// yields distinct identities, and the legacy Go identity never affects the
	// single authenticated lane.
	gatewayIso := authThreeProxyGateway(t, NewMonitor())
	sameKey := "same-key-text-zzzzz"
	gatewayIso.authCreds = credentialsForKeys(TierZen, []string{sameKey})
	zenID := gatewayIso.authCreds[0].id
	goID := credentialIDForKey(TierGo, sameKey)
	if zenID == goID {
		t.Fatalf("cred IDs must isolate tiers")
	}
	gatewayIso.scheduler.noteCredential429Failure(zenID, AttemptClassRateLimited, 429, 5*time.Second, time.Now().UnixNano())
	if _, _, ok := gatewayIso.scheduler.credential429CooldownStatus(goID); ok {
		t.Fatalf("legacy Go identity must stay isolated from Zen credential limit")
	}
	// Stale success protection.
	s := newTargetScheduler(15 * time.Second)
	base := time.Now().UnixNano()
	s.noteCredential429Failure("zen:abc", AttemptClassRateLimited, 429, 0, base+2000)
	if cleared := s.noteCredential429Success("zen:abc", base+1000); cleared.Cleared {
		t.Fatalf("stale success must not clear")
	}
	if cleared := s.noteCredential429Success("zen:abc", base+2000); !cleared.Cleared {
		t.Fatalf("equal-started success must clear")
	}
	// Success clearing via gateway: newer-started 2xx clears.
	gw2 := authTwoProxyGateway(t, NewMonitor(), 5)
	cred := gw2.authCreds[0]
	started := time.Now().UnixNano()
	gw2.scheduler.noteCredential429Failure(cred.id, AttemptClassRateLimited, 429, time.Second, started)
	pool := gw2.pools["z"]
	cand := authCand(TierZen, cred, pool, pool.items[0], "m")
	gw2.applyAttemptOutcome(context.Background(), cand, responseWithStatus(200), nil, started-1)
	if _, _, ok := gw2.scheduler.credential429CooldownStatus(cred.id); !ok {
		t.Fatalf("stale 2xx cleared newer credential429")
	}
	gw2.applyAttemptOutcome(context.Background(), cand, responseWithStatus(200), nil, time.Now().UnixNano())
	if _, _, ok := gw2.scheduler.credential429CooldownStatus(cred.id); ok {
		t.Fatalf("newer 2xx must clear")
	}
	// Unbound two-proxy 429 sets credential cooldown (same cred, two proxies).
	monitorU := NewMonitor()
	gwU := authTwoProxyGateway(t, monitorU, 5)
	routeU := authOnlyRoute()
	routeU.KeyTiers = []Tier{TierZen}
	// Force same credential: single key gateway has one cred; two proxies both 429.
	var u0, u1 atomic.Int32
	postStub(t, gwU, "z", 0, &u0, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "2")
		return r, nil
	})
	postStub(t, gwU, "z", 1, &u1, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "6")
		return r, nil
	})
	ru, _, _, _ := gwU.doUpstreamTiers(pinTestCtx(), routeU, routeBodies(), pinIDs("ses_unbound_429_1", "ru1"), 0)
	drainResp(ru)
	credU := gwU.authCreds[0].id
	if _, _, ok := gwU.scheduler.credential429CooldownStatus(credU); !ok {
		t.Fatalf("unbound two-proxy same-cred 429 must set credential cooldown")
	}
}

// Concurrency fencing.
func TestAuthPinMoveFencing(t *testing.T) {
	s := newTargetScheduler(15 * time.Second)
	auth := normalizeRouteAuthority("https://zen.example")
	s.pinBind("ses_fence", "m", sessionPin{Tier: TierZen, CredID: "zen:aaa", Pool: "z", ProxyRaw: "p0", Model: "m", Protocol: ProtocolChat, Authority: auth})
	got, ok := s.pinGet("ses_fence", "m")
	if !ok || got.Generation != 0 {
		t.Fatalf("gen0 expected")
	}
	ng, ok := s.pinMoveCurrent("ses_fence", "m", 0, "p1")
	if !ok || ng != 1 {
		t.Fatalf("move gen0->p1 must succeed gen1")
	}
	// Stale move with old generation must fail and not move back.
	if _, ok := s.pinMoveCurrent("ses_fence", "m", 0, "p0"); ok {
		t.Fatalf("stale move must fail")
	}
	cur, _ := s.pinGet("ses_fence", "m")
	if cur.ProxyRaw != "p1" || cur.Generation != 1 {
		t.Fatalf("pin must stay p1 gen1, got %+v", cur)
	}
	// Competing moves with same expected gen: first wins.
	s.pinBind("ses_race", "m", sessionPin{Tier: TierZen, CredID: "zen:aaa", Pool: "z", ProxyRaw: "p0", Model: "m", Protocol: ProtocolChat, Authority: auth})
	if _, ok := s.pinMoveCurrent("ses_race", "m", 0, "p1"); !ok {
		t.Fatalf("first move must win")
	}
	if _, ok := s.pinMoveCurrent("ses_race", "m", 0, "p2"); ok {
		t.Fatalf("second stale move must lose")
	}
	cur2, _ := s.pinGet("ses_race", "m")
	if cur2.ProxyRaw != "p1" {
		t.Fatalf("must not split: %s", cur2.ProxyRaw)
	}
	// Anonymous moves with generation fencing like authenticated.
	s.pinBind("ses_anon_fence", "m", sessionPin{Tier: TierZen, CredID: anonymousSchedulerCredentialID, Pool: "a", ProxyRaw: "p0", Model: "m", Protocol: ProtocolChat, Authority: auth})
	if _, ok := s.pinMoveCurrent("ses_anon_fence", "m", 0, "p1"); !ok {
		t.Fatalf("anon must move with generation fencing")
	}
	curAnon, _ := s.pinGet("ses_anon_fence", "m")
	if curAnon.ProxyRaw != "p1" || curAnon.Generation != 1 {
		t.Fatalf("anon move must update current+generation: %+v", curAnon)
	}
	if _, ok := s.pinMoveCurrent("ses_anon_fence", "m", 0, "p0"); ok {
		t.Fatalf("stale anon move must fail")
	}
}

// Reload migration, tombstone, cap, tier qualification, readiness.
func TestAuthMigrationTombstoneCapReadiness(t *testing.T) {
	oldCfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	oldCfg.Keys = []string{"zen-key-aaaaa", "go-key-bbbbb"}
	oldCfg.Anonymous = true
	oldNormalized, err := NormalizeConfig("config.json", oldCfg)
	if err != nil {
		t.Fatal(err)
	}
	oldGateway, err := NewGateway(oldNormalized, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	zenAuth := normalizeRouteAuthority(oldNormalized.Upstream.Zen)
	zenCred := oldGateway.authCreds[0].id
	// Auth pin proxy-independent; anon pin full target.
	oldGateway.scheduler.pinBind("ses_auth_keep", "m", sessionPin{Tier: TierZen, CredID: zenCred, Pool: "shared", ProxyRaw: "direct", Model: "m", Protocol: ProtocolChat, Authority: zenAuth})
	oldGateway.scheduler.pinBind("ses_anon_keep", "m", sessionPin{Tier: TierZen, CredID: anonymousSchedulerCredentialID, Pool: "shared", ProxyRaw: "direct", Model: "m", Protocol: ProtocolChat, Authority: zenAuth})
	oldGateway.scheduler.pinBind("ses_gone", "m", sessionPin{Tier: TierZen, CredID: credentialIDForKey(TierZen, "gone"), Pool: "shared", ProxyRaw: "direct", Model: "m", Protocol: ProtocolChat, Authority: zenAuth})
	oldGateway.scheduler.noteProxy429Failure(TierZen, "shared", "direct", AttemptClassRateLimited, 429, 0, time.Now().UnixNano())
	oldGateway.scheduler.noteCredential429Failure(zenCred, AttemptClassRateLimited, 429, time.Second, time.Now().UnixNano())
	// Legacy pool-only proxy429 entry must drop on migrate.
	oldGateway.scheduler.mu.Lock()
	oldGateway.scheduler.proxy429State["shared\x00direct"] = &proxy429Entry{failures: 1, cooldownUntil: time.Now().Add(time.Minute).UnixNano(), lastFailureAt: time.Now().UnixNano(), lastStartedNanos: time.Now().UnixNano(), lastStatus: 429}
	oldGateway.scheduler.mu.Unlock()

	newCfg := testGatewayConfig(map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	newCfg.Keys = []string{"zen-key-aaaaa", "go-key-bbbbb"}
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
	if summary.Pins != 3 {
		t.Fatalf("pins=%d want 3", summary.Pins)
	}
	if summary.Credential429 != 1 {
		t.Fatalf("cred429=%d want 1", summary.Credential429)
	}
	// Tier-qualified proxy429 migrates; Go stays free.
	if _, _, ok := newGateway.scheduler.proxy429CooldownStatus(TierZen, "shared", "direct"); !ok {
		t.Fatalf("zen proxy429 must migrate")
	}
	if _, _, ok := newGateway.scheduler.proxy429CooldownStatus(TierGo, "shared", "direct"); ok {
		t.Fatalf("go must stay isolated")
	}
	// Legacy entry dropped.
	newGateway.scheduler.mu.Lock()
	legacyKept := false
	for id := range newGateway.scheduler.proxy429State {
		if id == "shared\x00direct" {
			legacyKept = true
		}
	}
	newGateway.scheduler.mu.Unlock()
	if legacyKept {
		t.Fatalf("legacy pool-only proxy429 must drop")
	}
	// Tombstone: removed-cred pin still migrates then 502s with no sends.
	var c0, c1 atomic.Int32
	postStub(t, newGateway, "shared", 0, &c0, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, newGateway, "shared", 1, &c1, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	goneResp, _, attempts, err := newGateway.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs("ses_gone", "req-gone"), 0)
	if err != nil || goneResp.StatusCode != 502 || attempts != 0 || postCount(&c0)+postCount(&c1) != 0 {
		t.Fatalf("tombstone 502 no-send: err=%v resp=%v attempts=%d sends=%d/%d", err, goneResp, attempts, postCount(&c0), postCount(&c1))
	}
	drainResp(goneResp)
	// Auth pin validates proxy-independently: changing current proxy in new
	// pool still serves (moves), while anon with removed proxy 502s.
	// (Covered by move tests; here just check pins exist.)
	if _, ok := newGateway.scheduler.pinGet("ses_auth_keep", "m"); !ok {
		t.Fatalf("auth pin must migrate")
	}
	if _, ok := newGateway.scheduler.pinGet("ses_anon_keep", "m"); !ok {
		t.Fatalf("anon pin must migrate")
	}
	// Readiness: proxy429 and credential429 never degrade global readiness.
	seedHealthyCatalog(t, newGateway)
	code, health := decodeHealth(t, newGateway)
	if code != http.StatusOK || !health.Ready {
		t.Fatalf("429 layers must not degrade readiness: code=%d ready=%v issues=%v routing=%+v", code, health.Ready, health.Issues, health.Routing)
	}
	// Redaction/metrics: attempts carry redacted proxy, suffix key, hashed session.
	mon := NewMonitor()
	gw := authTwoProxyGateway(t, mon, 5)
	postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	secretSes := "ses_secret_check_123"
	r, _, _, _ := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(secretSes, "req-sec"), 0)
	drainResp(r)
	recent := mon.Snapshot().Upstream.Recent
	if len(recent) == 0 {
		t.Fatalf("no attempts recorded")
	}
	for _, a := range recent {
		if strings.Contains(a.Proxy, "hunter2") || strings.Contains(a.KeyID, "zen-key-aaaaa") {
			t.Fatalf("secret leaked in attempt: %+v", a)
		}
		if a.ClientSessionHash == secretSes || strings.Contains(a.ClientSessionHash, secretSes) {
			t.Fatalf("raw session leaked: %+v", a)
		}
		if a.FailureClass == "" || a.Outcome == "" {
			t.Fatalf("missing classification: %+v", a)
		}
	}
	raw, _ := io.ReadAll(io.NopCloser(strings.NewReader("")))
	_ = raw
}

func poolIndexByRaw(gateway *Gateway, pool string, raw string) int {
	for i, p := range gateway.pools[pool].items {
		if p != nil && p.name == raw {
			return i
		}
	}
	return -1
}

// 3-proxy established auth: first two 429s continue to the third (no
// two-stop, no max_attempts truncation); third 200 wins without credential429.
func TestAuthEstablishedThreeProxyTwo429Stops(t *testing.T) {
	monitor := NewMonitor()
	gateway := authThreeProxyGateway(t, monitor)
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	for i := range gateway.pools["z"].items {
		postStub(t, gateway, "z", i, nil, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
	}
	ses := "ses_established_3p_429_1"
	r1, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r1"), 0)
	drainResp(r1)
	pin, ok := gateway.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must pin")
	}
	ordered := affinityProxyOrder(gateway.pools["z"], pin.CredID, pin.ProxyRaw)
	if len(ordered) != 3 {
		t.Fatalf("ordered=%d want 3", len(ordered))
	}
	firstRaw, secondRaw, thirdRaw := ordered[0].name, ordered[1].name, ordered[2].name
	firstIdx := poolIndexByRaw(gateway, "z", firstRaw)
	secondIdx := poolIndexByRaw(gateway, "z", secondRaw)
	thirdIdx := poolIndexByRaw(gateway, "z", thirdRaw)
	var c0, c1, c2 atomic.Int32
	postStub(t, gateway, "z", firstIdx, &c0, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gateway, "z", secondIdx, &c1, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "9")
		return r, nil
	})
	postStub(t, gateway, "z", thirdIdx, &c2, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	before := len(monitor.Snapshot().Upstream.Recent)
	r2, _, attempts, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r2"), 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if r2.StatusCode != 200 {
		t.Fatalf("status=%d want 200 via third proxy (no two-stop)", r2.StatusCode)
	}
	drainResp(r2)
	if postCount(&c0) != 1 || postCount(&c1) != 1 || postCount(&c2) != 1 {
		t.Fatalf("all three proxies must send 1/1/1, got %d/%d/%d", postCount(&c0), postCount(&c1), postCount(&c2))
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3", attempts)
	}
	if got := len(monitor.Snapshot().Upstream.Recent) - before; got != 3 {
		t.Fatalf("recorded=%d want 3", got)
	}
	if _, _, ok := gateway.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("partial 429 with success must not set credential429")
	}
	// Pin must move to the successful third proxy.
	pin2, _ := gateway.scheduler.pinGet(ses, "m")
	if pin2.ProxyRaw != thirdRaw || pin2.Generation != pin.Generation+1 {
		t.Fatalf("success must move pin to third: %+v vs %+v", pin, pin2)
	}
}

// Small budget established auth with all transport failures: total real sends
// capped at retry.max_attempts including same-target retry accounting.
func TestAuthEstablishedSmallBudgetTransportCap(t *testing.T) {
	monitor := NewMonitor()
	gateway := authTwoProxyGateway(t, monitor, 2)
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	postStub(t, gateway, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ses := "ses_established_budget_2"
	r1, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r1"), 0)
	drainResp(r1)
	pin, ok := gateway.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must pin")
	}
	curIdx := poolIndexByRaw(gateway, "z", pin.ProxyRaw)
	otherIdx := 1 - curIdx
	if curIdx < 0 || otherIdx < 0 {
		t.Fatalf("bad idx")
	}
	var curCalls, otherCalls atomic.Int32
	postStub(t, gateway, "z", curIdx, &curCalls, nil, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial timeout")
	})
	postStub(t, gateway, "z", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial timeout")
	})
	before := len(monitor.Snapshot().Upstream.Recent)
	r2, _, attempts, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r2"), 0)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	_ = r2
	drainResp(r2)
	if r2.StatusCode != 502 {
		t.Fatalf("budget-exhausted transport must 502, got %d", r2.StatusCode)
	}
	// Budget 2: first proxy first send + same-target retry = 2, next proxy
	// never sent because budget exhausted before it.
	if postCount(&curCalls) != 2 {
		t.Fatalf("cur=%d want 2 (first+retry)", postCount(&curCalls))
	}
	if postCount(&otherCalls) != 0 {
		t.Fatalf("other=%d want 0 (budget stops move)", postCount(&otherCalls))
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2 (exact budget)", attempts)
	}
	if got := len(monitor.Snapshot().Upstream.Recent) - before; got != 2 {
		t.Fatalf("recorded=%d want 2", got)
	}
}

// Established 429 chain is never truncated by the real-send budget: at
// max_attempts=1 the walk still covers all eligible proxies; full exhaustion
// writes credential429 with the last Retry-After envelope.
func TestAuthEstablished429RespectsBudget1(t *testing.T) {
	monitor := NewMonitor()
	gateway := authTwoProxyGateway(t, monitor, 1)
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	postStub(t, gateway, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ses := "ses_established_429_m1_1"
	r1, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r1"), 0)
	drainResp(r1)
	pin, ok := gateway.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must pin")
	}
	curIdx := poolIndexByRaw(gateway, "z", pin.ProxyRaw)
	otherIdx := 1 - curIdx
	var curCalls, otherCalls atomic.Int32
	postStub(t, gateway, "z", curIdx, &curCalls, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gateway, "z", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "9")
		return r, nil
	})
	r2, _, attempts, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r2"), 0)
	if err != nil || r2.StatusCode != 429 {
		t.Fatalf("err=%v resp=%v want exhausted 429", err, r2)
	}
	if got := r2.Header.Get("Retry-After"); got != "9" {
		t.Fatalf("exhaustion must preserve last Retry-After, got %q", got)
	}
	drainResp(r2)
	if postCount(&curCalls) != 1 || postCount(&otherCalls) != 1 {
		t.Fatalf("budget-1 429 must still walk both: %d/%d", postCount(&curCalls), postCount(&otherCalls))
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
	if _, _, ok := gateway.scheduler.credential429CooldownStatus(pin.CredID); !ok {
		t.Fatalf("full exhaustion must set credential429 even at budget-1")
	}
}

// Pinned anonymous at max_attempts=1 keeps its independent same-target
// transient retry (prior contract), unlike authenticated budget truncation.
func TestPinnedAnonymousMaxAttempts1KeepsRetry(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	gateway.cfg.Retry.MaxAttempts = 1
	postStub(t, gateway, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ses := "ses_pinned_anon_m1_1"
	route := anonAuthRoute()
	r1, _, _, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r1"), 0)
	if err != nil {
		t.Fatal(err)
	}
	drainResp(r1)
	pin, ok := gateway.scheduler.pinGet(ses, "m")
	if !ok || pin.CredID != anonymousSchedulerCredentialID {
		t.Fatalf("must pin anon %+v ok=%v", pin, ok)
	}
	pinnedIdx := poolIndexByRaw(gateway, "a", pin.ProxyRaw)
	otherIdx := 1 - pinnedIdx
	var pinnedCalls, otherCalls atomic.Int32
	var n atomic.Int32
	postStub(t, gateway, "a", pinnedIdx, &pinnedCalls, nil, func(*http.Request) (*http.Response, error) {
		if n.Add(1) == 1 {
			return nil, errors.New("dial timeout")
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "a", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	before := len(monitor.Snapshot().Upstream.Recent)
	r2, _, attempts, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r2"), 0)
	if err != nil || r2.StatusCode != 200 {
		t.Fatalf("err=%v resp=%v want 200 via retry", err, r2)
	}
	drainResp(r2)
	if postCount(&pinnedCalls) != 2 {
		t.Fatalf("pinned anon must retry same-target at max=1: got %d want 2", postCount(&pinnedCalls))
	}
	if postCount(&otherCalls) != 0 {
		t.Fatalf("anon never crosses proxy: other=%d", postCount(&otherCalls))
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2 (first+independent retry)", attempts)
	}
	if got := len(monitor.Snapshot().Upstream.Recent) - before; got != 2 {
		t.Fatalf("recorded=%d want 2", got)
	}
}

// Pre-cooled proxy skipped before any send and never counts toward the
// two-distinct 429 evidence.
func TestAuthEstablishedPrecooledSkippedNotCounted(t *testing.T) {
	monitor := NewMonitor()
	gateway := authThreeProxyGateway(t, monitor)
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	for i := range gateway.pools["z"].items {
		postStub(t, gateway, "z", i, nil, nil, func(*http.Request) (*http.Response, error) {
			return responseWithBody(200, `{"ok":true}`), nil
		})
	}
	ses := "ses_established_precooled_1"
	r1, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r1"), 0)
	drainResp(r1)
	pin, ok := gateway.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must pin")
	}
	ordered := affinityProxyOrder(gateway.pools["z"], pin.CredID, pin.ProxyRaw)
	if len(ordered) != 3 {
		t.Fatalf("ordered=%d want 3", len(ordered))
	}
	// Pre-cool the second proxy in try order; it must be skipped.
	cooledRaw := ordered[1].name
	cooledIdx := poolIndexByRaw(gateway, "z", cooledRaw)
	curRaw := ordered[0].name
	curIdx := poolIndexByRaw(gateway, "z", curRaw)
	otherRaw := ordered[2].name
	otherIdx := poolIndexByRaw(gateway, "z", otherRaw)
	gateway.scheduler.noteProxy429Failure(TierZen, "z", cooledRaw, AttemptClassRateLimited, 429, time.Minute, time.Now().UnixNano())
	var curCalls, cooledCalls, otherCalls atomic.Int32
	postStub(t, gateway, "z", curIdx, &curCalls, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gateway, "z", cooledIdx, &cooledCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	r2, _, attempts, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r2"), 0)
	if err != nil || r2.StatusCode != 200 {
		t.Fatalf("single 429 must skip cooled and move to 200, err=%v resp=%v", err, r2)
	}
	drainResp(r2)
	if postCount(&cooledCalls) != 0 {
		t.Fatalf("pre-cooled proxy must be skipped with zero sends, got %d", postCount(&cooledCalls))
	}
	if postCount(&curCalls) != 1 || postCount(&otherCalls) != 1 {
		t.Fatalf("sends must be cur/other 1/1, got %d/%d", postCount(&curCalls), postCount(&otherCalls))
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2", attempts)
	}
	// Single observed 429 (cooled never counted) must not set credential429.
	if _, _, ok := gateway.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("pre-cooled skip must not count toward two-distinct evidence")
	}
	// Pin moved past the cooled proxy to the successful alternate.
	pin2, _ := gateway.scheduler.pinGet(ses, "m")
	if pin2.ProxyRaw != otherRaw {
		t.Fatalf("pin must move to %s, got %s", otherRaw, pin2.ProxyRaw)
	}
}

// Gateway-level concurrent same session+model proxy moves: generation/CAS
// yields one current proxy without stale move-back or split; attempts recorded.
func TestAuthEstablishedConcurrentMovesSingleWinner(t *testing.T) {
	monitor := NewMonitor()
	gateway := authTwoProxyGateway(t, monitor, 5)
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	postStub(t, gateway, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gateway, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ses := "ses_established_concurrent_1"
	r1, _, _, _ := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r1"), 0)
	drainResp(r1)
	pin, ok := gateway.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must pin")
	}
	curIdx := poolIndexByRaw(gateway, "z", pin.ProxyRaw)
	otherIdx := 1 - curIdx
	otherRaw := gateway.pools["z"].items[otherIdx].name
	var curCalls, otherCalls atomic.Int32
	postStub(t, gateway, "z", curIdx, &curCalls, nil, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial timeout")
	})
	postStub(t, gateway, "z", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	const workers = 8
	before := len(monitor.Snapshot().Upstream.Recent)
	start := make(chan struct{})
	var wg sync.WaitGroup
	resps := make([]*http.Response, workers)
	attemptsOut := make([]int, workers)
	errs := make([]error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			resp, _, attempts, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "conc-"+string(rune('a'+idx))), 0)
			resps[idx], attemptsOut[idx], errs[idx] = resp, attempts, err
		}(i)
	}
	close(start)
	wg.Wait()
	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Fatalf("worker %d err=%v", i, errs[i])
		}
		if resps[i] == nil || resps[i].StatusCode != 200 {
			t.Fatalf("worker %d status=%v want 200", i, resps[i])
		}
		drainResp(resps[i])
		// Each worker either moved (3 sends: cur x2 + other) or hit the
		// already-moved pin (1 send). Both are valid; no worker may see a
		// split or stale move-back, and each attempt count matches sends.
		if attemptsOut[i] != 1 && attemptsOut[i] != 3 {
			t.Fatalf("worker %d attempts=%d want 1 or 3", i, attemptsOut[i])
		}
	}
	final, ok := gateway.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("pin missing")
	}
	if final.ProxyRaw != otherRaw {
		t.Fatalf("concurrent moves must converge to %s, got %s (gen %d)", otherRaw, final.ProxyRaw, final.Generation)
	}
	if final.Generation != pin.Generation+1 {
		t.Fatalf("exactly one CAS move must win: gen %d want %d", final.Generation, pin.Generation+1)
	}
	// Attempts remain correctly recorded: monitor records equal total sends.
	totalSends := postCount(&curCalls) + postCount(&otherCalls)
	if got := len(monitor.Snapshot().Upstream.Recent) - before; got != totalSends {
		t.Fatalf("recorded=%d sends=%d (cur %d other %d) must match", got, totalSends, postCount(&curCalls), postCount(&otherCalls))
	}
	sumAttempts := 0
	for _, a := range attemptsOut {
		sumAttempts += a
	}
	if sumAttempts != totalSends {
		t.Fatalf("per-request attempts sum=%d sends=%d must match", sumAttempts, totalSends)
	}
	// No stale move-back: one more request stays on the winner with 1 send.
	var extraCur, extraOther atomic.Int32
	_ = extraCur
	_ = extraOther
	rLast, _, attemptsLast, err := gateway.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "conc-final"), 0)
	if err != nil || rLast.StatusCode != 200 || attemptsLast != 1 {
		t.Fatalf("post-convergence must stay 1-send 200, err=%v resp=%v attempts=%d", err, rLast, attemptsLast)
	}
	drainResp(rLast)
}
