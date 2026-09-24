package app

import (
	"net/http"
	"sync/atomic"
	"testing"
)

// Unbound 401 is a credential-scoped L2 cause: after the first real stable
// 401 for one CredID, remaining frozen candidates with the same CredID are
// skipped without a send. Skips count as unavailable without fake attempts.

func authHeaderKey(r *http.Request) string {
	if h := r.Header.Get("Authorization"); len(h) > 7 {
		return h[7:]
	}
	return ""
}

func twoCredTwoProxyGateway(t *testing.T, maxAttempts int) *Gateway {
	t.Helper()
	cfg := testGatewayConfig(
		map[string][]string{
			"a": {"direct"},
			"z": {"direct", "http://127.0.0.1:8081"},
		},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-401-scope-a", "zen-key-401-scope-b"}
	cfg.Retry.MaxAttempts = maxAttempts
	cfg.Retry.TransientRetryIntervalSeconds = 0
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	return gw
}

// 1) Auth unbound, one credential x >=2 proxies: first candidate immediate
// 401 => only one send for that credential; second credential is tried and
// can succeed/pin.
func TestUnboundAuth401SkipsSameCredential(t *testing.T) {
	gw := twoCredTwoProxyGateway(t, 1)
	ses := "ses_unbound_401_scope_1"
	// Freeze order to learn which credential is tried first.
	now := int64(0)
	// Use a fixed now via scheduler build with current time is fine; order is
	// session-HRW deterministic. Compute with the gateway scheduler directly.
	_ = now
	cands := gw.scheduler.orderCandidates(
		gw.scheduler.buildAuthCandidates(TierZen, gw.credentials(), gw.pools["z"], "m", 9223372036854775807),
		ses,
	)
	// Build with a far-future now would filter nothing extra (no cooldowns yet);
	// rebuild with real now for the actual request path consistency.
	// The order above may differ from request-time only by cooldown filtering
	// (none yet), so first-credential identity is stable.
	if len(cands) != 4 {
		t.Fatalf("frozen=%d want 4 (2 creds x 2 proxies)", len(cands))
	}
	firstCred := cands[0].CredID
	var firstKey, secondKey string
	for _, c := range gw.credentials() {
		if c.id == firstCred {
			firstKey = c.key
		} else if secondKey == "" {
			secondKey = c.key
		}
	}
	if firstKey == "" || secondKey == "" {
		t.Fatalf("cred keys missing first=%q second=%q", firstKey, secondKey)
	}
	var z0Total, z1Total atomic.Int32
	var firstCredTotal, secondCredTotal atomic.Int32
	mkFn := func(idx *atomic.Int32) func(*http.Request) (*http.Response, error) {
		return func(r *http.Request) (*http.Response, error) {
			idx.Add(1)
			k := authHeaderKey(r)
			if k == firstKey {
				firstCredTotal.Add(1)
				return responseWithBody(401, `{"error":"unauthorized"}`), nil
			}
			secondCredTotal.Add(1)
			return responseWithBody(200, `{"ok":true}`), nil
		}
	}
	postStub(t, gw, "z", 0, &z0Total, nil, mkFn(&z0Total))
	// Second stub shares the same branch but counts separately; z1Total double
	// counts via postStub wrapper + explicit add would double, so use nil counter
	// in wrapper and count inside fn.
	// Re-install z0 correctly: postStub already counted, so reset and use manual.
	// Simplify: reset counters and rely on fn counts only.
	z0Total.Store(0)
	z1Total.Store(0)
	firstCredTotal.Store(0)
	secondCredTotal.Store(0)
	// Re-stub with fn-only counting (wrapper nil) to avoid double count.
	postStub(t, gw, "z", 0, nil, nil, func(r *http.Request) (*http.Response, error) {
		z0Total.Add(1)
		k := authHeaderKey(r)
		if k == firstKey {
			firstCredTotal.Add(1)
			return responseWithBody(401, `{"error":"unauthorized"}`), nil
		}
		secondCredTotal.Add(1)
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "z", 1, nil, nil, func(r *http.Request) (*http.Response, error) {
		z1Total.Add(1)
		k := authHeaderKey(r)
		if k == firstKey {
			firstCredTotal.Add(1)
			return responseWithBody(401, `{"error":"unauthorized"}`), nil
		}
		secondCredTotal.Add(1)
		return responseWithBody(200, `{"ok":true}`), nil
	})
	_ = mkFn
	resp, _, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0)
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("second credential must succeed, got %d", resp.StatusCode)
	}
	if got := int(firstCredTotal.Load()); got != 1 {
		t.Fatalf("first credential sends=%d want 1 (second proxy skipped)", got)
	}
	if got := int(secondCredTotal.Load()); got != 1 {
		t.Fatalf("second credential sends=%d want 1", got)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2 (real sends only)", attempts)
	}
	pin, ok := gw.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("success must pin")
	}
	if pin.CredID == firstCred {
		t.Fatalf("pin must bind second credential, got first %q", pin.CredID)
	}
}

// 2) Auth unbound, initial transient followed by stable 401 in L1 => no
// remaining same-credential proxy send; attempts = L1 sends + distinct-cred send.
func TestUnboundAuthTransientThen401SkipsSameCredential(t *testing.T) {
	gw := twoCredTwoProxyGateway(t, 3)
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0
	ses := "ses_unbound_401_scope_l1_1"
	cands := gw.scheduler.orderCandidates(
		gw.scheduler.buildAuthCandidates(TierZen, gw.credentials(), gw.pools["z"], "m", 9223372036854775807),
		ses,
	)
	if len(cands) != 4 {
		t.Fatalf("frozen=%d want 4", len(cands))
	}
	firstCred := cands[0].CredID
	firstProxy := cands[0].ProxyRaw
	var firstKey, secondKey string
	for _, c := range gw.credentials() {
		if c.id == firstCred {
			firstKey = c.key
		} else if secondKey == "" {
			secondKey = c.key
		}
	}
	var z0Total, z1Total atomic.Int32
	var firstCredCalls, secondCredCalls atomic.Int32
	// Per-proxy call index to serve 500 then 401 on the first-tried proxy.
	var p0Calls, p1Calls atomic.Int32
	var z0First, z1First atomic.Int32
	poolItems := gw.pools["z"].items
	idxOf := func(raw string) int {
		for i, p := range poolItems {
			if p != nil && p.name == raw {
				return i
			}
		}
		return -1
	}
	firstIdx := idxOf(firstProxy)
	stubFn := func(proxyIdx int, total *atomic.Int32, calls *atomic.Int32, firstPerProxy *atomic.Int32) func(*http.Request) (*http.Response, error) {
		return func(r *http.Request) (*http.Response, error) {
			total.Add(1)
			k := authHeaderKey(r)
			if k == secondKey {
				secondCredCalls.Add(1)
				return responseWithBody(200, `{"ok":true}`), nil
			}
			if k != firstKey {
				return responseWithBody(500, `{"error":"unexpected key"}`), nil
			}
			firstCredCalls.Add(1)
			firstPerProxy.Add(1)
			if proxyIdx == firstIdx {
				if calls.Add(1) == 1 {
					return responseWithBody(500, `{"error":"flaky"}`), nil
				}
				return responseWithBody(401, `{"error":"unauthorized"}`), nil
			}
			return responseWithBody(401, `{"error":"must not be sent"}`), nil
		}
	}
	postStub(t, gw, "z", 0, nil, nil, stubFn(0, &z0Total, &p0Calls, &z0First))
	postStub(t, gw, "z", 1, nil, nil, stubFn(1, &z1Total, &p1Calls, &z1First))
	resp, _, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0)
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("distinct credential must succeed, got %d", resp.StatusCode)
	}
	// First credential: initial 500 + L1 retry 401 = 2 sends on the same proxy.
	if got := int(firstCredCalls.Load()); got != 2 {
		t.Fatalf("first credential sends=%d want 2 (500 + stable 401)", got)
	}
	if got := int(secondCredCalls.Load()); got != 1 {
		t.Fatalf("second credential sends=%d want 1", got)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3 (2 L1 + 1 distinct)", attempts)
	}
	// The non-first physical proxy must never see the first credential
	// (same-credential skip); the distinct credential may land on either proxy.
	otherFirst := int(z1First.Load())
	if firstIdx == 0 {
		otherFirst = int(z1First.Load())
		if got := int(p0Calls.Load()); got != 2 {
			t.Fatalf("first proxy L1 sends=%d want 2", got)
		}
	} else {
		otherFirst = int(z0First.Load())
		if got := int(p1Calls.Load()); got != 2 {
			t.Fatalf("first proxy L1 sends=%d want 2", got)
		}
	}
	if otherFirst != 0 {
		t.Fatalf("skipped same-credential proxy got %d first-cred sends, want 0", otherFirst)
	}
}

// 3) Anonymous unbound >=2 proxies immediate 401 => only one anonymous send,
// then authenticated lane proceeds.
func TestUnboundAnonymous401SkipsRemainingProxies(t *testing.T) {
	cfg := testGatewayConfig(
		map[string][]string{
			"a": {"direct", "http://127.0.0.1:8081"},
			"z": {"direct"},
		},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = true
	cfg.Keys = []string{"zen-key-401-anon-scope"}
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	var a0, a1, z0 atomic.Int32
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		a0.Add(1)
		return responseWithBody(401, `{"error":"unauthorized"}`), nil
	})
	postStub(t, gw, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		a1.Add(1)
		return responseWithBody(401, `{"error":"unauthorized"}`), nil
	})
	postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		z0.Add(1)
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ses := "ses_unbound_401_anon_scope_1"
	resp, _, attempts, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0)
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("authenticated lane must succeed, got %d", resp.StatusCode)
	}
	if got := int(a0.Load() + a1.Load()); got != 1 {
		t.Fatalf("anonymous sends=%d want 1 (a0=%d a1=%d)", got, a0.Load(), a1.Load())
	}
	if got := int(z0.Load()); got != 1 {
		t.Fatalf("auth sends=%d want 1", got)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d want 2 (1 anon + 1 auth)", attempts)
	}
}

// 4) Full unbound domain exhaustion can still allow active custom when 401
// skips remaining targets; native attempt count includes real sends only.
func TestUnbound401SkipsStillAllowCustom(t *testing.T) {
	var customHits atomic.Int32
	gw := unboundExhaustionGateway(t,
		[]string{"direct", "http://127.0.0.1:8081"},
		[]string{"direct", "http://127.0.0.1:8081"},
		[]string{"zen-key-401-custom-scope"},
		&customHits, true)
	var a0, a1, z0, z1 atomic.Int32
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		a0.Add(1)
		return responseWithBody(401, `{"error":"unauthorized"}`), nil
	})
	postStub(t, gw, "a", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		a1.Add(1)
		return responseWithBody(401, `{"error":"unauthorized"}`), nil
	})
	postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		z0.Add(1)
		return responseWithBody(401, `{"error":"unauthorized"}`), nil
	})
	postStub(t, gw, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		z1.Add(1)
		return responseWithBody(401, `{"error":"unauthorized"}`), nil
	})
	ses := "ses_unbound_401_custom_scope_1"
	resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("401-skip exhaustion must take over custom, got %d %+v", resp.StatusCode, eff)
	}
	if customHits.Load() != 1 {
		t.Fatalf("must hit custom once, got %d", customHits.Load())
	}
	if got := int(a0.Load() + a1.Load()); got != 1 {
		t.Fatalf("anon real sends=%d want 1 (a0=%d a1=%d)", got, a0.Load(), a1.Load())
	}
	if got := int(z0.Load() + z1.Load()); got != 1 {
		t.Fatalf("auth real sends=%d want 1 (z0=%d z1=%d)", got, z0.Load(), z1.Load())
	}
	// 1 anon + 1 auth + 1 custom.
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3 (real sends only)", attempts)
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("must bind fallback")
	}
	// No credential429 from 401 path.
	for _, c := range gw.credentials() {
		if _, _, ok := gw.scheduler.credential429CooldownStatus(c.id); ok {
			t.Fatalf("401 must not write credential429 for %q", c.id)
		}
	}
}
