package app

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Unbound authenticated request-local transport-suspect L2 cause: a real
// qualifying true transport error at the L1 final/advance point establishes
// the (tier/channel,pool,proxy) identity for this request; later frozen
// candidates with the same identity skip while the suspect cooldown is
// still active. Skips are zero POST/attempts/writes and count as
// unavailable+entered without touching live429/credential429.

// 1) First credential L1-final transport on P; later credential skips P
// while active, sends another proxy and succeeds/pins.
func TestUnboundAuthTransportSuspectSkipsSameProxyAcrossCreds(t *testing.T) {
	gw := twoCredTwoProxyGateway(t, 1)
	ses := "ses_unbound_suspect_scope_1"
	cands := gw.scheduler.orderCandidates(
		gw.scheduler.buildAuthCandidates(TierZen, gw.credentials(), gw.pools["z"], "m", 9223372036854775807),
		ses,
	)
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
		t.Fatalf("cred keys missing")
	}
	firstDirect := cands[0].ProxyRaw
	if cands[2].ProxyRaw != firstDirect {
		t.Fatalf("fixture assumption broken: first proxies differ %q vs %q", firstDirect, cands[2].ProxyRaw)
	}
	poolItems := gw.pools["z"].items
	idxOf := func(raw string) int {
		for i, p := range poolItems {
			if p != nil && p.name == raw {
				return i
			}
		}
		return -1
	}
	directIdx := idxOf(firstDirect)
	otherIdx := 1 - directIdx
	if directIdx < 0 || otherIdx < 0 {
		t.Fatalf("proxy idx missing direct=%d", directIdx)
	}
	var directPosts, otherPosts atomic.Int32
	var firstCredPosts, secondCredPosts atomic.Int32
	// Direct proxy: first cred transport error; second cred must never POST.
	postStub(t, gw, "z", directIdx, nil, nil, func(r *http.Request) (*http.Response, error) {
		directPosts.Add(1)
		k := authHeaderKey(r)
		if k == firstKey {
			firstCredPosts.Add(1)
			return nil, errors.New("dial timeout")
		}
		secondCredPosts.Add(1)
		return responseWithBody(200, `{"error":"must be skipped, got second-cred POST on direct"}`), nil
	})
	// Other proxy: first cred 500 (L1-final unavailable), second cred 200.
	postStub(t, gw, "z", otherIdx, nil, nil, func(r *http.Request) (*http.Response, error) {
		otherPosts.Add(1)
		k := authHeaderKey(r)
		if k == firstKey {
			firstCredPosts.Add(1)
			return responseWithBody(500, `{"error":"flaky"}`), nil
		}
		secondCredPosts.Add(1)
		return responseWithBody(200, `{"ok":true}`), nil
	})
	resp, _, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0)
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("second credential must succeed, got %d", resp.StatusCode)
	}
	if got := int(directPosts.Load()); got != 1 {
		t.Fatalf("direct proxy POSTs=%d want 1 (first-cred transport only, second-cred skipped)", got)
	}
	if got := int(otherPosts.Load()); got != 2 {
		t.Fatalf("other proxy POSTs=%d want 2 (first-cred 500 + second-cred 200)", got)
	}
	if got := int(firstCredPosts.Load()); got != 2 {
		t.Fatalf("first credential sends=%d want 2", got)
	}
	if got := int(secondCredPosts.Load()); got != 1 {
		t.Fatalf("second credential sends=%d want 1 (direct skipped)", got)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3 (real sends only)", attempts)
	}
	pin, ok := gw.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("success must pin")
	}
	if pin.CredID == firstCred {
		t.Fatalf("pin must bind second credential, got first")
	}
	if _, _, ok := gw.scheduler.suspectCooldownStatus(TierZen, "z", firstDirect); !ok {
		t.Fatalf("qualifying transport must write suspect cooldown for %q", firstDirect)
	}
	// Skips must not write credential429.
	for _, c := range gw.credentials() {
		if _, _, ok := gw.scheduler.credential429CooldownStatus(c.id); ok {
			t.Fatalf("transport-suspect path must not write credential429 for %q", c.id)
		}
	}
}

// 2) First credential establishes suspect for all shared proxies; later
// credential fully skips, all domains exhaust, active custom takes over.
func TestUnboundAuthTransportSuspectSkipsExhaustLaterCredentialAllowsCustom(t *testing.T) {
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-suspect-skip")))
	}))
	t.Cleanup(custom.Close)
	cfg := testGatewayConfig(
		map[string][]string{
			"a": {"direct"},
			"z": {"direct", "http://127.0.0.1:8081"},
		},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-suspect-skip-a", "zen-key-suspect-skip-b"}
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm-suspect-skip"}
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	ses := "ses_unbound_suspect_skip_custom_1"
	cands := gw.scheduler.orderCandidates(
		gw.scheduler.buildAuthCandidates(TierZen, gw.credentials(), gw.pools["z"], "m", 9223372036854775807),
		ses,
	)
	if len(cands) != 4 {
		t.Fatalf("frozen=%d want 4", len(cands))
	}
	firstCred := cands[0].CredID
	var firstKey string
	for _, c := range gw.credentials() {
		if c.id == firstCred {
			firstKey = c.key
		}
	}
	var z0, z1 atomic.Int32
	mkTransport := func(total *atomic.Int32) func(*http.Request) (*http.Response, error) {
		return func(r *http.Request) (*http.Response, error) {
			total.Add(1)
			if authHeaderKey(r) == firstKey {
				return nil, errors.New("dial timeout")
			}
			return responseWithBody(200, `{"error":"must be skipped"}`), nil
		}
	}
	postStub(t, gw, "z", 0, nil, nil, mkTransport(&z0))
	postStub(t, gw, "z", 1, nil, nil, mkTransport(&z1))
	resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0, unboundExhaustionExtra())
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("skip-exhaustion must take over custom, got %d %+v", resp.StatusCode, eff)
	}
	if customHits.Load() != 1 {
		t.Fatalf("must hit custom once, got %d", customHits.Load())
	}
	if got := int(z0.Load() + z1.Load()); got != 2 {
		t.Fatalf("native real sends=%d want 2 (first cred both proxies; second cred both skipped)", got)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3 (2 native + 1 custom; skips are not attempts)", attempts)
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("must bind fallback")
	}
}

// 3) Non-qualifying 408 does not trigger cross-credential suspect skip.
func TestUnboundAuthTransportSuspectIgnores408(t *testing.T) {
	gw := twoCredTwoProxyGateway(t, 1)
	ses := "ses_unbound_suspect_ignore_408_1"
	cands := gw.scheduler.orderCandidates(
		gw.scheduler.buildAuthCandidates(TierZen, gw.credentials(), gw.pools["z"], "m", 9223372036854775807),
		ses,
	)
	if len(cands) != 4 {
		t.Fatalf("frozen=%d want 4", len(cands))
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
	firstDirect := cands[0].ProxyRaw
	if cands[2].ProxyRaw != firstDirect {
		t.Fatalf("fixture assumption broken")
	}
	poolItems := gw.pools["z"].items
	idxOf := func(raw string) int {
		for i, p := range poolItems {
			if p != nil && p.name == raw {
				return i
			}
		}
		return -1
	}
	directIdx := idxOf(firstDirect)
	otherIdx := 1 - directIdx
	var directPosts atomic.Int32
	postStub(t, gw, "z", directIdx, nil, nil, func(r *http.Request) (*http.Response, error) {
		directPosts.Add(1)
		if authHeaderKey(r) == firstKey {
			return responseWithBody(408, `{"error":"timeout"}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})
	postStub(t, gw, "z", otherIdx, nil, nil, func(r *http.Request) (*http.Response, error) {
		if authHeaderKey(r) == firstKey {
			return responseWithBody(500, `{"error":"flaky"}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})
	resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0)
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	if got := int(directPosts.Load()); got != 2 {
		t.Fatalf("408 must not trigger suspect skip: direct POSTs=%d want 2 (first-cred 408 + second-cred send)", got)
	}
	if _, _, ok := gw.scheduler.suspectCooldownStatus(TierZen, "z", firstDirect); ok {
		t.Fatalf("408 must not write transport-suspect cooldown")
	}
}

// 4) Expired suspect does not skip despite request-local membership.
func TestShouldSkipRequestLocalSuspectExpiry(t *testing.T) {
	id := suspectIdentity(TierZen, "z", "direct")
	local := map[string]struct{}{id: {}}
	if !shouldSkipRequestLocalSuspect(local, TierZen, "z", "direct", true) {
		t.Fatalf("active suspect with membership must skip")
	}
	if shouldSkipRequestLocalSuspect(local, TierZen, "z", "direct", false) {
		t.Fatalf("expired suspect must not skip despite membership")
	}
	if shouldSkipRequestLocalSuspect(nil, TierZen, "z", "direct", true) {
		t.Fatalf("missing membership must not skip")
	}
	if shouldSkipRequestLocalSuspect(local, TierZen, "z", "http://127.0.0.1:8081", true) {
		t.Fatalf("different proxy must not skip")
	}
	// Controlled scheduler state: active then expired, no sleep.
	gw := twoCredTwoProxyGateway(t, 1)
	gw.scheduler.noteTransportSuspect(TierZen, "z", "direct", AttemptClassTransportFailure, 0, time.Now().UnixNano())
	_, _, active := gw.scheduler.suspectCooldownStatus(TierZen, "z", "direct")
	if !active {
		t.Fatalf("fresh suspect must be active")
	}
	if !shouldSkipRequestLocalSuspect(local, TierZen, "z", "direct", active) {
		t.Fatalf("fresh scheduler suspect with membership must skip")
	}
	gw.scheduler.mu.Lock()
	if e := gw.scheduler.suspectState[id]; e != nil {
		e.cooldownUntil = time.Now().Add(-time.Second).UnixNano()
	}
	gw.scheduler.mu.Unlock()
	_, _, active = gw.scheduler.suspectCooldownStatus(TierZen, "z", "direct")
	if active {
		t.Fatalf("manipulated suspect must read expired")
	}
	if shouldSkipRequestLocalSuspect(local, TierZen, "z", "direct", active) {
		t.Fatalf("expired scheduler suspect must not skip despite membership")
	}
}

// 5) Different pool identity not matched.
func TestRequestLocalSuspectIdentityIsPoolQualified(t *testing.T) {
	a := suspectIdentity(TierZen, "a", "direct")
	z := suspectIdentity(TierZen, "z", "direct")
	if a == z {
		t.Fatalf("same raw URL in distinct pools must not share identity")
	}
	set := map[string]struct{}{a: {}}
	if shouldSkipRequestLocalSuspect(set, TierZen, "z", "direct", true) {
		t.Fatalf("distinct pool identity must not skip")
	}
	if !shouldSkipRequestLocalSuspect(set, TierZen, "a", "direct", true) {
		t.Fatalf("same identity must skip when active")
	}
}
