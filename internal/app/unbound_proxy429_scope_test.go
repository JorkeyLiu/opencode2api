package app

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// Unbound authenticated request-local proxy429 L2 cause: a live 429 via a
// real send establishes a stable unavailable cause for its
// (tier/channel, pool, raw proxy) identity shared across credentials in the
// same request. Later frozen candidates with the same identity skip without
// a POST; skips count as unavailable+entered without attempts/writes.

// 1) First credential live-429s a proxy; later credential skips that same
// proxy without POST but succeeds on another proxy and pins. Attempts count
// real sends only.
func TestUnboundAuthProxy429SkipsSameProxyAcrossCreds(t *testing.T) {
	gw := twoCredTwoProxyGateway(t, 1)
	ses := "ses_unbound_proxy429_scope_1"
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
	// Both credentials prefer "direct" first (verified probe); cands[0] and
	// cands[2] share proxy "direct". First cred: direct=>429, other=>500
	// (L1-final unavailable, no proxy429). Second cred: direct must skip,
	// other=>200.
	firstDirect := cands[0].ProxyRaw
	secondFirst := cands[2].ProxyRaw
	if firstDirect != secondFirst {
		t.Fatalf("fixture assumption broken: first proxies differ %q vs %q", firstDirect, secondFirst)
	}
	var directPosts, otherPosts atomic.Int32
	var firstCredPosts, secondCredPosts atomic.Int32
	poolItems := gw.pools["z"].items
	idxOf := func(raw string) int {
		for i, p := range poolItems {
			if p != nil && p.name == raw {
				return i
			}
		}
		return -1
	}
	otherRaw := ""
	for _, p := range poolItems {
		if p != nil && p.name != firstDirect {
			otherRaw = p.name
		}
	}
	_ = otherRaw
	stubFn := func(r *http.Request) (*http.Response, error) {
		k := authHeaderKey(r)
		if k == firstKey {
			firstCredPosts.Add(1)
			// Distinguish proxy by caller? postStub wrapper per-index counts
			// direct/other separately; here decide by per-index fn below.
			return responseWithBody(429, `{"error":"slow"}`), nil
		}
		if k == secondKey {
			secondCredPosts.Add(1)
			return responseWithBody(200, `{"ok":true}`), nil
		}
		return responseWithBody(500, `{"error":"unexpected key"}`), nil
	}
	_ = stubFn
	directIdx := idxOf(firstDirect)
	otherIdx := 1 - directIdx
	if directIdx < 0 || otherIdx < 0 {
		t.Fatalf("proxy idx missing direct=%d", directIdx)
	}
	// direct proxy: first cred 429, second cred must never POST (skip).
	postStub(t, gw, "z", directIdx, nil, nil, func(r *http.Request) (*http.Response, error) {
		directPosts.Add(1)
		k := authHeaderKey(r)
		if k == firstKey {
			firstCredPosts.Add(1)
			rr := responseWithBody(429, `{"error":"slow"}`)
			rr.Header.Set("Retry-After", "9")
			return rr, nil
		}
		secondCredPosts.Add(1)
		return responseWithBody(200, `{"error":"must be skipped, got second-cred POST on direct"}`), nil
	})
	// other proxy: first cred 500 (unavailable, no proxy429), second cred 200.
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
		t.Fatalf("direct proxy POSTs=%d want 1 (first-cred 429 only, second-cred skipped)", got)
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
}

// 2) Prior live 429s covering every frozen proxy of a later credential make
// its domain entered+exhausted via skips; active custom takes over only when
// all unbound domains are otherwise exhausted. Skips are not attempts.
func TestUnboundAuthProxy429SkipsExhaustLaterCredentialAllowsCustom(t *testing.T) {
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-proxy429-skip")))
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
	cfg.Keys = []string{"zen-key-proxy429-skip-a", "zen-key-proxy429-skip-b"}
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	ch := FallbackChannelConfig{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm-proxy429-skip"}
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{ch}}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	ses := "ses_unbound_proxy429_skip_custom_1"
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
	var z0, z1 atomic.Int32
	postStub(t, gw, "z", 0, nil, nil, func(r *http.Request) (*http.Response, error) {
		z0.Add(1)
		k := authHeaderKey(r)
		if k == firstKey {
			rr := responseWithBody(429, `{"error":"slow"}`)
			rr.Header.Set("Retry-After", "9")
			return rr, nil
		}
		return responseWithBody(200, `{"error":"must be skipped"}`), nil
	})
	postStub(t, gw, "z", 1, nil, nil, func(r *http.Request) (*http.Response, error) {
		z1.Add(1)
		k := authHeaderKey(r)
		if k == firstKey {
			rr := responseWithBody(429, `{"error":"slow"}`)
			rr.Header.Set("Retry-After", "9")
			return rr, nil
		}
		return responseWithBody(200, `{"error":"must be skipped"}`), nil
	})
	_ = secondKey
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
	// 2 native + 1 custom.
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3 (skips are not attempts)", attempts)
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatalf("must bind fallback")
	}
}

// 3) One real live429 plus one request-local proxy skip for the same
// credential must NOT write credential429.
func TestUnboundAuthProxy429PartialPlusSkipNoCredential429(t *testing.T) {
	gw := twoCredTwoProxyGateway(t, 1)
	ses := "ses_unbound_proxy429_partial_1"
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
	// direct: first cred 429; second cred 429 would be skipped so stub must
	// never see second key (fail if it does).
	postStub(t, gw, "z", directIdx, nil, nil, func(r *http.Request) (*http.Response, error) {
		if authHeaderKey(r) == secondKey {
			return responseWithBody(200, `{"error":"must be skipped"}`), nil
		}
		if authHeaderKey(r) != firstKey {
			return responseWithBody(500, `{"error":"unexpected"}`), nil
		}
		rr := responseWithBody(429, `{"error":"slow"}`)
		rr.Header.Set("Retry-After", "9")
		return rr, nil
	})
	// other: first cred 500 (no proxy429), second cred live 429.
	postStub(t, gw, "z", otherIdx, nil, nil, func(r *http.Request) (*http.Response, error) {
		k := authHeaderKey(r)
		if k == firstKey {
			return responseWithBody(500, `{"error":"flaky"}`), nil
		}
		if k == secondKey {
			rr := responseWithBody(429, `{"error":"slow"}`)
			rr.Header.Set("Retry-After", "9")
			return rr, nil
		}
		return responseWithBody(500, `{"error":"unexpected"}`), nil
	})
	resp, _, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), pinIDs(ses, "r1"), 0)
	if err != nil || resp == nil {
		t.Fatalf("err=%v resp=%v", err, resp)
	}
	defer drainResp(resp)
	if resp.StatusCode != 429 {
		t.Fatalf("terminal must stay 429, got %d", resp.StatusCode)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3 (2 first-cred + 1 second-cred, 1 skip)", attempts)
	}
	// Second credential: 1 real live429 + 1 request-local skip => no credential429.
	var secondID string
	for _, c := range gw.credentials() {
		if c.key == secondKey {
			secondID = c.id
		}
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(secondID); ok {
		t.Fatalf("partial live429 + request-local skip must not write credential429")
	}
	var firstID string
	for _, c := range gw.credentials() {
		if c.key == firstKey {
			firstID = c.id
		}
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(firstID); ok {
		t.Fatalf("partial live429 + non-429 must not write credential429")
	}
	// Proxy429s from the two real 429 sends must exist (global layer intact).
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "z", firstDirect); !ok {
		t.Fatalf("direct proxy429 must exist from first-cred live 429")
	}
}

// 4) Request-local identity is (tier/channel, pool, raw proxy): same raw URL
// in a distinct pool must not match.
func TestRequestLocalProxy429IdentityIsPoolQualified(t *testing.T) {
	a := proxy429Identity(TierZen, "a", "direct")
	z := proxy429Identity(TierZen, "z", "direct")
	if a == z {
		t.Fatalf("same raw URL in distinct pools must not share identity")
	}
	set := map[string]struct{}{a: {}}
	if _, ok := set[z]; ok {
		t.Fatalf("distinct pool identity must not match request-local set")
	}
	if _, ok := set[a]; !ok {
		t.Fatalf("same identity must match")
	}
}
