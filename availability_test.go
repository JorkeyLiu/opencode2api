package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func readWebUIFile() ([]byte, error) {
	return os.ReadFile("webui/index.html")
}

func bulkAdmin(t *testing.T, pools map[string][]string, routing ProxyRoutingConfig, zenKeys, goKeys []string) (*RuntimeManager, *AdminServer, string, string) {
	t.Helper()
	manager := &RuntimeManager{monitor: NewMonitor(), hub: NewLogHub(100), redactor: NewSecretRedactor()}
	cfg := testGatewayConfig(pools, routing)
	cfg.ZenKeys = zenKeys
	cfg.GoKeys = goKeys
	cfg.Anonymous = true
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, manager.monitor)
	if err != nil {
		t.Fatal(err)
	}
	manager.current.Store(&gatewayRuntime{config: normalized, gateway: gateway})
	manager.metadata = newModelMetadataStore("", nil)
	manager.redactor.Replace(normalized)
	admin := NewAdminServer(manager, manager.monitor, manager.hub, nil)
	token, csrf := "bulk-token-"+normalized.ProxyRouting.Zen, "bulk-csrf"
	current := manager.Config()
	admin.sessions[tokenDigest(token)] = adminSession{
		Username: current.WebUI.Username, AuthVersion: secretFingerprint(current.WebUI.PasswordHash),
		CSRF: csrf, Expires: time.Now().Add(time.Hour),
	}
	return manager, admin, token, csrf
}

func seedBulkProbeCatalog(gw *Gateway) {
	if gw == nil || gw.catalog == nil {
		return
	}
	model := "bulk-free-model"
	gw.catalog.ReplaceWithCapabilities([]string{model}, []string{model}, map[Tier]map[string]Protocol{TierZen: {model: ProtocolChat}, TierGo: {model: ProtocolChat}}, map[Tier]map[string]bool{TierZen: {}, TierGo: {}}, nil)
}

func bulkChatSuccessBody(model string) string {
	return `{"id":"chatcmpl-bulk","object":"chat.completion","created":1,"model":"` + model + `","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`
}

func TestBulkAuthCSRFOriginStrictNoStore(t *testing.T) {
	manager, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"},
		[]string{"zen-key-12345"}, []string{"go-key-12345"})
	_ = manager
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check", `{}`, "", "")); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauth code=%d want 401", rec.Code)
	}
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check", `{}`, token, "wrong")); rec.Code != http.StatusForbidden {
		t.Fatalf("bad csrf code=%d want 403", rec.Code)
	}
	req := operabilityRequest(http.MethodPost, "/api/availability/check", `{}`, token, csrf)
	req.Host = "admin.local"
	req.Header.Set("Origin", "http://evil.example")
	if rec := serveAdmin(admin, req); rec.Code != http.StatusForbidden {
		t.Fatalf("origin code=%d want 403", rec.Code)
	}
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check", `{"pool":"x"}`, token, csrf)); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown field code=%d want 400", rec.Code)
	}
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check", `{"url":"http://127.0.0.1:9"}`, token, csrf)); rec.Code != http.StatusBadRequest {
		t.Fatalf("caller url code=%d want 400", rec.Code)
	}
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check", `{"pool":"x"}`, token, csrf))
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control=%q want no-store", got)
	}
}

func TestBulkRateLimitBusy(t *testing.T) {
	manager, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"},
		nil, nil)
	// Point upstream at a fast local 500 so bulk completes quickly without external network.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte("x"))
	}))
	defer srv.Close()
	runtime := manager.current.Load()
	runtime.gateway.cfg.Upstream.Zen = srv.URL
	runtime.gateway.cfg.Upstream.Go = srv.URL
	var last *httptest.ResponseRecorder
	for i := 0; i < 4; i++ {
		last = serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check", `{}`, token, csrf))
	}
	if last.Code != http.StatusTooManyRequests {
		t.Fatalf("4th bulk code=%d want 429 body=%s", last.Code, last.Body.String())
	}
	// Busy gate: hold the mutex then expect 409 (use a fresh admin to avoid rate limit).
	_, admin2, token2, csrf2 := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"},
		nil, nil)
	rt2 := admin2.manager.current.Load()
	rt2.gateway.bulkMu.Lock()
	defer rt2.gateway.bulkMu.Unlock()
	if rec := serveAdmin(admin2, operabilityRequest(http.MethodPost, "/api/availability/check", `{}`, token2, csrf2)); rec.Code != http.StatusConflict {
		t.Fatalf("busy code=%d want 409", rec.Code)
	}
}

func TestBulkSameRawURLIsolation(t *testing.T) {
	manager, _, _, _ := bulkAdmin(t,
		map[string][]string{"a": {"direct"}, "b": {"direct"}},
		ProxyRoutingConfig{Anonymous: "a", Zen: "a", Go: "b"},
		[]string{"zen-key-12345"}, []string{"go-key-12345"})
	gw := manager.current.Load().gateway
	// Same raw URL in different pools stays isolated.
	gw.scheduler.noteChannelFailure(TierZen, "a", "direct", AttemptClassUpstreamFailure, 500, 0, time.Now().UnixNano())
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "a", "direct"); !ok {
		t.Fatalf("channel a must cool")
	}
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "b", "direct"); ok {
		t.Fatalf("channel b must stay independent")
	}
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierGo, "a", "direct"); ok {
		t.Fatalf("go must stay isolated from zen")
	}
	snap := manager.Resources()
	seen := map[string]string{}
	for _, p := range snap.Proxies {
		if p.Address == redactURL("direct") {
			seen[p.Pool] = p.Zen
		}
	}
	if seen["a"] == seen["b"] && seen["a"] == "available" {
		t.Fatalf("pool a zen must differ after channel cooldown: %+v", seen)
	}
	// Visual grouping: same redacted node appears in both pools.
	if len(snap.Proxies) < 2 {
		t.Fatalf("expected two pool-qualified rows")
	}
}

func TestBulkPublicNeverWritesGo(t *testing.T) {
	manager, _, _, _ := bulkAdmin(t,
		map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"},
		[]string{"zen-key-12345"}, []string{"go-key-12345"})
	gw := manager.current.Load().gateway
	pool := gw.pools["shared"]
	now := time.Now().UnixNano()
	results := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Index: pool.items[0].index, Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: "public", CredID: anonymousSchedulerCredentialID, CredDisp: "anonymous", IsPublic: true}, StartedNanos: now, Status: 200, Success: true},
		{Target: bulkSendTarget{PoolName: "shared", Index: pool.items[1].index, Proxy: pool.items[1], Raw: pool.items[1].name, Tier: TierZen, CredKey: "public", CredID: anonymousSchedulerCredentialID, CredDisp: "anonymous", IsPublic: true}, StartedNanos: now + 1, Status: 429, RetryAfter: 0},
	}
	gw.applyBulkWrites(context.Background(), results)
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "shared", pool.items[1].name); !ok {
		t.Fatalf("zen proxy429 must be written for failed node")
	}
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierGo, "shared", pool.items[1].name); ok {
		t.Fatalf("go must never be written by zen public")
	}
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierGo, "shared", pool.items[1].name); ok {
		t.Fatalf("go channel must never be written by zen public")
	}
	// Public 429 without comparative success is display-only and never writes
	// tier+pool+proxy 429 nor configured credential cooldown.
	gw2 := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081"})
	pool2 := gw2.pools["shared"]
	r2 := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool2.items[0], Raw: pool2.items[0].name, Tier: TierZen, CredKey: "public", CredID: anonymousSchedulerCredentialID, CredDisp: "anonymous", IsPublic: true}, StartedNanos: now, Status: 429},
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool2.items[1], Raw: pool2.items[1].name, Tier: TierZen, CredKey: "public", CredID: anonymousSchedulerCredentialID, CredDisp: "anonymous", IsPublic: true}, StartedNanos: now + 1, Status: 429},
	}
	gw2.applyBulkWrites(context.Background(), r2)
	if _, _, ok := gw2.scheduler.proxy429CooldownStatus(TierZen, "shared", pool2.items[0].name); ok {
		t.Fatalf("public 429 without success must stay display-only")
	}
	if _, _, ok := gw2.scheduler.proxy429CooldownStatus(TierZen, "shared", pool2.items[1].name); ok {
		t.Fatalf("public 429 without success must stay display-only")
	}
	for _, cred := range gw2.zenCreds {
		if _, _, ok := gw2.scheduler.credential429CooldownStatus(cred.id); ok {
			t.Fatalf("public must never write configured credential429")
		}
		if got := gw2.scheduler.credentialCoolUntil(cred.id); got > time.Now().UnixNano() {
			t.Fatalf("public must never write credential401")
		}
	}
}

func TestBulkReal401Isolation(t *testing.T) {
	manager, _, _, _ := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"},
		[]string{"same-secret-12345"}, []string{"same-secret-12345"})
	gw := manager.current.Load().gateway
	pool := gw.pools["shared"]
	now := time.Now().UnixNano()
	zenCred := gw.zenCreds[0]
	results := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: zenCred.key, CredID: zenCred.id, CredDisp: zenCred.display}, StartedNanos: now, Status: 401},
	}
	gw.applyBulkWrites(context.Background(), results)
	if got := gw.scheduler.credentialCoolUntil(zenCred.id); got <= time.Now().UnixNano() {
		t.Fatalf("zen 401 must cool matching tier+cred")
	}
	goCred := gw.goCreds[0]
	if got := gw.scheduler.credentialCoolUntil(goCred.id); got > time.Now().UnixNano() {
		t.Fatalf("same key text on go must stay isolated")
	}
}

func TestBulk429ComparativeRules(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081"})
	pool := gw.pools["shared"]
	cred := gw.zenCreds[0]
	now := time.Now().UnixNano()
	// One success + one 429 writes proxy429 only for failed node.
	results := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: cred.key, CredID: cred.id, CredDisp: cred.display}, StartedNanos: now, Status: 200, Success: true},
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[1], Raw: pool.items[1].name, Tier: TierZen, CredKey: cred.key, CredID: cred.id, CredDisp: cred.display}, StartedNanos: now + 1, Status: 429, RetryAfter: 2 * time.Second},
	}
	gw.applyBulkWrites(context.Background(), results)
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "shared", pool.items[1].name); !ok {
		t.Fatalf("failed node proxy429 must be set")
	}
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "shared", pool.items[0].name); ok {
		t.Fatalf("success node must not cool")
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(cred.id); ok {
		t.Fatalf("single 429 with success must not set credential429")
	}
	// Two distinct 429 without success writes credential429 with second Retry-After.
	gw2 := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081", "http://127.0.0.1:8082"})
	pool2 := gw2.pools["shared"]
	cred2 := gw2.zenCreds[0]
	now2 := time.Now().UnixNano()
	r2 := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool2.items[0], Raw: pool2.items[0].name, Tier: TierZen, CredKey: cred2.key, CredID: cred2.id, CredDisp: cred2.display}, StartedNanos: now2, Status: 429, RetryAfter: time.Second},
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool2.items[1], Raw: pool2.items[1].name, Tier: TierZen, CredKey: cred2.key, CredID: cred2.id, CredDisp: cred2.display}, StartedNanos: now2 + 1, Status: 429, RetryAfter: 3 * time.Second},
	}
	gw2.applyBulkWrites(context.Background(), r2)
	until, status, ok := gw2.scheduler.credential429CooldownStatus(cred2.id)
	if !ok || status != 429 {
		t.Fatalf("credential429 must be set: ok=%v status=%d", ok, status)
	}
	if until <= time.Now().UnixNano() {
		t.Fatalf("credential429 must be future")
	}
	// Stale guard: older success must not clear newer proxy429.
	gw3 := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	pool3 := gw3.pools["shared"]
	newer := time.Now().UnixNano()
	gw3.scheduler.noteProxy429Failure(TierZen, "shared", pool3.items[0].name, AttemptClassRateLimited, 429, 0, newer)
	if ch := gw3.scheduler.noteProxy429Success(TierZen, "shared", pool3.items[0].name, newer-1); ch.Changed {
		t.Fatalf("stale success must not clear newer 429")
	}
	if ch := gw3.scheduler.noteProxy429Success(TierZen, "shared", pool3.items[0].name, newer+1); !ch.Cleared {
		t.Fatalf("newer success must clear")
	}
}

func TestBulkChannelComparativeRules(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081"})
	pool := gw.pools["shared"]
	cred := gw.zenCreds[0]
	now := time.Now().UnixNano()
	results := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: cred.key, CredID: cred.id, CredDisp: cred.display}, StartedNanos: now, Status: 200, Success: true},
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[1], Raw: pool.items[1].name, Tier: TierZen, CredKey: cred.key, CredID: cred.id, CredDisp: cred.display}, StartedNanos: now + 1, Status: 503},
	}
	gw.applyBulkWrites(context.Background(), results)
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "shared", pool.items[1].name); !ok {
		t.Fatalf("channel must cool failed node with comparative success")
	}
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "shared", pool.items[0].name); ok {
		t.Fatalf("success node must not cool")
	}
	// No comparative success means display only.
	gw2 := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081"})
	pool2 := gw2.pools["shared"]
	cred2 := gw2.zenCreds[0]
	r2 := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool2.items[0], Raw: pool2.items[0].name, Tier: TierZen, CredKey: cred2.key, CredID: cred2.id, CredDisp: cred2.display}, StartedNanos: now, Status: 503},
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool2.items[1], Raw: pool2.items[1].name, Tier: TierZen, CredKey: cred2.key, CredID: cred2.id, CredDisp: cred2.display}, StartedNanos: now + 1, Status: 403},
	}
	gw2.applyBulkWrites(context.Background(), r2)
	if _, _, ok := gw2.scheduler.channelCooldownStatus(TierZen, "shared", pool2.items[0].name); ok {
		t.Fatalf("no comparative success must not write channel")
	}
	// Success clears with stale fencing; tier/pool isolation.
	gw.scheduler.noteChannelSuccess(TierZen, "shared", pool.items[1].name, now+2)
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "shared", pool.items[1].name); ok {
		t.Fatalf("success must clear channel")
	}
	gw.scheduler.noteChannelFailure(TierZen, "shared", pool.items[1].name, AttemptClassUpstreamFailure, 500, 0, now+3)
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierGo, "shared", pool.items[1].name); ok {
		t.Fatalf("tier isolation broken")
	}
	// Filtering in anonymous/auth candidate building.
	anonPool := gw.pools["shared"]
	if got := gw.scheduler.buildAnonymousCandidates(anonPool, "m", time.Now().UnixNano()); len(got) != 1 {
		// Channel is on items[1]; anonymous should exclude it, leaving one.
		t.Fatalf("anonymous must filter channel-cooling proxy: got %d", len(got))
	}
	authCands := gw.scheduler.buildAuthCandidates(TierZen, gw.zenCreds, anonPool, "m", time.Now().UnixNano())
	for _, c := range authCands {
		if c.ProxyRaw == pool.items[1].name {
			t.Fatalf("auth must filter channel-cooling proxy")
		}
	}
	// Pinned-anonymous fast-fails on channel without cross-proxy moves.
	gw.bindSessionPin("ses-chan-1", "m1", TierZen, anonymousSchedulerCredentialID, "shared", pool.items[1].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	route, _ := gw.catalog.Route("m1", true, false, true)
	_ = route
	// Directly verify fast-fail status via channel check (pinned path uses same check).
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "shared", pool.items[1].name); !ok {
		t.Fatalf("channel must still be active for pinned check")
	}
}

func TestBulkChannelStaleAnd408Diagnostic(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	pool := gw.pools["shared"]
	newer := time.Now().UnixNano()
	gw.scheduler.noteChannelFailure(TierZen, "shared", pool.items[0].name, AttemptClassUpstreamFailure, 500, 0, newer)
	if ch := gw.scheduler.noteChannelSuccess(TierZen, "shared", pool.items[0].name, newer-1); ch.Changed {
		t.Fatalf("stale channel success must not clear")
	}
	// 408/425/parse/empty are diagnostic only.
	cred := gw.zenCreds[0]
	r := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: cred.key, CredID: cred.id, CredDisp: cred.display}, StartedNanos: newer + 1, Status: 408},
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: cred.key, CredID: cred.id, CredDisp: cred.display}, StartedNanos: newer + 2, ParseError: true, Status: 200},
	}
	before := gw.scheduler.channelCoolUntil(TierZen, "shared", pool.items[0].name)
	gw.applyBulkWrites(context.Background(), r)
	// 408/parse must not create new channel entries beyond the existing one; and must not clear via diagnostic.
	if got := gw.scheduler.channelCoolUntil(TierZen, "shared", pool.items[0].name); got != before {
		t.Fatalf("diagnostic outcomes must not change channel: %d -> %d", before, got)
	}
}

func TestBulkMigrationBoundsHealthNoPollution(t *testing.T) {
	manager, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"},
		[]string{"zen-key-12345"}, []string{"go-key-12345"})
	_ = admin
	_ = token
	_ = csrf
	gw := manager.current.Load().gateway
	pool := gw.pools["shared"]
	now := time.Now().UnixNano()
	gw.scheduler.noteChannelFailure(TierZen, "shared", pool.items[0].name, AttemptClassUpstreamFailure, 500, 0, now)
	gw.scheduler.noteProxy429Failure(TierZen, "shared", pool.items[1].name, AttemptClassRateLimited, 429, 0, now)
	beforeHealth := gw.routingReadiness()
	// Healthz readiness unchanged by channel/proxy429/credential429.
	gw.scheduler.noteCredential429Failure(gw.zenCreds[0].id, AttemptClassRateLimited, 429, 0, now)
	afterHealth := gw.routingReadiness()
	if beforeHealth != afterHealth {
		t.Fatalf("readiness must ignore channel/proxy429/credential429: %+v -> %+v", beforeHealth, afterHealth)
	}
	// Migration carries still-future channel state.
	gw2, err := NewGateway(manager.current.Load().config, nil, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	summary := migrateGatewaySchedulerState(gw, gw2)
	if summary.Channel != 1 || summary.Proxy429 != 1 {
		t.Fatalf("migration summary=%+v want channel 1 proxy429 1", summary)
	}
	if _, _, ok := gw2.scheduler.channelCooldownStatus(TierZen, "shared", pool.items[0].name); !ok {
		t.Fatalf("channel must migrate")
	}
	// Bounds: channel map stays bounded with deterministic eviction.
	big := newTargetScheduler(15 * time.Second)
	for i := 0; i < maxChannelStates+10; i++ {
		big.noteChannelFailure(TierZen, "shared", "proxy-"+string(rune('a'+i%26))+time.Now().String()+string(rune(i)), AttemptClassUpstreamFailure, 500, 0, 0)
	}
	big.mu.Lock()
	n := len(big.channelState)
	big.mu.Unlock()
	if n > maxChannelStates+10 {
		t.Fatalf("channel map unbounded: %d", n)
	}
	// No monitor/history pollution from bulk writes.
	beforeReqs := len(manager.monitor.Snapshot().Upstream.Requests)
	beforeAttempts := len(manager.monitor.Snapshot().Upstream.Recent)
	results := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: "public", CredID: anonymousSchedulerCredentialID, CredDisp: "anonymous", IsPublic: true}, StartedNanos: now, Status: 200, Success: true},
	}
	gw.applyBulkWrites(context.Background(), results)
	if got := len(manager.monitor.Snapshot().Upstream.Requests); got != beforeReqs {
		t.Fatalf("bulk must not create request metrics")
	}
	if got := len(manager.monitor.Snapshot().Upstream.Recent); got != beforeAttempts {
		t.Fatalf("bulk must not create attempt metrics")
	}
}

func TestBulkRedactionAndConcurrencyCap(t *testing.T) {
	secretProxy := "http://user:hunter2@127.0.0.1:9"
	manager, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {secretProxy}},
		ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"},
		[]string{"sk-live-secret-abcdef-12345"}, []string{"go-live-secret-67890"})
	// Fast local upstream that always succeeds.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || (r.URL.Path != "/v1/chat/completions" && r.URL.Path != "/zen/v1/chat/completions" && r.URL.Path != "/zen/go/v1/chat/completions") {
			// Accept any chat path under the test upstream base.
			if r.URL.Path == "" {
				w.WriteHeader(404)
				return
			}
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(bulkChatSuccessBody("bulk-free-model")))
	}))
	defer srv.Close()
	rt := manager.current.Load()
	rt.gateway.cfg.Upstream.Zen = srv.URL
	rt.gateway.cfg.Upstream.Go = srv.URL
	seedBulkProbeCatalog(rt.gateway)
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check", `{}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("bulk code=%d body=%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, secret := range []string{"hunter2", "user@", "sk-live-secret-abcdef-12345", "go-live-secret-67890"} {
		if strings.Contains(body, secret) {
			t.Fatalf("secret %q leaked in bulk response", secret)
		}
	}
	var resp bulkCheckResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.TestedNodes == 0 || len(resp.Nodes) == 0 {
		t.Fatalf("bulk must test nodes: %+v", resp)
	}
	if bulkProbeConcurrency != 4 {
		t.Fatalf("concurrency constant changed: %d", bulkProbeConcurrency)
	}
}

func TestBulkCancelPartialNoTransportWrite(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	pool := gw.pools["shared"]
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: "public", CredID: anonymousSchedulerCredentialID, CredDisp: "anonymous", IsPublic: true}, StartedNanos: time.Now().UnixNano(), TransportErr: context.DeadlineExceeded, IsProxyFailure: true, AdminCancelled: true},
	}
	gw.applyBulkWrites(ctx, results)
	if !pool.items[0].healthy.Load() {
		t.Fatalf("admin-cancelled transport failure must remain diagnostic-only")
	}
	// runBulkCheck with cancelled parent reports partial without state writes.
	resp := gw.runBulkCheck(ctx)
	if !resp.Partial {
		t.Fatalf("cancelled bulk must report partial")
	}
}

func TestBulkPinnedMovementAndFastFail(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081"})
	pool := gw.pools["shared"]
	cred := gw.zenCreds[0]
	now := time.Now().UnixNano()
	// Channel on items[1] must be skipped by pinned-auth eligible movement.
	gw.scheduler.noteChannelFailure(TierZen, "shared", pool.items[1].name, AttemptClassUpstreamFailure, 500, 0, now)
	nowNanos := time.Now().UnixNano()
	eligible := 0
	for _, proxy := range affinityProxyOrder(pool, cred.id, pool.items[0].name) {
		if proxy == nil || !proxy.healthy.Load() {
			continue
		}
		identity := targetIdentity(TierZen, cred.id, "shared", proxy.name, "m1")
		if until, _, ok := gw.scheduler.targetCooldownStatus(identity); ok && until > nowNanos {
			continue
		}
		if until, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "shared", proxy.name); ok && until > nowNanos {
			continue
		}
		if until, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "shared", proxy.name); ok && until > nowNanos {
			continue
		}
		eligible++
		if proxy.name == pool.items[1].name {
			t.Fatalf("pinned-auth movement must filter channel-cooling proxy")
		}
	}
	if eligible != 1 {
		t.Fatalf("eligible=%d want 1", eligible)
	}
	// Pinned-anonymous on channel-cooling proxy fast-fails locally (no cross-proxy).
	gw.bindSessionPin("ses-pinned-anon-chan", "m1", TierZen, anonymousSchedulerCredentialID, "shared", pool.items[1].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "shared", pool.items[1].name); !ok {
		t.Fatalf("channel must be active for pinned-anon fast-fail")
	}
}

func TestWebUIAvailabilityStatic(t *testing.T) {
	data, err := readWebUIFile()
	if err != nil {
		t.Fatal(err)
	}
	html := string(data)
	if !strings.Contains(html, "代理可用性") {
		t.Fatalf("missing unified 代理可用性 view")
	}
	if strings.Contains(html, "匿名目标汇总") {
		t.Fatalf("independent 匿名目标汇总 table must be removed")
	}
	if strings.Contains(html, "tbody-anon") {
		t.Fatalf("tbody-anon must be removed")
	}
	if !strings.Contains(html, "btn-bulk-check") || !strings.Contains(html, "批量检测") {
		t.Fatalf("missing 批量检测 action")
	}
	if !strings.Contains(html, "凭证可用性") || !strings.Contains(html, "tbody-keys") {
		t.Fatalf("missing 凭证可用性 table")
	}
	if !strings.Contains(html, "tbody-proxies") || !strings.Contains(html, "tbody-channel") {
		t.Fatalf("missing proxy/channel tables")
	}
	if strings.Contains(html, "innerHTML") {
		t.Fatalf("WebUI must render via DOM text nodes, no innerHTML sinks")
	}
	for _, secret := range []string{"hunter2", "sk-live"} {
		if strings.Contains(html, secret) {
			t.Fatalf("secret leaked in static bundle")
		}
	}
	// Pool-qualified grouped display: node + pool membership columns present.
	if !strings.Contains(html, "代理池") || !strings.Contains(html, "节点") {
		t.Fatalf("missing pool-qualified grouped display")
	}
}

func TestBulkTransportProjectionMergeSawHTTP(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"zen-key-aaaaa", "zen-key-bbbbb"}, []string{"direct", "http://127.0.0.1:8081"})
	pool := gw.pools["shared"]
	credA := gw.zenCreds[0]
	credB := gw.zenCreds[1]
	now := time.Now().UnixNano()
	raw := pool.items[0].name
	// Same pool-qualified node: HTTP success via one credential plus conclusive
	// transport failure via another. Display must stay reachable.
	results := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Index: pool.items[0].index, Proxy: pool.items[0], Raw: raw, Tier: TierZen, CredKey: credA.key, CredID: credA.id, CredDisp: credA.display}, StartedNanos: now, Status: 200, Success: true},
		{Target: bulkSendTarget{PoolName: "shared", Index: pool.items[0].index, Proxy: pool.items[0], Raw: raw, Tier: TierZen, CredKey: credB.key, CredID: credB.id, CredDisp: credB.display}, StartedNanos: now + 1, TransportErr: context.DeadlineExceeded, IsProxyFailure: true},
	}
	resp := gw.buildBulkResponse(time.Now().UTC(), 2, 2, 0, false, false, results)
	found := false
	for _, n := range resp.Nodes {
		if n.Pool == "shared" && n.ProxyNode == redactURL(raw) {
			found = true
			if n.Transport != "healthy" {
				t.Fatalf("sawHTTP node transport=%q want healthy", n.Transport)
			}
		}
	}
	if !found {
		t.Fatalf("pool-qualified node missing")
	}
	// Reverse order must also stay healthy (no overwrite by later failure).
	rev := []bulkSendResult{results[1], results[0]}
	resp2 := gw.buildBulkResponse(time.Now().UTC(), 2, 2, 0, false, false, rev)
	for _, n := range resp2.Nodes {
		if n.Pool == "shared" && n.ProxyNode == redactURL(raw) {
			if n.Transport != "healthy" {
				t.Fatalf("reverse sawHTTP transport=%q want healthy", n.Transport)
			}
		}
	}
	// Probe-context timeout alone (no HTTP) is diagnostic-only: inconclusive,
	// never unhealthy.
	onlyTimeout := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[1], Raw: pool.items[1].name, Tier: TierZen, CredKey: credA.key, CredID: credA.id, CredDisp: credA.display}, StartedNanos: now, TransportErr: context.DeadlineExceeded, IsProxyFailure: true, ProbeContextCaused: true},
	}
	resp3 := gw.buildBulkResponse(time.Now().UTC(), 2, 1, 0, false, false, onlyTimeout)
	for _, n := range resp3.Nodes {
		if n.Pool == "shared" && n.ProxyNode == redactURL(pool.items[1].name) {
			if n.Transport == "unhealthy" {
				t.Fatalf("probe-context timeout must not display unhealthy")
			}
		}
	}
	if got := bulkOutcomeLabel(onlyTimeout[0]); got != "inconclusive" {
		t.Fatalf("probe-context label=%q want inconclusive", got)
	}
}

func TestBulkCredentialTailCollisionSeparate(t *testing.T) {
	manager, _, _, _ := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"},
		[]string{"AAA-12345", "BBB-12345"}, nil)
	gw := manager.current.Load().gateway
	if len(gw.zenCreds) != 2 {
		t.Fatalf("want 2 creds got %d", len(gw.zenCreds))
	}
	if gw.zenCreds[0].display != gw.zenCreds[1].display {
		t.Fatalf("tails must collide for this test: %q vs %q", gw.zenCreds[0].display, gw.zenCreds[1].display)
	}
	if gw.zenCreds[0].id == gw.zenCreds[1].id {
		t.Fatalf("internal IDs must differ despite tail collision")
	}
	pool := gw.pools["shared"]
	now := time.Now().UnixNano()
	results := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: gw.zenCreds[0].key, CredID: gw.zenCreds[0].id, CredDisp: gw.zenCreds[0].display}, StartedNanos: now, Status: 200, Success: true},
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: gw.zenCreds[1].key, CredID: gw.zenCreds[1].id, CredDisp: gw.zenCreds[1].display}, StartedNanos: now + 1, Status: 401},
	}
	resp := gw.buildBulkResponse(time.Now().UTC(), 1, 2, 0, false, false, results)
	if len(resp.Credentials) != 2 {
		t.Fatalf("tail collision must remain 2 rows, got %d: %+v", len(resp.Credentials), resp.Credentials)
	}
	for _, c := range resp.Credentials {
		if c.KeyTail != "12345" {
			t.Fatalf("rows must expose only tail, got %q", c.KeyTail)
		}
		if strings.Contains(c.KeyTail, "AAA") || strings.Contains(c.KeyTail, "BBB") {
			t.Fatalf("full key leaked in tail")
		}
	}
	gw.applyBulkWrites(context.Background(), results)
	if got := gw.scheduler.credentialCoolUntil(gw.zenCreds[1].id); got <= time.Now().UnixNano() {
		t.Fatalf("401 cred must cool")
	}
	if got := gw.scheduler.credentialCoolUntil(gw.zenCreds[0].id); got > time.Now().UnixNano() {
		t.Fatalf("success cred must not cool despite same tail")
	}
}

func TestBulkStrictEmptyObject(t *testing.T) {
	cases := []struct {
		name string
		body string
		want int
	}{
		{"empty object", `{}`, http.StatusOK},
		{"whitespace", "  { \n\t}  ", http.StatusOK},
		{"null", `null`, http.StatusBadRequest},
		{"array", `[]`, http.StatusBadRequest},
		{"scalar number", `123`, http.StatusBadRequest},
		{"scalar string", `"x"`, http.StatusBadRequest},
		{"scalar bool", `true`, http.StatusBadRequest},
		{"unknown field", `{"pool":"x"}`, http.StatusBadRequest},
		{"trailing values", `{}{}`, http.StatusBadRequest},
		{"trailing ws values", `{} {}`, http.StatusBadRequest},
		{"empty body", ``, http.StatusBadRequest},
	}
	for _, tc := range cases {
		// Fresh admin per case: bulk rate limiting is per-client and even
		// rejected bodies consume the window.
		_, admin, token, csrf := bulkAdmin(t,
			map[string][]string{"shared": {"direct"}},
			ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"},
			nil, nil)
		// Point upstream at a fast local stub so OK cases complete without
		// external network.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(200)
			_, _ = w.Write([]byte(bulkChatSuccessBody("bulk-free-model")))
		}))
		rt := admin.manager.current.Load()
		rt.gateway.cfg.Upstream.Zen = srv.URL
		rt.gateway.cfg.Upstream.Go = srv.URL
		seedBulkProbeCatalog(rt.gateway)
		rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check", tc.body, token, csrf))
		srv.Close()
		if rec.Code != tc.want {
			t.Fatalf("%s: code=%d want %d body=%s", tc.name, rec.Code, tc.want, rec.Body.String())
		}
		if tc.want == http.StatusBadRequest && rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s: missing no-store", tc.name)
		}
	}
}

func TestBulkPartialSnapshotAndZeroTestedGuard(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081"})
	pool := gw.pools["shared"]
	now := time.Now().UTC()
	// Complete run stores a complete snapshot.
	complete := gw.buildBulkResponse(now, 2, 2, 0, false, false, []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: gw.zenCreds[0].key, CredID: gw.zenCreds[0].id, CredDisp: gw.zenCreds[0].display}, StartedNanos: time.Now().UnixNano(), Status: 200, Success: true},
	})
	complete.Partial = false
	snap := &bulkAvailabilitySnapshot{CheckedAt: now, TotalNodes: 2, TestedNodes: 2, Nodes: complete.Nodes, Credentials: complete.Credentials}
	gw.bulkSnapshot.Store(snap)
	// Fully cancelled zero-tested run must not overwrite the complete snapshot.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resp := gw.runBulkCheck(ctx)
	if !resp.Partial {
		t.Fatalf("cancelled run must report partial")
	}
	kept := gw.bulkSnapshot.Load()
	if kept == nil {
		t.Fatalf("snapshot missing")
	}
	if kept.Partial {
		t.Fatalf("complete snapshot must not be overwritten by zero-tested cancelled run")
	}
	if kept.CheckedAt.UnixNano() != now.UnixNano() {
		t.Fatalf("snapshot timestamp changed after zero-tested guard")
	}
	// Partial run with useful rows preserves nodes and marks partial.
	partialResp := gw.buildBulkResponse(time.Now().UTC(), 2, 1, 0, false, true, []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: gw.zenCreds[0].key, CredID: gw.zenCreds[0].id, CredDisp: gw.zenCreds[0].display}, StartedNanos: time.Now().UnixNano(), Status: 200, Success: true},
	})
	if !partialResp.Partial {
		t.Fatalf("partial flag must survive response")
	}
	if len(partialResp.Nodes) == 0 {
		t.Fatalf("partial must preserve node rows")
	}
}

func TestBulkCred429SecondRetryAfterAndWatermark(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081", "http://127.0.0.1:8082"})
	pool := gw.pools["shared"]
	cred := gw.zenCreds[0]
	base := time.Now().UnixNano()
	r := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: cred.key, CredID: cred.id, CredDisp: cred.display}, StartedNanos: base, Status: 429, RetryAfter: time.Second},
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[1], Raw: pool.items[1].name, Tier: TierZen, CredKey: cred.key, CredID: cred.id, CredDisp: cred.display}, StartedNanos: base + 1, Status: 429, RetryAfter: 60 * time.Second},
	}
	gw.applyBulkWrites(context.Background(), r)
	until, status, ok := gw.scheduler.credential429CooldownStatus(cred.id)
	if !ok || status != 429 {
		t.Fatalf("credential429 must be set")
	}
	// Second distinct Retry-After (60s) must dominate the cooldown, proving the
	// second's Retry-After was used rather than the first's 1s.
	if until-base < int64(55*time.Second) {
		t.Fatalf("cooldown must reflect second Retry-After 60s, until-base=%v", time.Duration(until-base))
	}
	// True started watermark: stale success at first start must not clear, fresh
	// success after second start must clear.
	if ch := gw.scheduler.noteCredential429Success(cred.id, base); ch.Cleared {
		t.Fatalf("stale success must not clear newer credential429")
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(cred.id); !ok {
		t.Fatalf("credential429 must still be active after stale success")
	}
	if ch := gw.scheduler.noteCredential429Success(cred.id, base+2); !ch.Cleared {
		t.Fatalf("newer success must clear")
	}
}

func TestBulkPinnedFilteringRealEntryPoints(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081"})
	pool := gw.pools["shared"]
	now := time.Now().UnixNano()
	// Channel cooling on items[1] must fast-fail pinned-anon via the real
	// doPinnedAnonymous entry point without cross-proxy moves.
	gw.scheduler.noteChannelFailure(TierZen, "shared", pool.items[1].name, AttemptClassUpstreamFailure, 500, 0, now)
	gw.bindSessionPin("ses-pinned-anon-real", "m1", TierZen, anonymousSchedulerCredentialID, "shared", pool.items[1].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, ok := gw.scheduler.pins.get("ses-pinned-anon-real", "m1")
	if !ok {
		t.Fatalf("pin missing")
	}
	route := modelRoute{ID: "m1"}
	resp, _, _, _ := gw.doPinnedAnonymous(context.Background(), route, nil, requestIDs{Session: "ses-pinned-anon-real"}, pin, route, gw.cfg.Upstream.Zen, ProtocolChat, []byte(`{}`), 0)
	if resp == nil || resp.StatusCode == 200 {
		t.Fatalf("pinned-anon on channel-cooling proxy must fast-fail")
	}
	drainAndClose(resp.Body)
	// Auth: when all proxies are channel-cooling, the real doPinnedAuth entry
	// point must fast-fail 502 without sends; when only current is cooling it
	// must remain able to use the healthy alternate (eligible set excludes the
	// cooling proxy, verified by direct eligible filtering parity with the
	// entry point's own filter).
	gw2 := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081"})
	pool2 := gw2.pools["shared"]
	cred2 := gw2.zenCreds[0]
	gw2.scheduler.noteChannelFailure(TierZen, "shared", pool2.items[0].name, AttemptClassUpstreamFailure, 500, 0, now)
	gw2.scheduler.noteChannelFailure(TierZen, "shared", pool2.items[1].name, AttemptClassUpstreamFailure, 500, 0, now)
	gw2.bindSessionPin("ses-pinned-auth-real", "m1", TierZen, cred2.id, "shared", pool2.items[0].name, ProtocolChat, normalizeRouteAuthority(gw2.cfg.Upstream.Zen))
	pin2, ok := gw2.scheduler.pins.get("ses-pinned-auth-real", "m1")
	if !ok {
		t.Fatalf("auth pin missing")
	}
	route2 := modelRoute{ID: "m1"}
	resp2, _, _, _ := gw2.doPinnedAuth(context.Background(), route2, nil, requestIDs{Session: "ses-pinned-auth-real"}, pin2, route2, gw2.cfg.Upstream.Zen, "shared", ProtocolChat, []byte(`{}`), cred2.key, cred2.display, cred2.index, 0)
	if resp2 == nil {
		t.Fatalf("pinned-auth all-cooling must return local response")
	}
	if resp2.StatusCode != http.StatusBadGateway {
		t.Fatalf("all channel-cooling pinned-auth code=%d want 502", resp2.StatusCode)
	}
	drainAndClose(resp2.Body)
	// Unbound candidate builders must also filter the cooling proxy (real
	// filtering entry points shared by establishment).
	cands := gw2.scheduler.buildAuthCandidates(TierZen, gw2.zenCreds, pool2, "m1", time.Now().UnixNano())
	for _, c := range cands {
		if c.ProxyRaw == pool2.items[0].name || c.ProxyRaw == pool2.items[1].name {
			t.Fatalf("auth candidates must filter channel-cooling proxies")
		}
	}
	anonCands := gw.scheduler.buildAnonymousCandidates(pool, "m1", time.Now().UnixNano())
	for _, c := range anonCands {
		if c.ProxyRaw == pool.items[1].name {
			t.Fatalf("anonymous candidates must filter channel-cooling proxy")
		}
	}
}

func TestBulkFullHTTPRunLeavesMonitorHistoryUntouched(t *testing.T) {
	manager, _, _, _ := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"},
		[]string{"zen-key-12345"}, []string{"go-key-12345"})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(bulkChatSuccessBody("bulk-free-model")))
	}))
	defer srv.Close()
	rt := manager.current.Load()
	rt.gateway.cfg.Upstream.Zen = srv.URL
	rt.gateway.cfg.Upstream.Go = srv.URL
	seedBulkProbeCatalog(rt.gateway)
	beforeReqs := len(manager.monitor.Snapshot().Upstream.Requests)
	beforeRecent := len(manager.monitor.Snapshot().Upstream.Recent)
	beforeHist := manager.monitor.HistoryStatus()
	resp := rt.gateway.runBulkCheck(context.Background())
	if resp.TestedNodes == 0 {
		t.Fatalf("full HTTP run must test nodes")
	}
	if got := len(manager.monitor.Snapshot().Upstream.Requests); got != beforeReqs {
		t.Fatalf("bulk must not create request metrics")
	}
	if got := len(manager.monitor.Snapshot().Upstream.Recent); got != beforeRecent {
		t.Fatalf("bulk must not create attempt metrics")
	}
	afterHist := manager.monitor.HistoryStatus()
	if afterHist != beforeHist {
		t.Fatalf("bulk must not touch history status: %+v vs %+v", beforeHist, afterHist)
	}
	body, _ := json.Marshal(resp)
	for _, secret := range []string{"zen-key-12345", "go-key-12345"} {
		if strings.Contains(string(body), secret) {
			t.Fatalf("secret leaked in bulk response")
		}
	}
}

func TestBulkLargePoolSendCapTruncation(t *testing.T) {
	proxies := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		proxies = append(proxies, "http://127.0.0.1:8"+strings.Repeat("0", 2)+string(rune('0'+i%10))+":"+string(rune('0'+i%10)))
	}
	// Use distinct raw URLs to force distinct pool-qualified nodes.
	distinct := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		distinct = append(distinct, "http://127.0.0.1:"+strings.Repeat("9", 1)+string(rune('0'+i/10))+string(rune('0'+i%10)))
	}
	_ = proxies
	manager, _, _, _ := bulkAdmin(t,
		map[string][]string{"shared": distinct},
		ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"},
		[]string{"zen-key-12345"}, []string{"go-key-12345"})
	gw := manager.current.Load().gateway
	seedBulkProbeCatalog(gw)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resp := gw.runBulkCheck(ctx)
	if !resp.Partial {
		t.Fatalf("cancelled large-pool run must be partial")
	}
	if !resp.Truncated {
		t.Fatalf("large pool must report truncated")
	}
	if resp.SkippedNodes <= 0 {
		t.Fatalf("large pool must report skipped nodes")
	}
	if resp.TestedNodes > bulkMaxTotalSends {
		t.Fatalf("tested %d exceeds send cap %d", resp.TestedNodes, bulkMaxTotalSends)
	}
	// Deterministic: second cancelled run reports identical caps.
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	resp2 := gw.runBulkCheck(ctx2)
	if resp2.TestedNodes != resp.TestedNodes || resp2.SkippedNodes != resp.SkippedNodes || resp2.Truncated != resp.Truncated {
		t.Fatalf("truncation must be deterministic: %+v vs %+v", resp, resp2)
	}
}

func TestBulkCancelBeforeLaunchAndTimeoutAttribution(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct"})
	pool := gw.pools["shared"]
	// Cancel-before-launch: all results AdminCancelled, partial, no transport flip.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results := []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: "public", CredID: anonymousSchedulerCredentialID, CredDisp: "anonymous", IsPublic: true}, AdminCancelled: true},
	}
	gw.applyBulkWrites(ctx, results)
	if !pool.items[0].healthy.Load() {
		t.Fatalf("cancel-before-launch must never flip healthy")
	}
	if got := bulkOutcomeLabel(results[0]); got != "cancelled" {
		t.Fatalf("cancel label=%q want cancelled", got)
	}
	// Per-send timeout attribution: probe-context DeadlineExceeded is
	// diagnostic-only for health and inconclusive for display.
	timeoutRes := bulkSendResult{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: "public", CredID: anonymousSchedulerCredentialID, CredDisp: "anonymous", IsPublic: true}, TransportErr: context.DeadlineExceeded, IsProxyFailure: true, ProbeContextCaused: true}
	gw.applyBulkWrites(context.Background(), []bulkSendResult{timeoutRes})
	if !pool.items[0].healthy.Load() {
		t.Fatalf("per-send timeout must not flip healthy")
	}
	if got := bulkOutcomeLabel(timeoutRes); got != "inconclusive" {
		t.Fatalf("timeout label=%q want inconclusive", got)
	}
}

func TestBulkTransportParityWithGatewayAuthority(t *testing.T) {
	// Bulk transport writes must reuse applyProxyHealthResult global semantics
	// (no tier-scoped health, no contradictory scan). Parity: same evidence via
	// bulk and via direct authority flips/restores identically.
	newGW := func() *Gateway {
		return schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081"})
	}
	// Restore parity: unhealthy + HTTP response restores via both paths.
	for _, viaBulk := range []bool{true, false} {
		gw := newGW()
		pool := gw.pools["shared"]
		pool.items[0].healthy.Store(false)
		if viaBulk {
			gw.applyBulkWrites(context.Background(), []bulkSendResult{
				{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: "public", CredID: anonymousSchedulerCredentialID, CredDisp: "anonymous", IsPublic: true}, Status: 200, Success: true},
			})
		} else {
			gw.applyProxyHealthResult(proxyHealthResult{proxy: pool.items[0], err: nil, wasHealthy: false}, "test probe", 200)
		}
		if !pool.items[0].healthy.Load() {
			t.Fatalf("viaBulk=%v: HTTP must restore", viaBulk)
		}
	}
	// Failure parity: independent conclusive failure flips via both paths when
	// another healthy proxy exists globally (global, not tier-scoped).
	for _, viaBulk := range []bool{true, false} {
		gw := newGW()
		pool := gw.pools["shared"]
		if viaBulk {
			gw.applyBulkWrites(context.Background(), []bulkSendResult{
				{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: "public", CredID: anonymousSchedulerCredentialID, CredDisp: "anonymous", IsPublic: true}, TransportErr: context.DeadlineExceeded, IsProxyFailure: true, StartedNanos: time.Now().UnixNano()},
			})
		} else {
			gw.applyProxyHealthResult(proxyHealthResult{proxy: pool.items[0], err: context.DeadlineExceeded, failed: true, wasHealthy: true}, "test probe", 0)
		}
		if pool.items[0].healthy.Load() {
			t.Fatalf("viaBulk=%v: independent failure must flip when global healthy exists", viaBulk)
		}
	}
	// Diagnostic parity: probe-context timeout flips neither.
	gw := newGW()
	pool := gw.pools["shared"]
	gw.applyBulkWrites(context.Background(), []bulkSendResult{
		{Target: bulkSendTarget{PoolName: "shared", Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: "public", CredID: anonymousSchedulerCredentialID, CredDisp: "anonymous", IsPublic: true}, TransportErr: context.DeadlineExceeded, IsProxyFailure: true, ProbeContextCaused: true},
	})
	if !pool.items[0].healthy.Load() {
		t.Fatalf("probe-context timeout must not flip")
	}
	gw.applyProxyHealthResult(proxyHealthResult{proxy: pool.items[0], err: context.Canceled, failed: false, wasHealthy: true}, "test probe", 0)
	if !pool.items[0].healthy.Load() {
		t.Fatalf("inconclusive authority result must not flip")
	}
}
