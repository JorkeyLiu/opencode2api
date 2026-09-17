package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// stubTransport stubs upstream inference per proxy client while counting
// sends per proxy.
type stubTransport struct {
	hits   *atomic.Int32
	status int
	body   string
}

func (s *stubTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	s.hits.Add(1)
	resp := &http.Response{
		StatusCode: s.status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(s.body)),
		Request:    req,
	}
	return resp, nil
}

func credentialAdmin(t *testing.T, pools map[string][]string, routing ProxyRoutingConfig, zenKeys, goKeys []string) (*RuntimeManager, *AdminServer, string, string) {
	t.Helper()
	return bulkAdmin(t, pools, routing, zenKeys, goKeys)
}

func credentialFingerprintFor(t *testing.T, manager *RuntimeManager, tier Tier, key string) string {
	t.Helper()
	_ = manager
	return credentialFingerprint(tier, key)
}

func TestCredentialCheckAuthCSRFOriginStrictNoStore(t *testing.T) {
	manager, admin, token, csrf := credentialAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	rt := manager.current.Load()
	fp := credentialFingerprintFor(t, manager, TierZen, rt.gateway.authCreds[0].key)
	_ = fp
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credential", `{"fingerprint":"`+fp+`"}`, "", "")); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauth code=%d want 401", rec.Code)
	}
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credential", `{"fingerprint":"`+fp+`"}`, token, "wrong")); rec.Code != http.StatusForbidden {
		t.Fatalf("bad csrf code=%d want 403", rec.Code)
	}
	req := operabilityRequest(http.MethodPost, "/api/availability/check-credential", `{"fingerprint":"`+fp+`"}`, token, csrf)
	req.Host = "admin.local"
	req.Header.Set("Origin", "http://evil.example")
	if rec := serveAdmin(admin, req); rec.Code != http.StatusForbidden {
		t.Fatalf("origin code=%d want 403", rec.Code)
	}
	// Strict bodies.
	for _, tc := range []struct {
		name string
		body string
	}{
		{"null", `null`},
		{"array", `[]`},
		{"scalar", `123`},
		{"unknown field", `{"fingerprint":"` + fp + `","url":"http://127.0.0.1:9"}`},
		{"missing", `{}`},
		{"empty fp", `{"fingerprint":""}`},
		{"trailing", `{"fingerprint":"` + fp + `"}{}`},
		{"empty body", ``},
	} {
		_, admin2, token2, csrf2 := credentialAdmin(t,
			map[string][]string{"shared": {"direct"}},
			ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
			[]string{"zen-key-12345"}, nil)
		rec := serveAdmin(admin2, operabilityRequest(http.MethodPost, "/api/availability/check-credential", tc.body, token2, csrf2))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: code=%d want 400 body=%s", tc.name, rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s: missing no-store", tc.name)
		}
	}
	// Unknown fingerprint.
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credential", `{"fingerprint":"deadbeef00"}`, token, csrf)); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown fp code=%d want 400", rec.Code)
	} else if !strings.Contains(rec.Body.String(), "unknown_credential") {
		t.Fatalf("unknown fp must carry unknown_credential, got %s", rec.Body.String())
	}
	// Tail-only must not resolve.
	tail := rt.gateway.authCreds[0].display
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credential", `{"fingerprint":"`+tail+`"}`, token, csrf)); rec.Code != http.StatusBadRequest {
		t.Fatalf("tail code=%d want 400", rec.Code)
	}
	// Raw key must not resolve and must never be echoed.
	raw := rt.gateway.authCreds[0].key
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credential", `{"fingerprint":"`+raw+`"}`, token, csrf))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("raw key code=%d want 400", rec.Code)
	}
	if strings.Contains(rec.Body.String(), raw) {
		t.Fatalf("raw key echoed in error")
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control=%q want no-store", got)
	}
}

func TestCustomCheckAuthCSRFOriginStrictNoStore(t *testing.T) {
	manager, admin, token, csrf := credentialAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		nil, nil)
	rt := manager.current.Load()
	rt.gateway.cfg.Fallback = FallbackConfig{Active: "", Channels: []FallbackChannelConfig{{Name: "c1", BaseURL: "https://api.example.com", APIKey: "secret-custom-11111", Model: "cm"}}}
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-custom", `{"name":"c1"}`, "", "")); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauth code=%d want 401", rec.Code)
	}
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-custom", `{"name":"c1"}`, token, "wrong")); rec.Code != http.StatusForbidden {
		t.Fatalf("bad csrf code=%d want 403", rec.Code)
	}
	req := operabilityRequest(http.MethodPost, "/api/availability/check-custom", `{"name":"c1"}`, token, csrf)
	req.Host = "admin.local"
	req.Header.Set("Origin", "http://evil.example")
	if rec := serveAdmin(admin, req); rec.Code != http.StatusForbidden {
		t.Fatalf("origin code=%d want 403", rec.Code)
	}
	for _, tc := range []struct {
		name string
		body string
	}{
		{"null", `null`},
		{"unknown field", `{"name":"c1","url":"http://127.0.0.1:9"}`},
		{"missing", `{}`},
		{"empty", `{"name":""}`},
		{"trailing", `{"name":"c1"}{}`},
	} {
		_, admin2, token2, csrf2 := credentialAdmin(t,
			map[string][]string{"shared": {"direct"}},
			ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
			nil, nil)
		rt2 := admin2.manager.current.Load()
		rt2.gateway.cfg.Fallback = FallbackConfig{Active: "", Channels: []FallbackChannelConfig{{Name: "c1", BaseURL: "https://api.example.com", APIKey: "secret-custom-11111", Model: "cm"}}}
		rec := serveAdmin(admin2, operabilityRequest(http.MethodPost, "/api/availability/check-custom", tc.body, token2, csrf2))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: code=%d want 400 body=%s", tc.name, rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s: missing no-store", tc.name)
		}
	}
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-custom", `{"name":"nope"}`, token, csrf)); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown channel code=%d want 400", rec.Code)
	} else if !strings.Contains(rec.Body.String(), "unknown_channel") {
		t.Fatalf("unknown channel must carry unknown_channel, got %s", rec.Body.String())
	}
}

func TestPerRowRateLimitBusy(t *testing.T) {
	manager, admin, token, csrf := credentialAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(bulkChatSuccessBody("bulk-free-model")))
	}))
	defer srv.Close()
	rt := manager.current.Load()
	rt.gateway.cfg.Upstream.Zen = srv.URL
	rt.gateway.cfg.Upstream.Go = srv.URL
	seedBulkProbeCatalog(rt.gateway)
	fp := credentialFingerprint(TierZen, rt.gateway.authCreds[0].key)
	var last *httptest.ResponseRecorder
	for i := 0; i < 4; i++ {
		last = serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credential", `{"fingerprint":"`+fp+`"}`, token, csrf))
	}
	if last.Code != http.StatusTooManyRequests {
		t.Fatalf("4th credential code=%d want 429 body=%s", last.Code, last.Body.String())
	}
	// Busy gate shares bulkMu.
	_, admin2, token2, csrf2 := credentialAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	rt2 := admin2.manager.current.Load()
	rt2.gateway.bulkMu.Lock()
	defer rt2.gateway.bulkMu.Unlock()
	fp2 := credentialFingerprint(TierZen, rt2.gateway.authCreds[0].key)
	if rec := serveAdmin(admin2, operabilityRequest(http.MethodPost, "/api/availability/check-credential", `{"fingerprint":"`+fp2+`"}`, token2, csrf2)); rec.Code != http.StatusConflict {
		t.Fatalf("busy credential code=%d want 409", rec.Code)
	}
	rt2.gateway.cfg.Fallback = FallbackConfig{Active: "", Channels: []FallbackChannelConfig{{Name: "c1", BaseURL: "https://api.example.com", APIKey: "k", Model: "cm"}}}
	if rec := serveAdmin(admin2, operabilityRequest(http.MethodPost, "/api/availability/check-custom", `{"name":"c1"}`, token2, csrf2)); rec.Code != http.StatusConflict {
		t.Fatalf("busy custom code=%d want 409", rec.Code)
	}
}

func TestCredentialEligibilityFiltering(t *testing.T) {
	manager, admin, token, csrf := credentialAdmin(t,
		map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	rt := manager.current.Load()
	gw := rt.gateway
	seedBulkProbeCatalog(gw)
	pool := gw.pools["shared"]
	var hits0, hits1 atomic.Int32
	pool.items[0].client.Transport = &stubTransport{hits: &hits0, status: 200, body: bulkChatSuccessBody("bulk-free-model")}
	pool.items[1].client.Transport = &stubTransport{hits: &hits1, status: 200, body: bulkChatSuccessBody("bulk-free-model")}
	// Cool proxy0 via tier-qualified proxy429; it must be excluded.
	gw.scheduler.noteProxy429Failure(TierZen, "shared", pool.items[0].name, AttemptClassRateLimited, 429, 0, time.Now().UnixNano())
	// Mark proxy1 healthy (default) and keep proxy0 healthy (cooling is
	// scheduler state, not transport health).
	fp := credentialFingerprint(TierZen, gw.authCreds[0].key)
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credential", `{"fingerprint":"`+fp+`"}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("credential code=%d body=%s", rec.Code, rec.Body.String())
	}
	if hits0.Load() != 0 {
		t.Fatalf("proxy429-cooled node must not be sent to, hits=%d", hits0.Load())
	}
	if hits1.Load() == 0 {
		t.Fatalf("eligible node must be sent to")
	}
	var resp credentialCheckResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Credential.Status != "available" {
		t.Fatalf("credential status=%q want available", resp.Credential.Status)
	}
	if strings.Contains(rec.Body.String(), gw.authCreds[0].key) {
		t.Fatalf("raw key leaked")
	}
	// Transport-unhealthy exclusion.
	pool.items[1].healthy.Store(false)
	hits0.Store(0)
	hits1.Store(0)
	// Fresh admin to avoid rate limit.
	_, admin2, token2, csrf2 := credentialAdmin(t,
		map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	rt2 := admin2.manager.current.Load()
	gw2 := rt2.gateway
	seedBulkProbeCatalog(gw2)
	pool2 := gw2.pools["shared"]
	var h0, h1 atomic.Int32
	pool2.items[0].client.Transport = &stubTransport{hits: &h0, status: 200, body: bulkChatSuccessBody("bulk-free-model")}
	pool2.items[1].client.Transport = &stubTransport{hits: &h1, status: 200, body: bulkChatSuccessBody("bulk-free-model")}
	pool2.items[0].healthy.Store(false)
	pool2.items[1].healthy.Store(true)
	fp2 := credentialFingerprint(TierZen, gw2.authCreds[0].key)
	rec2 := serveAdmin(admin2, operabilityRequest(http.MethodPost, "/api/availability/check-credential", `{"fingerprint":"`+fp2+`"}`, token2, csrf2))
	if rec2.Code != http.StatusOK {
		t.Fatalf("unhealthy-filter code=%d body=%s", rec2.Code, rec2.Body.String())
	}
	if h0.Load() != 0 {
		t.Fatalf("unhealthy node must not be sent to")
	}
	if h1.Load() == 0 {
		t.Fatalf("healthy node must be sent to")
	}
}

func TestCredentialZeroEligibleNoMutation(t *testing.T) {
	manager, admin, token, csrf := credentialAdmin(t,
		map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	rt := manager.current.Load()
	gw := rt.gateway
	seedBulkProbeCatalog(gw)
	pool := gw.pools["shared"]
	now := time.Now().UnixNano()
	gw.scheduler.noteProxy429Failure(TierZen, "shared", pool.items[0].name, AttemptClassRateLimited, 429, 0, now)
	gw.scheduler.noteProxy429Failure(TierZen, "shared", pool.items[1].name, AttemptClassRateLimited, 429, 0, now)
	// Seed a snapshot with nodes + another credential + custom + timestamp.
	checked := time.Now().UTC().Add(-time.Hour)
	nodeChecked := checked
	credChecked := checked
	snap := &bulkAvailabilitySnapshot{
		CheckedAt: checked, TotalNodes: 2, TestedNodes: 2,
		Nodes: []bulkNodeAvailability{
			{Pool: "shared", Index: 0, ProxyNode: redactURL(pool.items[0].name), Transport: "healthy", Zen: "success", LastChecked: &nodeChecked},
			{Pool: "shared", Index: 1, ProxyNode: redactURL(pool.items[1].name), Transport: "healthy", Zen: "success", LastChecked: &nodeChecked},
		},
		Credentials: []bulkCredentialAvailability{
			{Tier: "zen", KeyTail: gw.authCreds[0].display, Fingerprint: credentialFingerprint(TierZen, gw.authCreds[0].key), Pool: "shared", Status: "available", Reason: "success", LastChecked: &credChecked, CredID: gw.authCreds[0].id},
		},
		Custom: []bulkCustomAvailability{{Name: "c1", BaseURL: "https://api.example.com", Model: "cm", Status: "available", Reason: "success", LastChecked: &credChecked}},
	}
	gw.bulkSnapshot.Store(snap)
	proxyBefore, _, _ := gw.scheduler.proxy429CooldownStatus(TierZen, "shared", pool.items[0].name)
	_ = proxyBefore
	fp := credentialFingerprint(TierZen, gw.authCreds[0].key)
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credential", `{"fingerprint":"`+fp+`"}`, token, csrf))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("zero eligible code=%d want 503 body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "no_available_proxy") {
		t.Fatalf("must carry no_available_proxy, got %s", rec.Body.String())
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("missing no-store")
	}
	after := gw.bulkSnapshot.Load()
	if after == nil || !after.CheckedAt.Equal(checked) {
		t.Fatalf("snapshot time must not move on zero-eligible error")
	}
	if len(after.Nodes) != 2 || len(after.Custom) != 1 || len(after.Credentials) != 1 {
		t.Fatalf("snapshot must be unmutated: %+v", after)
	}
	if after.Credentials[0].LastChecked == nil || !after.Credentials[0].LastChecked.Equal(credChecked) {
		t.Fatalf("credential time must not move")
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(gw.authCreds[0].id); ok {
		t.Fatalf("credential429 must not be written on zero-eligible error")
	}
}

func TestCredentialOneVsTwo429Semantics(t *testing.T) {
	// One eligible proxy with fresh 429: display-only for credential429.
	manager, admin, token, csrf := credentialAdmin(t,
		map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	rt := manager.current.Load()
	gw := rt.gateway
	seedBulkProbeCatalog(gw)
	pool := gw.pools["shared"]
	var h0, h1 atomic.Int32
	pool.items[0].client.Transport = &stubTransport{hits: &h0, status: 429, body: `{"error":{"message":"slow"}}`}
	pool.items[1].client.Transport = &stubTransport{hits: &h1, status: 429, body: `{"error":{"message":"slow"}}`}
	// Exclude proxy1 via proxy429 so only proxy0 is eligible (single 429).
	gw.scheduler.noteProxy429Failure(TierZen, "shared", pool.items[1].name, AttemptClassRateLimited, 429, 0, time.Now().UnixNano())
	fp := credentialFingerprint(TierZen, gw.authCreds[0].key)
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credential", `{"fingerprint":"`+fp+`"}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("single-429 code=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp credentialCheckResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Credential.Status != "rate_limited" {
		t.Fatalf("single-429 credential status=%q want rate_limited", resp.Credential.Status)
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(gw.authCreds[0].id); ok {
		t.Fatalf("single newly observed 429 must stay display-only for credential429")
	}
	// Two eligible proxies both 429: credential429 must be written.
	manager2, admin2, token2, csrf2 := credentialAdmin(t,
		map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	rt2 := manager2.current.Load()
	gw2 := rt2.gateway
	seedBulkProbeCatalog(gw2)
	pool2 := gw2.pools["shared"]
	var g0, g1 atomic.Int32
	pool2.items[0].client.Transport = &stubTransport{hits: &g0, status: 429, body: `{"error":{"message":"slow"}}`}
	pool2.items[1].client.Transport = &stubTransport{hits: &g1, status: 429, body: `{"error":{"message":"slow"}}`}
	fp2 := credentialFingerprint(TierZen, gw2.authCreds[0].key)
	rec2 := serveAdmin(admin2, operabilityRequest(http.MethodPost, "/api/availability/check-credential", `{"fingerprint":"`+fp2+`"}`, token2, csrf2))
	if rec2.Code != http.StatusOK {
		t.Fatalf("two-429 code=%d body=%s", rec2.Code, rec2.Body.String())
	}
	if _, status, ok := gw2.scheduler.credential429CooldownStatus(gw2.authCreds[0].id); !ok || status != 429 {
		t.Fatalf("two distinct 429s must write credential429")
	}
}

func TestCredentialNoModelSemantics(t *testing.T) {
	manager, admin, token, csrf := credentialAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	rt := manager.current.Load()
	gw := rt.gateway
	// No catalog seeding: no directory model exists.
	pool := gw.pools["shared"]
	var hits atomic.Int32
	pool.items[0].client.Transport = &stubTransport{hits: &hits, status: 200, body: bulkChatSuccessBody("bulk-free-model")}
	fp := credentialFingerprint(TierZen, gw.authCreds[0].key)
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credential", `{"fingerprint":"`+fp+`"}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("no-model code=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp credentialCheckResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Credential.Status != "inconclusive" || resp.Credential.Reason != "no_model" {
		t.Fatalf("no-model row=%+v want inconclusive/no_model", resp.Credential)
	}
	if len(resp.NoModel) == 0 {
		t.Fatalf("no-model list must be present")
	}
	if hits.Load() != 0 {
		t.Fatalf("no-model must send nothing, hits=%d", hits.Load())
	}
	if strings.Contains(rec.Body.String(), gw.authCreds[0].key) {
		t.Fatalf("raw key leaked")
	}
	// Snapshot merge updates only that credential row/time.
	snap := gw.bulkSnapshot.Load()
	if snap == nil || len(snap.Credentials) != 1 {
		t.Fatalf("no-model must store one credential row: %+v", snap)
	}
	// Zero-eligible + no-model still returns inconclusive (no 503): the
	// no_available_proxy error applies only when a send would be required.
	_, admin3, token3, csrf3 := credentialAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	rt3 := admin3.manager.current.Load()
	gw3 := rt3.gateway
	// No catalog seeding here either; mark the only proxy unhealthy.
	gw3.pools["shared"].items[0].healthy.Store(false)
	fp3 := credentialFingerprint(TierZen, gw3.authCreds[0].key)
	rec3 := serveAdmin(admin3, operabilityRequest(http.MethodPost, "/api/availability/check-credential", `{"fingerprint":"`+fp3+`"}`, token3, csrf3))
	if rec3.Code != http.StatusOK {
		t.Fatalf("no-model zero-eligible code=%d want 200 body=%s", rec3.Code, rec3.Body.String())
	}
	var resp3 credentialCheckResponse
	if err := json.Unmarshal(rec3.Body.Bytes(), &resp3); err != nil {
		t.Fatal(err)
	}
	if resp3.Credential.Reason != "no_model" {
		t.Fatalf("no-model zero-eligible must stay no_model, got %+v", resp3.Credential)
	}
}

func TestCredentialSnapshotMergePreserves(t *testing.T) {
	manager, admin, token, csrf := credentialAdmin(t,
		map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"AAA-12345", "BBB-12345"}, nil)
	rt := manager.current.Load()
	gw := rt.gateway
	seedBulkProbeCatalog(gw)
	pool := gw.pools["shared"]
	for _, p := range pool.items {
		var h atomic.Int32
		_ = h
		p.client.Transport = &stubTransport{hits: new(atomic.Int32), status: 200, body: bulkChatSuccessBody("bulk-free-model")}
	}
	checked := time.Now().UTC().Add(-time.Hour)
	nodeChecked := checked
	otherChecked := checked
	snap := &bulkAvailabilitySnapshot{
		CheckedAt: checked, TotalNodes: 2, TestedNodes: 2,
		Nodes: []bulkNodeAvailability{
			{Pool: "shared", Index: 0, ProxyNode: redactURL(pool.items[0].name), Transport: "healthy", Zen: "success", LastChecked: &nodeChecked},
			{Pool: "shared", Index: 1, ProxyNode: redactURL(pool.items[1].name), Transport: "healthy", Zen: "success", LastChecked: &nodeChecked},
		},
		Credentials: []bulkCredentialAvailability{
			{Tier: "zen", KeyTail: gw.authCreds[0].display, Fingerprint: credentialFingerprint(TierZen, gw.authCreds[0].key), Pool: "shared", Status: "available", Reason: "success", LastChecked: &otherChecked, CredID: gw.authCreds[0].id},
			{Tier: "zen", KeyTail: gw.authCreds[1].display, Fingerprint: credentialFingerprint(TierZen, gw.authCreds[1].key), Pool: "shared", Status: "available", Reason: "success", LastChecked: &otherChecked, CredID: gw.authCreds[1].id},
		},
		Custom: []bulkCustomAvailability{{Name: "c1", BaseURL: "https://api.example.com", Model: "cm", Status: "available", Reason: "success", LastChecked: &otherChecked}},
	}
	gw.bulkSnapshot.Store(snap)
	fp := credentialFingerprint(TierZen, gw.authCreds[0].key)
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credential", `{"fingerprint":"`+fp+`"}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("merge code=%d body=%s", rec.Code, rec.Body.String())
	}
	after := gw.bulkSnapshot.Load()
	if !after.CheckedAt.Equal(checked) {
		t.Fatalf("global batch timestamp must be preserved")
	}
	if len(after.Nodes) != 2 || len(after.Custom) != 1 || len(after.Credentials) != 2 {
		t.Fatalf("merge must preserve rows: %+v", after)
	}
	for _, n := range after.Nodes {
		if n.LastChecked == nil || !n.LastChecked.Equal(nodeChecked) {
			t.Fatalf("node snapshots must not move: %+v", n)
		}
	}
	kept := false
	for _, c := range after.Credentials {
		if c.CredID == gw.authCreds[1].id {
			if c.LastChecked == nil || !c.LastChecked.Equal(otherChecked) {
				t.Fatalf("unrelated credential must not move")
			}
			kept = true
		}
	}
	if !kept {
		t.Fatalf("unrelated credential missing")
	}
	// Response contains only the selected credential.
	var resp credentialCheckResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Credential.CredID != "" && resp.Credential.Fingerprint != fp {
		t.Fatalf("response fingerprint mismatch: %+v", resp.Credential)
	}
	if len(resp.Credential.KeyTail) == 0 || strings.Contains(rec.Body.String(), gw.authCreds[0].key) {
		t.Fatalf("redaction broken")
	}
}

func TestCustomCheckNoSchedulerMerge(t *testing.T) {
	customSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm")))
	}))
	defer customSrv.Close()
	manager, admin, token, csrf := credentialAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	rt := manager.current.Load()
	gw := rt.gateway
	seedBulkProbeCatalog(gw)
	gw.cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{
		{Name: "c1", BaseURL: customSrv.URL, APIKey: "secret-custom-11111", Model: "cm", Protocol: ProtocolChat},
		{Name: "c2", BaseURL: "https://other.example", APIKey: "secret-custom-22222", Model: "cm", Protocol: ProtocolChat},
	}}
	// Seed native snapshot + one other custom row.
	checked := time.Now().UTC().Add(-time.Hour)
	otherChecked := checked
	pool := gw.pools["shared"]
	snap := &bulkAvailabilitySnapshot{
		CheckedAt: checked, TotalNodes: 1, TestedNodes: 1,
		Nodes: []bulkNodeAvailability{{Pool: "shared", Index: 0, ProxyNode: redactURL(pool.items[0].name), Transport: "healthy", Zen: "success", LastChecked: &otherChecked}},
		Credentials: []bulkCredentialAvailability{
			{Tier: "zen", KeyTail: gw.authCreds[0].display, Fingerprint: credentialFingerprint(TierZen, gw.authCreds[0].key), Pool: "shared", Status: "available", Reason: "success", LastChecked: &otherChecked, CredID: gw.authCreds[0].id},
		},
		Custom: []bulkCustomAvailability{{Name: "c2", BaseURL: "https://other.example", Model: "cm", Status: "unavailable", Reason: "auth_failure", LastChecked: &otherChecked}},
	}
	gw.bulkSnapshot.Store(snap)
	beforeReqs := len(manager.monitor.Snapshot().Upstream.Requests)
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-custom", `{"name":"c1"}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("custom code=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp customCheckResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Custom.Name != "c1" || resp.Custom.Status != "available" {
		t.Fatalf("custom row=%+v want c1/available", resp.Custom)
	}
	body := rec.Body.String()
	for _, secret := range []string{"secret-custom-11111", "secret-custom-22222"} {
		if strings.Contains(body, secret) {
			t.Fatalf("secret %q leaked", secret)
		}
	}
	// No scheduler/health/metrics/history writes.
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "shared", pool.items[0].name); ok {
		t.Fatalf("custom must not write proxy429")
	}
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "shared", pool.items[0].name); ok {
		t.Fatalf("custom must not write channel")
	}
	if got := gw.scheduler.credentialCoolUntil(gw.authCreds[0].id); got > time.Now().UnixNano() {
		t.Fatalf("custom must not write credential401")
	}
	if !pool.items[0].healthy.Load() {
		t.Fatalf("custom must not flip health")
	}
	if got := len(manager.monitor.Snapshot().Upstream.Requests); got != beforeReqs {
		t.Fatalf("custom must not record metrics")
	}
	// Merge preserves native + other custom + global timestamp.
	after := gw.bulkSnapshot.Load()
	if !after.CheckedAt.Equal(checked) {
		t.Fatalf("global timestamp must be preserved")
	}
	if len(after.Nodes) != 1 || len(after.Credentials) != 1 {
		t.Fatalf("native rows must be preserved: %+v", after)
	}
	if after.Nodes[0].LastChecked == nil || !after.Nodes[0].LastChecked.Equal(otherChecked) {
		t.Fatalf("node time must not move")
	}
	seen := map[string]bulkCustomAvailability{}
	for _, c := range after.Custom {
		seen[c.Name] = c
	}
	if len(seen) != 2 {
		t.Fatalf("both custom rows must survive: %+v", after.Custom)
	}
	if seen["c2"].LastChecked == nil || !seen["c2"].LastChecked.Equal(otherChecked) {
		t.Fatalf("other custom time must not move")
	}
	if seen["c1"].LastChecked == nil || seen["c1"].LastChecked.Equal(otherChecked) {
		t.Fatalf("selected custom time must advance")
	}
	// Resources projection exposes custom rows (survives refresh).
	res := manager.Resources()
	found := false
	for _, c := range res.Custom {
		if c.Name == "c1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("resources must project custom rows: %+v", res.Custom)
	}
	// Monitor keys carry fingerprint (never raw).
	foundFP := false
	for _, k := range res.Keys {
		if k.Fingerprint == credentialFingerprint(TierZen, gw.authCreds[0].key) {
			foundFP = true
			if strings.Contains(k.Fingerprint, gw.authCreds[0].key) {
				t.Fatalf("fingerprint leaks raw")
			}
		}
		if strings.Contains(k.ID, gw.authCreds[0].key) {
			t.Fatalf("key id leaks raw")
		}
	}
	if !foundFP {
		t.Fatalf("keys must carry fingerprint")
	}
}

func TestBatchExcludesCustomPreservesStored(t *testing.T) {
	customSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm")))
	}))
	defer customSrv.Close()
	var customHits atomic.Int32
	_ = customHits
	manager, admin, token, csrf := credentialAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(bulkChatSuccessBody("bulk-free-model")))
	}))
	defer srv.Close()
	rt := manager.current.Load()
	rt.gateway.cfg.Upstream.Zen = srv.URL
	rt.gateway.cfg.Upstream.Go = srv.URL
	seedBulkProbeCatalog(rt.gateway)
	// Point the custom channel at a counting wrapper by replacing the
	// fallback client transport is not injectable; instead assert via
	// response/snapshot semantics: batch must not include custom results
	// even when channels are configured, and must preserve stored rows.
	rt.gateway.cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{
		{Name: "c1", BaseURL: customSrv.URL, APIKey: "secret-custom-11111", Model: "cm", Protocol: ProtocolChat},
	}}
	storedAt := time.Now().UTC().Add(-time.Hour)
	stored := bulkCustomAvailability{Name: "c1", BaseURL: redactURL(customSrv.URL), Model: "cm", Status: "available", Reason: "success", LastChecked: &storedAt}
	rt.gateway.bulkSnapshot.Store(&bulkAvailabilitySnapshot{
		CheckedAt: storedAt, TotalNodes: 1, TestedNodes: 1,
		Custom: []bulkCustomAvailability{stored},
	})
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check", `{}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("batch code=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp bulkCheckResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Custom) != 0 {
		t.Fatalf("batch must not include custom results, got %+v", resp.Custom)
	}
	after := rt.gateway.bulkSnapshot.Load()
	if len(after.Custom) != 1 || after.Custom[0].Name != "c1" {
		t.Fatalf("batch must preserve stored custom rows: %+v", after.Custom)
	}
	if after.Custom[0].LastChecked == nil || !after.Custom[0].LastChecked.Equal(storedAt) {
		t.Fatalf("stored custom time must not move on batch")
	}
}

func TestPerRowWebUIContracts(t *testing.T) {
	data, err := readWebUIFile()
	if err != nil {
		t.Fatal(err)
	}
	html := string(data)
	// Credential + custom tables carry 操作 columns (proxy/custom use the
	// shared centered alignment class; credential keeps the plain header).
	if !strings.Contains(html, "上次检测</th>") || !strings.Contains(html, "操作</th>") {
		t.Fatalf("credential/custom tables must carry 操作 column")
	}
	if got := strings.Count(html, ">操作</th>"); got < 3 {
		t.Fatalf("操作 columns=%d want >=3 (proxy+credential+custom)", got)
	}
	for _, needle := range []string{
		`/api/availability/check-credential`,
		`/api/availability/check-custom`,
		`function probeCredential(`,
		`function probeCustom(`,
		`function mergeCustomRows(`,
		`function mergeCustomRowIntoCache(`,
		`S.customAvail`,
		`setAttribute("aria-label","检测凭证可用性")`,
		`setAttribute("aria-label","检测自定义渠道可用性")`,
		`k.fingerprint`,
		`fingerprint:fp`,
		`JSON.stringify({id:cid})`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("webui per-row contract missing %q", needle)
		}
	}
	// Proxy batch scope: the proxy bulkCheck calls only the proxy batch
	// endpoint. Credential/custom batch operations live in their own scoped
	// functions (bulkCheckCredentials/bulkCheckCustoms, covered by the scoped
	// batch contract test), never as aliases inside the proxy block and never
	// as per-row fan-out.
	bulkIdx := strings.Index(html, "function bulkCheck()")
	if bulkIdx < 0 {
		t.Fatal("missing bulkCheck")
	}
	bulkEnd := strings.Index(html[bulkIdx:], "function bulkCheckCredentials()")
	bulkBlock := html[bulkIdx:]
	if bulkEnd >= 0 {
		bulkBlock = bulkBlock[:bulkEnd]
	}
	if got := strings.Count(bulkBlock, "/api/availability/check\""); got != 1 {
		t.Fatalf("proxy bulkCheck must call only its scoped endpoint, got %d", got)
	}
	for _, forbidden := range []string{
		"/api/availability/check-node",
		`/api/availability/check-credential"`,
		"/api/availability/check-credentials",
		`/api/availability/check-custom"`,
		"/api/availability/check-customs",
	} {
		if strings.Contains(bulkBlock, forbidden) {
			t.Fatalf("proxy bulkCheck must not contain %q", forbidden)
		}
	}
	// Buttons reuse existing probe markup/styles; toasts use returned status.
	for _, needle := range []string{
		`className="ghost probe-btn"`,
		`className="probe-label"`,
		`className="probe-spin"`,
		`btn.classList.add("is-busy")`,
		`btn.classList.remove("is-busy")`,
		`toast("检测 "`,
		`refreshMonitor(`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("per-row button/toast contract missing %q", needle)
		}
	}
	// DOM text nodes only.
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
		if strings.Contains(html, sink) {
			t.Fatalf("forbidden DOM sink %q", sink)
		}
	}
	// Fingerprint identity: never tail-only, never raw key.
	if !strings.Contains(html, "fingerprint") {
		t.Fatalf("webui must use fingerprint identity")
	}
}
