package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Scoped batch availability checks: one backend operation per table.
//
//   - POST /api/availability/check-credentials: all configured authenticated
//     credentials only (no anonymous sends, no custom sends).
//   - POST /api/availability/check-customs: every configured custom channel
//     exactly once including inactive ones (no native sends).
//
// Both share the bulk rate limiter and the bulkMu single-flight gate with
// the proxy batch and the per-row checks, reuse concurrency 4, the
// overall/per-send timeouts, the total-send cap, and strict {} bodies.

func scopedBatchAdmin(t *testing.T, pools map[string][]string, routing ProxyRoutingConfig, zenKeys []string) (*RuntimeManager, *AdminServer, string, string) {
	t.Helper()
	return credentialAdmin(t, pools, routing, zenKeys, nil)
}

func TestCredentialsBatchAuthCSRFStrictNoStore(t *testing.T) {
	_, admin, token, csrf := scopedBatchAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"})
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credentials", `{}`, "", "")); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauth code=%d want 401", rec.Code)
	}
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credentials", `{}`, token, "wrong")); rec.Code != http.StatusForbidden {
		t.Fatalf("bad csrf code=%d want 403", rec.Code)
	}
	req := operabilityRequest(http.MethodPost, "/api/availability/check-credentials", `{}`, token, csrf)
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
		{"array", `[]`},
		{"scalar", `123`},
		{"unknown field", `{"pool":"x"}`},
		{"non-empty", `{"fingerprint":"abc"}`},
		{"trailing", `{}{}`},
		{"empty body", ``},
	} {
		_, admin2, token2, csrf2 := scopedBatchAdmin(t,
			map[string][]string{"shared": {"direct"}},
			ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
			[]string{"zen-key-12345"})
		rec := serveAdmin(admin2, operabilityRequest(http.MethodPost, "/api/availability/check-credentials", tc.body, token2, csrf2))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: code=%d want 400 body=%s", tc.name, rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s: missing no-store", tc.name)
		}
	}
}

func TestCustomsBatchAuthCSRFStrictNoStore(t *testing.T) {
	_, admin, token, csrf := scopedBatchAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		nil)
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-customs", `{}`, "", "")); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauth code=%d want 401", rec.Code)
	}
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-customs", `{}`, token, "wrong")); rec.Code != http.StatusForbidden {
		t.Fatalf("bad csrf code=%d want 403", rec.Code)
	}
	req := operabilityRequest(http.MethodPost, "/api/availability/check-customs", `{}`, token, csrf)
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
		{"array", `[]`},
		{"scalar", `123`},
		{"unknown field", `{"id":"c1"}`},
		{"non-empty", `{"name":"c1"}`},
		{"trailing", `{}{}`},
		{"empty body", ``},
	} {
		_, admin2, token2, csrf2 := scopedBatchAdmin(t,
			map[string][]string{"shared": {"direct"}},
			ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
			nil)
		rec := serveAdmin(admin2, operabilityRequest(http.MethodPost, "/api/availability/check-customs", tc.body, token2, csrf2))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: code=%d want 400 body=%s", tc.name, rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s: missing no-store", tc.name)
		}
	}
}

func TestCredentialsBatchAllCredentialsNoAnonNoCustom(t *testing.T) {
	var zenHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		zenHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(bulkChatSuccessBody("bulk-free-model")))
	}))
	defer srv.Close()
	var customHits atomic.Int32
	customSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm")))
	}))
	defer customSrv.Close()
	manager, admin, token, csrf := scopedBatchAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-aaaaa-11111", "zen-key-bbbbb-22222"})
	rt := manager.current.Load()
	rt.gateway.cfg.Upstream.Zen = srv.URL
	rt.gateway.cfg.Upstream.Go = srv.URL
	seedBulkProbeCatalog(rt.gateway)
	rt.gateway.cfg.Fallback = FallbackConfig{Channels: []FallbackChannelConfig{
		{ID: "c1", Name: "c1", BaseURL: customSrv.URL, APIKey: "secret-custom-11111", Model: "cm", Protocol: ProtocolChat},
	}}
	rt.gateway.customClient = customSrv.Client()
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credentials", `{}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("batch code=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp credentialsBatchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.TotalCredentials != 2 || len(resp.Credentials) != 2 {
		t.Fatalf("must cover all 2 credentials exactly once: %+v", resp)
	}
	sumTested := 0
	for _, c := range resp.Credentials {
		if c.TestedNodes == 0 {
			t.Fatalf("each credential must send: %+v", c)
		}
		if c.Status != "available" || c.Reason != "success" {
			t.Fatalf("row=%+v want available/success", c)
		}
		sumTested += c.TestedNodes
	}
	if resp.TestedNodes != sumTested {
		t.Fatalf("tested=%d want sum %d", resp.TestedNodes, sumTested)
	}
	// No anonymous sends: every native send belongs to a credential group.
	if got := int(zenHits.Load()); got != sumTested {
		t.Fatalf("native hits=%d want credential sends %d (no anonymous)", got, sumTested)
	}
	if got := customHits.Load(); got != 0 {
		t.Fatalf("custom hits=%d want 0", got)
	}
	// Scoped response carries no node or custom rows.
	var raw map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["nodes"]; ok {
		t.Fatal("credentials batch must not include node rows")
	}
	if _, ok := raw["custom"]; ok {
		t.Fatal("credentials batch must not include custom rows")
	}
	body := rec.Body.String()
	for _, secret := range []string{"zen-key-aaaaa-11111", "zen-key-bbbbb-22222", "secret-custom-11111"} {
		if strings.Contains(body, secret) {
			t.Fatalf("secret leaked in response")
		}
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control=%q want no-store", got)
	}
}

func TestCredentialsBatchNoKeys(t *testing.T) {
	var zenHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		zenHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(bulkChatSuccessBody("bulk-free-model")))
	}))
	defer srv.Close()
	manager, admin, token, csrf := scopedBatchAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		nil)
	rt := manager.current.Load()
	rt.gateway.cfg.Upstream.Zen = srv.URL
	seedBulkProbeCatalog(rt.gateway)
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credentials", `{}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("batch code=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp credentialsBatchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.TotalCredentials != 0 || len(resp.Credentials) != 0 {
		t.Fatalf("no keys must yield no rows: %+v", resp)
	}
	found := false
	for _, n := range resp.NoModel {
		if n == "unconfigured" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no keys must report unconfigured: %+v", resp.NoModel)
	}
	if got := zenHits.Load(); got != 0 {
		t.Fatalf("no keys must send nothing, hits=%d", got)
	}
}

func TestCredentialsBatchNoModel(t *testing.T) {
	var zenHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		zenHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(bulkChatSuccessBody("bulk-free-model")))
	}))
	defer srv.Close()
	manager, admin, token, csrf := scopedBatchAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"})
	rt := manager.current.Load()
	rt.gateway.cfg.Upstream.Zen = srv.URL
	// Empty catalog: no directory model for the authenticated lane.
	rt.gateway.catalog.ReplaceWithCapabilities(nil, nil, nil, nil, nil)
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credentials", `{}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("batch code=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp credentialsBatchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Credentials) != 1 {
		t.Fatalf("no-model must still yield one row per credential: %+v", resp)
	}
	if resp.Credentials[0].Status != "inconclusive" || resp.Credentials[0].Reason != "no_model" {
		t.Fatalf("row=%+v want inconclusive/no_model", resp.Credentials[0])
	}
	if got := zenHits.Load(); got != 0 {
		t.Fatalf("no-model must send nothing, hits=%d", got)
	}
}

func TestCredentialsBatchCapPartial(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(bulkChatSuccessBody("bulk-free-model")))
	}))
	defer srv.Close()
	keys := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		keys = append(keys, "zen-batch-key-abcdef-"+string(rune('a'+i/26))+string(rune('a'+i%26))+"-12345")
	}
	proxies := []string{"direct", "http://127.0.0.1:8081", "http://127.0.0.1:8082", "http://127.0.0.1:8083"}
	manager, admin, token, csrf := scopedBatchAdmin(t,
		map[string][]string{"shared": proxies},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		keys)
	rt := manager.current.Load()
	rt.gateway.cfg.Upstream.Zen = srv.URL
	rt.gateway.cfg.Upstream.Go = srv.URL
	seedBulkProbeCatalog(rt.gateway)
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credentials", `{}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("batch code=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp credentialsBatchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.TotalCredentials != 40 || len(resp.Credentials) != 40 {
		t.Fatalf("must include every credential: total=%d rows=%d", resp.TotalCredentials, len(resp.Credentials))
	}
	if !resp.Truncated || resp.SkippedNodes == 0 {
		t.Fatalf("cap overflow must report truncated/skipped: %+v", resp)
	}
	if resp.TestedNodes != bulkMaxTotalSends {
		t.Fatalf("tested=%d want cap %d", resp.TestedNodes, bulkMaxTotalSends)
	}
	seen := map[string]bool{}
	skippedRows := 0
	for _, c := range resp.Credentials {
		if seen[c.Fingerprint] {
			t.Fatalf("duplicate credential row %q", c.Fingerprint)
		}
		seen[c.Fingerprint] = true
		if c.Status == "inconclusive" && c.Reason == "skipped" {
			skippedRows++
		}
	}
	if skippedRows == 0 {
		t.Fatalf("capped credentials need explicit skipped rows: %+v", resp.Credentials)
	}
}

func TestCredentialsBatchWritesAndMerge(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(401)
		_, _ = w.Write([]byte(`{"error":{"message":"unauthorized"}}`))
	}))
	defer srv.Close()
	manager, admin, token, csrf := scopedBatchAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"})
	rt := manager.current.Load()
	gw := rt.gateway
	gw.cfg.Upstream.Zen = srv.URL
	gw.cfg.Upstream.Go = srv.URL
	seedBulkProbeCatalog(gw)
	// Seed node + custom rows with an older global timestamp.
	checked := time.Now().UTC().Add(-time.Hour)
	otherChecked := checked
	pool := gw.pools["shared"]
	snap := &bulkAvailabilitySnapshot{
		CheckedAt: checked, TotalNodes: 1, TestedNodes: 1,
		Nodes:  []bulkNodeAvailability{{Pool: "shared", Index: 0, ProxyNode: redactURL(pool.items[0].name), Transport: "healthy", Zen: "success", LastChecked: &otherChecked}},
		Custom: []bulkCustomAvailability{{ID: "c9", Name: "c9", BaseURL: "https://other.example", Model: "cm", Status: "available", Reason: "success", LastChecked: &otherChecked}},
	}
	gw.bulkSnapshot.Store(snap)
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credentials", `{}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("batch code=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp credentialsBatchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Credentials) != 1 || resp.Credentials[0].Status != "unavailable" || resp.Credentials[0].Reason != "auth_failure" {
		t.Fatalf("401 must classify unavailable/auth_failure: %+v", resp.Credentials)
	}
	// Shared write rule: real-credential 401 cools the credential.
	if got := gw.scheduler.credentialCoolUntil(gw.authCreds[0].id); got <= time.Now().UnixNano() {
		t.Fatal("401 must cool the credential")
	}
	// Merge preserves node/custom rows and the global timestamp.
	after := gw.bulkSnapshot.Load()
	if !after.CheckedAt.Equal(checked) {
		t.Fatal("global timestamp must be preserved")
	}
	if len(after.Nodes) != 1 || len(after.Custom) != 1 {
		t.Fatalf("node/custom rows must be preserved: %+v", after)
	}
	if len(after.Credentials) != 1 {
		t.Fatalf("credential row must merge: %+v", after.Credentials)
	}
	// Monitor keys carry fingerprint (never raw).
	res := manager.Resources()
	foundFP := false
	for _, k := range res.Keys {
		if k.Fingerprint == credentialFingerprint(TierZen, gw.authCreds[0].key) {
			foundFP = true
		}
		if strings.Contains(k.ID, gw.authCreds[0].key) {
			t.Fatal("key id leaks raw")
		}
	}
	if !foundFP {
		t.Fatal("keys must carry fingerprint")
	}
}

type customsBatchRecorder struct {
	mu     sync.Mutex
	hits   map[string]int
	bodies map[string]map[string]any
	header http.Header
	paths  map[string]string
	auths  map[string]string
}

func (r *customsBatchRecorder) handler(status int, body func(model string) string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		raw, _ := io.ReadAll(io.LimitReader(req.Body, 2<<20))
		var payload map[string]any
		_ = json.Unmarshal(raw, &payload)
		model, _ := payload["model"].(string)
		r.mu.Lock()
		if r.hits == nil {
			r.hits = map[string]int{}
		}
		if r.bodies == nil {
			r.bodies = map[string]map[string]any{}
		}
		if r.paths == nil {
			r.paths = map[string]string{}
		}
		if r.auths == nil {
			r.auths = map[string]string{}
		}
		// Attribute by Authorization key tail to stay key-agnostic.
		r.hits[req.URL.Path+"|"+req.Header.Get("Authorization")]++
		r.paths[req.Header.Get("Authorization")] = req.URL.Path
		r.auths[req.Header.Get("Authorization")] = req.Header.Get("Authorization")
		r.bodies[req.Header.Get("Authorization")] = payload
		if r.header == nil {
			r.header = req.Header.Clone()
		}
		r.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body(model)))
	}
}

func TestCustomsBatchAllChannelsExactlyOnce(t *testing.T) {
	var zenHits atomic.Int32
	zenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		zenHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(bulkChatSuccessBody("bulk-free-model")))
	}))
	defer zenSrv.Close()
	rec := &customsBatchRecorder{}
	customSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		raw, _ := io.ReadAll(io.LimitReader(req.Body, 2<<20))
		var payload map[string]any
		_ = json.Unmarshal(raw, &payload)
		model, _ := payload["model"].(string)
		rec.mu.Lock()
		if rec.hits == nil {
			rec.hits = map[string]int{}
		}
		rec.hits[req.URL.Path+"|"+req.Header.Get("Authorization")]++
		rec.mu.Unlock()
		proto := ProtocolChat
		if strings.HasSuffix(req.URL.Path, "/v1/responses") {
			proto = ProtocolResponses
		}
		w.WriteHeader(200)
		if proto == ProtocolResponses {
			_, _ = w.Write([]byte(fallbackResponsesOK(model)))
		} else {
			_, _ = w.Write([]byte(fallbackChatOK(model)))
		}
	}))
	defer customSrv.Close()
	manager, admin, token, csrf := scopedBatchAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"})
	rt := manager.current.Load()
	gw := rt.gateway
	gw.cfg.Upstream.Zen = zenSrv.URL
	gw.cfg.Upstream.Go = zenSrv.URL
	seedBulkProbeCatalog(gw)
	// One active plus two inactive channels, mixed protocols.
	gw.cfg.Fallback = FallbackConfig{Active: "ch-active", Channels: []FallbackChannelConfig{
		{ID: "ch-active", Name: "Active", BaseURL: customSrv.URL, APIKey: "secret-active-11111", Model: "m-chat", Protocol: ProtocolChat},
		{ID: "ch-idle-1", Name: "Idle One", BaseURL: customSrv.URL, APIKey: "secret-idle-22222", Model: "m-resp", Protocol: ProtocolResponses},
		{ID: "ch-idle-2", Name: "Idle Two", BaseURL: customSrv.URL + "/v1", APIKey: "secret-idle-33333", Model: "m-chat-2", Protocol: ProtocolChat},
	}}
	gw.customClient = customSrv.Client()
	beforeReqs := len(manager.monitor.Snapshot().Upstream.Requests)
	resp := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-customs", `{}`, token, csrf))
	if resp.Code != http.StatusOK {
		t.Fatalf("batch code=%d body=%s", resp.Code, resp.Body.String())
	}
	var out customsBatchResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.TotalChannels != 3 || len(out.Custom) != 3 {
		t.Fatalf("must include every channel exactly once: %+v", out)
	}
	seen := map[string]bool{}
	for _, c := range out.Custom {
		if seen[c.ID] {
			t.Fatalf("duplicate channel row %q", c.ID)
		}
		seen[c.ID] = true
		if c.Status != "available" || c.Reason != "success" {
			t.Fatalf("row=%+v want available/success", c)
		}
		if c.LastChecked == nil {
			t.Fatalf("row %q missing last_checked", c.ID)
		}
	}
	for _, id := range []string{"ch-active", "ch-idle-1", "ch-idle-2"} {
		if !seen[id] {
			t.Fatalf("missing channel %q in %+v", id, out.Custom)
		}
	}
	rec.mu.Lock()
	totalCustomHits := 0
	for _, n := range rec.hits {
		totalCustomHits += n
		if n != 1 {
			rec.mu.Unlock()
			t.Fatalf("each channel must probe exactly once, hits=%v", rec.hits)
		}
	}
	rec.mu.Unlock()
	if totalCustomHits != 3 {
		t.Fatalf("custom hits=%d want 3", totalCustomHits)
	}
	if got := zenHits.Load(); got != 0 {
		t.Fatalf("customs batch must not send native probes, zen hits=%d", got)
	}
	// Scoped response carries no node or credential rows.
	var raw map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["nodes"]; ok {
		t.Fatal("customs batch must not include node rows")
	}
	if _, ok := raw["credentials"]; ok {
		t.Fatal("customs batch must not include credential rows")
	}
	body := resp.Body.String()
	for _, secret := range []string{"secret-active-11111", "secret-idle-22222", "secret-idle-33333", "zen-key-12345"} {
		if strings.Contains(body, secret) {
			t.Fatalf("secret leaked in response")
		}
	}
	if got := len(manager.monitor.Snapshot().Upstream.Requests); got != beforeReqs {
		t.Fatal("customs batch must not record inference metrics")
	}
	if got := resp.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control=%q want no-store", got)
	}
}

func TestCustomsBatchRequestShapeAndEffort(t *testing.T) {
	type seen struct {
		path        string
		auth        string
		contentType string
		accept      string
		ua          string
		cli         string
		ses         string
		reqID       string
		prj         string
		body        map[string]any
	}
	var mu sync.Mutex
	got := map[string]seen{} // keyed by channel model
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		raw, _ := io.ReadAll(io.LimitReader(req.Body, 2<<20))
		var payload map[string]any
		_ = json.Unmarshal(raw, &payload)
		model, _ := payload["model"].(string)
		mu.Lock()
		got[model] = seen{
			path: req.URL.Path, auth: req.Header.Get("Authorization"),
			contentType: req.Header.Get("Content-Type"), accept: req.Header.Get("Accept"),
			ua: req.Header.Get("User-Agent"), cli: req.Header.Get("x-opencode-client"),
			ses: req.Header.Get("x-opencode-session"), reqID: req.Header.Get("x-opencode-request"),
			prj:  req.Header.Get("x-opencode-project"),
			body: payload,
		}
		mu.Unlock()
		w.WriteHeader(200)
		if strings.HasSuffix(req.URL.Path, "/v1/responses") {
			_, _ = w.Write([]byte(fallbackResponsesOK(model)))
		} else {
			_, _ = w.Write([]byte(fallbackChatOK(model)))
		}
	}))
	defer srv.Close()
	manager, admin, token, csrf := scopedBatchAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		nil)
	rt := manager.current.Load()
	gw := rt.gateway
	gw.cfg.Fallback = FallbackConfig{Active: "e-high", Channels: []FallbackChannelConfig{
		{ID: "e-high", Name: "High", BaseURL: srv.URL, APIKey: "secret-high-11111", Model: "m-high", Protocol: ProtocolChat, ReasoningEffort: "high"},
		{ID: "e-resp-high", Name: "RespHigh", BaseURL: srv.URL, APIKey: "secret-resp-22222", Model: "m-resp-high", Protocol: ProtocolResponses, ReasoningEffort: "high"},
		{ID: "e-inherit", Name: "Inherit", BaseURL: srv.URL, APIKey: "secret-inherit-33333", Model: "m-inherit", Protocol: ProtocolChat, ReasoningEffort: "inherit"},
		{ID: "e-default", Name: "Default", BaseURL: srv.URL, APIKey: "secret-default-44444", Model: "m-default", Protocol: ProtocolResponses, ReasoningEffort: ""},
	}}
	gw.customClient = srv.Client()
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-customs", `{}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("batch code=%d body=%s", rec.Code, rec.Body.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 4 {
		t.Fatalf("each channel must send once, got %d", len(got))
	}
	// Configured protocol endpoints and model identity.
	if got["m-high"].path != "/v1/chat/completions" {
		t.Fatalf("chat endpoint=%q", got["m-high"].path)
	}
	if got["m-resp-high"].path != "/v1/responses" {
		t.Fatalf("responses endpoint=%q", got["m-resp-high"].path)
	}
	// Shared request construction: auth, headers, non-streaming Accept.
	wantAuth := map[string]string{
		"m-high": "Bearer secret-high-11111", "m-resp-high": "Bearer secret-resp-22222",
		"m-inherit": "Bearer secret-inherit-33333", "m-default": "Bearer secret-default-44444",
	}
	for model, s := range got {
		if s.auth != wantAuth[model] {
			t.Fatalf("%s auth=%q", model, s.auth)
		}
		if s.contentType != "application/json" {
			t.Fatalf("%s content-type=%q", model, s.contentType)
		}
		if s.accept != "application/json" {
			t.Fatalf("%s accept=%q want non-streaming probe accept", model, s.accept)
		}
		if s.ua != opencodeWireUserAgent() {
			t.Fatalf("%s ua=%q", model, s.ua)
		}
		if s.cli != "cli" {
			t.Fatalf("%s x-opencode-client=%q", model, s.cli)
		}
		if !isCanonicalWireSession(s.ses) || !isCanonicalWireRequest(s.reqID) || !isCanonicalWireProject(s.prj) {
			t.Fatalf("%s probe must carry canonical session/request/project, got %q %q %q", model, s.ses, s.reqID, s.prj)
		}
		if st, _ := s.body["stream"].(bool); s.path == "/v1/chat/completions" && st {
			t.Fatalf("%s probe must be non-streaming", model)
		}
		if m, _ := s.body["model"].(string); m != model {
			t.Fatalf("body model=%q want %q", m, model)
		}
	}
	// Reasoning effort overlay via the shared helper.
	if v, _ := got["m-high"].body["reasoning_effort"].(string); v != "high" {
		t.Fatalf("chat/high must carry reasoning_effort=high, body=%v", got["m-high"].body)
	}
	rm, ok := got["m-resp-high"].body["reasoning"].(map[string]any)
	if !ok || rm["effort"] != "high" {
		t.Fatalf("responses/high must carry reasoning.effort=high, body=%v", got["m-resp-high"].body)
	}
	for _, model := range []string{"m-inherit", "m-default"} {
		for _, k := range []string{"reasoning_effort", "reasoning", "effort"} {
			if _, present := got[model].body[k]; present {
				t.Fatalf("%s must carry no strength (minimal body), found %q", model, k)
			}
		}
	}
}

func TestCustomsBatchExact200AndCap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		raw, _ := io.ReadAll(io.LimitReader(req.Body, 2<<20))
		var payload map[string]any
		_ = json.Unmarshal(raw, &payload)
		if model, _ := payload["model"].(string); model == "m-201" {
			w.WriteHeader(201)
			_, _ = w.Write([]byte(fallbackChatOK("m-201")))
			return
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("m-exact")))
	}))
	defer srv.Close()
	manager, admin, token, csrf := scopedBatchAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		nil)
	rt := manager.current.Load()
	gw := rt.gateway
	gw.cfg.Fallback = FallbackConfig{Channels: []FallbackChannelConfig{
		{ID: "c-exact", Name: "Exact", BaseURL: srv.URL, APIKey: "secret-exact-11111", Model: "m-exact", Protocol: ProtocolChat},
		{ID: "c-201", Name: "Created", BaseURL: srv.URL, APIKey: "secret-201-22222", Model: "m-201", Protocol: ProtocolChat},
	}}
	gw.customClient = srv.Client()
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-customs", `{}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("batch code=%d body=%s", rec.Code, rec.Body.String())
	}
	var out customsBatchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	byID := map[string]bulkCustomAvailability{}
	for _, c := range out.Custom {
		byID[c.ID] = c
	}
	if byID["c-exact"].Status != "available" {
		t.Fatalf("exact 200 must succeed: %+v", byID["c-exact"])
	}
	// Only valid HTTP 200 is success; 201 with a valid body stays real.
	if byID["c-201"].Status == "available" {
		t.Fatalf("201 must not count as success: %+v", byID["c-201"])
	}

	// Cap: channels beyond the total-send cap report skipped/inconclusive.
	channels := make([]FallbackChannelConfig, 0, bulkMaxTotalSends+2)
	for i := 0; i < bulkMaxTotalSends+2; i++ {
		id := "cap-" + strconv.Itoa(i)
		channels = append(channels, FallbackChannelConfig{
			ID: id, Name: "Cap " + strconv.Itoa(i),
			BaseURL: srv.URL, APIKey: "secret-cap-12345", Model: "m-exact", Protocol: ProtocolChat,
		})
	}
	manager2, admin2, token2, csrf2 := scopedBatchAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		nil)
	rt2 := manager2.current.Load()
	rt2.gateway.cfg.Fallback = FallbackConfig{Channels: channels}
	rt2.gateway.customClient = srv.Client()
	rec2 := serveAdmin(admin2, operabilityRequest(http.MethodPost, "/api/availability/check-customs", `{}`, token2, csrf2))
	if rec2.Code != http.StatusOK {
		t.Fatalf("cap batch code=%d body=%s", rec2.Code, rec2.Body.String())
	}
	var out2 customsBatchResponse
	if err := json.Unmarshal(rec2.Body.Bytes(), &out2); err != nil {
		t.Fatal(err)
	}
	if out2.TotalChannels != bulkMaxTotalSends+2 || len(out2.Custom) != bulkMaxTotalSends+2 {
		t.Fatalf("must include every channel: total=%d rows=%d", out2.TotalChannels, len(out2.Custom))
	}
	if !out2.Truncated || out2.SkippedChannels != 2 || out2.TestedChannels != bulkMaxTotalSends {
		t.Fatalf("cap must report truncated/skipped/tested: %+v", map[string]any{"truncated": out2.Truncated, "skipped": out2.SkippedChannels, "tested": out2.TestedChannels})
	}
	skippedRows := 0
	for _, c := range out2.Custom {
		if c.Status == "inconclusive" && c.Reason == "skipped" {
			skippedRows++
		}
	}
	if skippedRows != 2 {
		t.Fatalf("capped channels need explicit skipped rows, got %d", skippedRows)
	}
}

func TestCustomsBatchIsolationAndMerge(t *testing.T) {
	var zenHits atomic.Int32
	zenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		zenHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(bulkChatSuccessBody("bulk-free-model")))
	}))
	defer zenSrv.Close()
	customSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte(`{"error":{"message":"boom"}}`))
	}))
	defer customSrv.Close()
	manager, admin, token, csrf := scopedBatchAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"})
	rt := manager.current.Load()
	gw := rt.gateway
	gw.cfg.Upstream.Zen = zenSrv.URL
	gw.cfg.Upstream.Go = zenSrv.URL
	seedBulkProbeCatalog(gw)
	gw.cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{
		{ID: "c1", Name: "c1", BaseURL: customSrv.URL, APIKey: "secret-custom-11111", Model: "cm", Protocol: ProtocolChat},
	}}
	gw.customClient = customSrv.Client()
	// Seed native rows with an older global timestamp.
	checked := time.Now().UTC().Add(-time.Hour)
	otherChecked := checked
	pool := gw.pools["shared"]
	snap := &bulkAvailabilitySnapshot{
		CheckedAt: checked, TotalNodes: 1, TestedNodes: 1,
		Nodes: []bulkNodeAvailability{{Pool: "shared", Index: 0, ProxyNode: redactURL(pool.items[0].name), Transport: "healthy", Zen: "success", LastChecked: &otherChecked}},
		Credentials: []bulkCredentialAvailability{
			{Tier: "zen", KeyTail: gw.authCreds[0].display, Fingerprint: credentialFingerprint(TierZen, gw.authCreds[0].key), Pool: "shared", Status: "available", Reason: "success", LastChecked: &otherChecked, CredID: gw.authCreds[0].id},
		},
	}
	gw.bulkSnapshot.Store(snap)
	beforeReqs := len(manager.monitor.Snapshot().Upstream.Requests)
	beforeAttempts := len(manager.monitor.Snapshot().Upstream.Recent)
	beforeTakeovers := gw.scheduler.fallbacks.count()
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-customs", `{}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("batch code=%d body=%s", rec.Code, rec.Body.String())
	}
	var out customsBatchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Custom) != 1 || out.Custom[0].Status != "unavailable" || out.Custom[0].Reason != "upstream_failure" {
		t.Fatalf("500 must classify unavailable/upstream_failure: %+v", out.Custom)
	}
	// Isolation: no Zen scheduler/health/metrics/binding writes.
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "shared", pool.items[0].name); ok {
		t.Fatal("customs batch must not write proxy429")
	}
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "shared", pool.items[0].name); ok {
		t.Fatal("customs batch must not write channel")
	}
	if got := gw.scheduler.credentialCoolUntil(gw.authCreds[0].id); got > time.Now().UnixNano() {
		t.Fatal("customs batch must not write credential401")
	}
	if !pool.items[0].healthy.Load() {
		t.Fatal("customs batch must not flip transport health")
	}
	if got := len(manager.monitor.Snapshot().Upstream.Requests); got != beforeReqs {
		t.Fatal("customs batch must not record metrics")
	}
	if got := len(manager.monitor.Snapshot().Upstream.Recent); got != beforeAttempts {
		t.Fatal("customs batch must not record attempts")
	}
	if got := gw.scheduler.fallbacks.count(); got != beforeTakeovers {
		t.Fatal("customs batch must not bind takeover state")
	}
	if got := zenHits.Load(); got != 0 {
		t.Fatalf("customs batch must not send native probes, hits=%d", got)
	}
	// Merge: native rows and global timestamp preserved, custom row merged.
	after := gw.bulkSnapshot.Load()
	if !after.CheckedAt.Equal(checked) {
		t.Fatal("global timestamp must be preserved")
	}
	if len(after.Nodes) != 1 || len(after.Credentials) != 1 {
		t.Fatalf("native rows must be preserved: %+v", after)
	}
	if len(after.Custom) != 1 || after.Custom[0].ID != "c1" {
		t.Fatalf("custom row must merge: %+v", after.Custom)
	}
	if after.Custom[0].LastChecked == nil || after.Custom[0].LastChecked.Equal(otherChecked) {
		t.Fatal("probed custom time must advance")
	}
	res := manager.Resources()
	found := false
	for _, c := range res.Custom {
		if c.ID == "c1" && c.Status == "unavailable" {
			found = true
		}
	}
	if !found {
		t.Fatalf("resources must project customs batch rows: %+v", res.Custom)
	}
}

func TestCustomsBatchZeroChannels(t *testing.T) {
	manager, admin, token, csrf := scopedBatchAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		nil)
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-customs", `{}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("batch code=%d body=%s", rec.Code, rec.Body.String())
	}
	var out customsBatchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.TotalChannels != 0 || len(out.Custom) != 0 {
		t.Fatalf("zero channels must stay empty: %+v", out)
	}
	if manager.current.Load().gateway.bulkSnapshot.Load() != nil {
		t.Fatal("zero-channel batch must not create a snapshot")
	}
}

func TestScopedBatchesShareSingleFlight(t *testing.T) {
	manager, admin, token, csrf := scopedBatchAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"})
	gw := manager.current.Load().gateway
	gw.bulkMu.Lock()
	defer gw.bulkMu.Unlock()
	for _, path := range []string{"/api/availability/check-credentials", "/api/availability/check-customs", "/api/availability/check"} {
		if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, path, `{}`, token, csrf)); rec.Code != http.StatusConflict {
			t.Fatalf("%s: code=%d want 409 during single-flight", path, rec.Code)
		} else if !strings.Contains(rec.Body.String(), "bulk_busy") {
			t.Fatalf("%s must carry bulk_busy, got %s", path, rec.Body.String())
		}
	}
}

func TestScopedBatchWebUIContracts(t *testing.T) {
	data, err := readWebUIFile()
	if err != nil {
		t.Fatal(err)
	}
	html := string(data)
	// Three scope-correct batch buttons with explicit operator labels.
	for _, needle := range []string{
		`id="btn-bulk-check"`,
		`id="btn-bulk-check-credentials"`,
		`>批量检测凭证</button>`,
		`id="btn-bulk-check-customs"`,
		`>批量检测渠道</button>`,
		`id="bulk-credentials-state"`,
		`id="bulk-customs-state"`,
		`function bulkCheck(`,
		`function bulkCheckCredentials(`,
		`function bulkCheckCustoms(`,
		`function availBatchButtons(`,
		`function setAvailBusy(`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("scoped batch webui must contain %q", needle)
		}
	}
	// Each batch button wires only its scoped function.
	for _, needle := range []string{
		`$("btn-bulk-check-credentials"); if(b)b.addEventListener("click",bulkCheckCredentials);`,
		`$("btn-bulk-check-customs"); if(b)b.addEventListener("click",bulkCheckCustoms);`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("scoped batch wiring must contain %q", needle)
		}
	}
	// Scope correctness per function: exactly its backend operation, no
	// alias to another scope and no per-row fan-out.
	credBlock := sliceFn(html, "function bulkCheckCredentials()", "(function(){ var b=$(\"btn-bulk-check-credentials\")")
	customBlock := sliceFn(html, "function bulkCheckCustoms()", "(function(){ var b=$(\"btn-bulk-check-customs\")")
	proxyIdx := strings.Index(html, "function bulkCheck()")
	if proxyIdx < 0 {
		t.Fatal("missing bulkCheck")
	}
	proxyEnd := strings.Index(html[proxyIdx:], "function bulkCheckCredentials()")
	proxyBlock := html[proxyIdx:]
	if proxyEnd >= 0 {
		proxyBlock = proxyBlock[:proxyEnd]
	}
	if credBlock == "" || customBlock == "" {
		t.Fatal("missing scoped batch functions")
	}
	if got := strings.Count(credBlock, "/api/availability/check-credentials"); got != 1 {
		t.Fatalf("credentials batch must call its endpoint exactly once, got %d", got)
	}
	if got := strings.Count(customBlock, "/api/availability/check-customs"); got != 1 {
		t.Fatalf("customs batch must call its endpoint exactly once, got %d", got)
	}
	if got := strings.Count(proxyBlock, "/api/availability/check\""); got != 1 {
		t.Fatalf("proxy batch must call its endpoint exactly once, got %d", got)
	}
	for _, block := range []string{credBlock, customBlock, proxyBlock} {
		for _, forbidden := range []string{
			"/api/availability/check-node",
			`probeCredential(`, `probeCustom(`, `probeProxy(`,
		} {
			if strings.Contains(block, forbidden) {
				t.Fatalf("batch block must not contain %q (no per-row fan-out)", forbidden)
			}
		}
	}
	if strings.Contains(credBlock, "/api/availability/check-customs") {
		t.Fatal("credentials batch must not trigger the customs operation")
	}
	if strings.Contains(credBlock, `/api/availability/check-credential"`) {
		t.Fatal("credentials batch must not loop the per-row credential endpoint")
	}
	if strings.Contains(customBlock, "/api/availability/check-credentials") {
		t.Fatal("customs batch must not trigger the credentials operation")
	}
	if strings.Contains(customBlock, `/api/availability/check-custom"`) {
		t.Fatal("customs batch must not loop the per-row custom endpoint")
	}
	if strings.Contains(proxyBlock, "/api/availability/check-credentials") ||
		strings.Contains(proxyBlock, "/api/availability/check-customs") ||
		strings.Contains(proxyBlock, `/api/availability/check-credential"`) ||
		strings.Contains(proxyBlock, `/api/availability/check-custom"`) {
		t.Fatal("proxy batch must not trigger other scopes")
	}
	// Per-row actions remain.
	for _, needle := range []string{
		`function probeProxy(`,
		`function probeCredential(`,
		`function probeCustom(`,
		`/api/availability/check-node`,
		`/api/availability/check-credential`,
		`/api/availability/check-custom`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("per-row contract must remain for %q", needle)
		}
	}
	// Operator copy stays concise: batch button labels carry no API paths
	// or probe payload mechanics.
	for _, label := range []string{`>批量检测</button>`, `>批量检测凭证</button>`, `>批量检测渠道</button>`} {
		idx := strings.Index(html, label)
		if idx < 0 {
			t.Fatalf("missing batch label %q", label)
		}
		start := strings.LastIndex(html[:idx], "<button")
		if start < 0 || strings.Contains(html[start:idx], "/api/") {
			t.Fatalf("batch label %q must not expose API paths", label)
		}
	}
	// DOM text nodes only.
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
		if strings.Contains(html, sink) {
			t.Fatalf("forbidden DOM sink %q", sink)
		}
	}
}
