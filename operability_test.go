package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ---------- A. scheduler change snapshots ----------

func TestSchedulerChangeSnapshots(t *testing.T) {
	scheduler := newTargetScheduler(15 * time.Second)
	credID := "zen:cred-operability"
	target := targetIdentity(TierZen, credID, "shared", "direct", "m")

	first := scheduler.noteCredentialAuthFailure(credID)
	if !first.Changed || first.Failures != 1 || first.CooldownUntil <= time.Now().UnixNano() {
		t.Fatalf("first 401 must extend: %+v", first)
	}
	second := scheduler.noteCredentialAuthFailure(credID)
	if !second.Changed || second.Failures != 2 {
		t.Fatalf("escalating 401 must extend: %+v", second)
	}
	if second.CooldownUntil <= first.CooldownUntil {
		t.Fatalf("backoff must escalate: %d -> %d", first.CooldownUntil, second.CooldownUntil)
	}
	cleared := scheduler.noteCredentialSuccess(credID)
	if !cleared.Changed || !cleared.Cleared {
		t.Fatalf("success must clear stored credential state: %+v", cleared)
	}
	again := scheduler.noteCredentialSuccess(credID)
	if again.Changed || again.Cleared {
		t.Fatalf("second clear must be a no-op: %+v", again)
	}

	tChange := scheduler.noteTargetFailure(target, AttemptClassUpstreamFailure, 500, 0)
	if !tChange.Changed || tChange.Failures != 1 {
		t.Fatalf("target failure must extend: %+v", tChange)
	}
	tCleared := scheduler.noteTargetSuccess(target)
	if !tCleared.Changed || !tCleared.Cleared {
		t.Fatalf("success must clear stored target state: %+v", tCleared)
	}
	if secondClear := scheduler.noteTargetSuccess(target); secondClear.Changed || secondClear.Cleared {
		t.Fatalf("second target clear must be a no-op: %+v", secondClear)
	}
}

// Gateway-level: 400 never emits scheduler events and never changes state;
// set/clear events carry only the key suffix and the redacted node.
func TestGatewaySchedulerEventLogging(t *testing.T) {
	var buf bytes.Buffer
	level := new(slog.LevelVar)
	level.Set(slog.LevelDebug)
	hub := NewLogHub(100)
	redactor := NewSecretRedactor()
	logger := newStructuredLogger(&buf, level, hub, redactor)

	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Keys = []string{"sk-live-secret-abcdef-12345"}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	redactor.Replace(normalized)
	gateway, err := NewGateway(normalized, logger, NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	pool := gateway.pools["shared"]
	cred := gateway.authCreds[0]
	cand := authCand(TierZen, cred, pool, pool.items[0], "event-model")
	fullKey := "sk-live-secret-abcdef-12345"

	countEvents := func() map[string]int {
		counts := map[string]int{}
		for _, line := range strings.Split(buf.String(), "\n") {
			if !strings.Contains(line, `"event"`) {
				continue
			}
			for _, event := range []string{
				"credential_cooldown_set", "credential_cooldown_cleared",
				"target_cooldown_set", "target_cooldown_cleared",
			} {
				if strings.Contains(line, `"`+event+`"`) {
					counts[event]++
				}
			}
		}
		return counts
	}

	// 401 extends the credential cooldown.
	gateway.applyAttemptOutcome(context.Background(), cand, responseWithStatus(401), nil, time.Now().UnixNano())
	if got := countEvents()["credential_cooldown_set"]; got != 1 {
		t.Fatalf("credential_cooldown_set events=%d want 1\n%s", got, buf.String())
	}
	// 400 is neutral: no event, no state change.
	gateway.scheduler.noteTargetFailure(cand.Identity, AttemptClassUpstreamFailure, 500, 0)
	before := countEvents()
	cooled := gateway.scheduler.targetCoolUntil(cand.Identity)
	gateway.applyAttemptOutcome(context.Background(), cand, responseWithStatus(400), nil, time.Now().UnixNano())
	after := countEvents()
	for event, count := range before {
		if after[event] != count {
			t.Fatalf("400 emitted %s: %d -> %d", event, count, after[event])
		}
	}
	if got := gateway.scheduler.targetCoolUntil(cand.Identity); got != cooled {
		t.Fatalf("400 changed target cooldown")
	}
	// 2xx clears both layers that are actually stored.
	gateway.applyAttemptOutcome(context.Background(), cand, responseWithStatus(200), nil, time.Now().UnixNano())
	final := countEvents()
	if final["target_cooldown_cleared"] != 1 || final["credential_cooldown_cleared"] != 1 {
		t.Fatalf("clear events=%v want one target + one credential clear\n%s", final, buf.String())
	}
	// A second 2xx with nothing stored emits nothing new.
	gateway.applyAttemptOutcome(context.Background(), cand, responseWithStatus(200), nil, time.Now().UnixNano())
	steady := countEvents()
	for event, count := range final {
		if steady[event] != count {
			t.Fatalf("duplicate clear emitted %s: %d -> %d", event, count, steady[event])
		}
	}
	// Redaction: the full key and any fingerprint material must never appear;
	// the suffix display is the only key identity in the output.
	for _, line := range strings.Split(buf.String(), "\n") {
		if strings.Contains(line, fullKey) {
			t.Fatalf("full key leaked in scheduler event: %s", line)
		}
	}
	if !strings.Contains(buf.String(), `"key_id":"12345"`) {
		t.Fatalf("key suffix missing from scheduler events:\n%s", buf.String())
	}
}

func TestMigrationSummaryCounts(t *testing.T) {
	oldGateway := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081"})
	pool := oldGateway.pools["shared"]
	cred := oldGateway.authCreds[0]
	cand := authCand(TierZen, cred, pool, pool.items[0], "m")
	oldGateway.applyAttemptOutcome(context.Background(), cand, responseWithStatus(401), nil, time.Now().UnixNano())
	oldGateway.scheduler.noteTargetFailure(
		targetIdentity(TierZen, cred.id, "shared", pool.items[1].name, "m"),
		AttemptClassUpstreamFailure, 500, 0)
	oldGateway.pools["shared"].items[1].healthy.Store(false)

	newGateway := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081"})
	summary := migrateGatewaySchedulerState(oldGateway, newGateway)
	if summary.Credentials != 1 || summary.Targets != 1 || summary.Proxies != 2 {
		t.Fatalf("migration summary=%+v want {1 1 2}", summary)
	}
}

// ---------- B. POST /api/proxies/probe ----------

func operabilityAdmin(t *testing.T, proxies []string) (*RuntimeManager, *AdminServer, string, string) {
	t.Helper()
	manager := &RuntimeManager{monitor: NewMonitor(), hub: NewLogHub(100), redactor: NewSecretRedactor()}
	cfg := testGatewayConfig(map[string][]string{"shared": proxies}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	gateway, err := NewGateway(cfg, nil, manager.monitor)
	if err != nil {
		t.Fatal(err)
	}
	manager.current.Store(&gatewayRuntime{config: cfg, gateway: gateway})
	manager.metadata = newModelMetadataStore("", nil)
	manager.redactor.Replace(cfg)
	admin := NewAdminServer(manager, manager.monitor, manager.hub, nil)
	token, csrf := "operability-token", "operability-csrf"
	current := manager.Config()
	admin.sessions[tokenDigest(token)] = adminSession{
		Username: current.WebUI.Username, AuthVersion: secretFingerprint(current.WebUI.PasswordHash),
		CSRF: csrf, Expires: time.Now().Add(time.Hour),
	}
	return manager, admin, token, csrf
}

func operabilityRequest(method, path, body, token, csrf string) *http.Request {
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if token != "" {
		req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	}
	if csrf != "" {
		req.Header.Set("X-CSRF-Token", csrf)
	}
	return req
}

func serveAdmin(admin *AdminServer, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	admin.Handler().ServeHTTP(rec, req)
	return rec
}

func TestProxyProbeAuthAndValidation(t *testing.T) {
	_, admin, token, csrf := operabilityAdmin(t, []string{"direct"})

	// No session.
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/proxies/probe", `{"pool":"shared","index":0}`, "", "")); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated code=%d want 401", rec.Code)
	}
	// Bad CSRF.
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/proxies/probe", `{"pool":"shared","index":0}`, token, "wrong")); rec.Code != http.StatusForbidden {
		t.Fatalf("bad csrf code=%d want 403", rec.Code)
	}
	// Origin mismatch.
	req := operabilityRequest(http.MethodPost, "/api/proxies/probe", `{"pool":"shared","index":0}`, token, csrf)
	req.Host = "admin.local"
	req.Header.Set("Origin", "http://evil.example")
	if rec := serveAdmin(admin, req); rec.Code != http.StatusForbidden {
		t.Fatalf("origin mismatch code=%d want 403", rec.Code)
	}
	// Unknown pool.
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/proxies/probe", `{"pool":"nope","index":0}`, token, csrf)); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown pool code=%d want 400", rec.Code)
	}
	// Out-of-range index.
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/proxies/probe", `{"pool":"shared","index":7}`, token, csrf)); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad index code=%d want 400", rec.Code)
	}
	// Missing index.
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/proxies/probe", `{"pool":"shared"}`, token, csrf)); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing index code=%d want 400", rec.Code)
	}
	// Negative index.
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/proxies/probe", `{"pool":"shared","index":-1}`, token, csrf)); rec.Code != http.StatusBadRequest {
		t.Fatalf("negative index code=%d want 400", rec.Code)
	}
	// Strict JSON: raw URLs are never accepted.
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/proxies/probe", `{"pool":"shared","index":0,"url":"http://127.0.0.1:9"}`, token, csrf)); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown field code=%d want 400", rec.Code)
	}
	// no-store on a rejected call too.
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/proxies/probe", `{"pool":"nope","index":0}`, token, csrf))
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control=%q want no-store", got)
	}
}

func TestProxyProbeBusy(t *testing.T) {
	manager, admin, token, csrf := operabilityAdmin(t, []string{"direct"})
	runtime := manager.current.Load()
	proxy := runtime.gateway.pools["shared"].items[0]
	proxy.checking.Store(true)
	defer proxy.checking.Store(false)
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/proxies/probe", `{"pool":"shared","index":0}`, token, csrf))
	if rec.Code != http.StatusConflict {
		t.Fatalf("busy code=%d want 409", rec.Code)
	}
}

func TestProxyProbeSuccessFlipsHealthWithoutSchedulerPollution(t *testing.T) {
	manager, admin, token, csrf := operabilityAdmin(t, []string{"direct"})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("flavor=none"))
	}))
	defer server.Close()
	previousTarget := proxyProbeTarget
	proxyProbeTarget = server.URL
	defer func() { proxyProbeTarget = previousTarget }()

	runtime := manager.current.Load()
	gateway := runtime.gateway
	proxy := gateway.pools["shared"].items[0]
	proxy.healthy.Store(false)
	// Seed foreground scheduler state that the probe must not touch.
	cred := gateway.authCreds[0]
	pool := gateway.pools["shared"]
	cand := authCand(TierZen, cred, pool, proxy, "probe-model")
	gateway.scheduler.noteTargetFailure(cand.Identity, AttemptClassUpstreamFailure, 500, 0)
	targetUntil := gateway.scheduler.targetCoolUntil(cand.Identity)
	gateway.scheduler.noteCredentialAuthFailure(cred.id)
	credUntil := gateway.scheduler.credentialCoolUntil(cred.id)

	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/proxies/probe", `{"pool":"shared","index":0}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("probe code=%d body=%s want 200", rec.Code, rec.Body.String())
	}
	var response proxyProbeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Pool != "shared" || response.Index != 0 || response.Result != "healthy" {
		t.Fatalf("probe response=%+v", response)
	}
	if !response.Healthy || response.PreviousHealthy || !response.Changed {
		t.Fatalf("health transition wrong: %+v", response)
	}
	if response.Checking {
		t.Fatalf("checking must be released after the probe: %+v", response)
	}
	if response.ProxyNode == "" || response.ProxyNode == "direct" && false {
		t.Fatalf("proxy_node missing: %+v", response)
	}
	if !proxy.healthy.Load() {
		t.Fatalf("probe must flip transport health")
	}
	if got := gateway.scheduler.targetCoolUntil(cand.Identity); got != targetUntil {
		t.Fatalf("probe polluted target state")
	}
	if got := gateway.scheduler.credentialCoolUntil(cred.id); got != credUntil {
		t.Fatalf("probe polluted credential state")
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control=%q want no-store", got)
	}
	// Raw proxy material must never appear in the response body.
	if strings.Contains(rec.Body.String(), "hunter2") {
		t.Fatalf("proxy secret leaked in probe response")
	}
}

func TestProxyProbeRedactsSecretProxy(t *testing.T) {
	secretProxy := "http://user:hunter2@127.0.0.1:9"
	manager, admin, token, csrf := operabilityAdmin(t, []string{secretProxy})
	runtime := manager.current.Load()
	proxy := runtime.gateway.pools["shared"].items[0]
	proxy.healthy.Store(true)
	// 127.0.0.1:9 refuses connections, so the probe fails closed quickly.
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/proxies/probe", `{"pool":"shared","index":0}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("probe code=%d want 200 with embedded result", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "hunter2") || strings.Contains(body, "user@") {
		t.Fatalf("proxy secret leaked in probe response: %s", body)
	}
	var response proxyProbeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.ProxyNode == "" || strings.Contains(response.ProxyNode, "hunter2") || strings.Contains(response.ProxyNode, "user@") {
		t.Fatalf("proxy_node not redacted: %+v", response)
	}
	if response.Result != "unhealthy" && response.Result != "inconclusive" {
		t.Fatalf("refused proxy must not report healthy: %+v", response)
	}
}

func TestProxyProbeRateLimit(t *testing.T) {
	_, admin, token, csrf := operabilityAdmin(t, []string{"direct"})
	var last *httptest.ResponseRecorder
	for i := 0; i < 11; i++ {
		last = serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/proxies/probe", `{"pool":"nope","index":0}`, token, csrf))
	}
	if last.Code != http.StatusTooManyRequests {
		t.Fatalf("11th probe code=%d want 429", last.Code)
	}
}

// ---------- C. POST /api/models/refresh ----------

func operabilityRefreshServers(t *testing.T, modelsBody, capabilityBody string, modelsStatus, capabilityStatus int) (baseURL, capabilityURL, docsURL string) {
	t.Helper()
	mux := http.NewServeMux()
	// fetchModels appends /v1/models to the configured upstream base, so a
	// single handler serves both the Zen and Go passes.
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(modelsStatus)
		_, _ = w.Write([]byte(modelsBody))
	})
	mux.HandleFunc("/capabilities", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(capabilityStatus)
		_, _ = w.Write([]byte(capabilityBody))
	})
	mux.HandleFunc("/docs", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("no docs"))
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server.URL, server.URL + "/capabilities", server.URL + "/docs"
}

func pointRefreshAt(t *testing.T, manager *RuntimeManager, zenURL, goURL, capabilityURL, docsURL, metadataURL string) {
	t.Helper()
	runtime := manager.current.Load()
	runtime.gateway.cfg.Upstream.Zen = zenURL
	runtime.gateway.cfg.Upstream.Go = goURL
	previousCaps, previousZenDocs, previousGoDocs := openCodeCapabilitiesEndpoint, openCodeZenDocsEndpoint, openCodeGoDocsEndpoint
	openCodeCapabilitiesEndpoint, openCodeZenDocsEndpoint, openCodeGoDocsEndpoint = capabilityURL, docsURL, docsURL
	t.Cleanup(func() {
		openCodeCapabilitiesEndpoint, openCodeZenDocsEndpoint, openCodeGoDocsEndpoint = previousCaps, previousZenDocs, previousGoDocs
	})
	manager.metadata.endpoint = metadataURL
}

func TestModelsRefreshValidation(t *testing.T) {
	_, admin, token, csrf := operabilityAdmin(t, []string{"direct"})
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/models/refresh", `{"scope":"catalog"}`, "", "")); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated code=%d want 401", rec.Code)
	}
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/models/refresh", `{"scope":"catalog"}`, token, "wrong")); rec.Code != http.StatusForbidden {
		t.Fatalf("bad csrf code=%d want 403", rec.Code)
	}
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/models/refresh", `{"scope":"everything"}`, token, csrf)); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown scope code=%d want 400", rec.Code)
	}
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/models/refresh", `{"scope":"catalog","extra":1}`, token, csrf)); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown field code=%d want 400", rec.Code)
	}
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/models/refresh", `{"scope":"everything"}`, token, csrf))
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control=%q want no-store", got)
	}
}

func TestModelsRefreshBusy(t *testing.T) {
	manager, admin, token, csrf := operabilityAdmin(t, []string{"direct"})
	runtime := manager.current.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// A busy catalog gate reports busy instead of stacking.
	runtime.gateway.catalogRefreshMu.Lock()
	if _, busy, _ := manager.refreshModels(ctx, "catalog"); busy != "catalog" {
		runtime.gateway.catalogRefreshMu.Unlock()
		t.Fatalf("catalog busy=%q want catalog", busy)
	}
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/models/refresh", `{"scope":"catalog"}`, token, csrf))
	if rec.Code != http.StatusConflict {
		runtime.gateway.catalogRefreshMu.Unlock()
		t.Fatalf("busy catalog code=%d want 409", rec.Code)
	}
	runtime.gateway.catalogRefreshMu.Unlock()
	// A busy metadata gate reports busy.
	manager.metadata.refreshMu.Lock()
	if _, busy, _ := manager.refreshModels(ctx, "metadata"); busy != "metadata" {
		manager.metadata.refreshMu.Unlock()
		t.Fatalf("metadata busy=%q want metadata", busy)
	}
	// A scope that includes a busy component reports busy without stacking,
	// and the uncontended gate is released instead of leaked.
	if _, busy, _ := manager.refreshModels(ctx, "all"); busy != "metadata" {
		manager.metadata.refreshMu.Unlock()
		t.Fatalf("all with busy metadata: busy=%q want metadata", busy)
	}
	manager.metadata.refreshMu.Unlock()
	if !runtime.gateway.catalogRefreshMu.TryLock() {
		t.Fatalf("catalog gate leaked after refused refresh")
	}
	runtime.gateway.catalogRefreshMu.Unlock()
}

func TestModelsRefreshFailureRetainsSnapshotAndState(t *testing.T) {
	manager, admin, token, csrf := operabilityAdmin(t, []string{"direct"})
	baseURL, capabilityURL, docsURL := operabilityRefreshServers(t, `{"error":"down"}`, `{"error":"down"}`,
		http.StatusInternalServerError, http.StatusInternalServerError)
	pointRefreshAt(t, manager, baseURL, baseURL, capabilityURL, docsURL, capabilityURL)

	runtime := manager.current.Load()
	gateway := runtime.gateway
	gateway.catalog.ReplaceWithCapabilities([]string{"old-model"}, nil,
		map[Tier]map[string]Protocol{TierZen: {"old-model": ProtocolChat}}, nil, nil)
	seeded := seedRefreshForeground(t, gateway)
	before := snapshotRefreshState(gateway)

	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/models/refresh", `{"scope":"all"}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("refresh code=%d want 200 with embedded errors", rec.Code)
	}
	var response modelsRefreshResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Catalog.Refreshed || response.Metadata.Refreshed {
		t.Fatalf("failed refresh must not report refreshed: %+v", response)
	}
	if response.Catalog.Error == "" || response.Metadata.Error == "" {
		t.Fatalf("failed refresh must carry component errors: %+v", response)
	}
	snap := gateway.catalog.Snapshot()
	if snap.Zen != 1 {
		t.Fatalf("failed refresh must retain catalog: %+v", snap)
	}
	time.Sleep(50 * time.Millisecond)
	after := snapshotRefreshState(gateway)
	assertRefreshStateUnchanged(t, before, after)
	if got := gateway.scheduler.targetCoolUntil(seeded.Identity); got <= time.Now().UnixNano() {
		t.Fatalf("seeded target cooldown lost")
	}
	if _, _, ok := gateway.scheduler.proxy429CooldownStatus(TierZen, seeded.PoolName, seeded.ProxyRaw); !ok {
		t.Fatalf("seeded proxy429 cooldown lost")
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control=%q want no-store", got)
	}
}

func TestModelsRefreshSuccessUpdatesCatalogWithoutStatePollution(t *testing.T) {
	manager, admin, token, csrf := operabilityAdmin(t, []string{"direct"})
	modelsBody := `{"data":[{"id":"manual-model-1"}]}`
	capabilityBody := `{"opencode-zen":{"api":"https://opencode.ai/zen","npm":"openai-compatible","models":{"manual-model-1":{"id":"manual-model-1"}}}}`
	baseURL, capabilityURL, docsURL := operabilityRefreshServers(t, modelsBody, capabilityBody, http.StatusOK, http.StatusOK)
	pointRefreshAt(t, manager, baseURL, baseURL, capabilityURL, docsURL, capabilityURL)
	// The metadata endpoint needs models.dev shape, not the capability shape.
	metadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"opencode":{"models":{"manual-model-1":{"id":"manual-model-1","cost":{"input":0,"output":0}}}}}`))
	}))
	defer metadataServer.Close()
	manager.metadata.endpoint = metadataServer.URL

	runtime := manager.current.Load()
	gateway := runtime.gateway
	seeded := seedRefreshForeground(t, gateway)
	before := snapshotRefreshState(gateway)

	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/models/refresh", `{"scope":"all"}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("refresh code=%d body=%s want 200", rec.Code, rec.Body.String())
	}
	var response modelsRefreshResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !response.Catalog.Refreshed || !response.Metadata.Refreshed {
		t.Fatalf("successful refresh must report refreshed: %+v", response)
	}
	if response.CatalogSnapshot.Zen != 1 || response.MetadataSnapshot.Models != 1 {
		t.Fatalf("snapshots missing updates: %+v", response)
	}
	if strings.Contains(rec.Body.String(), "zen-key-12345") || strings.Contains(rec.Body.String(), "go-key-12345") {
		t.Fatalf("secret leaked in refresh response")
	}
	// No full model list is returned.
	if strings.Contains(rec.Body.String(), `"manual-model-1"`) && strings.Contains(rec.Body.String(), `"data"`) {
		t.Fatalf("refresh response must not return the full model list: %s", rec.Body.String())
	}
	time.Sleep(50 * time.Millisecond)
	after := snapshotRefreshState(gateway)
	assertRefreshStateUnchanged(t, before, after)
	if got := gateway.scheduler.targetCoolUntil(seeded.Identity); got <= time.Now().UnixNano() {
		t.Fatalf("seeded target cooldown lost")
	}
	if _, _, ok := gateway.scheduler.proxy429CooldownStatus(TierZen, seeded.PoolName, seeded.ProxyRaw); !ok {
		t.Fatalf("seeded proxy429 cooldown lost")
	}
}

func TestModelsRefreshRateLimit(t *testing.T) {
	manager, admin, token, csrf := operabilityAdmin(t, []string{"direct"})
	baseURL, capabilityURL, docsURL := operabilityRefreshServers(t, `{"error":"x"}`, `{"error":"x"}`,
		http.StatusInternalServerError, http.StatusInternalServerError)
	pointRefreshAt(t, manager, baseURL, baseURL, capabilityURL, docsURL, capabilityURL)
	var last *httptest.ResponseRecorder
	for i := 0; i < 4; i++ {
		last = serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/models/refresh", `{"scope":"catalog"}`, token, csrf))
	}
	if last.Code != http.StatusTooManyRequests {
		t.Fatalf("4th refresh code=%d want 429", last.Code)
	}
}
