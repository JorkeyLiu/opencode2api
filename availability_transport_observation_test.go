package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Fresh resources must not present the internal healthy default as a real
// probe result: transport observation stays empty with no timestamp, and
// credential probe fields stay empty.
func TestTransportObservationUndetectedIsEmpty(t *testing.T) {
	manager, _, _, _ := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	snap := manager.Resources()
	if len(snap.Proxies) != 1 {
		t.Fatalf("proxies=%d want 1", len(snap.Proxies))
	}
	p := snap.Proxies[0]
	if !p.Healthy {
		t.Fatalf("fresh proxy healthy must stay true (routing default)")
	}
	if p.Transport != "" {
		t.Fatalf("undetected transport=%q want empty dash", p.Transport)
	}
	if p.Reason != "" {
		t.Fatalf("undetected proxy reason=%q want empty", p.Reason)
	}
	if p.LastChecked != nil {
		t.Fatalf("undetected proxy last_checked must be nil")
	}
	if len(snap.Keys) != 1 {
		t.Fatalf("keys=%d want 1", len(snap.Keys))
	}
	k := snap.Keys[0]
	if k.ProbeStatus != "" || k.ProbeReason != "" || k.ProbeHTTPStatus != 0 {
		t.Fatalf("undetected credential probe must stay empty: %+v", k)
	}
	if k.LastChecked != nil {
		t.Fatalf("undetected credential last_checked must be nil")
	}
}

// Transport observation decouples from internal healthy: flipping healthy
// must not change the last probe transport, and a real probe updates the
// observation for both bulk and single checks without changing healthy
// routing semantics beyond the existing applyBulkWrites rules.
func TestTransportObservationDecoupledFromHealthy(t *testing.T) {
	manager, _, _, _ := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	gw := manager.current.Load().gateway
	seedBulkProbeCatalog(gw)
	var hits atomic.Int32
	pool := gw.pools["shared"]
	pool.items[0].client.Transport = &stubTransport{hits: &hits, status: 200, body: bulkChatSuccessBody("bulk-free-model")}
	// Single-node probe records a real transport observation.
	scoped := gw.runScopedCheck(context.Background(), "shared", 0)
	if len(scoped.Nodes) != 1 {
		t.Fatalf("scoped nodes=%d want 1", len(scoped.Nodes))
	}
	if scoped.Nodes[0].Transport != "healthy" {
		t.Fatalf("scoped transport=%q want healthy", scoped.Nodes[0].Transport)
	}
	snap := manager.Resources()
	if snap.Proxies[0].Transport != "healthy" {
		t.Fatalf("resources transport=%q want healthy", snap.Proxies[0].Transport)
	}
	if snap.Proxies[0].LastChecked == nil {
		t.Fatal("resources last_checked must be set after probe")
	}
	// Flip the internal routing signal: the display observation stays.
	pool.items[0].healthy.Store(false)
	snap2 := manager.Resources()
	if snap2.Proxies[0].Healthy {
		t.Fatal("healthy flip must be visible in healthy field")
	}
	if snap2.Proxies[0].Transport != "healthy" {
		t.Fatalf("transport must stay healthy after healthy flip, got %q", snap2.Proxies[0].Transport)
	}
	// Bulk probe also refreshes the same observation (back to healthy even
	// though internal healthy is false; bulk HTTP success restores healthy
	// via the existing transport-health rule, which is preserved).
	pool.items[0].healthy.Store(true)
	bulk := gw.runBulkCheck(context.Background())
	if len(bulk.Nodes) == 0 {
		t.Fatal("bulk must return node rows")
	}
	found := false
	for _, n := range bulk.Nodes {
		if n.Pool == "shared" && n.Transport == "healthy" {
			found = true
		}
	}
	if !found {
		t.Fatalf("bulk must record healthy transport: %+v", bulk.Nodes)
	}
	snap3 := manager.Resources()
	if snap3.Proxies[0].Transport != "healthy" {
		t.Fatalf("bulk resources transport=%q want healthy", snap3.Proxies[0].Transport)
	}
}

// Proxy rows preserve bulk node reason; credential rows carry HTTP status +
// reason through bulk and per-row paths into the resources projection.
func TestAvailabilityReasonAndHTTPStatusPassthrough(t *testing.T) {
	manager, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	gw := manager.current.Load().gateway
	seedBulkProbeCatalog(gw)
	var hits atomic.Int32
	pool := gw.pools["shared"]
	pool.items[0].client.Transport = &stubTransport{hits: &hits, status: 429, body: "limited"}
	// Single-node probe updates the proxy row: HTTP 429 proves transport
	// reachable (transport healthy) while the lane keeps the real 429 and
	// the node reason is preserved instead of dropped.
	scopedRec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-node", `{"pool":"shared","index":0}`, token, csrf))
	if scopedRec.Code != http.StatusOK {
		t.Fatalf("scoped code=%d body=%s", scopedRec.Code, scopedRec.Body.String())
	}
	snap := manager.Resources()
	if len(snap.Proxies) != 1 {
		t.Fatalf("proxies=%d want 1", len(snap.Proxies))
	}
	p := snap.Proxies[0]
	if p.Transport != "healthy" {
		t.Fatalf("proxy transport=%q want healthy (HTTP proves reachable)", p.Transport)
	}
	if p.AuthenticatedHTTPStatus != 429 && p.AnonymousHTTPStatus != 429 {
		t.Fatalf("proxy lane http must carry 429: anon=%d auth=%d", p.AnonymousHTTPStatus, p.AuthenticatedHTTPStatus)
	}
	if p.Reason == "" {
		t.Fatal("proxy reason must not be dropped")
	}
	if p.LastChecked == nil {
		t.Fatal("proxy last_checked must be set after probe")
	}
	// Per-row credential probe observes a real 429 with HTTP passthrough.
	fp := credentialFingerprint(TierZen, gw.authCreds[0].key)
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-credential", `{"fingerprint":"`+fp+`"}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("credential code=%d body=%s", rec.Code, rec.Body.String())
	}
	snap2 := manager.Resources()
	if len(snap2.Keys) != 1 {
		t.Fatalf("keys=%d want 1", len(snap2.Keys))
	}
	k := snap2.Keys[0]
	if k.ProbeHTTPStatus != 429 {
		t.Fatalf("credential probe http=%d want 429", k.ProbeHTTPStatus)
	}
	if k.ProbeReason != "rate_limited" {
		t.Fatalf("credential probe reason=%q want rate_limited", k.ProbeReason)
	}
	if k.LastChecked == nil {
		t.Fatal("credential last_checked must be set after probe")
	}
	_ = time.Now
}

// Six availability POST entries share no per-client rate limit: rapid repeats
// must never return 429. Single-flight 409, timeouts, and send caps remain.
func TestAvailabilityNoPerClientRateLimitAllEntries(t *testing.T) {
	manager, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"},
		[]string{"zen-key-12345"}, nil)
	gw := manager.current.Load().gateway
	seedBulkProbeCatalog(gw)
	var hits atomic.Int32
	gw.pools["shared"].items[0].client.Transport = &stubTransport{hits: &hits, status: 200, body: bulkChatSuccessBody("bulk-free-model")}
	// Custom channel backed by a fast local success server.
	customSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"hi"}}]}`))
	}))
	defer customSrv.Close()
	gw.cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{{ID: "c1", Name: "c1", BaseURL: customSrv.URL, APIKey: "k", Model: "cm", Protocol: ProtocolChat}}}
	fp := credentialFingerprint(TierZen, gw.authCreds[0].key)
	calls := []struct {
		name string
		body string
		path string
	}{
		{"bulk", `{}`, "/api/availability/check"},
		{"scoped", `{"pool":"shared","index":0}`, "/api/availability/check-node"},
		{"credential", `{"fingerprint":"` + fp + `"}`, "/api/availability/check-credential"},
		{"credentials", `{}`, "/api/availability/check-credentials"},
		{"custom", `{"id":"c1"}`, "/api/availability/check-custom"},
		{"customs", `{}`, "/api/availability/check-customs"},
	}
	for _, c := range calls {
		for i := 0; i < 4; i++ {
			rec := serveAdmin(admin, operabilityRequest(http.MethodPost, c.path, c.body, token, csrf))
			if rec.Code == http.StatusTooManyRequests {
				t.Fatalf("%s must not rate limit (429): body=%s", c.name, rec.Body.String())
			}
			if rec.Code != http.StatusOK {
				t.Fatalf("%s code=%d want 200 body=%s", c.name, rec.Code, rec.Body.String())
			}
		}
	}
}
