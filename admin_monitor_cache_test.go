package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMonitorCacheControlNoStore(t *testing.T) {
	manager := &RuntimeManager{logger: nil, monitor: NewMonitor(), hub: NewLogHub(100), redactor: NewSecretRedactor()}
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	gateway, err := NewGateway(cfg, nil, manager.monitor)
	if err != nil {
		t.Fatal(err)
	}
	manager.current.Store(&gatewayRuntime{config: cfg, gateway: gateway})
	admin := NewAdminServer(manager, manager.monitor, manager.hub, nil)
	// Bypass auth by calling the handler method through a session context is
	// complex; instead verify the handler sets the header via direct call
	// using an authenticated request scaffold.
	_ = admin
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/monitor", nil)
	// handleMonitor itself does not check auth (middleware does); calling it
	// directly must still emit Cache-Control: no-store without changing body shape.
	admin.handleMonitor(rec, req)
	if got := rec.Header().Get("Cache-Control"); got != "no-store" && got != "no-cache, no-store" {
		t.Fatalf("Cache-Control=%q want no-store", got)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d want 200", rec.Code)
	}
}
