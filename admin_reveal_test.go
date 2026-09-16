package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
)

func TestConfigRevealSessionAuth(t *testing.T) {
	manager, admin, token, csrf := operabilityAdmin(t, []string{"direct"})
	admin.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	// Attach a fallback channel so the reveal payload shape is exercised.
	current := manager.Config()
	current.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{
		{Name: "c1", BaseURL: "https://api.example.com", APIKey: "fallback-secret-abcde", Model: "m1"},
	}}
	normalized, err := NormalizeConfig("config.json", current)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(normalized, nil, manager.monitor)
	if err != nil {
		t.Fatal(err)
	}
	manager.current.Store(&gatewayRuntime{config: normalized, gateway: gateway})
	manager.redactor.Replace(normalized)

	// Authenticated POST {} succeeds without any password field.
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/config/reveal", `{}`, token, csrf))
	if rec.Code != http.StatusOK {
		t.Fatalf("reveal code=%d want 200 body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control=%q want no-store", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("reveal body is not JSON: %v", err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "zen-key-12345") {
		t.Fatalf("reveal must return full zen key, body=%s", body)
	}
	if !strings.Contains(body, "fallback-secret-abcde") {
		t.Fatalf("reveal must return full fallback key, body=%s", body)
	}
	fb, ok := payload["fallback"].(map[string]any)
	if !ok {
		t.Fatalf("reveal must contain fallback object, body=%s", body)
	}
	chans, ok := fb["channels"].([]any)
	if !ok || len(chans) != 1 {
		t.Fatalf("reveal must contain one fallback channel, body=%s", body)
	}

	// Default GET stays masked: no raw secrets.
	recGet := serveAdmin(admin, operabilityRequest(http.MethodGet, "/api/config", ``, token, ""))
	if recGet.Code != http.StatusOK {
		t.Fatalf("config GET code=%d want 200", recGet.Code)
	}
	if strings.Contains(recGet.Body.String(), "zen-key-12345") || strings.Contains(recGet.Body.String(), "fallback-secret-abcde") {
		t.Fatalf("masked GET must not leak raw secrets: %s", recGet.Body.String())
	}

	// Legacy password payload is now strictly rejected (unknown field).
	recPwd := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/config/reveal", `{"password":"whatever"}`, token, csrf))
	if recPwd.Code != http.StatusBadRequest {
		t.Fatalf("password field code=%d want 400 body=%s", recPwd.Code, recPwd.Body.String())
	}

	// Unauthenticated, bad CSRF, and origin mismatch are all refused.
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/config/reveal", `{}`, "", "")); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauth code=%d want 401", rec.Code)
	}
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/config/reveal", `{}`, token, "wrong")); rec.Code != http.StatusForbidden {
		t.Fatalf("bad csrf code=%d want 403", rec.Code)
	}
	req := operabilityRequest(http.MethodPost, "/api/config/reveal", `{}`, token, csrf)
	req.Host = "admin.local"
	req.Header.Set("Origin", "http://evil.example")
	if rec := serveAdmin(admin, req); rec.Code != http.StatusForbidden {
		t.Fatalf("origin mismatch code=%d want 403", rec.Code)
	}

	// Reveal is POST-only: GET on the reveal endpoint is never a reveal.
	if rec := serveAdmin(admin, operabilityRequest(http.MethodGet, "/api/config/reveal", ``, token, "")); rec.Code == http.StatusOK {
		t.Fatalf("GET reveal must not succeed with 200")
	}
}
