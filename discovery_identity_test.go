package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

type discoveryCaptured struct {
	ua, client, auth                  string
	session, request, project, parent string
	affinity, sid                     string
	apiKey                            string
	anthropicVersion                  string
}

func captureDiscoveryHeaders(r *http.Request) discoveryCaptured {
	return discoveryCaptured{
		ua:               r.Header.Get("User-Agent"),
		client:           r.Header.Get("x-opencode-client"),
		auth:             r.Header.Get("Authorization"),
		session:          r.Header.Get("x-opencode-session"),
		request:          r.Header.Get("x-opencode-request"),
		project:          r.Header.Get("x-opencode-project"),
		parent:           r.Header.Get("x-parent-session-id"),
		affinity:         r.Header.Get("x-session-affinity"),
		sid:              r.Header.Get("X-Session-Id"),
		apiKey:           r.Header.Get("x-api-key"),
		anthropicVersion: r.Header.Get("anthropic-version"),
	}
}

func assertDiscoveryCanonical(t *testing.T, got discoveryCaptured, wantAuth string) {
	t.Helper()
	if got.ua != opencodeWireUserAgent() {
		t.Fatalf("discovery UA=%q want bare %q", got.ua, opencodeWireUserAgent())
	}
	if got.ua != "opencode/1.18.31" {
		t.Fatalf("discovery UA=%q want canonical bare UA", got.ua)
	}
	if got.client != "cli" {
		t.Fatalf("discovery x-opencode-client=%q want cli", got.client)
	}
	if got.auth != wantAuth {
		t.Fatalf("discovery Authorization=%q want %q", got.auth, wantAuth)
	}
	for name, v := range map[string]string{
		"x-opencode-session":  got.session,
		"x-opencode-request":  got.request,
		"x-opencode-project":  got.project,
		"x-parent-session-id": got.parent,
	} {
		if v != "" {
			t.Fatalf("sessionless discovery must omit %s, got %q", name, v)
		}
	}
	if got.affinity != "" || got.sid != "" {
		t.Fatalf("discovery must omit generic affinity: %q %q", got.affinity, got.sid)
	}
	if got.apiKey != "" {
		t.Fatalf("OpenAI-family discovery must not send x-api-key, got %q", got.apiKey)
	}
	if got.anthropicVersion != "" {
		t.Fatalf("discovery must not send anthropic-version, got %q", got.anthropicVersion)
	}
}

func TestZenDiscoveryCanonicalIdentity(t *testing.T) {
	var got discoveryCaptured
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = captureDiscoveryHeaders(r)
		path = r.URL.Path
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[{"id":"zen-model-a"}]}`))
	}))
	defer srv.Close()
	models, status, err := fetchModels(context.Background(), srv.Client(), srv.URL, "zen-key-123")
	if err != nil {
		t.Fatal(err)
	}
	if status != 200 || len(models) != 1 || models[0] != "zen-model-a" {
		t.Fatalf("models=%v status=%d", models, status)
	}
	if path != "/v1/models" {
		t.Fatalf("path=%q want /v1/models", path)
	}
	assertDiscoveryCanonical(t, got, "Bearer zen-key-123")
}

func TestFallbackDiscoveryCanonicalIdentity(t *testing.T) {
	var got discoveryCaptured
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = captureDiscoveryHeaders(r)
		path = r.URL.Path
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[{"id":"custom-model-a"}]}`))
	}))
	defer srv.Close()
	models, err := fetchFallbackModels(context.Background(), srv.Client(), srv.URL, "custom-key-456")
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 1 || models[0] != "custom-model-a" {
		t.Fatalf("models=%v", models)
	}
	if path != "/v1/models" {
		t.Fatalf("path=%q want /v1/models", path)
	}
	assertDiscoveryCanonical(t, got, "Bearer custom-key-456")
}

func TestDiscoveryConstructorsShareAuthority(t *testing.T) {
	// Both thin constructors must produce the same sessionless canonical
	// identity as the shared core for the same endpoint/key.
	ctx := context.Background()
	zenReq, err := newZenDiscoveryRequest(ctx, "https://zen.example/", "k1")
	if err != nil {
		t.Fatal(err)
	}
	fbReq, err := newFallbackDiscoveryRequest(ctx, "https://custom.example/v1", "k1")
	if err != nil {
		t.Fatal(err)
	}
	coreReq, err := newOpenCodeDiscoveryRequest(ctx, "https://zen.example/v1/models", "k1")
	if err != nil {
		t.Fatal(err)
	}
	for name, req := range map[string]*http.Request{"zen": zenReq, "fallback": fbReq, "core": coreReq} {
		got := captureDiscoveryHeaders(req)
		assertDiscoveryCanonical(t, got, "Bearer k1")
		if req.Header.Get("Accept") != "application/json" {
			t.Fatalf("%s Accept=%q want application/json", name, req.Header.Get("Accept"))
		}
	}
	if zenReq.URL.String() != "https://zen.example/v1/models" {
		t.Fatalf("zen endpoint=%q", zenReq.URL.String())
	}
	if fbReq.URL.String() != "https://custom.example/v1/models" {
		t.Fatalf("fallback endpoint=%q", fbReq.URL.String())
	}
	if zenReq.Header.Get("User-Agent") != coreReq.Header.Get("User-Agent") ||
		zenReq.Header.Get("x-opencode-client") != coreReq.Header.Get("x-opencode-client") {
		t.Fatalf("thin constructors must share the core public identity")
	}
}

// TestDiscoveryCallSitesUseSharedAuthority is a static source contract: the
// two discovery call sites must build through the centralized authority and
// must never hand-write managed headers themselves. Managed-header
// generation lives in ids.go only.
func TestDiscoveryCallSitesUseSharedAuthority(t *testing.T) {
	modelsSrc, err := os.ReadFile("models.go")
	if err != nil {
		t.Fatal(err)
	}
	fallbackSrc, err := os.ReadFile("fallback.go")
	if err != nil {
		t.Fatal(err)
	}
	fetchModelsBody := funcBody(string(modelsSrc), "func fetchModels(")
	if fetchModelsBody == "" {
		t.Fatal("func fetchModels not found")
	}
	fetchFallbackBody := funcBody(string(fallbackSrc), "func fetchFallbackModels(")
	if fetchFallbackBody == "" {
		t.Fatal("func fetchFallbackModels not found")
	}
	managed := []string{
		"User-Agent", "x-opencode-client", "x-opencode-session",
		"x-opencode-request", "x-opencode-project", "x-parent-session-id",
		"Authorization", "x-api-key", "anthropic-version",
	}
	for _, header := range managed {
		for _, op := range []string{`.Set("` + header + `"`, `.Add("` + header + `"`, `["` + header + `"]`} {
			if strings.Contains(fetchModelsBody, op) {
				t.Fatalf("fetchModels must not directly write managed header %q via %q", header, op)
			}
			if strings.Contains(fetchFallbackBody, op) {
				t.Fatalf("fetchFallbackModels must not directly write managed header %q via %q", header, op)
			}
		}
	}
	if !strings.Contains(fetchModelsBody, "newZenDiscoveryRequest(") &&
		!strings.Contains(fetchModelsBody, "newOpenCodeDiscoveryRequest(") {
		t.Fatalf("fetchModels must build through the shared discovery authority")
	}
	if !strings.Contains(fetchFallbackBody, "newFallbackDiscoveryRequest(") &&
		!strings.Contains(fetchFallbackBody, "newOpenCodeDiscoveryRequest(") {
		t.Fatalf("fetchFallbackModels must build through the shared discovery authority")
	}
}

// funcBody extracts the source text of the named top-level func through the
// next top-level "\nfunc " boundary. It follows the existing WebUI contract
// test style (raw file needle search) rather than introducing an AST pass.
func funcBody(src, prefix string) string {
	start := strings.Index(src, prefix)
	if start < 0 {
		return ""
	}
	rest := src[start+len(prefix):]
	next := strings.Index(rest, "\nfunc ")
	if next < 0 {
		return src[start:]
	}
	return src[start : start+len(prefix)+next]
}
