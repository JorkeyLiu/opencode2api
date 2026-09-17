package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type failRoundTripper struct {
	err  error
	hits *atomic.Int32
}

func (f *failRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	if f.hits != nil {
		f.hits.Add(1)
	}
	if f.err != nil {
		return nil, f.err
	}
	return nil, errors.New("custom fallback transport failed")
}

func TestFallbackTakeoverTransportErrorNoPanicNoFallback(t *testing.T) {
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{Name: "c1", BaseURL: "https://custom.example", APIKey: "ck-secret-1", Model: "cm"}}, "c1")
	ses := "ses_takeover_transport_err"
	bindAnonPin(t, gw, ses, "m")
	var anonHits atomic.Int32
	postStub(t, gw, "a", 0, &anonHits, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	var customHits atomic.Int32
	gw.customClient = &http.Client{Transport: &failRoundTripper{err: errors.New("dial tcp: connection refused"), hits: &customHits}}
	route := anonAuthRoute()
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r-takeover-err"), 0, ex)
	// Must never be nil response + nil error (handleInference would panic on defer Body).
	if resp == nil && err == nil {
		t.Fatal("custom transport failure must not return nil response + nil error")
	}
	if err == nil {
		if resp != nil {
			drainResp(resp)
		}
		t.Fatal("custom transport failure must return an error")
	}
	if resp != nil {
		drainResp(resp)
		t.Fatal("transport error must not return a response alongside the error")
	}
	if eff.Tier != TierCustom {
		t.Fatalf("effective route must stay custom, got %+v", eff)
	}
	if anonHits.Load() != 1 {
		t.Fatalf("native must be attempted once before takeover, anon=%d", anonHits.Load())
	}
	if customHits.Load() != 1 {
		t.Fatalf("custom must be attempted once, custom=%d", customHits.Load())
	}
	// Session must already be bound to custom despite the failure (no fallback to native).
	binding, ok := gw.scheduler.fallbacks.get(ses)
	if !ok || binding.Name != "c1" {
		t.Fatalf("failed takeover must still bind session: %+v %v", binding, ok)
	}
	// Follow-up pinned request must stay on custom with no native send.
	anonBefore := anonHits.Load()
	resp2, eff2, _, err2 := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r-takeover-err-2"), 0, ex)
	if err2 == nil {
		if resp2 != nil {
			drainResp(resp2)
		}
		t.Fatal("pinned custom transport failure must still return an error")
	}
	if resp2 != nil {
		drainResp(resp2)
	}
	if eff2.Tier != TierCustom {
		t.Fatalf("follow-up must stay custom, got %+v", eff2)
	}
	if anonHits.Load() != anonBefore {
		t.Fatalf("pinned custom must not fall back to native: anon before=%d after=%d", anonBefore, anonHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatal("takeover binding must persist after transport error")
	}
}

func TestFallbackTakeoverContextCancel(t *testing.T) {
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{Name: "c1", BaseURL: "https://custom.example", APIKey: "ck-secret-1", Model: "cm"}}, "c1")
	ses := "ses_takeover_cancel"
	bindAnonPin(t, gw, ses, "m")
	var anonHits atomic.Int32
	postStub(t, gw, "a", 0, &anonHits, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	var customHits atomic.Int32
	// Live request context, but the custom send fails with a context error
	// (cancel/timeout during dial). The takeover error must propagate with
	// existing cancel semantics, never as nil,nil, and the session stays bound.
	gw.customClient = &http.Client{Transport: &failRoundTripper{err: context.Canceled, hits: &customHits}}
	route := anonAuthRoute()
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r-cancel"), 0, ex)
	if resp == nil && err == nil {
		t.Fatal("cancelled takeover must not return nil,nil")
	}
	if err == nil {
		if resp != nil {
			drainResp(resp)
		}
		t.Fatal("cancelled takeover must return context error")
	}
	if resp != nil {
		drainResp(resp)
	}
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancel must preserve context semantics, got %v", err)
	}
	if eff.Tier != TierCustom {
		t.Fatalf("cancel route must stay custom, got %+v", eff)
	}
	if customHits.Load() != 1 {
		t.Fatalf("custom must be attempted once, got %d", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatal("cancelled takeover must still leave the session bound")
	}
	// Pre-cancelled follow-up on the bound session must fail with context
	// semantics and keep the binding, without touching native.
	cancelCtx, cancel := context.WithCancel(pinTestCtx())
	cancel()
	anonBefore := anonHits.Load()
	resp2, _, _, err2 := gw.doUpstreamTiers(cancelCtx, route, routeBodies(), pinIDs(ses, "r-cancel-2"), 0, ex)
	if resp2 == nil && err2 == nil {
		t.Fatal("pre-cancelled pinned request must not return nil,nil")
	}
	if err2 == nil {
		if resp2 != nil {
			drainResp(resp2)
		}
		t.Fatal("pre-cancelled pinned request must return context error")
	}
	if resp2 != nil {
		drainResp(resp2)
	}
	if anonHits.Load() != anonBefore {
		t.Fatalf("cancelled pinned request must not touch native")
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); !ok {
		t.Fatal("binding must persist after cancelled pinned request")
	}
}

func TestBuildFallbackChatBodyRequiresContext(t *testing.T) {
	ch := FallbackChannelConfig{Name: "c1", BaseURL: "https://custom.example", APIKey: "k", Model: "cm"}
	route := anonAuthRoute()
	// Responses-shaped prepared body without client context must fail, never silently rewrite.
	responsesBodies := map[Tier][]byte{TierZen: []byte(`{"model":"m","input":"hi"}`)}
	if _, err := buildFallbackChatBody(upstreamExtra{}, false, responsesBodies, route, ch); err == nil {
		t.Fatal("responses prepared body without context must return error")
	}
	// Anthropic-shaped prepared body without client context must fail.
	anthropicBodies := map[Tier][]byte{TierZen: []byte(`{"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}],"max_tokens":8}`)}
	if _, err := buildFallbackChatBody(upstreamExtra{}, false, anthropicBodies, route, ch); err == nil {
		t.Fatal("anthropic prepared body without context must return error")
	}
	// Even a chat-looking prepared body without context must fail: source cannot be reliably attributed.
	chatBodies := map[Tier][]byte{TierZen: []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)}
	if _, err := buildFallbackChatBody(upstreamExtra{}, false, chatBodies, route, ch); err == nil {
		t.Fatal("prepared body without client context must return error even when chat-looking")
	}
	// Nil payload with hasEx must also fail.
	if _, err := buildFallbackChatBody(upstreamExtra{External: ProtocolChat}, true, chatBodies, route, ch); err == nil {
		t.Fatal("nil client payload must return error")
	}
	// Normal three-protocol path with client context still works.
	for _, tc := range []struct {
		external Protocol
		payload  map[string]any
	}{
		{ProtocolChat, map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}},
		{ProtocolResponses, map[string]any{"model": "m", "input": "hi"}},
		{ProtocolAnthropic, map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "max_tokens": 8}},
	} {
		body, err := buildFallbackChatBody(upstreamExtra{External: tc.external, Payload: tc.payload}, true, nil, route, ch)
		if err != nil {
			t.Fatalf("%s with context must succeed: %v", tc.external, err)
		}
		var decoded map[string]any
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded["model"] != "cm" {
			t.Fatalf("%s model rewrite missing", tc.external)
		}
		if _, ok := decoded["messages"]; !ok {
			t.Fatalf("%s chat body must carry messages", tc.external)
		}
	}
}

func fallbackDiscoverAdmin(t *testing.T, savedBase, savedKey, secondBase, secondKey string, logBuf *bytes.Buffer) (*AdminServer, string, string, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	savedHits := &atomic.Int32{}
	evilHits := &atomic.Int32{}
	var savedAuth atomic.Value
	savedAuth.Store("")
	var evilAuth atomic.Value
	evilAuth.Store("")
	_ = savedAuth
	_ = evilAuth
	savedSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		savedHits.Add(1)
		if got := r.Header.Get("Authorization"); got != "Bearer "+savedKey {
			w.WriteHeader(401)
			return
		}
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(404)
			return
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[{"id":"saved-model"}]}`))
	}))
	t.Cleanup(savedSrv.Close)
	evilSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		evilHits.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[{"id":"evil-model"}]}`))
	}))
	t.Cleanup(evilSrv.Close)
	actualSavedBase := savedSrv.URL
	if savedBase == "evil" {
		actualSavedBase = evilSrv.URL
	}
	manager := &RuntimeManager{monitor: NewMonitor(), hub: NewLogHub(100), redactor: NewSecretRedactor()}
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{
		{Name: "c1", BaseURL: actualSavedBase, APIKey: savedKey, Model: "m1"},
		{Name: "c2", BaseURL: secondBase, APIKey: secondKey, Model: "m2"},
	}}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, nil, manager.monitor)
	if err != nil {
		t.Fatal(err)
	}
	manager.current.Store(&gatewayRuntime{config: normalized, gateway: gw})
	manager.redactor.Replace(normalized)
	var logger *slog.Logger
	if logBuf != nil {
		logger = slog.New(slog.NewTextHandler(logBuf, nil))
	}
	admin := NewAdminServer(manager, manager.monitor, manager.hub, logger)
	token, csrf := "fallback-discover-token", "fallback-discover-csrf"
	current := manager.Config()
	admin.sessions[tokenDigest(token)] = adminSession{
		Username: current.WebUI.Username, AuthVersion: secretFingerprint(current.WebUI.PasswordHash),
		CSRF: csrf, Expires: time.Now().Add(time.Hour),
	}
	// Stash server URLs for callers via globals on test: return evil URL through second return trick.
	// Callers know savedSrv.URL is the saved base; evil URL is needed for attack cases.
	// We return admin/token/csrf plus hits; evil URL is obtained by re-deriving: tests create their own evil server when needed.
	// To keep this helper simple, expose both URLs via context: abuse logBuf? Instead return via package-level? Simpler: callers use this helper only for saved-channel cases and build attack URLs separately.
	_ = evilSrv
	adminFallbackTestEvilURL = evilSrv.URL
	adminFallbackTestSavedURL = savedSrv.URL
	return admin, token, csrf, savedHits, evilHits
}

var adminFallbackTestEvilURL string
var adminFallbackTestSavedURL string

func discoverPost(t *testing.T, admin *AdminServer, token, csrf string, payload map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	raw, _ := json.Marshal(payload)
	req := operabilityRequest(http.MethodPost, "/api/fallback/discover", string(raw), token, csrf)
	return serveAdmin(admin, req)
}

func TestFallbackDiscoverHardening(t *testing.T) {
	const savedKey = "saved-secret-key-12345"
	const secondKey = "second-secret-key-67890"

	t.Run("saved channel refresh omitted key succeeds", func(t *testing.T) {
		var logs bytes.Buffer
		admin, token, csrf, savedHits, _ := fallbackDiscoverAdmin(t, "", savedKey, "https://b.example", secondKey, &logs)
		savedBase := adminFallbackTestSavedURL
		rec := discoverPost(t, admin, token, csrf, map[string]any{"channel": "c1", "base_url": savedBase})
		if rec.Code != http.StatusOK {
			t.Fatalf("saved refresh omitted code=%d body=%s", rec.Code, rec.Body.String())
		}
		if savedHits.Load() != 1 {
			t.Fatalf("saved refresh must hit saved base once, got %d", savedHits.Load())
		}
		if !strings.Contains(rec.Body.String(), "saved-model") {
			t.Fatalf("saved refresh body=%s", rec.Body.String())
		}
		out := logs.String()
		if !strings.Contains(out, "c1") || !strings.Contains(out, redactURL(savedBase)) || !strings.Contains(out, "success") {
			t.Fatalf("audit log must contain channel+redacted base+result: %q", out)
		}
		if strings.Contains(out, savedKey) || strings.Contains(out, "Bearer") {
			t.Fatalf("audit log must not contain key material: %q", out)
		}
	})

	t.Run("saved channel refresh masked id succeeds", func(t *testing.T) {
		var logs bytes.Buffer
		admin, token, csrf, savedHits, _ := fallbackDiscoverAdmin(t, "", savedKey, "https://b.example", secondKey, &logs)
		savedBase := adminFallbackTestSavedURL
		rec := discoverPost(t, admin, token, csrf, map[string]any{"channel": "c1", "base_url": savedBase, "api_key": map[string]any{"id": secretFingerprint(savedKey)}})
		if rec.Code != http.StatusOK {
			t.Fatalf("masked id refresh code=%d body=%s", rec.Code, rec.Body.String())
		}
		if savedHits.Load() != 1 {
			t.Fatalf("masked id must hit saved base, got %d", savedHits.Load())
		}
	})

	t.Run("channel omitted base defaults to saved", func(t *testing.T) {
		var logs bytes.Buffer
		admin, token, csrf, savedHits, _ := fallbackDiscoverAdmin(t, "", savedKey, "https://b.example", secondKey, &logs)
		rec := discoverPost(t, admin, token, csrf, map[string]any{"channel": "c1"})
		if rec.Code != http.StatusOK {
			t.Fatalf("channel-only refresh code=%d body=%s", rec.Code, rec.Body.String())
		}
		if savedHits.Load() != 1 {
			t.Fatalf("channel-only must hit saved base, got %d", savedHits.Load())
		}
	})

	t.Run("base change with omitted key rejected without send", func(t *testing.T) {
		var logs bytes.Buffer
		admin, token, csrf, savedHits, evilHits := fallbackDiscoverAdmin(t, "", savedKey, "https://b.example", secondKey, &logs)
		evilBase := adminFallbackTestEvilURL
		beforeSaved, beforeEvil := savedHits.Load(), evilHits.Load()
		rec := discoverPost(t, admin, token, csrf, map[string]any{"channel": "c1", "base_url": evilBase})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("base-change omitted must be 400, got %d %s", rec.Code, rec.Body.String())
		}
		if savedHits.Load() != beforeSaved || evilHits.Load() != beforeEvil {
			t.Fatalf("rejected base-change must send nowhere: saved %d->%d evil %d->%d", beforeSaved, savedHits.Load(), beforeEvil, evilHits.Load())
		}
		if strings.Contains(rec.Body.String(), savedKey) {
			t.Fatal("rejection must not leak key")
		}
	})

	t.Run("base change with same-channel id rejected without send", func(t *testing.T) {
		var logs bytes.Buffer
		admin, token, csrf, savedHits, evilHits := fallbackDiscoverAdmin(t, "", savedKey, "https://b.example", secondKey, &logs)
		evilBase := adminFallbackTestEvilURL
		beforeSaved, beforeEvil := savedHits.Load(), evilHits.Load()
		rec := discoverPost(t, admin, token, csrf, map[string]any{"channel": "c1", "base_url": evilBase, "api_key": map[string]any{"id": secretFingerprint(savedKey)}})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("base-change id must be 400, got %d %s", rec.Code, rec.Body.String())
		}
		if savedHits.Load() != beforeSaved || evilHits.Load() != beforeEvil {
			t.Fatalf("rejected id base-change must send nowhere")
		}
	})

	t.Run("cross channel id rejected", func(t *testing.T) {
		var logs bytes.Buffer
		admin, token, csrf, savedHits, _ := fallbackDiscoverAdmin(t, "", savedKey, "https://b.example", secondKey, &logs)
		savedBase := adminFallbackTestSavedURL
		before := savedHits.Load()
		rec := discoverPost(t, admin, token, csrf, map[string]any{"channel": "c1", "base_url": savedBase, "api_key": map[string]any{"id": secretFingerprint(secondKey)}})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("cross-channel id must be 400, got %d %s", rec.Code, rec.Body.String())
		}
		if savedHits.Load() != before {
			t.Fatalf("cross-channel id must not send stored key anywhere")
		}
	})

	t.Run("unknown id rejected", func(t *testing.T) {
		var logs bytes.Buffer
		admin, token, csrf, savedHits, _ := fallbackDiscoverAdmin(t, "", savedKey, "https://b.example", secondKey, &logs)
		savedBase := adminFallbackTestSavedURL
		before := savedHits.Load()
		rec := discoverPost(t, admin, token, csrf, map[string]any{"channel": "c1", "base_url": savedBase, "api_key": map[string]any{"id": "stale-id-99"}})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("unknown id must be 400, got %d", rec.Code)
		}
		if savedHits.Load() != before {
			t.Fatalf("unknown id must not send")
		}
	})

	t.Run("no channel with id rejected without send", func(t *testing.T) {
		var logs bytes.Buffer
		admin, token, csrf, _, evilHits := fallbackDiscoverAdmin(t, "", savedKey, "https://b.example", secondKey, &logs)
		evilBase := adminFallbackTestEvilURL
		before := evilHits.Load()
		rec := discoverPost(t, admin, token, csrf, map[string]any{"base_url": evilBase, "api_key": map[string]any{"id": secretFingerprint(savedKey)}})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("no-channel id must be 400, got %d %s", rec.Code, rec.Body.String())
		}
		if evilHits.Load() != before {
			t.Fatalf("no-channel id must never send stored key to arbitrary URL")
		}
	})

	t.Run("explicit value to arbitrary base allowed", func(t *testing.T) {
		evilHits := &atomic.Int32{}
		evilSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			evilHits.Add(1)
			if r.Header.Get("Authorization") != "Bearer explicit-key-xyz" {
				w.WriteHeader(401)
				return
			}
			w.WriteHeader(200)
			_, _ = w.Write([]byte(`{"data":[{"id":"evil-ok"}]}`))
		}))
		defer evilSrv.Close()
		var logs bytes.Buffer
		admin, token, csrf, _, _ := fallbackDiscoverAdmin(t, "", savedKey, "https://b.example", secondKey, &logs)
		_ = adminFallbackTestEvilURL
		rec := discoverPost(t, admin, token, csrf, map[string]any{"base_url": evilSrv.URL, "api_key": map[string]any{"value": "explicit-key-xyz"}})
		if rec.Code != http.StatusOK {
			t.Fatalf("explicit value must succeed, got %d %s", rec.Code, rec.Body.String())
		}
		if evilHits.Load() != 1 {
			t.Fatalf("explicit value must hit caller URL once, got %d", evilHits.Load())
		}
		if strings.Contains(rec.Body.String(), "explicit-key-xyz") {
			t.Fatal("response must not echo key")
		}
	})

	t.Run("auth csrf no-store preserved", func(t *testing.T) {
		var logs bytes.Buffer
		admin, token, csrf, _, _ := fallbackDiscoverAdmin(t, "", savedKey, "https://b.example", secondKey, &logs)
		if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/fallback/discover", `{"channel":"c1"}`, "", "")); rec.Code != http.StatusUnauthorized {
			t.Fatalf("unauth code=%d want 401", rec.Code)
		}
		if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/fallback/discover", `{"channel":"c1"}`, token, "wrong")); rec.Code != http.StatusForbidden {
			t.Fatalf("bad csrf code=%d want 403", rec.Code)
		}
		rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/fallback/discover", `{"channel":"c1"}`, token, csrf))
		if got := rec.Header().Get("Cache-Control"); got != "no-store" {
			t.Fatalf("Cache-Control=%q want no-store", got)
		}
		_ = csrf
	})
}

func TestFallbackDiscoverFailureAuditRedacted(t *testing.T) {
	const savedKey = "audit-secret-key-99999"
	var logs bytes.Buffer
	admin, token, csrf, _, _ := fallbackDiscoverAdmin(t, "", savedKey, "https://b.example", "other-key", &logs)
	// Force a failure via unknown channel.
	rec := discoverPost(t, admin, token, csrf, map[string]any{"channel": "missing", "base_url": "https://evil.example"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown channel code=%d", rec.Code)
	}
	out := logs.String()
	if !strings.Contains(out, "missing") {
		t.Fatalf("failure audit must contain channel: %q", out)
	}
	if strings.Contains(out, savedKey) || strings.Contains(out, "Bearer") {
		t.Fatalf("failure audit must not contain key material: %q", out)
	}
	// Failure with explicit evil base must log redacted base, not raw secrets.
	logs.Reset()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
	}))
	defer bad.Close()
	rec2 := discoverPost(t, admin, token, csrf, map[string]any{"base_url": bad.URL, "api_key": map[string]any{"value": "explicit-fail-key"}})
	if rec2.Code != http.StatusBadGateway {
		t.Fatalf("discover fail code=%d want 502", rec2.Code)
	}
	out2 := logs.String()
	if !strings.Contains(out2, redactURL(bad.URL)) {
		t.Fatalf("failure audit must contain redacted base: %q", out2)
	}
	if strings.Contains(out2, "explicit-fail-key") {
		t.Fatalf("failure audit must not contain explicit key: %q", out2)
	}
	body, _ := io.ReadAll(rec2.Body)
	_ = body
	if strings.Contains(rec2.Body.String(), "explicit-fail-key") {
		t.Fatal("failure response must not echo key")
	}
	_ = time.Now
}
