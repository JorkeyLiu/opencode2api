package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fallbackResponsesOK(model string) string {
	return `{"id":"resp-1","object":"response","created_at":1,"model":"` + model + `","output":[{"type":"message","content":[{"type":"output_text","text":"hello"}]}]}`
}

func TestFallbackProtocolDefaultChat(t *testing.T) {
	cfg := testBaseConfig()
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{
		{Name: "c1", BaseURL: "https://a.example", APIKey: "k1", Model: "m1"},
	}}
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if norm.Fallback.Channels[0].Protocol != ProtocolChat {
		t.Fatalf("old config must default to chat, got %q", norm.Fallback.Channels[0].Protocol)
	}
	view := fallbackViewFromConfig(norm)
	if view.Channels[0].Protocol != ProtocolChat {
		t.Fatalf("view must passthrough chat, got %q", view.Channels[0].Protocol)
	}
	resolved, err := resolveFallbackInput(FallbackInput{
		Active: "c1",
		Channels: []FallbackChannelInput{
			{Name: "c1", BaseURL: "https://a.example", APIKey: SecretInput{Value: "k1"}, Model: "m1"},
		},
	}, norm.Fallback)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Channels[0].Protocol != ProtocolChat {
		t.Fatalf("omitted input protocol must default to chat, got %q", resolved.Channels[0].Protocol)
	}
	// Explicit responses round-trips.
	resolved2, err := resolveFallbackInput(FallbackInput{
		Active: "c1",
		Channels: []FallbackChannelInput{
			{Name: "c1", BaseURL: "https://a.example", APIKey: SecretInput{Value: "k1"}, Model: "m1", Protocol: ProtocolResponses},
		},
	}, norm.Fallback)
	if err != nil {
		t.Fatal(err)
	}
	if resolved2.Channels[0].Protocol != ProtocolResponses {
		t.Fatalf("responses must persist, got %q", resolved2.Channels[0].Protocol)
	}
	if got := fallbackProtocolDisplay(ProtocolResponses); got != "Responses" {
		t.Fatalf("display responses=%q", got)
	}
	if got := fallbackProtocolDisplay(ProtocolChat); got != "Chat Completions" {
		t.Fatalf("display chat=%q", got)
	}
}

func TestFallbackProtocolStrict(t *testing.T) {
	for _, bad := range []Protocol{"anthropic", "sse", "chat2", "http", "messages"} {
		c := testBaseConfig()
		c.Fallback = FallbackConfig{Channels: []FallbackChannelConfig{{Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "m", Protocol: bad}}}
		if _, err := NormalizeConfig("config.json", c); err == nil {
			t.Fatalf("protocol %q must be rejected", bad)
		}
	}
	// Case-insensitive acceptance + trim.
	c := testBaseConfig()
	c.Fallback = FallbackConfig{Channels: []FallbackChannelConfig{{Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "m", Protocol: " Responses "}}}
	norm, err := NormalizeConfig("config.json", c)
	if err != nil {
		t.Fatalf("responses with spaces must normalize: %v", err)
	}
	if norm.Fallback.Channels[0].Protocol != ProtocolResponses {
		t.Fatalf("normalized=%q want responses", norm.Fallback.Channels[0].Protocol)
	}
	// Strict JSON decoder must reject unknown channel field but accept protocol.
	var decoded Config
	raw := `{"fallback":{"active":"c","channels":[{"name":"c","base_url":"https://a.example","api_key":"k","model":"m","protocol":"responses"}]}}`
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	_ = decoded
}

func TestFallbackEndpointMatrix(t *testing.T) {
	host := "https://api.example.com"
	cases := []struct {
		base          string
		wantChat      string
		wantResponses string
		wantModels    string
	}{
		{host, host + "/v1/chat/completions", host + "/v1/responses", host + "/v1/models"},
		{host + "/", host + "/v1/chat/completions", host + "/v1/responses", host + "/v1/models"},
		{host + "/v1", host + "/v1/chat/completions", host + "/v1/responses", host + "/v1/models"},
		{host + "/v1/", host + "/v1/chat/completions", host + "/v1/responses", host + "/v1/models"},
		{host + "/v1/chat/completions", host + "/v1/chat/completions", host + "/v1/responses", host + "/v1/models"},
		{host + "/v1/responses", host + "/v1/chat/completions", host + "/v1/responses", host + "/v1/models"},
		{host + "/v1/chat/completions/", host + "/v1/chat/completions", host + "/v1/responses", host + "/v1/models"},
		{host + "/v1/responses/", host + "/v1/chat/completions", host + "/v1/responses", host + "/v1/models"},
		{host + "/v1/models", host + "/v1/chat/completions", host + "/v1/responses", host + "/v1/models"},
	}
	for _, tc := range cases {
		if got := fallbackEndpointURL(tc.base, ProtocolChat); got != tc.wantChat {
			t.Fatalf("base %q chat=%q want %q", tc.base, got, tc.wantChat)
		}
		if got := fallbackEndpointURL(tc.base, ProtocolResponses); got != tc.wantResponses {
			t.Fatalf("base %q responses=%q want %q", tc.base, got, tc.wantResponses)
		}
		if got := fallbackChatURL(tc.base); got != tc.wantChat {
			t.Fatalf("base %q chatURL=%q want %q", tc.base, got, tc.wantChat)
		}
		if got := fallbackResponsesURL(tc.base); got != tc.wantResponses {
			t.Fatalf("base %q responsesURL=%q want %q", tc.base, got, tc.wantResponses)
		}
		if got := fallbackModelsURL(tc.base); got != tc.wantModels {
			t.Fatalf("base %q models=%q want %q", tc.base, got, tc.wantModels)
		}
		// Cross-protocol base must never double the path.
		for _, ep := range []string{fallbackEndpointURL(tc.base, ProtocolChat), fallbackEndpointURL(tc.base, ProtocolResponses)} {
			if strings.Contains(ep, "/v1/v1/") || strings.Contains(ep, "chat/completions/v1") || strings.Contains(ep, "responses/v1") {
				t.Fatalf("base %q produced doubled path %q", tc.base, ep)
			}
		}
	}
	if got := fallbackEndpointURL("", ProtocolChat); got != "" {
		t.Fatalf("empty base must be empty, got %q", got)
	}
}

func TestFallbackResponsesRequestResponse(t *testing.T) {
	var gotPath string
	var gotBody []byte
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotBody, _ = io.ReadAll(r.Body)
		if r.Header.Get("Authorization") == "" {
			w.WriteHeader(401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackResponsesOK("configured-resp")))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k", Model: "configured-resp", Protocol: ProtocolResponses}}, "c1")
	ses := "ses_resp_req_1"
	bindAnonPin(t, gw, ses, "m")
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r-resp"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("responses takeover status=%d body=%s", resp.StatusCode, string(raw))
	}
	if eff.Tier != TierCustom || eff.Protocol != ProtocolResponses {
		t.Fatalf("effective route must be custom/responses, got %+v", eff)
	}
	if gotPath != "/v1/responses" {
		t.Fatalf("path=%q want /v1/responses", gotPath)
	}
	var payload map[string]any
	if err := json.Unmarshal(gotBody, &payload); err != nil {
		t.Fatal(err)
	}
	if stringAt(payload, "model") != "configured-resp" {
		t.Fatalf("model rewrite missing: %s", string(gotBody))
	}
	if _, ok := payload["input"]; !ok {
		t.Fatalf("responses body must carry input, got %s", string(gotBody))
	}
	// Stored upstream body must transcode back to the client protocol.
	if _, err := convertResponse(ProtocolResponses, ProtocolChat, raw); err != nil {
		t.Fatalf("responses->chat transcode failed: %v", err)
	}
	// Binding identity must include the protocol.
	binding, ok := gw.scheduler.fallbacks.get(ses)
	if !ok || binding.Protocol != ProtocolResponses {
		t.Fatalf("binding must carry responses, got %+v %v", binding, ok)
	}
}

func TestFallbackResponsesStream(t *testing.T) {
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k", Model: "configured-resp", Protocol: ProtocolResponses}}, "c1")
	ses := "ses_resp_stream_1"
	bindAnonPin(t, gw, ses, "m")
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "stream": true}}
	resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), map[Tier][]byte{TierZen: []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true}`), TierGo: []byte(`{"model":"m"}`)}, pinIDs(ses, "r"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 || eff.Protocol != ProtocolResponses || eff.Tier != TierCustom {
		t.Fatalf("stream takeover must be custom/responses 200, got %d %+v", resp.StatusCode, eff)
	}
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), "response.output_text.delta") {
		t.Fatalf("stream body missing responses marker: %q", string(raw))
	}
}

func TestFallbackCustomAvailabilityResponses(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"output":[{"type":"message","content":[{"type":"output_text","text":"hi"}]}]}`))
	}))
	defer srv.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{Name: "c1", BaseURL: srv.URL, APIKey: "k", Model: "cm", Protocol: ProtocolResponses}}, "c1")
	res := gw.bulkCustomProbeOnce(context.Background(), bulkCustomTarget{Name: "c1", BaseURL: srv.URL, Model: "cm", APIKey: "k", Protocol: ProtocolResponses})
	if !res.Success {
		t.Fatalf("responses probe must succeed: %+v", res)
	}
	if gotPath != "/v1/responses" {
		t.Fatalf("probe path=%q want /v1/responses", gotPath)
	}
	// Chat probe still hits chat endpoint.
	var chatPath string
	chatSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chatPath = r.URL.Path
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm")))
	}))
	defer chatSrv.Close()
	res2 := gw.bulkCustomProbeOnce(context.Background(), bulkCustomTarget{Name: "c1", BaseURL: chatSrv.URL, Model: "cm", APIKey: "k", Protocol: ProtocolChat})
	if !res2.Success || chatPath != "/v1/chat/completions" {
		t.Fatalf("chat probe must hit chat endpoint: %+v path=%q", res2, chatPath)
	}
}

func TestFallbackIdentityIncludesProtocol(t *testing.T) {
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackResponsesOK("cm")))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm", Protocol: ProtocolResponses}}, "c1")
	ses := "ses_proto_change"
	bindAnonPin(t, gw, ses, "m")
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	drainResp(resp)
	// Change only the protocol: old session must 502, never drift.
	gw.cfg.Fallback.Channels = []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "cm", Protocol: ProtocolChat}}
	resp2, eff2, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r2"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp2)
	if resp2.StatusCode != 502 {
		t.Fatalf("protocol change must 502, got %d", resp2.StatusCode)
	}
	if eff2.Tier != TierCustom {
		t.Fatalf("502 must stay custom, got %+v", eff2)
	}
}

func TestFallbackDiscoverReasons(t *testing.T) {
	// non_2xx.
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
	}))
	defer bad.Close()
	if _, err := fetchFallbackModels(context.Background(), nil, bad.URL, "k"); err == nil {
		t.Fatal("500 must fail")
	} else {
		detail, ok := asFallbackDiscoverError(err)
		if !ok || detail.Reason != fallbackDiscoverNon2xx || detail.HTTPStatus != 500 {
			t.Fatalf("non_2xx detail=%+v ok=%v err=%v", detail, ok, err)
		}
		if detail.Endpoint == "" || detail.ElapsedMS < 0 {
			t.Fatalf("endpoint/elapsed must be set: %+v", detail)
		}
		if strings.Contains(err.Error(), "k") || strings.Contains(detail.Endpoint, "k") {
			t.Fatal("discover error must not leak key")
		}
	}
	// invalid_json.
	badJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`not json`))
	}))
	defer badJSON.Close()
	if _, err := fetchFallbackModels(context.Background(), nil, badJSON.URL, "k"); err == nil {
		t.Fatal("invalid json must fail")
	} else if detail, ok := asFallbackDiscoverError(err); !ok || detail.Reason != fallbackDiscoverInvalidJSON {
		t.Fatalf("invalid_json detail=%+v ok=%v", detail, ok)
	}
	// empty_list.
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer empty.Close()
	if _, err := fetchFallbackModels(context.Background(), nil, empty.URL, "k"); err == nil {
		t.Fatal("empty must fail")
	} else if detail, ok := asFallbackDiscoverError(err); !ok || detail.Reason != fallbackDiscoverEmptyList {
		t.Fatalf("empty_list detail=%+v ok=%v", detail, ok)
	}
	// timeout via short client timeout + slow server.
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[{"id":"m"}]}`))
	}))
	defer slow.Close()
	shortClient := &http.Client{Timeout: 50 * time.Millisecond}
	if _, err := fetchFallbackModels(context.Background(), shortClient, slow.URL, "k"); err == nil {
		t.Fatal("timeout must fail")
	} else if detail, ok := asFallbackDiscoverError(err); !ok || detail.Reason != fallbackDiscoverTimeout {
		t.Fatalf("timeout detail=%+v ok=%v err=%v", detail, ok, err)
	}
}

func TestFallbackDiscoverClassifyUnit(t *testing.T) {
	if got := fallbackDiscoverTransportReason(context.DeadlineExceeded); got != fallbackDiscoverTimeout {
		t.Fatalf("deadline=%q", got)
	}
	if got := fallbackDiscoverTransportReason(&net.DNSError{Err: "no such host"}); got != fallbackDiscoverDNS {
		t.Fatalf("dns=%q", got)
	}
	if got := fallbackDiscoverTransportReason(errors.New("dial tcp: connection refused")); got != fallbackDiscoverConnRefused {
		t.Fatalf("refused=%q", got)
	}
	if got := fallbackDiscoverTransportReason(x509.UnknownAuthorityError{}); got != fallbackDiscoverTLS {
		t.Fatalf("tls=%q", got)
	}
	if got := fallbackDiscoverTransportReason(errors.New("boom")); got != fallbackDiscoverTransport {
		t.Fatalf("generic=%q", got)
	}
}

func TestFallbackDiscoverAdminShape(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte(`{"secret":"upstream-body-must-not-leak"}`))
	}))
	defer bad.Close()
	manager := &RuntimeManager{monitor: NewMonitor(), hub: NewLogHub(100), redactor: NewSecretRedactor()}
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	cfg.Fallback = FallbackConfig{Active: "", Channels: []FallbackChannelConfig{}}
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
	admin := NewAdminServer(manager, manager.monitor, manager.hub, nil)
	token, csrf := "discover-shape-token", "discover-shape-csrf"
	current := manager.Config()
	admin.sessions[tokenDigest(token)] = adminSession{
		Username: current.WebUI.Username, AuthVersion: secretFingerprint(current.WebUI.PasswordHash),
		CSRF: csrf, Expires: time.Now().Add(time.Hour),
	}
	raw, _ := json.Marshal(map[string]any{"base_url": bad.URL, "api_key": map[string]any{"value": "shape-secret-key"}})
	req := operabilityRequest(http.MethodPost, "/api/fallback/discover", string(raw), token, csrf)
	rec := serveAdmin(admin, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("code=%d want 502 body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control=%q", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	errObj, _ := payload["error"].(map[string]any)
	if errObj["code"] != "discover_failed" {
		t.Fatalf("code field=%v", errObj["code"])
	}
	if errObj["reason"] != fallbackDiscoverNon2xx {
		t.Fatalf("reason=%v want non_2xx", errObj["reason"])
	}
	if _, ok := errObj["endpoint"]; !ok {
		t.Fatal("endpoint must be present")
	}
	if _, ok := errObj["elapsed_ms"]; !ok {
		t.Fatal("elapsed_ms must be present")
	}
	body := rec.Body.String()
	if strings.Contains(body, "shape-secret-key") || strings.Contains(body, "upstream-body-must-not-leak") || strings.Contains(body, "Authorization") {
		t.Fatalf("discover response must not leak secrets/body: %s", body)
	}
	_ = atomic.Int32{}
}
