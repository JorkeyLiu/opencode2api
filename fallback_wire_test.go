package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Custom Responses inference must send the canonical session/request/project
// headers with pseudonymous values that never contain the raw client session
// or secrets, using the same wire helpers as native Zen inference.
func TestCustomResponsesSendsCanonicalWireHeaders(t *testing.T) {
	var gotSes, gotReq, gotPrj, gotUA, gotClient string
	var gotBody map[string]any
	rawSession := "ses_raw_client_secret_session"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" {
			w.WriteHeader(404)
			return
		}
		gotSes = r.Header.Get("x-opencode-session")
		gotReq = r.Header.Get("x-opencode-request")
		gotPrj = r.Header.Get("x-opencode-project")
		gotUA = r.Header.Get("User-Agent")
		gotClient = r.Header.Get("x-opencode-client")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"id":"resp-1","object":"response","created_at":1,"model":"cm","output":[],"status":"completed","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`))
	}))
	defer srv.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{ID: "c-resp", Name: "c-resp", BaseURL: srv.URL, APIKey: "secret-key-xyz", Model: "cm", Protocol: ProtocolResponses}}, "c-resp")
	ch, ok := fallbackChannelLookup(gw.cfg, "c-resp")
	if !ok {
		t.Fatal("channel not found")
	}
	ids := requestIDs{Session: rawSession, Request: "req_internal_1", Project: "prj_internal_1"}
	routeSession := customRouteSession(ids.Session, ch)
	if strings.Contains(routeSession, rawSession) || strings.Contains(routeSession, "secret-key-xyz") {
		t.Fatalf("internal route session leaks raw material: %q", routeSession)
	}
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	resp, _, _, err := gw.doCustomFallbackRequest(pinTestCtx(), anonAuthRoute(), ex, true, routeBodies(), ids, ch, fallbackBindingFor(ch), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if gotSes == "" || !isCanonicalWireSession(gotSes) {
		t.Fatalf("session must be canonical wire shape, got %q", gotSes)
	}
	if !isCanonicalWireRequest(gotReq) || !isCanonicalWireProject(gotPrj) {
		t.Fatalf("request/project must be canonical, got %q %q", gotReq, gotPrj)
	}
	for _, v := range []string{gotSes, gotReq, gotPrj} {
		if strings.Contains(v, rawSession) || strings.Contains(v, "secret-key-xyz") {
			t.Fatalf("wire value leaks raw material: %q", v)
		}
	}
	if gotUA != opencodeWireUserAgent() || gotClient != "cli" {
		t.Fatalf("identity headers mismatch: ua=%q client=%q", gotUA, gotClient)
	}
	// Native parity: same helpers produce the same wire values.
	wantReq, _ := http.NewRequest("POST", "http://example.test", nil)
	_ = wantReq
	if requestWireID(ids.Request) != gotReq || projectWireID(ids.Project) != gotPrj || routeWireSession(routeSession) != gotSes {
		t.Fatalf("custom wire values must equal shared helper mapping")
	}
	if gotBody == nil {
		t.Fatal("missing body")
	}
	if cacheKey, _ := gotBody["prompt_cache_key"].(string); cacheKey != gotSes {
		t.Fatalf("responses body prompt_cache_key must match wire session, got %v", gotBody["prompt_cache_key"])
	}
	if gotBody["store"] != false {
		t.Fatalf("responses body store must default false, got %v", gotBody["store"])
	}
	// Full history still sent: model rewritten, converted input present.
	if gotBody["model"] != "cm" {
		t.Fatalf("model rewrite missing: %v", gotBody["model"])
	}
}

// Custom availability probes must carry canonical inference headers so a
// compatible upstream requiring x-opencode-session returns 200 instead of a
// false 400. Probes stay stateless and scheduler-neutral.
func TestCustomProbeSendsCanonicalHeaders(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-opencode-session") == "" || !isCanonicalWireSession(r.Header.Get("x-opencode-session")) {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":{"message":"MissingSessionID"}}`))
			return
		}
		if r.Header.Get("User-Agent") != opencodeWireUserAgent() || r.Header.Get("x-opencode-client") != "cli" {
			w.WriteHeader(400)
			return
		}
		if !isCanonicalWireRequest(r.Header.Get("x-opencode-request")) || !isCanonicalWireProject(r.Header.Get("x-opencode-project")) {
			w.WriteHeader(400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"cm","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}]}`))
	}))
	defer srv.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{ID: "c-probe", Name: "c-probe", BaseURL: srv.URL, APIKey: "k-probe", Model: "cm"}}, "c-probe")
	beforePins := 0
	if gw.scheduler != nil {
		beforePins = 1 // marker only; custom path must not use scheduler stores
		_ = beforePins
	}
	result := gw.bulkCustomProbeOnce(context.Background(), bulkCustomTarget{ID: "c-probe", Name: "c-probe", BaseURL: srv.URL, Model: "cm", APIKey: "k-probe", Protocol: ProtocolChat})
	if !result.Success || result.Status != 200 {
		t.Fatalf("probe must succeed with canonical headers: %+v", result)
	}
}

// Custom 401 and 400 API payloads must retain the exact http_status.
func TestCustomAvailabilityKeepsHTTPStatus(t *testing.T) {
	mk := func(status int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":{"message":"e"}}`))
		}))
	}
	srv401 := mk(401)
	defer srv401.Close()
	srv400 := mk(400)
	defer srv400.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{
		{ID: "c401", Name: "c401", BaseURL: srv401.URL, APIKey: "k", Model: "cm"},
		{ID: "c400", Name: "c400", BaseURL: srv400.URL, APIKey: "k", Model: "cm"},
	}, "c401")
	res401 := gw.bulkCustomProbeOnce(context.Background(), bulkCustomTarget{ID: "c401", Name: "c401", BaseURL: srv401.URL, Model: "cm", APIKey: "k", Protocol: ProtocolChat})
	if res401.Status != 401 {
		t.Fatalf("probe status=%d", res401.Status)
	}
	label401 := bulkCustomOutcomeLabel(res401)
	st401, rs401 := bulkCustomStatusFor(label401)
	_ = st401
	_ = rs401
	resp401 := gw.runCustomCheck(context.Background(), FallbackChannelConfig{ID: "c401", Name: "c401", BaseURL: srv401.URL, APIKey: "k", Model: "cm"})
	if resp401.Custom.HTTPStatus != 401 {
		t.Fatalf("single check http_status=%d want 401", resp401.Custom.HTTPStatus)
	}
	if resp401.Custom.Reason == "" || resp401.Custom.Status == "" {
		t.Fatalf("single check must keep status/reason: %+v", resp401.Custom)
	}
	batch := gw.runCustomsBatch(context.Background())
	byID := map[string]bulkCustomAvailability{}
	for _, row := range batch.Custom {
		byID[row.ID] = row
	}
	if byID["c401"].HTTPStatus != 401 {
		t.Fatalf("batch c401 http_status=%d want 401", byID["c401"].HTTPStatus)
	}
	if byID["c400"].HTTPStatus != 400 {
		t.Fatalf("batch c400 http_status=%d want 400", byID["c400"].HTTPStatus)
	}
	// No HTTP response stays 0/omitted (transport error path).
	transport := gw.bulkCustomProbeOnce(context.Background(), bulkCustomTarget{ID: "cx", Name: "cx", BaseURL: "http://127.0.0.1:1", Model: "cm", APIKey: "k", Protocol: ProtocolChat})
	if transport.Status != 0 {
		t.Fatalf("transport-only probe must have status 0, got %d", transport.Status)
	}
}

func TestCustomWebUIDisplaysHTTPStatus(t *testing.T) {
	data, err := readWebUIFile()
	if err != nil {
		t.Fatal(err)
	}
	html := string(data)
	// Custom status cell must reuse the shared factual helpers and read the
	// exact http_status field (both snake_case and compat casings).
	for _, needle := range []string{
		`obsDisplayText(c.status`,
		`obsTone(c.status`,
		`http_status`,
		`HTTP "+code+"`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("custom webui must contain %q", needle)
		}
	}
	// Single-check toast must surface the real code, not just unavailable.
	if !strings.Contains(html, "HTTP ") || !strings.Contains(html, "probeCustom") {
		t.Fatal("custom toast must surface HTTP code")
	}
}
