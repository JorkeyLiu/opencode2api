package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Custom Responses availability probes must use the compatible minimal output
// limit (16): Zen-compatible Responses upstreams reject max_output_tokens=1
// with 400 (param=max_output_tokens) when the full OpenCode fingerprint is
// present, while 16 returns 200. Headers/session semantics must stay intact.
func TestCustomResponsesProbeUsesCompatibleOutputLimit(t *testing.T) {
	raw, err := bulkProbeRequestBody("cm", ProtocolResponses, "")
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	if got, ok := payload["max_output_tokens"].(float64); !ok || got != 16 {
		t.Fatalf("responses probe max_output_tokens=%v want 16", payload["max_output_tokens"])
	}
	if payload["stream"] != false {
		t.Fatalf("responses probe stream must stay false, got %v", payload["stream"])
	}
	if payload["model"] != "cm" {
		t.Fatalf("responses probe model=%v want cm", payload["model"])
	}

	routeSession := "rss_probe_compat_1"
	stamped, err := bulkProbeRequestBodyWithSession("cm", ProtocolResponses, "", routeSession)
	if err != nil {
		t.Fatal(err)
	}
	var stampedPayload map[string]any
	if err := json.Unmarshal(stamped, &stampedPayload); err != nil {
		t.Fatal(err)
	}
	if got, ok := stampedPayload["max_output_tokens"].(float64); !ok || got != 16 {
		t.Fatalf("stamped responses probe max_output_tokens=%v want 16", stampedPayload["max_output_tokens"])
	}
	wire := routeWireSession(routeSession)
	if stampedPayload["prompt_cache_key"] != wire {
		t.Fatalf("prompt_cache_key=%v want wire %q", stampedPayload["prompt_cache_key"], wire)
	}
	if stampedPayload["store"] != false {
		t.Fatalf("store must stay false, got %v", stampedPayload["store"])
	}

	var gotBody map[string]any
	var gotSes, gotReq, gotPrj, gotUA, gotClient, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotSes = r.Header.Get("x-opencode-session")
		gotReq = r.Header.Get("x-opencode-request")
		gotPrj = r.Header.Get("x-opencode-project")
		gotUA = r.Header.Get("User-Agent")
		gotClient = r.Header.Get("x-opencode-client")
		rawBody, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(rawBody, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"output":[{"type":"message","content":[{"type":"output_text","text":"hi"}]}]}`))
	}))
	defer srv.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{ID: "c-resp-compat", Name: "c-resp-compat", BaseURL: srv.URL, APIKey: "k-probe", Model: "cm", Protocol: ProtocolResponses}}, "c-resp-compat")
	res := gw.bulkCustomProbeOnce(context.Background(), bulkCustomTarget{ID: "c-resp-compat", Name: "c-resp-compat", BaseURL: srv.URL, Model: "cm", APIKey: "k-probe", Protocol: ProtocolResponses})
	if !res.Success || res.Status != 200 {
		t.Fatalf("responses probe must succeed: %+v", res)
	}
	if gotPath != "/v1/responses" {
		t.Fatalf("probe path=%q want /v1/responses", gotPath)
	}
	if got, ok := gotBody["max_output_tokens"].(float64); !ok || got != 16 {
		t.Fatalf("sent max_output_tokens=%v want 16", gotBody["max_output_tokens"])
	}
	if gotBody["stream"] != false {
		t.Fatalf("sent stream must stay false, got %v", gotBody["stream"])
	}
	if gotBody["prompt_cache_key"] != gotSes || gotSes == "" || !isCanonicalWireSession(gotSes) {
		t.Fatalf("prompt_cache_key must equal canonical wire session header, body=%v header=%q", gotBody["prompt_cache_key"], gotSes)
	}
	if gotBody["store"] != false {
		t.Fatalf("sent store must stay false, got %v", gotBody["store"])
	}
	if gotUA != opencodeWireUserAgent() || gotClient != "cli" {
		t.Fatalf("canonical identity headers mismatch: ua=%q client=%q", gotUA, gotClient)
	}
	if !isCanonicalWireRequest(gotReq) || !isCanonicalWireProject(gotPrj) {
		t.Fatalf("request/project must stay canonical, got %q %q", gotReq, gotPrj)
	}

	// Chat custom probes intentionally stay at the existing minimal limit.
	chatRaw, err := bulkProbeRequestBody("cm", ProtocolChat, "")
	if err != nil {
		t.Fatal(err)
	}
	var chatPayload map[string]any
	if err := json.Unmarshal(chatRaw, &chatPayload); err != nil {
		t.Fatal(err)
	}
	if got, ok := chatPayload["max_tokens"].(float64); !ok || got != 1 {
		t.Fatalf("chat probe max_tokens=%v want 1 (unchanged minimal)", chatPayload["max_tokens"])
	}
	if chatPayload["stream"] != false {
		t.Fatalf("chat probe stream must stay false, got %v", chatPayload["stream"])
	}
}
