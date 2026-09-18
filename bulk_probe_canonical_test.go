package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Probes must share the normal gateway construction invariants across all
// three lane protocols: same endpoint rule, same preparation path, same
// OpenCode/auth/mandatory headers as newUpstreamRequest.
func TestBulkProbeSharesGatewayConstruction(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	gw, err := NewGateway(cfg, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	base := gw.cfg.Upstream.Zen
	tgt := bulkSendTarget{PoolName: "shared", Tier: TierZen, CredKey: "probe-key-12345", CredID: "cred-probe", CredDisp: "12345", Raw: "direct"}
	for _, proto := range []Protocol{ProtocolChat, ProtocolResponses, ProtocolAnthropic} {
		model := "probe-model-" + string(proto)
		sendCtx := context.Background()
		req, ids, routeSession, body, err := buildBulkProbeRequest(sendCtx, base, tgt, proto, model)
		if err != nil {
			t.Fatalf("%s: build: %v", proto, err)
		}
		// Endpoint aligned to protocol via the shared rule.
		if want := strings.TrimRight(base, "/") + protocolPath(proto); req.URL.String() != want {
			t.Fatalf("%s: endpoint=%q want %q", proto, req.URL.String(), want)
		}
		// Shared header parity: rebuild through newUpstreamRequest directly.
		wantReq, err := newUpstreamRequest(sendCtx, base, proto, body, ids, tgt.CredKey, routeSession)
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range []string{"Content-Type", "Accept", "User-Agent", "x-opencode-client", "x-opencode-session", "x-opencode-request", "x-opencode-project"} {
			if req.Header.Get(h) != wantReq.Header.Get(h) {
				t.Fatalf("%s: header %s=%q want %q", proto, h, req.Header.Get(h), wantReq.Header.Get(h))
			}
		}
		if req.Header.Get("x-session-affinity") != "" || req.Header.Get("X-Session-Id") != "" {
			t.Fatalf("%s: must omit generic affinity headers", proto)
		}
		if proto == ProtocolAnthropic {
			if req.Header.Get("x-api-key") != tgt.CredKey {
				t.Fatalf("anthropic key=%q", req.Header.Get("x-api-key"))
			}
			if req.Header.Get("anthropic-version") != "2023-06-01" {
				t.Fatalf("anthropic-version=%q", req.Header.Get("anthropic-version"))
			}
			if req.Header.Get("anthropic-beta") != "interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14" {
				t.Fatalf("anthropic-beta=%q", req.Header.Get("anthropic-beta"))
			}
			if req.Header.Get("Authorization") != "" {
				t.Fatalf("anthropic must not send Bearer auth")
			}
		} else if req.Header.Get("Authorization") != "Bearer "+tgt.CredKey {
			t.Fatalf("%s: auth=%q", proto, req.Header.Get("Authorization"))
		}
		// Body model aligned to the selected probe model.
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatal(err)
		}
		if stringAt(payload, "model") != model {
			t.Fatalf("%s: body model=%q want %q", proto, stringAt(payload, "model"), model)
		}
		// Responses mandatory defaults identical to the gateway path.
		wire := routeWireSession(routeSession)
		if proto == ProtocolResponses {
			if stringAt(payload, "prompt_cache_key") != wire {
				t.Fatalf("responses prompt_cache_key=%q want %q", stringAt(payload, "prompt_cache_key"), wire)
			}
			if v, ok := payload["store"]; !ok || v != false {
				t.Fatalf("responses store must default false, got %v", payload["store"])
			}
		} else {
			if _, ok := payload["prompt_cache_key"]; ok {
				t.Fatalf("%s: must not add prompt_cache_key", proto)
			}
			if _, ok := payload["store"]; ok {
				t.Fatalf("%s: must not add store", proto)
			}
		}
		// Header/body session consistency: same canonical wire session.
		if req.Header.Get("x-opencode-session") != wire {
			t.Fatalf("%s: header session must equal body wire session", proto)
		}
		if req.Header.Get("x-opencode-request") != requestWireID(ids.Request) || req.Header.Get("x-opencode-project") != projectWireID(ids.Project) {
			t.Fatalf("%s: request/project must use shared wire mapping", proto)
		}
	}
}

// Header/body identity consistency: the wire session in headers must equal
// the session material injected into the body for every protocol.
func TestBulkProbeHeaderBodyIdentityConsistent(t *testing.T) {
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	gw, err := NewGateway(cfg, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	base := gw.cfg.Upstream.Zen
	tgt := bulkSendTarget{PoolName: "shared", Tier: TierZen, CredKey: "public", CredID: anonymousSchedulerCredentialID, CredDisp: "anonymous", Raw: "direct", IsPublic: true}
	for _, proto := range []Protocol{ProtocolChat, ProtocolResponses, ProtocolAnthropic} {
		req, _, routeSession, body, err := buildBulkProbeRequest(context.Background(), base, tgt, proto, "m-"+string(proto))
		if err != nil {
			t.Fatal(err)
		}
		wire := routeWireSession(routeSession)
		if req.Header.Get("x-opencode-session") != wire {
			t.Fatalf("%s: header/body session mismatch", proto)
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatal(err)
		}
		if proto == ProtocolResponses {
			if stringAt(payload, "prompt_cache_key") != wire {
				t.Fatalf("responses body session must equal header")
			}
		}
		// A body carrying explicit session fields must be canonicalized to
		// the same wire value (gateway applyRouteSessionToBody semantics).
		withSession, _ := json.Marshal(map[string]any{"model": "m", "conversation_id": "old", "metadata": map[string]any{"session_id": "old"}})
		rewritten, err := applyRouteSessionToBody(withSession, routeSession, proto, false)
		if err != nil {
			t.Fatal(err)
		}
		var rp map[string]any
		if err := json.Unmarshal(rewritten, &rp); err != nil {
			t.Fatal(err)
		}
		if stringAt(rp, "conversation_id") != wire || stringAt(rp, "metadata", "session_id") != wire {
			t.Fatalf("%s: rewritten body must match header wire", proto)
		}
	}
}

// Scheduler/pin/route-session neutrality: probes never read cooldowns,
// never read/write route-session overrides, never establish pins, and never
// emit foreground metrics/history.
func TestBulkProbeSchedulerNeutrality(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081"})
	pool := gw.pools["shared"]
	cred := gw.authCreds[0]
	now := time.Now().UnixNano()
	// Arm cooldowns that a scheduler-reading probe would honor; the probe
	// must still send (no pre-read).
	gw.scheduler.noteProxy429Failure(TierZen, "shared", pool.items[0].name, AttemptClassRateLimited, 429, 0, now)
	gw.scheduler.noteChannelFailure(TierZen, "shared", pool.items[0].name, AttemptClassUpstreamFailure, 500, 0, now)
	sends := 0
	pool.items[0].client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		sends++
		return responseWithBody(200, bulkChatSuccessBody("m")), nil
	})}
	pinsBefore := gw.scheduler.pins.count()
	sessBefore := gw.scheduler.routeSessions.count()
	proxy429Before, _, _ := gw.scheduler.proxy429CooldownStatus(TierZen, "shared", pool.items[0].name)
	if proxy429Before == 0 {
		t.Fatalf("setup must arm proxy429")
	}
	beforeReqs := len(gw.monitor.Snapshot().Upstream.Requests)
	beforeRecent := len(gw.monitor.Snapshot().Upstream.Recent)
	tgt := bulkSendTarget{PoolName: "shared", Index: pool.items[0].index, Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: cred.key, CredID: cred.id, CredDisp: cred.display, ProbeModel: "m", ProbeProtocol: ProtocolChat}
	res := gw.bulkProbeOnce(context.Background(), tgt)
	if sends != 1 {
		t.Fatalf("probe must send despite active cooldowns (no pre-read), sends=%d", sends)
	}
	if !res.Success {
		t.Fatalf("probe must succeed: %+v", res)
	}
	if gw.scheduler.pins.count() != pinsBefore {
		t.Fatalf("probe must not establish pins")
	}
	if gw.scheduler.routeSessions.count() != sessBefore {
		t.Fatalf("probe must not write route-session overrides")
	}
	if until, _, _ := gw.scheduler.proxy429CooldownStatus(TierZen, "shared", pool.items[0].name); until != proxy429Before {
		t.Fatalf("probe must not alter scheduler cooldowns")
	}
	if got := len(gw.monitor.Snapshot().Upstream.Requests); got != beforeReqs {
		t.Fatalf("probe must not emit request metrics")
	}
	if got := len(gw.monitor.Snapshot().Upstream.Recent); got != beforeRecent {
		t.Fatalf("probe must not emit attempt metrics")
	}
	// Deterministic: same target rebuilds the identical request.
	r1, _, rs1, b1, err := buildBulkProbeRequest(context.Background(), gw.cfg.Upstream.Zen, tgt, ProtocolChat, "m")
	if err != nil {
		t.Fatal(err)
	}
	r2, _, rs2, b2, err := buildBulkProbeRequest(context.Background(), gw.cfg.Upstream.Zen, tgt, ProtocolChat, "m")
	if err != nil {
		t.Fatal(err)
	}
	if rs1 != rs2 || string(b1) != string(b2) || r1.Header.Get("x-opencode-session") != r2.Header.Get("x-opencode-session") {
		t.Fatalf("probe construction must be deterministic")
	}
	// Scope parity with the gateway helper: both native channels are
	// proxy-independent.
	anonTgt := bulkSendTarget{PoolName: "shared", Tier: TierZen, CredID: anonymousSchedulerCredentialID, Raw: "direct"}
	authTgt := bulkSendTarget{PoolName: "shared", Tier: TierZen, CredID: cred.id, Raw: "direct"}
	if bulkProbeScope("https://zen.example", anonTgt, ProtocolChat).ProxyRaw != "" {
		t.Fatalf("anonymous scope must be proxy-independent")
	}
	if bulkProbeScope("https://zen.example", authTgt, ProtocolChat).ProxyRaw != "" {
		t.Fatalf("authenticated scope must exclude proxy")
	}
}
