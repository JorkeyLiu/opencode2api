package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestWireShapesExact(t *testing.T) {
	s := routeWireSession("rss_unit_test_token")
	if !isCanonicalWireSession(s) {
		t.Fatalf("route wire session shape invalid: %q", s)
	}
	if len(s) != 30 {
		t.Fatalf("wire session len=%d want 30", len(s))
	}
	m := requestWireID("req_unit_test")
	if !isCanonicalWireRequest(m) {
		t.Fatalf("request wire shape invalid: %q", m)
	}
	if len(m) != 30 {
		t.Fatalf("wire request len=%d want 30", len(m))
	}
	p := projectWireID("prj_unit_test")
	if !isCanonicalWireProject(p) {
		t.Fatalf("project wire shape invalid: %q", p)
	}
	par := parentWireSession("raw-parent-signal")
	if !isCanonicalWireSession(par) {
		t.Fatalf("parent wire shape invalid: %q", par)
	}
	if got := parentWireSession(""); got != "" {
		t.Fatalf("empty parent must stay empty, got %q", got)
	}
	if !isCanonicalWireSession(routeWireSession(deriveFirstRouteSession(bulkProbeIDs().Session, routeSessionScope{Authority: "https://zen.example", Tier: TierZen, CredID: anonymousSchedulerCredentialID, Pool: "shared", ProxyRaw: "direct", Protocol: ProtocolChat}))) {
		t.Fatalf("probe session shape invalid")
	}
	if !isCanonicalWireRequest(requestWireID(bulkProbeIDs().Request)) {
		t.Fatalf("probe request shape invalid: %q", requestWireID(bulkProbeIDs().Request))
	}
	if !isCanonicalWireProject(projectWireID(bulkProbeIDs().Project)) {
		t.Fatalf("probe project shape invalid: %q", projectWireID(bulkProbeIDs().Project))
	}
	// Negative shapes.
	for _, bad := range []string{"", "ses_short", "msg_short", "bulk-probe", "rss_abc", "ses_UPPERCASE00000000000000", strings.Repeat("g", 40)} {
		if isCanonicalWireSession(bad) {
			t.Fatalf("bad session accepted: %q", bad)
		}
		if isCanonicalWireRequest(bad) {
			t.Fatalf("bad request accepted: %q", bad)
		}
	}
	if isCanonicalWireProject("bulk-probe") || isCanonicalWireProject("prj_abc") {
		t.Fatalf("bad project accepted")
	}
}

func TestWireStabilityAndDomainSeparation(t *testing.T) {
	a1 := routeWireSession("rss_same")
	a2 := routeWireSession("rss_same")
	if a1 != a2 {
		t.Fatalf("route wire must be stable: %q vs %q", a1, a2)
	}
	b := routeWireSession("rss_other")
	if a1 == b {
		t.Fatalf("different route tokens must differ")
	}
	// Same raw signal across domains must differ (except shared prefix).
	sessRoute := buildWireSession("same-signal", "route-wire-session-v1")
	sessParent := buildWireSession("same-signal", "wire-parent-v1")
	if sessRoute == sessParent {
		t.Fatalf("domain separation failed for session: %q", sessRoute)
	}
	reqA := buildWireRequest("same-signal", "wire-request-v1")
	reqB := buildWireRequest("same-signal", "wire-probe-request-v1")
	if reqA == reqB {
		t.Fatalf("domain separation failed for request")
	}
	projA := buildWireProject("same-signal", "wire-project-v1")
	projB := buildWireProject("same-signal", "wire-probe-project-v1")
	if projA == projB {
		t.Fatalf("domain separation failed for project")
	}
	// No raw leakage.
	raw := "super-secret-wire-signal-xyz"
	w := routeWireSession(raw)
	if strings.Contains(w, raw) {
		t.Fatalf("wire leaks raw signal: %q", w)
	}
}

func TestBase62FixedLeadingZeroes(t *testing.T) {
	if got := base62Fixed(make([]byte, 10), 14); got != strings.Repeat("0", 14) {
		t.Fatalf("zero bytes must encode to all zeroes, got %q", got)
	}
	if got := base62Fixed([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, 14); got != strings.Repeat("0", 13)+"1" {
		t.Fatalf("single unit must preserve leading zeroes, got %q", got)
	}
	for _, width := range []int{14} {
		_ = width
	}
	// Alphabet check on a real encoding.
	enc := base62Fixed([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 14)
	if len(enc) != 14 || !isBase62(enc) {
		t.Fatalf("base62 encoding invalid: %q", enc)
	}
}

func TestInternalRouteStaysRSSWireIsCanonical(t *testing.T) {
	client := "ses_client_wire_1"
	scope := routeSessionScope{Authority: "https://zen.example", Tier: TierZen, CredID: anonymousSchedulerCredentialID, Pool: "a", ProxyRaw: "direct", Protocol: ProtocolChat}
	internal := deriveFirstRouteSession(client, scope)
	if !strings.HasPrefix(internal, "rss_") {
		t.Fatalf("internal must stay rss_*, got %q", internal)
	}
	if internal == client {
		t.Fatalf("internal must differ from client")
	}
	wire := routeWireSession(internal)
	if !isCanonicalWireSession(wire) {
		t.Fatalf("wire must be canonical, got %q", wire)
	}
	if strings.HasPrefix(wire, "rss_") || wire == internal || strings.Contains(wire, internal) {
		t.Fatalf("wire must not leak internal token: %q vs %q", wire, internal)
	}
}

func TestNewUpstreamRequestOfficialHeaders(t *testing.T) {
	ids := requestIDs{Session: "ses_client_hdr_1", Request: "req_hdr_1", Project: "prj_hdr_1", ParentSession: "raw-parent-1"}
	body := []byte(`{"model":"m"}`)
	for _, proto := range []Protocol{ProtocolChat, ProtocolResponses, ProtocolAnthropic} {
		req, err := newUpstreamRequest(context.Background(), "https://zen.example", proto, body, ids, "k", "rss_hdr_token")
		if err != nil {
			t.Fatal(err)
		}
		if got := req.Header.Get("User-Agent"); got != "opencode/1.18.31" {
			t.Fatalf("%s UA=%q want bare", proto, got)
		}
		if got := req.Header.Get("x-opencode-client"); got != "cli" {
			t.Fatalf("%s client=%q", proto, got)
		}
		ses := req.Header.Get("x-opencode-session")
		if !isCanonicalWireSession(ses) {
			t.Fatalf("%s session=%q not canonical", proto, ses)
		}
		if ses != routeWireSession("rss_hdr_token") {
			t.Fatalf("%s session mismatch wire", proto)
		}
		msg := req.Header.Get("x-opencode-request")
		if !isCanonicalWireRequest(msg) || msg != requestWireID("req_hdr_1") {
			t.Fatalf("%s request=%q", proto, msg)
		}
		prj := req.Header.Get("x-opencode-project")
		if !isCanonicalWireProject(prj) || prj != projectWireID("prj_hdr_1") {
			t.Fatalf("%s project=%q", proto, prj)
		}
		par := req.Header.Get("x-parent-session-id")
		if !isCanonicalWireSession(par) || par != parentWireSession("raw-parent-1") {
			t.Fatalf("%s parent=%q", proto, par)
		}
		if v := req.Header.Get("x-session-affinity"); v != "" {
			t.Fatalf("%s must omit x-session-affinity, got %q", proto, v)
		}
		if v := req.Header.Get("X-Session-Id"); v != "" {
			t.Fatalf("%s must omit X-Session-Id, got %q", proto, v)
		}
		// Auth/protocol headers unchanged.
		if proto == ProtocolAnthropic {
			if req.Header.Get("x-api-key") != "k" {
				t.Fatalf("anthropic key missing")
			}
		} else if req.Header.Get("Authorization") != "Bearer k" {
			t.Fatalf("%s auth missing", proto)
		}
	}
	// No parent -> header omitted.
	idsNoParent := requestIDs{Session: "s", Request: "req_x", Project: "prj_x"}
	req, err := newUpstreamRequest(context.Background(), "https://zen.example", ProtocolChat, body, idsNoParent, "k", "rss_t")
	if err != nil {
		t.Fatal(err)
	}
	if v := req.Header.Get("x-parent-session-id"); v != "" {
		t.Fatalf("missing parent must omit header, got %q", v)
	}
}

func TestResponsesBodyWireDefaults(t *testing.T) {
	wire := routeWireSession("rss_resp_wire_1")
	// Existing fields canonicalized, prompt_cache_key set, store defaulted.
	raw, _ := json.Marshal(map[string]any{"model": "m", "conversation_id": "old", "metadata": map[string]any{"session_id": "old"}})
	out, err := applyRouteSessionToBody(raw, "rss_resp_wire_1", ProtocolResponses, false)
	if err != nil {
		t.Fatal(err)
	}
	var p map[string]any
	if err := json.Unmarshal(out, &p); err != nil {
		t.Fatal(err)
	}
	if got := stringAt(p, "conversation_id"); got != wire {
		t.Fatalf("conversation_id=%q want %q", got, wire)
	}
	if got := stringAt(p, "metadata", "session_id"); got != wire {
		t.Fatalf("session_id=%q want %q", got, wire)
	}
	if got := stringAt(p, "prompt_cache_key"); got != wire {
		t.Fatalf("prompt_cache_key=%q want %q", got, wire)
	}
	if v, ok := p["store"]; !ok || v != false {
		t.Fatalf("store default must be false, got %v", p["store"])
	}
	// Explicit store preserved.
	rawExp, _ := json.Marshal(map[string]any{"model": "m", "store": true})
	outExp, err := applyRouteSessionToBody(rawExp, "rss_resp_wire_1", ProtocolResponses, false)
	if err != nil {
		t.Fatal(err)
	}
	var pExp map[string]any
	if err := json.Unmarshal(outExp, &pExp); err != nil {
		t.Fatal(err)
	}
	if v := pExp["store"]; v != true {
		t.Fatalf("explicit store must be preserved, got %v", v)
	}
	if got := stringAt(pExp, "prompt_cache_key"); got != wire {
		t.Fatalf("prompt_cache_key with explicit store=%q want %q", got, wire)
	}
	// Chat must not gain Responses defaults.
	rawChat, _ := json.Marshal(map[string]any{"model": "m", "conversation_id": "old"})
	outChat, err := applyRouteSessionToBody(rawChat, "rss_resp_wire_1", ProtocolChat, false)
	if err != nil {
		t.Fatal(err)
	}
	var pChat map[string]any
	if err := json.Unmarshal(outChat, &pChat); err != nil {
		t.Fatal(err)
	}
	if _, ok := pChat["prompt_cache_key"]; ok {
		t.Fatalf("chat must not add prompt_cache_key")
	}
	if _, ok := pChat["store"]; ok {
		t.Fatalf("chat must not add store")
	}
}

func TestReplayWireRotatesAndStaysFinal(t *testing.T) {
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	var sessions []string
	var bodies [][]byte
	calls := 0
	stubProxy(t, gateway, "a", 0, func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		sessions = append(sessions, r.Header.Get("x-opencode-session"))
		bodies = append(bodies, raw)
		// Generic affinity must be absent on real inference.
		if v := r.Header.Get("x-session-affinity"); v != "" {
			return responseWithBody(500, `{"error":"affinity leak"}`), nil
		}
		if v := r.Header.Get("X-Session-Id"); v != "" {
			return responseWithBody(500, `{"error":"affinity leak"}`), nil
		}
		calls++
		if calls == 1 {
			return responseWithBody(400, `{"error":"bad"}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})
	stubProxy(t, gateway, "a", 1, func(*http.Request) (*http.Response, error) {
		return responseWithBody(200, `{"ok":true}`), nil
	})
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ids := clientSessionIDs("ses_client_replay_wire_1")
	resp, _, attempts, err := gateway.doUpstreamTiers(ctx, anonAuthRoute(), routeBodiesWithSession(), ids, 0)
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatalf("status=%v want 200", resp)
	}
	drainAndClose(resp.Body)
	if attempts != 2 || len(sessions) != 2 {
		t.Fatalf("attempts=%d sessions=%d want 2/2", attempts, len(sessions))
	}
	for _, s := range sessions {
		if !isCanonicalWireSession(s) {
			t.Fatalf("replay wire must be canonical, got %q", s)
		}
	}
	if sessions[0] == sessions[1] {
		t.Fatalf("replay must rotate wire session")
	}
	for i, raw := range bodies {
		var p map[string]any
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Fatal(err)
		}
		if got := stringAt(p, "conversation_id"); got != sessions[i] {
			t.Fatalf("body[%d] must match wire header", i)
		}
	}
}

func TestBulkProbeOfficialIdentity(t *testing.T) {
	// Probe internal identity is deterministic and scheduler-neutral; wire
	// values derive only through the shared gateway mappers.
	idsA, idsB := bulkProbeIDs(), bulkProbeIDs()
	if idsA != idsB {
		t.Fatalf("probe identity must be deterministic: %+v vs %+v", idsA, idsB)
	}
	if !isCanonicalWireRequest(requestWireID(idsA.Request)) || !isCanonicalWireProject(projectWireID(idsA.Project)) {
		t.Fatalf("probe request/project must be canonical")
	}
	// Live header check via bulkProbeOnce: must equal the shared
	// newUpstreamRequest construction for the same inputs.
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	gw, err := NewGateway(cfg, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	seedBulkProbeCatalog(gw)
	var gotSes, gotReq, gotPrj, gotAffinity, gotSid, gotUA, gotAccept, gotClient string
	var gotBody []byte
	pool := gw.pools["shared"]
	pool.items[0].client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotSes = r.Header.Get("x-opencode-session")
		gotReq = r.Header.Get("x-opencode-request")
		gotPrj = r.Header.Get("x-opencode-project")
		gotAffinity = r.Header.Get("x-session-affinity")
		gotSid = r.Header.Get("X-Session-Id")
		gotUA = r.Header.Get("User-Agent")
		gotAccept = r.Header.Get("Accept")
		gotClient = r.Header.Get("x-opencode-client")
		gotBody, _ = io.ReadAll(r.Body)
		return responseWithBody(200, bulkChatSuccessBody("bulk-free-model")), nil
	})}
	pinsBefore := gw.scheduler.pins.count()
	sessBefore := gw.scheduler.routeSessions.count()
	tgt := bulkSendTarget{PoolName: "shared", Index: 0, Proxy: pool.items[0], Raw: pool.items[0].name, Tier: TierZen, CredKey: "public", CredID: anonymousSchedulerCredentialID, CredDisp: "anonymous", IsPublic: true, ProbeModel: "bulk-free-model", ProbeProtocol: ProtocolChat}
	res := gw.bulkProbeOnce(context.Background(), tgt)
	if !res.Success {
		t.Fatalf("probe must succeed: %+v", res)
	}
	ids := bulkProbeIDs()
	scope := bulkProbeScope(gw.cfg.Upstream.Zen, tgt, ProtocolChat)
	routeSession := deriveFirstRouteSession(ids.Session, scope)
	if want := routeWireSession(routeSession); !isCanonicalWireSession(gotSes) || gotSes != want {
		t.Fatalf("probe session header=%q want %q", gotSes, want)
	}
	if want := requestWireID(ids.Request); !isCanonicalWireRequest(gotReq) || gotReq != want {
		t.Fatalf("probe request header=%q want %q", gotReq, want)
	}
	if want := projectWireID(ids.Project); !isCanonicalWireProject(gotPrj) || gotPrj != want {
		t.Fatalf("probe project header=%q want %q", gotPrj, want)
	}
	// Shared construction parity: identical inputs through newUpstreamRequest
	// must yield identical headers.
	wantReq, err := newUpstreamRequest(context.Background(), gw.cfg.Upstream.Zen, ProtocolChat, gotBody, ids, tgt.CredKey, routeSession)
	if err != nil {
		t.Fatal(err)
	}
	if wantReq.Header.Get("x-opencode-session") != gotSes || wantReq.Header.Get("x-opencode-request") != gotReq || wantReq.Header.Get("x-opencode-project") != gotPrj {
		t.Fatalf("probe headers must match shared newUpstreamRequest")
	}
	if wantReq.Header.Get("User-Agent") != gotUA || wantReq.Header.Get("Accept") != gotAccept || wantReq.Header.Get("x-opencode-client") != gotClient {
		t.Fatalf("probe UA/Accept/client must match shared construction")
	}
	if wantReq.Header.Get("Authorization") != "Bearer "+tgt.CredKey {
		t.Fatalf("probe auth must match shared construction")
	}
	if gotAffinity != "" || gotSid != "" {
		t.Fatalf("probe must omit generic affinity: %q %q", gotAffinity, gotSid)
	}
	if gotUA != "opencode/1.18.31" {
		t.Fatalf("probe UA=%q want bare", gotUA)
	}
	if gw.scheduler.pins.count() != pinsBefore || gw.scheduler.routeSessions.count() != sessBefore {
		t.Fatalf("probe must stay scheduler-neutral")
	}
}
