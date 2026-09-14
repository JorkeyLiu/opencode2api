package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestCacheHitMissDerivation(t *testing.T) {
	cases := []struct {
		name     string
		usage    bridgeUsage
		wantHit  int
		wantMiss int
	}{
		{"ordinary", bridgeUsage{Input: 100, Output: 20, Cached: 30}, 30, 70},
		{"no_cache", bridgeUsage{Input: 50, Output: 10}, 0, 50},
		{"all_cached", bridgeUsage{Input: 40, Output: 5, Cached: 40}, 40, 0},
		{"cached_exceeds_input_clamp", bridgeUsage{Input: 10, Output: 5, Cached: 30}, 30, 0},
		{"negative_clamp", bridgeUsage{Input: -5, Output: -1, Cached: -3}, 0, 0},
		{"with_creation_bundled", bridgeUsage{Input: 120, Output: 10, Cached: 30, CacheCreation: 50}, 30, 90},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hit, miss := cacheHitMiss(tc.usage)
			if hit != tc.wantHit || miss != tc.wantMiss {
				t.Fatalf("hit=%d miss=%d want hit=%d miss=%d", hit, miss, tc.wantHit, tc.wantMiss)
			}
			// Miss bundles creation/write: ordinary uncached cannot be split out.
			if tc.usage.CacheCreation != 0 && miss != max(tc.usage.Input-tc.usage.Cached, 0) {
				t.Fatalf("miss must bundle creation: %+v", tc.usage)
			}
		})
	}
}

func TestMonitorRecordPlainUsagePropagation(t *testing.T) {
	m := NewMonitor()
	// Same-protocol plain body via the existing generic decoder.
	body := []byte(`{"id":"x","model":"m","choices":[],"usage":{"prompt_tokens":100,"completion_tokens":20,"total_tokens":120,"prompt_tokens_details":{"cached_tokens":30}}}`)
	usage, reported := extractResponseUsage(ProtocolChat, body)
	if !reported {
		t.Fatalf("plain chat usage must report")
	}
	meta := &requestMeta{
		Model: "m", Tier: "zen", Protocol: "chat", ClientSessionHash: clientSessionHash("ses_plain_u1"),
		Request: "req-plain-u1", Channel: "key", KeyID: "KKKKK", Attempts: 1,
		Usage: usage, UsageReported: reported,
	}
	m.Record("/v1/chat/completions", 200, 5*time.Millisecond, meta)
	snap := m.Snapshot()
	var found *UpstreamRequest
	for i, r := range snap.Upstream.Requests {
		if r.RequestID == "req-plain-u1" {
			found = &snap.Upstream.Requests[i]
		}
	}
	if found == nil {
		t.Fatalf("plain request missing")
	}
	if !found.UsageReported {
		t.Fatalf("plain request must be usage_reported: %+v", found)
	}
	if found.InputTokens != 100 || found.OutputTokens != 20 {
		t.Fatalf("plain input/output=%d/%d want 100/20: %+v", found.InputTokens, found.OutputTokens, found)
	}
	if found.CacheHitTokens != 30 || found.CacheMissTokens != 70 {
		t.Fatalf("plain hit/miss=%d/%d want 30/70: %+v", found.CacheHitTokens, found.CacheMissTokens, found)
	}
	// Cross-protocol plain body via the existing generic anthropic decoder.
	abody := []byte(`{"id":"x","model":"m","usage":{"input_tokens":60,"cache_read_input_tokens":15,"cache_creation_input_tokens":25,"output_tokens":10}}`)
	ausage, areported := extractResponseUsage(ProtocolAnthropic, abody)
	if !areported {
		t.Fatalf("plain anthropic usage must report")
	}
	// Anthropic decoder totals input as 60+15+25=100; hit=15, miss=85 (bundles creation).
	if ausage.Input != 100 || ausage.Cached != 15 || ausage.CacheCreation != 25 {
		t.Fatalf("anthropic decode=%+v", ausage)
	}
	ahit, amiss := cacheHitMiss(ausage)
	if ahit != 15 || amiss != 85 {
		t.Fatalf("anthropic hit/miss=%d/%d want 15/85", ahit, amiss)
	}

	// Failed 400 without upstream usage remains unknown.
	m.Record("/v1/chat/completions", 400, time.Millisecond, &requestMeta{
		Model: "m", Tier: "zen", Protocol: "chat", ClientSessionHash: clientSessionHash("ses_plain_u2"),
		Request: "req-plain-400", Channel: "key", KeyID: "KKKKK", Attempts: 1,
	})
	snap2 := m.Snapshot()
	for _, r := range snap2.Upstream.Requests {
		if r.RequestID == "req-plain-400" {
			if r.UsageReported {
				t.Fatalf("400 without usage must stay unknown: %+v", r)
			}
			data, _ := json.Marshal(r)
			if strings.Contains(string(data), "cache_hit_tokens") || strings.Contains(string(data), "cache_miss_tokens") {
				t.Fatalf("unknown usage must omit cache fields: %s", data)
			}
			if strings.Contains(string(data), `"input_tokens"`) {
				t.Fatalf("unknown usage must omit input tokens: %s", data)
			}
		}
	}
}

func TestMonitorRecordStreamingUsagePropagation(t *testing.T) {
	// Streaming path via the existing generic SSE decoder (chat usage frame).
	observer := newStreamUsageObserver(ProtocolChat)
	frame := "data: {\"id\":\"chatcmpl-x\",\"choices\":[],\"usage\":{\"prompt_tokens\":80,\"completion_tokens\":10,\"total_tokens\":90,\"prompt_tokens_details\":{\"cached_tokens\":50}}}\n\n"
	if _, err := observer.Write([]byte(frame)); err != nil {
		t.Fatal(err)
	}
	usage := observer.Finish()
	if !observer.Reported() {
		t.Fatalf("streaming usage must report")
	}
	hit, miss := cacheHitMiss(usage)
	if hit != 50 || miss != 30 {
		t.Fatalf("stream hit/miss=%d/%d want 50/30: %+v", hit, miss, usage)
	}
	m := NewMonitor()
	m.Record("/v1/chat/completions", 200, 5*time.Millisecond, &requestMeta{
		Model: "m", Tier: "zen", Protocol: "chat", ClientSessionHash: clientSessionHash("ses_stream_u1"),
		Request: "req-stream-u1", Channel: "key", KeyID: "KKKKK", Attempts: 1, Stream: true,
		Usage: usage, UsageReported: observer.Reported(),
	})
	snap := m.Snapshot()
	found := false
	for _, r := range snap.Upstream.Requests {
		if r.RequestID == "req-stream-u1" {
			found = true
			if !r.UsageReported || r.CacheHitTokens != 50 || r.CacheMissTokens != 30 || r.InputTokens != 80 || r.OutputTokens != 10 {
				t.Fatalf("stream request propagation failed: %+v", r)
			}
		}
	}
	if !found {
		t.Fatalf("stream request missing")
	}
	// Attempts never receive request-final usage.
	m.RecordAttempt(UpstreamAttempt{
		Time: time.Now().UTC(), RequestID: "req-stream-u1", Model: "m", Tier: "zen", Protocol: "chat",
		Attempt: 1, KeyID: "KKKKK", Channel: "key", Proxy: "direct", Status: 200, DurationMS: 5,
		Success: true, FailureClass: AttemptClassSuccess,
	})
	snap2 := m.Snapshot()
	if len(snap2.Upstream.Recent) == 0 {
		t.Fatalf("attempt missing")
	}
	data, _ := json.Marshal(snap2.Upstream.Recent[len(snap2.Upstream.Recent)-1])
	if strings.Contains(string(data), "cache_hit_tokens") || strings.Contains(string(data), "usage_reported") {
		t.Fatalf("attempt must not carry request-final usage: %s", data)
	}
}

func TestHistoryCacheCompatibilityAndRedaction(t *testing.T) {
	now := time.Now().UTC()
	// New request line round-trips additive usage fields.
	req := UpstreamRequest{
		Time: now, RequestID: "hr-cache-1", Model: "m", Tier: "zen", Protocol: "chat",
		ClientSessionHash: clientSessionHash("ses_hist_cache"), KeyID: "sk-live-99999", Channel: "key",
		Proxy: "http://user:hunter2@127.0.0.1:8080", ProxyPool: "z",
		Attempts: 1, Status: 200, DurationMS: 1, Success: true, Outcome: "success",
		UsageReported: true, InputTokens: 100, OutputTokens: 20, CacheHitTokens: 30, CacheMissTokens: 70,
	}
	line := historyRequestLine{
		V: historySchemaV, Kind: string(historyKindRequest), Time: req.Time.UTC().Format(time.RFC3339Nano),
		RequestID: req.RequestID, Model: req.Model, Tier: req.Tier, Protocol: req.Protocol,
		ClientSessionHash: req.ClientSessionHash,
		KeyID:             historyKeySuffix(req.KeyID, req.Anonymous), Channel: req.Channel, Anonymous: req.Anonymous,
		ProxyPool: req.ProxyPool, ProxyNode: redactURL(req.Proxy),
		Attempts: req.Attempts, Status: req.Status, DurationMS: req.DurationMS,
		Success: req.Success, Outcome: req.Outcome,
		UsageReported: req.UsageReported, InputTokens: req.InputTokens, OutputTokens: req.OutputTokens,
		CacheHitTokens: req.CacheHitTokens, CacheMissTokens: req.CacheMissTokens,
	}
	// F1: key/proxy redaction readback on the persisted projection.
	if line.KeyID != "99999" {
		t.Fatalf("history key must persist suffix only, got %q", line.KeyID)
	}
	if strings.Contains(line.ProxyNode, "hunter2") || strings.Contains(line.ProxyNode, "user@") {
		t.Fatalf("history proxy leaks credentials: %q", line.ProxyNode)
	}
	if strings.Contains(line.ClientSessionHash, "ses_hist_cache") {
		t.Fatalf("history hash leaks raw session: %q", line.ClientSessionHash)
	}
	data, _ := json.Marshal(line)
	var decoded historyRequestLine
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.UsageReported || decoded.CacheHitTokens != 30 || decoded.CacheMissTokens != 70 || decoded.InputTokens != 100 || decoded.OutputTokens != 20 {
		t.Fatalf("history usage round-trip failed: %+v", decoded)
	}
	// Old lines (no usage fields) decode as unknown, not zero-as-known.
	var old historyRequestLine
	if err := json.Unmarshal([]byte(`{"v":1,"kind":"request","time":"`+now.UTC().Format(time.RFC3339Nano)+`","request_id":"old-cache","model":"m","channel":"key","attempts":1,"status":200,"duration_ms":1,"success":true}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.UsageReported || old.CacheHitTokens != 0 || old.CacheMissTokens != 0 {
		t.Fatalf("old line must be unknown, got %+v", old)
	}
	// New line ignored by old readers.
	type oldRequestReader struct {
		V         int    `json:"v"`
		RequestID string `json:"request_id"`
		Model     string `json:"model"`
	}
	var minimal oldRequestReader
	if err := json.Unmarshal(data, &minimal); err != nil {
		t.Fatal(err)
	}
	if minimal.RequestID != "hr-cache-1" || minimal.Model != "m" {
		t.Fatalf("old reader failed: %+v", minimal)
	}
	// Unknown usage omits cache fields on the wire.
	unknown := UpstreamRequest{RequestID: "r", Model: "m", Channel: "not_routed", Attempts: 0, Status: 400, Success: false, Outcome: "not_routed"}
	udata, _ := json.Marshal(unknown)
	if strings.Contains(string(udata), "cache_hit_tokens") || strings.Contains(string(udata), "cache_miss_tokens") {
		t.Fatalf("unknown must omit cache fields: %s", udata)
	}
}

func TestNon400AndTransientRetryReplayFlagsFalse(t *testing.T) {
	// F3: ordinary non-400 rejection and transient same-target retry never
	// set route-session replay flags.
	monitor := NewMonitor()
	gateway := routing400Gateway(t, monitor)
	stubProxy(t, gateway, "z", 0, func(*http.Request) (*http.Response, error) {
		return responseWithBody(404, `{"error":"nope"}`), nil
	})
	route := authOnlyRoute()
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{})
	ids := clientSessionIDs("ses_diag_f3_404")
	resp, _, err := gateway.doUpstream(ctx, route, routeBodies(), ids)
	if err != nil {
		t.Fatal(err)
	}
	drainAndClose(resp.Body)
	recent := monitor.Snapshot().Upstream.Recent
	if len(recent) == 0 {
		t.Fatalf("no attempts")
	}
	for _, a := range recent {
		if a.RouteSessionReplay || a.DroppedPreviousResponseID || a.DroppedReasoningRefs {
			t.Fatalf("ordinary 404 must leave replay flags false: %+v", a)
		}
	}

	// Transient same-target retry (500 then 200): retry is not a 400 replay.
	monitor2 := NewMonitor()
	gateway2 := routing400Gateway(t, monitor2)
	calls := 0
	stubProxy(t, gateway2, "z", 0, func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return responseWithBody(500, `{"error":"boom"}`), nil
		}
		return responseWithBody(200, `{"ok":true}`), nil
	})
	// Force a single-candidate tier so the 500 stays a same-target transient retry path.
	single := modelRoute{
		ID: "m", Tier: TierZen, Protocol: ProtocolChat,
		Protocols: map[Tier]Protocol{TierZen: ProtocolChat},
		Anonymous: false, KeyTiers: []Tier{TierZen},
	}
	singleBodies := map[Tier][]byte{TierZen: []byte(`{"model":"m"}`)}
	resp2, _, err := gateway2.doUpstream(context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{}), single, singleBodies, clientSessionIDs("ses_diag_f3_transient"))
	if err != nil {
		t.Fatal(err)
	}
	drainAndClose(resp2.Body)
	for _, a := range monitor2.Snapshot().Upstream.Recent {
		if a.RouteSessionReplay || a.DroppedPreviousResponseID || a.DroppedReasoningRefs {
			t.Fatalf("transient retry must leave replay flags false: %+v", a)
		}
	}
}
