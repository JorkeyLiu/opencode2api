package app

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// fallbackUnboundAuthGateway builds an auth-only gateway with two proxies in
// pool "z" and one custom fallback channel active. Retry is minimal to keep
// deterministic counts: L1 same-target is 1, so one send per candidate.
func fallbackUnboundAuthGateway(t *testing.T, customURL, active string) *Gateway {
	t.Helper()
	cfg := testGatewayConfig(
		map[string][]string{"z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-single-key"}
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.retryTransientIntervalPresent = true
	cfg.Fallback = FallbackConfig{
		Active: active,
		Channels: []FallbackChannelConfig{
			{Name: active, ID: active, BaseURL: customURL, APIKey: "k-unbound-waiter", Model: "cm-unbound"},
		},
	}
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), NewMonitor())
	if err != nil {
		t.Fatal(err)
	}
	return gw
}

// TestFallbackUnboundMissedEntryAdoptsCustom verifies the observable-window
// fallback recheck at doUnboundEstablishment loop-start.
//
// Proven: an unbound call that missed the doUpstreamTiers entry fallback.get
// (simulated deterministically by calling doUnboundEstablishment directly
// after the owner already bound fallback and released) must not send native
// and must serve exclusively via doCustomFallbackPinned with preserved
// route/crossing. The test asserts loop-start adoption: TierCustom, no extra
// native POST beyond owner's exhaustion, customHits grows by one, no new pin,
// same binding identity, and inflight/reserved == 0 with capacity intact.
//
// Not proven: the concurrent waiter queue window where B is already blocking
// on claim.done when A binds. That window has no natural deterministic sync
// point without a test hook/counter, so it is intentionally not claimed here.
// The production loop-start recheck covers both missed-entry and wakeup, but
// this test only exercises the missed-entry phase via direct
// doUnboundEstablishment after owner completion.
func TestFallbackUnboundMissedEntryAdoptsCustom(t *testing.T) {
	var customHits atomic.Int32
	customBodies := make(chan []byte, 4)
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customHits.Add(1)
		raw, _ := io.ReadAll(r.Body)
		customBodies <- raw
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-unbound")))
	}))
	defer custom.Close()

	gw := fallbackUnboundAuthGateway(t, custom.URL, "c-unbound")
	ses := "ses_unbound_missed_same"
	route := authOnlyRoute()
	route.KeyTiers = []Tier{TierZen}
	bodies := routeBodies()
	extra := upstreamExtra{
		External: ProtocolChat,
		Payload: map[string]any{
			"model": "m",
			"messages": []any{
				map[string]any{"role": "user", "content": "hi"},
			},
		},
	}

	var nativeCalls atomic.Int32
	postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		nativeCalls.Add(1)
		resp := responseWithBody(429, `{"error":"slow"}`)
		resp.Header.Set("Retry-After", "1")
		return resp, nil
	})
	postStub(t, gw, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		nativeCalls.Add(1)
		resp := responseWithBody(429, `{"error":"slow"}`)
		resp.Header.Set("Retry-After", "1")
		return resp, nil
	})

	// Owner establishes fallback via the normal entry path. This is sequential
	// and deterministic: no concurrency, no sleep.
	respOwner, effOwner, _, err := gw.doUpstreamTiers(pinTestCtx(), route, bodies, pinIDs(ses, "req-owner"), 0, extra)
	if err != nil || respOwner == nil || respOwner.StatusCode != 200 || effOwner.Tier != TierCustom {
		t.Fatalf("owner must succeed via custom: err=%v resp=%v eff=%+v", err, respOwner, effOwner)
	}
	drainResp(respOwner)
	if got := nativeCalls.Load(); got != 2 {
		t.Fatalf("owner must exhaust both native proxies: nativeCalls=%d want 2", got)
	}
	if got := customHits.Load(); got != 1 {
		t.Fatalf("owner customHits=%d want 1", got)
	}
	binding, ok := gw.scheduler.fallbacks.get(ses)
	if !ok {
		t.Fatalf("owner must bind fallback session")
	}
	if binding.Model != "cm-unbound" {
		t.Fatalf("binding model=%q want cm-unbound", binding.Model)
	}
	if _, ok := gw.scheduler.pinGet(ses, "m"); ok {
		t.Fatalf("custom takeover must not create a pin")
	}

	// B missed the entry fallback.get: simulate by calling
	// doUnboundEstablishment directly after owner release. Loop-start must
	// adopt the existing binding without any native send.
	respFollower, effFollower, _, err := gw.doUnboundEstablishment(pinTestCtx(), route, bodies, pinIDs(ses, "req-follower"), 0, extra)
	if err != nil || respFollower == nil || respFollower.StatusCode != 200 {
		t.Fatalf("follower must succeed via custom loop-start: err=%v resp=%v", err, respFollower)
	}
	if effFollower.Tier != TierCustom {
		t.Fatalf("follower must stay on custom, got %+v", effFollower)
	}
	drainResp(respFollower)

	if got := nativeCalls.Load(); got != 2 {
		t.Fatalf("follower must not send native: nativeCalls=%d want 2 (owner both proxies only)", got)
	}
	if got := customHits.Load(); got != 2 {
		t.Fatalf("customHits=%d want 2 (owner + follower via same binding)", got)
	}
	if _, ok := gw.scheduler.pinGet(ses, "m"); ok {
		t.Fatalf("missed-entry follower must not create a pin")
	}
	b2, ok2 := gw.scheduler.fallbacks.get(ses)
	if !ok2 || b2.ID != binding.ID || b2.Model != binding.Model {
		t.Fatalf("follower must adopt same custom binding: before %+v after %+v", binding, b2)
	}
	if n := gw.scheduler.pins.inflightCount(); n != 0 {
		t.Fatalf("claim leak inflight=%d", n)
	}
	if n := gw.scheduler.pins.reservedCount(); n != 0 {
		t.Fatalf("reservation leak reserved=%d", n)
	}
	// Prepared route/crossing preservation: custom bodies must carry channel model.
	select {
	case raw := <-customBodies:
		var p map[string]any
		_ = json.Unmarshal(raw, &p)
		if p["model"] != "cm-unbound" {
			t.Fatalf("owner custom model rewrite missing: %s", string(raw))
		}
		raw2 := <-customBodies
		var p2 map[string]any
		_ = json.Unmarshal(raw2, &p2)
		if p2["model"] != "cm-unbound" {
			t.Fatalf("follower custom model rewrite missing: %s", string(raw2))
		}
	default:
		t.Fatalf("expected 2 custom bodies, got none")
	}
}

// TestFallbackUnboundMissedEntryPendingStripsNativeRefs verifies the same
// missed-entry loop-start path when the owner's custom takeover failed and
// left the binding pending (Established=false). The follower still must
// adopt via doCustomFallbackPinned and keep the crossing semantics (strip
// native Responses refs). Deterministic sequential phases only.
//
// Proven: pending binding is observed at loop-start, follower does not send
// native, follower's custom send still strips previous_response_id and
// reasoning items via the pending crossing, inflight/reserved == 0, no pin,
// same session binding. Two custom sends total (owner failure + follower
// success) both stripped; follower 200 then establishes the binding.
//
// Not proven: concurrent waiter blocking on claim.done. See previous test's
// limitation — this is a missed-entry recheck test, not a waiter-queue test.
func TestFallbackUnboundMissedEntryPendingStripsNativeRefs(t *testing.T) {
	var customHits atomic.Int32
	customBodies := make(chan []byte, 4)
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := customHits.Add(1)
		raw, _ := io.ReadAll(r.Body)
		customBodies <- raw
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		// Strict pending gate: native-issued refs must be absent on every pending send.
		if v, _ := body["previous_response_id"].(string); v == pendingNativePrevID {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":{"message":"encrypted_content not issued"}}`))
			return
		}
		if items, ok := body["input"].([]any); ok {
			for _, it := range items {
				if m, _ := it.(map[string]any); m != nil && m["type"] == "reasoning" {
					if enc, _ := m["encrypted_content"].(string); enc == pendingNativeEnc {
						w.WriteHeader(400)
						_, _ = w.Write([]byte(`{"error":{"message":"encrypted_content not issued"}}`))
						return
					}
				}
			}
		}
		if n == 1 {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":{"message":"custom boom"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackResponsesOK("cm-pending-unbound")))
	}))
	defer custom.Close()

	cfg := testGatewayConfig(
		map[string][]string{"z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-single-key"}
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.retryTransientIntervalPresent = true
	cfg.Fallback = FallbackConfig{
		Active:   "c-resp-unbound",
		Channels: []FallbackChannelConfig{{ID: "c-resp-unbound", Name: "c-resp-unbound", BaseURL: custom.URL, APIKey: "k-resp", Model: "cm-pending-unbound", Protocol: ProtocolResponses}},
	}
	normalized, _ := NormalizeConfig("config.json", cfg)
	gw, _ := NewGateway(normalized, discardGatewayLogger(), NewMonitor())

	ses := "ses_unbound_pending_missed_1"
	route := modelRoute{ID: "m", Tier: TierZen, Protocol: ProtocolResponses, Protocols: map[Tier]Protocol{TierZen: ProtocolResponses}, Anonymous: false, KeyTiers: []Tier{TierZen}}
	bodies := routeBodies()
	extra := pendingNativePayload()

	var nativeCalls atomic.Int32
	postStub(t, gw, "z", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		nativeCalls.Add(1)
		r := responseWithBody(429, `{"error":"slow"}`)
		r.Header.Set("Retry-After", "1")
		return r, nil
	})
	postStub(t, gw, "z", 1, nil, nil, func(*http.Request) (*http.Response, error) {
		nativeCalls.Add(1)
		r := responseWithBody(429, `{"error":"slow"}`)
		r.Header.Set("Retry-After", "1")
		return r, nil
	})

	// Owner fails on custom but still binds pending. Sequential, no sleep.
	respOwner, effOwner, _, err := gw.doUpstreamTiers(pinTestCtx(), route, bodies, pinIDs(ses, "req-owner"), 0, extra)
	if err != nil {
		t.Fatalf("owner err=%v", err)
	}
	if respOwner == nil || respOwner.StatusCode != 500 {
		body, _ := io.ReadAll(respOwner.Body)
		if respOwner != nil {
			respOwner.Body.Close()
		}
		t.Fatalf("owner custom error must return 500, got %v %s eff=%+v", respOwner, string(body), effOwner)
	}
	rawOwner, _ := io.ReadAll(respOwner.Body)
	respOwner.Body.Close()
	_ = rawOwner
	if effOwner.Tier != TierCustom {
		t.Fatalf("owner must be TierCustom even on 500, got %+v", effOwner)
	}
	if got := nativeCalls.Load(); got != 2 {
		t.Fatalf("owner nativeCalls=%d want 2", got)
	}
	if got := customHits.Load(); got != 1 {
		t.Fatalf("owner customHits=%d want 1", got)
	}
	b, ok := gw.scheduler.fallbacks.get(ses)
	if !ok {
		t.Fatalf("failed takeover must still bind session")
	}
	if b.Established {
		t.Fatalf("binding must stay pending after custom 500")
	}
	raw1 := <-customBodies
	if bodyHasNativeRefs(t, raw1) {
		t.Fatalf("first pending custom must strip native refs")
	}

	// Follower missed entry: directly enter doUnboundEstablishment. Must still
	// cross (strip) and succeed. The second custom call should be the recovery
	// 200 that establishes the binding.
	respF, effF, _, err := gw.doUnboundEstablishment(pinTestCtx(), route, bodies, pinIDs(ses, "req-follower"), 0, extra)
	if err != nil {
		t.Fatalf("follower err=%v", err)
	}
	if respF == nil || respF.StatusCode != 200 || effF.Tier != TierCustom {
		body, _ := io.ReadAll(respF.Body)
		if respF != nil {
			respF.Body.Close()
		}
		t.Fatalf("pending follower must succeed on custom 200, got %v %s eff=%+v", respF, string(body), effF)
	}
	drainResp(respF)
	if got := nativeCalls.Load(); got != 2 {
		t.Fatalf("pending follower must not send native: calls=%d want 2", got)
	}
	if got := customHits.Load(); got != 2 {
		t.Fatalf("customHits=%d want 2", got)
	}
	raw2 := <-customBodies
	if bodyHasNativeRefs(t, raw2) {
		t.Fatalf("pending follower must still strip native refs")
	}
	if _, ok := gw.scheduler.pinGet(ses, "m"); ok {
		t.Fatalf("pending takeover must not create pin")
	}
	if n := gw.scheduler.pins.inflightCount(); n != 0 {
		t.Fatalf("inflight leak=%d want 0", n)
	}
	if n := gw.scheduler.pins.reservedCount(); n != 0 {
		t.Fatalf("reserved leak=%d want 0", n)
	}
	// After follower 2xx the binding must be established; further checks would
	// be in the full pending suite.
	b2, _ := gw.scheduler.fallbacks.get(ses)
	if !b2.Established {
		t.Fatalf("follower 2xx must establish the binding")
	}
}

// TestFallbackUnboundDifferentModelAdoptsSessionBinding verifies that fallback
// is session-keyed: after a session is bound to custom via model "m", a later
// unbound call for the same session but a different model "other" must still
// adopt the same session binding deterministically. This needs no timing
// trick: B enters doUnboundEstablishment after the bind is complete.
//
// Proven: session binding priority over model, loop-start adoption without
// native send, no pin for either model, same binding ID shared across models,
// inflight/reserved == 0.
//
// Not proven: the racy owner double-check window where two different-model
// owners race between loop-start and claim. That defensive second recheck
// remains in production but has no deterministic repro without a hook; this
// test covers the simpler sequential adoption case which is fully deterministic
// and complements the existing claim cap/cancel regression.
func TestFallbackUnboundDifferentModelAdoptsSessionBinding(t *testing.T) {
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		customHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm-diff-model")))
	}))
	defer custom.Close()

	cfg := testGatewayConfig(
		map[string][]string{"z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "z", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-single-key"}
	cfg.Retry.MaxAttempts = 1
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.retryTransientIntervalPresent = true
	cfg.Fallback = FallbackConfig{
		Active:   "c-diff",
		Channels: []FallbackChannelConfig{{ID: "c-diff", Name: "c-diff", BaseURL: custom.URL, APIKey: "k-diff", Model: "cm-diff-model"}},
	}
	normalized, _ := NormalizeConfig("config.json", cfg)
	gw, _ := NewGateway(normalized, discardGatewayLogger(), NewMonitor())

	ses := "ses_unbound_diff_model_det"
	routeM := authOnlyRoute() // ID "m"
	routeOther := authOnlyRoute()
	routeOther.ID = "other"
	bodiesM := map[Tier][]byte{TierZen: []byte(`{"model":"m"}`)}
	bodiesOther := map[Tier][]byte{TierZen: []byte(`{"model":"other"}`)}

	var nativeCalls atomic.Int32
	handler := func(*http.Request) (*http.Response, error) {
		nativeCalls.Add(1)
		r := responseWithBody(429, `{"error":"slow"}`)
		r.Header.Set("Retry-After", "1")
		return r, nil
	}
	postStub(t, gw, "z", 0, nil, nil, handler)
	postStub(t, gw, "z", 1, nil, nil, handler)

	extraM := upstreamExtra{
		External: ProtocolChat,
		Payload: map[string]any{
			"model": "m",
			"messages": []any{
				map[string]any{"role": "user", "content": "hi"},
			},
		},
	}
	respOwner, effOwner, _, err := gw.doUpstreamTiers(pinTestCtx(), routeM, bodiesM, pinIDs(ses, "req-owner-m"), 0, extraM)
	if err != nil || respOwner == nil || respOwner.StatusCode != 200 || effOwner.Tier != TierCustom {
		t.Fatalf("owner err=%v resp=%v eff=%+v want 200 custom", err, respOwner, effOwner)
	}
	drainResp(respOwner)
	if got := nativeCalls.Load(); got != 2 {
		t.Fatalf("owner nativeCalls=%d want 2", got)
	}
	if got := customHits.Load(); got != 1 {
		t.Fatalf("owner customHits=%d want 1", got)
	}
	binding, ok := gw.scheduler.fallbacks.get(ses)
	if !ok {
		t.Fatalf("owner must bind session")
	}

	// Deterministic sequential adoption for different model via
	// doUnboundEstablishment (missed-entry simulation). No concurrency.
	extraOther := upstreamExtra{
		External: ProtocolChat,
		Payload: map[string]any{
			"model":    "other",
			"messages": []any{map[string]any{"role": "user", "content": "hi"}},
		},
	}
	respOther, effOther, _, err := gw.doUnboundEstablishment(pinTestCtx(), routeOther, bodiesOther, pinIDs(ses, "req-other"), 0, extraOther)
	if err != nil || respOther == nil || respOther.StatusCode != 200 {
		t.Fatalf("different-model follower err=%v resp=%v want 200", err, respOther)
	}
	if effOther.Tier != TierCustom {
		t.Fatalf("different-model must adopt session fallback custom, got %+v", effOther)
	}
	drainResp(respOther)
	if got := nativeCalls.Load(); got != 2 {
		t.Fatalf("different-model must not send native: nativeCalls=%d want 2", got)
	}
	if got := customHits.Load(); got != 2 {
		t.Fatalf("customHits=%d want 2 (owner + different-model follower)", got)
	}
	if _, ok := gw.scheduler.pinGet(ses, "m"); ok {
		t.Fatalf("session fallback must not create pin for m")
	}
	if _, ok := gw.scheduler.pinGet(ses, "other"); ok {
		t.Fatalf("session fallback must not create pin for other")
	}
	b2, ok2 := gw.scheduler.fallbacks.get(ses)
	if !ok2 || b2.ID != binding.ID || b2.Model != binding.Model {
		t.Fatalf("different-model must adopt same session binding: before %+v after %+v", binding, b2)
	}
	if n := gw.scheduler.pins.inflightCount(); n != 0 {
		t.Fatalf("inflight leak=%d", n)
	}
	if n := gw.scheduler.pins.reservedCount(); n != 0 {
		t.Fatalf("reserved leak=%d", n)
	}
}
