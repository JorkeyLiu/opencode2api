package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

const (
	pendingNativePrevID = "resp-native-1"
	pendingNativeEnc    = "enc-native"
	pendingCustomPrevID = "resp-custom-1"
	pendingCustomEnc    = "enc-custom-1"
)

func pendingResponsesRoute() modelRoute {
	return modelRoute{ID: "m", Tier: TierZen, Protocol: ProtocolResponses, Protocols: map[Tier]Protocol{TierZen: ProtocolResponses}, Anonymous: true, KeyTiers: []Tier{TierZen}}
}

func pendingNativePayload() upstreamExtra {
	return upstreamExtra{External: ProtocolResponses, Payload: map[string]any{
		"model":                "m",
		"previous_response_id": pendingNativePrevID,
		"input": []any{
			map[string]any{"type": "message", "role": "user", "content": "hi"},
			map[string]any{"type": "reasoning", "id": "rs_native_1", "encrypted_content": pendingNativeEnc},
		},
	}}
}

func pendingCustomPayload() upstreamExtra {
	return upstreamExtra{External: ProtocolResponses, Payload: map[string]any{
		"model":                "m",
		"previous_response_id": pendingCustomPrevID,
		"input": []any{
			map[string]any{"type": "message", "role": "user", "content": "second"},
			map[string]any{"type": "reasoning", "id": "rs_custom_1", "encrypted_content": pendingCustomEnc},
			map[string]any{"type": "function_call_output", "call_id": "c9", "output": "ok"},
		},
	}}
}

func bodyHasNativeRefs(t *testing.T, raw []byte) bool {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("custom body must decode: %v", err)
	}
	if v, ok := body["previous_response_id"]; ok {
		if s, _ := v.(string); s == pendingNativePrevID {
			return true
		}
		// Any previous_response_id on a pending send carrying the native
		// payload is a leak; custom-issued IDs use a different value.
		if s, _ := v.(string); s != "" && s != pendingCustomPrevID {
			return true
		}
	}
	if items, ok := body["input"].([]any); ok {
		for _, item := range items {
			m, _ := item.(map[string]any)
			if m == nil {
				continue
			}
			if m["type"] == "reasoning" {
				if enc, _ := m["encrypted_content"].(string); enc == pendingNativeEnc {
					return true
				}
				// Any reasoning item surviving a pending send of the native
				// payload is a leak (pending must drop all of them).
				return true
			}
		}
	}
	return false
}

func bindResponsesAnonPin(t *testing.T, gw *Gateway, session string) {
	t.Helper()
	pool := gw.pools["a"]
	if pool == nil || len(pool.items) == 0 {
		t.Fatal("anon pool missing")
	}
	gw.bindSessionPin(session, "m", TierZen, anonymousSchedulerCredentialID, "a", pool.items[0].name, ProtocolResponses, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
}

// First custom 429 leaves the binding pending; the next pinned send must
// still cross (strip native refs) so a strict stub never sees them. The first
// 2xx then establishes the binding and custom refs are preserved after.
func TestFallbackPendingStaysCrossingAfterCustom429(t *testing.T) {
	var mu sync.Mutex
	var bodies [][]byte
	var calls atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" {
			w.WriteHeader(404)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, raw)
		mu.Unlock()
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		// Strict authority gate: only native-issued refs are rejected.
		// Custom-issued refs (pendingCustomPrevID/Enc) are accepted so the
		// established follow-up can prove they are preserved.
		if v, _ := body["previous_response_id"].(string); v == pendingNativePrevID {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":{"message":"encrypted_content was not issued to this caller"}}`))
			return
		}
		if items, ok := body["input"].([]any); ok {
			for _, item := range items {
				m, _ := item.(map[string]any)
				if m != nil && m["type"] == "reasoning" {
					if enc, _ := m["encrypted_content"].(string); enc == pendingNativeEnc {
						w.WriteHeader(400)
						_, _ = w.Write([]byte(`{"error":{"message":"encrypted_content was not issued to this caller"}}`))
						return
					}
				}
			}
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"error":{"message":"slow"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackResponsesOK("cm-pending")))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{ID: "c-resp-pending", Name: "c-resp-pending", BaseURL: custom.URL, APIKey: "k-pending", Model: "cm-pending", Protocol: ProtocolResponses}}, "c-resp-pending")
	ses := "ses_pending_429_1"
	bindResponsesAnonPin(t, gw, ses)
	var anonHits atomic.Int32
	postStub(t, gw, "a", 0, &anonHits, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	route := pendingResponsesRoute()

	resp1, _, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r-p1"), 0, pendingNativePayload())
	if err != nil {
		t.Fatal(err)
	}
	raw1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	if resp1.StatusCode != 429 {
		t.Fatalf("first custom 429 must return as-is, got %d %s", resp1.StatusCode, string(raw1))
	}
	b, ok := gw.scheduler.fallbacks.get(ses)
	if !ok {
		t.Fatal("failed takeover must still bind session")
	}
	if b.Established {
		t.Fatal("binding must stay pending after custom 429")
	}
	if anonHits.Load() != 1 {
		t.Fatalf("first request must touch native once, anon=%d", anonHits.Load())
	}

	// Second request: same native payload on the pending binding. Must still
	// cross (no native refs on the wire) and this 200 establishes it.
	resp2, eff2, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r-p2"), 0, pendingNativePayload())
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp2)
	if resp2.StatusCode != 200 || eff2.Tier != TierCustom {
		t.Fatalf("pending follow-up must succeed on custom, got %d %+v", resp2.StatusCode, eff2)
	}
	if anonHits.Load() != 1 {
		t.Fatalf("pending follow-up must not re-enter native, anon=%d", anonHits.Load())
	}
	mu.Lock()
	if len(bodies) != 2 {
		mu.Unlock()
		t.Fatalf("custom sends=%d want 2", len(bodies))
	}
	second := append([]byte(nil), bodies[1]...)
	mu.Unlock()
	var secondBody map[string]any
	if err := json.Unmarshal(second, &secondBody); err != nil {
		t.Fatal(err)
	}
	if _, ok := secondBody["previous_response_id"]; ok {
		t.Fatalf("pending follow-up must strip previous_response_id, got %v", secondBody["previous_response_id"])
	}
	if items, ok := secondBody["input"].([]any); ok {
		for _, item := range items {
			if m, _ := item.(map[string]any); m != nil && m["type"] == "reasoning" {
				t.Fatalf("pending follow-up must drop reasoning items, got %v", m)
			}
		}
	}
	b2, _ := gw.scheduler.fallbacks.get(ses)
	if !b2.Established {
		t.Fatal("first custom 2xx must establish the binding")
	}

	// Third request: custom-issued refs on the established binding stay intact.
	resp3, _, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r-p3"), 0, pendingCustomPayload())
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp3)
	if resp3.StatusCode != 200 {
		t.Fatalf("established follow-up status=%d want 200", resp3.StatusCode)
	}
	mu.Lock()
	if len(bodies) != 3 {
		mu.Unlock()
		t.Fatalf("custom sends=%d want 3", len(bodies))
	}
	third := append([]byte(nil), bodies[2]...)
	mu.Unlock()
	var thirdBody map[string]any
	if err := json.Unmarshal(third, &thirdBody); err != nil {
		t.Fatal(err)
	}
	if thirdBody["previous_response_id"] != pendingCustomPrevID {
		t.Fatalf("established send must keep custom previous_response_id, got %v", thirdBody["previous_response_id"])
	}
	found := false
	if items, ok := thirdBody["input"].([]any); ok {
		for _, item := range items {
			if m, _ := item.(map[string]any); m != nil && m["type"] == "reasoning" {
				found = true
				if m["encrypted_content"] != pendingCustomEnc {
					t.Fatalf("custom reasoning must stay byte-identical, got %v", m)
				}
			}
		}
	}
	if !found {
		t.Fatal("established send must preserve the custom reasoning item")
	}
}

// First custom 400/500 also leaves the binding pending: the error returns
// as-is, native is never retried, and the next send still crosses.
func TestFallbackPendingStaysCrossingAfterCustom400And500(t *testing.T) {
	for _, status := range []int{400, 500} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var mu sync.Mutex
			var bodies [][]byte
			first := true
			custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				mu.Lock()
				bodies = append(bodies, raw)
				mu.Unlock()
				if first {
					first = false
					w.WriteHeader(status)
					_, _ = w.Write([]byte(`{"error":{"message":"custom-first-failure"}}`))
					return
				}
				var body map[string]any
				_ = json.Unmarshal(raw, &body)
				if v, _ := body["previous_response_id"].(string); v == pendingNativePrevID {
					w.WriteHeader(400)
					_, _ = w.Write([]byte(`{"error":{"message":"encrypted_content was not issued to this caller"}}`))
					return
				}
				if items, ok := body["input"].([]any); ok {
					for _, item := range items {
						if m, _ := item.(map[string]any); m != nil && m["type"] == "reasoning" {
							w.WriteHeader(400)
							_, _ = w.Write([]byte(`{"error":{"message":"encrypted_content was not issued to this caller"}}`))
							return
						}
					}
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)
				_, _ = w.Write([]byte(fallbackResponsesOK("cm-recover")))
			}))
			defer custom.Close()
			gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{ID: "c-pend", Name: "c-pend", BaseURL: custom.URL, APIKey: "k", Model: "cm-recover", Protocol: ProtocolResponses}}, "c-pend")
			ses := "ses_pending_fail_" + strings.ReplaceAll(strings.ToLower(http.StatusText(status)), " ", "_")
			bindResponsesAnonPin(t, gw, ses)
			var anonHits atomic.Int32
			postStub(t, gw, "a", 0, &anonHits, nil, func(*http.Request) (*http.Response, error) {
				return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
			})
			route := pendingResponsesRoute()
			resp1, _, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r-f1"), 0, pendingNativePayload())
			if err != nil {
				t.Fatal(err)
			}
			raw1, _ := io.ReadAll(resp1.Body)
			resp1.Body.Close()
			if resp1.StatusCode != status {
				t.Fatalf("custom %d must return as-is, got %d %s", status, resp1.StatusCode, string(raw1))
			}
			if !strings.Contains(string(raw1), "custom-first-failure") {
				t.Fatalf("custom error body must pass through, got %s", string(raw1))
			}
			if b, ok := gw.scheduler.fallbacks.get(ses); !ok || b.Established {
				t.Fatalf("binding must exist and stay pending after custom %d: %+v %v", status, b, ok)
			}
			mu.Lock()
			if len(bodies) != 1 {
				mu.Unlock()
				t.Fatalf("custom sends=%d want 1", len(bodies))
			}
			firstBody := append([]byte(nil), bodies[0]...)
			mu.Unlock()
			if bodyHasNativeRefs(t, firstBody) {
				t.Fatal("first custom send must already strip native refs")
			}
			resp2, eff2, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r-f2"), 0, pendingNativePayload())
			if err != nil {
				t.Fatal(err)
			}
			defer drainResp(resp2)
			if resp2.StatusCode != 200 || eff2.Tier != TierCustom {
				t.Fatalf("pending follow-up after %d must succeed on custom, got %d %+v", status, resp2.StatusCode, eff2)
			}
			if anonHits.Load() != 1 {
				t.Fatalf("pending follow-up must not re-enter native, anon=%d", anonHits.Load())
			}
			mu.Lock()
			second := append([]byte(nil), bodies[1]...)
			mu.Unlock()
			if bodyHasNativeRefs(t, second) {
				t.Fatalf("pending follow-up after %d must still strip native refs", status)
			}
			if b, _ := gw.scheduler.fallbacks.get(ses); !b.Established {
				t.Fatal("recovery 2xx must establish the binding")
			}
		})
	}
}

type captureFailTransport struct {
	mu     sync.Mutex
	bodies [][]byte
	failed atomic.Bool
	target http.RoundTripper
}

func (c *captureFailTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	var raw []byte
	if r.Body != nil {
		raw, _ = io.ReadAll(r.Body)
	}
	c.mu.Lock()
	c.bodies = append(c.bodies, raw)
	c.mu.Unlock()
	if c.failed.CompareAndSwap(false, true) {
		return nil, errors.New("dial tcp: connection refused")
	}
	// Restore the consumed body so the forwarded send carries the same bytes.
	if raw != nil {
		r.Body = io.NopCloser(bytes.NewReader(raw))
		r.ContentLength = int64(len(raw))
	}
	return c.target.RoundTrip(r)
}

// Transport failure on the first custom send also stays pending: the error
// returns (no native retry) and the next send still crosses.
func TestFallbackPendingStaysCrossingAfterTransportError(t *testing.T) {
	var bodies [][]byte
	var mu sync.Mutex
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, raw)
		mu.Unlock()
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		if v, _ := body["previous_response_id"].(string); v == pendingNativePrevID {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":{"message":"encrypted_content was not issued to this caller"}}`))
			return
		}
		if items, ok := body["input"].([]any); ok {
			for _, item := range items {
				if m, _ := item.(map[string]any); m != nil && m["type"] == "reasoning" {
					if enc, _ := m["encrypted_content"].(string); enc == pendingNativeEnc {
						w.WriteHeader(400)
						_, _ = w.Write([]byte(`{"error":{"message":"encrypted_content was not issued to this caller"}}`))
						return
					}
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackResponsesOK("cm-transport")))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{ID: "c-tr", Name: "c-tr", BaseURL: custom.URL, APIKey: "k", Model: "cm-transport", Protocol: ProtocolResponses}}, "c-tr")
	ses := "ses_pending_transport_1"
	bindResponsesAnonPin(t, gw, ses)
	var anonHits atomic.Int32
	postStub(t, gw, "a", 0, &anonHits, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	cap := &captureFailTransport{target: http.DefaultTransport}
	gw.customClient = &http.Client{Transport: cap}
	route := pendingResponsesRoute()
	resp1, eff1, _, err1 := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r-t1"), 0, pendingNativePayload())
	if err1 == nil {
		if resp1 != nil {
			drainResp(resp1)
		}
		t.Fatal("transport failure must return an error")
	}
	if resp1 != nil {
		drainResp(resp1)
		t.Fatal("transport error must not return a response alongside the error")
	}
	if eff1.Tier != TierCustom {
		t.Fatalf("transport route must stay custom, got %+v", eff1)
	}
	if b, ok := gw.scheduler.fallbacks.get(ses); !ok || b.Established {
		t.Fatalf("binding must stay pending after transport error: %+v %v", b, ok)
	}
	cap.mu.Lock()
	if len(cap.bodies) != 1 {
		cap.mu.Unlock()
		t.Fatalf("transport sends=%d want 1", len(cap.bodies))
	}
	firstBody := append([]byte(nil), cap.bodies[0]...)
	cap.mu.Unlock()
	if bodyHasNativeRefs(t, firstBody) {
		t.Fatal("transport-failed first send must already strip native refs")
	}
	// Second request goes through the real server via the same transport.
	resp2, _, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r-t2"), 0, pendingNativePayload())
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp2)
	if resp2.StatusCode != 200 {
		t.Fatalf("pending follow-up after transport must succeed, got %d", resp2.StatusCode)
	}
	if anonHits.Load() != 1 {
		t.Fatalf("pending follow-up must not re-enter native, anon=%d", anonHits.Load())
	}
	mu.Lock()
	if len(bodies) != 1 {
		mu.Unlock()
		t.Fatalf("server sends=%d want 1", len(bodies))
	}
	second := append([]byte(nil), bodies[0]...)
	mu.Unlock()
	if bodyHasNativeRefs(t, second) {
		t.Fatal("pending follow-up after transport must still strip native refs")
	}
	if b, _ := gw.scheduler.fallbacks.get(ses); !b.Established {
		t.Fatal("recovery 2xx must establish the binding")
	}
}

// Concurrent pending takeovers stay first-wins on one channel and every
// pending send crosses; capacity still fails closed without recording.
func TestFallbackPendingConcurrentFirstWinsAndCapacity(t *testing.T) {
	var mu sync.Mutex
	var bodies [][]byte
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, raw)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackResponsesOK("cm-conc")))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{ID: "c-conc", Name: "c-conc", BaseURL: custom.URL, APIKey: "k", Model: "cm-conc", Protocol: ProtocolResponses}}, "c-conc")
	ses := "ses_pending_conc_1"
	bindResponsesAnonPin(t, gw, ses)
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	route := pendingResponsesRoute()
	const workers = 8
	var wg sync.WaitGroup
	errs := make([]error, workers)
	codes := make([]int, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp, eff, _, err := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r-conc"), 0, pendingNativePayload())
			errs[idx] = err
			if err == nil && resp != nil {
				codes[idx] = resp.StatusCode
				drainResp(resp)
				if eff.Tier != TierCustom {
					errs[idx] = errors.New("route left custom")
				}
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Fatalf("worker %d err=%v", i, errs[i])
		}
		if codes[i] != 200 {
			t.Fatalf("worker %d status=%d want 200", i, codes[i])
		}
	}
	if n := gw.scheduler.fallbacks.count(); n != 1 {
		t.Fatalf("concurrent takeovers must record one binding, got %d", n)
	}
	b, ok := gw.scheduler.fallbacks.get(ses)
	if !ok || b.ID != "c-conc" || !b.Established {
		t.Fatalf("concurrent binding must be established on c-conc: %+v %v", b, ok)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) == 0 {
		t.Fatal("custom must receive concurrent sends")
	}
	for i, raw := range bodies {
		var body map[string]any
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatalf("body %d must decode: %v", i, err)
		}
		if _, has := body["previous_response_id"]; has {
			t.Fatalf("concurrent pending send %d must strip previous_response_id", i)
		}
		if items, ok := body["input"].([]any); ok {
			for _, item := range items {
				if m, _ := item.(map[string]any); m != nil && m["type"] == "reasoning" {
					t.Fatalf("concurrent pending send %d must drop reasoning items", i)
				}
			}
		}
		if body["model"] != "cm-conc" {
			t.Fatalf("concurrent send %d model=%v", i, body["model"])
		}
	}

	// Capacity still fails closed: a full store refuses the overflow session
	// without recording it.
	full := newFallbackTakeoverStore()
	for i := 0; i < fallbackTakeoverStoreCap; i++ {
		key := "cap-" + fallbackItoa(i)
		if _, _, isFull := full.bind(key, fallbackBinding{ID: "c", Name: "c", BaseURL: "https://a.example", KeyHash: "h", Model: "m"}); isFull {
			t.Fatal("should not be full before cap")
		}
	}
	if _, _, isFull := full.bind("overflow", fallbackBinding{ID: "c", Name: "c", BaseURL: "https://a.example", KeyHash: "h", Model: "m"}); !isFull {
		t.Fatal("overflow must report full")
	}
	if _, ok := full.get("overflow"); ok {
		t.Fatal("overflow must not be recorded")
	}
}
