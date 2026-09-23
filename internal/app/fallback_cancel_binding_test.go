package app

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
)

// Store-level ctx-aware bind/markEstablished: pre-cancel never writes, normal
// first-wins/cap/establish preserved, legacy APIs unchanged.
func TestFallbackTakeoverStoreContextAware(t *testing.T) {
	// Pre-cancelled bind creates nothing.
	st := newFallbackTakeoverStore()
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	b := fallbackBinding{ID: "c", Name: "c", BaseURL: "https://a.example", KeyHash: "h", Model: "m"}
	stored, inserted, full, cancelled := st.bindContext(cancelCtx, "s-cancel", b)
	if !cancelled || inserted || full {
		t.Fatalf("pre-cancel bind must report cancelled only: inserted=%v full=%v cancelled=%v", inserted, full, cancelled)
	}
	_ = stored
	if _, ok := st.get("s-cancel"); ok {
		t.Fatal("pre-cancel bind must not record a pending entry")
	}
	if n := st.count(); n != 0 {
		t.Fatalf("pre-cancel bind must leave store empty, got %d", n)
	}

	// Live bind inserts pending.
	live := context.Background()
	stored, inserted, full, cancelled = st.bindContext(live, "s-cancel", b)
	if cancelled || !inserted || full {
		t.Fatalf("live bind must insert: inserted=%v full=%v cancelled=%v", inserted, full, cancelled)
	}
	if got, ok := st.get("s-cancel"); !ok || got.Established {
		t.Fatalf("live bind must leave pending: %+v %v", got, ok)
	}

	// First-wins even under cancellation: existing winner returned, not cancelled/capacity.
	cancelCtx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	other := fallbackBinding{ID: "other", Name: "other", BaseURL: "https://b.example", KeyHash: "h2", Model: "m2"}
	got, inserted, full, cancelled := st.bindContext(cancelCtx2, "s-cancel", other)
	if cancelled || inserted || full {
		t.Fatalf("existing binding under cancel must not report cancelled/insert/full: %v %v %v", inserted, full, cancelled)
	}
	if got.ID != "c" {
		t.Fatalf("first-wins violated under cancel: %+v", got)
	}

	// Pre-cancelled markEstablished never flips pending.
	if flipped := st.markEstablishedContext(cancelCtx2, "s-cancel"); flipped {
		t.Fatal("pre-cancel markEstablished must not flip")
	}
	if got, _ := st.get("s-cancel"); got.Established {
		t.Fatal("pending must stay pending after cancelled mark")
	}
	// Live mark flips once, idempotent after.
	if flipped := st.markEstablishedContext(live, "s-cancel"); !flipped {
		t.Fatal("live markEstablished must flip pending")
	}
	if got, _ := st.get("s-cancel"); !got.Established {
		t.Fatal("binding must be established after live mark")
	}
	if flipped := st.markEstablishedContext(live, "s-cancel"); flipped {
		t.Fatal("second mark on established must report false")
	}
	// Unknown session never created by either mark path.
	st2 := newFallbackTakeoverStore()
	if flipped := st2.markEstablishedContext(live, "missing"); flipped {
		t.Fatal("unknown session mark must not flip")
	}
	if _, ok := st2.get("missing"); ok {
		t.Fatal("unknown session mark must not create an entry")
	}
	st2.markEstablished("missing")
	if _, ok := st2.get("missing"); ok {
		t.Fatal("legacy mark must not create an entry")
	}

	// Capacity preserved for ctx-aware bind: full store refuses overflow, overflow absent.
	fullStore := newFallbackTakeoverStore()
	for i := 0; i < fallbackTakeoverStoreCap; i++ {
		key := "cap-ctx-" + fallbackItoa(i)
		if _, _, isFull, isCancelled := fullStore.bindContext(live, key, fallbackBinding{ID: "c", Name: "c", BaseURL: "https://a.example", KeyHash: "h", Model: "m"}); isFull || isCancelled {
			t.Fatalf("should not be full/cancelled before cap at %d", i)
		}
	}
	if _, _, isFull, isCancelled := fullStore.bindContext(live, "overflow-ctx", fallbackBinding{ID: "c", Name: "c", BaseURL: "https://a.example", KeyHash: "h", Model: "m"}); !isFull || isCancelled {
		t.Fatalf("overflow must report full only: full=%v cancelled=%v", isFull, isCancelled)
	}
	if _, ok := fullStore.get("overflow-ctx"); ok {
		t.Fatal("overflow must not be recorded")
	}
}

// Direct maybeTakeover with a pre-cancelled context must not create a pending
// entry and must not issue a custom Do. It stops faithfully with the context
// error (handled=true), never an empty/capacity verdict.
func TestMaybeTakeoverPreCancelledNoPendingNoCustomDo(t *testing.T) {
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackResponsesOK("cm-cancel")))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{ID: "c-cancel", Name: "c-cancel", BaseURL: custom.URL, APIKey: "k-cancel", Model: "cm-cancel", Protocol: ProtocolResponses}}, "c-cancel")
	gw.customClient = custom.Client()
	ses := "ses_takeover_precancel_1"
	route := pendingResponsesRoute()
	cancelCtx, cancel := context.WithCancel(pinTestCtx())
	cancel()
	resp, eff, _, handled, takeErr := gw.maybeTakeoverCustomFallback(cancelCtx, route, routeBodies(), pinIDs(ses, "r-pre"), 0, 1, pendingNativePayload())
	if !handled {
		t.Fatal("pre-cancelled takeover must report handled (faithful stop)")
	}
	if takeErr == nil || !isContextCancelled(cancelCtx) {
		t.Fatalf("pre-cancelled takeover must return context error, got %v", takeErr)
	}
	if resp != nil {
		drainResp(resp)
		t.Fatal("pre-cancelled takeover must not return a custom response")
	}
	_ = eff
	if customHits.Load() != 0 {
		t.Fatalf("pre-cancelled takeover must not issue custom Do, hits=%d", customHits.Load())
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatal("pre-cancelled takeover must not record a pending entry")
	}
	if n := gw.scheduler.fallbacks.count(); n != 0 {
		t.Fatalf("store must stay empty, got %d", n)
	}
}

// Cancelled pinned send keeps the legally-established pending binding pending
// (no flip), issues no custom Do, never re-enters native, and the next live
// request still crosses (strips native refs) before establishing.
func TestPinnedCancelledKeepsPendingAndCrossing(t *testing.T) {
	var mu sync.Mutex
	var bodies [][]byte
	var customHits atomic.Int32
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customHits.Add(1)
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
					w.WriteHeader(400)
					_, _ = w.Write([]byte(`{"error":{"message":"encrypted_content was not issued to this caller"}}`))
					return
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackResponsesOK("cm-cancel-pin")))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{ID: "c-cancel-pin", Name: "c-cancel-pin", BaseURL: custom.URL, APIKey: "k-cancel-pin", Model: "cm-cancel-pin", Protocol: ProtocolResponses}}, "c-cancel-pin")
	gw.customClient = custom.Client()
	ses := "ses_pinned_cancel_1"
	route := pendingResponsesRoute()
	bodiesMap := routeBodies()
	ex := pendingNativePayload()

	// Legally establish pending before any cancellation (unconditional legacy
	// bind mirrors a takeover that won its lock before cancel).
	ch, ok := activeFallbackChannel(gw.cfg)
	if !ok {
		t.Fatal("active channel missing")
	}
	if _, inserted, full := gw.scheduler.fallbacks.bind(ses, fallbackBindingFor(ch)); !inserted || full {
		t.Fatal("setup bind must insert pending")
	}
	stored, ok := gw.scheduler.fallbacks.get(ses)
	if !ok || stored.Established {
		t.Fatalf("setup must leave pending: %+v %v", stored, ok)
	}

	// Cancelled pinned send: faithful stop, no custom Do, still pending.
	cancelCtx, cancel := context.WithCancel(pinTestCtx())
	cancel()
	before := customHits.Load()
	resp, _, _, sendErr := gw.doCustomFallbackPinned(cancelCtx, route, bodiesMap, pinIDs(ses, "r-cancel-pin"), stored, 0, ex)
	if sendErr == nil || (!isContextCancelled(cancelCtx) && sendErr != context.Canceled && sendErr != context.DeadlineExceeded) {
		if resp != nil {
			drainResp(resp)
		}
		t.Fatalf("cancelled pinned send must return context error, got resp=%v err=%v", resp, sendErr)
	}
	if resp != nil {
		drainResp(resp)
	}
	if customHits.Load() != before {
		t.Fatalf("cancelled pinned send must not issue custom Do, before=%d after=%d", before, customHits.Load())
	}
	after, ok := gw.scheduler.fallbacks.get(ses)
	if !ok {
		t.Fatal("cancelled pinned send must not delete the pending binding")
	}
	if after.Established {
		t.Fatal("cancelled pinned send must not flip pending to established")
	}

	// Next live request still serves custom exclusively and still crosses:
	// native refs stripped, then the 200 establishes the binding.
	var anonHits atomic.Int32
	postStub(t, gw, "a", 0, &anonHits, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	_ = anonHits
	liveBinding, ok := gw.scheduler.fallbacks.get(ses)
	if !ok {
		t.Fatal("pending must persist for the next live request")
	}
	resp2, eff2, _, err2 := gw.doCustomFallbackPinned(pinTestCtx(), route, bodiesMap, pinIDs(ses, "r-live-after-cancel"), liveBinding, 0, ex)
	if err2 != nil {
		t.Fatalf("live follow-up must succeed: %v", err2)
	}
	defer drainResp(resp2)
	if resp2.StatusCode != 200 || eff2.Tier != TierCustom {
		t.Fatalf("live follow-up must succeed on custom, got %d %+v", resp2.StatusCode, eff2)
	}
	mu.Lock()
	if len(bodies) != 1 {
		mu.Unlock()
		t.Fatalf("custom sends=%d want 1 (cancelled send must not count)", len(bodies))
	}
	only := append([]byte(nil), bodies[0]...)
	mu.Unlock()
	if bodyHasNativeRefs(t, only) {
		t.Fatal("live follow-up on still-pending binding must strip native refs (crossing preserved)")
	}
	if got, _ := gw.scheduler.fallbacks.get(ses); !got.Established {
		t.Fatal("first live custom 2xx must establish the binding")
	}
}

// cancelAfterSendCustomTripper proves the post-send cancel window on the
// gateway wiring: the custom POST is really issued (body captured, hits+1),
// then the request context is cancelled inside RoundTrip, yet the tripper
// still returns (200, nil). Production must then keep the binding pending via
// markEstablishedContext (no flip) without rolling back or fanning out.
type cancelAfterSendCustomTripper struct {
	cancel context.CancelFunc
	hits   *atomic.Int32
	mu     *sync.Mutex
	bodies *[][]byte
	model  string
}

func (t *cancelAfterSendCustomTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	n := t.hits.Add(1)
	if req.Body != nil {
		raw, _ := io.ReadAll(req.Body)
		t.mu.Lock()
		*t.bodies = append(*t.bodies, raw)
		t.mu.Unlock()
	}
	// Cancel after the POST has been issued, then still report HTTP 200.
	if n == 1 && t.cancel != nil {
		t.cancel()
	}
	return responseWithBody(200, fallbackResponsesOK(t.model)), nil
}

// maybeTakeover native->custom first crossing with cancel-after-send: native
// 429 is observed once, the custom POST is sent exactly once and returns 200,
// but the binding must stay pending (Established=false) with identity intact.
// No successor native/custom send happens in that request; the next live
// request still goes custom-exclusive with crossing cleanup and only then
// establishes.
func TestMaybeTakeoverCancelAfterSendKeepsPending(t *testing.T) {
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{ID: "c-take-cancel-send", Name: "c-take-cancel-send", BaseURL: custom.URL, APIKey: "k-take-cancel", Model: "cm-take-cancel", Protocol: ProtocolResponses}}, "c-take-cancel-send")
	ses := "ses_takeover_cancel_after_send_1"
	bindResponsesAnonPin(t, gw, ses)
	var anonHits atomic.Int32
	postStub(t, gw, "a", 0, &anonHits, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	var mu sync.Mutex
	var bodies [][]byte
	var customHits atomic.Int32
	base := pinTestCtx()
	ctx, cancel := context.WithCancel(base)
	gw.customClient = &http.Client{Transport: &cancelAfterSendCustomTripper{
		cancel: cancel,
		hits:   &customHits,
		mu:     &mu,
		bodies: &bodies,
		model:  "cm-take-cancel",
	}}
	route := pendingResponsesRoute()
	resp, eff, _, err := gw.doUpstreamTiers(ctx, route, routeBodies(), pinIDs(ses, "r-take-cancel-send"), 0, pendingNativePayload())
	if err != nil {
		t.Fatalf("cancel-after-send custom 200 must return the response as-is: %v", err)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("cancel-after-send custom must return 200 as-is, got %d", resp.StatusCode)
	}
	if eff.Tier != TierCustom {
		t.Fatalf("takeover route must stay custom-exclusive, got %+v", eff)
	}
	if anonHits.Load() != 1 {
		t.Fatalf("native must be attempted exactly once with no successor native, anon=%d", anonHits.Load())
	}
	if customHits.Load() != 1 {
		t.Fatalf("custom POST must be issued exactly once, custom=%d", customHits.Load())
	}
	bound, ok := gw.scheduler.fallbacks.get(ses)
	if !ok {
		t.Fatal("legally bound pending must persist after cancel-after-send 200")
	}
	if bound.Established {
		t.Fatal("cancel-after-send 200 must not flip pending to established")
	}
	ch, ok := activeFallbackChannel(gw.cfg)
	if !ok {
		t.Fatal("active channel missing")
	}
	want := fallbackBindingFor(ch)
	if bound.ID != want.ID || bound.BaseURL != want.BaseURL || bound.KeyHash != want.KeyHash || bound.Model != want.Model || bound.Protocol != want.Protocol {
		t.Fatalf("binding identity must be preserved: got %+v want %+v", bound, want)
	}
	mu.Lock()
	if len(bodies) != 1 {
		mu.Unlock()
		t.Fatalf("custom sends=%d want 1", len(bodies))
	}
	first := append([]byte(nil), bodies[0]...)
	mu.Unlock()
	if bodyHasNativeRefs(t, first) {
		t.Fatal("first native->custom crossing send must strip native refs even though cancel lands after send")
	}

	// Next live request: still custom-exclusive (no new native), still
	// crossing (native refs stripped), and this live 200 establishes.
	liveAnonBefore := anonHits.Load()
	resp2, eff2, _, err2 := gw.doUpstreamTiers(pinTestCtx(), route, routeBodies(), pinIDs(ses, "r-take-cancel-send-2"), 0, pendingNativePayload())
	if err2 != nil {
		t.Fatalf("live follow-up must succeed: %v", err2)
	}
	defer drainResp(resp2)
	if resp2.StatusCode != 200 || eff2.Tier != TierCustom {
		t.Fatalf("live follow-up must succeed on custom, got %d %+v", resp2.StatusCode, eff2)
	}
	if anonHits.Load() != liveAnonBefore {
		t.Fatalf("pending follow-up must not re-enter native, before=%d after=%d", liveAnonBefore, anonHits.Load())
	}
	if customHits.Load() != 2 {
		t.Fatalf("live follow-up must add exactly one custom send, custom=%d", customHits.Load())
	}
	mu.Lock()
	if len(bodies) != 2 {
		mu.Unlock()
		t.Fatalf("custom sends=%d want 2", len(bodies))
	}
	second := append([]byte(nil), bodies[1]...)
	mu.Unlock()
	if bodyHasNativeRefs(t, second) {
		t.Fatal("live follow-up on still-pending binding must strip native refs (crossing preserved)")
	}
	if got, _ := gw.scheduler.fallbacks.get(ses); !got.Established {
		t.Fatal("first live custom 2xx after the cancelled one must establish the binding")
	}
}

// Already-pending doCustomFallbackPinned with cancel-after-send: the custom
// POST is sent exactly once and returns 200, but the binding stays pending
// with identity intact and no native is touched. The next live pinned send
// still crosses and only then establishes.
func TestPinnedCancelAfterSendKeepsPending(t *testing.T) {
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{ID: "c-pin-cancel-send", Name: "c-pin-cancel-send", BaseURL: custom.URL, APIKey: "k-pin-cancel", Model: "cm-pin-cancel", Protocol: ProtocolResponses}}, "c-pin-cancel-send")
	ses := "ses_pinned_cancel_after_send_1"
	route := pendingResponsesRoute()
	bodiesMap := routeBodies()
	ex := pendingNativePayload()
	ch, ok := activeFallbackChannel(gw.cfg)
	if !ok {
		t.Fatal("active channel missing")
	}
	if _, inserted, full := gw.scheduler.fallbacks.bind(ses, fallbackBindingFor(ch)); !inserted || full {
		t.Fatal("setup bind must insert pending")
	}
	stored, ok := gw.scheduler.fallbacks.get(ses)
	if !ok || stored.Established {
		t.Fatalf("setup must leave pending: %+v %v", stored, ok)
	}
	var anonHits atomic.Int32
	postStub(t, gw, "a", 0, &anonHits, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	var mu sync.Mutex
	var bodies [][]byte
	var customHits atomic.Int32
	ctx, cancel := context.WithCancel(pinTestCtx())
	gw.customClient = &http.Client{Transport: &cancelAfterSendCustomTripper{
		cancel: cancel,
		hits:   &customHits,
		mu:     &mu,
		bodies: &bodies,
		model:  "cm-pin-cancel",
	}}
	resp, eff, _, sendErr := gw.doCustomFallbackPinned(ctx, route, bodiesMap, pinIDs(ses, "r-pin-cancel-send"), stored, 0, ex)
	if sendErr != nil {
		if resp != nil {
			drainResp(resp)
		}
		t.Fatalf("cancel-after-send pinned 200 must return the response as-is: %v", sendErr)
	}
	defer drainResp(resp)
	if resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("cancel-after-send pinned must return custom 200, got %d %+v", resp.StatusCode, eff)
	}
	if customHits.Load() != 1 {
		t.Fatalf("cancelled pinned POST must be issued exactly once, custom=%d", customHits.Load())
	}
	if anonHits.Load() != 0 {
		t.Fatalf("pinned path must never touch native, anon=%d", anonHits.Load())
	}
	after, ok := gw.scheduler.fallbacks.get(ses)
	if !ok {
		t.Fatal("cancel-after-send must not delete the pending binding")
	}
	if after.Established {
		t.Fatal("cancel-after-send 200 must not flip pending to established")
	}
	want := fallbackBindingFor(ch)
	if after.ID != want.ID || after.BaseURL != want.BaseURL || after.KeyHash != want.KeyHash || after.Model != want.Model || after.Protocol != want.Protocol {
		t.Fatalf("binding identity must be preserved: got %+v want %+v", after, want)
	}
	mu.Lock()
	if len(bodies) != 1 {
		mu.Unlock()
		t.Fatalf("custom sends=%d want 1", len(bodies))
	}
	first := append([]byte(nil), bodies[0]...)
	mu.Unlock()
	if bodyHasNativeRefs(t, first) {
		t.Fatal("pending pinned send must strip native refs even though cancel lands after send")
	}

	// Next live pinned send: still crossing, then establishes.
	liveBinding, ok := gw.scheduler.fallbacks.get(ses)
	if !ok {
		t.Fatal("pending must persist for the next live request")
	}
	resp2, eff2, _, err2 := gw.doCustomFallbackPinned(pinTestCtx(), route, bodiesMap, pinIDs(ses, "r-pin-live-after"), liveBinding, 0, ex)
	if err2 != nil {
		t.Fatalf("live follow-up must succeed: %v", err2)
	}
	defer drainResp(resp2)
	if resp2.StatusCode != 200 || eff2.Tier != TierCustom {
		t.Fatalf("live follow-up must succeed on custom, got %d %+v", resp2.StatusCode, eff2)
	}
	if customHits.Load() != 2 {
		t.Fatalf("live follow-up must add exactly one custom send, custom=%d", customHits.Load())
	}
	if anonHits.Load() != 0 {
		t.Fatalf("live pinned follow-up must not touch native, anon=%d", anonHits.Load())
	}
	mu.Lock()
	if len(bodies) != 2 {
		mu.Unlock()
		t.Fatalf("custom sends=%d want 2", len(bodies))
	}
	second := append([]byte(nil), bodies[1]...)
	mu.Unlock()
	if bodyHasNativeRefs(t, second) {
		t.Fatal("live follow-up on still-pending binding must strip native refs (crossing preserved)")
	}
	if got, _ := gw.scheduler.fallbacks.get(ses); !got.Established {
		t.Fatal("first live custom 2xx after the cancelled one must establish the binding")
	}
}
