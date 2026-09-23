package app

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Pinned anonymous single/current proxy stream-startup failure is observed to
// the L1 limit on the same target: local 502, never walks another proxy,
// never takes over custom even when active. Uses real stream-gate behavior
// (empty SSE => startup sentinel), not direct helper calls.
func TestPinnedAnonStreamStartupFailureNoWalkNoFallback(t *testing.T) {
	var customHits atomic.Int32
	gw := pinnedAnonTwoProxyGateway(t, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 3
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0
	gw.cfg.Retry.TimeoutSeconds = 5

	ses := "ses_pinned_stream_startup_anon_1"
	pool := gw.pools["a"]
	if pool == nil || len(pool.items) != 2 {
		t.Fatalf("anon pool items=%d want 2", len(pool.items))
	}
	gw.bindSessionPin(ses, "m", TierZen, anonymousSchedulerCredentialID, "a", pool.items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, ok := gw.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("must bind pinned anon")
	}
	ordered := affinityProxyOrder(pool, pin.CredID, pin.ProxyRaw)
	if len(ordered) != 2 {
		t.Fatalf("ordered=%d want 2", len(ordered))
	}
	if ordered[0].name != pin.ProxyRaw {
		t.Fatalf("ordered[0]=%q want pinned %q", ordered[0].name, pin.ProxyRaw)
	}
	pinnedIdx := poolIndexByRaw(gw, "a", pin.ProxyRaw)
	otherIdx := poolIndexByRaw(gw, "a", ordered[1].name)
	if pinnedIdx < 0 || otherIdx < 0 {
		t.Fatalf("idx not found pinned=%d other=%d", pinnedIdx, otherIdx)
	}

	var pinnedCalls, otherCalls atomic.Int32
	pinnedCap := &capturedUpstream{}
	postStub(t, gw, "a", pinnedIdx, &pinnedCalls, pinnedCap, func(*http.Request) (*http.Response, error) {
		return sseResponse(""), nil
	})
	postStub(t, gw, "a", otherIdx, &otherCalls, nil, func(*http.Request) (*http.Response, error) {
		return sseResponse(chatTextSSE("should-not") + chatDoneSSE()), nil
	})

	streamCtx, cancel := context.WithTimeout(context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true}), 10*time.Second)
	defer cancel()
	ids := requestIDs{Session: ses, Request: "req-pinned-anon-startup", Project: "prj-test"}
	route := anonAuthRoute()
	route.ID = "m"
	resp, eff, attempts, err := gw.doUpstreamTiers(streamCtx, route, streamBodies(), ids, 0, pinnedAnonConsumeExtra())
	if err != nil {
		t.Fatalf("pinned anon startup failure must return response, err=%v", err)
	}
	if resp == nil {
		t.Fatalf("nil resp")
	}
	defer drainResp(resp)
	if resp.StatusCode != 502 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d want 502, body=%q", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if eff.Tier == TierCustom {
		t.Fatalf("must not take over custom on pinned anon startup failure")
	}
	if customHits.Load() != 0 {
		t.Fatalf("custom hits=%d want 0 (no fallback)", customHits.Load())
	}
	if got := postCount(&pinnedCalls); got != 3 {
		t.Fatalf("pinnedCalls=%d want 3 (initial + 2 same-target L1 observations)", got)
	}
	if got := postCount(&otherCalls); got != 0 {
		t.Fatalf("otherCalls=%d want 0 (no proxy walk)", got)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3", attempts)
	}
	// Isolation: startup cools only its single target; proxy429/channel/
	// credential429/credential401/suspect must stay unset, proxy stays healthy.
	now := time.Now().UnixNano()
	for _, p := range pool.items {
		if until, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "a", p.name); ok && until > now {
			t.Fatalf("proxy429 must not be set for startup failure, proxy %q until %d", p.name, until)
		}
		if until, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "a", p.name); ok && until > now {
			t.Fatalf("channel must not be set for startup failure, proxy %q until %d", p.name, until)
		}
		if until, _, ok := gw.scheduler.suspectCooldownStatus(TierZen, "a", p.name); ok && until > now {
			t.Fatalf("suspect must not be set for startup failure, proxy %q until %d", p.name, until)
		}
		if p == nil || !p.healthy.Load() {
			t.Fatalf("proxy %q must stay healthy after startup failure", p.name)
		}
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("credential429 must not be written for pinned anon startup failure")
	}
	if _, _, ok := gw.scheduler.credentialCooldownStatus(pin.CredID); ok {
		t.Fatalf("credential 401 must not be written for pinned anon startup failure")
	}
	identPinned := targetIdentity(TierZen, pin.CredID, "a", pin.ProxyRaw, "m")
	if until, _, ok := gw.scheduler.targetCooldownStatus(identPinned); !ok || until <= time.Now().UnixNano() {
		t.Fatalf("pinned target must be cooled after startup failure")
	}
	identOther := targetIdentity(TierZen, pin.CredID, "a", ordered[1].name, "m")
	if until, _, ok := gw.scheduler.targetCooldownStatus(identOther); ok && until > time.Now().UnixNano() {
		t.Fatalf("other target must not be cooled (never sent), until %d", until)
	}
	// Pin/current selection unchanged: anonymous bindings never move.
	after, ok := gw.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("pin must survive startup failure")
	}
	if after.ProxyRaw != pin.ProxyRaw || after.Pool != "a" || after.CredID != anonymousSchedulerCredentialID || after.Tier != TierZen {
		t.Fatalf("pin must stay on current selection, before=%+v after=%+v", pin, after)
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("must not bind fallback on single-proxy startup failure")
	}
	// Identity preservation: every same-target retry carries the same
	// proxy-free wire session, never the raw client session.
	sessions, bodies := pinnedCap.get()
	if len(sessions) != 3 {
		t.Fatalf("captured sessions=%d want 3 (one per L1 send)", len(sessions))
	}
	for i, s := range sessions {
		if s == "" {
			t.Fatalf("wire session %d empty", i)
		}
		if s == ses {
			t.Fatalf("wire session must be pseudonymous, got raw %q", s)
		}
		if s != sessions[0] {
			t.Fatalf("wire session must stay stable across L1 retries, got %q vs %q", s, sessions[0])
		}
	}
	for i := 1; i < len(bodies); i++ {
		if string(bodies[i]) != string(bodies[0]) {
			t.Fatalf("candidate body must stay identical across L1 retries")
		}
	}
}

// Bounded pinned-anon L2 consumption: first/current proxy live 429 causes an
// actual switch/send to the next frozen eligible proxy; the next proxy reaches
// L1-final stream-startup failure via the real gate; consumed=true and all
// eligible attempted, so active custom takes over successfully.
func TestPinnedAnonConsumption429ThenStartupTakeover(t *testing.T) {
	var customHits atomic.Int32
	gw := pinnedAnonTwoProxyGateway(t, &customHits, true)
	gw.cfg.Retry.MaxAttempts = 2
	gw.cfg.Retry.TransientRetryIntervalSeconds = 0

	ses := "ses_pinned_anon_consume_429_startup_1"
	pool := gw.pools["a"]
	if pool == nil || len(pool.items) != 2 {
		t.Fatalf("anon pool items=%d want 2", len(pool.items))
	}
	gw.bindSessionPin(ses, "m", TierZen, anonymousSchedulerCredentialID, "a", pool.items[0].name, ProtocolChat, normalizeRouteAuthority(gw.cfg.Upstream.Zen))
	pin, _ := gw.scheduler.pinGet(ses, "m")
	ordered := affinityProxyOrder(pool, pin.CredID, pin.ProxyRaw)
	if len(ordered) != 2 {
		t.Fatalf("ordered=%d want 2", len(ordered))
	}
	if ordered[0].name != pin.ProxyRaw {
		t.Fatalf("ordered[0]=%q want pinned %q", ordered[0].name, pin.ProxyRaw)
	}
	var p0Calls, p1Calls atomic.Int32
	p0Cap := &capturedUpstream{}
	p1Cap := &capturedUpstream{}
	postStub(t, gw, "a", poolIndexByRaw(gw, "a", ordered[0].name), &p0Calls, p0Cap, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gw, "a", poolIndexByRaw(gw, "a", ordered[1].name), &p1Calls, p1Cap, func(*http.Request) (*http.Response, error) {
		return sseResponse(""), nil
	})

	streamCtx, cancel := context.WithTimeout(context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true}), 10*time.Second)
	defer cancel()
	resp, eff, attempts, err := gw.doUpstreamTiers(streamCtx, anonAuthRoute(), streamBodies(), pinIDs(ses, "r1"), 0, pinnedAnonConsumeExtra())
	if err != nil || resp == nil || resp.StatusCode != 200 || eff.Tier != TierCustom {
		t.Fatalf("anon 429->L1-final startup must take over custom 200, err=%v resp=%v eff=%+v", err, resp, eff)
	}
	drainResp(resp)
	if customHits.Load() != 1 {
		t.Fatalf("hits=%d want 1", customHits.Load())
	}
	if postCount(&p0Calls) != 1 {
		t.Fatalf("p0 sends=%d want 1 (live 429 consume action)", postCount(&p0Calls))
	}
	if postCount(&p1Calls) != 2 {
		t.Fatalf("p1 sends=%d want 2 (L1 observation limit includes first send)", postCount(&p1Calls))
	}
	if attempts != 4 {
		t.Fatalf("attempts=%d want 4 (1 native 429 + 2 native startup + 1 custom)", attempts)
	}
	if eff.ID != "cm-anon-consume" {
		t.Fatalf("effective model=%q want cm-anon-consume rewrite", eff.ID)
	}
	binding, ok := gw.scheduler.fallbacks.get(ses)
	if !ok {
		t.Fatalf("must bind fallback")
	}
	if binding.Model != "cm-anon-consume" {
		t.Fatalf("fallback binding wrong: %+v", binding)
	}
	// Pin stays on its durable anonymous identity; only the session-level
	// custom binding is added (no cross-key/pool/channel move).
	after, ok := gw.scheduler.pinGet(ses, "m")
	if !ok {
		t.Fatalf("pin must survive custom takeover")
	}
	if after.ProxyRaw != pin.ProxyRaw || after.Pool != "a" || after.CredID != anonymousSchedulerCredentialID || after.Tier != TierZen {
		t.Fatalf("pin must stay on current anon selection, before=%+v after=%+v", pin, after)
	}
	// Scheduler rules: non-429 takeover never writes credential429 or
	// credential401; proxy429 only from the real 429; target only for the
	// startup proxy; no channel/suspect from either.
	if _, _, ok := gw.scheduler.credential429CooldownStatus(pin.CredID); ok {
		t.Fatalf("startup takeover must not write credential429")
	}
	if _, _, ok := gw.scheduler.credentialCooldownStatus(pin.CredID); ok {
		t.Fatalf("startup must not write credential 401 cooldown")
	}
	now := time.Now().UnixNano()
	if until, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "a", ordered[0].name); !ok || until <= now {
		t.Fatalf("real 429 must write proxy429 for the 429 proxy, ok=%v until=%d", ok, until)
	}
	if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, "a", ordered[1].name); ok {
		t.Fatalf("startup must not write proxy429 for the startup proxy")
	}
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "a", ordered[0].name); ok {
		t.Fatalf("429 without comparative success must not write channel")
	}
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, "a", ordered[1].name); ok {
		t.Fatalf("startup must not write channel for the startup proxy")
	}
	if _, _, ok := gw.scheduler.suspectCooldownStatus(TierZen, "a", ordered[0].name); ok {
		t.Fatalf("429 must not write suspect")
	}
	if _, _, ok := gw.scheduler.suspectCooldownStatus(TierZen, "a", ordered[1].name); ok {
		t.Fatalf("startup must not write suspect")
	}
	startupTarget := targetIdentity(TierZen, pin.CredID, "a", ordered[1].name, "m")
	if until, _, ok := gw.scheduler.targetCooldownStatus(startupTarget); !ok || until <= time.Now().UnixNano() {
		t.Fatalf("startup must cool its single target, ok=%v until=%d", ok, until)
	}
	firstTarget := targetIdentity(TierZen, pin.CredID, "a", ordered[0].name, "m")
	if _, _, ok := gw.scheduler.targetCooldownStatus(firstTarget); ok {
		t.Fatalf("429 must not write target cooldown for the 429 proxy")
	}
	for _, p := range pool.items {
		if p == nil || !p.healthy.Load() {
			t.Fatalf("proxy %q must stay healthy (429/startup never flip health)", p.name)
		}
	}
	// Native sends share one proxy-free wire session; no raw session leaks.
	s0, _ := p0Cap.get()
	s1, _ := p1Cap.get()
	if len(s0) != 1 || len(s1) != 2 {
		t.Fatalf("captured native sends p0=%d p1=%d want 1/2", len(s0), len(s1))
	}
	all := append(append([]string{}, s0...), s1...)
	for i, s := range all {
		if s == "" {
			t.Fatalf("native wire session %d empty", i)
		}
		if s == ses {
			t.Fatalf("native wire session must be pseudonymous, got raw %q", s)
		}
		if s != all[0] {
			t.Fatalf("native wire session must stay stable across p0->p1, got %q vs %q", s, all[0])
		}
	}
	// Session stays custom: a follow-up serves via custom without new natives.
	p0Before := postCount(&p0Calls)
	p1Before := postCount(&p1Calls)
	streamCtx2, cancel2 := context.WithTimeout(context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Stream: true}), 10*time.Second)
	defer cancel2()
	resp2, eff2, _, err2 := gw.doUpstreamTiers(streamCtx2, anonAuthRoute(), streamBodies(), pinIDs(ses, "r2"), 0, pinnedAnonConsumeExtra())
	if err2 != nil || resp2 == nil || resp2.StatusCode != 200 || eff2.Tier != TierCustom {
		t.Fatalf("bound session must stay custom 200, err=%v resp=%v eff=%+v", err2, resp2, eff2)
	}
	drainResp(resp2)
	if customHits.Load() != 2 {
		t.Fatalf("follow-up custom hits=%d want 2", customHits.Load())
	}
	if postCount(&p0Calls) != p0Before || postCount(&p1Calls) != p1Before {
		t.Fatalf("follow-up must not send natives, p0=%d->%d p1=%d->%d", p0Before, postCount(&p0Calls), p1Before, postCount(&p1Calls))
	}
}
