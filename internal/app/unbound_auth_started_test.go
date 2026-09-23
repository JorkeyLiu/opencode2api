package app

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestUnboundAuthStartedCredentialEvidence(t *testing.T) {
	monitor := NewMonitor()
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct"}, "z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 5
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	cred := gw.credentials()[0]
	ses := "ses_unbound_started_evidence_1"
	ids := pinIDs(ses, "r1")
	now := time.Now().UnixNano()
	cands := gw.scheduler.orderCandidates(gw.scheduler.buildAuthCandidates(TierZen, gw.credentials(), gw.pools["z"], "m", now), ses)
	if len(cands) != 2 {
		t.Fatalf("cands=%d want 2", len(cands))
	}
	// Ensure both proxies belong to same credential.
	for _, c := range cands {
		if c.CredID != cred.id {
			t.Fatalf("cred mismatch %q vs %q", c.CredID, cred.id)
		}
	}
	firstRaw := cands[0].ProxyRaw
	secondRaw := cands[1].ProxyRaw
	firstIdx := poolIndexByRaw(gw, "z", firstRaw)
	secondIdx := poolIndexByRaw(gw, "z", secondRaw)
	var secondArrivalNanos atomic.Int64
	postStub(t, gw, "z", firstIdx, nil, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gw, "z", secondIdx, nil, nil, func(*http.Request) (*http.Response, error) {
		secondArrivalNanos.Store(time.Now().UnixNano())
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "9")
		return r, nil
	})
	resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 429 {
		t.Fatalf("full 429 must keep 429, err=%v resp=%v", err, resp)
	}
	if got := resp.Header.Get("Retry-After"); got != "9" {
		drainResp(resp)
		t.Fatalf("last Retry-After=%q want 9 (Started must retain last live 429)", got)
	}
	drainResp(resp)
	until, status, ok := gw.scheduler.credential429CooldownStatus(cred.id)
	if !ok || status != 429 {
		t.Fatalf("full exhaustion must write credential429")
	}
	if until <= 0 || until <= time.Now().UnixNano() {
		t.Fatalf("credential429 until=%d must be non-zero future deadline", until)
	}
	entry := gw.scheduler.cred429State[cred.id]
	if entry == nil {
		t.Fatalf("credential429 entry missing")
	}
	if entry.lastStartedNanos == 0 {
		t.Fatalf("credential429 lastStartedNanos must be non-zero (real Started propagated, not fallback)")
	}
	if entry.lastStartedNanos <= 1 {
		t.Fatalf("credential429 lastStartedNanos=%d must be >1 to make stale fencing meaningful", entry.lastStartedNanos)
	}
	if sec := secondArrivalNanos.Load(); sec == 0 {
		t.Fatalf("second 429 arrival nanos not captured")
	} else if entry.lastStartedNanos >= sec {
		t.Fatalf("credential429 lastStartedNanos=%d must be < second arrival %d (propagated send-start precedes server receipt)", entry.lastStartedNanos, sec)
	}
	if entry.failures == 0 {
		t.Fatalf("credential429 failures must be non-zero")
	}
	if failures, snapUntil := gw.scheduler.credential429Snapshot(cred.id); failures == 0 || snapUntil != until {
		t.Fatalf("credential429 snapshot mismatch failures=%d until=%d want non-zero and %d", failures, snapUntil, until)
	}
	if ch := gw.scheduler.noteCredential429Success(cred.id, 1); ch.Changed {
		t.Fatalf("stale Started=1 must not clear credential429 (fencing)")
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(cred.id); !ok {
		t.Fatalf("credential429 must remain after stale success")
	}
}

// L1-final 429 on the first candidate still counts as live-429 exhaustion
// evidence: A observes 503,503 then stable 429 (Retry-After 4) within
// MaxAttempts=3, B returns live 429 (Retry-After 9). Full exhaustion must
// write credential429 with the last Retry-After and a real late Started that
// fences stale success. Custom stays inactive; the terminal envelope stays
// the faithful native 429.
func TestUnboundAuthL1Final429StartedFencing(t *testing.T) {
	monitor := NewMonitor()
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct"}, "z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.retryTransientIntervalPresent = true
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	cred := gw.credentials()[0]
	ses := "ses_unbound_l1final429_started_1"
	ids := pinIDs(ses, "r1")
	now := time.Now().UnixNano()
	cands := gw.scheduler.orderCandidates(gw.scheduler.buildAuthCandidates(TierZen, gw.credentials(), gw.pools["z"], "m", now), ses)
	if len(cands) != 2 {
		t.Fatalf("cands=%d want 2", len(cands))
	}
	for _, c := range cands {
		if c.CredID != cred.id {
			t.Fatalf("cred mismatch %q vs %q", c.CredID, cred.id)
		}
	}
	firstIdx := poolIndexByRaw(gw, "z", cands[0].ProxyRaw)
	secondIdx := poolIndexByRaw(gw, "z", cands[1].ProxyRaw)
	var aFirstArrival, bArrival atomic.Int64
	var aCalls, bCalls atomic.Int32
	var aSeq atomic.Int32
	postStub(t, gw, "z", firstIdx, &aCalls, nil, func(*http.Request) (*http.Response, error) {
		n := aSeq.Add(1)
		if n == 1 {
			f := time.Now().UnixNano()
			aFirstArrival.Store(f)
			// Busy-spin (no sleep) so the later B send-start is strictly
			// greater than A's first arrival without wall-clock brittleness.
			for time.Now().UnixNano() <= f {
			}
		}
		if n <= 2 {
			return responseWithBody(503, `{"error":"boom"}`), nil
		}
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "4")
		return r, nil
	})
	postStub(t, gw, "z", secondIdx, &bCalls, nil, func(*http.Request) (*http.Response, error) {
		bArrival.Store(time.Now().UnixNano())
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "9")
		return r, nil
	})
	resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 429 {
		t.Fatalf("L1-final 429 + 429 must keep faithful 429, err=%v resp=%v", err, resp)
	}
	if got := resp.Header.Get("Retry-After"); got != "9" {
		drainResp(resp)
		t.Fatalf("last Retry-After=%q want 9 (last live 429 wins)", got)
	}
	drainResp(resp)
	if eff.Tier != TierZen {
		t.Fatalf("custom inactive must stay native, eff=%+v", eff)
	}
	if attempts != 4 {
		t.Fatalf("attempts=%d want 4 (A x3 L1 + B x1)", attempts)
	}
	if postCount(&aCalls) != 3 || postCount(&bCalls) != 1 {
		t.Fatalf("sends A=%d B=%d want 3/1", postCount(&aCalls), postCount(&bCalls))
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("custom inactive must not bind fallback")
	}
	if _, ok := gw.scheduler.pinGet(ses, "m"); ok {
		t.Fatalf("full 429 must not pin session+model")
	}
	until, status, ok := gw.scheduler.credential429CooldownStatus(cred.id)
	if !ok || status != 429 {
		t.Fatalf("L1-final 429 exhaustion must write credential429")
	}
	if until <= time.Now().UnixNano() {
		t.Fatalf("credential429 until=%d must be future deadline", until)
	}
	entry := gw.scheduler.cred429State[cred.id]
	if entry == nil {
		t.Fatalf("credential429 entry missing")
	}
	if entry.lastStartedNanos == 0 || entry.lastStartedNanos <= 1 {
		t.Fatalf("credential429 lastStartedNanos=%d must be real (>1)", entry.lastStartedNanos)
	}
	aFirst := aFirstArrival.Load()
	bArr := bArrival.Load()
	if aFirst == 0 || bArr == 0 {
		t.Fatalf("arrival nanos not captured aFirst=%d b=%d", aFirst, bArr)
	}
	if entry.lastStartedNanos <= aFirst {
		t.Fatalf("credential429 lastStartedNanos=%d must be later than A first arrival %d (late send)", entry.lastStartedNanos, aFirst)
	}
	if entry.lastStartedNanos >= bArr {
		t.Fatalf("credential429 lastStartedNanos=%d must be < B arrival %d (send-start precedes receipt)", entry.lastStartedNanos, bArr)
	}
	if failures, snapUntil := gw.scheduler.credential429Snapshot(cred.id); failures == 0 || snapUntil != until {
		t.Fatalf("credential429 snapshot mismatch failures=%d until=%d want non-zero and %d", failures, snapUntil, until)
	}
	if ch := gw.scheduler.noteCredential429Success(cred.id, 1); ch.Changed {
		t.Fatalf("stale Started=1 must not clear credential429 (fencing)")
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(cred.id); !ok {
		t.Fatalf("credential429 must remain after stale success")
	}
	// Equal Started clears per scheduler contract (only strictly-stale fences).
	ls := entry.lastStartedNanos
	if ch := gw.scheduler.noteCredential429Success(cred.id, ls); !ch.Cleared || !ch.Changed {
		t.Fatalf("equal Started=%d must clear credential429, got %+v", ls, ch)
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(cred.id); ok {
		t.Fatalf("credential429 must be cleared after equal Started")
	}
}

// Control: A reaches L1-final 503 (never 429) while B returns live 429.
// Only one of two eligible proxies supplied live-429 evidence, so no
// credential429 write even though both objects are unavailable.
func TestUnboundAuthL1Final503NoCredential429(t *testing.T) {
	monitor := NewMonitor()
	cfg := testGatewayConfig(
		map[string][]string{"a": {"direct"}, "z": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "a", Authenticated: "z"},
	)
	cfg.Anonymous = false
	cfg.Keys = []string{"zen-key-aaaaa"}
	cfg.Retry.MaxAttempts = 3
	cfg.Retry.TransientRetryIntervalSeconds = 0
	cfg.retryTransientIntervalPresent = true
	normalized, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(normalized, discardGatewayLogger(), monitor)
	if err != nil {
		t.Fatal(err)
	}
	cred := gw.credentials()[0]
	ses := "ses_unbound_l1final503_nocred429_1"
	ids := pinIDs(ses, "r1")
	now := time.Now().UnixNano()
	cands := gw.scheduler.orderCandidates(gw.scheduler.buildAuthCandidates(TierZen, gw.credentials(), gw.pools["z"], "m", now), ses)
	if len(cands) != 2 {
		t.Fatalf("cands=%d want 2", len(cands))
	}
	firstIdx := poolIndexByRaw(gw, "z", cands[0].ProxyRaw)
	secondIdx := poolIndexByRaw(gw, "z", cands[1].ProxyRaw)
	var aCalls, bCalls atomic.Int32
	postStub(t, gw, "z", firstIdx, &aCalls, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(503, `{"error":"boom"}`), nil
	})
	postStub(t, gw, "z", secondIdx, &bCalls, nil, func(*http.Request) (*http.Response, error) {
		r := responseWithBody(429, `{"error":"t"}`)
		r.Header.Set("Retry-After", "9")
		return r, nil
	})
	resp, eff, attempts, err := gw.doUpstreamTiers(pinTestCtx(), authOnlyRoute(), routeBodies(), ids, 0)
	if err != nil || resp == nil || resp.StatusCode != 429 {
		t.Fatalf("L1-final 503 + single 429 must keep faithful 429, err=%v resp=%v", err, resp)
	}
	if got := resp.Header.Get("Retry-After"); got != "9" {
		drainResp(resp)
		t.Fatalf("Retry-After=%q want 9 (sole live 429)", got)
	}
	drainResp(resp)
	if eff.Tier != TierZen {
		t.Fatalf("custom inactive must stay native, eff=%+v", eff)
	}
	if attempts != 4 {
		t.Fatalf("attempts=%d want 4 (A x3 L1 + B x1)", attempts)
	}
	if postCount(&aCalls) != 3 || postCount(&bCalls) != 1 {
		t.Fatalf("sends A=%d B=%d want 3/1", postCount(&aCalls), postCount(&bCalls))
	}
	if _, _, ok := gw.scheduler.credential429CooldownStatus(cred.id); ok {
		t.Fatalf("partial live-429 (L1-final 503 + one 429) must not write credential429")
	}
	if _, ok := gw.scheduler.fallbacks.get(ses); ok {
		t.Fatalf("custom inactive must not bind fallback")
	}
}
