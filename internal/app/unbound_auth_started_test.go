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
