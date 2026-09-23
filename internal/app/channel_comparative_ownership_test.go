package app

import (
	"context"
	"testing"
	"time"
)

// Foreground inference ownership: 403/5xx cools only the per-target entry.
// It must never write the tier-qualified channel cooldown merely because one
// node failed. Channel writes belong to comparative management probe logic
// (success on one node plus failure on another in the same tier+credential
// group); lone failures stay display-only at the channel layer.
func TestForeground4035xxWritesTargetOnlyNoChannel(t *testing.T) {
	for _, status := range []int{403, 500, 503} {
		t.Run(statusString(status), func(t *testing.T) {
			gw := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081"})
			pool := gw.pools["shared"]
			cred := gw.authCreds[0]
			cand := authCand(TierZen, cred, pool, pool.items[0], "model-a")
			gw.applyAttemptOutcome(context.Background(), cand, responseWithStatus(status), nil, time.Now().UnixNano())
			if _, gotStatus, ok := gw.scheduler.targetCooldownStatus(cand.Identity); !ok || gotStatus != status {
				t.Fatalf("status %d: target must cool with status %d: ok=%v got=%d", status, status, ok, gotStatus)
			}
			if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, cand.PoolName, cand.ProxyRaw); ok {
				t.Fatalf("status %d: foreground must not write channel", status)
			}
			if _, _, ok := gw.scheduler.channelCooldownStatus(TierGo, cand.PoolName, cand.ProxyRaw); ok {
				t.Fatalf("status %d: foreground must never write Go channel", status)
			}
			if _, _, ok := gw.scheduler.proxy429CooldownStatus(TierZen, cand.PoolName, cand.ProxyRaw); ok {
				t.Fatalf("status %d: foreground 403/5xx must not write proxy429", status)
			}
			if got := gw.scheduler.credentialCoolUntil(cand.CredID); got > time.Now().UnixNano() {
				t.Fatalf("status %d: foreground 403/5xx must not cool credential", status)
			}
			// Sibling node shares no channel fate from one foreground failure.
			if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, pool.name, pool.items[1].name); ok {
				t.Fatalf("status %d: sibling channel must stay clear", status)
			}
		})
	}
}

// Same ownership for the anonymous Zen lane: foreground 5xx cools only the
// anonymous per-target entry, never the shared Zen channel entry.
func TestForegroundAnonymous5xxWritesTargetOnlyNoChannel(t *testing.T) {
	gw := schedulerTestGateway(t, []string{"zen-key-aaaaa"}, []string{"direct", "http://127.0.0.1:8081"})
	pool := gw.pools["shared"]
	cands := gw.scheduler.buildAnonymousCandidates(pool, "model-a", time.Now().UnixNano())
	if len(cands) == 0 {
		t.Fatalf("anonymous candidates missing")
	}
	cand := cands[0]
	gw.applyAttemptOutcome(context.Background(), cand, responseWithStatus(500), nil, time.Now().UnixNano())
	if _, _, ok := gw.scheduler.targetCooldownStatus(cand.Identity); !ok {
		t.Fatalf("anonymous foreground 500 must cool its target")
	}
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierZen, cand.PoolName, cand.ProxyRaw); ok {
		t.Fatalf("anonymous foreground 500 must not write channel")
	}
	if _, _, ok := gw.scheduler.channelCooldownStatus(TierGo, cand.PoolName, cand.ProxyRaw); ok {
		t.Fatalf("anonymous foreground must never write Go channel")
	}
}

func statusString(status int) string {
	switch status {
	case 403:
		return "403"
	case 500:
		return "500"
	case 503:
		return "503"
	default:
		return "unknown"
	}
}
