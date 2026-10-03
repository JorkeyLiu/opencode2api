package app

import (
	"context"
	"testing"
)

// Pre-cancelled bindCtx never writes; uncancelled bind keeps first-wins and
// generation-zero semantics. The lock-held ctx recheck is the local
// linearization point; no cross ctx/store atomicity is claimed.
func TestPinBindCtxPreCancelledNoWrite(t *testing.T) {
	sched := &targetScheduler{pins: newSessionPinStore()}
	pin := sessionPin{Tier: TierZen, CredID: anonymousSchedulerCredentialID, Pool: "a", ProxyRaw: "proxy-a", Protocol: ProtocolChat, Authority: "auth"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sched.pinBindCtx(ctx, "ses", "m", pin) {
		t.Fatalf("pre-cancelled bindCtx must not insert")
	}
	if _, ok := sched.pinGet("ses", "m"); ok {
		t.Fatalf("pre-cancelled bindCtx must not write pin")
	}
	if !sched.pinBindCtx(context.Background(), "ses", "m", pin) {
		t.Fatalf("uncancelled bindCtx must insert")
	}
	got, ok := sched.pinGet("ses", "m")
	if !ok {
		t.Fatalf("bindCtx must be observable via pinGet")
	}
	if got.ProxyRaw != "proxy-a" || got.Generation != 0 {
		t.Fatalf("bindCtx must keep proxy and generation zero: %+v", got)
	}
	other := pin
	other.ProxyRaw = "proxy-b"
	if sched.pinBindCtx(context.Background(), "ses", "m", other) {
		t.Fatalf("second bindCtx must not overwrite first-wins pin")
	}
	if got, _ := sched.pinGet("ses", "m"); got.ProxyRaw != "proxy-a" {
		t.Fatalf("first-wins violated: %+v", got)
	}
	// Pre-cancelled bind over an existing pin keeps the original.
	if sched.pinBindCtx(ctx, "ses", "m", other) {
		t.Fatalf("pre-cancelled bindCtx over existing pin must report no insert")
	}
	if got, _ := sched.pinGet("ses", "m"); got.ProxyRaw != "proxy-a" {
		t.Fatalf("pre-cancelled bindCtx must not disturb existing pin: %+v", got)
	}
}

// Pre-cancelled moveCurrentCtx never mutates; uncancelled moves keep exact
// generation-fencing semantics (bump on change, stale gen rejected, same
// proxy idempotent without bump).
func TestPinMoveCurrentCtxPreCancelledNoMove(t *testing.T) {
	sched := &targetScheduler{pins: newSessionPinStore()}
	pin := sessionPin{Tier: TierZen, CredID: "cred", Pool: "z", ProxyRaw: "proxy-a", Protocol: ProtocolChat, Authority: "auth"}
	sched.pinBind("ses", "m", pin)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if gen, ok := sched.pinMoveCurrentCtx(ctx, "ses", "m", 0, "z", "proxy-b"); ok || gen != 0 {
		t.Fatalf("pre-cancelled moveCtx must not move: gen=%d ok=%v", gen, ok)
	}
	if got, _ := sched.pinGet("ses", "m"); got.ProxyRaw != "proxy-a" || got.Generation != 0 {
		t.Fatalf("pre-cancelled moveCtx must leave pin intact: %+v", got)
	}
	if gen, ok := sched.pinMoveCurrentCtx(context.Background(), "ses", "m", 0, "z", "proxy-b"); !ok || gen != 1 {
		t.Fatalf("uncancelled moveCtx must bump generation: gen=%d ok=%v", gen, ok)
	}
	if got, _ := sched.pinGet("ses", "m"); got.ProxyRaw != "proxy-b" || got.Generation != 1 {
		t.Fatalf("moveCtx must update proxy with fencing: %+v", got)
	}
	if gen, ok := sched.pinMoveCurrentCtx(context.Background(), "ses", "m", 0, "z", "proxy-c"); ok || gen != 1 {
		t.Fatalf("stale generation must be rejected: gen=%d ok=%v", gen, ok)
	}
	if gen, ok := sched.pinMoveCurrentCtx(context.Background(), "ses", "m", 1, "z", "proxy-b"); !ok || gen != 1 {
		t.Fatalf("same-proxy move must be idempotent without bump: gen=%d ok=%v", gen, ok)
	}
	if got, _ := sched.pinGet("ses", "m"); got.ProxyRaw != "proxy-b" || got.Generation != 1 {
		t.Fatalf("idempotent move must not bump: %+v", got)
	}
}
