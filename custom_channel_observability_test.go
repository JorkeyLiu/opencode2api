package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestFallbackObservabilityChannelIdentity(t *testing.T) {
	if got := fallbackObservabilityChannel("c1"); got != "custom:c1" {
		t.Fatalf("channel=%q want custom:c1", got)
	}
	if got := fallbackObservabilityChannel(""); got != "custom" {
		t.Fatalf("empty channel=%q want custom", got)
	}
	if string(TierCustom) != "custom" {
		t.Fatalf("tier custom changed: %q", string(TierCustom))
	}
}

func TestCustomUsageSplitsPerChannelKeepsTiersTotal(t *testing.T) {
	m := NewMonitor()
	rec := func(channel, model string, in, out int) {
		meta := &requestMeta{
			Model: model, Tier: string(TierCustom), Protocol: "chat",
			Request: "req-" + channel + "-" + model, KeyID: channel, Channel: channel,
			Attempts: 1, UsageReported: true,
			Usage: bridgeUsage{Input: in, Output: out, Total: in + out},
		}
		m.Record("/v1/chat/completions", 200, time.Millisecond, meta)
	}
	rec("custom:c1", "cm", 10, 5)
	rec("custom:c2", "cm", 100, 50)
	snap := m.Snapshot()
	ch := snap.Usage.Window.Channels
	if len(ch) < 2 || ch["custom:c1"].Input != 10 || ch["custom:c2"].Input != 100 {
		t.Fatalf("channels split missing: %+v", ch)
	}
	tiers := snap.Usage.Window.Tiers
	if tiers["custom"].Input != 110 || tiers["custom"].Output != 55 {
		t.Fatalf("tiers.custom must stay total: %+v", tiers)
	}
	life := snap.Usage.Lifetime.Channels
	if life["custom:c1"].Input != 10 || life["custom:c2"].Input != 100 {
		t.Fatalf("lifetime channels missing: %+v", life)
	}
	if snap.Usage.Lifetime.Tiers["custom"].Input != 110 {
		t.Fatalf("lifetime tiers.custom must stay total: %+v", snap.Usage.Lifetime.Tiers)
	}
	// Per-channel call counts are additive and do not break tiers.
	if snap.Channels["custom:c1"] != 1 || snap.Channels["custom:c2"] != 1 {
		t.Fatalf("channel call counts missing: %+v", snap.Channels)
	}
	if snap.Tiers["custom"] != 2 {
		t.Fatalf("tier call counts must stay total: %+v", snap.Tiers)
	}
	// Zen behavior unchanged: single tier row, no custom split interference.
	m2 := NewMonitor()
	m2.Record("/v1/chat/completions", 200, time.Millisecond, &requestMeta{
		Model: "m", Tier: "zen", Protocol: "chat", Request: "r1",
		KeyID: "KKKKK", Channel: "key", Attempts: 1,
		UsageReported: true, Usage: bridgeUsage{Input: 7, Output: 3, Total: 10},
	})
	s2 := m2.Snapshot()
	if s2.Usage.Window.Tiers["zen"].Input != 7 {
		t.Fatalf("zen tiers changed: %+v", s2.Usage.Window.Tiers)
	}
}

func TestCustomAttemptChannelsSplitAndCredentialDedup(t *testing.T) {
	m := NewMonitor()
	now := time.Now().UTC()
	m.RecordAttempt(UpstreamAttempt{Time: now, RequestID: "a1", Model: "cm", Tier: "custom", Attempt: 1, KeyID: "custom:c1", Channel: "custom:c1", Proxy: "direct", Status: 200, DurationMS: 5, Success: true, FailureClass: AttemptClassSuccess})
	m.RecordAttempt(UpstreamAttempt{Time: now, RequestID: "a2", Model: "cm", Tier: "custom", Attempt: 1, KeyID: "custom:c2", Channel: "custom:c2", Proxy: "direct", Status: 200, DurationMS: 5, Success: true, FailureClass: AttemptClassSuccess})
	snap := m.Snapshot()
	if snap.Upstream.Lifetime.Channels["custom:c1"].Total != 1 || snap.Upstream.Lifetime.Channels["custom:c2"].Total != 1 {
		t.Fatalf("attempt channels must split: %+v", snap.Upstream.Lifetime.Channels)
	}
	if snap.Upstream.Lifetime.Tiers["custom"].Total != 2 {
		t.Fatalf("attempt tiers.custom must stay total: %+v", snap.Upstream.Lifetime.Tiers)
	}
	// Old merged record keeps its key.
	m.RecordAttempt(UpstreamAttempt{Time: now, RequestID: "a3", Model: "cm", Tier: "custom", Attempt: 1, KeyID: "custom:c1", Channel: "custom", Proxy: "direct", Status: 200, DurationMS: 5, Success: true, FailureClass: AttemptClassSuccess})
	snap2 := m.Snapshot()
	if snap2.Upstream.Lifetime.Channels["custom"].Total != 1 {
		t.Fatalf("old custom row must persist: %+v", snap2.Upstream.Lifetime.Channels)
	}
	// Credential dedup never yields custom:custom:<name>.
	for _, a := range []UpstreamAttempt{
		{KeyID: "custom:c1", Channel: "custom:c1"},
		{KeyID: "custom:c1", Channel: "custom"},
		{KeyID: "custom:c9", Channel: "custom:c9"},
	} {
		if got := resourceCredentialID(a); got != "custom:"+strings.TrimPrefix(a.KeyID, "custom:") {
			t.Fatalf("credID=%q want single custom prefix for %+v", got, a)
		}
		if strings.Contains(resourceCredentialID(a), "custom:custom:") {
			t.Fatalf("double prefix for %+v", a)
		}
	}
}

func TestHistoryChannelPrefixAndExact(t *testing.T) {
	if !historyChannelMatches("custom:c1", "custom") {
		t.Fatal("channel=custom must match custom:c1")
	}
	if !historyChannelMatches("custom", "custom") {
		t.Fatal("channel=custom must match legacy custom")
	}
	if historyChannelMatches("custom:c2", "custom:c1") {
		t.Fatal("exact filter must not match other channel")
	}
	if !historyChannelMatches("custom:c1", "custom:c1") {
		t.Fatal("exact filter must match")
	}
	if historyChannelMatches("custom", "custom:c1") {
		t.Fatal("old custom must not forge a name for exact filter")
	}
	if historyChannelMatches("zen", "custom") {
		t.Fatal("zen must not match custom")
	}
	// Store-level prefix/exact behavior for both requests and attempts.
	s, _ := openTestHistory(t, HistoryConfig{Enabled: true, Directory: "history", RetentionDays: 7, MaxBytesMB: 128})
	now := time.Now().UTC()
	s.EnqueueRequest(UpstreamRequest{Time: now, RequestID: "cr1", Model: "cm", Tier: "custom", KeyID: "custom:c1", Channel: "custom:c1", Attempts: 1, Status: 200, DurationMS: 1, Success: true, Outcome: "success"})
	s.EnqueueRequest(UpstreamRequest{Time: now.Add(time.Second), RequestID: "cr2", Model: "cm", Tier: "custom", KeyID: "custom:c2", Channel: "custom:c2", Attempts: 1, Status: 200, DurationMS: 1, Success: true, Outcome: "success"})
	s.EnqueueRequest(UpstreamRequest{Time: now.Add(2 * time.Second), RequestID: "cr3", Model: "cm", Tier: "custom", KeyID: "custom:c1", Channel: "custom", Attempts: 1, Status: 200, DurationMS: 1, Success: true, Outcome: "success"})
	s.EnqueueAttempt(UpstreamAttempt{Time: now, RequestID: "cr1", Model: "cm", Tier: "custom", Attempt: 1, KeyID: "custom:c1", Channel: "custom:c1", Proxy: "direct", Status: 200, DurationMS: 1, Success: true, FailureClass: AttemptClassSuccess})
	s.EnqueueAttempt(UpstreamAttempt{Time: now.Add(time.Second), RequestID: "cr2", Model: "cm", Tier: "custom", Attempt: 1, KeyID: "custom:c2", Channel: "custom:c2", Proxy: "direct", Status: 200, DurationMS: 1, Success: true, FailureClass: AttemptClassSuccess})
	waitHistoryQueueDrained(t, s)
	flushHistoryBuffers(s)
	from, to := now.Add(-time.Hour), now.Add(time.Hour)
	all, err := queryHistoryRequests(context.Background(), s, historyQueryFilter{From: from, To: to, Limit: 10, Channel: "custom"})
	if err != nil {
		t.Fatal(err)
	}
	if len(all.Items) != 3 {
		t.Fatalf("channel=custom must match all 3, got %d", len(all.Items))
	}
	exact, err := queryHistoryRequests(context.Background(), s, historyQueryFilter{From: from, To: to, Limit: 10, Channel: "custom:c1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(exact.Items) != 1 {
		t.Fatalf("exact custom:c1 must match 1, got %d", len(exact.Items))
	}
	attAll, err := queryHistoryAttempts(context.Background(), s, historyQueryFilter{From: from, To: to, Limit: 10, Channel: "custom"})
	if err != nil {
		t.Fatal(err)
	}
	if len(attAll.Items) != 2 {
		t.Fatalf("attempt channel=custom must match 2, got %d", len(attAll.Items))
	}
	attExact, err := queryHistoryAttempts(context.Background(), s, historyQueryFilter{From: from, To: to, Limit: 10, Channel: "custom:c2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(attExact.Items) != 1 {
		t.Fatalf("attempt exact custom:c2 must match 1, got %d", len(attExact.Items))
	}
}

func TestWebUICustomChannelHelpers(t *testing.T) {
	html := readConfigWebUI(t)
	for _, needle := range []string{
		`function customChannelOf(a)`,
		`function isCustomRow(a)`,
		`custom:`,
		`tbody-usage-tier`,
		`chanTokens`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("webui custom helper missing %q", needle)
		}
	}
	// New channel form must be understood alongside the legacy merged row.
	if !strings.Contains(html, `k==="custom"||k.slice(0,7)==="custom:"`) && !strings.Contains(html, `k.slice(0,7)==="custom:"`) {
		t.Fatal("usage table must handle old and new custom keys")
	}
}
