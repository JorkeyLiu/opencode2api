package main

import (
	"context"
	"testing"
	"time"
)

func usageRowByModel(models []usageAggregateModel, name string) usageAggregateModel {
	for _, r := range models {
		if r.Model == name {
			return r
		}
	}
	return usageAggregateModel{}
}

func usageRowByUpstream(rows []usageAggregateUpstream, name string) usageAggregateUpstream {
	for _, r := range rows {
		if r.Upstream == name {
			return r
		}
	}
	return usageAggregateUpstream{}
}

func TestHistoryUsageRowLegacyCustomIncomplete(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)
	date := base.Format("20060102")
	ts := func(i int) string { return base.Add(time.Duration(i) * time.Second).UTC().Format(time.RFC3339Nano) }
	// Legacy custom row: input/output nonzero, total absent (no marker).
	writeProxyRequestsFile(t, hdir, date, 1, []historyRequestLine{
		{Time: ts(1), RequestID: "legacy-custom-1", Model: "legacy-m", Tier: "custom", Channel: "custom:alpha", Attempts: 1, Status: 200, Success: true, UsageReported: true, InputTokens: 20, OutputTokens: 10},
	})
	res, err := queryHistoryUsageAggregate(context.Background(), store, base.Add(-time.Hour), base.Add(time.Hour), 200)
	if err != nil {
		t.Fatal(err)
	}
	if !res.LegacyIncomplete {
		t.Fatal("legacy row must keep global legacy_incomplete")
	}
	m := usageRowByModel(res.Models, "legacy-m")
	if m.Model == "" {
		t.Fatalf("missing model row: %+v", res.Models)
	}
	if m.Total != 0 {
		t.Fatalf("legacy total must not be synthesized, got %d", m.Total)
	}
	if m.TotalComplete {
		t.Fatalf("legacy row total must be incomplete: %+v", m)
	}
	if m.ReasoningComplete {
		t.Fatalf("legacy row reasoning must be incomplete: %+v", m)
	}
	// No synthesis from input+output: 20+10 must not become 30.
	if m.Total == m.Input+m.Output {
		t.Fatalf("total must not equal input+output synthesis: %+v", m)
	}
	u := usageRowByUpstream(res.Upstreams, "custom:alpha")
	if u.Upstream == "" {
		t.Fatalf("missing upstream row: %+v", res.Upstreams)
	}
	if u.TotalComplete || u.ReasoningComplete {
		t.Fatalf("legacy upstream row must be incomplete: %+v", u)
	}
}

func TestHistoryUsageRowCompleteZeroStaysNumeric(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)
	date := base.Format("20060102")
	ts := base.UTC().Format(time.RFC3339Nano)
	writeProxyRequestsFile(t, hdir, date, 1, []historyRequestLine{
		{Time: ts, RequestID: "complete-zero-1", Model: "zero-m", Tier: "zen", Channel: "key", Attempts: 1, Status: 200, Success: true, UsageReported: true, InputTokens: 5, OutputTokens: 3, UsageDetailComplete: true},
	})
	res, err := queryHistoryUsageAggregate(context.Background(), store, base.Add(-time.Hour), base.Add(time.Hour), 200)
	if err != nil {
		t.Fatal(err)
	}
	if res.LegacyIncomplete {
		t.Fatalf("complete zero must not set legacy_incomplete: %+v", res)
	}
	m := usageRowByModel(res.Models, "zero-m")
	if !m.TotalComplete || !m.ReasoningComplete {
		t.Fatalf("complete zero row must be complete: %+v", m)
	}
	if m.Total != 0 || m.Reasoning != 0 {
		t.Fatalf("complete zero totals must stay numeric zero: %+v", m)
	}
}

func TestHistoryUsageRowMixedLegacyPlusCompleteIsIncomplete(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)
	date := base.Format("20060102")
	ts := func(i int) string { return base.Add(time.Duration(i) * time.Second).UTC().Format(time.RFC3339Nano) }
	writeProxyRequestsFile(t, hdir, date, 1, []historyRequestLine{
		{Time: ts(1), RequestID: "mix-legacy", Model: "mix-m", Tier: "zen", Channel: "key", Attempts: 1, Status: 200, Success: true, UsageReported: true, InputTokens: 20, OutputTokens: 10},
		{Time: ts(2), RequestID: "mix-complete", Model: "mix-m", Tier: "zen", Channel: "key", Attempts: 1, Status: 200, Success: true, UsageReported: true, InputTokens: 1, OutputTokens: 1, ReasoningTokens: 2, TotalTokens: 4, UsageDetailComplete: true},
	})
	res, err := queryHistoryUsageAggregate(context.Background(), store, base.Add(-time.Hour), base.Add(time.Hour), 200)
	if err != nil {
		t.Fatal(err)
	}
	m := usageRowByModel(res.Models, "mix-m")
	if m.TotalComplete || m.ReasoningComplete {
		t.Fatalf("mixed legacy+complete row must be incomplete: %+v", m)
	}
	// Partial sum is the sum of persisted totals only (0 + 4), never synthesized.
	if m.Total != 4 || m.Reasoning != 2 {
		t.Fatalf("mixed row must keep persisted partial sums without synthesis: %+v", m)
	}
	if m.Input != 21 || m.Output != 11 {
		t.Fatalf("mixed input/output must sum normally: %+v", m)
	}
}

func TestHistoryUsageRowCompleteNativeAndCustomSumNormally(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)
	date := base.Format("20060102")
	ts := func(i int) string { return base.Add(time.Duration(i) * time.Second).UTC().Format(time.RFC3339Nano) }
	writeProxyRequestsFile(t, hdir, date, 1, []historyRequestLine{
		{Time: ts(1), RequestID: "n1", Model: "native-m", Tier: "zen", Channel: "key", Attempts: 1, Status: 200, Success: true, UsageReported: true, InputTokens: 10, OutputTokens: 5, ReasoningTokens: 2, TotalTokens: 17, UsageDetailComplete: true},
		{Time: ts(2), RequestID: "n2", Model: "native-m", Tier: "zen", Channel: "key", Attempts: 1, Status: 200, Success: true, UsageReported: true, InputTokens: 3, OutputTokens: 3, ReasoningTokens: 1, TotalTokens: 7, UsageDetailComplete: true},
		{Time: ts(3), RequestID: "c1", Model: "custom-m", Tier: "custom", Channel: "custom:beta", Attempts: 1, Status: 200, Success: true, UsageReported: true, InputTokens: 7, OutputTokens: 7, TotalTokens: 14, UsageDetailComplete: true},
	})
	res, err := queryHistoryUsageAggregate(context.Background(), store, base.Add(-time.Hour), base.Add(time.Hour), 200)
	if err != nil {
		t.Fatal(err)
	}
	if res.LegacyIncomplete {
		t.Fatalf("all-complete rows must not set legacy_incomplete: %+v", res)
	}
	native := usageRowByModel(res.Models, "native-m")
	if !native.TotalComplete || !native.ReasoningComplete {
		t.Fatalf("complete native row must be complete: %+v", native)
	}
	if native.Total != 24 || native.Reasoning != 3 || native.Input != 13 || native.Output != 8 {
		t.Fatalf("complete native row must sum normally: %+v", native)
	}
	custom := usageRowByModel(res.Models, "custom-m")
	if !custom.TotalComplete || !custom.ReasoningComplete {
		t.Fatalf("complete custom row must be complete: %+v", custom)
	}
	if custom.Total != 14 {
		t.Fatalf("complete custom row must sum normally: %+v", custom)
	}
	// Range and grouping unchanged: zen upstream + custom:beta upstream present.
	uzen := usageRowByUpstream(res.Upstreams, "zen")
	if uzen.Upstream == "" || !uzen.TotalComplete || uzen.Total != 24 {
		t.Fatalf("zen upstream must sum normally: %+v", res.Upstreams)
	}
	ubeta := usageRowByUpstream(res.Upstreams, "custom:beta")
	if ubeta.Upstream == "" || !ubeta.TotalComplete || ubeta.Total != 14 {
		t.Fatalf("custom:beta upstream must sum normally: %+v", res.Upstreams)
	}
}
