package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func usageAggModels(items []usageAggregateModel) map[string]usageAggregateModel {
	out := make(map[string]usageAggregateModel, len(items))
	for _, r := range items {
		out[r.Model] = r
	}
	return out
}

func usageAggUpstreams(items []usageAggregateUpstream) map[string]usageAggregateUpstream {
	out := make(map[string]usageAggregateUpstream, len(items))
	for _, r := range items {
		out[r.Upstream] = r
	}
	return out
}

func TestHistoryUsageAggregateCallsVsUsageAndTokens(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-30 * time.Minute)
	date := base.Format("20060102")
	ts := func(i int) string { return base.Add(time.Duration(i) * time.Second).UTC().Format(time.RFC3339Nano) }
	writeProxyRequestsFile(t, hdir, date, 1, []historyRequestLine{
		{Time: ts(1), RequestID: "r1", Model: "m1", Tier: "zen", Channel: "key", Attempts: 1, Status: 200, Success: true, UsageReported: true, InputTokens: 100, OutputTokens: 40, CacheHitTokens: 10, ReasoningTokens: 5, TotalTokens: 145, UsageDetailComplete: true},
		{Time: ts(2), RequestID: "r2", Model: "m1", Tier: "zen", Channel: "key", Attempts: 1, Status: 200, Success: true, UsageReported: false},
		{Time: ts(3), RequestID: "r3", Model: "m2", Tier: "go", Channel: "key", Attempts: 1, Status: 500, Success: false, UsageReported: true, InputTokens: 10, OutputTokens: 2, ReasoningTokens: 1, TotalTokens: 13, UsageDetailComplete: true},
	})
	res, err := queryHistoryUsageAggregate(context.Background(), store, base.Add(-time.Hour), base.Add(time.Hour), 200)
	if err != nil {
		t.Fatal(err)
	}
	if res.Truncated {
		t.Fatal("unexpected truncation")
	}
	if res.TotalModels != 2 || res.TotalUpstreams != 2 {
		t.Fatalf("totals models=%d upstreams=%d %+v", res.TotalModels, res.TotalUpstreams, res)
	}
	byModel := usageAggModels(res.Models)
	m1 := byModel["m1"]
	// calls counts all valid request lines, usage_calls only reported.
	if m1.Calls != 2 || m1.UsageCalls != 1 {
		t.Fatalf("m1 calls=%+v", m1)
	}
	if m1.Input != 100 || m1.Output != 40 || m1.Cached != 10 || m1.Reasoning != 5 || m1.Total != 145 {
		t.Fatalf("m1 tokens gated to reported only: %+v", m1)
	}
	m2 := byModel["m2"]
	if m2.Calls != 1 || m2.UsageCalls != 1 || m2.Input != 10 || m2.Total != 13 {
		t.Fatalf("m2=%+v", m2)
	}
	// Totals mirror the same gate.
	if res.Totals.Calls != 3 || res.Totals.UsageCalls != 2 {
		t.Fatalf("totals=%+v", res.Totals)
	}
	if res.Totals.Input != 110 || res.Totals.Output != 42 || res.Totals.Cached != 10 || res.Totals.Reasoning != 6 || res.Totals.Total != 158 {
		t.Fatalf("totals tokens=%+v", res.Totals)
	}
	// Sorted by total_tokens desc.
	if res.Models[0].Model != "m1" {
		t.Fatalf("sort=%v", res.Models)
	}
	if res.From == "" || res.To == "" {
		t.Fatal("from/to must be set")
	}
	if res.LegacyIncomplete {
		t.Fatalf("explicit complete lines must not flag legacy_incomplete: %+v", res)
	}
}

func TestHistoryUsageAggregateCustomOldNewSplit(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-20 * time.Minute)
	date := base.Format("20060102")
	ts := func(i int) string { return base.Add(time.Duration(i) * time.Second).UTC().Format(time.RFC3339Nano) }
	writeProxyRequestsFile(t, hdir, date, 1, []historyRequestLine{
		{Time: ts(1), RequestID: "c0", Model: "m", Tier: "custom", Channel: "custom", Attempts: 1, Status: 200, Success: true, UsageReported: true, InputTokens: 5, OutputTokens: 5, TotalTokens: 10, UsageDetailComplete: true},
		{Time: ts(2), RequestID: "c1", Model: "m", Tier: "custom", Channel: "custom:alpha", Attempts: 1, Status: 200, Success: true, UsageReported: true, InputTokens: 7, OutputTokens: 7, TotalTokens: 14, UsageDetailComplete: true},
		{Time: ts(3), RequestID: "c2", Model: "m", Tier: "custom", Channel: "custom:beta", Attempts: 1, Status: 200, Success: true, UsageReported: false},
		{Time: ts(4), RequestID: "z1", Model: "m", Tier: "zen", Channel: "anonymous", Attempts: 1, Status: 200, Success: true, UsageReported: true, InputTokens: 3, OutputTokens: 3, TotalTokens: 6, UsageDetailComplete: true},
	})
	res, err := queryHistoryUsageAggregate(context.Background(), store, base.Add(-time.Hour), base.Add(time.Hour), 200)
	if err != nil {
		t.Fatal(err)
	}
	byUp := usageAggUpstreams(res.Upstreams)
	if _, ok := byUp["custom"]; !ok {
		t.Fatalf("legacy custom merged row missing: %+v", res.Upstreams)
	}
	if _, ok := byUp["custom:alpha"]; !ok {
		t.Fatalf("new custom:alpha row missing: %+v", res.Upstreams)
	}
	if _, ok := byUp["custom:beta"]; !ok {
		t.Fatalf("new custom:beta row missing: %+v", res.Upstreams)
	}
	if byUp["custom"].Calls != 1 || byUp["custom:alpha"].Calls != 1 || byUp["custom:beta"].Calls != 1 {
		t.Fatalf("custom calls=%+v", byUp)
	}
	if byUp["custom:beta"].UsageCalls != 0 || byUp["custom:beta"].Total != 0 {
		t.Fatalf("unreported custom:beta must gate tokens: %+v", byUp["custom:beta"])
	}
	if _, ok := byUp["zen"]; !ok {
		t.Fatalf("zen upstream must use tier: %+v", res.Upstreams)
	}
}

func TestHistoryUsageAggregateRangeSortLimit(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-40 * time.Minute)
	date := base.Format("20060102")
	oldTS := base.Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano)
	writeProxyRequestsFile(t, hdir, date, 1, []historyRequestLine{
		{Time: oldTS, RequestID: "old", Model: "old-model", Tier: "zen", Channel: "key", Attempts: 1, Status: 200, Success: true, UsageReported: true, InputTokens: 999, OutputTokens: 999, TotalTokens: 1998, UsageDetailComplete: true},
		{Time: base.Add(time.Second).UTC().Format(time.RFC3339Nano), RequestID: "a", Model: "b-model", Tier: "zen", Channel: "key", Attempts: 1, Status: 200, Success: true, UsageReported: true, InputTokens: 1, OutputTokens: 1, TotalTokens: 2, UsageDetailComplete: true},
		{Time: base.Add(2 * time.Second).UTC().Format(time.RFC3339Nano), RequestID: "b", Model: "a-model", Tier: "go", Channel: "key", Attempts: 1, Status: 200, Success: true, UsageReported: true, InputTokens: 50, OutputTokens: 50, TotalTokens: 100, UsageDetailComplete: true},
	})
	from := base.Add(-time.Hour)
	to := base.Add(2 * time.Hour)
	full, err := queryHistoryUsageAggregate(context.Background(), store, from, to, 200)
	if err != nil {
		t.Fatal(err)
	}
	if full.TotalModels != 2 {
		t.Fatalf("out-of-range row leaked: %+v", full.Models)
	}
	if full.Models[0].Model != "a-model" {
		t.Fatalf("total_tokens desc sort: %+v", full.Models)
	}
	lim, err := queryHistoryUsageAggregate(context.Background(), store, from, to, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !lim.Truncated || lim.TotalModels != 2 || len(lim.Models) != 1 || len(lim.Upstreams) != 1 {
		t.Fatalf("limit=%+v", lim)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := queryHistoryUsageAggregate(cancelled, store, from, to, 200); err == nil {
		t.Fatal("expected context error")
	}
}

func TestHistoryUsageAggregateLegacyIncompleteAndRedaction(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)
	date := base.Format("20060102")
	secret := "sk-secret-REDact-002"
	// Legacy line: usage reported but raw JSON lacks reasoning/total keys.
	legacy := `{"v":1,"kind":"request","time":"` + base.UTC().Format(time.RFC3339Nano) + `","request_id":"legacy1","model":"m","tier":"zen","channel":"key","usage_reported":true,"input_tokens":20,"output_tokens":10}` + "\n"
	if err := os.WriteFile(filepath.Join(hdir, "requests-"+date+"-001.ndjson"), []byte(legacy), 0600); err != nil {
		t.Fatal(err)
	}
	_ = secret
	res, err := queryHistoryUsageAggregate(context.Background(), store, base.Add(-time.Hour), base.Add(time.Hour), 200)
	if err != nil {
		t.Fatal(err)
	}
	if !res.LegacyIncomplete {
		t.Fatalf("legacy usage line must set legacy_incomplete: %+v", res)
	}
	if res.Totals.Input != 20 || res.Totals.Reasoning != 0 || res.Totals.Total != 0 {
		t.Fatalf("legacy tokens must not be estimated: %+v", res.Totals)
	}
	raw, _ := json.Marshal(res)
	for _, needle := range []string{"key_id", "session", "proxy", "request_id", "error", "http", "127.0.0.1"} {
		if strings.Contains(string(raw), needle) {
			t.Fatalf("usage-aggregate leaked %q: %s", needle, raw)
		}
	}
}

func TestHistoryUsageAggregateExplicitCompleteZeroStaysComplete(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)
	date := base.Format("20060102")
	ts := base.UTC().Format(time.RFC3339Nano)
	// New complete write with zero reasoning/total still carries the explicit marker.
	writeProxyRequestsFile(t, hdir, date, 1, []historyRequestLine{
		{Time: ts, RequestID: "new-zero", Model: "m", Tier: "zen", Channel: "key", Attempts: 1, Status: 200, Success: true, UsageReported: true, InputTokens: 20, OutputTokens: 10, UsageDetailComplete: true},
	})
	res, err := queryHistoryUsageAggregate(context.Background(), store, base.Add(-time.Hour), base.Add(time.Hour), 200)
	if err != nil {
		t.Fatal(err)
	}
	if res.LegacyIncomplete {
		t.Fatalf("explicit complete zero must not set legacy_incomplete: %+v", res)
	}
	if res.Totals.UsageCalls != 1 || res.Totals.Input != 20 || res.Totals.Reasoning != 0 || res.Totals.Total != 0 {
		t.Fatalf("complete zero tokens must accumulate without estimation: %+v", res.Totals)
	}
}

func TestHistoryUsageAggregateOldRowExplicitIncomplete(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)
	date := base.Format("20060102")
	ts := base.UTC().Format(time.RFC3339Nano)
	// Old row: UsageReported=true but explicit marker absent (false), even with
	// zero reasoning/total the aggregate must flag incomplete without key sniffing.
	writeProxyRequestsFile(t, hdir, date, 1, []historyRequestLine{
		{Time: ts, RequestID: "old-zero", Model: "m", Tier: "zen", Channel: "key", Attempts: 1, Status: 200, Success: true, UsageReported: true, InputTokens: 20, OutputTokens: 10},
	})
	res, err := queryHistoryUsageAggregate(context.Background(), store, base.Add(-time.Hour), base.Add(time.Hour), 200)
	if err != nil {
		t.Fatal(err)
	}
	if !res.LegacyIncomplete {
		t.Fatalf("old row without explicit marker must set legacy_incomplete: %+v", res)
	}
	if res.Totals.Reasoning != 0 || res.Totals.Total != 0 {
		t.Fatalf("old row tokens must not be estimated: %+v", res.Totals)
	}
}

func TestHistoryUsageAggregateEnqueueZeroMarksComplete(t *testing.T) {
	mon := NewMonitor()
	sinkDir := t.TempDir()
	cfgPath := filepath.Join(sinkDir, "config.json")
	if err := os.WriteFile(cfgPath, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	store := OpenHistoryStore(cfgPath, HistoryConfig{Enabled: true, Directory: "h", RetentionDays: 7, MaxBytesMB: 32}, nil, NewSecretRedactor())
	defer store.Close()
	mon.SetHistorySink(store)
	meta := &requestMeta{
		Model: "zero-complete-m", Tier: "zen", Channel: "key", KeyID: "tail1",
		Request: "req-zero-complete-1", UsageReported: true,
		Usage: bridgeUsage{Input: 8, Output: 4},
	}
	mon.Record("/v1/chat/completions", 200, 5*time.Millisecond, meta)
	if !store.CloseWithTimeout(5 * time.Second) {
		t.Fatal("history close timed out")
	}
	hdir := store.historyReadDir()
	if hdir == "" {
		hdir = ResolveHistoryDir(cfgPath, "h")
	}
	entries, err := os.ReadDir(hdir)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "requests-") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(hdir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || !strings.Contains(line, "req-zero-complete-1") {
				continue
			}
			if !strings.Contains(line, `"usage_detail_complete":true`) {
				t.Fatalf("zero-usage enqueue must persist explicit complete marker: %s", line)
			}
			var v historyRequestLine
			if err := json.Unmarshal([]byte(line), &v); err != nil {
				t.Fatal(err)
			}
			if !v.UsageDetailComplete {
				t.Fatalf("decoded marker must be true: %+v", v)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("zero-usage persisted request line not found")
	}
}

func TestHistoryUsageAggregateNewFieldsPersisted(t *testing.T) {
	mon := NewMonitor()
	sinkDir := t.TempDir()
	cfgPath := filepath.Join(sinkDir, "config.json")
	if err := os.WriteFile(cfgPath, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	store := OpenHistoryStore(cfgPath, HistoryConfig{Enabled: true, Directory: "h", RetentionDays: 7, MaxBytesMB: 32}, nil, NewSecretRedactor())
	defer store.Close()
	mon.SetHistorySink(store)
	meta := &requestMeta{
		Model: "persist-m", Tier: "zen", Channel: "key", KeyID: "tail1",
		Request: "req-persist-1", UsageReported: true,
		Usage: bridgeUsage{Input: 30, Output: 12, Cached: 4, Reasoning: 6, Total: 42},
	}
	mon.Record("/v1/chat/completions", 200, 5*time.Millisecond, meta)
	if !store.CloseWithTimeout(5 * time.Second) {
		t.Fatal("history close timed out")
	}
	hdir := store.historyReadDir()
	if hdir == "" {
		// Store went inactive; re-resolve via config dir for the assertion.
		hdir = ResolveHistoryDir(cfgPath, "h")
	}
	entries, err := os.ReadDir(hdir)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "requests-") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(hdir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || !strings.Contains(line, "req-persist-1") {
				continue
			}
			var v historyRequestLine
			if err := json.Unmarshal([]byte(line), &v); err != nil {
				t.Fatal(err)
			}
			if v.ReasoningTokens != 6 || v.TotalTokens != 42 || v.InputTokens != 30 || v.OutputTokens != 12 {
				t.Fatalf("persisted fields=%+v", v)
			}
			if !v.UsageReported {
				t.Fatal("usage_reported must persist")
			}
			if !v.UsageDetailComplete {
				t.Fatalf("usage_detail_complete must persist for reported lines: %+v", v)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("persisted request line not found")
	}
	// Old request lines without the new keys still decode as v1 (no upgrade break).
	var old historyRequestLine
	if err := json.Unmarshal([]byte(`{"v":1,"kind":"request","time":"2026-01-01T00:00:00Z","request_id":"o","model":"m","channel":"key"}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.ReasoningTokens != 0 || old.TotalTokens != 0 {
		t.Fatalf("old line must decode zero: %+v", old)
	}
	if old.UsageDetailComplete {
		t.Fatalf("old line must decode incomplete marker false: %+v", old)
	}
}

func TestHistoryUsageAggregateHTTP(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	store := OpenHistoryStore(cfgPath, HistoryConfig{Enabled: true, Directory: "h", RetentionDays: 7, MaxBytesMB: 128}, nil, NewSecretRedactor())
	defer store.Close()
	mon := NewMonitor()
	admin, token, _ := historyAuthedAdmin(t, store, mon)
	get := func(path string, withAuth bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if withAuth {
			req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
		}
		rec := httptest.NewRecorder()
		admin.Handler().ServeHTTP(rec, req)
		return rec
	}
	if rec := get("/api/history/usage-aggregate", false); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauth=%d", rec.Code)
	}
	rec := get("/api/history/usage-aggregate", true)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("no-store=%q", got)
	}
	var body usageAggregateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Models == nil || body.Upstreams == nil {
		t.Fatal("models/upstreams must be non-nil")
	}
	now := time.Now().UTC()
	from := now.Add(time.Hour).Format(time.RFC3339)
	to := now.Format(time.RFC3339)
	if rec := get("/api/history/usage-aggregate?from="+from+"&to="+to, true); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid range=%d", rec.Code)
	}
	wideFrom := now.Add(-8 * 24 * time.Hour).Format(time.RFC3339)
	wideTo := now.Format(time.RFC3339)
	if rec := get("/api/history/usage-aggregate?from="+wideFrom+"&to="+wideTo, true); rec.Code != http.StatusBadRequest {
		t.Fatalf("wide range=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := get("/api/history/usage-aggregate?limit=zzz", true); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad limit=%d", rec.Code)
	}
}

func TestHistoryUsageAggregateDisabled(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	store := OpenHistoryStore(cfgPath, HistoryConfig{Enabled: false, RetentionDays: 7, MaxBytesMB: 128}, nil, NewSecretRedactor())
	defer store.Close()
	mon := NewMonitor()
	admin, token, _ := historyAuthedAdmin(t, store, mon)
	req := httptest.NewRequest(http.MethodGet, "/api/history/usage-aggregate", nil)
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	rec := httptest.NewRecorder()
	admin.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d", rec.Code)
	}
	var body usageAggregateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Active || len(body.Models) != 0 || len(body.Upstreams) != 0 {
		t.Fatalf("disabled must be inactive empty: %+v", body)
	}
}

func TestHistoryUsageAggregateTimeout(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)
	date := base.Format("20060102")
	writeProxyRequestsFile(t, hdir, date, 1, []historyRequestLine{
		{Time: base.UTC().Format(time.RFC3339Nano), RequestID: "t1", Model: "m", Tier: "zen", Channel: "key", Attempts: 1, Status: 200, Success: true, UsageReported: true, InputTokens: 1, OutputTokens: 1, TotalTokens: 2},
	})
	oldHook := historyScanHook
	historyScanHook = func() {
		time.Sleep(50 * time.Millisecond)
	}
	defer func() { historyScanHook = oldHook }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err := queryHistoryUsageAggregate(ctx, store, base.Add(-time.Hour), base.Add(time.Hour), 200); err == nil {
		t.Fatal("expected timeout error")
	}
}
