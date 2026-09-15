package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func writeProxyAttemptsFile(t *testing.T, dir, date string, seq int, rows []historyAttemptLine) {
	t.Helper()
	var sb strings.Builder
	for _, v := range rows {
		v.V = historySchemaV
		v.Kind = string(historyKindAttempt)
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		sb.Write(data)
		sb.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(dir, "attempts-"+date+"-"+padSeq(seq)+".ndjson"), []byte(sb.String()), 0600); err != nil {
		t.Fatal(err)
	}
}

func writeProxyRequestsFile(t *testing.T, dir, date string, seq int, rows []historyRequestLine) {
	t.Helper()
	var sb strings.Builder
	for _, v := range rows {
		v.V = historySchemaV
		v.Kind = string(historyKindRequest)
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		sb.Write(data)
		sb.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(dir, "requests-"+date+"-"+padSeq(seq)+".ndjson"), []byte(sb.String()), 0600); err != nil {
		t.Fatal(err)
	}
}

func proxyStatsByKey(items []proxyStatRow) map[string]proxyStatRow {
	out := make(map[string]proxyStatRow, len(items))
	for _, r := range items {
		out[r.ProxyPool+"\x00"+r.ProxyNode] = r
	}
	return out
}

func TestHistoryProxyStatsAggregateCorrectness(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-30 * time.Minute)
	date := base.Format("20060102")
	ts := func(i int) string { return base.Add(time.Duration(i) * time.Second).UTC().Format(time.RFC3339Nano) }
	writeProxyAttemptsFile(t, hdir, date, 1, []historyAttemptLine{
		{Time: ts(1), RequestID: "r1", Model: "m", Tier: "zen", Attempt: 1, Channel: "key", ProxyPool: "shared", ProxyNode: "direct", Status: 200, DurationMS: 100, Success: true, FailureClass: AttemptClassSuccess},
		{Time: ts(2), RequestID: "r1", Model: "m", Tier: "zen", Attempt: 2, Channel: "key", ProxyPool: "shared", ProxyNode: "direct", Status: 429, DurationMS: 200, Success: false, FailureClass: AttemptClassRateLimited},
		{Time: ts(3), RequestID: "r2", Model: "m", Tier: "zen", Attempt: 1, Channel: "key", ProxyPool: "shared", ProxyNode: "direct", Status: 500, DurationMS: 300, Success: false, FailureClass: AttemptClassUpstreamFailure},
		{Time: ts(4), RequestID: "r3", Model: "m", Tier: "zen", Attempt: 1, Channel: "key", ProxyPool: "shared", ProxyNode: "http://127.0.0.1:8080", Status: 200, DurationMS: 50, Success: true, FailureClass: AttemptClassSuccess},
		{Time: ts(5), RequestID: "r4", Model: "m", Tier: "zen", Attempt: 1, Channel: "key", ProxyPool: "other", ProxyNode: "direct", Status: 401, DurationMS: 80, Success: false, FailureClass: AttemptClassAuthFailure},
		{Time: ts(6), RequestID: "r5", Model: "m", Tier: "zen", Attempt: 1, Channel: "key", ProxyPool: "", ProxyNode: "", Status: 200, DurationMS: 10, Success: true, FailureClass: AttemptClassSuccess},
	})
	writeProxyRequestsFile(t, hdir, date, 1, []historyRequestLine{
		{Time: ts(3), RequestID: "r2", Model: "m", Tier: "zen", Channel: "key", ProxyPool: "shared", ProxyNode: "direct", Attempts: 2, Status: 500, DurationMS: 300, Success: false, UsageReported: true, InputTokens: 100, OutputTokens: 40, CacheHitTokens: 10},
		{Time: ts(4), RequestID: "r3", Model: "m", Tier: "zen", Channel: "key", ProxyPool: "shared", ProxyNode: "http://127.0.0.1:8080", Attempts: 1, Status: 200, DurationMS: 50, Success: true, UsageReported: false},
		{Time: ts(7), RequestID: "r6", Model: "m", Tier: "zen", Channel: "key", ProxyPool: "shared", ProxyNode: "direct", Attempts: 1, Status: 200, DurationMS: 20, Success: true, UsageReported: false},
	})
	from := base.Add(-time.Hour)
	to := base.Add(time.Hour)
	res, err := queryHistoryProxyStats(context.Background(), store, from, to, 200)
	if err != nil {
		t.Fatal(err)
	}
	if res.Truncated {
		t.Fatal("unexpected truncation")
	}
	if res.TotalProxies != 4 {
		t.Fatalf("total=%d rows=%v", res.TotalProxies, res.Items)
	}
	byKey := proxyStatsByKey(res.Items)
	// Pool-qualified: shared/direct vs other/direct are distinct.
	shared := byKey["shared\x00direct"]
	if shared.Attempts != 3 || shared.Success != 1 || shared.Failed != 2 {
		t.Fatalf("shared/direct counts=%+v", shared)
	}
	if shared.RateLimited != 1 || shared.UpstreamFailures != 1 || shared.AuthFailures != 0 || shared.TransportFailures != 0 || shared.OtherFailures != 0 {
		t.Fatalf("shared/direct buckets=%+v", shared)
	}
	if shared.SuccessRate != float64(1)/float64(3) {
		t.Fatalf("success_rate=%v", shared.SuccessRate)
	}
	if shared.AvgDurationMS != float64(600)/float64(3) {
		t.Fatalf("avg=%v", shared.AvgDurationMS)
	}
	// Request tokens attach to the same pool-qualified proxy; unreported requests do not create usage.
	if shared.UsageRequests != 1 || shared.InputTokens != 100 || shared.OutputTokens != 40 || shared.CacheHitTokens != 10 || !shared.HasUsage {
		t.Fatalf("shared/direct usage=%+v", shared)
	}
	node := byKey["shared\x00http://127.0.0.1:8080"]
	if node.Attempts != 1 || !node.HasUsage == false && node.UsageRequests != 0 {
		t.Fatalf("node usage must be zero without reported requests: %+v", node)
	}
	if node.UsageRequests != 0 || node.HasUsage {
		t.Fatalf("node usage must be empty: %+v", node)
	}
	other := byKey["other\x00direct"]
	if other.Attempts != 1 || other.AuthFailures != 1 {
		t.Fatalf("other/direct=%+v", other)
	}
	empty := byKey["\x00unavailable"]
	if empty.Attempts != 1 || empty.Success != 1 {
		t.Fatalf("empty identity=%+v", empty)
	}
	// Deterministic sort: most attempts first.
	if res.Items[0].ProxyPool != "shared" || res.Items[0].ProxyNode != "direct" {
		t.Fatalf("sort order=%v", res.Items)
	}
	// 2xx never counts as failure: each row's buckets must sum to failed.
	for _, r := range res.Items {
		if r.Success+r.Failed != r.Attempts {
			t.Fatalf("row %+v success+failed != attempts", r)
		}
		if r.RateLimited+r.AuthFailures+r.UpstreamFailures+r.TransportFailures+r.OtherFailures != r.Failed {
			t.Fatalf("row %+v buckets != failed", r)
		}
	}
	// Response carries no key/body diagnostics.
	raw, _ := json.Marshal(res)
	for _, needle := range []string{"key_id", "error_hint", "request_id", "client_session"} {
		if strings.Contains(string(raw), needle) {
			t.Fatalf("proxy-stats leaked %q", needle)
		}
	}
}

func TestHistoryProxyStatsFailureBuckets(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-20 * time.Minute)
	date := base.Format("20060102")
	ts := func(i int) string { return base.Add(time.Duration(i) * time.Second).UTC().Format(time.RFC3339Nano) }
	writeProxyAttemptsFile(t, hdir, date, 1, []historyAttemptLine{
		{Time: ts(1), RequestID: "a", Model: "m", Attempt: 1, Channel: "key", ProxyPool: "p", ProxyNode: "n", Status: 429, DurationMS: 1, Success: false, FailureClass: AttemptClassRateLimited},
		{Time: ts(2), RequestID: "b", Model: "m", Attempt: 1, Channel: "key", ProxyPool: "p", ProxyNode: "n", Status: 401, DurationMS: 1, Success: false, FailureClass: AttemptClassAuthFailure},
		{Time: ts(3), RequestID: "c", Model: "m", Attempt: 1, Channel: "key", ProxyPool: "p", ProxyNode: "n", Status: 403, DurationMS: 1, Success: false, FailureClass: AttemptClassAuthFailure},
		{Time: ts(4), RequestID: "d", Model: "m", Attempt: 1, Channel: "key", ProxyPool: "p", ProxyNode: "n", Status: 502, DurationMS: 1, Success: false, FailureClass: AttemptClassUpstreamFailure},
		{Time: ts(5), RequestID: "e", Model: "m", Attempt: 1, Channel: "key", ProxyPool: "p", ProxyNode: "n", Status: 0, DurationMS: 1, Success: false, FailureClass: AttemptClassTransportFailure},
		{Time: ts(6), RequestID: "f", Model: "m", Attempt: 1, Channel: "key", ProxyPool: "p", ProxyNode: "n", Status: 400, DurationMS: 1, Success: false, FailureClass: AttemptClassClientRejected},
		{Time: ts(7), RequestID: "g", Model: "m", Attempt: 1, Channel: "key", ProxyPool: "p", ProxyNode: "n", Status: 408, DurationMS: 1, Success: false, FailureClass: AttemptClassTransientClient},
		{Time: ts(8), RequestID: "h", Model: "m", Attempt: 1, Channel: "key", ProxyPool: "p", ProxyNode: "n", Status: 200, DurationMS: 1, Success: true, FailureClass: AttemptClassSuccess},
	})
	res, err := queryHistoryProxyStats(context.Background(), store, base.Add(-time.Hour), base.Add(time.Hour), 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 1 {
		t.Fatalf("items=%d", len(res.Items))
	}
	r := res.Items[0]
	if r.Attempts != 8 || r.Success != 1 || r.Failed != 7 {
		t.Fatalf("counts=%+v", r)
	}
	if r.RateLimited != 1 || r.AuthFailures != 1 || r.UpstreamFailures != 2 || r.TransportFailures != 1 || r.OtherFailures != 2 {
		t.Fatalf("buckets=%+v", r)
	}
}

func TestHistoryProxyStatsRangeSortLimit(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-40 * time.Minute)
	date := base.Format("20060102")
	oldTS := base.Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano)
	// Out-of-range row must be excluded.
	writeProxyAttemptsFile(t, hdir, date, 1, []historyAttemptLine{
		{Time: oldTS, RequestID: "old", Model: "m", Attempt: 1, Channel: "key", ProxyPool: "p", ProxyNode: "old-node", Status: 200, DurationMS: 1, Success: true, FailureClass: AttemptClassSuccess},
	})
	for i := 0; i < 5; i++ {
		pool := "b"
		node := "n" + strconv.Itoa(i)
		attempts := 1
		switch i {
		case 0:
			pool, node, attempts = "a", "x", 5
		case 1:
			pool, node, attempts = "a", "y", 5
		case 2:
			pool, node, attempts = "b", "m", 3
		}
		var rows []historyAttemptLine
		for k := 0; k < attempts; k++ {
			rows = append(rows, historyAttemptLine{Time: base.Add(time.Duration(i*10+k) * time.Second).UTC().Format(time.RFC3339Nano), RequestID: "r" + strconv.Itoa(i) + "-" + strconv.Itoa(k), Model: "m", Attempt: 1, Channel: "key", ProxyPool: pool, ProxyNode: node, Status: 200, DurationMS: 10, Success: true, FailureClass: AttemptClassSuccess})
		}
		writeProxyAttemptsFile(t, hdir, date, 10+i, rows)
	}
	from := base.Add(-time.Hour)
	to := base.Add(2 * time.Hour)
	full, err := queryHistoryProxyStats(context.Background(), store, from, to, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(full.Items) != 5 {
		t.Fatalf("items=%d %+v", len(full.Items), full.Items)
	}
	for _, r := range full.Items {
		if r.ProxyNode == "old-node" {
			t.Fatal("out-of-range row leaked")
		}
	}
	// Tie breaker: equal attempts sorted by pool/node.
	if full.Items[0].ProxyPool != "a" || full.Items[0].ProxyNode != "x" || full.Items[1].ProxyNode != "y" {
		t.Fatalf("tie order=%v", full.Items)
	}
	lim, err := queryHistoryProxyStats(context.Background(), store, from, to, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !lim.Truncated || lim.TotalProxies != 5 || len(lim.Items) != 2 {
		t.Fatalf("limit=%+v", lim)
	}
	if lim.Items[0].Attempts != 5 || lim.Items[1].Attempts != 5 {
		t.Fatalf("limited order=%v", lim.Items)
	}
	// Cancelled context aborts without partial results.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := queryHistoryProxyStats(cancelled, store, from, to, 200); err == nil {
		t.Fatal("expected context error")
	}
}

func TestHistoryProxyStatsRedaction(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)
	date := base.Format("20060102")
	secret := "sk-secret-REDact-001"
	writeProxyAttemptsFile(t, hdir, date, 1, []historyAttemptLine{
		{Time: base.UTC().Format(time.RFC3339Nano), RequestID: "red", Model: "m", Attempt: 1, KeyID: secret, Channel: "key", ProxyPool: "shared", ProxyNode: "http://user:" + secret + "@127.0.0.1:8080", Status: 500, DurationMS: 5, Success: false, FailureClass: AttemptClassUpstreamFailure},
	})
	res, err := queryHistoryProxyStats(context.Background(), store, base.Add(-time.Hour), base.Add(time.Hour), 200)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(res)
	if strings.Contains(string(raw), secret) || strings.Contains(string(raw), "user:") {
		t.Fatalf("proxy-stats leaked credentials: %s", raw)
	}
	if len(res.Items) != 1 {
		t.Fatalf("items=%d", len(res.Items))
	}
	if strings.Contains(res.Items[0].ProxyNode, "@") && strings.Contains(res.Items[0].ProxyNode, "user") {
		t.Fatalf("node not redacted: %q", res.Items[0].ProxyNode)
	}
}

func TestHistoryProxyStatsHTTP(t *testing.T) {
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
	if rec := get("/api/history/proxy-stats", false); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauth=%d", rec.Code)
	}
	rec := get("/api/history/proxy-stats", true)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("no-store=%q", got)
	}
	var body proxyStatsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Items == nil {
		t.Fatal("items must be non-nil")
	}
	// Invalid range and limit reuse existing controls.
	now := time.Now().UTC()
	from := now.Add(time.Hour).Format(time.RFC3339)
	to := now.Format(time.RFC3339)
	if rec := get("/api/history/proxy-stats?from="+from+"&to="+to, true); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid range=%d", rec.Code)
	}
	wideFrom := now.Add(-8 * 24 * time.Hour).Format(time.RFC3339)
	wideTo := now.Format(time.RFC3339)
	if rec := get("/api/history/proxy-stats?from="+wideFrom+"&to="+wideTo, true); rec.Code != http.StatusBadRequest {
		t.Fatalf("wide range=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := get("/api/history/proxy-stats?limit=zzz", true); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad limit=%d", rec.Code)
	}
	// Attempt diagnostics stay preserved on the existing endpoint.
	base := time.Now().UTC().Truncate(time.Second)
	hdir := store.historyReadDir()
	writeProxyAttemptsFile(t, hdir, base.Format("20060102"), 9, []historyAttemptLine{
		{Time: base.Format(time.RFC3339Nano), RequestID: "diag400", Model: "m", Attempt: 1, Channel: "key", ProxyPool: "p", ProxyNode: "n", Status: 400, DurationMS: 1, Success: false, FailureClass: AttemptClassClientRejected, ErrorHint: "unknown", ErrorType: "invalid_request", ErrorCode: "bad", ErrorFingerprint: "e400_a1b2c3d4"},
	})
	arec := get("/api/history/attempts?from="+base.Add(-time.Hour).Format(time.RFC3339)+"&to="+base.Add(time.Hour).Format(time.RFC3339), true)
	if arec.Code != http.StatusOK || !strings.Contains(arec.Body.String(), "client_rejected") {
		t.Fatalf("attempt diagnostics lost: %d %s", arec.Code, arec.Body.String())
	}
}

func TestHistoryProxyStatsDisabled(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	store := OpenHistoryStore(cfgPath, HistoryConfig{Enabled: false, RetentionDays: 7, MaxBytesMB: 128}, nil, NewSecretRedactor())
	defer store.Close()
	mon := NewMonitor()
	admin, token, _ := historyAuthedAdmin(t, store, mon)
	req := httptest.NewRequest(http.MethodGet, "/api/history/proxy-stats", nil)
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	rec := httptest.NewRecorder()
	admin.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d", rec.Code)
	}
	var body proxyStatsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Active || len(body.Items) != 0 {
		t.Fatalf("disabled must be inactive empty: %+v", body)
	}
}
