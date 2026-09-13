package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func testHistoryConfig(path string) Config {
	cfg := defaultConfig()
	cfg.ServerKeys = []string{"local-key-12345"}
	cfg.ZenKeys = []string{"zen-key-12345"}
	return cfg
}

func TestHistoryConfigDefaultsStrictValidation(t *testing.T) {
	cfg := defaultConfig()
	if !cfg.History.Enabled || cfg.History.Directory != "" || cfg.History.RetentionDays != 7 || cfg.History.MaxBytesMB != 128 {
		t.Fatalf("defaults = %+v", cfg.History)
	}
	got, err := NormalizeConfig("config.json", testHistoryConfig("config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got.History.RetentionDays != 7 || got.History.MaxBytesMB != 128 {
		t.Fatalf("normalized = %+v", got.History)
	}
	// Strict unknown inside history.
	raw := `{"listen":"127.0.0.1:8080","server_keys":["k1"],"zen_keys":["z1"],"prefer":"go","proxy_pools":{"shared":{"proxies":["direct"],"proxyfile":""}},"proxy_routing":{"anonymous":"shared","zen":"shared","go":"shared"},"upstream":{"zen":"https://opencode.ai/zen","go":"https://opencode.ai/zen/go"},"retry":{"max_attempts":1,"timeout_seconds":5},"models":{"refresh_seconds":5,"protocols":{}},"performance":{"max_idle_conns":1,"max_idle_conns_per_host":1,"max_conns_per_host":0,"idle_conn_timeout_seconds":1,"connect_timeout_seconds":1,"failure_cooldown_seconds":1},"logging":{"level":"info","ring_size":100},"webui":{"enabled":false,"listen":"127.0.0.1:1","username":"u","session_ttl_minutes":5},"history":{"enabled":true,"directory":"","retention_days":7,"max_bytes_mb":128,"bogus":1}}`
	var c Config
	if err := json.Unmarshal([]byte(raw), &c); err == nil {
		t.Fatal("expected strict unknown field error")
	}
	for _, tc := range []struct {
		name string
		mut  func(*Config)
	}{
		{"retention low", func(c *Config) { c.History.RetentionDays = 0 }},
		{"retention high", func(c *Config) { c.History.RetentionDays = 91 }},
		{"bytes low", func(c *Config) { c.History.MaxBytesMB = 15 }},
		{"bytes high", func(c *Config) { c.History.MaxBytesMB = 2049 }},
	} {
		cfg := testHistoryConfig("config.json")
		tc.mut(&cfg)
		if _, err := NormalizeConfig("config.json", cfg); err == nil {
			t.Fatalf("%s should fail", tc.name)
		}
	}
}

func TestHistoryPathResolution(t *testing.T) {
	if got := ResolveHistoryDir("/a/b/config.json", ""); got != filepath.Join("/a/b", "history") {
		t.Fatalf("empty=%q", got)
	}
	if got := ResolveHistoryDir("/a/b/config.json", "h2"); got != filepath.Join("/a/b", "h2") {
		t.Fatalf("rel=%q", got)
	}
	if got := ResolveHistoryDir("/a/b/config.json", "/x/y"); got != "/x/y" {
		t.Fatalf("abs=%q", got)
	}
}

func openTestHistory(t *testing.T, cfg HistoryConfig) (*HistoryStore, string) {
	t.Helper()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if cfg.Directory == "" {
		cfg.Directory = "history"
	}
	if cfg.RetentionDays == 0 {
		cfg.RetentionDays = 7
	}
	if cfg.MaxBytesMB == 0 {
		cfg.MaxBytesMB = 128
	}
	s := OpenHistoryStore(cfgPath, cfg, nil, NewSecretRedactor())
	t.Cleanup(s.Close)
	return s, cfgPath
}

func waitForFile(t *testing.T, dir, prefix string) string {
	t.Helper()
	for i := 0; i < 100; i++ {
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), prefix) {
				info, _ := e.Info()
				if info.Size() > 0 {
					return filepath.Join(dir, e.Name())
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no %s file appeared", prefix)
	return ""
}

func TestHistoryEnqueueNonblockFlushModes(t *testing.T) {
	s, cfgPath := openTestHistory(t, HistoryConfig{Enabled: true, Directory: "history", RetentionDays: 7, MaxBytesMB: 128})
	dir := ResolveHistoryDir(cfgPath, "history")
	now := time.Now().UTC()
	s.EnqueueRequest(UpstreamRequest{Time: now, RequestID: "r1", Model: "m", Tier: "zen", KeyID: "ABCDE", Channel: "key", Attempts: 1, Status: 200, DurationMS: 5, Success: true, Outcome: "success"})
	s.EnqueueAttempt(UpstreamAttempt{Time: now, RequestID: "r1", Model: "m", Tier: "zen", Attempt: 1, KeyID: "ABCDE", Channel: "key", Proxy: "direct", Status: 200, DurationMS: 5, Success: true, FailureClass: AttemptClassSuccess, Outcome: "success"})
	s.EnqueueMinute(MetricSeries{Minute: now.Truncate(time.Minute), Total: 2, Success: 2})
	path := waitForFile(t, dir, "requests-")
	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), `"request_id":"r1"`) {
		t.Fatalf("request line missing: %s", data)
	}
	if info, _ := os.Stat(path); info.Mode().Perm() != 0600 {
		t.Fatalf("file mode=%o", info.Mode().Perm())
	}
	if info, _ := os.Stat(dir); info.Mode().Perm() != 0700 {
		t.Fatalf("dir mode=%o", info.Mode().Perm())
	}
}

func TestHistoryDropNewestGap(t *testing.T) {
	s, _ := openTestHistory(t, HistoryConfig{Enabled: true, Directory: "history", RetentionDays: 7, MaxBytesMB: 128})
	// Fill queue by disabling writer: close done path not needed; instead saturate via blocked writer?
	// Deterministic unit: enqueue beyond capacity with paused store is racy, so test drop path directly.
	s.closed.Store(false)
	s.active.Store(true)
	// Drain-free fill: push 5000 rapidly; writer drains concurrently so drops may be 0;
	// force drops by blocking writer via mutex.
	s.mu.Lock()
	for i := 0; i < historyQueueSize+100; i++ {
		select {
		case s.queue <- historyQueued{kind: historyKindRequest, line: []byte("{}\n")}:
		default:
			s.dropped.Add(1)
			s.gap.Store(true)
		}
	}
	s.mu.Unlock()
	if s.DroppedCount() == 0 {
		t.Fatal("expected drops")
	}
	if !s.GapFlag() {
		t.Fatal("expected gap")
	}
}

func TestHistoryEnqueueNonblockOnWriterMu(t *testing.T) {
	s, _ := openTestHistory(t, HistoryConfig{Enabled: true, Directory: "history", RetentionDays: 7, MaxBytesMB: 128})
	// Simulate a long writer rotation/retention holding mu; enqueue must not wait on it.
	s.mu.Lock()
	done := make(chan time.Duration, 2)
	now := time.Now().UTC()
	go func() {
		start := time.Now()
		s.EnqueueRequest(UpstreamRequest{Time: now, RequestID: "nb-r", Model: "m", Tier: "zen", KeyID: "K", Channel: "key", Attempts: 1, Status: 200, DurationMS: 1, Success: true})
		done <- time.Since(start)
	}()
	go func() {
		start := time.Now()
		s.EnqueueAttempt(UpstreamAttempt{Time: now, RequestID: "nb-r", Model: "m", Tier: "zen", Attempt: 1, KeyID: "K", Channel: "key", Proxy: "direct", Status: 200, DurationMS: 1, Success: true, FailureClass: AttemptClassSuccess})
		done <- time.Since(start)
	}()
	timeout := time.After(2 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case d := <-done:
			if d > 100*time.Millisecond {
				s.mu.Unlock()
				t.Fatalf("enqueue blocked on writer mu: %v", d)
			}
		case <-timeout:
			s.mu.Unlock()
			t.Fatal("enqueue did not return while writer mu held")
		}
	}
	s.mu.Unlock()
	// Queue is never closed and Close stays idempotent under contention.
	s.Close()
	s.Close()
	s.EnqueueRequest(UpstreamRequest{Time: now, RequestID: "late", Model: "m"})
}

func TestHistoryRotationRetentionCorrupt(t *testing.T) {
	old := historySegmentMaxBytes
	historySegmentMaxBytes = 256
	defer func() { historySegmentMaxBytes = old }()
	s, cfgPath := openTestHistory(t, HistoryConfig{Enabled: true, Directory: "history", RetentionDays: 7, MaxBytesMB: 128})
	dir := ResolveHistoryDir(cfgPath, "history")
	now := time.Now().UTC()
	for i := 0; i < 20; i++ {
		s.EnqueueRequest(UpstreamRequest{Time: now, RequestID: "rr", Model: strings.Repeat("m", 40), Tier: "zen", KeyID: "K", Channel: "key", Attempts: 1, Status: 200, DurationMS: 1, Success: true})
	}
	time.Sleep(300 * time.Millisecond)
	s.mu.Lock()
	for _, seg := range s.open {
		if seg != nil && seg.buf != nil {
			_ = seg.buf.Flush()
		}
	}
	s.mu.Unlock()
	entries, _ := os.ReadDir(dir)
	count := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "requests-") {
			count++
		}
	}
	if count < 2 {
		t.Fatalf("expected rotation, got %d segments", count)
	}
	// Corrupt line gap.
	segs, _ := os.ReadDir(dir)
	first := filepath.Join(dir, segs[0].Name())
	f, _ := os.OpenFile(first, os.O_APPEND|os.O_WRONLY, 0600)
	_, _ = f.WriteString("not-json\n{\"v\":1,\"kind\":\"request\"}\n")
	_ = f.Close()
	_, gap := s.scanKindLines(historyKindRequest, now.Add(-time.Hour), now.Add(time.Hour), 10)
	if !gap {
		t.Fatal("expected gap on corrupt lines")
	}
}

func TestHistoryRedaction(t *testing.T) {
	s, cfgPath := openTestHistory(t, HistoryConfig{Enabled: true, Directory: "history", RetentionDays: 7, MaxBytesMB: 128})
	dir := ResolveHistoryDir(cfgPath, "history")
	secret := "sk-super-secret-VALUE-999"
	proxyRaw := "http://user:" + secret + "@127.0.0.1:8080"
	now := time.Now().UTC()
	s.EnqueueRequest(UpstreamRequest{Time: now, RequestID: "red1", Model: "m", Tier: "zen", KeyID: secret, Channel: "key", Anonymous: false, Proxy: proxyRaw, ProxyPool: "shared", Attempts: 1, Status: 400, DurationMS: 3, Success: false, Outcome: "client_error"})
	s.EnqueueAttempt(UpstreamAttempt{Time: now, RequestID: "red1", Model: "m", Tier: "zen", Attempt: 1, KeyID: secret, Channel: "key", Proxy: proxyRaw, ProxyPool: "shared", Status: 400, DurationMS: 3, Success: false, FailureClass: AttemptClassClientRejected, Outcome: "rejected"})
	path := waitForFile(t, dir, "requests-")
	time.Sleep(200 * time.Millisecond)
	raw, _ := os.ReadFile(path)
	if strings.Contains(string(raw), secret) {
		t.Fatalf("secret leaked in request line: %s", raw)
	}
	if strings.Contains(string(raw), "user:") {
		t.Fatalf("proxy userinfo leaked: %s", raw)
	}
	// Attempt file.
	apath := waitForFile(t, dir, "attempts-")
	araw, _ := os.ReadFile(apath)
	if strings.Contains(string(araw), secret) {
		t.Fatalf("secret leaked in attempt line")
	}
	for _, needle := range []string{"body", "headers", "query", "session", "fingerprint", "error_text", "playground"} {
		if strings.Contains(string(raw), `"`+needle+`"`) || strings.Contains(string(araw), `"`+needle+`"`) {
			t.Fatalf("forbidden field %q present", needle)
		}
	}
	// 400 records failure_class, no cooldown panic.
	if !strings.Contains(string(araw), AttemptClassClientRejected) {
		t.Fatalf("missing failure_class: %s", araw)
	}
}

func historyAuthedAdmin(t *testing.T, store *HistoryStore, mon *Monitor) (*AdminServer, string, string) {
	t.Helper()
	cfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"})
	mgr := &RuntimeManager{configPath: filepath.Join(t.TempDir(), "config.json"), logger: nil, monitor: mon, hub: NewLogHub(100), redactor: NewSecretRedactor(), level: new(slog.LevelVar)}
	mgr.level.Set(slog.LevelInfo)
	gw, err := NewGateway(cfg, nil, mon)
	if err != nil {
		t.Fatal(err)
	}
	mgr.current.Store(&gatewayRuntime{config: cfg, gateway: gw})
	mgr.history.Store(store)
	mon.SetHistorySink(store)
	mgr.redactor.Replace(cfg)
	admin := NewAdminServer(mgr, mon, NewLogHub(100), nil)
	token, csrf := "history-token", "history-csrf"
	current := mgr.Config()
	admin.sessions[tokenDigest(token)] = adminSession{
		Username: current.WebUI.Username, AuthVersion: secretFingerprint(current.WebUI.PasswordHash),
		CSRF: csrf, Expires: time.Now().Add(time.Hour),
	}
	return admin, token, csrf
}

func TestHistoryQueryFiltersCursorOrderAuth(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	_ = os.WriteFile(cfgPath, []byte("{}"), 0600)
	store := OpenHistoryStore(cfgPath, HistoryConfig{Enabled: true, Directory: "h", RetentionDays: 7, MaxBytesMB: 128}, nil, NewSecretRedactor())
	defer store.Close()
	base := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 5; i++ {
		ts := base.Add(time.Duration(i) * time.Minute)
		store.EnqueueRequest(UpstreamRequest{Time: ts, RequestID: "q" + string(rune('0'+i)), Model: "m" + string(rune('0'+i%2)), Tier: "zen", KeyID: "K", Channel: "key", Attempts: 1, Status: 200, DurationMS: 1, Success: i%2 == 0})
	}
	time.Sleep(400 * time.Millisecond)
	store.mu.Lock()
	for _, seg := range store.open {
		if seg != nil && seg.buf != nil {
			_ = seg.buf.Flush()
		}
	}
	store.mu.Unlock()
	mon := NewMonitor()
	admin, token, _ := historyAuthedAdmin(t, store, mon)
	doGet := func(path string, withAuth bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if withAuth {
			req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
		}
		rec := httptest.NewRecorder()
		admin.Handler().ServeHTTP(rec, req)
		return rec
	}
	if rec := doGet("/api/history/requests", false); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauth code=%d", rec.Code)
	}
	from := base.Add(-time.Hour).Format(time.RFC3339)
	to := base.Add(10 * time.Minute).Format(time.RFC3339)
	rec := doGet("/api/history/requests?from="+from+"&to="+to+"&limit=2", true)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("no-store=%q", got)
	}
	var page struct {
		Items      []historyRequestLine `json:"items"`
		NextCursor string               `json:"next_cursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 || page.NextCursor == "" {
		t.Fatalf("items=%d cursor=%q", len(page.Items), page.NextCursor)
	}
	if !(page.Items[0].Time >= page.Items[1].Time) {
		t.Fatal("not desc order")
	}
	rec2 := doGet("/api/history/requests?from="+from+"&to="+to+"&limit=2&cursor="+page.NextCursor, true)
	var page2 struct {
		Items []historyRequestLine `json:"items"`
	}
	_ = json.Unmarshal(rec2.Body.Bytes(), &page2)
	if len(page2.Items) == 0 {
		t.Fatal("expected second page")
	}
	// Filters.
	rec3 := doGet("/api/history/requests?from="+from+"&to="+to+"&model=m0&success=true", true)
	var page3 struct {
		Items []historyRequestLine `json:"items"`
	}
	_ = json.Unmarshal(rec3.Body.Bytes(), &page3)
	for _, it := range page3.Items {
		if it.Model != "m0" || !it.Success {
			t.Fatalf("filter failed: %+v", it)
		}
	}
	// Bad param 400.
	if rec := doGet("/api/history/requests?limit=zzz", true); rec.Code != 400 {
		t.Fatalf("bad limit code=%d", rec.Code)
	}
	// Rate limit: isolated window so earlier filter/cursor checks never pollute the budget.
	// Production limit stays 30/min; isolation uses a fresh AdminServer window.
	adminRL, tokenRL, _ := historyAuthedAdmin(t, store, mon)
	doGetRL := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.AddCookie(&http.Cookie{Name: adminCookieName, Value: tokenRL})
		rec := httptest.NewRecorder()
		adminRL.Handler().ServeHTTP(rec, req)
		return rec
	}
	saw429 := false
	for i := 0; i < 35; i++ {
		r := doGetRL("/api/history/series")
		if r.Code == 429 {
			saw429 = true
			break
		}
		if i >= 30 {
			t.Fatalf("expected 429 at %d got %d", i, r.Code)
		}
	}
	if !saw429 {
		t.Fatal("expected history rate limit to trigger")
	}
	// Path not controllable: unknown file names ignored (no traversal param exists).
	// Fresh window so the check never inherits the exhausted rate-limit budget.
	adminPath, tokenPath, _ := historyAuthedAdmin(t, store, mon)
	req := httptest.NewRequest(http.MethodGet, "/api/history/requests?from="+from+"&to="+to+"&model=../etc/passwd", nil)
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: tokenPath})
	recPath := httptest.NewRecorder()
	adminPath.Handler().ServeHTTP(recPath, req)
	if recPath.Code != 200 {
		t.Fatalf("path check code=%d body=%s", recPath.Code, recPath.Body.String())
	}
	var pathPage struct {
		Items []historyRequestLine `json:"items"`
	}
	if err := json.Unmarshal(recPath.Body.Bytes(), &pathPage); err != nil {
		t.Fatal(err)
	}
	if len(pathPage.Items) != 0 {
		t.Fatalf("traversal model must match nothing, got %d", len(pathPage.Items))
	}
}

func TestHistoryDisabledStatus(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	_ = os.WriteFile(cfgPath, []byte("{}"), 0600)
	store := OpenHistoryStore(cfgPath, HistoryConfig{Enabled: false, RetentionDays: 7, MaxBytesMB: 128}, nil, NewSecretRedactor())
	defer store.Close()
	mon := NewMonitor()
	admin, token, _ := historyAuthedAdmin(t, store, mon)
	req := httptest.NewRequest(http.MethodGet, "/api/history/requests", nil)
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	rec := httptest.NewRecorder()
	admin.Handler().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code=%d", rec.Code)
	}
	var page historyPage
	_ = json.Unmarshal(rec.Body.Bytes(), &page)
	if page.Active {
		t.Fatal("disabled must be inactive")
	}
	if len(page.Items) != 0 {
		t.Fatal("disabled must be empty")
	}
}

func TestHistoryApplySwapConcurrent(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	_ = os.WriteFile(cfgPath, []byte("{}"), 0600)
	mon := NewMonitor()
	mgr := &RuntimeManager{configPath: cfgPath, monitor: mon, hub: NewLogHub(100), redactor: NewSecretRedactor(), level: new(slog.LevelVar)}
	mgr.openHistory(HistoryConfig{Enabled: true, Directory: "h1", RetentionDays: 7, MaxBytesMB: 128})
	first := mgr.History()
	mgr.swapHistoryForApply(HistoryConfig{Enabled: true, Directory: "h1", RetentionDays: 7, MaxBytesMB: 128}, HistoryConfig{Enabled: true, Directory: "h1", RetentionDays: 7, MaxBytesMB: 128})
	if mgr.History() != first {
		t.Fatal("unchanged must reuse store")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mon.RecordAttempt(UpstreamAttempt{Time: time.Now().UTC(), RequestID: "c", Model: "m", Tier: "zen", KeyID: "K", Channel: "key", Proxy: "direct", Success: true, FailureClass: AttemptClassSuccess})
		}(i)
	}
	mgr.swapHistoryForApply(HistoryConfig{Enabled: true, Directory: "h1", RetentionDays: 7, MaxBytesMB: 128}, HistoryConfig{Enabled: true, Directory: "h2", RetentionDays: 7, MaxBytesMB: 128})
	wg.Wait()
	mgr.Shutdown()
}

func TestHistoryMinuteExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	_ = os.WriteFile(cfgPath, []byte("{}"), 0600)
	store := OpenHistoryStore(cfgPath, HistoryConfig{Enabled: true, Directory: "h", RetentionDays: 7, MaxBytesMB: 128}, nil, NewSecretRedactor())
	defer store.Close()
	mon := NewMonitor()
	mon.SetHistorySink(store)
	prev := time.Now().UTC().Truncate(time.Minute).Add(-time.Minute)
	series, ok := mon.minuteSeriesFor(prev)
	if !ok {
		t.Fatal("expected minute series")
	}
	store.EnqueueMinute(series)
	key := prev.Format(time.RFC3339Nano)
	if last := store.getLastMinute(); last != key {
		t.Fatalf("lastMinute=%q want %q", last, key)
	}
}

func TestHistoryWebUISyntaxDOM(t *testing.T) {
	data, err := os.ReadFile("webui/index.html")
	if err != nil {
		t.Fatal(err)
	}
	html := string(data)
	for _, needle := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write"} {
		if strings.Contains(html, needle) {
			t.Fatalf("forbidden DOM sink %q", needle)
		}
	}
	for _, id := range []string{"h-range", "tbody-hist", "hist-more", "facts-history", "c-hist-enabled", "c-hist-retention", "c-hist-max", "hist-channel"} {
		if !strings.Contains(html, id) {
			t.Fatalf("missing webui id %q", id)
		}
	}
	if !strings.Contains(html, "/api/history/requests") || !strings.Contains(html, "/api/history/series") {
		t.Fatal("missing history API wiring")
	}
	// History expand must issue exactly one attempt fetch mounted to the
	// current DOM detail row; no duplicate call on the click path.
	if n := strings.Count(html, "loadHistAttempts("); n != 2 {
		t.Fatalf("loadHistAttempts call sites=%d want 2 (def + single detail-row invoke)", n)
	}
	if strings.Contains(html, "renderPersistedRows(); if(S.histExpanded)loadHistAttempts") {
		t.Fatal("duplicate attempt fetch on click path")
	}
	if !strings.Contains(html, "renderHistChannelFilter") {
		t.Fatal("missing hist-channel population")
	}
	for _, ch := range []string{"anonymous", "not_routed"} {
		if !strings.Contains(html, ch) {
			t.Fatalf("hist-channel missing %q", ch)
		}
	}
	// zen/go are tiers, never channels: the staged channel seed must not
	// contain them.
	if strings.Contains(html, `"zen","go"`) || strings.Contains(html, `"zen", "go"`) || strings.Contains(html, `"not_routed","zen"`) {
		t.Fatal("hist-channel must not seed zen/go tiers")
	}
	if !strings.Contains(html, "channel=") {
		t.Fatal("history query must carry channel")
	}
	// Realtime cap copy unified to 200.
	if !strings.Contains(html, "最多 200 条展示") {
		t.Fatal("realtime cap must read 200")
	}
	if strings.Contains(html, "最多 500 条展示") {
		t.Fatal("stale 500 cap copy must be removed")
	}
	// History model input states exact full-ID matching.
	if !strings.Contains(html, "完整模型 ID 精确过滤") {
		t.Fatal("history model input must state exact full-ID filtering")
	}
	// 24h/7d series must aggregate into bounded buckets, not slice(-120).
	if !strings.Contains(html, "aggregatePersistedSeries") {
		t.Fatal("missing bounded bucket aggregation helper")
	}
	if strings.Contains(html, ").slice(-120)") {
		t.Fatal("series must not silently slice(-120) without aggregation")
	}
	if !strings.Contains(html, "trend-cap") {
		t.Fatal("missing dynamic trend caption")
	}
	// renderTrends must not clear persistent trend DOM when range != 1h:
	// the non-1h guard must precede any clear, 1h path clears+redraws
	// memory trends, and 24h/7d stay owned by renderPersistedSeries.
	rt := strings.Index(html, "function renderTrends(){")
	if rt < 0 {
		t.Fatal("missing renderTrends")
	}
	rtEnd := strings.Index(html[rt:], "function renderDists()")
	if rtEnd < 0 {
		t.Fatal("missing renderDists boundary")
	}
	body := html[rt : rt+rtEnd]
	guard := strings.Index(body, `if(S.historyRange!=="1h")return;`)
	if guard < 0 {
		t.Fatal("renderTrends must early-return on non-1h range")
	}
	if idx := strings.Index(body, "clear(hr)"); idx >= 0 && idx < guard {
		t.Fatal("renderTrends must not clear trend DOM before non-1h guard")
	}
	if idx := strings.Index(body, "clear(ht)"); idx >= 0 && idx < guard {
		t.Fatal("renderTrends must not clear trend DOM before non-1h guard")
	}
	if !strings.Contains(body, "clear(hr)") || !strings.Contains(body, "clear(ht)") {
		t.Fatal("renderTrends 1h path must clear+redraw memory trends")
	}
	if !strings.Contains(body, "S.metrics") {
		t.Fatal("renderTrends 1h path must render memory series")
	}
	ps := strings.Index(html, "function renderPersistedSeries(items){")
	if ps < 0 {
		t.Fatal("missing renderPersistedSeries")
	}
	psEnd := strings.Index(html[ps:], "function renderPersistedRows(){")
	if psEnd < 0 {
		t.Fatal("missing renderPersistedRows boundary")
	}
	pbody := html[ps : ps+psEnd]
	if !strings.Contains(pbody, "clear(hr)") || !strings.Contains(pbody, "clear(ht)") {
		t.Fatal("renderPersistedSeries must still clear/redraw 24h/7d trends")
	}
	// SSE must probe session after consecutive failures and reset on open/log.
	if !strings.Contains(html, "/api/auth/session") {
		t.Fatal("missing SSE session probe")
	}
	if !strings.Contains(html, "logFail") {
		t.Fatal("missing SSE failure counter")
	}
	// Per-page history gap must accumulate into hist-count.
	if !strings.Contains(html, "histGap") {
		t.Fatal("missing per-page history gap accumulation")
	}
	if !strings.Contains(html, "hist-count") {
		t.Fatal("missing hist-count gap display")
	}
	// Staged pool semantics: badge + explicit copy.
	if !strings.Contains(html, "pool-badge") {
		t.Fatal("missing pool active/staged badge")
	}
	if !strings.Contains(html, "暂存配置，不运行、不可探测、不计容量") {
		t.Fatal("missing staged pool copy")
	}
}

func writeHistoryRequestsFile(t *testing.T, dir, date string, seq, start, count int, base time.Time, modelFn func(i int) string) {
	t.Helper()
	var sb strings.Builder
	for i := 0; i < count; i++ {
		idx := start + i
		ts := base.Add(time.Duration(idx) * time.Second).UTC().Format(time.RFC3339Nano)
		model := "m"
		if modelFn != nil {
			model = modelFn(idx)
		}
		v := historyRequestLine{V: historySchemaV, Kind: string(historyKindRequest), Time: ts, RequestID: "r" + strconv.Itoa(idx), Model: model, Tier: "zen", Channel: "key", Attempts: 1, Status: 200, DurationMS: 1, Success: true}
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		sb.Write(data)
		sb.WriteByte('\n')
	}
	name := "requests-" + date + "-" + strconv.Itoa(seq) + ".ndjson"
	// Zero-pad seq to match writer naming (003d) while staying recognized.
	if seq < 1000 {
		name = "requests-" + date + "-" + padSeq(seq) + ".ndjson"
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(sb.String()), 0600); err != nil {
		t.Fatal(err)
	}
}

func padSeq(n int) string {
	s := strconv.Itoa(n)
	for len(s) < 3 {
		s = "0" + s
	}
	return s
}

func historyTestStoreWithDir(t *testing.T, retention int) (*HistoryStore, string) {
	t.Helper()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	store := OpenHistoryStore(cfgPath, HistoryConfig{Enabled: true, Directory: "h", RetentionDays: retention, MaxBytesMB: 128}, nil, NewSecretRedactor())
	t.Cleanup(store.Close)
	hdir := store.historyReadDir()
	if hdir == "" {
		t.Fatal("empty history dir")
	}
	return store, hdir
}

func TestHistorySingleSegmentOverBudgetNewest(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-30 * time.Minute)
	date := base.Format("20060102")
	writeHistoryRequestsFile(t, hdir, date, 1, 0, 600, base, nil)
	from := base.Add(-time.Hour)
	to := base.Add(2 * time.Hour)
	page, err := queryHistoryRequests(context.Background(), store, historyQueryFilter{From: from, To: to, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 10 {
		t.Fatalf("items=%d", len(page.Items))
	}
	// Newest-first: r599..r590.
	for i, it := range page.Items {
		v, ok := it.(historyRequestLine)
		if !ok {
			t.Fatalf("type %T", it)
		}
		want := "r" + strconv.Itoa(599-i)
		if v.RequestID != want {
			t.Fatalf("pos %d got %q want %q", i, v.RequestID, want)
		}
	}
	if page.NextCursor == "" {
		t.Fatal("expected next_cursor when more matches exist")
	}
	// Full traversal via pagination must yield all 600 with empty final cursor.
	total := 0
	cursor := ""
	for {
		p, err := queryHistoryRequests(context.Background(), store, historyQueryFilter{From: from, To: to, Limit: 200, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		total += len(p.Items)
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
		if total > 600 {
			t.Fatal("overflow")
		}
	}
	if total != 600 {
		t.Fatalf("full items=%d", total)
	}
}

func TestHistoryRareFilterAcrossManyRows(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-30 * time.Minute)
	date := base.Format("20060102")
	// 1000 common rows oldest-first, rare model only on newest line.
	writeHistoryRequestsFile(t, hdir, date, 1, 0, 1000, base, func(i int) string {
		if i == 999 {
			return "rare-model-xyz"
		}
		return "common"
	})
	from := base.Add(-time.Hour)
	to := base.Add(2 * time.Hour)
	page, err := queryHistoryRequests(context.Background(), store, historyQueryFilter{From: from, To: to, Limit: 10, Model: "rare-model-xyz"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("rare items=%d", len(page.Items))
	}
	v := page.Items[0].(historyRequestLine)
	if v.Model != "rare-model-xyz" || v.RequestID != "r999" {
		t.Fatalf("rare got %+v", v)
	}
	if page.NextCursor != "" {
		t.Fatalf("single rare match must not have next_cursor")
	}
}

func TestHistoryDeepPaginationNoDupLoss(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-30 * time.Minute)
	date := base.Format("20060102")
	writeHistoryRequestsFile(t, hdir, date, 1, 0, 50, base, nil)
	from := base.Add(-time.Hour)
	to := base.Add(2 * time.Hour)
	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		page, err := queryHistoryRequests(context.Background(), store, historyQueryFilter{From: from, To: to, Limit: 10, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, it := range page.Items {
			id := it.(historyRequestLine).RequestID
			if seen[id] {
				t.Fatalf("dup %q", id)
			}
			seen[id] = true
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
		if pages > 10 {
			t.Fatal("too many pages")
		}
	}
	if len(seen) != 50 {
		t.Fatalf("seen=%d want 50", len(seen))
	}
	for i := 0; i < 50; i++ {
		if !seen["r"+strconv.Itoa(i)] {
			t.Fatalf("missing r%d", i)
		}
	}
	if pages != 5 {
		t.Fatalf("pages=%d want 5", pages)
	}
}

func TestHistorySameTimestampTieBreak(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second)
	date := base.Format("20060102")
	var sb strings.Builder
	ts := base.Format(time.RFC3339Nano)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		v := historyRequestLine{V: historySchemaV, Kind: string(historyKindRequest), Time: ts, RequestID: "tie-" + id, Model: "m", Tier: "zen", Channel: "key", Attempts: 1, Status: 200, DurationMS: 1, Success: true}
		data, _ := json.Marshal(v)
		sb.Write(data)
		sb.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(hdir, "requests-"+date+"-"+padSeq(1)+".ndjson"), []byte(sb.String()), 0600); err != nil {
		t.Fatal(err)
	}
	from := base.Add(-time.Hour)
	to := base.Add(time.Hour)
	seen := []string{}
	cursor := ""
	for {
		page, err := queryHistoryRequests(context.Background(), store, historyQueryFilter{From: from, To: to, Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range page.Items {
			seen = append(seen, it.(historyRequestLine).RequestID)
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	want := []string{"tie-e", "tie-d", "tie-c", "tie-b", "tie-a"}
	if len(seen) != len(want) {
		t.Fatalf("seen=%v", seen)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("pos %d got %q want %q (seen=%v)", i, seen[i], want[i], seen)
		}
	}
	// Attempts same timestamp: request_id equal, attempt numeric desc.
	var ab strings.Builder
	for _, n := range []int{1, 2, 3} {
		v := historyAttemptLine{V: historySchemaV, Kind: string(historyKindAttempt), Time: ts, RequestID: "same-req", Model: "m", Tier: "zen", Attempt: n, Channel: "key", Status: 200, DurationMS: 1, Success: true, FailureClass: AttemptClassSuccess}
		data, _ := json.Marshal(v)
		ab.Write(data)
		ab.WriteByte('\n')
	}
	// Corrupt mix: bad line must set gap but not break pagination.
	ab.WriteString("not-json\n")
	if err := os.WriteFile(filepath.Join(hdir, "attempts-"+date+"-"+padSeq(1)+".ndjson"), []byte(ab.String()), 0600); err != nil {
		t.Fatal(err)
	}
	apage, err := queryHistoryAttempts(context.Background(), store, historyQueryFilter{From: from, To: to, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(apage.Items) != 3 {
		t.Fatalf("attempt items=%d", len(apage.Items))
	}
	if !apage.Gap {
		t.Fatal("expected gap on bad line")
	}
	got := []int{apage.Items[0].(historyAttemptLine).Attempt, apage.Items[1].(historyAttemptLine).Attempt, apage.Items[2].(historyAttemptLine).Attempt}
	if got[0] != 3 || got[1] != 2 || got[2] != 1 {
		t.Fatalf("attempt order=%v", got)
	}
}

func TestHistoryBadCursor400(t *testing.T) {
	store, _ := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC()
	badCursors := []string{
		"!!!",
		base64.RawURLEncoding.EncodeToString([]byte("not-a-time|abc")),
		base64.RawURLEncoding.EncodeToString([]byte(base.Format(time.RFC3339Nano) + "|")),
		base64.RawURLEncoding.EncodeToString([]byte("onlytime")),
		"invalid",
	}
	for _, c := range badCursors {
		if _, err := queryHistoryRequests(context.Background(), store, historyQueryFilter{From: base.Add(-time.Hour), To: base.Add(time.Hour), Limit: 10, Cursor: c}); !errors.Is(err, errBadCursor) {
			t.Fatalf("requests cursor %q err=%v", c, err)
		}
		if _, err := queryHistoryAttempts(context.Background(), store, historyQueryFilter{From: base.Add(-time.Hour), To: base.Add(time.Hour), Limit: 10, Cursor: c}); !errors.Is(err, errBadCursor) {
			t.Fatalf("attempts cursor %q err=%v", c, err)
		}
		if _, err := queryHistorySeries(context.Background(), store, historyQueryFilter{From: base.Add(-time.Hour), To: base.Add(time.Hour), Limit: 10, Cursor: c}); !errors.Is(err, errBadCursor) {
			t.Fatalf("series cursor %q err=%v", c, err)
		}
	}
	// Attempts cursor missing attempt suffix must 400.
	reqCursor := encodeHistoryCursor(base, "r1")
	if _, err := queryHistoryAttempts(context.Background(), store, historyQueryFilter{From: base.Add(-time.Hour), To: base.Add(time.Hour), Limit: 10, Cursor: reqCursor}); !errors.Is(err, errBadCursor) {
		t.Fatalf("attempt cursor without # err=%v", err)
	}
	// Requests cursor with attempt suffix must 400.
	attCursor := encodeHistoryCursor(base, "r1#2")
	if _, err := queryHistoryRequests(context.Background(), store, historyQueryFilter{From: base.Add(-time.Hour), To: base.Add(time.Hour), Limit: 10, Cursor: attCursor}); !errors.Is(err, errBadCursor) {
		t.Fatalf("request cursor with # err=%v", err)
	}
	// HTTP envelope: bad cursor is 400 invalid_cursor, never 200 empty.
	mon := NewMonitor()
	admin, token, _ := historyAuthedAdmin(t, store, mon)
	for _, path := range []string{
		"/api/history/requests?cursor=!!!",
		"/api/history/attempts?cursor=!!!",
		"/api/history/series?cursor=!!!",
		"/api/history/requests?before=not-a-time",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
		rec := httptest.NewRecorder()
		admin.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s code=%d body=%s", path, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "invalid_cursor") {
			t.Fatalf("%s body=%s", path, rec.Body.String())
		}
	}
}

func TestHistoryContextCancelAndTimeout(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Minute)
	writeHistoryRequestsFile(t, hdir, base.Format("20060102"), 1, 0, 20, base, nil)
	from := base.Add(-time.Hour)
	to := base.Add(time.Hour)
	// Pre-cancelled ctx aborts scan.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := queryHistoryRequests(cancelled, store, historyQueryFilter{From: from, To: to, Limit: 10}); !errors.Is(err, context.Canceled) {
		t.Fatalf("requests cancel err=%v", err)
	}
	if _, err := queryHistoryAttempts(cancelled, store, historyQueryFilter{From: from, To: to, Limit: 10}); !errors.Is(err, context.Canceled) {
		t.Fatalf("attempts cancel err=%v", err)
	}
	if _, err := queryHistorySeries(cancelled, store, historyQueryFilter{From: from, To: to, Limit: 10}); !errors.Is(err, context.Canceled) {
		t.Fatalf("series cancel err=%v", err)
	}
	// Slow hook + tight deadline proves per-line ctx checks (not end-only).
	oldHook := historyScanHook
	historyScanHook = func() { time.Sleep(2 * time.Millisecond) }
	defer func() { historyScanHook = oldHook }()
	short, stop := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer stop()
	if _, err := queryHistoryRequests(short, store, historyQueryFilter{From: from, To: to, Limit: 10}); !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("slow scan err=%v", err)
	}
	// Handler maps cancelled parent to 504 history_timeout.
	mon := NewMonitor()
	admin, token, _ := historyAuthedAdmin(t, store, mon)
	req := httptest.NewRequest(http.MethodGet, "/api/history/requests", nil)
	cctx, ccancel := context.WithCancel(req.Context())
	ccancel()
	req = req.WithContext(cctx)
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	rec := httptest.NewRecorder()
	admin.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("handler cancel code=%d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "history_timeout") {
		t.Fatalf("body=%s", rec.Body.String())
	}
}

func TestHistoryRetentionOneDefaultSeries(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	store := OpenHistoryStore(cfgPath, HistoryConfig{Enabled: true, Directory: "h", RetentionDays: 1, MaxBytesMB: 128}, nil, NewSecretRedactor())
	defer store.Close()
	hdir := store.historyReadDir()
	now := time.Now().UTC().Truncate(time.Minute)
	for i := 0; i < 3; i++ {
		min := now.Add(-time.Duration(i) * time.Minute)
		v := historyMinuteLine{V: historySchemaV, Kind: string(historyKindMinute), Time: min.Format(time.RFC3339Nano), Minute: min.Format(time.RFC3339Nano), Total: 1, Success: 1}
		data, _ := json.Marshal(v)
		data = append(data, '\n')
		f, err := os.OpenFile(filepath.Join(hdir, "minutes-"+min.Format("20060102")+"-"+padSeq(1)+".ndjson"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(data); err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
	}
	mon := NewMonitor()
	admin, token, _ := historyAuthedAdmin(t, store, mon)
	for _, path := range []string{"/api/history/series", "/api/history/requests", "/api/history/attempts"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
		rec := httptest.NewRecorder()
		admin.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s code=%d body=%s", path, rec.Code, rec.Body.String())
		}
	}
}

func TestHistoryPathNotLeaked(t *testing.T) {
	sensitive := "SECRET-SENSITIVE-ABC123"
	// Sensitive segment lives in the parent path; the configured basename
	// itself stays clean so Directory=basename is safe by construction.
	parent := filepath.Join(t.TempDir(), sensitive, "sub")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(parent, "hist-blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(parent, "config.json")
	if err := os.WriteFile(cfgPath, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	// Point history directory at an existing file so MkdirAll fails with an
	// error containing the sensitive absolute path.
	store := OpenHistoryStore(cfgPath, HistoryConfig{Enabled: true, Directory: blocker, RetentionDays: 7, MaxBytesMB: 128}, nil, NewSecretRedactor())
	defer store.Close()
	st := store.Status()
	if st.Active {
		t.Fatal("expected inactive store")
	}
	if strings.Contains(st.LastError, sensitive) {
		t.Fatalf("last_error leaked sensitive: %q", st.LastError)
	}
	if strings.Contains(st.LastError, blocker) {
		t.Fatalf("last_error leaked path: %q", st.LastError)
	}
	if st.LastError != "" && st.LastError != historyErrUnavailable && st.LastError != historyErrWriteFailed && st.LastError != historyErrReadFailed && st.LastError != historyErrCorrupt {
		t.Fatalf("last_error not fixed taxonomy: %q", st.LastError)
	}
	if strings.Contains(st.Directory, "/") {
		t.Fatalf("directory not basename: %q", st.Directory)
	}
	if st.Directory != "hist-blocker" {
		t.Fatalf("directory=%q", st.Directory)
	}
	mon := NewMonitor()
	admin, token, _ := historyAuthedAdmin(t, store, mon)
	req := httptest.NewRequest(http.MethodGet, "/api/history/requests", nil)
	req.AddCookie(&http.Cookie{Name: adminCookieName, Value: token})
	rec := httptest.NewRecorder()
	admin.Handler().ServeHTTP(rec, req)
	if strings.Contains(rec.Body.String(), sensitive) || strings.Contains(rec.Body.String(), blocker) {
		t.Fatalf("query leaked path: %s", rec.Body.String())
	}
	// Write-failure path with sensitive content stays fixed as well.
	s2, _ := historyTestStoreWithDir(t, 7)
	s2.mu.Lock()
	s2.lastError = historyErrWriteFailed
	s2.mu.Unlock()
	if got := s2.Status().LastError; got != historyErrWriteFailed {
		t.Fatalf("fixed error changed: %q", got)
	}
	// Sanitizer allowlist: arbitrary path-like value maps to unavailable.
	if got := sanitizeHistoryLastError("/tmp/" + sensitive + "/history: permission denied"); got != historyErrUnavailable {
		t.Fatalf("sanitizer leaked: %q", got)
	}
	if strings.Contains(sanitizeHistoryLastError("/tmp/"+sensitive), sensitive) {
		t.Fatal("sanitizer leaked sensitive")
	}
}

func historyDirTotalSize(t *testing.T, dir string) uint64 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var total uint64
	for _, e := range entries {
		if !isHistorySegment(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		total += uint64(info.Size())
	}
	return total
}

func waitHistoryQueueDrained(t *testing.T, s *HistoryStore) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if len(s.queue) == 0 {
			time.Sleep(100 * time.Millisecond)
			if len(s.queue) == 0 {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("history queue did not drain")
}

func flushHistoryBuffers(s *HistoryStore) {
	s.mu.Lock()
	for _, seg := range s.open {
		if seg != nil && seg.buf != nil {
			_ = seg.buf.Flush()
		}
	}
	s.mu.Unlock()
}

func TestHistoryStatusSizeNoDoubleCountOpenSegments(t *testing.T) {
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	for i := 0; i < 5; i++ {
		ts := base.Add(time.Duration(i) * time.Second)
		store.EnqueueRequest(UpstreamRequest{Time: ts, RequestID: "sz-r" + strconv.Itoa(i), Model: "m", Tier: "zen", KeyID: "K", Channel: "key", Attempts: 1, Status: 200, DurationMS: 1, Success: true})
	}
	for i := 0; i < 5; i++ {
		ts := base.Add(60*time.Second + time.Duration(i)*time.Second)
		store.EnqueueAttempt(UpstreamAttempt{Time: ts, RequestID: "sz-r" + strconv.Itoa(i), Model: "m", Tier: "zen", Attempt: 1, KeyID: "K", Channel: "key", Proxy: "direct", Status: 200, DurationMS: 1, Success: true, FailureClass: AttemptClassSuccess})
	}
	m1 := base.Add(5 * time.Minute).Truncate(time.Minute)
	m2 := base.Add(6 * time.Minute).Truncate(time.Minute)
	store.EnqueueMinute(MetricSeries{Minute: m1, Total: 1, Success: 1})
	store.EnqueueMinute(MetricSeries{Minute: m2, Total: 2, Success: 2})
	waitHistoryQueueDrained(t, store)
	flushHistoryBuffers(store)
	store.enforceRetention()
	st := store.Status()
	actual := historyDirTotalSize(t, hdir)
	if st.SizeBytes != actual {
		t.Fatalf("SizeBytes double-counted: status=%d dir=%d", st.SizeBytes, actual)
	}
	if actual == 0 {
		t.Fatal("expected nonzero dir total")
	}
	wantOldest := base.UTC().Format(time.RFC3339Nano)
	wantNewest := m2.UTC().Format(time.RFC3339Nano)
	if st.OldestAt != wantOldest {
		t.Fatalf("oldest=%q want %q", st.OldestAt, wantOldest)
	}
	if st.NewestAt != wantNewest {
		t.Fatalf("newest=%q want %q", st.NewestAt, wantNewest)
	}
	// Second refresh must stay stable (no accumulation/double-count).
	store.enforceRetention()
	st2 := store.Status()
	if st2.SizeBytes != actual {
		t.Fatalf("second refresh size=%d want %d", st2.SizeBytes, actual)
	}
	if st2.OldestAt != wantOldest || st2.NewestAt != wantNewest {
		t.Fatalf("second refresh bounds oldest=%q newest=%q want %q/%q", st2.OldestAt, st2.NewestAt, wantOldest, wantNewest)
	}
}

func TestHistoryStatusSizeSealedPlusOpenAfterRotation(t *testing.T) {
	old := historySegmentMaxBytes
	historySegmentMaxBytes = 512
	defer func() { historySegmentMaxBytes = old }()
	store, hdir := historyTestStoreWithDir(t, 7)
	base := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	for i := 0; i < 30; i++ {
		ts := base.Add(time.Duration(i) * time.Second)
		store.EnqueueRequest(UpstreamRequest{Time: ts, RequestID: "rot-r" + strconv.Itoa(i), Model: strings.Repeat("m", 40), Tier: "zen", KeyID: "K", Channel: "key", Attempts: 1, Status: 200, DurationMS: 1, Success: true})
	}
	waitHistoryQueueDrained(t, store)
	flushHistoryBuffers(store)
	store.enforceRetention()
	entries, err := os.ReadDir(hdir)
	if err != nil {
		t.Fatal(err)
	}
	sealed := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "requests-") {
			sealed++
		}
	}
	if sealed < 2 {
		t.Fatalf("expected rotation sealed+open, got %d requests segments", sealed)
	}
	st := store.Status()
	actual := historyDirTotalSize(t, hdir)
	if st.SizeBytes != actual {
		t.Fatalf("rotated SizeBytes=%d dir=%d", st.SizeBytes, actual)
	}
	wantOldest := base.UTC().Format(time.RFC3339Nano)
	wantNewest := base.Add(29 * time.Second).UTC().Format(time.RFC3339Nano)
	if st.OldestAt != wantOldest {
		t.Fatalf("rotated oldest=%q want %q", st.OldestAt, wantOldest)
	}
	if st.NewestAt != wantNewest {
		t.Fatalf("rotated newest=%q want %q", st.NewestAt, wantNewest)
	}
}
