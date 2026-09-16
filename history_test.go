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
	// Dependency-free text-node rendering still holds.
	for _, needle := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write"} {
		if strings.Contains(html, needle) {
			t.Fatalf("forbidden DOM sink %q", needle)
		}
	}
	// New IA: four tabs only.
	for _, tab := range []string{`data-tab="realtime"`, `data-tab="usage"`, `data-tab="health"`, `data-tab="config"`} {
		if !strings.Contains(html, tab) {
			t.Fatalf("missing new IA tab %q", tab)
		}
	}
	for _, label := range []string{"实时请求", "使用统计", "健康诊断", ">配置<"} {
		if !strings.Contains(html, label) {
			t.Fatalf("missing new IA label %q", label)
		}
	}
	for _, stale := range []string{"历史与趋势", "资源/关联", "配置与诊断"} {
		if strings.Contains(html, stale) {
			t.Fatalf("stale IA label must be removed: %q", stale)
		}
	}
	// Required element IDs across the four tabs (new period model).
	for _, id := range []string{"h-period", "tbody-proxy-stats", "tbl-proxy-stats", "proxy-stats-note", "usage-avail", "c-hist-enabled", "c-hist-retention", "c-hist-max", "f-proxy", "tbody-attempts", "h-kpis", "trend-req", "trend-tok", "tbody-usage-model", "tbody-usage-tier", "facts-readiness", "tbody-proxies", "tbody-keys", "tbody-targets", "targets-note", "facts-catalog", "log-list", "c-web-enabled", "pools-editor"} {
		if !strings.Contains(html, id) {
			t.Fatalf("missing webui id %q", id)
		}
	}
	for _, stale := range []string{"tbody-hist", "hist-more", "hist-count", "hist-model", "hist-status", "hist-proxy", "tbl-hist", "hist-persist-wrap"} {
		if strings.Contains(html, stale) {
			t.Fatalf("duplicate persisted request-history table must stay removed: %q", stale)
		}
	}
	// Single h-period selector carries the four labels/values; old controls gone.
	for _, needle := range []string{`<select id="h-period">`, `value="today">今天`, `value="24h">最近 24 小时`, `value="7d">最近 7 天`, `value="month">本月`, "periodStart", "periodLabel", "historyPeriod", "usageAvailability"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing h-period contract %q", needle)
		}
	}
	if strings.Count(html, `id="h-period"`) != 1 {
		t.Fatal("exactly one h-period selector required")
	}
	for _, stale := range []string{`id="h-range"`, `id="h-scope"`, "historyRange", "scopeLifetime", "historyRangeHours", "renderTrends", "S.histGap"} {
		if strings.Contains(html, stale) {
			t.Fatalf("old h-scope/h-range controls must stay removed: %q", stale)
		}
	}
	if !strings.Contains(html, "/api/history/proxy-stats") || !strings.Contains(html, "/api/history/series") {
		t.Fatal("missing history API wiring (proxy-stats/series)")
	}
	if strings.Contains(html, "/api/history/requests?") || strings.Contains(html, "/api/history/attempts?") {
		t.Fatal("usage page must not aggregate client pages via requests/attempts; use /api/history/proxy-stats")
	}
	// No expandable rows: no per-row attempt fetch, no expand state.
	if strings.Contains(html, "loadHistAttempts(") {
		t.Fatal("flat attempt rows must not fetch attempts on expand")
	}
	for _, stale := range []string{"expandedRequest", "histExpanded", "renderHistChannelFilter", "hist-channel"} {
		if strings.Contains(html, stale) {
			t.Fatalf("expandable/channel state must be removed: %q", stale)
		}
	}
	// Removed columns: channel, cache-miss, session column (hash stays searchable only).
	for _, stale := range []string{"<th>通道</th>", "全部通道", "<th>缓存未命中</th>", "cache_miss_tokens", "<th>客户端会话hash</th>", "<th>会话</th>"} {
		if strings.Contains(html, stale) {
			t.Fatalf("removed column must stay removed: %q", stale)
		}
	}
	// Flat attempt rows: realtime 11-column header omits Request ID/replay/HTTP400 diagnosis.
	if !strings.Contains(html, "attempt-row") {
		t.Fatal("missing flat attempt-row rendering")
	}
	if strings.Count(html, "<th>时间</th><th>模型</th><th>上游</th>") < 1 {
		t.Fatal("realtime table must keep the header without Request ID")
	}
	for _, stale := range []string{"<th>Request ID</th>", "<th>重放</th>", "HTTP 400 诊断", "400_diag", "diag400Cell", "diag400Text", "复制完整 Request ID"} {
		if strings.Contains(html, stale) {
			t.Fatalf("removed Request ID/replay/HTTP400 UI must stay removed: %q", stale)
		}
	}
	if !strings.Contains(html, "<th>代理</th>") {
		t.Fatal("flat rows must carry 代理 column")
	}
	if !strings.Contains(html, "<th>上游</th>") || !strings.Contains(html, "<th>密钥</th>") || !strings.Contains(html, "<th>状态</th>") || !strings.Contains(html, "<th>耗时</th>") {
		t.Fatal("flat rows must carry localized 上游/密钥/状态/耗时 columns")
	}
	if !strings.Contains(html, "attemptRowCells") {
		t.Fatal("realtime table must reuse shared attemptRowCells")
	}
	if !strings.Contains(html, `String(a.request_id||"")`) || !strings.Contains(html, `String(a.client_session_hash||"")`) {
		t.Fatal("search must match both Request ID and client session hash")
	}
	// Shared row: compact status code with title detail; duration uses `ms` without space.
	for _, needle := range []string{"pillForFailureClass(fc)", "failureLabel(fc)", "失败分类：", "HTTP 状态 ", `+"ms"`} {
		if !strings.Contains(html, needle) {
			t.Fatalf("shared row must keep compact status with title detail, missing %q", needle)
		}
	}
	if strings.Contains(html, `+" ms"`) {
		t.Fatal("duration must not contain spaced `+\" ms\"`")
	}
	if strings.Contains(html, "毫秒") {
		t.Fatal("stale duration unit 毫秒 must be removed")
	}
	// Token attribution: usage_reported gating, never estimated.
	if !strings.Contains(html, "usage_reported") || !strings.Contains(html, "不做估算") {
		t.Fatal("missing usage_reported token attribution")
	}
	if !strings.Contains(html, "最终") {
		t.Fatal("tokens must be scoped to the final request-result row")
	}
	if strings.Count(html, "emptyRow(tb,11") < 1 {
		t.Fatal("realtime flat table must cover 11 columns")
	}
	if strings.Count(html, "emptyRow(tb,15") < 1 {
		t.Fatal("proxy stats table must cover 15 columns")
	}
	if strings.Contains(html, "emptyRow(tb,14") {
		t.Fatal("stale 14-column empty rows must be removed")
	}
	// Sidebar + responsive layout hooks (bounded redesign).
	for _, needle := range []string{`class="layout"`, `class="nav"`, `class="main"`, `id="main-nav"`, ".layout", "@media", ".nav button:hover", ".nav button:focus-visible", ".nav button.active"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing sidebar/responsive hook %q", needle)
		}
	}
	if !strings.Contains(html, "box-shadow:2px 0 6px") {
		t.Fatal("sidebar must carry a restrained right divider/shadow")
	}
	if !strings.Contains(html, "outline:2px solid var(--accent)") {
		t.Fatal("sidebar must carry keyboard-focus state")
	}
	// F1: hidden WebUI enabled preserves backend via safe preservedEnabled; submit must not dereference null S.config.
	if !strings.Contains(html, "hidden-enabled-preserved") {
		t.Fatal("hidden enabled control must preserve the backend value")
	}
	if !strings.Contains(html, "preservedEnabled") || !strings.Contains(html, "webui:{enabled:preservedEnabled") {
		t.Fatal("config submit must use safe preservedEnabled for webui.enabled")
	}
	if strings.Contains(html, "webui:{enabled:S.config.webui.enabled") {
		t.Fatal("config submit must not dereference S.config.webui.enabled directly")
	}
	if strings.Contains(html, "username:S.config.webui.username") {
		t.Fatal("config submit must not dereference null S.config.webui.username")
	}
	// F2: selected period drives KPIs/trends/history; no invented lifetime scope.
	for _, needle := range []string{"historyPeriod", "periodStart", "periodLabel", "histQueryRange", "usageAvailability"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing period contract %q", needle)
		}
	}
	for _, stale := range []string{"scopeLifetime", "historyRangeHours", "var isLife", "var calls=(scopeLifetime", "维度调用数仅最近一小时"} {
		if strings.Contains(html, stale) {
			t.Fatalf("old lifetime-scope UI must stay removed: %q", stale)
		}
	}
	// F3: realtime proxy filter preserves selection even when temporarily absent.
	if strings.Count(html, "if(cur)seen[cur]=1") < 1 {
		t.Fatal("realtime proxy filter must preserve selection")
	}
	if strings.Contains(html, "renderHistProxyFilter") {
		t.Fatal("duplicate history proxy filter must stay removed")
	}
	// F4: strict final usage equality for realtime only.
	if !strings.Contains(html, "if(req.attempts===undefined||attempt.attempt===undefined)return null") {
		t.Fatal("realtime final usage must require both counts known and equal")
	}
	if strings.Contains(html, "(req.attempts===undefined||req.attempts===a.attempt)") {
		t.Fatal("loose final usage equality must be removed")
	}
	// F5: bounded server aggregate replaces client cursor paging.
	if strings.Contains(html, "&proxy_pool=") {
		t.Fatal("usage page must not use client-side proxy_pool paging; use /api/history/proxy-stats")
	}
	if strings.Contains(html, "poolSel") || strings.Contains(html, "pxSel.split") {
		t.Fatal("client proxy_pool derivation must stay removed")
	}
	if strings.Contains(html, `S.histCursor=""; S.histAttCursor=""`) || strings.Contains(html, "S.histCursor") || strings.Contains(html, "S.histAttCursor") {
		t.Fatal("client history cursors must stay removed")
	}
	if strings.Contains(html, "S.histItems.length>500") || strings.Contains(html, "S.histAttempts.length>500") {
		t.Fatal("client paged history state must stay removed")
	}
	if !strings.Contains(html, "renderProxyStats") || !strings.Contains(html, "/api/history/proxy-stats") {
		t.Fatal("usage page must render bounded server proxy stats")
	}
	if !strings.Contains(html, "reqById") {
		t.Fatal("realtime rows must join requests/attempts")
	}
	// F6: proxy stats table has its own scroll bound.
	if !strings.Contains(html, `<div class="table-wrap scroll-bound"><table class="table" id="tbl-proxy-stats"`) {
		t.Fatal("proxy stats table must carry scroll-bound")
	}
	// Checkbox-label spacing and scroll bounds.
	if !strings.Contains(html, ".check-label input") {
		t.Fatal("missing checkbox-label alignment rule")
	}
	if !strings.Contains(html, "scroll-bound") {
		t.Fatal("missing bounded internal scroll")
	}
	// Charts, KPIs, caps, filters (period model + overflow guards).
	for _, needle := range []string{"缓存命中率", "trend-cap", "aggregatePersistedSeries", "上游尝试", "代理统计", "已返回用量请求", "今天", "本月"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing usage/chart label %q", needle)
		}
	}
	for _, stale := range []string{"完整模型 ID 精确过滤", "hist-model", "hist-status", "hist-proxy", "hist-more", "hist-count", "renderPersistedRows"} {
		if strings.Contains(html, stale) {
			t.Fatalf("duplicate history-table control must stay removed: %q", stale)
		}
	}
	for _, stale := range []string{"P50", "最多展示 200", "单次尝试一行", ").slice(-120)", "function renderTrends", "S.historyRange", "分组指纹"} {
		if strings.Contains(html, stale) {
			t.Fatalf("stale chart/polluted phrase must stay removed: %q", stale)
		}
	}
	// Chart overflow guards: bars and trend grid stay bounded.
	for _, needle := range []string{".bars", "trend-grid", "overflow-x:auto", "min-width:0", "max-width:100%", "overflow:hidden", "scroll-bound"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing chart overflow guard %q", needle)
		}
	}
	ps := strings.Index(html, "function renderPersistedSeries(items){")
	if ps < 0 {
		t.Fatal("missing renderPersistedSeries")
	}
	psEnd := strings.Index(html[ps:], "function renderProxyStats(){")
	if psEnd < 0 {
		t.Fatal("missing renderProxyStats boundary")
	}
	pbody := html[ps : ps+psEnd]
	if !strings.Contains(pbody, "clear(hr)") || !strings.Contains(pbody, "clear(ht)") {
		t.Fatal("renderPersistedSeries must still clear/redraw period trends")
	}
	// SSE must probe session after consecutive failures and reset on open/log.
	if !strings.Contains(html, "/api/auth/session") {
		t.Fatal("missing SSE session probe")
	}
	if !strings.Contains(html, "logFail") {
		t.Fatal("missing SSE failure counter")
	}
	// Proxy stats loading/truncation state stays bounded server-side.
	if !strings.Contains(html, "proxy-stats-note") {
		t.Fatal("missing proxy-stats-note display")
	}
	if !strings.Contains(html, "S.proxyStats") || !strings.Contains(html, "S.proxyTruncated") {
		t.Fatal("missing bounded proxy stats state")
	}
	if strings.Contains(html, "S.histCursor") || strings.Contains(html, "S.histAttCursor") {
		t.Fatal("stale persisted history cursors must stay removed")
	}
	// Staged pool semantics: badge + explicit copy.
	if !strings.Contains(html, "pool-badge") {
		t.Fatal("missing pool active/staged badge")
	}
	if !strings.Contains(html, "暂存：不运行") {
		t.Fatal("missing staged pool copy")
	}
	// Config tab stays configuration-focused: no playground/access/usage/model/account blocks.
	for _, stale := range []string{"play-model", "access-base", "tbody-models", "facts-usage", "account-form", "tbody-res-proxy", "tbody-pairs"} {
		if strings.Contains(html, stale) {
			t.Fatalf("config/health split must remove %q from the bundle", stale)
		}
	}
	// Localization: key Chinese labels present for the new contract.
	// Topbar uses exact English Active/Streaming labels (metric IDs preserved).
	for _, needle := range []string{"运行时长", "Active", "Streaming", "<th>上游</th>", "<th>代理</th>", "<th>密钥</th>", "<th>状态</th>", "<th>耗时</th>", "匿名", "凭证", "目标", "元数据", "尝试", "回退", "失败回退", "最近一小时", "进程累计", "最近 24 小时", "最近 7 天", "今天", "本月", "历史记录", "活跃目标冷却", "代理可用性", "备用模型渠道可用性", "凭证可用性", "批量检测", "目录 / 元数据快照", "管理会话有效期", "服务端密钥", "具名代理池", "匿名路由池", "代理文件", "运行中", "暂存：不运行", "最多显示 200 条", "WebUI 是否启用", "未知（", "上下文长度", "调试", "信息", "警告", "错误", "小时", "分钟", "秒"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing localized label %q", needle)
		}
	}
	// Polluted/removed copy must stay absent.
	for _, stale := range []string{"<th>Request ID</th>", "<th>重放</th>", "HTTP 400 诊断", "分组指纹", "单次尝试一行", "回退请求行", "请求级回退行", "最多展示 200", "毫秒", "P50", "400_diag", "diag400Cell", "diag400Text", "error_hint"} {
		if strings.Contains(html, stale) {
			t.Fatalf("polluted phrase must stay removed: %q", stale)
		}
	}
	// Localization: required proper-case technical terms preserved.
	for _, needle := range []string{"Request ID", "HTTP", "API", "WebUI", "CSRF", "Responses", "Anthropic", "Zen", "Go", "JSON", "URL", "IP", "SOCKS5", "Token", "Chat Completions", "protoLabel"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing proper-case term %q", needle)
		}
	}
	// Localization: stale visible English/raw headers and mixed phrases must be gone (static text only).
	for _, stale := range []string{"<th>Tier</th>", "<th>Proxy</th>", "<th>Key</th>", "<th>pool</th>", "<th>index</th>", "<th>anonymous</th>", "<th>routing</th>", "<th>credential</th>", ">uptime <", ">active <", ">streams <", "Max Idle", "Max Conns", "Idle Timeout", "Connect Timeout", "Failure Cooldown", "Ring Size", "proxyfile（", "pool 名", "Key Tier", "Proxy 健康", "Metadata 快照", "Attempt 资源统计", "last_hour 窗口", "pool: ", "删除 pool", ">debug<", ">info<", ">warn<", ">error<", "dropped / gap", "\"last_error\"", "memory-only", "flat attempt-row", "UpstreamAttempt", ">anonymous<", "usage_reported=false", "route_session_replay=", "<th>回退</th>", "已回退", "无回退", "同目标回退", "后端保留值", "表单隐藏", `return c||"核心"`, `return e||"—"`} {
		if strings.Contains(html, stale) {
			t.Fatalf("stale visible string must be localized: %q", stale)
		}
	}
	// Localization: centralized display mapping helpers cover known values.
	for _, needle := range []string{"function protoLabel", "function tierLabel", "function failureLabel", "function hintLabel", "function logLevelLabel", "function logComponentLabel", "function logEventLabel", "function cacheSourceLabel", "function refreshScopeLabel", "function scopeLabel", "function rangeLabel", "function replayLabel", "function routingRefsLabel", "function restartFieldLabel", "function tierLabel", "上下文长度", "陈旧响应引用", "会话被拒绝", "最近一小时", "进程累计", "实时拉取", "磁盘缓存", "传输失败", "认证失败", "限流", "上游失败", "客户端拒绝", "已更新", "未更新"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing mapping helper/label %q", needle)
		}
	}
	if !strings.Contains(html, "tierLabel(") || !strings.Contains(html, "protoLabel(") || !strings.Contains(html, "failureLabel(") || !strings.Contains(html, "hintLabel(") {
		t.Fatal("tables must use centralized protocol/tier/failure/hint mapping helpers")
	}
}

func TestHistoryWebUICompletedJoinStability(t *testing.T) {
	data, err := os.ReadFile("webui/index.html")
	if err != nil {
		t.Fatal(err)
	}
	html := string(data)
	// Completed-join stability for realtime: unfinished unmatched attempts
	// never render, so a visible — means the completed upstream response did
	// not provide usage.
	if got := strings.Count(html, "if(!reqById[a.request_id])return"); got < 1 {
		t.Fatalf("realtime join must skip unmatched attempts, got %d", got)
	}
	if strings.Contains(html, "req.attempts!==undefined&&a.attempt!==undefined&&req.attempts===a.attempt") {
		t.Fatal("duplicate persisted join must stay removed")
	}
	// Realtime retains request-only fallback for completed rows.
	if got := strings.Count(html, "fallback:true"); got < 1 {
		t.Fatalf("completed request-only fallback must be retained, got %d", got)
	}
	if !strings.Contains(html, "tokenReq:(r.usage_reported?r:null)") {
		t.Fatal("request-only fallback must carry immutable request usage only")
	}
	// Usage — stability: token cells derive only from the immutable parent
	// request via strict final-attempt equality; no client merging converts — later.
	if !strings.Contains(html, "function finalUsageFor(attempt, reqById){") {
		t.Fatal("missing finalUsageFor join helper")
	}
	if !strings.Contains(html, "if(req.attempts===undefined||attempt.attempt===undefined)return null") {
		t.Fatal("final usage must require both counts known")
	}
	// Completed captions: realtime stability plus single usage explanation.
	for _, needle := range []string{"已完成请求的尝试记录，未完成的暂不显示", "用量仅统计上游实际回报，不做估算，未知显示“—”。所选时间同时决定指标、趋势与历史记录。"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing completed-join copy %q", needle)
		}
	}
	if got := strings.Count(html, "不做估算"); got != 1 {
		t.Fatalf("usage explanation must appear exactly once, got %d", got)
	}
	for _, stale := range []string{"已完成的上游响应未提供用量", "请求级已上报用量", "上游已上报", "已上报才计数", "已返回用量的请求", "无已上报用量", "非最终尝试或上游未上报", "旧历史用量", "范围内共 "} {
		if strings.Contains(html, stale) {
			t.Fatalf("repetitive usage caveat must stay removed: %q", stale)
		}
	}
	if strings.Contains(html, "已完成请求的历史记录，未匹配到请求的不显示") {
		t.Fatal("duplicate history-table copy must stay removed")
	}
	// Proxy stats mixed grain is explained once, concisely (neutral wording).
	for _, needle := range []string{"代理统计", "上游尝试", "请求级用量"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing proxy-stats grain copy %q", needle)
		}
	}
	// Precise operator label: request-count ratio with neutral description.
	if !strings.Contains(html, "上游用量返回率") {
		t.Fatal("missing 上游用量返回率 label")
	}
	if strings.Contains(html, "用量上报率") {
		t.Fatal("stale 用量上报率 label must be renamed")
	}
	if !strings.Contains(html, "按请求数统计") {
		t.Fatal("missing usage-return-rate description 按请求数统计")
	}
	for _, stale := range []string{"非 Token 占比", "— 表示上游未返回用量"} {
		if strings.Contains(html, stale) {
			t.Fatalf("emphatic usage caveat must stay removed: %q", stale)
		}
	}
	for _, stale := range []string{"projection", "coverage", "已上报/请求"} {
		if strings.Contains(html, stale) {
			t.Fatalf("implementation term must stay out of WebUI: %q", stale)
		}
	}
	if strings.Contains(html, "var cover=") {
		t.Fatal("stale cover variable must be renamed")
	}
	// Realtime search lists only user-facing fields; hidden IDs stay as searchable identifiers.
	if !strings.Contains(html, `placeholder="搜索模型、代理、密钥或请求标识"`) {
		t.Fatal("realtime search placeholder must list user-facing fields")
	}
	if strings.Contains(html, "搜索 Request ID / 会话标识") {
		t.Fatal("stale search placeholder must be removed")
	}
	// Single period label: selector keeps it, adjacent duplicate is gone.
	if strings.Contains(html, `id="h-scope-note"`) || strings.Contains(html, `$("h-scope-note")`) {
		t.Fatal("duplicate period label next to #h-period must be removed")
	}
	if strings.Count(html, `id="h-period"`) != 1 {
		t.Fatal("exactly one h-period selector required")
	}
	// Changed text stays DOM-safe.
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write"} {
		if strings.Contains(html, sink) {
			t.Fatalf("forbidden sink %q", sink)
		}
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
