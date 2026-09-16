package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestScopedAuthCSRFOriginStrictNoStore(t *testing.T) {
	manager, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"},
		[]string{"zen-key-12345"}, []string{"go-key-12345"})
	_ = manager
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-node", `{"pool":"shared","index":0}`, "", "")); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauth code=%d want 401", rec.Code)
	}
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-node", `{"pool":"shared","index":0}`, token, "wrong")); rec.Code != http.StatusForbidden {
		t.Fatalf("bad csrf code=%d want 403", rec.Code)
	}
	req := operabilityRequest(http.MethodPost, "/api/availability/check-node", `{"pool":"shared","index":0}`, token, csrf)
	req.Host = "admin.local"
	req.Header.Set("Origin", "http://evil.example")
	if rec := serveAdmin(admin, req); rec.Code != http.StatusForbidden {
		t.Fatalf("origin code=%d want 403", rec.Code)
	}
	if rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-node", `{"pool":"shared","index":0,"url":"http://127.0.0.1:9"}`, token, csrf)); rec.Code != http.StatusBadRequest {
		t.Fatalf("caller url code=%d want 400", rec.Code)
	}
	rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-node", `{"pool":"nope","index":0}`, token, csrf))
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control=%q want no-store", got)
	}
}

func TestScopedStrictValidation(t *testing.T) {
	cases := []struct {
		name string
		body string
		want int
	}{
		{"null", `null`, http.StatusBadRequest},
		{"array", `[]`, http.StatusBadRequest},
		{"scalar", `123`, http.StatusBadRequest},
		{"unknown field", `{"pool":"shared","index":0,"url":"http://127.0.0.1:9"}`, http.StatusBadRequest},
		{"missing pool", `{"index":0}`, http.StatusBadRequest},
		{"missing index", `{"pool":"shared"}`, http.StatusBadRequest},
		{"negative index", `{"pool":"shared","index":-1}`, http.StatusBadRequest},
		{"unknown pool", `{"pool":"nope","index":0}`, http.StatusBadRequest},
		{"index out of range", `{"pool":"shared","index":7}`, http.StatusBadRequest},
		{"trailing values", `{"pool":"shared","index":0}{}`, http.StatusBadRequest},
		{"empty body", ``, http.StatusBadRequest},
	}
	for _, tc := range cases {
		_, admin, token, csrf := bulkAdmin(t,
			map[string][]string{"shared": {"direct"}},
			ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"},
			nil, nil)
		rec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-node", tc.body, token, csrf))
		if rec.Code != tc.want {
			t.Fatalf("%s: code=%d want %d body=%s", tc.name, rec.Code, tc.want, rec.Body.String())
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s: missing no-store", tc.name)
		}
	}
}

func TestScopedRateLimitBusy(t *testing.T) {
	manager, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"},
		nil, nil)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(bulkChatSuccessBody("bulk-free-model")))
	}))
	defer srv.Close()
	rt := manager.current.Load()
	rt.gateway.cfg.Upstream.Zen = srv.URL
	rt.gateway.cfg.Upstream.Go = srv.URL
	seedBulkProbeCatalog(rt.gateway)
	var last *httptest.ResponseRecorder
	for i := 0; i < 4; i++ {
		last = serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check-node", `{"pool":"shared","index":0}`, token, csrf))
	}
	if last.Code != http.StatusTooManyRequests {
		t.Fatalf("4th scoped code=%d want 429 body=%s", last.Code, last.Body.String())
	}
	_, admin2, token2, csrf2 := bulkAdmin(t,
		map[string][]string{"shared": {"direct"}},
		ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"},
		nil, nil)
	rt2 := admin2.manager.current.Load()
	rt2.gateway.bulkMu.Lock()
	defer rt2.gateway.bulkMu.Unlock()
	if rec := serveAdmin(admin2, operabilityRequest(http.MethodPost, "/api/availability/check-node", `{"pool":"shared","index":0}`, token2, csrf2)); rec.Code != http.StatusConflict {
		t.Fatalf("busy code=%d want 409", rec.Code)
	}
}

func TestScopedSameSchemaMergePreservesUnrelated(t *testing.T) {
	manager, admin, token, csrf := bulkAdmin(t,
		map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"},
		[]string{"zen-key-12345"}, []string{"go-key-12345"})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(bulkChatSuccessBody("bulk-free-model")))
	}))
	defer srv.Close()
	rt := manager.current.Load()
	rt.gateway.cfg.Upstream.Zen = srv.URL
	rt.gateway.cfg.Upstream.Go = srv.URL
	seedBulkProbeCatalog(rt.gateway)
	// Baseline bulk run to establish two-node snapshot.
	bulkRec := serveAdmin(admin, operabilityRequest(http.MethodPost, "/api/availability/check", `{}`, token, csrf))
	if bulkRec.Code != http.StatusOK {
		t.Fatalf("bulk code=%d body=%s", bulkRec.Code, bulkRec.Body.String())
	}
	var bulkResp bulkCheckResponse
	if err := json.Unmarshal(bulkRec.Body.Bytes(), &bulkResp); err != nil {
		t.Fatal(err)
	}
	if len(bulkResp.Nodes) < 2 {
		t.Fatalf("bulk must return 2 nodes, got %+v", bulkResp)
	}
	snapBefore := rt.gateway.bulkSnapshot.Load()
	if snapBefore == nil {
		t.Fatal("bulk snapshot missing")
	}
	globalBefore := snapBefore.CheckedAt
	node1Before := time.Time{}
	for _, n := range snapBefore.Nodes {
		if n.Index == 1 && n.Pool == "shared" {
			if n.LastChecked != nil {
				node1Before = *n.LastChecked
			}
		}
	}
	if node1Before.IsZero() {
		t.Fatal("node 1 last-checked missing before scoped")
	}
	time.Sleep(5 * time.Millisecond)
	// Scoped check for node 0 only. Use a fresh admin to avoid bulk rate limit.
	_, admin2, token2, csrf2 := bulkAdmin(t,
		map[string][]string{"shared": {"direct", "http://127.0.0.1:8081"}},
		ProxyRoutingConfig{Anonymous: "shared", Zen: "shared", Go: "shared"},
		[]string{"zen-key-12345"}, []string{"go-key-12345"})
	// Point the second gateway at the same stub and copy the snapshot so the
	// merge-preservation assertion is meaningful.
	rt2 := admin2.manager.current.Load()
	rt2.gateway.cfg.Upstream.Zen = srv.URL
	rt2.gateway.cfg.Upstream.Go = srv.URL
	seedBulkProbeCatalog(rt2.gateway)
	rt2.gateway.bulkSnapshot.Store(snapBefore)
	scopedRec := serveAdmin(admin2, operabilityRequest(http.MethodPost, "/api/availability/check-node", `{"pool":"shared","index":0}`, token2, csrf2))
	if scopedRec.Code != http.StatusOK {
		t.Fatalf("scoped code=%d body=%s", scopedRec.Code, scopedRec.Body.String())
	}
	body := scopedRec.Body.String()
	for _, secret := range []string{"zen-key-12345", "go-key-12345", "user@", "hunter2"} {
		if strings.Contains(body, secret) {
			t.Fatalf("secret %q leaked in scoped response", secret)
		}
	}
	var scoped bulkCheckResponse
	if err := json.Unmarshal(scopedRec.Body.Bytes(), &scoped); err != nil {
		t.Fatal(err)
	}
	if scoped.TotalNodes != 1 {
		t.Fatalf("scoped total_nodes=%d want 1", scoped.TotalNodes)
	}
	if len(scoped.Nodes) != 1 {
		t.Fatalf("scoped nodes=%d want 1", len(scoped.Nodes))
	}
	if scoped.Nodes[0].Pool != "shared" || scoped.Nodes[0].Index != 0 {
		t.Fatalf("scoped node identity wrong: %+v", scoped.Nodes[0])
	}
	if len(scoped.Custom) != 0 {
		t.Fatalf("scoped must not include custom channels, got %d", len(scoped.Custom))
	}
	if scoped.TestedNodes == 0 {
		t.Fatalf("scoped must test the node: %+v", scoped)
	}
	// Same outcome labels as batch: success maps to node available.
	found := false
	for _, n := range bulkResp.Nodes {
		if n.Pool == "shared" && n.Index == 0 {
			if (n.Zen == "success" || n.Zen == "available") != (scoped.Nodes[0].Zen == "success" || scoped.Nodes[0].Zen == "available") {
				t.Fatalf("scoped zen label diverged from batch: batch=%q scoped=%q", n.Zen, scoped.Nodes[0].Zen)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("batch node 0 missing for label parity")
	}
	// Merge preserves unrelated node and global batch timestamp.
	after := rt2.gateway.bulkSnapshot.Load()
	if after == nil {
		t.Fatal("merged snapshot missing")
	}
	if !after.CheckedAt.Equal(globalBefore) {
		t.Fatalf("global batch timestamp must be preserved: %v vs %v", after.CheckedAt, globalBefore)
	}
	kept := false
	for _, n := range after.Nodes {
		if n.Pool == "shared" && n.Index == 1 {
			if n.LastChecked == nil || !n.LastChecked.Equal(node1Before) {
				t.Fatalf("unrelated node 1 last-checked must be preserved: %+v vs %v", n.LastChecked, node1Before)
			}
			kept = true
		}
	}
	if !kept {
		t.Fatal("unrelated node 1 missing after merge")
	}
	// Resources projection refreshes the scoped row without dropping others.
	res := admin2.manager.Resources()
	seen := map[int]bool{}
	for _, p := range res.Proxies {
		if p.Pool == "shared" {
			seen[p.Index] = true
			if p.LastChecked == nil {
				t.Fatalf("proxy %d missing last-checked after scoped merge", p.Index)
			}
		}
	}
	if !seen[0] || !seen[1] {
		t.Fatalf("both pool rows must survive scoped merge: %+v", seen)
	}
}

func TestScopedWebUIContracts(t *testing.T) {
	data, err := readWebUIFile()
	if err != nil {
		t.Fatal(err)
	}
	html := string(data)
	// Single explanatory sentence under 使用统计, exactly once.
	want := "用量仅统计上游实际回报，不做估算，未知显示“—”。所选时间同时决定指标、趋势与历史记录。"
	if got := strings.Count(html, want); got != 1 {
		t.Fatalf("usage sentence count=%d want 1", got)
	}
	if got := strings.Count(html, "不做估算"); got != 1 {
		t.Fatalf("不做估算 count=%d want 1", got)
	}
	// No dynamic per-model/per-upstream footer.
	for _, stale := range []string{"范围内共 ", "旧历史用量", "实测 ", "，跳过 ", "，自定义 ", "，无模型 "} {
		if idx := strings.Index(html, "function bulkCheck()"); idx >= 0 {
			end := strings.Index(html[idx:], "function manualRefresh(")
			block := html[idx:]
			if end >= 0 {
				block = block[:end]
			}
			if strings.Contains(block, stale) {
				t.Fatalf("bulk completion must not contain %q", stale)
			}
		}
		_ = stale
	}
	if !strings.Contains(html, "批量检测完成：共检测 ") || !strings.Contains(html, "可用 ") || !strings.Contains(html, "不可用 ") {
		t.Fatal("bulk completion must read 批量检测完成：共检测 X 个节点，可用 Y 个，不可用 Z 个")
	}
	// In-progress concise, no protocol trivia.
	bulkIdx := strings.Index(html, "function bulkCheck()")
	if bulkIdx < 0 {
		t.Fatal("missing bulkCheck")
	}
	bulkEnd := strings.Index(html[bulkIdx:], "function manualRefresh(")
	bulkBlock := html[bulkIdx:]
	if bulkEnd >= 0 {
		bulkBlock = bulkBlock[:bulkEnd]
	}
	if !strings.Contains(bulkBlock, "批量检测进行中…") {
		t.Fatal("bulk in-progress must be concise 批量检测进行中…")
	}
	for _, trivia := range []string{"POST stream", "stream:false", "并发 4", "单发 10", "整体 30", "无模型 "} {
		if strings.Contains(bulkBlock, trivia) {
			t.Fatalf("bulk in-progress must not expose trivia %q", trivia)
		}
	}
	// Node availability derives from native lanes only.
	if !strings.Contains(html, "function bulkNodeAvailable(") {
		t.Fatal("missing bulkNodeAvailable helper")
	}
	if strings.Contains(html, "function bulkNodeAvailable(") {
		start := strings.Index(html, "function bulkNodeAvailable(")
		end := strings.Index(html[start:], "function probeProxy(")
		block := html[start:]
		if end >= 0 {
			block = block[:end]
		}
		if !strings.Contains(block, `"available"`) || !strings.Contains(block, `"success"`) {
			t.Fatal("node availability must treat available/success lanes")
		}
		if strings.Contains(block, "custom") || strings.Contains(block, "no_model") {
			t.Fatal("node availability must not include custom channels or no_model text")
		}
	}
	// Batch control upper-right with normal spacing; progress hint below table right-aligned.
	if !strings.Contains(html, `justify-content:flex-end`) {
		t.Fatal("批量检测 control must sit upper-right")
	}
	if !strings.Contains(html, `id="bulk-state"`) || !strings.Contains(html, `text-align:right`) {
		t.Fatal("completion/error hint must be right-aligned below the table")
	}
	// Transport-only probe contract preserved server-side but unused by the table button.
	if !strings.Contains(html, "/api/availability/check-node") {
		t.Fatal("table 检测 button must use the scoped availability path")
	}
	probeIdx := strings.Index(html, "function probeProxy(p, btn)")
	probeEnd := strings.Index(html[probeIdx:], "function bulkCheck()")
	probeBlock := html[probeIdx : probeIdx+probeEnd]
	if strings.Contains(probeBlock, "/api/proxies/probe") {
		t.Fatal("table 检测 button must stop using the transport-only probe")
	}
}
