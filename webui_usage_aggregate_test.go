package main

import (
	"os"
	"strings"
	"testing"
)

func loadWebUIBundle(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("webui/index.html")
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestWebUIUsageAggregateRangeContract(t *testing.T) {
	html := loadWebUIBundle(t)
	// Third persisted request with shared seq guard.
	if !strings.Contains(html, "/api/history/usage-aggregate") {
		t.Fatal("missing usage-aggregate request")
	}
	for _, needle := range []string{"/api/history/series", "/api/history/proxy-stats", "S.histSeq", "S.usageAggregate"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing range contract %q", needle)
		}
	}
	if strings.Count(html, "seq!==S.histSeq") < 3 {
		t.Fatal("all three persisted requests must share the histSeq guard")
	}
	// Range tables come from the aggregate, not monitor last_hour.
	if !strings.Contains(html, "renderUsageAggregateTables") {
		t.Fatal("range tables must render from usage-aggregate")
	}
	if strings.Contains(html, "renderUsageTables(use)") {
		t.Fatal("old renderUsageTables(use) from monitor must be removed")
	}
	if strings.Contains(html, "m.usage&&m.usage.last_hour") {
		t.Fatal("range tables must not read monitor usage.last_hour")
	}
	for _, stale := range []string{"(m.models||{})[name]", "(m.tiers||{})[name]", "chanCounts[ck]", "最近一小时调用数", "调用数为最近一小时"} {
		if strings.Contains(html, stale) {
			t.Fatalf("stale last_hour table source must be removed: %q", stale)
		}
	}
	// refreshMonitor (5s poll) must not overwrite the range aggregate.
	monIdx := strings.Index(html, "function refreshMonitor(")
	if monIdx < 0 {
		t.Fatal("missing refreshMonitor")
	}
	monEnd := strings.Index(html[monIdx:], "function renderTopbar()")
	if monEnd < 0 {
		t.Fatal("missing refreshMonitor boundary")
	}
	monBody := html[monIdx : monIdx+monEnd]
	if strings.Contains(monBody, "usage-aggregate") || strings.Contains(monBody, "S.usageAggregate") {
		t.Fatal("refreshMonitor poll must not touch the range aggregate")
	}
	if !strings.Contains(html, "setInterval(refreshMonitor,5000)") {
		t.Fatal("missing 5s monitor poll")
	}
	// Range copy states the selected period and keeps token/ms formatting.
	for _, needle := range []string{"所选时间范围内", "所选范围", "usage-model-note", "usage-tier-note", "历史记录未启用", "暂无范围用量", "范围用量加载", `toLocaleString("en-US")`, `+"ms"`} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing range/loading/format copy %q", needle)
		}
	}
}

func TestWebUIUsageAggregateIndependentLoading(t *testing.T) {
	html := loadWebUIBundle(t)
	// Independent aggregate loading state exists alongside proxy histLoading.
	if !strings.Contains(html, "usageAggregateLoading") {
		t.Fatal("missing independent usageAggregateLoading state")
	}
	if !strings.Contains(html, "usageAggregateLoading:false") && !strings.Contains(html, "usageAggregateLoading: false") {
		t.Fatal("S must init usageAggregateLoading")
	}
	// loadPersistedHistory starts aggregate loading; only the aggregate
	// success/failure with valid seq clears it.
	loadIdx := strings.Index(html, "function loadPersistedHistory(")
	if loadIdx < 0 {
		t.Fatal("missing loadPersistedHistory")
	}
	loadEnd := strings.Index(html[loadIdx:], "function aggregatePersistedSeries(")
	if loadEnd < 0 {
		t.Fatal("missing loadPersistedHistory boundary")
	}
	loadBody := html[loadIdx : loadIdx+loadEnd]
	if !strings.Contains(loadBody, "S.usageAggregateLoading=true") {
		t.Fatal("loadPersistedHistory must start aggregate loading")
	}
	if strings.Count(loadBody, "S.usageAggregateLoading=false") != 2 {
		t.Fatalf("aggregate success/failure must each clear loading once, got %d", strings.Count(loadBody, "S.usageAggregateLoading=false"))
	}
	// Proxy-stats handlers must not touch the aggregate loading flag (no flash).
	proxyIdx := strings.Index(loadBody, "/api/history/proxy-stats")
	if proxyIdx < 0 {
		t.Fatal("missing proxy-stats request")
	}
	aggIdx := strings.Index(loadBody, "/api/history/usage-aggregate")
	if aggIdx < 0 {
		t.Fatal("missing usage-aggregate request")
	}
	proxySlice := loadBody[proxyIdx:aggIdx]
	if strings.Contains(proxySlice, "usageAggregateLoading") {
		t.Fatal("proxy-stats response must not touch usageAggregateLoading")
	}
	aggSlice := loadBody[aggIdx:]
	if !strings.Contains(aggSlice, "seq!==S.histSeq") {
		t.Fatal("aggregate handlers must keep the histSeq guard")
	}
	// Range tables render loading from the independent flag, not histLoading.
	renderIdx := strings.Index(html, "function renderUsageAggregateTables()")
	if renderIdx < 0 {
		t.Fatal("missing renderUsageAggregateTables")
	}
	renderEnd := strings.Index(html[renderIdx:], "function histQueryRange()")
	if renderEnd < 0 {
		t.Fatal("missing renderUsageAggregateTables boundary")
	}
	renderBody := html[renderIdx : renderIdx+renderEnd]
	if !strings.Contains(renderBody, "S.usageAggregateLoading") {
		t.Fatal("range tables must read usageAggregateLoading")
	}
	if strings.Contains(renderBody, "S.histLoading?\"加载中…\":\"暂无范围用量\"") || strings.Contains(renderBody, "S.histLoading?\"范围用量加载中") {
		t.Fatal("range empty/loading copy must not depend on proxy histLoading")
	}
	// Period switch resets the independent flag; reset path keeps seq guard.
	if !strings.Contains(html, `S.usageAggregateLoading=true; S.histSeq++`) {
		t.Fatal("period switch must reset aggregate loading before seq bump")
	}
	// refreshMonitor must stay independent of the aggregate loading flag.
	monIdx := strings.Index(html, "function refreshMonitor(")
	monEnd := strings.Index(html[monIdx:], "function renderTopbar()")
	monBody := html[monIdx : monIdx+monEnd]
	if strings.Contains(monBody, "usageAggregateLoading") {
		t.Fatal("refreshMonitor poll must not touch aggregate loading")
	}
}

func TestWebUIStickyHeaderScope(t *testing.T) {
	html := loadWebUIBundle(t)
	needle := ".table-wrap.scroll-bound table thead th"
	if !strings.Contains(html, needle) {
		t.Fatal("missing sticky header selector for scroll-bound tables")
	}
	idx := strings.Index(html, needle)
	ruleEnd := strings.Index(html[idx:], "}")
	if ruleEnd < 0 {
		t.Fatal("sticky rule unterminated")
	}
	rule := html[idx : idx+ruleEnd]
	for _, want := range []string{"position:sticky", "top:0", "z-index:5", "background:#f2f4f2", "box-shadow"} {
		if !strings.Contains(rule, want) {
			t.Fatalf("sticky rule missing %q: %q", want, rule)
		}
	}
	// Realtime attempts table must stay page-scrolled without sticky header.
	if !strings.Contains(html, `<div class="table-wrap"><table class="table" id="tbl-attempts"`) {
		t.Fatal("realtime attempts table must keep a non-scroll-bound container")
	}
	if strings.Contains(html, ".table-wrap table thead th{position:sticky") {
		t.Fatal("sticky must be scoped to scroll-bound only")
	}
	// Toast/modal/topbar layers stay above the sticky header.
	for _, want := range []string{".toast{", "z-index:50", ".modal{", "z-index:60", ".topbar{", "z-index:20"} {
		if !strings.Contains(html, want) {
			t.Fatalf("missing overlay layer %q", want)
		}
	}
}
