package main

import (
	"strings"
	"testing"
)

func refreshHTML(t *testing.T) string {
	t.Helper()
	return readConfigWebUI(t)
}

func sliceBetween(html, start, end string) string {
	s := strings.Index(html, start)
	if s < 0 {
		return ""
	}
	rest := html[s:]
	if end == "" {
		return rest
	}
	e := strings.Index(rest[len(start):], end)
	if e < 0 {
		return rest
	}
	return rest[:len(start)+e+len(end)]
}

func TestWebUIUsageTabEntryRefresh(t *testing.T) {
	html := refreshHTML(t)
	for _, needle := range []string{
		"function ensureUsageFreshOnTabEntry()",
		"function refreshUsageBackground()",
		"function usageAutoTick()",
		"function usageRefreshIntervalMs()",
		"function installVisibilityRefreshOnce()",
		"S.activeTab",
		`S.activeTab=name`,
		`if(name==="usage"){ ensureUsageFreshOnTabEntry(); }`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("usage tab entry must contain %q", needle)
		}
	}
	swIdx := strings.Index(html, "function switchTab(name)")
	if swIdx < 0 {
		t.Fatal("missing switchTab")
	}
	swEnd := strings.Index(html[swIdx:], "function pillForRequest(")
	if swEnd < 0 {
		t.Fatal("missing switchTab boundary")
	}
	swBlock := html[swIdx : swIdx+swEnd]
	if !strings.Contains(swBlock, "ensureUsageFreshOnTabEntry") {
		t.Fatal("switchTab usage entry must trigger usage freshness")
	}
	if !strings.Contains(swBlock, `return refreshMonitor({force:true})`) {
		t.Fatal("switchTab health entry must immediately refresh monitor")
	}
	// Single-flight: tab entry must not fire while a range load is in flight.
	entryIdx := strings.Index(html, "function ensureUsageFreshOnTabEntry()")
	entryBlock := html[entryIdx : entryIdx+800]
	if !strings.Contains(entryBlock, "S.histLoading||S.usageAggregateLoading") {
		t.Fatal("tab entry must skip when a range request is already in flight")
	}
	if !strings.Contains(entryBlock, "document.hidden") {
		t.Fatal("tab entry must skip when hidden")
	}
	if !strings.Contains(entryBlock, "loadPersistedHistory(true)") {
		t.Fatal("tab entry must request the selected range when empty")
	}
	if !strings.Contains(entryBlock, "refreshUsageBackground()") {
		t.Fatal("tab entry must background-refresh when data exists")
	}
}

func TestWebUIUsageCadenceRateSafety(t *testing.T) {
	html := refreshHTML(t)
	ivIdx := strings.Index(html, "function usageRefreshIntervalMs()")
	if ivIdx < 0 {
		t.Fatal("missing usageRefreshIntervalMs")
	}
	ivBlock := html[ivIdx : ivIdx+600]
	if !strings.Contains(ivBlock, "return 60000") || !strings.Contains(ivBlock, "return 30000") {
		t.Fatal("usage cadence must be 30s for today/24h and 60s for 7d/month")
	}
	if !strings.Contains(ivBlock, "7d") || !strings.Contains(ivBlock, "month") {
		t.Fatal("long-range cadence must gate on 7d/month")
	}
	// Three history endpoints per refresh; both cadences stay below 30/min/IP.
	// 30s => 6 req/min, 60s => 3 req/min.
	if !strings.Contains(html, "30 req/min/IP") && !strings.Contains(html, "30/min/IP") {
		t.Fatal("cadence must document rate safety below 30/min/IP")
	}
	if !strings.Contains(html, "S.usageTimer=setInterval(usageAutoTick,5000)") {
		t.Fatal("usage auto-tick must be owned via S.usageTimer at 5s gate")
	}
	tickIdx := strings.Index(html, "function usageAutoTick()")
	tickBlock := html[tickIdx : tickIdx+700]
	for _, needle := range []string{"document.hidden", "isUsageTabActive()", "S.histLoading||S.usageAggregateLoading", "usageRefreshIntervalMs()", "S.usageLastRefresh", "refreshUsageBackground()"} {
		if !strings.Contains(tickBlock, needle) {
			t.Fatalf("usage tick must contain %q", needle)
		}
	}
	bgIdx := strings.Index(html, "function refreshUsageBackground()")
	bgBlock := html[bgIdx : bgIdx+500]
	for _, needle := range []string{"document.hidden", "!isUsageTabActive()", "S.histLoading||S.usageAggregateLoading", "loadPersistedHistory(false)"} {
		if !strings.Contains(bgBlock, needle) {
			t.Fatalf("background refresh must contain %q", needle)
		}
	}
	// Lifecycle: owned timer cleared on logout, installed once on login.
	if !strings.Contains(html, "if(S.usageTimer){clearInterval(S.usageTimer); S.usageTimer=null;}") {
		t.Fatal("logout must clear the owned usage timer")
	}
	if !strings.Contains(html, "installVisibilityRefreshOnce()") {
		t.Fatal("login must install visibility refresh")
	}
	if !strings.Contains(html, "installVisibilityRefreshOnce._done") {
		t.Fatal("visibility listener must be installed once")
	}
}

func TestWebUIVisibilityRefresh(t *testing.T) {
	html := refreshHTML(t)
	if !strings.Contains(html, `document.addEventListener("visibilitychange"`) {
		t.Fatal("missing visibilitychange listener")
	}
	visIdx := strings.Index(html, `document.addEventListener("visibilitychange"`)
	visBlock := html[visIdx : visIdx+800]
	for _, needle := range []string{"document.hidden", `activeTabName()==="usage"`, `activeTabName()==="health"`, "ensureUsageFreshOnTabEntry()", "refreshMonitor({force:true})", "if(!S.csrf)return;"} {
		if !strings.Contains(visBlock, needle) {
			t.Fatalf("visibility handler must contain %q", needle)
		}
	}
	if !strings.Contains(visBlock, "view-app") || !strings.Contains(visBlock, `classList.contains("hidden")`) {
		t.Fatal("visibility handler must gate on view-app hidden (login view active)")
	}
	// Monitor poll skips when hidden, explicit entry forces.
	monIdx := strings.Index(html, "function refreshMonitor(opts)")
	if monIdx < 0 {
		t.Fatal("missing fenced refreshMonitor(opts)")
	}
	monBlock := html[monIdx : monIdx+600]
	if !strings.Contains(monBlock, "document.hidden&&!opts.force") {
		t.Fatal("monitor must skip when hidden unless forced")
	}
	// Selected period/filters preserved: visibility path never resets historyPeriod.
	if strings.Contains(visBlock, "S.historyPeriod=") {
		t.Fatal("visibility refresh must not reset the selected period")
	}
}

func TestWebUIUsagePeriodFencingAndStalePreservation(t *testing.T) {
	html := refreshHTML(t)
	loadIdx := strings.Index(html, "function loadPersistedHistory(reset)")
	if loadIdx < 0 {
		t.Fatal("missing loadPersistedHistory")
	}
	loadEnd := strings.Index(html[loadIdx:], "function aggregatePersistedSeries(")
	if loadEnd < 0 {
		t.Fatal("missing loadPersistedHistory boundary")
	}
	loadBlock := html[loadIdx : loadIdx+loadEnd]
	for _, needle := range []string{"var reqPeriod=S.historyPeriod", "reqPeriod!==S.historyPeriod", "var isBackground=!reset", "S.usageAutoError", "自动刷新失败，已保留旧数据", "S.usageLastRefresh=Date.now()"} {
		if !strings.Contains(loadBlock, needle) {
			t.Fatalf("range fencing must contain %q", needle)
		}
	}
	if got := strings.Count(loadBlock, "seq!==S.histSeq"); got < 3 {
		t.Fatalf("all three persisted requests must keep histSeq guard, got %d", got)
	}
	if got := strings.Count(loadBlock, "reqPeriod!==S.historyPeriod"); got < 3 {
		t.Fatalf("all three persisted requests must discard on period change, got %d", got)
	}
	if got := strings.Count(loadBlock, "if(isBackground)"); got < 3 {
		t.Fatalf("all three failure paths must branch on background, got %d", got)
	}
	// Aggregate recovery: stale note clears only after all three succeed.
	if !strings.Contains(loadBlock, "var bgFailed=false") {
		t.Fatal("background refresh must track per-refresh aggregate failure")
	}
	if got := strings.Count(loadBlock, "bgFailed=true"); got < 3 {
		t.Fatalf("each background failure path must mark aggregate failure, got %d", got)
	}
	if strings.Contains(loadBlock, "if(!isBackground)S.usageAutoError") {
		t.Fatal("background success must not clear the stale note per-request (early clear)")
	}
	if !strings.Contains(loadBlock, "return Promise.all([p1,p2,p3]).then(function(){") {
		t.Fatal("stale-note recovery must complete via Promise.all over the three range requests")
	}
	aggIdx := strings.Index(loadBlock, "return Promise.all([p1,p2,p3]).then(function(){")
	aggBlock := loadBlock[aggIdx:]
	for _, needle := range []string{"if(!isBackground||bgFailed)return;", "if(seq!==S.histSeq||reqPeriod!==S.historyPeriod)return;", `S.usageAutoError="";`} {
		if !strings.Contains(aggBlock, needle) {
			t.Fatalf("aggregate recovery must contain %q", needle)
		}
	}
	if got := strings.Count(loadBlock, `S.usageAutoError="";`); got != 2 {
		t.Fatalf("stale note must clear only at reset and aggregate completion, got %d clears", got)
	}
	// Background failures preserve tables: stale note rendered without clearing.
	if !strings.Contains(html, `if(S.usageAutoError){ note(mn,S.usageAutoError); note(tn,S.usageAutoError); }`) {
		t.Fatal("aggregate tables must surface the background stale note")
	}
	if !strings.Contains(html, `S.usageAutoError?(" · "+S.usageAutoError):""`) {
		t.Fatal("proxy stats note must surface the background stale note")
	}
	// Initial/manual load keeps current clearing behavior.
	if !strings.Contains(loadBlock, "S.usageAggregate=null; S.usageAggregateError=\"范围用量加载失败，已保留当前范围空状态\"") {
		t.Fatal("manual range failure must keep the empty-state error copy")
	}
}

func TestWebUIMonitorNewestResponseGuard(t *testing.T) {
	html := refreshHTML(t)
	if !strings.Contains(html, "monitorSeq:0") {
		t.Fatal("S must init monitorSeq")
	}
	monIdx := strings.Index(html, "function refreshMonitor(opts)")
	if monIdx < 0 {
		t.Fatal("missing refreshMonitor(opts)")
	}
	monEnd := strings.Index(html[monIdx:], "function renderTopbar()")
	if monEnd < 0 {
		t.Fatal("missing refreshMonitor boundary")
	}
	monBody := html[monIdx : monIdx+monEnd]
	for _, needle := range []string{"var seq=++S.monitorSeq", "if(seq!==S.monitorSeq)return", "pruneCustomAvail("} {
		if !strings.Contains(monBody, needle) {
			t.Fatalf("monitor fencing must contain %q", needle)
		}
	}
	if got := strings.Count(monBody, "if(seq!==S.monitorSeq)return"); got < 2 {
		t.Fatalf("both monitor success and failure must fence stale responses, got %d", got)
	}
	// 5s cheap poll stays; renderUsage must not recurse into monitor.
	if !strings.Contains(html, "setInterval(refreshMonitor,5000)") {
		t.Fatal("cheap 5s monitor poll must stay")
	}
	ruIdx := strings.Index(html, "function renderUsage()")
	ruEnd := strings.Index(html[ruIdx:], "function renderPersistedKPIs()")
	ruBlock := html[ruIdx : ruIdx+ruEnd]
	if strings.Contains(ruBlock, "refreshMonitor") {
		t.Fatal("renderUsage must not recurse into refreshMonitor")
	}
}

func TestWebUIPostActionAwaitRefresh(t *testing.T) {
	html := refreshHTML(t)
	for _, tc := range []struct{ fn, api, extra string }{
		{"function probeProxy(p, btn)", `return api("/api/availability/check-node"`, "return refreshMonitor({force:true})"},
		{"function probeCredential(k, btn)", `return api("/api/availability/check-credential"`, "return refreshMonitor({force:true})"},
		{"function probeCustom(c, btn)", `return api("/api/availability/check-custom"`, "return refreshMonitor({force:true})"},
		{"function bulkCheck()", `return api("/api/availability/check"`, "return refreshMonitor({force:true})"},
		{"function manualRefresh(scope)", `return api("/api/models/refresh"`, "return refreshMonitor({force:true})"},
	} {
		idx := strings.Index(html, tc.fn)
		if idx < 0 {
			t.Fatalf("missing %s", tc.fn)
		}
		rest := html[idx:]
		nextFn := strings.Index(rest[len(tc.fn):], "\nfunction ")
		var block string
		if nextFn < 0 {
			block = rest
		} else {
			block = rest[:len(tc.fn)+nextFn]
		}
		if !strings.Contains(block, tc.api) {
			t.Fatalf("%s must return its api call %q", tc.fn, tc.api)
		}
		if !strings.Contains(block, tc.extra) {
			t.Fatalf("%s must await/return refreshMonitor before unlocking", tc.fn)
		}
	}
	// Config save/apply/reload await monitor and reconcile ghost cache.
	for _, needle := range []string{
		"function handleConfigSaveSuccess(sentUpdate,res)",
		"function reloadConfigFromDisk(btn)",
		"pruneCustomAvailByConfig",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("config freshness must contain %q", needle)
		}
	}
	successIdx := strings.Index(html, "function handleConfigSaveSuccess(sentUpdate,res)")
	rest := html[successIdx:]
	nextFn := strings.Index(rest[len("function handleConfigSaveSuccess(sentUpdate,res)"):], "\nfunction ")
	successBlock := rest
	if nextFn >= 0 {
		successBlock = rest[:len("function handleConfigSaveSuccess(sentUpdate,res)")+nextFn]
	}
	for _, needle := range []string{"pruneCustomAvailByConfig", "return refreshMonitor({force:true})"} {
		if !strings.Contains(successBlock, needle) {
			t.Fatalf("config save success must contain %q", needle)
		}
	}
	reloadIdx := strings.Index(html, "function reloadConfigFromDisk(btn)")
	reloadEnd := strings.Index(html[reloadIdx:], "function refreshMonitor(opts)")
	reloadBlock := html[reloadIdx : reloadIdx+reloadEnd]
	for _, needle := range []string{"pruneCustomAvailByConfig", "refreshMonitor({force:true})"} {
		if !strings.Contains(reloadBlock, needle) {
			t.Fatalf("disk reload success must contain %q", needle)
		}
	}
	// Fallback/pool mutations route through the awaited save.
	for _, fn := range []string{"function saveFallbackModal()", "function deleteFallbackModal()", "function savePoolModal()", "function deletePoolModal()"} {
		idx := strings.Index(html, fn)
		if idx < 0 {
			t.Fatalf("missing %s", fn)
		}
		rest := html[idx:]
		nextFn := strings.Index(rest[len(fn):], "\nfunction ")
		var block string
		if nextFn < 0 {
			block = rest
		} else {
			block = rest[:len(fn)+nextFn]
		}
		if !strings.Contains(block, "return saveConfigAndWait()") {
			t.Fatalf("%s must return saveConfigAndWait so UI updates after refresh", fn)
		}
	}
	// Fallback model discovery never mutates availability: no extra refresh.
	discIdx := strings.Index(html, "/api/fallback/discover")
	if discIdx < 0 {
		t.Fatal("missing fallback discover")
	}
	discBlock := html[discIdx : discIdx+2000]
	if strings.Contains(discBlock, "refreshMonitor") {
		t.Fatal("fallback model discovery must not trigger availability refresh")
	}
}

func TestWebUICustomImmediateAndGhostCleanup(t *testing.T) {
	html := refreshHTML(t)
	probeIdx := strings.Index(html, "function probeCustom(c, btn)")
	if probeIdx < 0 {
		t.Fatal("missing probeCustom")
	}
	probeEnd := strings.Index(html[probeIdx:], "function bulkCheck()")
	probeBlock := html[probeIdx : probeIdx+probeEnd]
	if !strings.Contains(probeBlock, "mergeCustomRowIntoCache(row)") {
		t.Fatal("custom probe must merge the fresh row into cache")
	}
	if strings.Contains(probeBlock, "renderHealth()") {
		t.Fatal("custom probe must not render mid-flight before refreshMonitor (detaches busy button)")
	}
	if !strings.Contains(probeBlock, "return refreshMonitor({force:true})") {
		t.Fatal("custom probe must rely on the subsequent refreshMonitor render")
	}
	// Ghost cleanup: cached rows never override server rows, deleted/renamed pruned.
	for _, needle := range []string{
		"function pruneCustomAvail(serverCustom)",
		"function pruneCustomAvailByConfig()",
		"S.customAvail=(S.customAvail||[]).filter",
		"function mergeCustomRows(server, cached)",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("ghost cleanup must contain %q", needle)
		}
	}
	mergeIdx := strings.Index(html, "function mergeCustomRows(server, cached)")
	mergeBlock := html[mergeIdx : mergeIdx+600]
	srvPos := strings.Index(mergeBlock, "out.push(s)")
	cachedPos := strings.Index(mergeBlock, "out.push(c)")
	if srvPos < 0 || cachedPos < 0 || srvPos > cachedPos {
		t.Fatal("server rows must win over stale cached rows in merge order")
	}
	if !strings.Contains(html, "pruneCustomAvail((data.resources&&data.resources.custom)||null)") {
		t.Fatal("monitor snapshot must reconcile custom cache against server rows")
	}
	if got := strings.Count(html, "pruneCustomAvailByConfig"); got < 4 {
		t.Fatalf("config/load/save paths must reconcile ghost cache, got %d", got)
	}
	// Safety + copy: dependency-free DOM text, concise operator wording.
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
		if strings.Contains(html, sink) {
			t.Fatalf("forbidden DOM sink %q", sink)
		}
	}
	for _, needle := range []string{"自动刷新失败，已保留旧数据", "共 ", "个自定义渠道"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("operator copy must contain %q", needle)
		}
	}
}

func TestWebUIUsageAutoErrorRecoveryAggregate(t *testing.T) {
	html := refreshHTML(t)
	loadIdx := strings.Index(html, "function loadPersistedHistory(reset)")
	if loadIdx < 0 {
		t.Fatal("missing loadPersistedHistory")
	}
	loadEnd := strings.Index(html[loadIdx:], "function aggregatePersistedSeries(")
	if loadEnd < 0 {
		t.Fatal("missing loadPersistedHistory boundary")
	}
	loadBlock := html[loadIdx : loadIdx+loadEnd]
	// Recovery clears only after the complete background refresh for the captured seq/period.
	if !strings.Contains(loadBlock, "return Promise.all([p1,p2,p3]).then(function(){") {
		t.Fatal("recovery must wait for Promise.all over the three range requests")
	}
	aggIdx := strings.Index(loadBlock, "return Promise.all([p1,p2,p3]).then(function(){")
	aggBlock := loadBlock[aggIdx:]
	if !strings.Contains(aggBlock, "bgFailed") || !strings.Contains(aggBlock, `S.usageAutoError="";`) {
		t.Fatal("aggregate completion must clear the stale note only when no part failed")
	}
	if !strings.Contains(aggBlock, "seq!==S.histSeq||reqPeriod!==S.historyPeriod") {
		t.Fatal("aggregate recovery must keep seq/period fencing")
	}
	// No early clear: per-request success must not reset the stale note.
	if strings.Contains(loadBlock, "if(!isBackground)S.usageAutoError") {
		t.Fatal("per-request success must not clear the stale note early")
	}
	// Any single failure keeps the concise stale note and preserves good data.
	if got := strings.Count(loadBlock, "bgFailed=true"); got < 3 {
		t.Fatalf("any failed part must block recovery, got %d marks", got)
	}
	if !strings.Contains(loadBlock, "自动刷新失败，已保留旧数据") {
		t.Fatal("failed background refresh must keep the concise stale note")
	}
}

func TestWebUIVisibilityAfterLogoutNoAPI(t *testing.T) {
	html := refreshHTML(t)
	// Logout resets active state and keeps owned timers cleared.
	loginIdx := strings.Index(html, "function showLogin(){")
	if loginIdx < 0 {
		t.Fatal("missing showLogin")
	}
	loginBlock := html[loginIdx : loginIdx+800]
	for _, needle := range []string{
		`S.csrf=""`,
		`if(S.timer){clearInterval(S.timer); S.timer=null;}`,
		`if(S.usageTimer){clearInterval(S.usageTimer); S.usageTimer=null;}`,
		`S.activeTab="realtime"`,
		"S.monitorSeq++",
		"S.histSeq++",
	} {
		if !strings.Contains(loginBlock, needle) {
			t.Fatalf("logout reset must contain %q", needle)
		}
	}
	// Visibility handler gates on auth + console visibility before any API call.
	visIdx := strings.Index(html, `document.addEventListener("visibilitychange"`)
	if visIdx < 0 {
		t.Fatal("missing visibilitychange listener")
	}
	visBlock := html[visIdx : visIdx+800]
	if !strings.Contains(visBlock, "if(!S.csrf)return;") {
		t.Fatal("visibility after logout must not trigger API calls (S.csrf gate)")
	}
	if !strings.Contains(visBlock, "view-app") {
		t.Fatal("visibility handler must gate on view-app hidden")
	}
	// Refresh entries also gate on auth so stray ticks never send after logout.
	for _, fn := range []string{
		"function refreshMonitor(opts)",
		"function refreshUsageBackground()",
		"function ensureUsageFreshOnTabEntry()",
		"function usageAutoTick()",
	} {
		idx := strings.Index(html, fn)
		if idx < 0 {
			t.Fatalf("missing %s", fn)
		}
		block := html[idx : idx+500]
		if !strings.Contains(block, "if(!S.csrf)") {
			t.Fatalf("%s must gate on S.csrf so logout triggers no API calls", fn)
		}
	}
}
