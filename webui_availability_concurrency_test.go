package main

import (
	"strings"
	"testing"
)

func availConcurrencyHTML(t *testing.T) string {
	t.Helper()
	data, err := readWebUIFile()
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// Batch/single busy-state separation: batch owns the global lock, singles
// own only their own button lifecycle.
func TestWebUIAvailBusySeparation(t *testing.T) {
	html := availConcurrencyHTML(t)
	for _, needle := range []string{
		`availBatchBusy`,
		`availSingles`,
		`function isAvailBatchActive(`,
		`function setAvailBatchBusy(`,
		`function setAvailBusy(`,
		`function availSingleBegin(`,
		`function availSingleEnd(`,
		`function applyAvailBusyState(`,
		`function availSinglesActive(`,
		`data-avail-key`,
		`delete S.availSingles`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("availability concurrency contract must contain %q", needle)
		}
	}
	// Dead helper must stay removed: renderHealth uses one centralized path.
	if strings.Contains(html, "function reapplyAvailSingles(") {
		t.Fatal("dead reapplyAvailSingles must stay removed; renderHealth uses applyAvailBusyState")
	}
	// Centralized helpers wire batch + active-single state in one place.
	singlesActive := sliceFn(html, "function availSinglesActive(", "function applyAvailBusyState(")
	if singlesActive == "" {
		t.Fatal("missing availSinglesActive")
	}
	if !strings.Contains(singlesActive, "S.availSingles") {
		t.Fatal("availSinglesActive must read S.availSingles")
	}
	applyState := sliceFn(html, "function applyAvailBusyState(", "function setAvailBatchBusy(")
	if applyState == "" {
		t.Fatal("missing applyAvailBusyState")
	}
	for _, needle := range []string{
		`isAvailBatchActive()`,
		`S.availSingles`,
		`markProbeBusy(`,
		`disabled=true`,
	} {
		if !strings.Contains(applyState, needle) {
			t.Fatalf("centralized busy helper must contain %q", needle)
		}
	}
	// Batch helper disables every per-row button plus the batch buttons.
	batchHelper := sliceFn(html, "function setAvailBatchBusy(", "function setAvailBusy(")
	if batchHelper == "" {
		t.Fatal("missing setAvailBatchBusy")
	}
	for _, needle := range []string{
		`availBatchButtons()`,
		`querySelectorAll(".probe-btn")`,
	} {
		if !strings.Contains(batchHelper, needle) {
			t.Fatalf("batch helper must contain %q (disables all detection controls)", needle)
		}
	}
	// Compat wrapper stays so older contracts keep resolving.
	if !strings.Contains(html, "function setAvailBusy(on){ setAvailBatchBusy(on); }") {
		t.Fatal("setAvailBusy must remain as a batch-only compat wrapper")
	}
	// Single completion clears only its own key; it must never clear batch
	// state or another active single.
	singleEnd := sliceFn(html, "function availSingleEnd(", "function availBusyMessage(")
	if singleEnd == "" {
		t.Fatal("missing availSingleEnd")
	}
	if !strings.Contains(singleEnd, "delete S.availSingles") {
		t.Fatal("single completion must delete only its own availSingles key")
	}
	for _, forbidden := range []string{"setAvailBatchBusy(false)", "setAvailBusy(false)"} {
		if strings.Contains(singleEnd, forbidden) {
			t.Fatalf("single completion must not call %q (would clear batch/another single)", forbidden)
		}
	}
}

func availSingleBlock(t *testing.T, html, fn, end string) string {
	t.Helper()
	block := sliceFn(html, fn, end)
	if block == "" {
		t.Fatalf("missing %s", fn)
	}
	return block
}

// Singles check only the batch flag, mark only their own button busy, and
// finalize only their own button after the forced monitor refresh.
func TestWebUISingleBusyIsLocal(t *testing.T) {
	html := availConcurrencyHTML(t)
	singles := []struct{ fn, end string }{
		{"function probeProxy(p, btn)", "function customRowKey("},
		{"function probeCredential(k, btn)", "function probeCustom(c, btn)"},
		{"function probeCustom(c, btn)", "function availBatchButtons("},
	}
	for _, s := range singles {
		block := availSingleBlock(t, html, s.fn, s.end)
		if !strings.Contains(block, "isAvailBatchActive()") {
			t.Fatalf("%s must gate only on the batch flag", s.fn)
		}
		if !strings.Contains(block, "availSingleBegin(") {
			t.Fatalf("%s must mark only its own button busy", s.fn)
		}
		if !strings.Contains(block, "return refreshMonitor({force:true})") {
			t.Fatalf("%s must await refreshMonitor before finalizing", s.fn)
		}
		if !strings.Contains(block, "availSingleEnd(") {
			t.Fatalf("%s must finalize only its own button", s.fn)
		}
		for _, forbidden := range []string{
			"setAvailBusy(true)", "setAvailBatchBusy(true)",
			"setAvailBusy(false)", "setAvailBatchBusy(false)",
			`querySelectorAll(".probe-btn")`,
		} {
			if strings.Contains(block, forbidden) {
				t.Fatalf("%s must not contain %q (would disable peers or own the batch lock)", s.fn, forbidden)
			}
		}
		if strings.Contains(block, "if(S.availBusy)") {
			t.Fatalf("%s must not gate on the legacy global S.availBusy", s.fn)
		}
	}
	// Custom single keeps the audited fix: no mid-flight render before the
	// forced monitor refresh (that would detach the busy button).
	probeBlock := availSingleBlock(t, html, "function probeCustom(c, btn)", "function availBatchButtons(")
	if strings.Contains(probeBlock, "renderHealth()") {
		t.Fatal("custom probe must not render mid-flight before refreshMonitor (detaches busy button)")
	}
}

// Batches gate on the batch flag and own the global disable helper. While
// individual probes are active the same-UI batch entry rejects locally with
// an operator message and never takes the batch lock or calls the API.
func TestWebUIBatchDisablesAll(t *testing.T) {
	html := availConcurrencyHTML(t)
	batches := []struct{ fn, end string }{
		{"function bulkCheck()", "(function(){ var b=$(\"btn-bulk-check\")"},
		{"function bulkCheckCredentials()", "(function(){ var b=$(\"btn-bulk-check-credentials\")"},
		{"function bulkCheckCustoms()", "(function(){ var b=$(\"btn-bulk-check-customs\")"},
	}
	for _, b := range batches {
		block := availSingleBlock(t, html, b.fn, b.end)
		if !strings.Contains(block, "isAvailBatchActive()") {
			t.Fatalf("%s must check the batch flag", b.fn)
		}
		if !strings.Contains(block, "availSinglesActive()") {
			t.Fatalf("%s must check active singles before starting", b.fn)
		}
		if !strings.Contains(block, "单个检测进行中") {
			t.Fatalf("%s must reject with the singles-active operator message", b.fn)
		}
		if !strings.Contains(block, `toast("单个检测进行中，请完成后再开始批量检测",true)`) {
			t.Fatalf("%s must toast the singles-active rejection as bad", b.fn)
		}
		if !strings.Contains(block, "setAvailBatchBusy(true)") {
			t.Fatalf("%s must take the batch lock", b.fn)
		}
		if !strings.Contains(block, "setAvailBatchBusy(false)") {
			t.Fatalf("%s must release the batch lock", b.fn)
		}
		if !strings.Contains(block, "return refreshMonitor({force:true})") {
			t.Fatalf("%s must await refreshMonitor before unlocking", b.fn)
		}
		if strings.Contains(block, "availSingleBegin(") || strings.Contains(block, "availSingleEnd(") {
			t.Fatalf("%s must not use single-button state", b.fn)
		}
		// Singles-active guard must precede the batch lock and the batch API
		// so the rejection never globally disables controls or sends.
		guardIdx := strings.Index(block, "availSinglesActive()")
		lockIdx := strings.Index(block, "setAvailBatchBusy(true)")
		if guardIdx < 0 || lockIdx < 0 || guardIdx > lockIdx {
			t.Fatalf("%s must check availSinglesActive before setAvailBatchBusy(true)", b.fn)
		}
		apiIdx := strings.Index(block, `api("/api/availability/`)
		if apiIdx < 0 || guardIdx > apiIdx {
			t.Fatalf("%s must reject on active singles before calling the batch API", b.fn)
		}
	}
}

// renderHealth recreates per-row buttons, so it must reapply both the batch
// disable and each still-active single busy state through one centralized
// helper instead of erasing them or diverging into duplicated logic.
func TestWebUIRenderHealthReappliesBusy(t *testing.T) {
	html := availConcurrencyHTML(t)
	idx := strings.Index(html, "function renderHealth(){")
	if idx < 0 {
		t.Fatal("missing renderHealth")
	}
	rest := html[idx:]
	end := strings.Index(rest, "function nodeAnonObsOf(")
	var block string
	if end < 0 {
		block = rest
	} else {
		block = rest[:end]
	}
	for _, needle := range []string{
		`data-avail-key`,
		`applyAvailBusyState(`,
	} {
		if !strings.Contains(block, needle) {
			t.Fatalf("renderHealth must reapply busy state via %q", needle)
		}
	}
	// Centralized path: renderHealth delegates to the helper; the helper
	// owns the batch/single busy-state logic. No duplicated divergent inline
	// copies may reappear in renderHealth.
	if got := strings.Count(block, "applyAvailBusyState("); got < 3 {
		t.Fatalf("renderHealth must call applyAvailBusyState for proxy/credential/custom rows, got %d", got)
	}
	for _, stale := range []string{`var _pk=`, `var _kk=`, `var _ck=`} {
		if strings.Contains(block, stale) {
			t.Fatalf("renderHealth must not contain duplicated inline busy logic %q; use applyAvailBusyState", stale)
		}
	}
	applyState := sliceFn(html, "function applyAvailBusyState(", "function setAvailBatchBusy(")
	if applyState == "" {
		t.Fatal("missing applyAvailBusyState")
	}
	for _, needle := range []string{
		`S.availSingles`,
		`isAvailBatchActive()`,
		`markProbeBusy(`,
	} {
		if !strings.Contains(applyState, needle) {
			t.Fatalf("centralized helper must reapply busy state via %q", needle)
		}
	}
	// All dynamic availability content stays text-safe.
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
		if strings.Contains(block, sink) {
			t.Fatalf("renderHealth must not use forbidden sink %q", sink)
		}
	}
}
