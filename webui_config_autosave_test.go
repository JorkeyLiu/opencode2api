package main

import (
	"strings"
	"testing"
)

func TestWebUIConfigAutosaveEngine(t *testing.T) {
	html := readConfigWebUI(t)
	for _, needle := range []string{
		"function collectConfigUpdate()",
		"function validateConfigUpdate()",
		"function queueConfigSave(immediate)",
		"function saveConfigNow()",
		"function saveConfigAndWait()",
		"function flushConfigSave()",
		"function handleConfigSaveSuccess(sentUpdate,res)",
		"function reconcileAfterSuccess(sentUpdate, serverConfig)",
		"function setSaveStatus(",
		`id="save-status"`,
		"S.cfgSave",
		"seq:0",
		"inFlight:null",
		"debounce:null",
		"dirty:false",
		"inFlightSeq",
		`if(S.cfgSave.inFlight)return S.cfgSave.inFlight`,
		`setTimeout(function(){ S.cfgSave.debounce=null; flushConfigSave(); },600)`,
		`api("/api/config",{method:"PUT"`,
		"JSON.stringify(sentUpdate)",
		"modalOpen()",
		"unknown or stale secret id",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("autosave engine must contain %q", needle)
		}
	}
	// Old success must not clobber newer input: dirty during flight skips fillConfig.
	if !strings.Contains(html, "hadDirtyDuring") {
		t.Fatal("engine must track dirty-during-flight to avoid clobbering newer input")
	}
	if !strings.Contains(html, "if(!hadDirtyDuring)") {
		t.Fatal("fillConfig must run only when no subsequent dirty")
	}
	// Success uses server authority but preserves in-flight edits via reconciliation.
	for _, needle := range []string{
		"S.config=res.config",
		"appendPromotedChips",
		`ta.value=curLines.join`,
		"renderPoolsList(); refreshPoolBadges()",
		"renderFallbackList()",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("reconciliation must contain %q", needle)
		}
	}
	// No concurrent PUT: single flight + queued latest snapshot.
	if strings.Contains(html, "Promise.all([api(\"/api/config\"") {
		t.Fatal("must not fire concurrent PUTs")
	}
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
		if strings.Contains(html, sink) {
			t.Fatalf("forbidden DOM sink %q", sink)
		}
	}
}

func TestWebUIConfigAutosaveBindings(t *testing.T) {
	html := readConfigWebUI(t)
	// Select/checkbox change saves immediately.
	for _, needle := range []string{
		`"c-prefer","c-level"`,
		`"c-anonymous","c-hist-enabled"`,
		`on(id,"change",function(){ queueConfigSave(true)`,
		`refreshPoolBadges(); queueConfigSave(true)`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("immediate binding must contain %q", needle)
		}
	}
	// Text/number/URL: input debounces, change/blur flushes.
	if !strings.Contains(html, `on(id,"input",function(){ queueConfigSave(false)`) {
		t.Fatal("text inputs must debounce via queueConfigSave(false)")
	}
	if !strings.Contains(html, `on(id,"blur",function(){ if(S.cfgSave.debounce||S.cfgSave.dirty)queueConfigSave(true)`) {
		t.Fatal("text inputs must flush on blur")
	}
	// Advanced protocols JSON only saves when parseable.
	for _, needle := range []string{
		`on("c-protocols","input"`,
		`on("c-protocols","change"`,
		`on("c-protocols","blur"`,
		`JSON.parse(raw); queueConfigSave(false)`,
		`JSON.parse(raw); queueConfigSave(true)`,
		`模型协议手动指定 JSON 无效`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("protocols binding must contain %q", needle)
		}
	}
	// Batch key textareas never save per keystroke: only change/blur.
	for _, needle := range []string{
		`on(n+"-new","change"`,
		`on(n+"-new","blur"`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("batch textarea must save on %q", needle)
		}
	}
	if strings.Contains(html, `on(n+"-new","input"`) {
		t.Fatal("batch textarea must not save on input (per-keystroke PUT forbidden)")
	}
	// Chips remove saves immediately.
	if !strings.Contains(html, `chip.remove(); queueConfigSave(true)`) {
		t.Fatal("chip remove must queue an immediate save")
	}
	// Pool textarea inside modal is draft-only until modal save.
	if !strings.Contains(html, `add.setAttribute("data-role","new")`) && !strings.Contains(html, `setAttribute("data-role","new")`) {
		t.Fatal("pool modal draft textarea must stay draft-only")
	}
}

func TestWebUIConfigModalPersistence(t *testing.T) {
	html := readConfigWebUI(t)
	for _, needle := range []string{
		"function saveFallbackModal()",
		"function deleteFallbackModal()",
		"function savePoolModal()",
		"function deletePoolModal()",
		"saveConfigAndWait()",
		"closeFallbackModal()",
		"closePoolModal()",
		"fallback-modal-error",
		"pool-modal-error",
		`$("fallback-modal-save")`,
		`$("pool-modal-save")`,
		`$("fallback-modal-delete")`,
		`$("pool-modal-delete")`,
		"disabled=true",
		"disabled=false",
		"mapFallbackActiveOnRename(String(S.fbDraft.active",
		"mapRoutingOnRename(oldSel,oldName,name)",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("modal persistence must contain %q", needle)
		}
	}
	// Success closes only inside the save promise continuation.
	for _, fn := range []string{"function saveFallbackModal()", "function deleteFallbackModal()", "function savePoolModal()", "function deletePoolModal()"} {
		idx := strings.Index(html, fn)
		if idx < 0 {
			t.Fatalf("missing %s", fn)
		}
		end := strings.Index(html[idx:], "function ")
		var block string
		if end < 0 {
			block = html[idx:]
		} else {
			// Find next function boundary after current one.
			next := strings.Index(html[idx+len(fn):], "function ")
			if next < 0 {
				block = html[idx:]
			} else {
				block = html[idx : idx+len(fn)+next]
			}
		}
		if !strings.Contains(block, "saveConfigAndWait()") {
			t.Fatalf("%s must persist via saveConfigAndWait", fn)
		}
		thenIdx := strings.Index(block, "saveConfigAndWait()")
		closeIdx := strings.Index(block[thenIdx:], "close")
		catchIdx := strings.Index(block[thenIdx:], ".catch(")
		if closeIdx < 0 || catchIdx < 0 || closeIdx > catchIdx {
			t.Fatalf("%s must close only on success (before catch), failure keeps modal open", fn)
		}
		catchBlock := block[thenIdx+catchIdx:]
		if !strings.Contains(catchBlock, "textContent=(err&&err.message)") {
			t.Fatalf("%s failure must surface modal error without dropping draft", fn)
		}
		if strings.Contains(catchBlock, "closeFallbackModal()") && strings.Contains(fn, "Fallback") && strings.Contains(fn, "save") {
			// saveFallbackModal catch must not close; delete catch must not close either.
			t.Fatalf("%s failure must keep modal open, not close", fn)
		}
	}
	// Fallback secret hygiene in modal save path: failure keeps input, no secret toast/console.
	if strings.Contains(html, "toast(st.originalSecret") || strings.Contains(html, "toast(found") {
		t.Fatal("modal failure must not toast plaintext")
	}
	if strings.Contains(html, "console.log(st.originalSecret") || strings.Contains(html, "console.log(found") {
		t.Fatal("modal failure must not console.log plaintext")
	}
}

func TestWebUIConfigInstantRows(t *testing.T) {
	html := readConfigWebUI(t)
	// Fallback row selection persists immediately.
	rowIdx := strings.Index(html, "function fallbackListRow(ch, idx)")
	if rowIdx < 0 {
		t.Fatal("missing fallbackListRow")
	}
	rowEnd := strings.Index(html[rowIdx:], "function fallbackNormalizeBase")
	if rowEnd < 0 {
		t.Fatal("missing fallbackNormalizeBase boundary")
	}
	rowBlock := html[rowIdx : rowIdx+rowEnd]
	if !strings.Contains(rowBlock, "setFallbackActive(n)") {
		t.Fatal("row click must set active")
	}
	if !strings.Contains(rowBlock, "queueConfigSave(true)") {
		t.Fatal("row click must persist immediately via queueConfigSave(true)")
	}
	// Deactivate persists immediately.
	if !strings.Contains(html, "setFallbackActive(\"\")") {
		t.Fatal("deactivate must clear active")
	}
	clearIdx := strings.Index(html, `var b=$("btn-fallback-clear")`)
	if clearIdx < 0 {
		// Fallback: search by id reference.
		clearIdx = strings.Index(html, "btn-fallback-clear")
	}
	if clearIdx < 0 {
		t.Fatal("missing btn-fallback-clear binding")
	}
	clearBlock := html[clearIdx : clearIdx+600]
	if !strings.Contains(clearBlock, "queueConfigSave(true)") {
		t.Fatal("deactivate must persist immediately")
	}
	// Routing selects persist immediately.
	if !strings.Contains(html, `refreshPoolBadges(); queueConfigSave(true)`) {
		t.Fatal("routing selects must refresh badges and persist immediately")
	}
}

func TestWebUIConfigReloadNoConcurrency(t *testing.T) {
	html := readConfigWebUI(t)
	for _, needle := range []string{
		`$("btn-refresh")`,
		`function reloadConfigFromDisk(btn)`,
		`reloadConfigFromDisk($("btn-refresh"))`,
		`从磁盘重载`,
		"S.cfgSave.reloading=true",
		"S.cfgSave.reloading=false",
		`S.cfgSave.inFlight)||Promise.resolve()`,
		"flushConfigSave()",
		`/api/config/reload`,
		"重载中",
		"磁盘配置已重载",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("reload guard must contain %q", needle)
		}
	}
	if !strings.Contains(html, "queueConfigSave") {
		t.Fatal("reload must coordinate with the autosave queue")
	}
	// Dirty flush failure must block the disk reload (no overwrite of unsaved edits).
	reloadIdx := strings.Index(html, "function reloadConfigFromDisk(btn)")
	if reloadIdx < 0 {
		t.Fatal("missing reloadConfigFromDisk")
	}
	reloadEnd := strings.Index(html[reloadIdx:], "function refreshMonitor()")
	if reloadEnd < 0 {
		// Fallback boundary: next top-level listener.
		reloadEnd = strings.Index(html[reloadIdx:], "(function bindConfigAutosave(){")
	}
	if reloadEnd < 0 {
		t.Fatal("missing reload boundary")
	}
	reloadBlock := html[reloadIdx : reloadIdx+reloadEnd]
	if !strings.Contains(reloadBlock, "if(S.cfgSave&&S.cfgSave.dirty)") {
		t.Fatal("reload must flush only when dirty")
	}
	if !strings.Contains(reloadBlock, ".catch(function(err)") {
		t.Fatal("reload dirty flush must handle failure")
	}
	// Failure path re-enables the button and never reaches doReload.
	catchIdx := strings.Index(reloadBlock, "flushConfigSave()")
	if catchIdx < 0 {
		t.Fatal("missing flushConfigSave in reload block")
	}
	afterFlush := reloadBlock[catchIdx:]
	if !strings.Contains(afterFlush, "btn.disabled=false") {
		t.Fatal("dirty failure must restore the reload button")
	}
	if !strings.Contains(afterFlush, "throw err") {
		t.Fatal("dirty failure must abort before doReload (throw)")
	}
	// Topbar reload must not fan out to monitor/debug refresh.
	if strings.Contains(reloadBlock, "refreshMonitor()") || strings.Contains(reloadBlock, "loadDebugModels()") {
		t.Fatal("topbar reload must not call refreshMonitor/loadDebugModels")
	}
	// Auto monitor polling stays: 5s interval + independent catalog buttons.
	for _, needle := range []string{"setInterval(refreshMonitor,5000)", "btn-refresh-catalog", "btn-refresh-metadata", "btn-refresh-all"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("monitor/catalog refresh must stay intact: %q", needle)
		}
	}
}

func TestWebUIConfigNoBottomSaveRestartHint(t *testing.T) {
	html := readConfigWebUI(t)
	if strings.Contains(html, "保存并应用") {
		t.Fatal("bottom unified save copy must be gone")
	}
	// Bottom sticky operation zone is gone entirely: no container, no buttons.
	if strings.Contains(html, "config-sticky") {
		t.Fatal("bottom config-sticky container must be removed")
	}
	if strings.Contains(html, "btn-reload") || strings.Contains(html, "btn-reveal") {
		t.Fatal("bottom reload/reveal buttons must be removed (topbar reload only)")
	}
	// No buttons may remain at the config bottom: the form must close without
	// a trailing action bar. Check the tail between the last config card and form close.
	formStart := strings.Index(html, `<form id="config-form"`)
	if formStart < 0 {
		t.Fatal("missing config-form")
	}
	formEnd := strings.Index(html[formStart:], "</form>")
	if formEnd < 0 {
		t.Fatal("missing config-form close")
	}
	formBlock := html[formStart : formStart+formEnd]
	lastCard := strings.LastIndex(formBlock, "监听与管理")
	if lastCard < 0 {
		t.Fatal("missing last config card boundary")
	}
	bottomTail := formBlock[lastCard:]
	if strings.Contains(bottomTail, "<button") {
		t.Fatal("config bottom must not contain buttons after sticky removal")
	}
	// save-status survives as a lightweight topbar span, not a bottom bar.
	if !strings.Contains(html, `id="save-status"`) {
		t.Fatal("save-status must survive (topbar lightweight status)")
	}
	topbarStart := strings.Index(html, `<header class="topbar">`)
	if topbarStart < 0 {
		t.Fatal("missing topbar")
	}
	topbarEnd := strings.Index(html[topbarStart:], "</header>")
	if topbarEnd < 0 {
		t.Fatal("missing topbar close")
	}
	topbar := html[topbarStart : topbarStart+topbarEnd]
	if !strings.Contains(topbar, `id="save-status"`) {
		t.Fatal("save-status must live in the topbar, not the removed bottom bar")
	}
	if !strings.Contains(topbar, `id="btn-refresh"`) || !strings.Contains(topbar, "从磁盘重载") {
		t.Fatal("topbar must carry the 从磁盘重载 reload button")
	}
	if strings.Contains(topbar, `id="btn-reveal"`) {
		t.Fatal("topbar must not carry the removed global reveal button")
	}
	// Restart hint stays via topbar + single toast on save success.
	for _, needle := range []string{
		"restart_required",
		"restartFieldLabel",
		"需重启",
		"restart-notice",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("restart hint contract must contain %q", needle)
		}
	}
	// Non-intrusive status element exists; success path sets 已保存 without per-keystroke toast.
	if !strings.Contains(html, `setSaveStatus("已保存"`) {
		t.Fatal("success must update save-status, not toast per keystroke")
	}
	if !strings.Contains(html, `setSaveStatus("保存中`) {
		t.Fatal("saving status must exist")
	}
	if !strings.Contains(html, `setSaveStatus("保存失败"`) {
		t.Fatal("failure status must exist")
	}
}

func TestWebUIConfigUpdateSuccessToast(t *testing.T) {
	html := readConfigWebUI(t)
	// Success toast fires only on the server-success path.
	successIdx := strings.Index(html, "function handleConfigSaveSuccess(sentUpdate,res)")
	if successIdx < 0 {
		t.Fatal("missing handleConfigSaveSuccess")
	}
	// Bound the success handler block before the next top-level function.
	rest := html[successIdx:]
	nextFn := strings.Index(rest[len("function handleConfigSaveSuccess(sentUpdate,res)"):], "\nfunction ")
	var successBlock string
	if nextFn < 0 {
		successBlock = rest
	} else {
		successBlock = rest[:len("function handleConfigSaveSuccess(sentUpdate,res)")+nextFn]
	}
	for _, needle := range []string{`toast("配置已更新")`, `toast("配置已更新；"`, `toast("配置已更新，需重启")`} {
		if !strings.Contains(successBlock, needle) {
			t.Fatalf("success path must contain %q (single PUT success toast, restart merges)", needle)
		}
	}
	// Queue/input path must never toast success: only marks dirty/status.
	queueIdx := strings.Index(html, "function queueConfigSave(immediate)")
	if queueIdx < 0 {
		t.Fatal("missing queueConfigSave")
	}
	queueEnd := strings.Index(html[queueIdx:], "function saveConfigNow()")
	if queueEnd < 0 {
		t.Fatal("missing saveConfigNow boundary for queue block")
	}
	queueBlock := html[queueIdx : queueIdx+queueEnd]
	if strings.Contains(queueBlock, "配置已更新") {
		t.Fatal("queue/input path must not toast 配置已更新; only server success may")
	}
	bindIdx := strings.Index(html, "(function bindConfigAutosave(){")
	if bindIdx < 0 {
		t.Fatal("missing bindConfigAutosave")
	}
	bindEnd := strings.Index(html[bindIdx:], "function revealConfig()")
	if bindEnd < 0 {
		t.Fatal("missing revealConfig boundary for bind block")
	}
	bindBlock := html[bindIdx : bindIdx+bindEnd]
	if strings.Contains(bindBlock, "配置已更新") {
		t.Fatal("input/change/blur bindings must not toast 配置已更新 directly")
	}
	// Modal save/delete success must reuse the single PUT toast: no extra channel/pool success toasts.
	for _, dup := range []string{"已保存渠道", "已新增渠道", "已删除渠道", "已保存代理池", "已新增代理池", "已删除代理池"} {
		if strings.Contains(html, dup) {
			t.Fatalf("modal success must reuse the single PUT toast, duplicate %q must be gone", dup)
		}
	}
	// Failure path still surfaces errors without a success toast.
	if !strings.Contains(html, `toast((err&&err.message)||"保存失败",true)`) {
		t.Fatal("failure path must keep error toast without success toast")
	}
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
		if strings.Contains(html, sink) {
			t.Fatalf("forbidden DOM sink %q", sink)
		}
	}
}
