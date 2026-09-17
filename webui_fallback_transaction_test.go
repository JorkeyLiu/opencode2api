package main

import (
	"strings"
	"testing"
)

func fallbackSaveBlock(t *testing.T, html string) (saveBlock, catchBlock string) {
	t.Helper()
	idx := strings.Index(html, "function saveFallbackModal()")
	if idx < 0 {
		t.Fatal("missing saveFallbackModal")
	}
	end := strings.Index(html[idx:], "function deleteFallbackModal()")
	if end < 0 {
		t.Fatal("missing deleteFallbackModal boundary for save block")
	}
	saveBlock = html[idx : idx+end]
	cIdx := strings.Index(saveBlock, ".catch(function(err)")
	if cIdx < 0 {
		t.Fatal("missing save catch block")
	}
	catchBlock = saveBlock[cIdx:]
	return saveBlock, catchBlock
}

func fallbackDeleteBlock(t *testing.T, html string) (delBlock, catchBlock string) {
	t.Helper()
	idx := strings.Index(html, "function deleteFallbackModal()")
	if idx < 0 {
		t.Fatal("missing deleteFallbackModal")
	}
	end := strings.Index(html[idx:], "function renderChips(")
	if end < 0 {
		t.Fatal("missing renderChips boundary for delete block")
	}
	delBlock = html[idx : idx+end]
	cIdx := strings.Index(delBlock, ".catch(function(err)")
	if cIdx < 0 {
		t.Fatal("missing delete catch block")
	}
	catchBlock = delBlock[cIdx:]
	return delBlock, catchBlock
}

func TestWebUIFallbackAddFailureRollback(t *testing.T) {
	html := readConfigWebUI(t)
	_, catch := fallbackSaveBlock(t, html)
	// Exact restore to the pre-action snapshot.
	if !strings.Contains(catch, "S.fbDraft.channels=backup.channels; S.fbDraft.active=backup.active;") {
		t.Fatal("save failure must exactly restore S.fbDraft channels/active from backup")
	}
	// Prior bug re-pushed failed values after restoring backup; that must stay removed.
	for _, stale := range []string{
		"S.fbDraft.channels.push(draftEntry)",
		"draftEntry",
		"S.fbDraft.channels[st.index]=re",
		"var re={id:chanID",
		"S.fbDraft.channels.splice(k,1)",
	} {
		if strings.Contains(catch, stale) {
			t.Fatalf("save failure must not push failed values back after restore: %q", stale)
		}
	}
	// No second push of any kind in the failure path: retry submits exactly one candidate.
	if strings.Contains(catch, ".push(") {
		t.Fatal("save failure path must not push any entry; retry submits exactly one candidate")
	}
	// Failure keeps the modal open with the server error visible and refreshes the list.
	if strings.Contains(catch, "closeFallbackModal()") {
		t.Fatal("save failure must keep modal open, not close")
	}
	if !strings.Contains(catch, `if(errEl)errEl.textContent=(err&&err.message)||"保存失败"`) {
		t.Fatal("save failure must surface the server error in the modal")
	}
	if !strings.Contains(catch, "renderFallbackList()") {
		t.Fatal("save failure must re-render the restored draft list")
	}
}

func TestWebUIFallbackSameModalRetry(t *testing.T) {
	html := readConfigWebUI(t)
	save, catch := fallbackSaveBlock(t, html)
	// Stable generated ID reused across retries in the same modal.
	if !strings.Contains(save, "String(st.chanID||fallbackGenerateChannelID())") {
		t.Fatal("new-channel save must reuse the stable modal chanID across retries")
	}
	// Strict guards stay but must exclude the editing row so a retry never self-collides.
	for _, needle := range []string{
		`if(i!==st.index&&String(chans[i].id||"").trim()===chanID)`,
		`if(j!==st.index&&String(chans[j].name||"").trim()===name)`,
	} {
		if !strings.Contains(save, needle) {
			t.Fatalf("duplicate guards must exclude the editing row: %q", needle)
		}
	}
	// Failure must not touch modal fields: values live in DOM/S.fbModal until success.
	for _, stale := range []string{"S.fbModal=", "st.fields", "keyInput.value="} {
		if strings.Contains(catch, stale) {
			t.Fatalf("save failure must preserve modal form values, must not contain %q", stale)
		}
	}
}

func TestWebUIFallbackCloseReopenClean(t *testing.T) {
	html := readConfigWebUI(t)
	openIdx := strings.Index(html, "function openFallbackModal(idx)")
	if openIdx < 0 {
		t.Fatal("missing openFallbackModal")
	}
	openEnd := strings.Index(html[openIdx:], "function closeFallbackModal()")
	if openEnd < 0 {
		t.Fatal("missing closeFallbackModal boundary")
	}
	openBlock := html[openIdx : openIdx+openEnd]
	if !strings.Contains(openBlock, "fallbackGenerateChannelID()") {
		t.Fatal("new-channel open must generate a fresh stable ID")
	}
	if !strings.Contains(openBlock, "isNew?fallbackGenerateChannelID()") {
		t.Fatal("fresh ID must apply to the new-channel path")
	}
	closeIdx := strings.Index(html, "function closeFallbackModal()")
	if closeIdx < 0 {
		t.Fatal("missing closeFallbackModal")
	}
	closeEnd := strings.Index(html[closeIdx:], "function saveFallbackModal()")
	if closeEnd < 0 {
		t.Fatal("missing saveFallbackModal boundary for close block")
	}
	closeBlock := html[closeIdx : closeIdx+closeEnd]
	// Close wipes plaintext and drops modal identity so the next new-channel open starts clean.
	if !strings.Contains(closeBlock, `S.fbModal={index:null,origName:""`) {
		t.Fatal("close must reset fbModal state")
	}
	if strings.Contains(closeBlock, "chanID") {
		t.Fatal("close reset must drop the modal chanID so reopen generates a fresh ID")
	}
	if !strings.Contains(closeBlock, `value=""`) {
		t.Fatal("close must wipe the key input value")
	}
}

func TestWebUIFallbackEditFailureRollback(t *testing.T) {
	html := readConfigWebUI(t)
	_, catch := fallbackSaveBlock(t, html)
	if !strings.Contains(catch, "S.fbDraft.channels=backup.channels; S.fbDraft.active=backup.active;") {
		t.Fatal("edit failure must restore the old list/active state globally")
	}
	if strings.Contains(catch, "S.fbDraft.channels[st.index]") {
		t.Fatal("edit failure must not write edited values back into the restored draft")
	}
	if strings.Contains(catch, "closeFallbackModal()") {
		t.Fatal("edit failure must keep modal open for retry")
	}
	if !strings.Contains(catch, `if(errEl)errEl.textContent=(err&&err.message)||"保存失败"`) {
		t.Fatal("edit failure must surface the server error for correction")
	}
}

func TestWebUIFallbackDeleteFailureRollback(t *testing.T) {
	html := readConfigWebUI(t)
	_, catch := fallbackDeleteBlock(t, html)
	if !strings.Contains(catch, "S.fbDraft.channels=backup.channels; S.fbDraft.active=backup.active;") {
		t.Fatal("delete failure must restore the deleted entry and active reference")
	}
	if strings.Contains(catch, "closeFallbackModal()") {
		t.Fatal("delete failure must keep modal open, not close")
	}
	if !strings.Contains(catch, `if(errEl)errEl.textContent=(err&&err.message)||"删除失败"`) {
		t.Fatal("delete failure must surface the server error in the modal")
	}
	if !strings.Contains(catch, "renderFallbackList()") {
		t.Fatal("delete failure must re-render the restored draft list")
	}
}

func TestWebUIFallbackReconcileIDFirst(t *testing.T) {
	html := readConfigWebUI(t)
	idx := strings.Index(html, "function reconcileAfterSuccess(sentUpdate, serverConfig)")
	if idx < 0 {
		t.Fatal("missing reconcileAfterSuccess")
	}
	end := strings.Index(html[idx:], "function revealConfig()")
	if end < 0 {
		// Fallback boundary: the config submit listener follows reconcile.
		end = strings.Index(html[idx:], `$("config-form").addEventListener`)
	}
	if end < 0 {
		t.Fatal("missing reconcile boundary")
	}
	block := html[idx : idx+end]
	// Stable ID first; legacy name match only for id-less payloads.
	for _, needle := range []string{
		"var byId={};",
		"var byNameLegacy={};",
		"var cid=String(c.id||",
		"byId[cid]:byNameLegacy[",
		"if(c._newKey)delete c._newKey",
		"c.api_key={id:skid,display:sdisp}",
		"renderFallbackList()",
	} {
		if !strings.Contains(block, needle) {
			t.Fatalf("ID-first reconciliation must contain %q", needle)
		}
	}
	// Pure name-keyed matching must stay removed: a display-name rename must
	// resolve by stable ID, never create a duplicate/stale entry.
	if strings.Contains(block, "var byName={};") {
		t.Fatal("name-only reconcile map must stay removed; match by stable ID first")
	}
	if strings.Contains(block, "byName[String(c.name") {
		t.Fatal("reconcile must not match drafts by display name; ID-first only")
	}
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
		if strings.Contains(block, sink) {
			t.Fatalf("forbidden DOM sink %q", sink)
		}
	}
}
