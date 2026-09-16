package main

import (
	"os"
	"strings"
	"testing"
)

func readConfigWebUI(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("webui/index.html")
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestWebUIConfigGroups(t *testing.T) {
	html := readConfigWebUI(t)
	for _, needle := range []string{
		"系统配置",
		"接入与鉴权",
		"代理池与路由",
		"备用模型渠道",
		"上游服务与重试",
		"模型目录",
		"网络与性能",
		"日志与历史",
		"监听与管理",
		"首选上游顺序",
		"当前启用渠道",
		"启用此渠道",
		"停用备用渠道",
		"当匿名渠道在已有对话中暂时不可用时，可由已启用的备用渠道继续处理",
		"config-sticky",
		"config-item-card",
		"config-item-grid",
		"完整地址预览",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing config group copy %q", needle)
		}
	}
	// Advanced JSON stays inside the form behind a details element.
	if !strings.Contains(html, "<details") {
		t.Fatal("advanced JSON must use details")
	}
	formStart := strings.Index(html, `<form id="config-form">`)
	if formStart < 0 {
		t.Fatal("missing config-form")
	}
	formEnd := strings.Index(html[formStart:], "</form>")
	if formEnd < 0 {
		t.Fatal("missing config-form close")
	}
	form := html[formStart : formStart+formEnd]
	if !strings.Contains(form, `id="c-protocols"`) {
		t.Fatal("c-protocols must stay inside the form")
	}
	if !strings.Contains(form, "<details") {
		t.Fatal("details must wrap the advanced JSON inside the form")
	}
	// Single submit in the whole bundle: login + config save only.
	if got := strings.Count(html, `type="submit"`); got != 2 {
		t.Fatalf("submit count=%d want 2 (login + config save)", got)
	}
	// All long-lived config element IDs preserved.
	for _, id := range []string{
		"c-prefer", "c-listen", "c-web-listen", "c-session", "c-web-enabled",
		"c-anonymous", "server_keys-chips", "server_keys-new",
		"zen_keys-chips", "zen_keys-new", "go_keys-chips", "go_keys-new",
		"pools-editor", "btn-pool-add", "pools-error",
		"c-route-anon", "c-route-zen", "c-route-go",
		"fallback-editor", "btn-fallback-add", "c-fallback-active",
		"btn-fallback-activate", "btn-fallback-clear", "fallback-error",
		"c-up-zen", "c-up-go", "c-attempts", "c-timeout",
		"c-refresh", "c-protocols",
		"c-idle", "c-idle-host", "c-max-host", "c-idle-timeout", "c-connect", "c-cooldown",
		"c-level", "c-ring",
		"c-hist-enabled", "c-hist-dir", "c-hist-retention", "c-hist-max",
		"config-form", "btn-reload", "btn-reveal",
	} {
		if !strings.Contains(html, id) {
			t.Fatalf("missing config id %q", id)
		}
	}
}

func TestWebUIFallbackProtocolKeyPreview(t *testing.T) {
	html := readConfigWebUI(t)
	for _, needle := range []string{
		"fb-protocol",
		"fb-name",
		"fb-base",
		"fb-model",
		"fb-effort",
		"fb-key-new",
		"fb-key-id",
		"fb-key-toggle",
		"fb-key-status",
		"fb-key-hint",
		"fb-url-preview",
		"fb-url-models",
		"fb-url-request",
		"fb-discover-status",
		"fb-discover",
		`value="chat"`,
		`value="responses"`,
		"Chat Completions",
		"Responses",
		"fallbackProtocolValue",
		"fallbackEffortValue",
		"fillModelSelect",
		"ch.protocol",
		"protocol:proto",
		"reasoning_effort:effort",
		"思考强度",
		"供应商默认",
		"仅从列表选择",
		"当前配置：",
		"供应商列表未返回",
		"保存后将替换已保存密钥",
		"已保存密钥",
		"fallbackAPIRoot",
		"fallbackEndpointURL",
		"fallbackModelsURL",
		"模型列表地址：",
		"请求地址：",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing fallback contract %q", needle)
		}
	}
	// Model select is the sole editable/submitted source: data-role fb-model
	// must be on a select, never a free-text input; the legacy picker role and
	// the redundant apply button must be gone.
	if !strings.Contains(html, `modelSel.setAttribute("data-role","fb-model")`) && !strings.Contains(html, `setAttribute("data-role","fb-model")`) {
		t.Fatal("fb-model select creation missing")
	}
	if strings.Contains(html, `modelInput.setAttribute("data-role","fb-model")`) {
		t.Fatal("free-text fb-model input must be removed; select is the sole source")
	}
	if strings.Contains(html, `data-role","fb-models"`) || strings.Contains(html, `data-role="fb-models"`) {
		t.Fatal("legacy fb-models role must be removed; fb-model select is the sole source")
	}
	if strings.Contains(html, "fb-use-model") || strings.Contains(html, "选用") {
		t.Fatal("redundant apply-model button must be removed; select change submits directly")
	}
	// Effort select carries the four options with Low/Medium/High display.
	for _, needle := range []string{`"low"`, `"medium"`, `"high"`, `"Low"`, `"Medium"`, `"High"`} {
		if !strings.Contains(html, needle) {
			t.Fatalf("effort selector must contain %q", needle)
		}
	}
	// Key entry is a fresh password input, never prefilled with masked material.
	if !strings.Contains(html, `keyInput.type="password"`) {
		t.Fatal("fallback key input must default to password")
	}
	if !strings.Contains(html, `keyInput.value=""`) {
		t.Fatal("fallback key input must start empty, never masked material")
	}
	if !strings.Contains(html, "显示") || !strings.Contains(html, "隐藏") {
		t.Fatal("fallback key toggle must carry show/hide copy")
	}
	// Empty base renders an em-dash placeholder.
	if !strings.Contains(html, "—") {
		t.Fatal("URL preview must render em-dash for empty base")
	}
	// Discovery consumes the safe structured error fields.
	for _, needle := range []string{"errObj.reason", "errObj.http_status", "errObj.endpoint", "errObj.elapsed_ms", "fallbackDiscoverReasonCN"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("discovery must consume structured field %q", needle)
		}
	}
	for _, needle := range []string{"DNS解析失败", "连接被拒绝", "请求超时", "TLS错误", "上游HTTP", "响应不是有效JSON", "模型列表为空"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing diagnosable discover copy %q", needle)
		}
	}
}

func TestWebUIFallbackActiveRenameMapping(t *testing.T) {
	html := readConfigWebUI(t)
	// Mapping helper must exist alongside the pool routing helper with the same style.
	for _, needle := range []string{
		"function mapFallbackActiveOnRename(active, oldName, newName)",
		"if(active===oldName)return newName",
		"function mapRoutingOnRename(sel, oldName, newName)",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing fallback rename mapping %q", needle)
		}
	}
	// Rename handler must remap the enabled reference before rebuilding options.
	for _, needle := range []string{
		`card.getAttribute("data-channel")`,
		"mapFallbackActiveOnRename(cur,oldName,next)",
		`card.setAttribute("data-channel",next)`,
		"syncFallbackActive(mapped)",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("fallback rename handler must contain %q", needle)
		}
	}
	// Old buggy shape (rebuild with the stale value) must be gone.
	if strings.Contains(html, `card.setAttribute("data-channel",nameInput.value.trim());`+"\n    syncFallbackActive($(\"c-fallback-active\").value);") {
		t.Fatal("fallback rename must map active through mapFallbackActiveOnRename, not sync stale value directly")
	}
	// Delete keeps the existing clear-when-enabled semantics.
	if !strings.Contains(html, "card.remove(); syncFallbackActive(") {
		t.Fatal("fallback delete must keep sync-after-remove semantics")
	}
	// Explicit active must win over stale select value, otherwise rename A->B loses B.
	if !strings.Contains(html, "(active!==undefined&&active!==null)?active:(sel.value") {
		t.Fatal("syncFallbackActive must prefer the explicit active so rename A->B selects B")
	}
	if !strings.Contains(html, `sel.value=found?cur:""`) {
		t.Fatal("syncFallbackActive must clear when the active name no longer exists (delete B clears)")
	}
}

func TestWebUIConfigNoInternalCopy(t *testing.T) {
	html := readConfigWebUI(t)
	for _, stale := range []string{"pinned", "429接管", "会话级固化", "不随active"} {
		if strings.Contains(html, stale) {
			t.Fatalf("internal copy must stay removed: %q", stale)
		}
	}
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
		if strings.Contains(html, sink) {
			t.Fatalf("forbidden DOM sink %q", sink)
		}
	}
	// Pool cards share the unified card style, no inline temp styling.
	if !strings.Contains(html, "pool-card config-item-card") {
		t.Fatal("pool cards must use the unified card style")
	}
	if !strings.Contains(html, "fb-card config-item-card") {
		t.Fatal("fallback cards must use the unified card style")
	}
	if strings.Contains(html, `card.style.border="1px solid var(--line)"`) {
		t.Fatal("dynamic cards must not carry inline temp borders")
	}
}

func TestWebUIConfigDesignSystem(t *testing.T) {
	html := readConfigWebUI(t)
	for _, needle := range []string{
		"repeat(12,minmax(0,1fr))",
		"minmax(0,1fr)",
		"span-3", "span-4", "span-5", "span-6", "span-8", "span-12",
		"check-field", "check-row",
		"pool-status", "status-panel",
		"fb-model-row",
		"min-height:36px",
		"min-width:0",
		"text-overflow:ellipsis",
		"config-item-grid",
		"(min-width:800px) and (max-width:1199px)",
		"(max-width:799px)",
		"max-width:1280px",
		"匿名通道",
		"运行状态",
		"代理文件",
		`.key-row{display:flex;gap:8px;align-items:center;min-width:0;max-width:100%;width:100%}`,
		`.fb-model-row{display:grid;grid-template-columns:auto minmax(0,1fr);gap:8px;align-items:center;max-width:100%;width:100%;min-width:0}`,
		`.fb-model-row select{min-width:0;max-width:100%;width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}`,
		`var keyWrap=document.createElement("div"); keyWrap.className="field span-6";`,
		`var modelWrap=document.createElement("div"); modelWrap.className="field span-6";`,
		`(active!==undefined&&active!==null)?active:(sel.value||"")`,
		`sel.title=sel.value`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing design-system contract %q", needle)
		}
	}
	if strings.Contains(html, "repeat(auto-fit") {
		t.Fatal("unstable auto-fit layout must be replaced by explicit 12-col grid")
	}
	if strings.Contains(html, "(max-width:1100px)") {
		t.Fatal("legacy single breakpoint must be replaced by 800/1200 breakpoints")
	}
	// Key/model must share one row on desktop/tablet (span-6 each), never full-row span-12.
	if strings.Contains(html, `keyWrap.className="field span-12"`) {
		t.Fatal("fallback key field must be span-6, not span-12 full row")
	}
	if strings.Contains(html, `modelWrap.className="field span-12"`) {
		t.Fatal("fallback model field must be span-6, not span-12 full row")
	}
	// Model row must fill its half column, never a fixed 720px or full-card width.
	if strings.Contains(html, ".fb-model-row{display:grid;grid-template-columns:auto minmax(0,1fr);gap:8px;align-items:center;max-width:720px") {
		t.Fatal("fb-model-row must not use fixed 720px; it must fill its half column with max-width:100%")
	}
	if strings.Contains(html, ".config-item-grid>.field.span-3,.config-item-grid>.field.span-4,.config-item-grid>.field.span-5{grid-column:span 6}") {
		t.Fatal("tablet must keep proto/base/effort 3/5/4 on one row, not collapse to span 6")
	}
	// Mobile stacks via the single 799px rule; no extra full-card model max-width.
	if !strings.Contains(html, ".fb-model-row{grid-template-columns:1fr;max-width:100%;width:100%}") {
		t.Fatal("mobile fb-model-row must stack full width within its field")
	}
}
