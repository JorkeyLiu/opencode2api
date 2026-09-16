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
		"fb-key-new",
		"fb-key-id",
		"fb-models",
		"fb-key-toggle",
		"fb-key-status",
		"fb-key-hint",
		"fb-url-preview",
		"fb-url-models",
		"fb-url-request",
		"fb-discover-status",
		`value="chat"`,
		`value="responses"`,
		"Chat Completions",
		"Responses",
		"fallbackProtocolValue",
		"ch.protocol",
		"protocol:proto",
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
