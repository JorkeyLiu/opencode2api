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
		"停用备用渠道",
		"config-item-grid",
		"完整地址预览",
		"fallback-modal",
		"fallback-modal-body",
		"fallback-modal-save",
		"fallback-modal-delete",
		"pool-modal",
		"pool-modal-body",
		"pool-modal-save",
		"pool-modal-delete",
		"save-status",
		"从磁盘重载",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing config group copy %q", needle)
		}
	}
	// Unified bottom save is gone: autosave persists, modals save independently.
	for _, stale := range []string{"保存并应用", "验证、保存并应用", "fallback-active-note", "当前启用：", "当前未启用备用渠道"} {
		if strings.Contains(html, stale) {
			t.Fatalf("unified save / active-note copy must stay removed: %q", stale)
		}
	}
	// Duplicate enable concepts are gone: only list selection + deactivate remain.
	for _, stale := range []string{"启用此渠道", "启用所选渠道", "c-fallback-active", "btn-fallback-activate", "当前启用渠道"} {
		if strings.Contains(html, stale) {
			t.Fatalf("duplicate enable concept must stay removed: %q", stale)
		}
	}
	// Advanced JSON stays inside the form behind a details element.
	if !strings.Contains(html, "<details") {
		t.Fatal("advanced JSON must use details")
	}
	formStart := strings.Index(html, `<form id="config-form"`)
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
	// Only the login form keeps a submit button; config persists via autosave.
	if got := strings.Count(html, `type="submit"`); got != 1 {
		t.Fatalf("submit count=%d want 1 (login only, config autosaves)", got)
	}
	// All long-lived config element IDs preserved (sticky/reveal removed, topbar reload + status remain).
	for _, id := range []string{
		"c-prefer", "c-listen", "c-web-listen", "c-session", "c-web-enabled",
		"c-anonymous", "server_keys-chips", "server_keys-new",
		"zen_keys-chips", "zen_keys-new", "go_keys-chips", "go_keys-new",
		"pools-editor", "btn-pool-add", "pools-error",
		"c-route-anon", "c-route-zen", "c-route-go",
		"fallback-editor", "btn-fallback-add", "btn-fallback-clear", "fallback-error",
		"fallback-modal", "fallback-modal-body",
		"fallback-modal-save", "fallback-modal-delete", "fallback-modal-cancel",
		"pool-modal", "pool-modal-body",
		"pool-modal-save", "pool-modal-delete", "pool-modal-cancel",
		"c-up-zen", "c-up-go", "c-attempts", "c-timeout",
		"c-refresh", "c-protocols",
		"c-idle", "c-idle-host", "c-max-host", "c-idle-timeout", "c-connect", "c-cooldown",
		"c-level", "c-ring",
		"c-hist-enabled", "c-hist-dir", "c-hist-retention", "c-hist-max",
		"config-form", "btn-refresh", "save-status",
	} {
		if !strings.Contains(html, id) {
			t.Fatalf("missing config id %q", id)
		}
	}
	// Global sensitive-value button/modal and bottom sticky bar are gone.
	for _, stale := range []string{"config-sticky", "btn-reveal", "btn-reload", "查看敏感值", "二次验证"} {
		if strings.Contains(html, stale) {
			t.Fatalf("global reveal/sticky must stay removed: %q", stale)
		}
	}
	if strings.Contains(html, `<div id="modal"`) || strings.Contains(html, `id="modal-content"`) || strings.Contains(html, `id="modal-close"`) {
		t.Fatal("global sensitive modal must stay removed (fallback/pool modals remain)")
	}
	if strings.Contains(html, `$("modal")`) || strings.Contains(html, `$("modal-content")`) || strings.Contains(html, `$("modal-close")`) {
		t.Fatal("global sensitive modal must stay removed (fallback/pool modals remain)")
	}
	// Topbar reload entry lives in the header.
	if !strings.Contains(html, `id="btn-refresh"`) || !strings.Contains(html, "从磁盘重载") {
		t.Fatal("topbar must carry 从磁盘重载 reload entry")
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
		"fb-url-preview",
		"fb-url-models",
		"fb-url-request",
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
		"当前配置：",
		"供应商列表未返回",
		"openFallbackModal",
		"saveFallbackModal",
		"deleteFallbackModal",
		"fallback-modal-save",
		"fallback-modal-delete",
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
	// Masked tail sentence must be gone.
	if strings.Contains(html, "已保存密钥") {
		t.Fatal("fallback must not show saved-key tail copy")
	}
	if strings.Contains(html, "保存后将替换已保存密钥") {
		t.Fatal("legacy replace-key hint must be removed")
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
	// Key entry defaults to password with masked value, never placeholder-masked.
	if !strings.Contains(html, `keyInput.type="password"`) {
		t.Fatal("fallback key input must default to password")
	}
	if !strings.Contains(html, `keyInput.value=pendingNew||savedDisplay`) {
		t.Fatal("editing channel must default input value to masked display, not placeholder")
	}
	if strings.Contains(html, `placeholder=savedDisplay`) {
		t.Fatal("masked display must live in input value, never placeholder")
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

func TestWebUIFallbackEyeReauthMasked(t *testing.T) {
	html := readConfigWebUI(t)
	for _, needle := range []string{
		`keyToggle.className="icon-btn"`,
		`aria-label","显示密钥"`,
		`aria-label","隐藏密钥"`,
		`setEye(false)`,
		`st.revealed`,
		`st.originalSecret`,
		`origDisplay`,
		`origId`,
		`revealConfig()`,
		`body:"{}"`,
		`String(chans[i].name||"")===String(st.origName`,
		`fallbackSecretById(candidates,savedSecretId)`,
		`keyInput.value=pendingNew||savedDisplay`,
		`keyInput.value=savedDisplay`,
		`st.originalSecret=""`,
		`delete entry._newKey`,
		`entry._newKey=nv`,
		`S.fbModal={index:null,origName:"",origDisplay`,
		`X-CSRF-Token`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing eye/reveal contract %q", needle)
		}
	}
	// Logged-in session reveals directly: no second password prompt, no password payload.
	for _, stale := range []string{
		`prompt("请再次输入管理密码`,
		`请再次输入管理密码以查看该渠道密钥`,
		`请再次输入管理密码以查看敏感值`,
		`revealConfig(pw)`,
		`revealConfig(password)`,
		`function revealConfig(password)`,
		`password:password`,
		`{password:`,
		`二次验证`,
	} {
		if strings.Contains(html, stale) {
			t.Fatalf("password-gated reveal must stay removed: %q", stale)
		}
	}
	// Reveal stays POST-only behind the login session: never a GET.
	if strings.Contains(html, `/api/config/reveal",{method:"GET"`) || strings.Contains(html, `api("/api/config/reveal",{method:"GET"`) {
		t.Fatal("reveal must stay POST-only, never GET")
	}
	// Plaintext must never enter toast/console or a long-lived draft.
	if strings.Contains(html, `toast(found`) || strings.Contains(html, `toast(st.originalSecret`) {
		t.Fatal("plaintext must never enter toast")
	}
	if strings.Contains(html, `console.log(found`) || strings.Contains(html, `console.log(st.originalSecret`) {
		t.Fatal("plaintext must never enter console")
	}
	if strings.Contains(html, `keyToggle.textContent`) {
		t.Fatal("eye toggle must render SVG icons, not textContent glyphs")
	}
	if strings.Contains(html, `keyToggle.innerHTML`) {
		t.Fatal("eye toggle must build SVG via DOM, not innerHTML")
	}
	// Standard dependency-free eye / eye-off inline SVG built via createElementNS.
	for _, needle := range []string{
		`createElementNS(eyeSvgNS,"svg")`,
		`createElementNS(eyeSvgNS,"path")`,
		`createElementNS(eyeSvgNS,"circle")`,
		`createElementNS(eyeSvgNS,"line")`,
		`M1 12s4-8 11-8`,
		`fbEyeIcon(open)`,
		`clear(keyToggle); keyToggle.appendChild(fbEyeIcon(open))`,
		`.key-row button svg{width:18px`,
		`aria-hidden","true"`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("eye icon contract must contain %q", needle)
		}
	}
	// Masked state renders plain eye; visible state adds the slash line for eye-off.
	if !strings.Contains(html, `eyeSlash.setAttribute("x1","1")`) {
		t.Fatal("visible state must render eye-off slash line")
	}
	// Overlay/adornment: eye lives inside the key input on the right, input keeps right padding.
	for _, needle := range []string{
		`keyToggle.type="button"`,
		`.key-row{position:relative`,
		`padding-right:40px`,
		`position:absolute;right:4px`,
		`transform:translateY(-50%)`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("eye overlay contract must contain %q", needle)
		}
	}
	if strings.Contains(html, `.key-row{display:flex;gap:8px;align-items:center`) {
		t.Fatal("eye button must be an input-internal overlay, not an external flex row")
	}
	for _, stale := range []string{"fb-key-status", "fb-key-hint", "留空保留", "已输入新密钥", "新建渠道需填写", "已保存密钥", "留空保留已保存"} {
		if strings.Contains(html, stale) {
			t.Fatalf("redundant key helper must stay removed: %q", stale)
		}
	}
	if strings.Contains(html, `toast(found`) || strings.Contains(html, `toast(st.originalSecret`) {
		t.Fatal("plaintext must never enter toast")
	}
}

func TestWebUIFallbackDiscoverToastOnly(t *testing.T) {
	html := readConfigWebUI(t)
	for _, needle := range []string{
		`toast("已获取 "`,
		`toast("未返回模型"`,
		`toast("已选择模型 "`,
		`discBtn.textContent="获取中`,
		`discBtn.disabled=true`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("discovery toast contract must contain %q", needle)
		}
	}
	for _, stale := range []string{
		"fb-discover-status",
		"仅从列表选择",
		"新建渠道请先获取",
		"已选中当前值",
		"切换即生效",
		"保存后持久化",
		"保存后生效",
		"提交配置后生效",
		"仅支持 HTTP/HTTPS",
		"空=供应商默认",
	} {
		if strings.Contains(html, stale) {
			t.Fatalf("simplified modal must not contain %q", stale)
		}
	}
	if strings.Contains(html, `modelLabel.textContent="模型（`) {
		t.Fatal("model label must be plain 模型 without parenthetical")
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
	// Compact list + modal is the only editor: rename maps active, delete clears it.
	// Enabled state is expressed only by list highlight (.active) and pill (已启用/未启用).
	for _, needle := range []string{
		"function renderFallbackList()",
		"function fallbackListRow(ch, idx)",
		"function openFallbackModal(idx)",
		"function saveFallbackModal()",
		"function deleteFallbackModal()",
		"function setFallbackActive(name)",
		"S.fbDraft",
		"mapFallbackActiveOnRename(String(S.fbDraft.active",
		"fb-row",
		`row.className="fb-row"+(String(S.fbDraft.active`,
		`"已启用"`,
		`"未启用"`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("fallback list/modal must contain %q", needle)
		}
	}
	for _, stale := range []string{"fallback-active-note", "当前启用：", "当前未启用备用渠道"} {
		if strings.Contains(html, stale) {
			t.Fatalf("active-note text must stay removed: %q", stale)
		}
	}
	// Expanded per-channel cards and duplicate enable buttons are gone.
	for _, stale := range []string{"function fallbackCard(ch)", "fb-card", "启用此渠道", "btn-fallback-activate"} {
		if strings.Contains(html, stale) {
			t.Fatalf("expanded fallback card must stay removed: %q", stale)
		}
	}
	// Delete-active clears the selection.
	if !strings.Contains(html, `if(String(S.fbDraft.active||"")===name)S.fbDraft.active=""`) {
		t.Fatal("fallback delete must clear active when the enabled channel is removed")
	}
}

func TestWebUIPoolListModal(t *testing.T) {
	html := readConfigWebUI(t)
	for _, needle := range []string{
		"pool-list",
		"pool-row",
		"pool-modal",
		"pool-modal-body",
		"pool-modal-save",
		"pool-modal-delete",
		"pool-modal-cancel",
		"function renderPoolsList()",
		"function poolListRow(p, idx)",
		"function openPoolModal(idx)",
		"function savePoolModal()",
		"function deletePoolModal()",
		"function closePoolModal()",
		"S.poolDraft",
		"S.poolModal",
		"节点 ",
		"运行中",
		"暂存",
		"每行新增一个代理节点",
		"代理文件",
		"运行状态",
		"仍被路由引用",
		"mapRoutingOnRename(oldSel,oldName,name)",
		"c-route-anon",
		"c-route-zen",
		"c-route-go",
		"poolInputs",
		"poolInputNames",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("pool list/modal must contain %q", needle)
		}
	}
	for _, stale := range []string{"pool-card", "function poolCard("} {
		if strings.Contains(html, stale) {
			t.Fatalf("inline pool card must stay removed: %q", stale)
		}
	}
	if strings.Contains(html, "fb-discover") && strings.Contains(html, "pool-discover") {
		t.Fatal("proxy pools must not gain model discovery")
	}
	// Routing guards: three routes required, delete blocked while referenced.
	for _, needle := range []string{
		"匿名 / Zen / Go 必须各选择一个代理池",
		"至少需要一个代理池",
		"refs.a===cur||refs.z===cur||refs.g===cur",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("pool routing guard must contain %q", needle)
		}
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
	if !strings.Contains(html, "pool-row") {
		t.Fatal("pool compact list must use pool-row")
	}
	if !strings.Contains(html, "fb-row") {
		t.Fatal("fallback compact list must use fb-row")
	}
	if !strings.Contains(html, "fallback-modal") {
		t.Fatal("fallback editor must use a dedicated modal")
	}
	if !strings.Contains(html, "pool-modal") {
		t.Fatal("pool editor must use a dedicated modal")
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
		`.key-row{position:relative;display:block;min-width:0;max-width:100%;width:100%}`,
		`.key-row input{width:100%;min-width:0;padding-right:40px;box-sizing:border-box}`,
		`.key-row button{position:absolute;right:4px;top:50%;transform:translateY(-50%)`,
		`.key-row button:focus-visible`,
		`.fb-model-row{display:grid;grid-template-columns:auto minmax(0,1fr);gap:8px;align-items:center;max-width:100%;width:100%;min-width:0}`,
		`.fb-model-row select{min-width:0;max-width:100%;width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}`,
		`var keyWrap=document.createElement("div"); keyWrap.className="field span-6";`,
		`var modelWrap=document.createElement("div"); modelWrap.className="field span-6";`,
		`sel.title=sel.value`,
		"fb-row",
		"fallback-modal",
		"isCustomRow",
		"customChannelOf",
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

func TestWebUISidebarScrollBackground(t *testing.T) {
	html := readConfigWebUI(t)
	// Viewport shell: app fills viewport, body/shell do not page-scroll on desktop.
	for _, needle := range []string{
		`#view-app{height:100vh;height:100dvh;display:flex;flex-direction:column;min-height:0;overflow:hidden}`,
		`.layout{flex:1;min-height:0;overflow:hidden;`,
		`.main{flex:1;min-width:0;min-height:0;overflow-y:auto;`,
		`.topbar{flex:none;position:sticky;top:0;`,
		`@media (min-width:1200px){`,
		`.layout::before{display:none}`,
		`.main>*{max-width:1280px;`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing viewport shell contract %q", needle)
		}
	}
	if got := strings.Count(html, ".layout::before{display:none}"); got != 3 {
		t.Fatalf("layout backdrop must stay disabled in desktop, tablet and mobile queries, got %d", got)
	}
	if strings.Contains(html, `.layout::before{content:"";position:absolute;`) {
		t.Fatal("legacy absolute layout backdrop must stay removed")
	}
	// Grid/flex only: no fixed sidebar with magic margin offset.
	if strings.Contains(html, `.nav{position:fixed;`) {
		t.Fatal("sidebar must use flex column, not fixed+magic offset")
	}
	if strings.Contains(html, `.main{margin:0 0 0 212px;`) {
		t.Fatal("content must not use 212px left-offset margin; flex row carries the sidebar")
	}
	// min-height:0 lets flex scroll children shrink correctly.
	for _, needle := range []string{
		`.layout{flex:1;min-height:0;`,
		`min-height:0;overflow-y:auto;overflow-x:hidden;z-index:15}`,
		`.main{flex:1;min-width:0;min-height:0;overflow-y:auto;`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("scroll flex child must carry min-height:0 %q", needle)
		}
	}
	// Narrow breakpoints keep top-nav behavior without forcing a fixed full-height sidebar.
	for _, needle := range []string{
		`@media (min-width:800px) and (max-width:1199px){`,
		`@media (max-width:799px){`,
		`#view-app{height:auto;min-height:100vh;min-height:100dvh;overflow:visible;display:block}`,
		`.topbar{position:static}`,
		`.main{margin:0;width:100%;`,
		`overflow:visible;`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("responsive override must contain %q", needle)
		}
	}
	// Login view must not be captured by the app shell layout.
	if !strings.Contains(html, `.login{min-height:100vh;`) {
		t.Fatal("login view must keep its own full-height centered layout")
	}
	if !strings.Contains(html, `<section id="view-login"`) {
		t.Fatal("login section must stay outside the app shell")
	}
}

func TestWebUIAppShellScrollToastModal(t *testing.T) {
	html := readConfigWebUI(t)
	// Only the main content scrolls; sidebar/topbar stay visible via flex shell.
	for _, needle := range []string{
		`#view-app{height:100vh;height:100dvh;display:flex;flex-direction:column;min-height:0;overflow:hidden}`,
		`.layout{flex:1;min-height:0;overflow:hidden;`,
		`.main{flex:1;min-width:0;min-height:0;overflow-y:auto;`,
		`.toast{position:fixed;`,
		`.modal{position:fixed;`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("app shell scroll contract must contain %q", needle)
		}
	}
	// Toast/modal stay viewport-relative siblings outside the scrolling main.
	mainClose := strings.Index(html, "</main>")
	if mainClose < 0 {
		t.Fatal("missing </main> boundary")
	}
	afterMain := html[mainClose:]
	for _, id := range []string{`id="toast"`, `id="fallback-modal"`, `id="pool-modal"`} {
		if !strings.Contains(afterMain, id) {
			t.Fatalf("overlay %q must live outside scrolling main for viewport display", id)
		}
	}
	if strings.Contains(afterMain, `id="modal"`) || strings.Contains(html, `id="modal-content"`) {
		t.Fatal("global sensitive modal must stay removed")
	}
	mainOpen := strings.Index(html, "<main")
	if mainOpen < 0 {
		t.Fatal("missing <main")
	}
	mainBlock := html[mainOpen:mainClose]
	for _, id := range []string{`id="toast"`, `class="toast"`, `class="modal"`} {
		if strings.Contains(mainBlock, id) {
			t.Fatalf("overlay %q must not be nested inside scrolling content", id)
		}
	}
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
		if strings.Contains(html, sink) {
			t.Fatalf("forbidden DOM sink %q", sink)
		}
	}
}

func TestWebUIConfigUpdateToastSingleFlight(t *testing.T) {
	html := readConfigWebUI(t)
	if !strings.Contains(html, `toast("配置已更新")`) {
		t.Fatal("PUT success must toast 配置已更新")
	}
	successIdx := strings.Index(html, "function handleConfigSaveSuccess(sentUpdate,res)")
	if successIdx < 0 {
		t.Fatal("missing handleConfigSaveSuccess")
	}
	if !strings.Contains(html[successIdx:], `toast("配置已更新")`) {
		t.Fatal("配置已更新 toast must live on the server-success path")
	}
	// Modal open/close and pure GET/load/reload must not toast success.
	for _, fn := range []string{"function openFallbackModal(", "function closeFallbackModal()", "function openPoolModal(", "function closePoolModal()", "function loadConfig()"} {
		idx := strings.Index(html, fn)
		if idx < 0 {
			continue
		}
		end := strings.Index(html[idx:], "\nfunction ")
		var block string
		if end < 0 {
			block = html[idx:]
		} else {
			block = html[idx : idx+end]
		}
		if strings.Contains(block, "配置已更新") {
			t.Fatalf("%s must not toast 配置已更新 (GET/open/close only)", fn)
		}
	}
	// Modal persistence reuses the single PUT toast: no second channel/pool success toast.
	for _, dup := range []string{"已保存渠道", "已新增渠道", "已删除渠道", "已保存代理池", "已新增代理池", "已删除代理池"} {
		if strings.Contains(html, dup) {
			t.Fatalf("modal must not emit duplicate success toast %q", dup)
		}
	}
}

func TestWebUIFallbackAutocompleteHygiene(t *testing.T) {
	html := readConfigWebUI(t)
	if !strings.Contains(html, `<form id="config-form" autocomplete="off">`) {
		t.Fatal("config-form must carry autocomplete=off to suppress credential capture")
	}
	if !strings.Contains(html, `keyInput.autocomplete="new-password"`) {
		t.Fatal("fallback key input must use autocomplete new-password, not off")
	}
	if strings.Contains(html, `keyInput.autocomplete="off"`) {
		t.Fatal("fallback key input must not use autocomplete off; new-password suppresses save")
	}
	for _, needle := range []string{
		`nameInput.setAttribute("autocomplete","off")`,
		`nameInput.setAttribute("autocapitalize","off")`,
		`nameInput.setAttribute("autocorrect","off")`,
		`nameInput.setAttribute("spellcheck","false")`,
		`baseInput.setAttribute("autocomplete","off")`,
		`baseInput.setAttribute("autocapitalize","off")`,
		`baseInput.setAttribute("autocorrect","off")`,
		`baseInput.setAttribute("spellcheck","false")`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("missing fallback non-credential hygiene %q", needle)
		}
	}
	for _, bad := range []string{
		`nameInput.setAttribute("name"`,
		`baseInput.setAttribute("name"`,
		`setAttribute("username"`,
	} {
		if strings.Contains(html, bad) {
			t.Fatalf("fallback name/base must not carry credential-ish attribute %q", bad)
		}
	}
	if strings.Contains(html, "fb-name\"].autocomplete=\"username\"") || strings.Contains(html, "fb-base\"].autocomplete=\"username\"") {
		t.Fatal("fallback name/base must not use username autocomplete")
	}
	// Login keeps credential semantics; key toggle/masked-id/model discovery stay intact.
	for _, needle := range []string{
		`autocomplete="username"`,
		`autocomplete="current-password"`,
		`keyInput.type="password"`,
		`data-role","fb-key-toggle"`,
		`data-role","fb-key-id"`,
		`fallbackInputs`,
		`fallbackDiscoverReasonCN`,
		`/api/fallback/discover`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("login/key/discovery contract must stay intact: %q", needle)
		}
	}
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
		if strings.Contains(html, sink) {
			t.Fatalf("forbidden DOM sink %q", sink)
		}
	}
}

func TestWebUICustomRealtimeDisplay(t *testing.T) {
	html := readConfigWebUI(t)
	for _, needle := range []string{
		"function isCustomRow(a)",
		"function customChannelOf(a)",
		`k.slice(0,7)==="custom:"`,
		"自定义渠道",
		`td.textContent="自定义"`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("custom realtime display must contain %q", needle)
		}
	}
	// Tier/channel labels keep the custom vocabulary without breaking aggregation keys.
	if !strings.Contains(html, `if(t==="custom")return "自定义"`) {
		t.Fatal("tierLabel must map custom to display text")
	}
	if !strings.Contains(html, `if(c==="custom")return "自定义"`) {
		t.Fatal("channelLabel must map custom to display text")
	}
	// Batch key chips UI stays untouched.
	for _, needle := range []string{"server_keys-chips", "server_keys-new", "zen_keys-chips", "zen_keys-new", "go_keys-chips", "go_keys-new", "每行新增一个密钥", "移除"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("batch key UI must stay intact: %q", needle)
		}
	}
}

func TestWebUILoginClearsPlaintextModals(t *testing.T) {
	html := readConfigWebUI(t)
	// Login-entry path must purge plaintext holders: fallback channel modal
	// and revealed secret-chip labels (global modal is gone).
	showIdx := strings.Index(html, "function showLogin()")
	if showIdx < 0 {
		t.Fatal("missing showLogin")
	}
	showEnd := strings.Index(html[showIdx:], "function switchTab(")
	if showEnd < 0 {
		t.Fatal("missing switchTab boundary for showLogin block")
	}
	showBlock := html[showIdx : showIdx+showEnd]
	if !strings.Contains(showBlock, "clearSensitiveModalsForLogin") {
		t.Fatal("showLogin must invoke the shared sensitive-modal cleanup helper")
	}
	// Helper exists and reuses closeFallbackModal with init-safe guards
	// (typeof check, no showLogin recursion).
	helperIdx := strings.Index(html, "function clearSensitiveModalsForLogin()")
	if helperIdx < 0 {
		t.Fatal("missing clearSensitiveModalsForLogin helper")
	}
	helperEnd := strings.Index(html[helperIdx:], "function showLogin()")
	if helperEnd < 0 {
		t.Fatal("missing showLogin boundary for helper block")
	}
	helper := html[helperIdx : helperIdx+helperEnd]
	for _, needle := range []string{
		`typeof closeFallbackModal`,
		`closeFallbackModal()`,
		`$("fallback-modal")`,
		`$("fallback-modal-body")`,
		`keyInput`,
		`value=""`,
		`revealed:false`,
		`originalSecret:""`,
		`resetSecretChipLabels`,
		`clearSecretRevealCache`,
	} {
		if !strings.Contains(helper, needle) {
			t.Fatalf("login cleanup helper must contain %q", needle)
		}
	}
	for _, stale := range []string{`$("modal")`, `$("modal-content")`, `$("modal-close")`, `id="modal"`} {
		if strings.Contains(helper, stale) {
			t.Fatalf("global modal reference must stay removed from helper: %q", stale)
		}
	}
	if strings.Contains(helper, "showLogin()") {
		t.Fatal("cleanup helper must not call showLogin (no recursion)")
	}
	// Helper fallback path resets fbModal state even when closeFallbackModal
	// is unavailable at init time.
	if !strings.Contains(helper, `S.fbModal={index:null,origName:"",origDisplay:"",origId:"",revealed:false,originalSecret:""}`) {
		t.Fatal("helper fallback path must reset S.fbModal plaintext state")
	}
	// closeFallbackModal itself wipes the live DOM key value before dropping
	// the body, so detached nodes keep no plaintext.
	closeIdx := strings.Index(html, "function closeFallbackModal()")
	if closeIdx < 0 {
		t.Fatal("missing closeFallbackModal")
	}
	closeEnd := strings.Index(html[closeIdx:], "function saveFallbackModal()")
	if closeEnd < 0 {
		t.Fatal("missing saveFallbackModal boundary for close block")
	}
	closeBlock := html[closeIdx : closeIdx+closeEnd]
	for _, needle := range []string{
		`fields&&st`,
		`keyInput`,
		`value=""`,
		`$("fallback-modal")`,
		`$("fallback-modal-body")`,
		`revealed:false`,
		`originalSecret:""`,
	} {
		if !strings.Contains(closeBlock, needle) {
			t.Fatalf("closeFallbackModal must contain %q", needle)
		}
	}
	// Login, reveal, eye and save behaviour stays intact.
	for _, needle := range []string{
		`$("login-form")`,
		`api("/api/auth/login"`,
		`function revealConfig()`,
		`api("/api/config/reveal"`,
		`fbEyeIcon(open)`,
		`function saveFallbackModal()`,
		`function makeSecretChip(`,
		`function toggleSecretChip(`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("login/reveal/eye/save contract must stay intact: %q", needle)
		}
	}
}

func TestWebUISecretChipToggle(t *testing.T) {
	html := readConfigWebUI(t)
	for _, needle := range []string{
		`function makeSecretChip(item, group)`,
		`function toggleSecretChip(labelEl, group)`,
		`function secretRevealShared()`,
		`function secretByFingerprint(candidates, wantId)`,
		`function clearSecretRevealCache()`,
		`function resetSecretChipLabels()`,
		`secret-label`,
		`setAttribute("role","button")`,
		`setAttribute("tabindex","0")`,
		`aria-pressed`,
		`data-display`,
		`data-revealed`,
		`data-group`,
		`revealConfig().then`,
		`fallbackSecretFingerprint(secret)`,
		`toast("未找到该密钥"`,
		`stopPropagation)ev.stopPropagation`,
		`chip.remove(); queueConfigSave(true)`,
		`cursor:pointer`,
		`label.className="secret-label"`,
		`S.secretReveal`,
		`S.secretReveal={seq:0,promise:null,data:null}`,
		`clearSecretRevealCache();`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("secret chip toggle must contain %q", needle)
		}
	}
	// Fingerprint matching is exact by data-id; never blind positional.
	if !strings.Contains(html, `chipEl.getAttribute("data-id")`) {
		t.Fatal("chip toggle must read data-id fingerprint from the chip")
	}
	if !strings.Contains(html, `fp===wantId?secret:""`) {
		t.Fatal("chip reveal must match by SHA-256 fingerprint equality")
	}
	// Keyboard access: Enter and Space toggle.
	if !strings.Contains(html, `k==="Enter"`) || !strings.Contains(html, `k===13`) {
		t.Fatal("secret label must toggle on Enter/Space")
	}
	// Toggle never writes drafts or triggers autosave.
	toggleIdx := strings.Index(html, "function toggleSecretChip(labelEl, group)")
	if toggleIdx < 0 {
		t.Fatal("missing toggleSecretChip")
	}
	toggleEnd := strings.Index(html[toggleIdx:], "function makeSecretChip(")
	if toggleEnd < 0 {
		t.Fatal("missing makeSecretChip boundary for toggle block")
	}
	toggleBlock := html[toggleIdx : toggleIdx+toggleEnd]
	if strings.Contains(toggleBlock, "queueConfigSave") {
		t.Fatal("chip toggle must not trigger queueConfigSave")
	}
	if strings.Contains(toggleBlock, ".value=") {
		t.Fatal("chip toggle must not write textarea/input values")
	}
	// Plaintext stays in memory/DOM only.
	for _, bad := range []string{`toast(found`, `console.log(found`, `localStorage`} {
		if strings.Contains(toggleBlock, bad) {
			t.Fatalf("chip plaintext must never enter %q", bad)
		}
	}
	// fillConfig/loadConfig/reconcile clear the shared reveal cache.
	for _, fn := range []string{"function loadConfig()", "function fillConfig(v)", "function reconcileAfterSuccess(sentUpdate, serverConfig)"} {
		idx := strings.Index(html, fn)
		if idx < 0 {
			t.Fatalf("missing %s", fn)
		}
		end := strings.Index(html[idx:], "function ")
		var block string
		if end < 0 {
			block = html[idx:]
		} else {
			next := strings.Index(html[idx+len(fn):], "function ")
			if next < 0 {
				block = html[idx:]
			} else {
				block = html[idx : idx+len(fn)+next]
			}
		}
		if !strings.Contains(block, "clearSecretRevealCache") {
			t.Fatalf("%s must clear the shared reveal cache", fn)
		}
	}
	// Promoted chips reuse the same clickable builder.
	if !strings.Contains(html, "host.appendChild(makeSecretChip(item,name))") {
		t.Fatal("renderChips/appendPromotedChips must share makeSecretChip")
	}
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
		if strings.Contains(html, sink) {
			t.Fatalf("forbidden DOM sink %q", sink)
		}
	}
}
