package main

import (
	"strings"
	"testing"
)

func proxyAvailHTML(t *testing.T) string {
	t.Helper()
	return readConfigWebUI(t)
}

func sliceFn(html, start, end string) string {
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

func TestWebUIProxyLaneProjection(t *testing.T) {
	html := proxyAvailHTML(t)
	for _, fn := range []string{
		"function proxyRoutingHas(p,name)",
		"function proxyAnonLaneOn(p)",
		"function proxyAuthLaneOn(p)",
		"function proxyAnonDashTitle(p)",
		"function proxyAuthDashTitle(p)",
		"function obsLabel(v)",
		"function laneObsOf(p,lane)",
		"function obsTone(obs,code)",
		"function obsDisplayText(obs,code)",
	} {
		if !strings.Contains(html, fn) {
			t.Fatalf("missing projection helper %q", fn)
		}
	}
	// Canonical lanes: anonymous + authenticated routing only.
	for _, needle := range []string{
		`proxyRoutingHas(p,"anonymous")`,
		`proxyRoutingHas(p,"authenticated")`,
		"anonymous_observation",
		"authenticated_observation",
		"anonymous_http_status",
		"authenticated_http_status",
		"auth_keys",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("canonical lane must contain %q", needle)
		}
	}
	// Old Zen/Go split must be gone from lane logic.
	for _, stale := range []string{
		"function proxyZenLaneOn(p)",
		"function proxyGoLaneOn(p)",
		"function proxyZenDisplay(p)",
		"function proxyGoDisplay(p)",
		"function proxyZenDashTitle(p)",
		"function proxyGoDashTitle(p)",
		"function channelStatusLabel(",
		"function bulkNodeAvailable(",
		"zen_keys",
		"go_keys",
		`proxyRoutingHas(p,"zen")`,
		`proxyRoutingHas(p,"go")`,
	} {
		if strings.Contains(html, stale) {
			t.Fatalf("legacy lane helper must stay removed: %q", stale)
		}
	}
	// Actual-status mapping: HTTP code drives display and color.
	for _, needle := range []string{
		`if(code===200)return "ok"`,
		`if(code===429)return "warn"`,
		`if(code)return String(code)`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("actual-status mapping must contain %q", needle)
		}
	}
	// Localized observation labels, never raw backend enums in cells.
	for _, needle := range []string{
		`if(v==="unconfigured")return "未配置"`,
		`if(v==="no_model")return "无可用模型"`,
		`if(v==="untested")return "未检测"`,
		`if(v==="inconclusive")return "未定"`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("observation label must contain %q", needle)
		}
	}
	// Dash titles distinguish unrouted-pool / unconfigured-channel.
	for _, needle := range []string{
		"该池未用于该通道",
		"未配置该通道",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("dash title must contain %q", needle)
		}
	}
}

func TestWebUIProxyActualStatusCells(t *testing.T) {
	html := proxyAvailHTML(t)
	// Header carries the canonical three columns.
	for _, needle := range []string{"<th>传输</th>", "<th>匿名</th>", "<th>认证</th>"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("proxy table must contain canonical column %q", needle)
		}
	}
	for _, stale := range []string{"<th>Zen 通道</th>", "<th>Go 通道</th>", "Go 通道", "Zen 通道", "冷却原因", "p.cooldown_reason"} {
		if strings.Contains(html, stale) {
			t.Fatalf("legacy proxy column must stay removed: %q", stale)
		}
	}
	// Cells gate on the canonical lane, not on transport health.
	if got := strings.Count(html, "!proxyAnonLaneOn(p)"); got < 1 {
		t.Fatal("anonymous cell must gate on !proxyAnonLaneOn(p)")
	}
	if got := strings.Count(html, "!proxyAuthLaneOn(p)"); got < 1 {
		t.Fatal("authenticated cell must gate on !proxyAuthLaneOn(p)")
	}
	for _, needle := range []string{
		"td.title=proxyAnonDashTitle(p)",
		"td.title=proxyAuthDashTitle(p)",
		"laneObsOf(p,\"anonymous\")",
		"laneObsOf(p,\"authenticated\")",
		"obsTone(lane.obs,lane.code)",
		"obsDisplayText(lane.obs,lane.code)",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("actual-status cell must contain %q", needle)
		}
	}
	// Transport column still shows the pill.
	if !strings.Contains(html, `p.healthy?"正常":"不可用"`) {
		t.Fatal("transport column must keep 正常/不可用 pill")
	}
	// Auth no-key state is visibly 未配置, never a misleading usable state.
	if !strings.Contains(html, "未配置") {
		t.Fatal("auth no-key state must visibly say 未配置")
	}
	if !strings.Contains(html, "未配置：暂无认证密钥") {
		t.Fatal("empty credential table must say 未配置：暂无认证密钥")
	}
	// Cooldown diagnostics stay as separate tables with simplified captions.
	for _, needle := range []string{"tbody-ratelimit", "tbody-channel", "tbody-targets", "限流中的代理", "通道冷却中的节点", "冷却中的目标"} {
		if !strings.Contains(html, needle) {
			t.Fatalf("cooldown diagnostic must contain %q", needle)
		}
	}
}

func TestWebUICustomAvailabilityProjection(t *testing.T) {
	html := proxyAvailHTML(t)
	// Configured rows always exist before probing; observations overlay.
	for _, needle := range []string{
		"function mergeCustomRows(server, cached)",
		"function customStatusLabel(v)",
		"function customReasonLabel(v)",
		"keyWrap.appendChild(keyRow)",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("custom projection must contain %q", needle)
		}
	}
	// Localized status mapping, never raw backend enum strings in cells.
	for _, needle := range []string{
		`customStatusLabel(c.status||"untested")`,
		`customReasonLabel(c.reason||"—")`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("custom cell must map through localized labels: %q", needle)
		}
	}
	customIdx := strings.Index(html, "tbody-custom")
	if customIdx < 0 {
		t.Fatal("missing tbody-custom")
	}
	customBlock := html[customIdx : customIdx+3000]
	if strings.Contains(customBlock, `el("span",v,"pill`) && strings.Contains(customBlock, `td.title="状态："+v`) {
		t.Fatal("custom status cell must not print raw backend enum strings")
	}
	// Toasts report localized status, never raw enums or generic available counts.
	for _, needle := range []string{
		`customStatusLabel(String(row.status||"untested"))`,
		`credStatusLabel(String(cred.status||"untested"))`,
		"nodeResultText(node)",
		"批量检测完成：共检测 ",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("detection toast must report actual observation %q", needle)
		}
	}
	if strings.Contains(html, "bulkNodeAvailable") {
		t.Fatal("generic available counting must stay removed from detection toasts")
	}
	if strings.Contains(html, "可用 \"+avail") || strings.Contains(html, `可用 "+avail`) {
		t.Fatal("bulk toast must not count generic available")
	}
}

func TestWebUIProxySingleProbeButton(t *testing.T) {
	html := proxyAvailHTML(t)
	// Button structure: centered ghost button with label + spinner spans.
	// Idle shows centered 检测; busy hides the label and shows only a centered
	// spinner without reserving text/spinner space; completion restores 检测.
	for _, needle := range []string{
		`probeBtn.className="ghost probe-btn"`,
		`probeLabel.className="probe-label"`,
		`probeLabel.textContent="检测"`,
		`probeSpin.className="probe-spin"`,
		`.probe-btn{min-width:`,
		`.probe-btn .probe-spin{`,
		`.probe-btn.is-busy .probe-spin{`,
		`.probe-btn.is-busy .probe-label{display:none}`,
		"@keyframes probe-spin",
		"prefers-reduced-motion",
		`btn.classList.add("is-busy")`,
		`btn.classList.remove("is-busy")`,
		`setAttribute("aria-busy","true")`,
		`removeAttribute("aria-busy")`,
		`setAttribute("aria-label","正在检测")`,
		`setAttribute("aria-label","检测节点可用性")`,
		`btn.disabled=true`,
		`btn.disabled=false`,
		`justify-content:center`,
		`display:none`,
		`/api/availability/check-node`,
		`function nodeResultText(`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("single probe button must contain %q", needle)
		}
	}
	// Spinner must not reserve space when idle; busy shows only the spinner.
	if strings.Contains(html, "visibility:hidden") || strings.Contains(html, "visibility:visible") {
		t.Fatal("spinner must not reserve space via visibility hidden/visible")
	}
	if !strings.Contains(html, ".probe-btn .probe-spin{display:none") {
		t.Fatal("idle spinner must use display:none without reserving space")
	}
	// Single-check path must use the scoped availability endpoint, never the
	// transport-only probe contract (which stays preserved server-side).
	probeIdx := strings.Index(html, "function probeProxy(p, btn)")
	if probeIdx < 0 {
		t.Fatal("missing probeProxy")
	}
	probeEnd := strings.Index(html[probeIdx:], "function bulkCheck()")
	if probeEnd < 0 {
		t.Fatal("missing bulkCheck boundary")
	}
	probeBlock := html[probeIdx : probeIdx+probeEnd]
	if strings.Contains(probeBlock, "/api/proxies/probe") {
		t.Fatal("availability table 检测 button must not use the transport-only probe endpoint")
	}
	if strings.Contains(probeBlock, "探测") {
		t.Fatal("single probe path must not contain 探测 copy")
	}
	for _, wide := range []string{"探测中", "检测中"} {
		if strings.Contains(probeBlock, wide) {
			t.Fatalf("single probe path must not use wide busy copy %q", wide)
		}
	}
	// Button creation must not use the old single-text assignment.
	if strings.Contains(html, `probeBtn.textContent="探测"`) || strings.Contains(html, `probeBtn.textContent="检测"`) {
		t.Fatal("probe button must use label/spinner spans, not direct textContent assignment")
	}
	// Batch action stays.
	if !strings.Contains(html, "btn-bulk-check") || !strings.Contains(html, "批量检测") {
		t.Fatal("batch 批量检测 action must stay")
	}
	// Dynamic values stay textContent-based; no HTML sinks.
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
		if strings.Contains(html, sink) {
			t.Fatalf("forbidden DOM sink %q", sink)
		}
	}
	for _, needle := range []string{
		`td.textContent="—"`,
		`probeLabel.textContent="检测"`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("dynamic text must use textContent %q", needle)
		}
	}
}
