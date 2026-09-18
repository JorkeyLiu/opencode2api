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
	// Undetected/no-data (untested/inconclusive/empty) renders as "—", never
	// "未检测"/"未定"; unconfigured/no_model keep operator-facing copy.
	for _, needle := range []string{
		`if(v==="unconfigured")return "未配置"`,
		`if(v==="no_model")return "无可用模型"`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("observation label must contain %q", needle)
		}
	}
	for _, stale := range []string{
		`if(v==="untested")return "未检测"`,
		`if(v==="inconclusive")return "未定"`,
	} {
		if strings.Contains(html, stale) {
			t.Fatalf("undetected must render as dash, stale label must stay removed: %q", stale)
		}
	}
	if !strings.Contains(html, `v==="untested"||v==="inconclusive")return "—"`) && !strings.Contains(html, `v==="untested"||v==="inconclusive"`) {
		t.Fatal("undetected/inconclusive must map to dash")
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
	for _, needle := range []string{`<th class="avail-c">传输</th>`, `<th class="avail-c">匿名</th>`, `<th class="avail-c">认证</th>`} {
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
	// Transport column uses the independent probe observation, never the
	// internal healthy default. Undetected renders as dash.
	for _, needle := range []string{
		"function transportObsOf(p)",
		"function transportDisplayLabel(v)",
		"function transportTone(v)",
		"transportObsOf(p)",
		"尚未检测",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("transport observation column must contain %q", needle)
		}
	}
	if strings.Contains(html, `p.healthy?"正常":"不可用"`) {
		t.Fatal("transport column must not use internal healthy default")
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
	// Credential toasts carry HTTP status + reason; custom toasts carry HTTP.
	for _, needle := range []string{
		`customStatusLabel(String(row.status`,
		`credStatusLabel(String(cred.status`,
		"nodeResultText(node)",
		"批量检测完成：共检测 ",
		`HTTP "+code`,
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

func TestWebUIAvailabilityTableAlignment(t *testing.T) {
	html := proxyAvailHTML(t)
	// Reusable scoped alignment classes, centered via the cell (pill stays inline-block).
	for _, needle := range []string{
		`table.table th.avail-c,table.table td.avail-c{text-align:center}`,
		`table.table th.avail-l,table.table td.avail-l{text-align:left}`,
		`table.table th.avail-ts,table.table td.avail-ts{text-align:left;white-space:nowrap}`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("alignment CSS must contain %q", needle)
		}
	}
	// Proxy header alignment: text left, sequence/status/action centered, timestamp left+nowrap.
	for _, needle := range []string{
		`<th class="avail-l">节点</th>`,
		`<th class="avail-l">代理池</th>`,
		`<th class="avail-c">序号</th>`,
		`<th class="avail-l">路由通道</th>`,
		`<th class="avail-c">传输</th>`,
		`<th class="avail-c">匿名</th>`,
		`<th class="avail-c">认证</th>`,
		`<th class="avail-ts">上次检测</th>`,
		`<th class="avail-c">操作</th>`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("proxy header must contain %q", needle)
		}
	}
	// Custom header alignment mirrors the same reusable classes.
	for _, needle := range []string{
		`<th class="avail-l">渠道</th>`,
		`<th class="avail-l">地址</th>`,
		`<th class="avail-l">模型</th>`,
		`<th class="avail-c">状态</th>`,
		`<th class="avail-l">原因</th>`,
		`<th class="avail-ts">上次检测</th>`,
		`<th class="avail-c">操作</th>`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("custom header must contain %q", needle)
		}
	}
	// Proxy cells: th/td class parity.
	for _, needle := range []string{
		`td.className="wrap avail-l"`,
		`cell(tr,p.proxy_pool||p.pool||"—","avail-l")`,
		`cell(tr,p.index,"avail-c")`,
		`var op=document.createElement("td"); op.className="avail-c";`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("proxy cell must contain %q", needle)
		}
	}
	if got := strings.Count(html, `td.className="avail-c"`); got < 3 {
		t.Fatalf("proxy status/action cells must use avail-c at least 3 times, got %d", got)
	}
	// Custom cells: th/td class parity.
	for _, needle := range []string{
		`td.className="avail-l"`,
		`td.className="wrap avail-l"`,
		`var cop=document.createElement("td"); cop.className="avail-c";`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("custom cell must contain %q", needle)
		}
	}
	proxyBlock := sliceFn(html, `var tp=$("tbody-proxies")`, `emptyRow(tp,9,`)
	customBlock := sliceFn(html, `var tcb=$("tbody-custom")`, `emptyRow(tcb,7,`)
	if proxyBlock == "" {
		t.Fatal("missing proxy render block")
	}
	if customBlock == "" {
		t.Fatal("missing custom render block")
	}
	// Centered sequence/status/action parity inside each render block.
	for _, needle := range []string{`avail-c`, `avail-ts`} {
		if !strings.Contains(proxyBlock, needle) {
			t.Fatalf("proxy block must contain %q", needle)
		}
		if !strings.Contains(customBlock, needle) {
			t.Fatalf("custom block must contain %q", needle)
		}
	}
	// Left/nowrap timestamp parity with matching tooltip semantics in both tables.
	// Single-row and batch checks share the same per-row timestamp; undetected
	// renders as dash with 尚未检测.
	for _, block := range []string{proxyBlock, customBlock} {
		if !strings.Contains(block, `td.className="avail-ts"`) {
			t.Fatal("timestamp cell must use avail-ts")
		}
		if !strings.Contains(block, `td.title=c.last_checked?("上次检测："+fmtDateTime(c.last_checked)):"尚未检测"`) &&
			!strings.Contains(block, `td.title=p.last_checked?("上次检测："+fmtDateTime(p.last_checked)):"尚未检测"`) {
			t.Fatal("timestamp cell must keep 上次检测/尚未检测 tooltip semantics")
		}
	}
	if !strings.Contains(customBlock, `td.title=c.last_checked?("上次检测："+fmtDateTime(c.last_checked)):"尚未检测"`) {
		t.Fatal("custom timestamp must carry title parity without changing timestamp semantics")
	}
	// Text columns stay left and keep wrapping for long values.
	for _, needle := range []string{`td.className="wrap avail-l"`} {
		if !strings.Contains(proxyBlock, needle) {
			t.Fatalf("proxy text cell must contain %q", needle)
		}
		if !strings.Contains(customBlock, needle) {
			t.Fatalf("custom text cell must contain %q", needle)
		}
	}
	// Empty-row colspans unchanged.
	for _, needle := range []string{`emptyRow(tp,9,"暂无代理")`, `emptyRow(tcb,7,"暂无自定义渠道")`} {
		if !strings.Contains(html, needle) {
			t.Fatalf("empty row must stay %q", needle)
		}
	}
	// No margin/inline-offset hacks in these render blocks.
	for _, hack := range []string{`margin`, `translate`, `.style.`, `paddingLeft`, `padding-left`, `textAlign`, `style="text-align`, `px`} {
		if strings.Contains(proxyBlock, hack) {
			t.Fatalf("proxy render block must not use offset hack %q", hack)
		}
		if strings.Contains(customBlock, hack) {
			t.Fatalf("custom render block must not use offset hack %q", hack)
		}
	}
	// Preserved layout/scrolling behavior and bulk action alignment.
	for _, needle := range []string{
		`table-wrap scroll-bound`,
		`position:sticky`,
		`min-width:760px`,
		`btn-bulk-check`,
		`批量检测`,
		`justify-content:flex-end`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("preserved layout must contain %q", needle)
		}
	}
	// Probe button dimensions/spinner/interaction unchanged.
	for _, needle := range []string{
		`.probe-btn{min-width:`,
		`probeBtn.className="ghost probe-btn"`,
		`cbtn.className="ghost probe-btn"`,
		`btn.classList.add("is-busy")`,
		`btn.classList.remove("is-busy")`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("probe button contract must contain %q", needle)
		}
	}
}
