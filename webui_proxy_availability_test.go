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
		"function proxyZenLaneOn(p)",
		"function proxyGoLaneOn(p)",
		"function proxyZenDisplay(p)",
		"function proxyGoDisplay(p)",
		"function proxyZenDashTitle(p)",
		"function proxyGoDashTitle(p)",
	} {
		if !strings.Contains(html, fn) {
			t.Fatalf("missing projection helper %q", fn)
		}
	}
	// Zen lane: routed by anonymous or zen, then anonymous flag or zen_keys>0.
	// Must use structure fields, never the Chinese routing display helper.
	zenIdx := strings.Index(html, "function proxyZenLaneOn(p)")
	if zenIdx < 0 {
		t.Fatal("missing proxyZenLaneOn")
	}
	zenEnd := strings.Index(html[zenIdx:], "function proxyGoLaneOn(p)")
	if zenEnd < 0 {
		t.Fatal("missing proxyGoLaneOn boundary")
	}
	zenBlock := html[zenIdx : zenIdx+zenEnd]
	for _, needle := range []string{
		`proxyRoutingHas(p,"anonymous")`,
		`proxyRoutingHas(p,"zen")`,
		"p.anonymous",
		"zen_keys",
		"zk>0",
		"anonActive||zk>0",
	} {
		if !strings.Contains(zenBlock, needle) {
			t.Fatalf("zen lane must contain %q", needle)
		}
	}
	if strings.Contains(zenBlock, "routingRefsLabel") || strings.Contains(zenBlock, "routingRefLabel") {
		t.Fatal("zen lane must not depend on Chinese routing display helpers")
	}
	// Go lane: only go routing + go_keys>0; anonymous must never count as Go.
	goIdx := strings.Index(html, "function proxyGoLaneOn(p)")
	if goIdx < 0 {
		t.Fatal("missing proxyGoLaneOn")
	}
	goEnd := strings.Index(html[goIdx:], "function proxyZenDashTitle(p)")
	if goEnd < 0 {
		t.Fatal("missing proxyZenDashTitle boundary")
	}
	goBlock := html[goIdx : goIdx+goEnd]
	for _, needle := range []string{
		"go_keys",
		`proxyRoutingHas(p,"go")`,
		"gk>0",
	} {
		if !strings.Contains(goBlock, needle) {
			t.Fatalf("go lane must contain %q", needle)
		}
	}
	if strings.Contains(goBlock, "p.anonymous") || strings.Contains(goBlock, "p.Anonymous") {
		t.Fatal("go lane must never count anonymous as Go config")
	}
	if strings.Contains(goBlock, "routingRefsLabel") || strings.Contains(goBlock, "routingRefLabel") {
		t.Fatal("go lane must not depend on Chinese routing display helpers")
	}
	// Display projections: unhealthy short-circuits to dash before any pill.
	for _, needle := range []string{
		"function proxyZenDisplay(p)",
		"function proxyGoDisplay(p)",
		"p.healthy===false",
		`return "—"`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("display projection must contain %q", needle)
		}
	}
	// Dash titles distinguish transport / unrouted-pool / unconfigured-channel.
	for _, needle := range []string{
		"传输不可用",
		"该池未用于该通道",
		"未配置该通道",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("dash title must contain %q", needle)
		}
	}
}

func TestWebUIProxyUnhealthyDoubleDash(t *testing.T) {
	html := proxyAvailHTML(t)
	// Render path: both Zen and Go cells check healthy===false first and emit plain dash.
	if got := strings.Count(html, "p.healthy===false||!proxyZenLaneOn(p)"); got < 1 {
		t.Fatalf("zen cell must gate on p.healthy===false||!proxyZenLaneOn(p)")
	}
	if got := strings.Count(html, "p.healthy===false||!proxyGoLaneOn(p)"); got < 1 {
		t.Fatalf("go cell must gate on p.healthy===false||!proxyGoLaneOn(p)")
	}
	for _, needle := range []string{
		"td.title=proxyZenDashTitle(p)",
		"td.title=proxyGoDashTitle(p)",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("dash cell must carry title %q", needle)
		}
	}
	// Cooldown reason column is preserved.
	if !strings.Contains(html, "p.cooldown_reason") {
		t.Fatal("cooldown reason column must be preserved")
	}
	// Transport column still shows the pill; Zen/Go unhealthy must not render a transport pill.
	if !strings.Contains(html, `p.healthy?"正常":"不可用"`) {
		t.Fatal("transport column must keep 正常/不可用 pill")
	}
}

func TestWebUIProxySingleProbeButton(t *testing.T) {
	html := proxyAvailHTML(t)
	// Button structure: fixed-width ghost button with label + fixed spinner spans.
	for _, needle := range []string{
		`probeBtn.className="ghost probe-btn"`,
		`probeLabel.className="probe-label"`,
		`probeLabel.textContent="检测"`,
		`probeSpin.className="probe-spin"`,
		`.probe-btn{min-width:`,
		`.probe-btn .probe-spin{`,
		`.probe-btn.is-busy .probe-spin{`,
		"@keyframes probe-spin",
		"prefers-reduced-motion",
		`btn.classList.add("is-busy")`,
		`btn.classList.remove("is-busy")`,
		`setAttribute("aria-busy","true")`,
		`removeAttribute("aria-busy")`,
		`setAttribute("aria-label","正在检测")`,
		`setAttribute("aria-label","检测单节点传输")`,
		`btn.disabled=true`,
		`btn.disabled=false`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("single probe button must contain %q", needle)
		}
	}
	// Spinner reserves width when idle so the column never shifts.
	if !strings.Contains(html, "visibility:hidden") || !strings.Contains(html, "visibility:visible") {
		t.Fatal("spinner must reserve fixed width via visibility hidden/visible")
	}
	// Visible copy stays 检测; no wide busy copy in the single-probe path.
	probeIdx := strings.Index(html, "function probeProxy(p, btn)")
	if probeIdx < 0 {
		t.Fatal("missing probeProxy")
	}
	probeEnd := strings.Index(html[probeIdx:], "function bulkCheck()")
	if probeEnd < 0 {
		t.Fatal("missing bulkCheck boundary")
	}
	probeBlock := html[probeIdx : probeIdx+probeEnd]
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
	// Batch action stays untouched.
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
