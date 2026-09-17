package main

import (
	"os"
	"strings"
	"testing"
)

func readRealtimeWebUI(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("webui/index.html")
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// Realtime 代理 column is display-only: native rows show the node URL alone,
// custom fallback rows show an em dash. Backend proxy_pool/proxy_node,
// scheduler identity, persistence, and split pool/node tables are untouched.
func TestWebUIRealtimeProxyDisplayOnly(t *testing.T) {
	html := readRealtimeWebUI(t)
	// Helpers exist.
	for _, needle := range []string{
		`function realtimeProxyLabel(a)`,
		`function realtimeProxyTitle(a,label)`,
		`if(isCustomRow(a))return "—"`,
		`自定义渠道不使用代理池节点`,
		`"代理节点："+label`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("realtime proxy helper missing %q", needle)
		}
	}
	// Realtime cell renders via the display-only helpers.
	if !strings.Contains(html, `var label=realtimeProxyLabel(a); td.textContent=label; td.title=realtimeProxyTitle(a,label);`) {
		t.Fatal("realtime 代理 cell must render via realtimeProxyLabel/Title")
	}
	// Realtime cell must not combine pool/node.
	cellIdx := strings.Index(html, "function attemptRowCells(tr, a, tokenReq, isFallback)")
	if cellIdx < 0 {
		t.Fatal("missing attemptRowCells")
	}
	cellEnd := strings.Index(html[cellIdx:], "function renderRealtime()")
	if cellEnd < 0 {
		t.Fatal("missing renderRealtime boundary")
	}
	cellBlock := html[cellIdx : cellIdx+cellEnd]
	if strings.Contains(cellBlock, "proxyLabel(") {
		t.Fatal("realtime cell must not use pool/node proxyLabel; node-only display required")
	}
	if strings.Contains(cellBlock, "代理池/节点：") {
		t.Fatal("realtime tooltip must not use pool/node copy")
	}
	// Filter uses native node URLs and omits custom dash rows.
	filterIdx := strings.Index(html, "function renderProxyFilter()")
	if filterIdx < 0 {
		t.Fatal("missing renderProxyFilter")
	}
	filterEnd := strings.Index(html[filterIdx:], "/* shared single-attempt row builder.")
	if filterEnd < 0 {
		t.Fatal("missing row-builder boundary")
	}
	filterBlock := html[filterIdx : filterIdx+filterEnd]
	if strings.Contains(filterBlock, "proxyLabel(") {
		t.Fatal("proxy filter must use node URLs, not pool/node proxyLabel")
	}
	for _, needle := range []string{
		`if(isCustomRow(a))return;`,
		`if(isCustomRow(r))return;`,
		`String(a.proxy_node||a.proxy||"")`,
		`String(r.proxy_node||r.proxy||"")`,
	} {
		if !strings.Contains(filterBlock, needle) {
			t.Fatalf("proxy filter must skip custom rows and list node URLs, missing %q", needle)
		}
	}
	// Filter comparison is consistent with display.
	if !strings.Contains(html, `if(px&&realtimeProxyLabel(a)!==px)return false;`) {
		t.Fatal("realtime filter comparison must use realtimeProxyLabel")
	}
	// Example native node URL must never be pool-prefixed in realtime copy.
	if strings.Contains(html, `shared/socks5://`) {
		t.Fatal("realtime display must never pool-prefix node URLs")
	}
	// Custom rows: dash text with matching no-node title.
	if !strings.Contains(cellBlock, `isCustomRow(a)`) {
		t.Fatal("realtime cell must branch on custom rows for dash display")
	}
}

func TestWebUIRealtimeProxyPreservesSplitTables(t *testing.T) {
	html := readRealtimeWebUI(t)
	// Split pool/node tables stay intact (proxy stats, targets, channels, cooldowns).
	for _, needle := range []string{
		`cell(tr,p.proxy_pool||"—")`,
		`cell(tr,k.id,"wrap")`,
		`cell(tr,t.proxy_node||"—","wrap")`,
		`cell(tr,l.proxy_pool||"—"); cell(tr,l.proxy_node||"—","wrap")`,
		`emptyRow(tb,15`,
		`tbody-proxies`,
		`tbody-keys`,
		`tbody-targets`,
		`tbody-custom`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("split pool/node table must stay intact, missing %q", needle)
		}
	}
	// Free-text search still matches both pool and node fields.
	if !strings.Contains(html, `String(a.proxy_pool||a.ProxyPool||"")`) || !strings.Contains(html, `String(a.proxy_node||a.proxy||a.Proxy||"")`) {
		t.Fatal("free-text search must keep matching both proxy_pool and proxy_node")
	}
	// Backend proxy fields still flow into request fallback rows (display-only change).
	if !strings.Contains(html, `proxy_pool:r.proxy_pool,proxy_node:(r.proxy_node||r.proxy)`) {
		t.Fatal("request fallback rows must keep backend proxy_pool/proxy_node fields")
	}
	// No unsanitized sinks introduced.
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
		if strings.Contains(html, sink) {
			t.Fatalf("forbidden DOM sink %q", sink)
		}
	}
}
