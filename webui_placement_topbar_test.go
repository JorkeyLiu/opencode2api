package main

import (
	"strings"
	"testing"
)

func TestWebUIHealthSectionOrderAndTopbarLabels(t *testing.T) {
	html := loadWebUIBundle(t)
	// Topbar labels exactly Active and Streaming with metric IDs/values preserved.
	if !strings.Contains(html, `<span class="stat">Active <b id="t-active">—</b></span>`) {
		t.Fatal("topbar must label t-active exactly as Active")
	}
	if !strings.Contains(html, `<span class="stat">Streaming <b id="t-streams">—</b></span>`) {
		t.Fatal("topbar must label t-streams exactly as Streaming")
	}
	if strings.Contains(html, "活跃请求 <b") || strings.Contains(html, "活跃流 <b") {
		t.Fatal("old Chinese topbar labels must be removed")
	}
	// Metric wiring preserved.
	for _, needle := range []string{`$("t-active")`, `$("t-streams")`, `m.active_requests`, `m.active_streams`} {
		if !strings.Contains(html, needle) {
			t.Fatalf("topbar metric wiring must be preserved: %q", needle)
		}
	}
	// Health order: 代理可用性 < 备用模型渠道可用性 < 凭证可用性.
	proxyIdx := strings.Index(html, "<h2>代理可用性</h2>")
	fallbackIdx := strings.Index(html, "<h2>备用模型渠道可用性</h2>")
	credIdx := strings.Index(html, "<h2>凭证可用性</h2>")
	if proxyIdx < 0 || fallbackIdx < 0 || credIdx < 0 {
		t.Fatal("health sections 代理可用性/备用模型渠道可用性/凭证可用性 must all exist")
	}
	if !(proxyIdx < fallbackIdx && fallbackIdx < credIdx) {
		t.Fatalf("order must be 代理可用性 < 备用模型渠道可用性 < 凭证可用性, got %d/%d/%d", proxyIdx, fallbackIdx, credIdx)
	}
	// Fallback section intact: single instance with its table/note IDs.
	if strings.Count(html, "<h2>备用模型渠道可用性</h2>") != 1 {
		t.Fatal("fallback section must appear exactly once")
	}
	for _, needle := range []string{`id="tbody-custom"`, `id="custom-note"`} {
		if !strings.Contains(html, needle) {
			t.Fatalf("fallback section must keep %q", needle)
		}
	}
}

func TestWebUIUsageCompletenessRenderContract(t *testing.T) {
	html := loadWebUIBundle(t)
	// Helpers distinguish unknown (—) from legitimate zero.
	for _, needle := range []string{
		`function usageReasoningText(r){ return (r.reasoning_complete===false)?"—":fmtToken(r.reasoning_tokens); }`,
		`function usageTotalText(r){ return (r.total_complete===false)?"—":fmtToken(r.total_tokens); }`,
		`cell(tr,fmtToken(r.cached_tokens)); cell(tr,usageReasoningText(r)); cell(tr,usageTotalText(r));`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("usage completeness render contract must contain %q", needle)
		}
	}
	// Both range tables (model + upstream) use the helpers.
	if strings.Count(html, `cell(tr,usageReasoningText(r)); cell(tr,usageTotalText(r));`) != 2 {
		t.Fatal("both model and upstream tables must use completeness helpers")
	}
	// Missing flags (old cached payloads) stay numeric via ===false check.
	if !strings.Contains(html, "reasoning_complete===false") || !strings.Contains(html, "total_complete===false") {
		t.Fatal("completeness check must use strict ===false for backward compatibility")
	}
}
