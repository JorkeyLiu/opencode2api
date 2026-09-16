package main

import (
	"strings"
	"testing"
)

func TestWebUIGlobalFormatRules(t *testing.T) {
	html := readConfigWebUI(t)

	// Format helpers: fmtToken uses en-US grouping; fmtMs has no space before ms.
	if !strings.Contains(html, `function fmtToken(v){ return Number(v||0).toLocaleString("en-US"); }`) {
		t.Fatal("webui must define fmtToken with en-US grouping")
	}
	if !strings.Contains(html, `function fmtRealtimeInt(v){ return fmtToken(v); }`) {
		t.Fatal("fmtRealtimeInt must converge to fmtToken alias for compatibility")
	}
	if !strings.Contains(html, `function fmtMs(v){ if(v===undefined||v===null)return "—"; return Number(v)+"ms"; }`) {
		t.Fatal("fmtMs helper must render as Number(v)+\"ms\" without space")
	}

	// Token formatting coverage:
	// 1. Realtime table token columns
	for _, needle := range []string{
		`fmtToken(tokenReq.cache_hit_tokens||tokenReq.CacheHitTokens||0)`,
		`fmtToken(iv)+" / "+fmtToken(ov)`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("realtime token display must contain %q", needle)
		}
	}

	// 2. Usage KPI token display (input/output, cache, reasoning)
	if !strings.Contains(html, `kpi("输入 / 输出",fmtToken(it)+" / "+fmtToken(ot),"缓存 "+fmtToken(ct)+" · 推理 "+fmtToken(rt));`) {
		t.Fatal("usage KPI must format input/output/cache/reasoning tokens with fmtToken")
	}

	// 3. Usage tables (by model & by tier)
	for _, needle := range []string{
		`cell(tr,fmtToken(c.input_tokens)); cell(tr,fmtToken(c.output_tokens));`,
		`cell(tr,fmtToken(c.cached_tokens)); cell(tr,fmtToken(c.reasoning_tokens)); cell(tr,fmtToken(c.total_tokens));`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("usage table tokens must contain %q", needle)
		}
	}

	// 4. Token trend hover title
	if !strings.Contains(html, `fmtToken(p.input_tokens||0)`) ||
		!strings.Contains(html, `fmtToken(p.output_tokens||0)`) ||
		!strings.Contains(html, `fmtToken(p.cached_tokens||0)`) ||
		!strings.Contains(html, `fmtToken(p.reasoning_tokens||0)`) ||
		!strings.Contains(html, `fmtToken(p.total_tokens||0)`) {
		t.Fatal("persisted token hover title must format token values with fmtToken")
	}

	// 5. Proxy stats table tokens
	for _, needle := range []string{
		`var iv=fmtToken(p.input_tokens); var ov=fmtToken(p.output_tokens); td.textContent=iv+" / "+ov;`,
		`td.textContent=fmtToken(p.cache_hit_tokens);`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("proxy stats tokens must contain %q", needle)
		}
	}

	// Non-token counters stay with fmtInt (not mis-converted)
	for _, needle := range []string{
		`kpi("请求数",fmtInt(total),"成功 "+fmtInt(success)+" · 失败 "+fmtInt(errors));`,
		`td.textContent=fmtInt(p.attempts);`,
		`td.textContent=fmtInt(p.success);`,
		`td.textContent=fmtInt(p.failed);`,
		`cell(tr,fmtInt(p.rate_limited));`,
		`cell(tr,fmtInt(p.auth_failures));`,
		`cell(tr,fmtInt(p.upstream_failures));`,
		`cell(tr,fmtInt(p.transport_failures));`,
		`cell(tr,fmtInt(p.other_failures));`,
		`fact(fc,"元数据模型数", fmtInt(md.models));`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("non-token integer must keep fmtInt: %q", needle)
		}
	}

	// Milliseconds formatting coverage across all visible areas:
	// 1. Realtime duration
	if !strings.Contains(html, `cell(tr,(d===undefined||d===null)?"—":d+"ms");`) {
		t.Fatal("realtime duration must format as d+\"ms\"")
	}

	// 2. Upstream attempt avg duration KPI
	if !strings.Contains(html, `avg=(sum/n).toFixed(1)+"ms";`) {
		t.Fatal("KPI avg duration must format as (sum/n).toFixed(1)+\"ms\"")
	}

	// 3. Proxy stats avg duration cell
	if !strings.Contains(html, `td.textContent=(d===undefined||d===null)?"—":(Number(d).toFixed(1)+"ms");`) {
		t.Fatal("proxy stats avg duration must format as Number(d).toFixed(1)+\"ms\"")
	}

	// 4. Single proxy probe toast
	if !strings.Contains(html, `res.duration_ms+"ms）"`) {
		t.Fatal("proxy probe toast must format as res.duration_ms+\"ms）\"")
	}

	// 5. Manual refresh status note and toast
	if !strings.Contains(html, `(res.duration_ms||0)+"ms）"`) {
		t.Fatal("manual refresh status/toast must format as (res.duration_ms||0)+\"ms）\"")
	}

	// 6. Fallback model discovery error detail toast
	if !strings.Contains(html, `detail+="，耗时 "+elapsed+"ms";`) {
		t.Fatal("fallback discover error toast must format as elapsed+\"ms\"")
	}

	// Negative assertion: no visible space before ms in duration expressions
	for _, spaced := range []string{`+" ms"`, `+ " ms"`, `" ms"`, ` ms）`, ` ms"`} {
		if strings.Contains(html, spaced) {
			t.Fatalf("user-visible duration must not contain spaced ms: %q", spaced)
		}
	}

	// Machine data and API fields preserved without alteration
	for _, field := range []string{"duration_ms", "avg_duration_ms", "elapsed_ms"} {
		if !strings.Contains(html, field) {
			t.Fatalf("machine field %q must not be renamed", field)
		}
	}

	// Safety: no dangerous DOM sinks introduced
	for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
		if strings.Contains(html, sink) {
			t.Fatalf("forbidden DOM sink in index.html %q", sink)
		}
	}
}
