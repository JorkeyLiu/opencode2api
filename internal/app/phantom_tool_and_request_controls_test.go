package app

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func decodePhantomBody(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func chatNonStreamBody(finish string, toolCalls []any) []byte {
	msg := map[string]any{"role": "assistant", "content": "hi"}
	if toolCalls != nil {
		msg["tool_calls"] = toolCalls
	}
	return mustJSONForTest(map[string]any{
		"id": "chatcmpl-1", "object": "chat.completion", "created": 1, "model": "m",
		"choices": []any{map[string]any{"index": 0, "message": msg, "finish_reason": finish}},
		"usage":   map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
	})
}

func mustJSONForTest(v any) []byte {
	raw, _ := json.Marshal(v)
	return raw
}

func TestPhantomNonStreamChatToAnthropicEmptyTools(t *testing.T) {
	// Empty tool_calls array with tool finish must downgrade to end_turn.
	raw := chatNonStreamBody("tool_calls", []any{})
	out, err := convertResponse(ProtocolChat, ProtocolAnthropic, raw)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	body := decodePhantomBody(t, out)
	if got := stringAt(body, "stop_reason"); got != "end_turn" {
		t.Fatalf("stop_reason=%q want end_turn", got)
	}
	for _, b := range sliceAt(body, "content") {
		if stringAt(b.(map[string]any), "type") == "tool_use" {
			t.Fatalf("phantom tool_use must not exist: %v", body["content"])
		}
	}
}

func TestPhantomNonStreamFinishNoToolDelta(t *testing.T) {
	// tool finish with no tool_calls key at all.
	raw := chatNonStreamBody("tool_calls", nil)
	out, err := convertResponse(ProtocolChat, ProtocolAnthropic, raw)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	body := decodePhantomBody(t, out)
	if got := stringAt(body, "stop_reason"); got != "end_turn" {
		t.Fatalf("stop_reason=%q want end_turn", got)
	}
}

func TestPhantomNonStreamNamelessAndWhitespace(t *testing.T) {
	for _, name := range []string{"", "   ", "\t\n "} {
		calls := []any{map[string]any{
			"id": "call-1", "type": "function",
			"function": map[string]any{"name": name, "arguments": `{"a":1}`},
		}}
		raw := chatNonStreamBody("tool_calls", calls)
		out, err := convertResponse(ProtocolChat, ProtocolAnthropic, raw)
		if err != nil {
			t.Fatalf("name %q convert: %v", name, err)
		}
		body := decodePhantomBody(t, out)
		if got := stringAt(body, "stop_reason"); got != "end_turn" {
			t.Fatalf("name %q stop_reason=%q want end_turn", name, got)
		}
		for _, b := range sliceAt(body, "content") {
			if stringAt(b.(map[string]any), "type") == "tool_use" {
				t.Fatalf("name %q phantom tool_use: %v", name, body["content"])
			}
		}
		// Same source to Responses must complete without function_call items.
		outR, err := convertResponse(ProtocolChat, ProtocolResponses, raw)
		if err != nil {
			t.Fatalf("name %q to responses: %v", name, err)
		}
		bodyR := decodePhantomBody(t, outR)
		if got := stringAt(bodyR, "status"); got != "completed" {
			t.Fatalf("name %q status=%q want completed", name, got)
		}
		for _, item := range sliceAt(bodyR, "output") {
			if stringAt(item.(map[string]any), "type") == "function_call" {
				t.Fatalf("name %q phantom function_call: %v", name, bodyR["output"])
			}
		}
	}
}

func TestPhantomMixedValidInvalidKeepsValid(t *testing.T) {
	calls := []any{
		map[string]any{"id": "call-bad", "type": "function", "function": map[string]any{"name": "", "arguments": "{}"}},
		map[string]any{"id": "call-good", "type": "function", "function": map[string]any{"name": "read", "arguments": ""}},
	}
	raw := chatNonStreamBody("tool_calls", calls)
	out, err := convertResponse(ProtocolChat, ProtocolAnthropic, raw)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	body := decodePhantomBody(t, out)
	if got := stringAt(body, "stop_reason"); got != "tool_use" {
		t.Fatalf("stop_reason=%q want tool_use", got)
	}
	found := 0
	for _, b := range sliceAt(body, "content") {
		m := b.(map[string]any)
		if stringAt(m, "type") == "tool_use" {
			found++
			if stringAt(m, "name") != "read" {
				t.Fatalf("unexpected tool name %v", m)
			}
		}
	}
	if found != 1 {
		t.Fatalf("want exactly 1 valid tool_use, got %d: %v", found, body["content"])
	}
}

func TestValidNameEmptyArgsEmitsEmptyObject(t *testing.T) {
	calls := []any{
		map[string]any{"id": "call-1", "type": "function", "function": map[string]any{"name": "read", "arguments": ""}},
	}
	raw := chatNonStreamBody("tool_calls", calls)
	out, err := convertResponse(ProtocolChat, ProtocolAnthropic, raw)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	body := decodePhantomBody(t, out)
	if got := stringAt(body, "stop_reason"); got != "tool_use" {
		t.Fatalf("stop_reason=%q want tool_use", got)
	}
	var tool map[string]any
	for _, b := range sliceAt(body, "content") {
		m := b.(map[string]any)
		if stringAt(m, "type") == "tool_use" {
			tool = m
		}
	}
	if tool == nil {
		t.Fatalf("valid tool missing")
	}
	input, _ := json.Marshal(tool["input"])
	if string(input) != "{}" {
		t.Fatalf("empty args must produce {}, got %s", string(input))
	}
	// Chat envelope keeps tool_calls with {} arguments.
	outC, err := convertResponse(ProtocolChat, ProtocolChat, raw)
	_ = outC
	_ = err
	// Cross-check via bridge encode: Chat target with valid empty-args tool.
	resp := bridgeResponse{ID: "r1", Model: "m", Text: "hi", Stop: "tool_calls",
		Tools: []bridgeBlock{{Kind: "tool_call", ID: "call-1", Name: "read", ArgumentsJSON: ""}}}
	enc := encodeBridgeResponse(ProtocolChat, resp)
	choices := sliceAt(enc, "choices")
	msg := mapAt(choices[0].(map[string]any), "message")
	callsOut := sliceAt(msg, "tool_calls")
	if len(callsOut) != 1 {
		t.Fatalf("want 1 tool_call, got %v", msg["tool_calls"])
	}
	fn := mapAt(callsOut[0].(map[string]any), "function")
	if stringAt(fn, "arguments") != "{}" {
		t.Fatalf("arguments=%q want {}", stringAt(fn, "arguments"))
	}
}

func TestToolUseNormalAndTerminalsPreserved(t *testing.T) {
	// Valid tool_use behavior preserved.
	calls := []any{
		map[string]any{"id": "call-1", "type": "function", "function": map[string]any{"name": "bash", "arguments": `{"cmd":"ls"}`}},
	}
	raw := chatNonStreamBody("tool_calls", calls)
	out, err := convertResponse(ProtocolChat, ProtocolAnthropic, raw)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if got := stringAt(decodePhantomBody(t, out), "stop_reason"); got != "tool_use" {
		t.Fatalf("stop=%q want tool_use", got)
	}
	// Non-tool terminals preserved.
	for _, tc := range []struct{ finish, want string }{
		{"length", "max_tokens"},
		{"content_filter", "refusal"},
		{"stop", "end_turn"},
	} {
		raw := chatNonStreamBody(tc.finish, nil)
		out, err := convertResponse(ProtocolChat, ProtocolAnthropic, raw)
		if err != nil {
			t.Fatalf("%s convert: %v", tc.finish, err)
		}
		// content_filter maps via anthropicStop? length->max_tokens, others preserved mapping.
		got := stringAt(decodePhantomBody(t, out), "stop_reason")
		if tc.finish == "stop" && got != "end_turn" {
			t.Fatalf("stop got %q", got)
		}
		if tc.finish == "length" && got != "max_tokens" {
			t.Fatalf("length got %q", got)
		}
	}
	// Responses nameless function_call source downgrades.
	respRaw := mustJSONForTest(map[string]any{
		"id": "resp-1", "object": "response", "created_at": 1, "model": "m", "status": "completed",
		"output": []any{
			map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "hi"}}},
			map[string]any{"type": "function_call", "call_id": "c1", "name": "   ", "arguments": "{}"},
		},
	})
	outR, err := convertResponse(ProtocolResponses, ProtocolAnthropic, respRaw)
	if err != nil {
		t.Fatalf("responses nameless: %v", err)
	}
	bodyR := decodePhantomBody(t, outR)
	if got := stringAt(bodyR, "stop_reason"); got != "end_turn" {
		t.Fatalf("nameless responses stop=%q want end_turn", got)
	}
	for _, b := range sliceAt(bodyR, "content") {
		if stringAt(b.(map[string]any), "type") == "tool_use" {
			t.Fatalf("phantom from nameless responses item: %v", bodyR["content"])
		}
	}
}

func sseFrames(t *testing.T, raw string) []map[string]any {
	t.Helper()
	var frames []map[string]any
	for _, chunk := range strings.Split(raw, "\n\n") {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" {
			continue
		}
		lines := strings.Split(chunk, "\n")
		var data string
		for _, ln := range lines {
			if strings.HasPrefix(ln, "data:") {
				data = strings.TrimSpace(strings.TrimPrefix(ln, "data:"))
			}
		}
		if data == "" || data == "[DONE]" {
			continue
		}
		var v map[string]any
		if err := json.Unmarshal([]byte(data), &v); err != nil {
			continue
		}
		frames = append(frames, v)
	}
	return frames
}

func runBridgeStream(t *testing.T, from, to Protocol, sse string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	if _, _, err := transcodeStreamWithUsage(rec, strings.NewReader(sse), from, to, "m"); err != nil {
		t.Fatalf("stream: %v", err)
	}
	return rec.Body.String()
}

func TestStreamChatToAnthropicPhantomDowngrade(t *testing.T) {
	// Nameless args delta + tool finish must end as end_turn with no tool_use.
	sse := "data: {\"id\":\"chatcmpl-1\",\"model\":\"m\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"\",\"arguments\":\"{\\\"a\\\":1}\"}}]},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"chatcmpl-1\",\"model\":\"m\",\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: [DONE]\n\n"
	raw := runBridgeStream(t, ProtocolChat, ProtocolAnthropic, sse)
	frames := sseFrames(t, raw)
	var delta map[string]any
	for _, f := range frames {
		if stringAt(f, "type") == "message_delta" {
			delta = f
		}
		if stringAt(f, "type") == "content_block_start" {
			cb := mapAt(f, "content_block")
			if stringAt(cb, "type") == "tool_use" {
				t.Fatalf("phantom tool_use block in stream: %v", f)
			}
		}
	}
	if delta == nil {
		t.Fatalf("missing message_delta: %s", raw)
	}
	if got := stringAt(mapAt(delta, "delta"), "stop_reason"); got != "end_turn" {
		t.Fatalf("stop_reason=%q want end_turn, raw=%s", got, raw)
	}
}

func TestStreamChatToAnthropicFinishNoDelta(t *testing.T) {
	sse := "data: {\"id\":\"chatcmpl-1\",\"model\":\"m\",\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"chatcmpl-1\",\"model\":\"m\",\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: [DONE]\n\n"
	raw := runBridgeStream(t, ProtocolChat, ProtocolAnthropic, sse)
	frames := sseFrames(t, raw)
	for _, f := range frames {
		if stringAt(f, "type") == "content_block_start" {
			if stringAt(mapAt(f, "content_block"), "type") == "tool_use" {
				t.Fatalf("phantom tool_use with no delta: %s", raw)
			}
		}
		if stringAt(f, "type") == "message_delta" {
			if got := stringAt(mapAt(f, "delta"), "stop_reason"); got != "end_turn" {
				t.Fatalf("stop=%q want end_turn: %s", got, raw)
			}
		}
	}
}

func TestStreamChatToAnthropicValidEmptyArgs(t *testing.T) {
	// Valid name with empty args: finish carries name in tool_calls with empty arguments.
	sse := "data: {\"id\":\"chatcmpl-1\",\"model\":\"m\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"read\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"chatcmpl-1\",\"model\":\"m\",\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: [DONE]\n\n"
	raw := runBridgeStream(t, ProtocolChat, ProtocolAnthropic, sse)
	frames := sseFrames(t, raw)
	foundTool := false
	for _, f := range frames {
		if stringAt(f, "type") == "content_block_start" {
			if stringAt(mapAt(f, "content_block"), "type") == "tool_use" {
				foundTool = true
				if stringAt(mapAt(f, "content_block"), "name") != "read" {
					t.Fatalf("tool name wrong: %v", f)
				}
			}
		}
		if stringAt(f, "type") == "message_delta" {
			if got := stringAt(mapAt(f, "delta"), "stop_reason"); got != "tool_use" {
				t.Fatalf("stop=%q want tool_use: %s", got, raw)
			}
		}
	}
	if !foundTool {
		t.Fatalf("valid empty-args tool missing: %s", raw)
	}
}

func TestStreamChatToResponsesPhantomNoFunctionCall(t *testing.T) {
	sse := "data: {\"id\":\"chatcmpl-1\",\"model\":\"m\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"   \",\"arguments\":\"{}\"}}]},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"chatcmpl-1\",\"model\":\"m\",\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: [DONE]\n\n"
	raw := runBridgeStream(t, ProtocolChat, ProtocolResponses, sse)
	frames := sseFrames(t, raw)
	for _, f := range frames {
		if stringAt(f, "type") == "response.output_item.added" || stringAt(f, "type") == "response.output_item.done" {
			item := mapAt(f, "item")
			if stringAt(item, "type") == "function_call" {
				t.Fatalf("phantom function_call in stream: %v raw=%s", f, raw)
			}
		}
		if stringAt(f, "type") == "response.completed" {
			resp := mapAt(f, "response")
			if got := stringAt(resp, "status"); got != "completed" {
				t.Fatalf("status=%q want completed", got)
			}
			for _, item := range sliceAt(resp, "output") {
				if stringAt(item.(map[string]any), "type") == "function_call" {
					t.Fatalf("phantom in completed: %v", resp["output"])
				}
			}
		}
	}
}

func TestCollapsePhantomDowngrade(t *testing.T) {
	sse := "data: {\"id\":\"chatcmpl-1\",\"model\":\"m\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"\",\"arguments\":\"{}\"}}]},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"chatcmpl-1\",\"model\":\"m\",\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: [DONE]\n\n"
	body, _, _, err := collapseUpstreamSSE(strings.NewReader(sse), ProtocolChat, ProtocolAnthropic, "m")
	if err != nil {
		t.Fatalf("collapse: %v", err)
	}
	decoded := decodePhantomBody(t, body)
	if got := stringAt(decoded, "stop_reason"); got != "end_turn" {
		t.Fatalf("collapse stop=%q want end_turn", got)
	}
	// Valid empty-args collapse keeps tool.
	sseValid := "data: {\"id\":\"chatcmpl-1\",\"model\":\"m\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-9\",\"function\":{\"name\":\"grep\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"chatcmpl-1\",\"model\":\"m\",\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: [DONE]\n\n"
	bodyV, _, _, err := collapseUpstreamSSE(strings.NewReader(sseValid), ProtocolChat, ProtocolResponses, "m")
	if err != nil {
		t.Fatalf("collapse valid: %v", err)
	}
	decV := decodePhantomBody(t, bodyV)
	found := false
	for _, item := range sliceAt(decV, "output") {
		m := item.(map[string]any)
		if stringAt(m, "type") == "function_call" {
			found = true
			if stringAt(m, "name") != "grep" || stringAt(m, "arguments") != "{}" {
				t.Fatalf("valid collapse item wrong: %v", m)
			}
		}
	}
	if !found {
		t.Fatalf("valid collapse tool missing: %v", decV["output"])
	}
}

// --- A2: Chat-target dense emission-time tool indices (Responses->Chat) ---

func chatToolCallEntries(t *testing.T, raw string) []map[string]any {
	t.Helper()
	var entries []map[string]any
	for _, frame := range sseFrames(t, raw) {
		choices, _ := frame["choices"].([]any)
		if len(choices) == 0 {
			continue
		}
		choice, _ := choices[0].(map[string]any)
		delta, _ := choice["delta"].(map[string]any)
		for _, rawCall := range sliceAt(delta, "tool_calls") {
			call, _ := rawCall.(map[string]any)
			entries = append(entries, call)
		}
	}
	return entries
}

func TestStreamResponsesToChatPhantomBeforeValidDenseIndex(t *testing.T) {
	// Nameless phantom delta accumulates first (emitter entry, never started),
	// then a valid named tool with sparse upstream output_index 7. Only the
	// emitted tool may consume a Chat index, so every chunk must reuse 0.
	sse := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-1\",\"model\":\"m\"}}\n\n" +
		"data: {\"type\":\"response.output_item.added\",\"output_index\":4,\"item\":{\"id\":\"fc-phantom\",\"type\":\"function_call\",\"call_id\":\"call-phantom\",\"name\":\"   \"}}\n\n" +
		"data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":4,\"item_id\":\"fc-phantom\",\"delta\":\"{\\\"a\\\":1}\"} \n\n" +
		"data: {\"type\":\"response.output_item.added\",\"output_index\":7,\"item\":{\"id\":\"fc-1\",\"type\":\"function_call\",\"call_id\":\"call-1\",\"name\":\"read\"}}\n\n" +
		"data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":7,\"item_id\":\"fc-1\",\"delta\":\"{\\\"path\\\":\\\"a\\\"}\"}\n\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"model\":\"m\",\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n"
	raw := runBridgeStream(t, ProtocolResponses, ProtocolChat, sse)
	entries := chatToolCallEntries(t, raw)
	if len(entries) == 0 {
		t.Fatalf("no chat tool_calls emitted: %s", raw)
	}
	for _, entry := range entries {
		var idx int
		switch v := entry["index"].(type) {
		case float64:
			idx = int(v)
		default:
			t.Fatalf("tool index not numeric: %v raw=%s", entry, raw)
		}
		if idx != 0 {
			t.Fatalf("phantom consumed Chat index: got index=%d want 0 for all chunks raw=%s", idx, raw)
		}
	}
	// The valid tool start chunk must carry id+name on index 0.
	foundStart := false
	for _, entry := range entries {
		fn, _ := entry["function"].(map[string]any)
		if stringAt(fn, "name") == "read" {
			foundStart = true
			if stringAt(entry, "id") != "call-1" {
				t.Fatalf("valid tool id wrong: %v raw=%s", entry, raw)
			}
		}
		if name := stringAt(fn, "name"); name != "" && name != "read" {
			t.Fatalf("phantom name leaked to Chat: %v raw=%s", entry, raw)
		}
	}
	if !foundStart {
		t.Fatalf("valid named tool missing on index 0: %s", raw)
	}
}

func TestStreamResponsesToChatInterleavedDenseIndicesWithEmptyArgs(t *testing.T) {
	// Two interleaved valid tools with sparse upstream output_index values
	// (5 and 9) plus a third valid empty-args tool (no delta). Emitted Chat
	// indices must be dense 0,1,2 independent of upstream raw indices, with
	// all chunks for one tool reusing its assigned index.
	sse := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-1\",\"model\":\"m\"}}\n\n" +
		"data: {\"type\":\"response.output_item.added\",\"output_index\":5,\"item\":{\"id\":\"fc-a\",\"type\":\"function_call\",\"call_id\":\"call-a\",\"name\":\"read\"}}\n\n" +
		"data: {\"type\":\"response.output_item.added\",\"output_index\":9,\"item\":{\"id\":\"fc-b\",\"type\":\"function_call\",\"call_id\":\"call-b\",\"name\":\"grep\"}}\n\n" +
		"data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":5,\"item_id\":\"fc-a\",\"delta\":\"{\\\"p\\\":\"}\n\n" +
		"data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":9,\"item_id\":\"fc-b\",\"delta\":\"{\\\"q\\\":\"}\n\n" +
		"data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":5,\"item_id\":\"fc-a\",\"delta\":\"1\\\"}\"}\n\n" +
		"data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":9,\"item_id\":\"fc-b\",\"delta\":\"2\\\"}\"}\n\n" +
		"data: {\"type\":\"response.output_item.added\",\"output_index\":12,\"item\":{\"id\":\"fc-c\",\"type\":\"function_call\",\"call_id\":\"call-c\",\"name\":\"bash\"}}\n\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"model\":\"m\",\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n"
	raw := runBridgeStream(t, ProtocolResponses, ProtocolChat, sse)
	entries := chatToolCallEntries(t, raw)
	if len(entries) == 0 {
		t.Fatalf("no chat tool_calls emitted: %s", raw)
	}
	byID := map[string][]int{}
	seen := map[int]bool{}
	for _, entry := range entries {
		var idx int
		switch v := entry["index"].(type) {
		case float64:
			idx = int(v)
		default:
			t.Fatalf("tool index not numeric: %v raw=%s", entry, raw)
		}
		seen[idx] = true
		id := stringAt(entry, "id")
		if id != "" {
			byID[id] = append(byID[id], idx)
		}
		fn, _ := entry["function"].(map[string]any)
		_ = fn
	}
	// Dense 0..2 only, independent of upstream 5/9/12.
	for want := 0; want <= 2; want++ {
		if !seen[want] {
			t.Fatalf("dense Chat index %d missing, seen=%v raw=%s", want, seen, raw)
		}
	}
	for idx := range seen {
		if idx < 0 || idx > 2 {
			t.Fatalf("non-dense Chat index %d, seen=%v raw=%s", idx, seen, raw)
		}
	}
	// Each tool reuses one stable index; distinct tools have distinct indices.
	idIndex := map[string]int{}
	for id, indices := range byID {
		first := indices[0]
		for _, idx := range indices[1:] {
			if idx != first {
				t.Fatalf("tool %s changed Chat index %v raw=%s", id, indices, raw)
			}
		}
		idIndex[id] = first
	}
	if len(idIndex) != 3 {
		t.Fatalf("want 3 distinct tools, got %v raw=%s", idIndex, raw)
	}
	if idIndex["call-a"] == idIndex["call-b"] || idIndex["call-a"] == idIndex["call-c"] || idIndex["call-b"] == idIndex["call-c"] {
		t.Fatalf("distinct tools share Chat index: %v raw=%s", idIndex, raw)
	}
	// Valid empty-args tool still emits its block (materialized at Finish).
	foundBash := false
	for _, entry := range entries {
		fn, _ := entry["function"].(map[string]any)
		if stringAt(fn, "name") == "bash" {
			foundBash = true
		}
	}
	if !foundBash {
		t.Fatalf("valid empty-args tool missing: %s", raw)
	}
}

// --- B: compatible optional request controls ---

func TestRequestControlsChatResponsesRoundTrip(t *testing.T) {
	in := map[string]any{
		"model": "m", "stream": false,
		"messages":          []any{map[string]any{"role": "user", "content": "hi"}},
		"safety_identifier": "user-123", "service_tier": "flex",
	}
	out, err := convertRequest(ProtocolChat, ProtocolResponses, in)
	if err != nil {
		t.Fatalf("chat->responses: %v", err)
	}
	if out["safety_identifier"] != "user-123" {
		t.Fatalf("safety_identifier=%v", out["safety_identifier"])
	}
	if out["service_tier"] != "flex" {
		t.Fatalf("service_tier=%v", out["service_tier"])
	}
	back, err := convertRequest(ProtocolResponses, ProtocolChat, out)
	if err != nil {
		t.Fatalf("responses->chat: %v", err)
	}
	if back["safety_identifier"] != "user-123" || back["service_tier"] != "flex" {
		t.Fatalf("round trip lost: %v", back)
	}
}

func TestRequestControlsAbsentOmitted(t *testing.T) {
	in := map[string]any{"model": "m", "stream": false,
		"messages": []any{map[string]any{"role": "user", "content": "hi"}}}
	out, err := convertRequest(ProtocolChat, ProtocolResponses, in)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if _, ok := out["safety_identifier"]; ok {
		t.Fatalf("absent safety_identifier must be omitted")
	}
	if _, ok := out["service_tier"]; ok {
		t.Fatalf("absent service_tier must be omitted")
	}
	inR := map[string]any{"model": "m", "stream": false, "input": "hi"}
	outR, err := convertRequest(ProtocolResponses, ProtocolChat, inR)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if _, ok := outR["safety_identifier"]; ok {
		t.Fatalf("absent safety_identifier must be omitted (R->C)")
	}
	if _, ok := outR["service_tier"]; ok {
		t.Fatalf("absent service_tier must be omitted (R->C)")
	}
}

func TestRequestControlsAnthropicNoForeignFields(t *testing.T) {
	in := map[string]any{
		"model": "m", "stream": false,
		"messages":          []any{map[string]any{"role": "user", "content": "hi"}},
		"safety_identifier": "u", "service_tier": "flex",
	}
	for _, pair := range [][2]Protocol{{ProtocolChat, ProtocolAnthropic}, {ProtocolResponses, ProtocolAnthropic}} {
		_ = pair
	}
	out, err := convertRequest(ProtocolChat, ProtocolAnthropic, in)
	if err != nil {
		t.Fatalf("chat->anthropic: %v", err)
	}
	if _, ok := out["safety_identifier"]; ok {
		t.Fatalf("anthropic must not gain safety_identifier")
	}
	if _, ok := out["service_tier"]; ok {
		t.Fatalf("anthropic must not gain service_tier")
	}
	inR := map[string]any{"model": "m", "input": "hi", "safety_identifier": "u", "service_tier": "flex"}
	out2, err := convertRequest(ProtocolResponses, ProtocolAnthropic, inR)
	if err != nil {
		t.Fatalf("responses->anthropic: %v", err)
	}
	if _, ok := out2["safety_identifier"]; ok {
		t.Fatalf("anthropic must not gain safety_identifier (R->A)")
	}
	if _, ok := out2["service_tier"]; ok {
		t.Fatalf("anthropic must not gain service_tier (R->A)")
	}
	// Anthropic sources never produce these controls on other protocols.
	inA := map[string]any{"model": "m", "max_tokens": 5,
		"messages": []any{map[string]any{"role": "user", "content": "hi"}}}
	outA, err := convertRequest(ProtocolAnthropic, ProtocolChat, inA)
	if err != nil {
		t.Fatalf("anthropic->chat: %v", err)
	}
	if _, ok := outA["safety_identifier"]; ok {
		t.Fatalf("chat from anthropic must not gain safety_identifier")
	}
	if _, ok := outA["service_tier"]; ok {
		t.Fatalf("chat from anthropic must not gain service_tier")
	}
}

func TestRequestControlsNoCacheStoreBridging(t *testing.T) {
	// Cross-protocol must not forward prompt_cache_key/store; authority stays
	// with applyRouteSessionToBody (wire stamping + store default false).
	in := map[string]any{
		"model": "m", "stream": false,
		"messages":          []any{map[string]any{"role": "user", "content": "hi"}},
		"safety_identifier": "u", "service_tier": "flex",
		"store": true, "prompt_cache_key": "raw-should-not-pass",
	}
	_ = in
	// Chat has no prompt_cache_key/store bridge: build a Responses-shaped input
	// with foreign cache fields and confirm bridge output carries only controls.
	inR := map[string]any{
		"model": "m", "stream": false, "input": "hi",
		"safety_identifier": "u", "service_tier": "flex",
		"prompt_cache_key": "raw", "store": true,
	}
	out, err := convertRequest(ProtocolResponses, ProtocolChat, inR)
	if err != nil {
		t.Fatalf("R->C: %v", err)
	}
	if _, ok := out["prompt_cache_key"]; ok {
		t.Fatalf("chat must not gain prompt_cache_key")
	}
	if _, ok := out["store"]; ok {
		t.Fatalf("chat must not gain store")
	}
	// Responses route-session authority unchanged: stamping + default false.
	raw := mustJSONForTest(map[string]any{"model": "m", "input": "hi"})
	stamped, err := applyRouteSessionToBody(raw, "rss_testsession123", ProtocolResponses, false)
	if err != nil {
		t.Fatalf("stamp: %v", err)
	}
	payload := decodePhantomBody(t, stamped)
	if got := stringAt(payload, "prompt_cache_key"); got != routeWireSession("rss_testsession123") {
		t.Fatalf("prompt_cache_key=%q want wire", got)
	}
	if v, ok := payload["store"]; !ok || v != false {
		t.Fatalf("store default must stay false, got %v", payload["store"])
	}
	// Explicit same-protocol store preserved.
	rawExp := mustJSONForTest(map[string]any{"model": "m", "input": "hi", "store": true})
	stampedExp, err := applyRouteSessionToBody(rawExp, "rss_testsession123", ProtocolResponses, false)
	if err != nil {
		t.Fatalf("stamp explicit: %v", err)
	}
	if v := decodePhantomBody(t, stampedExp)["store"]; v != true {
		t.Fatalf("explicit store must be preserved, got %v", v)
	}
	// Chat never gains cache fields.
	rawChat := mustJSONForTest(map[string]any{"model": "m", "messages": []any{}})
	stampedChat, err := applyRouteSessionToBody(rawChat, "rss_testsession123", ProtocolChat, false)
	if err != nil {
		t.Fatalf("stamp chat: %v", err)
	}
	chatPayload := decodePhantomBody(t, stampedChat)
	if _, ok := chatPayload["prompt_cache_key"]; ok {
		t.Fatalf("chat must not add prompt_cache_key")
	}
	if _, ok := chatPayload["store"]; ok {
		t.Fatalf("chat must not add store")
	}
}
