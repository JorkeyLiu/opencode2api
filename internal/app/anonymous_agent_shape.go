package app

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// Native Zen free-tier agent shaping (authorized: anonymous AND authenticated).
//
// Upstream policy evidence: canonical identity alone is insufficient for
// native Zen free models. Agent-shaped streams (stream:true plus the core
// tool set) are accepted. This file is the single shared authority for that
// shaping: normal native inference (anonymous/authenticated, unbound and
// pinned), exact-400 replay, and native availability probes all pass through
// it. The free decision reuses the local model metadata classification
// (zero input+output cost or case-insensitive free ID, never hardcoded IDs)
// and is independent of cfg.Anonymous. Native paid models, custom fallback
// channels, custom probes, model discovery, capability fetches, proxy health,
// and the admin transport probe never use it.
//
// The shaped body always forces upstream stream:true and ensures the five
// core tools are present. Caller history, fields, and extra tools are
// preserved. Non-stream callers still receive collapsed protocol-correct JSON:
// the gateway consumes the upstream SSE fully before writing client bytes.

var nativeAgentCoreTools = []string{"bash", "edit", "glob", "grep", "read"}

// anonymousCoreTools is the legacy name kept for existing tests/callers.
// It aliases the single native authority above; no independent copy exists.
var anonymousCoreTools = nativeAgentCoreTools

var nativeAgentCoreToolDescriptions = map[string]string{
	"bash": "Run shell commands",
	"edit": "Edit files",
	"glob": "Find files by pattern",
	"grep": "Search file contents",
	"read": "Read files",
}

var anonymousCoreToolDescriptions = nativeAgentCoreToolDescriptions

// isAnonymousCandidate reports the fixed anonymous public credential/channel.
// Retained for observability only; shaping no longer gates on lane.
func isAnonymousCandidate(cand targetCandidate) bool {
	return cand.CredID == anonymousSchedulerCredentialID
}

// applyNativeAgentShape mutates one prepared upstream payload for the
// native Zen free tier (anonymous and authenticated): it forces stream:true
// and ensures all five core tools exist. Missing tools are appended in
// deterministic core order; caller definitions with the same name are never
// overwritten. Absent/null tools become an array; a present non-array or
// structurally unrecognized entry fails loudly instead of being silently
// discarded. This is the single shaping authority; there is no independent
// auth copy.
func applyNativeAgentShape(protocol Protocol, payload map[string]any) error {
	if payload == nil {
		return fmt.Errorf("native agent shape requires an object body")
	}
	payload["stream"] = true
	if protocol == ProtocolChat {
		ensureChatStreamOptions(payload)
	}
	return ensureNativeAgentCoreTools(protocol, payload)
}

// applyAnonymousAgentShape is the legacy name delegating to the single
// native authority above.
func applyAnonymousAgentShape(protocol Protocol, payload map[string]any) error {
	return applyNativeAgentShape(protocol, payload)
}

func ensureChatStreamOptions(payload map[string]any) {
	raw, ok := payload["stream_options"]
	opts, isMap := raw.(map[string]any)
	if !ok || !isMap {
		payload["stream_options"] = map[string]any{"include_usage": true}
		return
	}
	opts["include_usage"] = true
}

func ensureNativeAgentCoreTools(protocol Protocol, payload map[string]any) error {
	raw, exists := payload["tools"]
	if !exists || raw == nil {
		payload["tools"] = nativeAgentCoreToolDefs(protocol, nil)
		return nil
	}
	arr, ok := raw.([]any)
	if !ok {
		return fmt.Errorf("tools must be an array")
	}
	present := make(map[string]bool, len(arr))
	for i, entry := range arr {
		item, ok := entry.(map[string]any)
		if !ok {
			return fmt.Errorf("tools[%d] must be an object", i)
		}
		name, ok := nativeAgentToolName(protocol, item)
		if !ok {
			return fmt.Errorf("tools[%d] must be a function tool", i)
		}
		if name != "" {
			present[name] = true
		}
	}
	for _, name := range nativeAgentCoreTools {
		if present[name] {
			continue
		}
		arr = append(arr, nativeAgentCoreToolDef(protocol, name))
	}
	payload["tools"] = arr
	return nil
}

func ensureAnonymousCoreTools(protocol Protocol, payload map[string]any) error {
	return ensureNativeAgentCoreTools(protocol, payload)
}

// nativeAgentToolName extracts the tool name for the target protocol. It
// returns ok=false only for structurally unrecognized entries that must fail
// loudly. A structurally valid entry without a name returns ("", true) so the
// caller preserves it without treating it as covering a core tool.
func nativeAgentToolName(protocol Protocol, item map[string]any) (string, bool) {
	switch protocol {
	case ProtocolChat, ProtocolResponses:
		if stringAt(item, "type") != "function" {
			return "", false
		}
		if protocol == ProtocolChat {
			fn, ok := item["function"].(map[string]any)
			if !ok {
				return "", false
			}
			return stringAt(fn, "name"), true
		}
		return stringAt(item, "name"), true
	case ProtocolAnthropic:
		return stringAt(item, "name"), true
	default:
		return "", false
	}
}

func anonymousToolName(protocol Protocol, item map[string]any) (string, bool) {
	return nativeAgentToolName(protocol, item)
}

func nativeAgentCoreToolDefs(protocol Protocol, _ map[string]bool) []any {
	out := make([]any, 0, len(nativeAgentCoreTools))
	for _, name := range nativeAgentCoreTools {
		out = append(out, nativeAgentCoreToolDef(protocol, name))
	}
	return out
}

func anonymousCoreToolDefs(protocol Protocol, _ map[string]bool) []any {
	return nativeAgentCoreToolDefs(protocol, nil)
}

func nativeAgentCoreToolDef(protocol Protocol, name string) map[string]any {
	desc := nativeAgentCoreToolDescriptions[name]
	if desc == "" {
		desc = name
	}
	schema := map[string]any{"type": "object", "properties": map[string]any{}}
	switch protocol {
	case ProtocolResponses:
		return map[string]any{
			"type": "function", "name": name, "description": desc,
			"parameters": schema, "strict": false,
		}
	case ProtocolAnthropic:
		return map[string]any{
			"name": name, "description": desc,
			"input_schema": schema,
		}
	default:
		return map[string]any{
			"type": "function",
			"function": map[string]any{
				"name": name, "description": desc, "parameters": schema,
			},
		}
	}
}

func anonymousCoreToolDef(protocol Protocol, name string) map[string]any {
	return nativeAgentCoreToolDef(protocol, name)
}

// shapedNativeAgentBody applies the shared native free agent shape to one
// frozen canonical body. It decodes, shapes, and re-encodes; route-session
// fields are preserved for the caller to stamp afterwards via
// applyRouteSessionToBody. Paid and custom bodies never pass through here.
func shapedNativeAgentBody(canonical []byte, protocol Protocol) ([]byte, error) {
	var payload map[string]any
	if err := json.Unmarshal(canonical, &payload); err != nil {
		return nil, fmt.Errorf("native agent shape: %w", err)
	}
	if err := applyNativeAgentShape(protocol, payload); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("request contains unsupported JSON values")
	}
	return encoded, nil
}

// shapedAnonymousBody is the legacy name delegating to the single native
// authority above.
func shapedAnonymousBody(canonical []byte, protocol Protocol) ([]byte, error) {
	return shapedNativeAgentBody(canonical, protocol)
}

// collapseUpstreamSSE consumes one upstream SSE stream for a non-stream
// caller and returns the collapsed target-protocol JSON body. It reuses the
// existing bridge stream parser for protocol parsing and the bridge response
// encoders for the target envelope, so text, reasoning/thinking, tool calls,
// stop reason, IDs, and upstream-reported usage (never estimated) match the
// streaming path. A structured upstream error event or a malformed/truncated
// stream returns an error and must never become a clean success.
func collapseUpstreamSSE(reader io.Reader, from, to Protocol, model string) ([]byte, bridgeUsage, bool, error) {
	parser := &bridgeStreamParser{
		protocol:          from,
		tools:             map[string]bool{},
		toolIDs:           map[string]string{},
		toolNames:         map[string]string{},
		responseArgs:      map[string]bool{},
		responseReasoning: map[string]bool{},
	}
	var text strings.Builder
	var reasoning []bridgeBlock
	var responseID, responseModel string
	var stop string
	var usage bridgeUsage
	var reported bool
	tools := map[string]*bridgeStreamTool{}
	order := []string{}
	normalDone := false
	toolFor := func(key string) *bridgeStreamTool {
		if key == "" {
			key = fmt.Sprintf("tool-%d", len(order))
		}
		if current := tools[key]; current != nil {
			return current
		}
		tool := &bridgeStreamTool{Key: key, ID: randomID("call", 12), ItemID: randomID("fc", 12), Index: len(order)}
		tools[key] = tool
		order = append(order, key)
		return tool
	}
	collapseErr := func(message string) error {
		return fmt.Errorf("upstream stream failed: %s", message)
	}
	readErr := readSSE(reader, func(eventName, data string) error {
		events, err := parser.Parse(eventName, data)
		if err != nil {
			return err
		}
		for _, event := range events {
			switch event.Kind {
			case "start":
				if event.ResponseID != "" && responseID == "" {
					responseID = event.ResponseID
				}
				if event.Model != "" {
					responseModel = event.Model
				}
			case "text":
				text.WriteString(event.Text)
			case "reasoning":
				if event.Encrypted != "" {
					reasoning = append(reasoning, bridgeBlock{Kind: "reasoning", Text: event.Text, Encrypted: event.Encrypted})
				} else if event.Text != "" {
					if len(reasoning) == 0 || reasoning[len(reasoning)-1].Encrypted != "" {
						reasoning = append(reasoning, bridgeBlock{Kind: "reasoning", Text: event.Text})
					} else {
						reasoning[len(reasoning)-1].Text += event.Text
					}
				}
			case "reasoning_signature":
				if event.Signature == "" {
					break
				}
				if len(reasoning) == 0 || reasoning[len(reasoning)-1].Encrypted != "" {
					reasoning = append(reasoning, bridgeBlock{Kind: "reasoning", Signature: event.Signature})
				} else {
					reasoning[len(reasoning)-1].Signature += event.Signature
				}
			case "tool_start":
				tool := toolFor(event.ToolKey)
				if event.ToolID != "" {
					tool.ID = event.ToolID
				}
				if isUsableToolName(event.ToolName) {
					tool.Name = event.ToolName
				}
			case "tool_delta":
				tool := toolFor(event.ToolKey)
				if event.ToolID != "" {
					tool.ID = event.ToolID
				}
				if isUsableToolName(event.ToolName) {
					tool.Name = event.ToolName
				}
				tool.Arguments.WriteString(event.Text)
			case "usage":
				if event.Usage != nil {
					reported = true
					mergeBridgeUsage(&usage, *event.Usage)
				}
			case "finish":
				stop = event.Stop
			case "error":
				return collapseErr(firstString(event.Error, "upstream stream failed"))
			case "done":
				normalDone = true
				return errStreamNormalTermination
			}
		}
		return nil
	})
	if readErr != nil {
		if errors.Is(readErr, errStreamNormalTermination) {
			readErr = nil
		} else {
			if strings.HasPrefix(readErr.Error(), "upstream stream failed:") {
				return nil, bridgeUsage{}, false, readErr
			}
			return nil, bridgeUsage{}, false, fmt.Errorf("upstream SSE stream failed: %v", readErr)
		}
	}
	if !normalDone {
		return nil, bridgeUsage{}, false, fmt.Errorf("upstream SSE stream ended before a terminal event")
	}
	if stop == "" {
		hasUsable := false
		for _, key := range order {
			if tool := tools[key]; tool != nil && isUsableToolName(tool.Name) {
				hasUsable = true
				break
			}
		}
		if hasUsable {
			stop = "tool_calls"
		} else {
			stop = "stop"
		}
	}
	// Downgrade a phantom tool terminal when no usable tool block exists.
	// Empty arguments stay valid; only blank names are phantom.
	if isToolFinishSignal(stop) {
		hasUsable := false
		for _, key := range order {
			if tool := tools[key]; tool != nil && isUsableToolName(tool.Name) {
				hasUsable = true
				break
			}
		}
		if !hasUsable {
			stop = "stop"
		}
	}
	finalModel := firstString(responseModel, model)
	// The bridge stream parser exposes no upstream-created timestamp surface
	// (start events carry only ID/model), so collapsed output falls back to the
	// current Unix time like decodeBridgeResponse and the streaming emitter.
	response := bridgeResponse{
		ID:        responseID,
		Model:     finalModel,
		Text:      text.String(),
		Stop:      stop,
		Usage:     usage,
		Created:   time.Now().Unix(),
		Reasoning: reasoning,
	}
	for _, key := range order {
		tool := tools[key]
		if tool == nil || !isUsableToolName(tool.Name) {
			continue
		}
		response.Tools = append(response.Tools, bridgeBlock{
			Kind: "tool_call", ID: tool.ID, Name: tool.Name,
			ArgumentsJSON: tool.Arguments.String(),
		})
	}
	encoded, err := json.Marshal(encodeBridgeResponse(to, response))
	if err != nil {
		return nil, bridgeUsage{}, false, fmt.Errorf("upstream SSE stream failed: %v", err)
	}
	return encoded, usage, reported, nil
}

// collapseNativeAgentProbeBody validates one native free probe HTTP body.
// Plain JSON completions validate directly; event-stream bytes collapse
// internally to JSON first so valid SSE never reports a false parse_error.
// Paid probes use plain validation only via bulkProbeSuccessBody.
func collapseNativeAgentProbeBody(protocol Protocol, model string, raw []byte) ([]byte, bool) {
	if bulkProbeSuccessBody(protocol, raw) {
		return raw, true
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || !bytes.Contains(trimmed, []byte("data:")) {
		return nil, false
	}
	collapsed, _, _, err := collapseUpstreamSSE(bytes.NewReader(raw), protocol, protocol, model)
	if err != nil {
		return nil, false
	}
	if !bulkProbeSuccessBody(protocol, collapsed) {
		return nil, false
	}
	return collapsed, true
}

func collapseAnonymousProbeBody(protocol Protocol, model string, raw []byte) ([]byte, bool) {
	return collapseNativeAgentProbeBody(protocol, model, raw)
}
