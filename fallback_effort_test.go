package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestFallbackEffortStrictAndDefault(t *testing.T) {
	cfg := testBaseConfig()
	cfg.Fallback = FallbackConfig{Channels: []FallbackChannelConfig{
		{Name: "c1", BaseURL: "https://a.example", APIKey: "k1", Model: "m1"},
	}}
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := norm.Fallback.Channels[0].ReasoningEffort; got != "" {
		t.Fatalf("omitted effort must default to empty, got %q", got)
	}
	for _, bad := range []string{"extreme", "none", "auto", "LOWER-high", "123"} {
		c := testBaseConfig()
		c.Fallback = FallbackConfig{Channels: []FallbackChannelConfig{{Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "m", ReasoningEffort: bad}}}
		if _, err := NormalizeConfig("config.json", c); err == nil {
			t.Fatalf("effort %q must be rejected", bad)
		}
	}
	// Case-insensitive + trim normalization.
	c := testBaseConfig()
	c.Fallback = FallbackConfig{Channels: []FallbackChannelConfig{{Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "m", ReasoningEffort: " Medium "}}}
	norm2, err := NormalizeConfig("config.json", c)
	if err != nil {
		t.Fatal(err)
	}
	if norm2.Fallback.Channels[0].ReasoningEffort != "medium" {
		t.Fatalf("effort normalize=%q want medium", norm2.Fallback.Channels[0].ReasoningEffort)
	}
	// Strict decoder rejects unknown effort-adjacent field.
	var fb FallbackConfig
	dec := json.NewDecoder(strings.NewReader(`{"active":"","channels":[{"name":"c","base_url":"https://a.example","api_key":"k","model":"m","bogus_effort":"low"}]}`))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&fb); err == nil {
		t.Fatal("unknown channel field must be rejected")
	}
	// Marshal roundtrip persists effort.
	data, err := json.Marshal(norm2)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"reasoning_effort"`) {
		t.Fatal("marshal must persist reasoning_effort")
	}
}

func TestFallbackEffortAdminRoundtrip(t *testing.T) {
	cfg := testBaseConfig()
	cfg.Fallback = FallbackConfig{Active: "c1", Channels: []FallbackChannelConfig{
		{Name: "c1", BaseURL: "https://a.example", APIKey: "secret-effort-11111", Model: "m1", Protocol: ProtocolChat, ReasoningEffort: "high"},
	}}
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	view := fallbackViewFromConfig(norm)
	if len(view.Channels) != 1 || view.Channels[0].ReasoningEffort != "high" {
		t.Fatalf("view must carry effort: %+v", view)
	}
	if view.Channels[0].Protocol != ProtocolChat {
		t.Fatalf("view protocol=%q", view.Channels[0].Protocol)
	}
	resolved, err := resolveFallbackInput(FallbackInput{
		Active: "c1",
		Channels: []FallbackChannelInput{
			{Name: "c1", BaseURL: "https://a.example", APIKey: SecretInput{ID: secretFingerprint("secret-effort-11111")}, Model: "m1", Protocol: ProtocolChat, ReasoningEffort: "low"},
		},
	}, norm.Fallback)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Channels[0].ReasoningEffort != "low" {
		t.Fatalf("resolve must carry effort, got %q", resolved.Channels[0].ReasoningEffort)
	}
	// Omitted effort defaults to supplier default.
	resolved2, err := resolveFallbackInput(FallbackInput{
		Active: "c1",
		Channels: []FallbackChannelInput{
			{Name: "c1", BaseURL: "https://a.example", APIKey: SecretInput{Value: "secret-effort-11111"}, Model: "m1"},
		},
	}, norm.Fallback)
	if err != nil {
		t.Fatal(err)
	}
	if resolved2.Channels[0].ReasoningEffort != "" || resolved2.Channels[0].Protocol != ProtocolChat {
		t.Fatalf("omitted effort/protocol must default: %+v", resolved2.Channels[0])
	}
	// Invalid effort through admin path must fail.
	if _, err := resolveFallbackInput(FallbackInput{
		Channels: []FallbackChannelInput{
			{Name: "c1", BaseURL: "https://a.example", APIKey: SecretInput{Value: "k"}, Model: "m", ReasoningEffort: "ultra"},
		},
	}, norm.Fallback); err == nil {
		t.Fatal("invalid effort must fail admin resolve")
	}
}

func effortPayload(model string, external Protocol, payload map[string]any) upstreamExtra {
	return upstreamExtra{External: external, Payload: payload}
}

func TestFallbackEffortChatAndResponses(t *testing.T) {
	chatCh := FallbackChannelConfig{Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "cm", Protocol: ProtocolChat, ReasoningEffort: "high"}
	got, err := buildFallbackRequestBody(effortPayload("m", ProtocolChat, map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}), true, nil, modelRoute{}, chatCh)
	if err != nil {
		t.Fatal(err)
	}
	var chatBody map[string]any
	if err := json.Unmarshal(got, &chatBody); err != nil {
		t.Fatal(err)
	}
	if chatBody["reasoning_effort"] != "high" {
		t.Fatalf("chat effort=%v", chatBody["reasoning_effort"])
	}
	if chatBody["model"] != "cm" {
		t.Fatalf("chat model=%v", chatBody["model"])
	}
	respCh := FallbackChannelConfig{Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "cm", Protocol: ProtocolResponses, ReasoningEffort: "medium"}
	got2, err := buildFallbackRequestBody(effortPayload("m", ProtocolChat, map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}), true, nil, modelRoute{}, respCh)
	if err != nil {
		t.Fatal(err)
	}
	var respBody map[string]any
	if err := json.Unmarshal(got2, &respBody); err != nil {
		t.Fatal(err)
	}
	reasoning, ok := respBody["reasoning"].(map[string]any)
	if !ok || reasoning["effort"] != "medium" {
		t.Fatalf("responses reasoning=%v", respBody["reasoning"])
	}
}

func TestFallbackEffortResponsesPreservesMap(t *testing.T) {
	ch := FallbackChannelConfig{Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "cm", Protocol: ProtocolResponses, ReasoningEffort: "low"}
	// Client already carries reasoning summary + encrypted content: bridge may
	// surface reasoning as a map; the override must keep other keys.
	converted := map[string]any{
		"model":     "cm",
		"reasoning": map[string]any{"effort": "high", "summary": "auto", "encrypted_content": "abc"},
	}
	applyFallbackReasoningEffort(converted, fallbackChannelEffort(ch), ProtocolResponses)
	got, ok := converted["reasoning"].(map[string]any)
	if !ok {
		t.Fatalf("reasoning must stay map: %v", converted["reasoning"])
	}
	if got["effort"] != "low" || got["summary"] != "auto" || got["encrypted_content"] != "abc" {
		t.Fatalf("map keys not preserved: %v", got)
	}
	// Non-map reasoning is replaced by the effort map.
	converted2 := map[string]any{"model": "cm", "reasoning": "high"}
	applyFallbackReasoningEffort(converted2, "medium", ProtocolResponses)
	got2, ok := converted2["reasoning"].(map[string]any)
	if !ok || got2["effort"] != "medium" {
		t.Fatalf("string reasoning must become effort map: %v", converted2["reasoning"])
	}
}

func TestFallbackEffortEmptyStripsSupplierDefault(t *testing.T) {
	ch := FallbackChannelConfig{ID: "c", Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "cm", Protocol: ProtocolChat, ReasoningEffort: ""}
	// Chat client already carries reasoning_effort: supplier-default must strip it.
	got, err := buildFallbackRequestBody(effortPayload("m", ProtocolChat, map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "reasoning_effort": "medium"}), true, nil, modelRoute{}, ch)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(got, &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["reasoning_effort"]; ok {
		t.Fatalf("supplier-default must strip chat reasoning_effort, got %v", body["reasoning_effort"])
	}
	if _, ok := body["reasoning"]; ok {
		t.Fatalf("supplier-default must strip chat reasoning, got %v", body["reasoning"])
	}
	// Responses supplier-default must remove the target reasoning control.
	respCh := FallbackChannelConfig{ID: "c", Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "cm", Protocol: ProtocolResponses, ReasoningEffort: ""}
	converted := map[string]any{"model": "cm", "input": "hi", "reasoning": map[string]any{"effort": "high"}}
	applyFallbackReasoningEffort(converted, fallbackChannelEffort(respCh), ProtocolResponses)
	if _, ok := converted["reasoning"]; ok {
		t.Fatalf("supplier-default must strip responses reasoning: %v", converted)
	}
	// History/content fields unrelated to strength must survive stripping.
	histChat := map[string]any{"model": "cm", "messages": []any{map[string]any{"role": "assistant", "content": "hi", "reasoning_content": "kept"}}, "reasoning_effort": "high"}
	applyFallbackReasoningEffort(histChat, "", ProtocolChat)
	if _, ok := histChat["reasoning_effort"]; ok {
		t.Fatal("strength must be stripped")
	}
	msgs, _ := histChat["messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("history messages must survive: %v", histChat["messages"])
	}
	if m, _ := msgs[0].(map[string]any); m["reasoning_content"] != "kept" {
		t.Fatalf("reasoning_content history must survive: %v", m)
	}
	histResp := map[string]any{"model": "cm", "input": []any{map[string]any{"type": "reasoning", "summary": "kept"}}, "reasoning": map[string]any{"effort": "high"}}
	applyFallbackReasoningEffort(histResp, "", ProtocolResponses)
	if _, ok := histResp["reasoning"]; ok {
		t.Fatal("responses strength must be stripped")
	}
	if _, ok := histResp["input"]; !ok {
		t.Fatal("responses input history must survive")
	}
}

func TestFallbackEffortInheritPreserves(t *testing.T) {
	// Inherit preserves the converted strength; absent strength injects nothing.
	ch := FallbackChannelConfig{ID: "c", Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "cm", Protocol: ProtocolChat, ReasoningEffort: "inherit"}
	got, err := buildFallbackRequestBody(effortPayload("m", ProtocolChat, map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "reasoning_effort": "medium"}), true, nil, modelRoute{}, ch)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(got, &body); err != nil {
		t.Fatal(err)
	}
	if body["reasoning_effort"] != "medium" {
		t.Fatalf("inherit must preserve converted chat strength, got %v", body["reasoning_effort"])
	}
	respCh := FallbackChannelConfig{ID: "c", Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "cm", Protocol: ProtocolResponses, ReasoningEffort: "inherit"}
	converted := map[string]any{"model": "cm", "input": "hi", "reasoning": map[string]any{"effort": "high", "summary": "auto"}}
	applyFallbackReasoningEffort(converted, fallbackChannelEffort(respCh), ProtocolResponses)
	if got, ok := converted["reasoning"].(map[string]any); !ok || got["effort"] != "high" {
		t.Fatalf("inherit must preserve responses reasoning: %v", converted["reasoning"])
	}
	empty := map[string]any{"model": "cm", "input": "hi"}
	applyFallbackReasoningEffort(empty, "inherit", ProtocolResponses)
	if _, ok := empty["reasoning"]; ok {
		t.Fatalf("inherit with no strength must inject nothing: %v", empty)
	}
	emptyChat := map[string]any{"model": "cm", "messages": []any{}}
	applyFallbackReasoningEffort(emptyChat, "inherit", ProtocolChat)
	if _, ok := emptyChat["reasoning_effort"]; ok {
		t.Fatalf("inherit with no strength must inject nothing: %v", emptyChat)
	}
}

func TestFallbackEffortAnthropicBothProtocols(t *testing.T) {
	anthroPayload := func() map[string]any {
		return map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "max_tokens": 8}
	}
	chatCh := FallbackChannelConfig{Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "cm", Protocol: ProtocolChat, ReasoningEffort: "low"}
	got, err := buildFallbackRequestBody(effortPayload("m", ProtocolAnthropic, anthroPayload()), true, nil, modelRoute{}, chatCh)
	if err != nil {
		t.Fatal(err)
	}
	var chatBody map[string]any
	if err := json.Unmarshal(got, &chatBody); err != nil {
		t.Fatal(err)
	}
	if chatBody["reasoning_effort"] != "low" {
		t.Fatalf("anthropic->chat effort=%v", chatBody["reasoning_effort"])
	}
	respCh := FallbackChannelConfig{Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "cm", Protocol: ProtocolResponses, ReasoningEffort: "high"}
	got2, err := buildFallbackRequestBody(effortPayload("m", ProtocolAnthropic, anthroPayload()), true, nil, modelRoute{}, respCh)
	if err != nil {
		t.Fatal(err)
	}
	var respBody map[string]any
	if err := json.Unmarshal(got2, &respBody); err != nil {
		t.Fatal(err)
	}
	reasoning, ok := respBody["reasoning"].(map[string]any)
	if !ok || reasoning["effort"] != "high" {
		t.Fatalf("anthropic->responses reasoning=%v", respBody["reasoning"])
	}
}

func TestFallbackEffortIdentityExcludesEffort(t *testing.T) {
	base := FallbackChannelConfig{Name: "c1", BaseURL: "https://a.example/", APIKey: "k1", Model: "cm", Protocol: ProtocolChat, ReasoningEffort: ""}
	other := FallbackChannelConfig{Name: "c1", BaseURL: "https://a.example", APIKey: "k1", Model: "cm", Protocol: ProtocolChat, ReasoningEffort: "high"}
	b1 := fallbackBindingFor(base)
	b2 := fallbackBindingFor(other)
	if b1 != b2 {
		t.Fatalf("effort must not affect binding identity: %+v vs %+v", b1, b2)
	}
	if !b1.matchesChannel(other) || !b2.matchesChannel(base) {
		t.Fatal("effort-only change must still match the bound channel (no 502)")
	}
	// Protocol still participates in identity.
	protoChanged := FallbackChannelConfig{Name: "c1", BaseURL: "https://a.example", APIKey: "k1", Model: "cm", Protocol: ProtocolResponses, ReasoningEffort: ""}
	if b1.matchesChannel(protoChanged) {
		t.Fatal("protocol change must still mismatch")
	}
}
