package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func mustNormalizeFallback(t *testing.T, fb FallbackConfig) FallbackConfig {
	t.Helper()
	cfg := testBaseConfig()
	cfg.Fallback = fb
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	return norm.Fallback
}

func TestFallbackLegacyNameOnlyMigration(t *testing.T) {
	fb := mustNormalizeFallback(t, FallbackConfig{
		Active: "c1",
		Channels: []FallbackChannelConfig{
			{Name: "c1", BaseURL: "https://a.example", APIKey: "k1", Model: "m1"},
		},
	})
	if fb.Channels[0].ID != "c1" {
		t.Fatalf("legacy valid name must derive ID as-is, got %q", fb.Channels[0].ID)
	}
	if fb.Active != "c1" {
		t.Fatalf("legacy active must normalize to ID, got %q", fb.Active)
	}
	// Canonical save includes id.
	data, err := json.Marshal(fb)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"id"`) {
		t.Fatalf("canonical output must include id: %s", string(data))
	}
	// Strict decoder still rejects unknown channel fields.
	var decoded FallbackConfig
	dec := json.NewDecoder(strings.NewReader(`{"active":"","channels":[{"id":"c","name":"c","base_url":"https://a.example","api_key":"k","model":"m","bogus":1}]}`))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&decoded); err == nil {
		t.Fatal("unknown channel field must be rejected")
	}
}

func TestFallbackOpenCodeGoSave(t *testing.T) {
	fb := mustNormalizeFallback(t, FallbackConfig{
		Active: "opencode-go",
		Channels: []FallbackChannelConfig{
			{ID: "opencode-go", Name: "OpenCode Go", BaseURL: "https://a.example", APIKey: "k1", Model: "m1"},
		},
	})
	if fb.Channels[0].Name != "OpenCode Go" {
		t.Fatalf("display name must survive, got %q", fb.Channels[0].Name)
	}
	if fb.Active != "opencode-go" {
		t.Fatalf("active must stay ID, got %q", fb.Active)
	}
	// Legacy active referencing the display name normalizes to the ID.
	fb2 := mustNormalizeFallback(t, FallbackConfig{
		Active: "OpenCode Go",
		Channels: []FallbackChannelConfig{
			{ID: "opencode-go", Name: "OpenCode Go", BaseURL: "https://a.example", APIKey: "k1", Model: "m1"},
		},
	})
	if fb2.Active != "opencode-go" {
		t.Fatalf("legacy display active must normalize to ID, got %q", fb2.Active)
	}
	// Slug derivation for spaced names without explicit ID.
	fb3 := mustNormalizeFallback(t, FallbackConfig{
		Channels: []FallbackChannelConfig{
			{Name: "OpenCode Go", BaseURL: "https://a.example", APIKey: "k1", Model: "m1"},
		},
	})
	if fb3.Channels[0].ID != "opencode-go" {
		t.Fatalf("spaced name must slug-derive ID, got %q", fb3.Channels[0].ID)
	}
}

func TestFallbackIDUniquenessAndActive(t *testing.T) {
	dupID := FallbackConfig{Channels: []FallbackChannelConfig{
		{ID: "c", Name: "First", BaseURL: "https://a.example", APIKey: "k1", Model: "m1"},
		{ID: "c", Name: "Second", BaseURL: "https://b.example", APIKey: "k2", Model: "m2"},
	}}
	if _, err := NormalizeConfig("config.json", withFallback(dupID)); err == nil || !strings.Contains(err.Error(), "duplicate fallback channel id") {
		t.Fatalf("duplicate IDs must fail with id-specific error, got %v", err)
	}
	dupName := FallbackConfig{Channels: []FallbackChannelConfig{
		{ID: "a", Name: "Same", BaseURL: "https://a.example", APIKey: "k1", Model: "m1"},
		{ID: "b", Name: "Same", BaseURL: "https://b.example", APIKey: "k2", Model: "m2"},
	}}
	if _, err := NormalizeConfig("config.json", withFallback(dupName)); err == nil || !strings.Contains(err.Error(), "duplicate fallback channel display name") {
		t.Fatalf("duplicate display names must fail with display-specific error, got %v", err)
	}
	for _, bad := range []string{"has space", "a/b", "x!y", strings.Repeat("z", 65)} {
		badCfg := FallbackConfig{Channels: []FallbackChannelConfig{
			{ID: bad, Name: "N", BaseURL: "https://a.example", APIKey: "k", Model: "m"},
		}}
		if _, err := NormalizeConfig("config.json", withFallback(badCfg)); err == nil || !strings.Contains(err.Error(), "invalid fallback channel id") {
			t.Fatalf("bad id %q must fail with channel-id error, got %v", bad, err)
		}
	}
	// Proxy-pool wording must not leak into channel-ID errors.
	badCfg := FallbackConfig{Channels: []FallbackChannelConfig{
		{ID: "bad id", Name: "N", BaseURL: "https://a.example", APIKey: "k", Model: "m"},
	}}
	if _, err := NormalizeConfig("config.json", withFallback(badCfg)); err == nil || strings.Contains(err.Error(), "proxy pool") {
		t.Fatalf("channel-id error must not use proxy-pool wording: %v", err)
	}
	// Unknown active.
	unknown := FallbackConfig{Active: "nope", Channels: []FallbackChannelConfig{
		{ID: "c1", Name: "C1", BaseURL: "https://a.example", APIKey: "k", Model: "m"},
	}}
	if _, err := NormalizeConfig("config.json", withFallback(unknown)); err == nil {
		t.Fatal("unknown active must fail")
	}
}

func TestFallbackIDNameNamespaceAmbiguity(t *testing.T) {
	// One channel's stable ID equals another channel's display name: must
	// reject in both channel orderings so ID-first lookup with legacy-name
	// fallback can never silently select the wrong channel.
	mk := func(first, second FallbackChannelConfig) FallbackConfig {
		return FallbackConfig{Channels: []FallbackChannelConfig{first, second}}
	}
	a := FallbackChannelConfig{ID: "alpha", Name: "Alpha", BaseURL: "https://a.example", APIKey: "k1", Model: "m1"}
	b := FallbackChannelConfig{ID: "beta", Name: "alpha", BaseURL: "https://b.example", APIKey: "k2", Model: "m2"}
	for idx, fb := range []FallbackConfig{mk(a, b), mk(b, a)} {
		cfg := testBaseConfig()
		cfg.Fallback = fb
		_, err := NormalizeConfig("config.json", cfg)
		if err == nil || !strings.Contains(err.Error(), "conflicts with display name") {
			t.Fatalf("ordering %d: cross-channel id/name collision must fail with conflict error, got %v", idx, err)
		}
		if !strings.Contains(err.Error(), `"alpha"`) {
			t.Fatalf("ordering %d: conflict error must identify the colliding value, got %v", idx, err)
		}
	}
	// Same-channel ID==name stays valid for legacy compatibility.
	valid := FallbackConfig{
		Active: "c1",
		Channels: []FallbackChannelConfig{
			{ID: "c1", Name: "c1", BaseURL: "https://a.example", APIKey: "k1", Model: "m1"},
			{ID: "other", Name: "Other", BaseURL: "https://b.example", APIKey: "k2", Model: "m2"},
		},
	}
	cfg := testBaseConfig()
	cfg.Fallback = valid
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatalf("same-channel id==name must stay valid, got %v", err)
	}
	if norm.Fallback.Active != "c1" {
		t.Fatalf("active=%q want c1", norm.Fallback.Active)
	}
}

func withFallback(fb FallbackConfig) Config {
	cfg := testBaseConfig()
	cfg.Fallback = fb
	return cfg
}

func TestFallbackRenamePreservesBinding(t *testing.T) {
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("cm")))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{
		{ID: "c1", Name: "First", BaseURL: custom.URL, APIKey: "k1", Model: "cm"},
	}, "c1")
	ses := "ses_rename_keep"
	bindAnonPin(t, gw, ses, "m")
	postStub(t, gw, "a", 0, nil, nil, func(*http.Request) (*http.Response, error) {
		return responseWithBody(429, `{"error":{"message":"slow"}}`), nil
	})
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	resp, _, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r1"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	drainResp(resp)
	before, ok := gw.scheduler.fallbacks.get(ses)
	if !ok || before.ID != "c1" {
		t.Fatalf("binding must carry stable ID: %+v %v", before, ok)
	}
	// Display-name-only edit: same ID, new name. Pinned session must keep serving.
	gw.cfg.Fallback.Channels = []FallbackChannelConfig{
		{ID: "c1", Name: "Renamed OpenCode Go", BaseURL: custom.URL, APIKey: "k1", Model: "cm"},
	}
	gw.cfg.Fallback.Active = "c1"
	resp2, eff2, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r2"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp2)
	if resp2.StatusCode != 200 || eff2.Tier != TierCustom {
		t.Fatalf("rename must preserve binding: %d %+v", resp2.StatusCode, eff2)
	}
	// Identity change (same ID, different key) must 502.
	gw.cfg.Fallback.Channels = []FallbackChannelConfig{
		{ID: "c1", Name: "Renamed OpenCode Go", BaseURL: custom.URL, APIKey: "changed-key", Model: "cm"},
	}
	resp3, _, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r3"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp3)
	if resp3.StatusCode != 502 {
		t.Fatalf("key change must 502, got %d", resp3.StatusCode)
	}
	// Delete must 502.
	gw.cfg.Fallback.Channels = []FallbackChannelConfig{}
	gw.cfg.Fallback.Active = ""
	resp4, _, _, err := gw.doUpstreamTiers(pinTestCtx(), anonAuthRoute(), routeBodies(), pinIDs(ses, "r4"), 0, ex)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp4)
	if resp4.StatusCode != 502 {
		t.Fatalf("delete must 502, got %d", resp4.StatusCode)
	}
}

func TestFallbackMaskedRevealDiscoverByID(t *testing.T) {
	cfg := testBaseConfig()
	cfg.Fallback = FallbackConfig{Active: "opencode-go", Channels: []FallbackChannelConfig{
		{ID: "opencode-go", Name: "OpenCode Go", BaseURL: "https://a.example", APIKey: "secret-id-key-11111", Model: "m1", Protocol: ProtocolChat},
	}}
	norm, err := NormalizeConfig("config.json", cfg)
	if err != nil {
		t.Fatal(err)
	}
	view := fallbackViewFromConfig(norm)
	if len(view.Channels) != 1 || view.Channels[0].ID != "opencode-go" || view.Channels[0].Name != "OpenCode Go" {
		t.Fatalf("view must carry id+display: %+v", view)
	}
	if strings.Contains(view.Channels[0].APIKey.Display, "secret-id-key") {
		t.Fatal("masked display must not contain full key")
	}
	// Masked-id roundtrip keyed by stable ID.
	resolved, err := resolveFallbackInput(FallbackInput{
		Active: "opencode-go",
		Channels: []FallbackChannelInput{
			{ID: "opencode-go", Name: "OpenCode Go", BaseURL: "https://a.example", APIKey: SecretInput{ID: secretFingerprint("secret-id-key-11111")}, Model: "m1", Protocol: ProtocolChat},
		},
	}, norm.Fallback)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Channels[0].APIKey != "secret-id-key-11111" || resolved.Channels[0].ID != "opencode-go" {
		t.Fatalf("resolve must restore key by ID: %+v", resolved.Channels[0])
	}
	// Legacy name-only input still resolves via deterministic derivation.
	legacy, err := resolveFallbackInput(FallbackInput{
		Active: "c1",
		Channels: []FallbackChannelInput{
			{Name: "c1", BaseURL: "https://a.example", APIKey: SecretInput{ID: secretFingerprint("secret-key-11111")}, Model: "m1"},
		},
	}, FallbackConfig{Channels: []FallbackChannelConfig{{ID: "c1", Name: "c1", BaseURL: "https://a.example", APIKey: "secret-key-11111", Model: "m1"}}})
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Channels[0].ID != "c1" || legacy.Channels[0].APIKey != "secret-key-11111" {
		t.Fatalf("legacy input must derive ID and resolve: %+v", legacy.Channels[0])
	}
	// Discovery by stable ID.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[{"id":"alpha"}]}`))
	}))
	defer srv.Close()
	models, err := fetchFallbackModels(context.Background(), nil, srv.URL, "k")
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 1 || models[0] != "alpha" {
		t.Fatalf("discover=%v", models)
	}
	manager := &RuntimeManager{monitor: NewMonitor(), hub: NewLogHub(100), redactor: NewSecretRedactor()}
	dcfg := testGatewayConfig(map[string][]string{"shared": {"direct"}}, ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"})
	dcfg.Fallback = FallbackConfig{Active: "did", Channels: []FallbackChannelConfig{{ID: "did", Name: "Display", BaseURL: srv.URL, APIKey: "k", Model: "alpha"}}}
	dnorm, err := NormalizeConfig("config.json", dcfg)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := NewGateway(dnorm, nil, manager.monitor)
	if err != nil {
		t.Fatal(err)
	}
	manager.current.Store(&gatewayRuntime{config: dnorm, gateway: gw})
	manager.redactor.Replace(dnorm)
	admin := NewAdminServer(manager, manager.monitor, manager.hub, nil)
	token, csrf := "disc-id-token", "disc-id-csrf"
	current := manager.Config()
	admin.sessions[tokenDigest(token)] = adminSession{
		Username: current.WebUI.Username, AuthVersion: secretFingerprint(current.WebUI.PasswordHash),
		CSRF: csrf, Expires: time.Now().Add(time.Hour),
	}
	// Discover with the stable ID.
	raw, _ := json.Marshal(map[string]any{"channel": "did"})
	req := operabilityRequest(http.MethodPost, "/api/fallback/discover", string(raw), token, csrf)
	rec := serveAdmin(admin, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("discover by id code=%d body=%s", rec.Code, rec.Body.String())
	}
	// Legacy display-name reference still resolves.
	raw2, _ := json.Marshal(map[string]any{"channel": "Display"})
	req2 := operabilityRequest(http.MethodPost, "/api/fallback/discover", string(raw2), token, csrf)
	rec2 := serveAdmin(admin, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("discover by legacy name code=%d body=%s", rec2.Code, rec2.Body.String())
	}
	// Per-row availability check by ID keys the snapshot by ID.
	customResp := gw.runCustomCheck(context.Background(), dnorm.Fallback.Channels[0])
	if customResp.Custom.ID != "did" || customResp.Custom.Name != "Display" {
		t.Fatalf("custom check must carry id+display: %+v", customResp.Custom)
	}
	if got := customAvailabilityKey(customResp.Custom); got != "did" {
		t.Fatalf("availability key must be ID, got %q", got)
	}
	if got := fallbackObservabilityChannel("did"); got != "custom:did" {
		t.Fatalf("observability key must be custom:did, got %q", got)
	}
	_ = io.Discard
}

func TestFallbackReasoningInheritVsDefaultCrossProtocol(t *testing.T) {
	// Responses source carrying strength -> chat target.
	respSource := func(strength map[string]any) map[string]any {
		return map[string]any{"model": "m", "input": "hi", "reasoning": strength}
	}
	supplier := FallbackChannelConfig{ID: "c", Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "cm", Protocol: ProtocolChat, ReasoningEffort: ""}
	got, err := buildFallbackRequestBody(upstreamExtra{External: ProtocolResponses, Payload: respSource(map[string]any{"effort": "high"})}, true, nil, modelRoute{}, supplier)
	if err != nil {
		t.Fatal(err)
	}
	var chatBody map[string]any
	if err := json.Unmarshal(got, &chatBody); err != nil {
		t.Fatal(err)
	}
	if _, ok := chatBody["reasoning_effort"]; ok {
		t.Fatalf("supplier-default must strip cross-protocol chat strength: %v", chatBody["reasoning_effort"])
	}
	inherit := FallbackChannelConfig{ID: "c", Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "cm", Protocol: ProtocolChat, ReasoningEffort: "inherit"}
	got2, err := buildFallbackRequestBody(upstreamExtra{External: ProtocolResponses, Payload: respSource(map[string]any{"effort": "high"})}, true, nil, modelRoute{}, inherit)
	if err != nil {
		t.Fatal(err)
	}
	var chatBody2 map[string]any
	if err := json.Unmarshal(got2, &chatBody2); err != nil {
		t.Fatal(err)
	}
	if chatBody2["reasoning_effort"] == nil {
		t.Fatalf("inherit must preserve cross-protocol chat strength: %v", chatBody2)
	}
	// Chat source -> responses target.
	chatSource := map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "reasoning_effort": "low"}
	respSupplier := FallbackChannelConfig{ID: "c", Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "cm", Protocol: ProtocolResponses, ReasoningEffort: ""}
	got3, err := buildFallbackRequestBody(upstreamExtra{External: ProtocolChat, Payload: chatSource}, true, nil, modelRoute{}, respSupplier)
	if err != nil {
		t.Fatal(err)
	}
	var respBody map[string]any
	if err := json.Unmarshal(got3, &respBody); err != nil {
		t.Fatal(err)
	}
	if _, ok := respBody["reasoning"]; ok {
		t.Fatalf("supplier-default must strip cross-protocol responses reasoning: %v", respBody["reasoning"])
	}
	respInherit := FallbackChannelConfig{ID: "c", Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "cm", Protocol: ProtocolResponses, ReasoningEffort: "inherit"}
	got4, err := buildFallbackRequestBody(upstreamExtra{External: ProtocolChat, Payload: chatSource}, true, nil, modelRoute{}, respInherit)
	if err != nil {
		t.Fatal(err)
	}
	var respBody2 map[string]any
	if err := json.Unmarshal(got4, &respBody2); err != nil {
		t.Fatal(err)
	}
	reasoning, ok := respBody2["reasoning"].(map[string]any)
	if !ok || reasoning["effort"] != "low" {
		t.Fatalf("inherit must preserve cross-protocol responses strength: %v", respBody2["reasoning"])
	}
	// Explicit efforts unchanged.
	for _, effort := range []string{"low", "medium", "high"} {
		ch := FallbackChannelConfig{ID: "c", Name: "c", BaseURL: "https://a.example", APIKey: "k", Model: "cm", Protocol: ProtocolChat, ReasoningEffort: effort}
		raw, err := buildFallbackRequestBody(upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "m", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}, true, nil, modelRoute{}, ch)
		if err != nil {
			t.Fatal(err)
		}
		var parsed map[string]any
		if err := json.Unmarshal(raw, &parsed); err != nil {
			t.Fatal(err)
		}
		if parsed["reasoning_effort"] != effort {
			t.Fatalf("explicit %q must override, got %v", effort, parsed["reasoning_effort"])
		}
	}
	// Reasoning effort stays out of binding identity and probes.
	b1 := fallbackBindingFor(FallbackChannelConfig{ID: "c", Name: "C", BaseURL: "https://a.example", APIKey: "k", Model: "m", Protocol: ProtocolChat, ReasoningEffort: ""})
	b2 := fallbackBindingFor(FallbackChannelConfig{ID: "c", Name: "C", BaseURL: "https://a.example", APIKey: "k", Model: "m", Protocol: ProtocolChat, ReasoningEffort: "inherit"})
	if b1 != b2 {
		t.Fatalf("effort must not affect binding: %+v vs %+v", b1, b2)
	}
}

func TestFallbackWebUIIDAndChromeHygiene(t *testing.T) {
	data, err := readWebUIFile()
	if err != nil {
		t.Fatal(err)
	}
	html := string(data)
	for _, needle := range []string{
		`function fallbackGenerateChannelID()`,
		`fallbackDisplayNameForID`,
		`customRowKey`,
		`继承自请求`,
		`供应商默认`,
		`key-row input.masked`,
		`-webkit-text-security`,
		`keyInput.setAttribute("autocomplete","off")`,
		`keyInput.type="text"`,
		`function setMasked(masked)`,
		`data-id",cid`,
		`out.push({id:cid`,
		`JSON.stringify({id:cid})`,
		`autocomplete="username"`,
		`autocomplete="current-password"`,
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("webui contract missing %q", needle)
		}
	}
	for _, stale := range []string{
		`keyInput.type="password"`,
		`keyInput.autocomplete="new-password"`,
		`innerHTML`, `outerHTML`, `insertAdjacentHTML`, `document.write`, `eval(`,
	} {
		if strings.Contains(html, stale) {
			t.Fatalf("webui must not contain %q", stale)
		}
	}
	// Chrome's browser-chrome password-save prompt cannot be fully asserted by
	// static unit tests: no browser automation is available in this gate, so
	// this test asserts DOM hygiene only (non-password text input,
	// autocomplete off, no credential-like name, masked class toggle) while
	// the real admin login keeps username/current-password semantics.
}
