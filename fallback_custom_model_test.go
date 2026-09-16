package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCustomFallbackUsesChannelModel(t *testing.T) {
	custom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fallbackChatOK("chan-model")))
	}))
	defer custom.Close()
	gw, _ := fallbackTestGateway(t, []FallbackChannelConfig{{Name: "c1", BaseURL: custom.URL, APIKey: "k1", Model: "chan-model"}}, "c1")
	ch, _ := fallbackChannelByName(gw.cfg, "c1")
	route := anonAuthRoute()
	route.ID = "client-model"
	ctx := context.WithValue(context.Background(), requestMetaKey{}, &requestMeta{Model: "client-model", Request: "req-custom-model"})
	ids := pinIDs("ses_custom_model", "req-custom-model")
	ex := upstreamExtra{External: ProtocolChat, Payload: map[string]any{"model": "client-model", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}}
	resp, eff, _, err := gw.doCustomFallbackRequest(ctx, route, ex, true, routeBodies(), ids, ch, fallbackBindingFor(ch), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer drainResp(resp)
	if eff.Tier != TierCustom {
		t.Fatalf("tier=%q want custom", eff.Tier)
	}
	if eff.ID != "chan-model" {
		t.Fatalf("effective model=%q want chan-model", eff.ID)
	}
	meta, _ := ctx.Value(requestMetaKey{}).(*requestMeta)
	if meta == nil || meta.Model != "chan-model" {
		t.Fatalf("request meta model=%+v want chan-model", meta)
	}
	if meta.Tier != "custom" || meta.Channel != "custom" || meta.KeyID != "custom:c1" {
		t.Fatalf("custom observability identity changed: %+v", meta)
	}
	recent := gw.monitor.Snapshot().Upstream.Recent
	if len(recent) == 0 {
		t.Fatal("no attempts recorded")
	}
	last := recent[len(recent)-1]
	if last.Model != "chan-model" {
		t.Fatalf("attempt model=%q want chan-model", last.Model)
	}
	if last.Tier != "custom" || last.Channel != "custom" || last.KeyID != "custom:c1" {
		t.Fatalf("attempt identity changed: %+v", last)
	}
}

func TestNativeAttemptModelUnchanged(t *testing.T) {
	m := NewMonitor()
	m.RecordAttempt(UpstreamAttempt{
		RequestID: "r-native", Model: "client-m", Tier: "zen", Protocol: "chat",
		Attempt: 1, KeyID: "KKKKK", Channel: "key", Proxy: "direct", Status: 200,
		DurationMS: 1, Success: true, FailureClass: AttemptClassSuccess,
	})
	recent := m.Snapshot().Upstream.Recent
	if len(recent) != 1 || recent[0].Model != "client-m" {
		t.Fatalf("native attempt model must stay client model: %+v", recent)
	}
}

func TestDecodeOpenAIUsageCacheWriteTokens(t *testing.T) {
	// Responses-style input_tokens_details.cache_write_tokens must feed CacheCreation.
	u := decodeOpenAIUsage(map[string]any{
		"input_tokens":  10,
		"output_tokens": 2,
		"total_tokens":  12,
		"input_tokens_details": map[string]any{
			"cached_tokens":      3,
			"cache_write_tokens": 7,
		},
	})
	if u.CacheCreation != 7 {
		t.Fatalf("CacheCreation=%d want 7: %+v", u.CacheCreation, u)
	}
	if u.Cached != 3 || u.Input != 10 {
		t.Fatalf("other fields changed: %+v", u)
	}
	// Existing priority preserved: explicit cache_creation_input_tokens wins.
	u2 := decodeOpenAIUsage(map[string]any{
		"prompt_tokens":     10,
		"completion_tokens": 2,
		"total_tokens":      12,
		"prompt_tokens_details": map[string]any{
			"cached_tokens":               1,
			"cache_creation_input_tokens": 4,
			"cache_write_tokens":          9,
		},
		"input_tokens_details": map[string]any{
			"cache_write_tokens": 7,
		},
	})
	if u2.CacheCreation != 4 {
		t.Fatalf("priority changed, CacheCreation=%d want 4: %+v", u2.CacheCreation, u2)
	}
}
