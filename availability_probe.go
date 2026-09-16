package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// bulkProbeModel selects a real directory model for availability probing.
// It never hardcodes model IDs: the first sorted catalog model satisfying the
// tier lane is used. Anonymous requires a free Zen-servable model;
// authenticated Zen/Go require a model actually served by that tier.
func (g *Gateway) bulkProbeModel(tier Tier, public bool) (string, Protocol, bool) {
	if g == nil || g.catalog == nil {
		return "", "", false
	}
	for _, model := range g.catalog.List() {
		if public {
			if !g.catalog.anonymousDecision(model).Allowed {
				continue
			}
			route, err := g.catalog.Route(model, false, false, true)
			if err != nil || !route.Anonymous || route.Tier != TierZen {
				continue
			}
			return model, route.ProtocolFor(TierZen), true
		}
		if tier == TierZen {
			route, err := g.catalog.Route(model, true, false, false)
			if err != nil {
				continue
			}
			serves := route.Tier == TierZen
			if !serves {
				for _, t := range route.KeyTiers {
					if t == TierZen {
						serves = true
						break
					}
				}
			}
			if !serves {
				continue
			}
			return model, route.ProtocolFor(TierZen), true
		}
		if tier == TierGo {
			route, err := g.catalog.Route(model, false, true, false)
			if err != nil {
				continue
			}
			serves := route.Tier == TierGo
			if !serves {
				for _, t := range route.KeyTiers {
					if t == TierGo {
						serves = true
						break
					}
				}
			}
			if !serves {
				continue
			}
			return model, route.ProtocolFor(TierGo), true
		}
	}
	return "", "", false
}

func bulkProbeRequestBody(model string, protocol Protocol) ([]byte, error) {
	var payload map[string]any
	switch protocol {
	case ProtocolResponses:
		payload = map[string]any{"model": model, "input": "hi", "max_output_tokens": 1, "stream": false}
	case ProtocolAnthropic:
		payload = map[string]any{"model": model, "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "max_tokens": 1}
	default:
		payload = map[string]any{"model": model, "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "max_tokens": 1, "stream": false}
	}
	return json.Marshal(payload)
}

func bulkProbeSuccessBody(protocol Protocol, body []byte) bool {
	if len(bytes.TrimSpace(body)) == 0 {
		return false
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return false
	}
	switch protocol {
	case ProtocolChat:
		if _, ok := payload["choices"]; ok {
			return true
		}
		if _, ok := payload["error"]; ok {
			return false
		}
		return len(payload) > 0
	case ProtocolResponses:
		if _, ok := payload["output"]; ok {
			return true
		}
		return len(payload) > 0
	case ProtocolAnthropic:
		if _, ok := payload["content"]; ok {
			return true
		}
		return len(payload) > 0
	default:
		return len(payload) > 0
	}
}

type bulkCustomTarget struct {
	Name    string
	BaseURL string
	Model   string
	APIKey  string
}

type bulkCustomResult struct {
	Target       bulkCustomTarget
	StartedNanos int64
	DurationMS   int64
	TransportErr error
	IsProxy      bool
	Cancelled    bool
	Status       int
	Success      bool
	ParseError   bool
}

func bulkCustomOutcomeLabel(r bulkCustomResult) string {
	if r.Cancelled {
		return "cancelled"
	}
	if r.TransportErr != nil {
		return "inconclusive"
	}
	if r.ParseError {
		return "parse_error"
	}
	switch {
	case r.Success:
		return "success"
	case r.Status == 401:
		return "auth_failure"
	case r.Status == 429:
		return "rate_limited"
	case r.Status == 403 || r.Status >= 500:
		return "upstream_failure"
	case r.Status == 408 || r.Status == 425:
		return "transient_client"
	case r.Status >= 400 && r.Status < 500:
		return "client_rejected"
	case r.Status != 0:
		return "other_response"
	default:
		return "inconclusive"
	}
}

func (g *Gateway) bulkCustomProbeOnce(parent context.Context, tgt bulkCustomTarget) bulkCustomResult {
	endpoint := fallbackChatURL(tgt.BaseURL)
	started := time.Now()
	startedNanos := started.UnixNano()
	if endpoint == "" || strings.TrimSpace(tgt.APIKey) == "" || strings.TrimSpace(tgt.Model) == "" {
		return bulkCustomResult{Target: tgt, StartedNanos: startedNanos, DurationMS: 0, Cancelled: parent.Err() != nil}
	}
	body, err := bulkProbeRequestBody(strings.TrimSpace(tgt.Model), ProtocolChat)
	if err != nil {
		return bulkCustomResult{Target: tgt, StartedNanos: startedNanos, Cancelled: parent.Err() != nil}
	}
	sendCtx, cancel := context.WithTimeout(parent, bulkPerSendTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(sendCtx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return bulkCustomResult{Target: tgt, StartedNanos: startedNanos, DurationMS: max(time.Since(started).Milliseconds(), 0), TransportErr: err, Cancelled: parent.Err() != nil}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", opencodeUserAgent())
	req.Header.Set("x-opencode-client", "cli")
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(tgt.APIKey))
	resp, err := g.fallbackCustomClient().Do(req)
	durationMS := max(time.Since(started).Milliseconds(), 0)
	if err != nil {
		return bulkCustomResult{Target: tgt, StartedNanos: startedNanos, DurationMS: durationMS, TransportErr: err, Cancelled: parent.Err() != nil}
	}
	defer resp.Body.Close()
	status := resp.StatusCode
	if status/100 != 2 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return bulkCustomResult{Target: tgt, StartedNanos: startedNanos, DurationMS: durationMS, Status: status}
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return bulkCustomResult{Target: tgt, StartedNanos: startedNanos, DurationMS: durationMS, Status: status, ParseError: true}
	}
	if !bulkProbeSuccessBody(ProtocolChat, raw) {
		return bulkCustomResult{Target: tgt, StartedNanos: startedNanos, DurationMS: durationMS, Status: status, ParseError: true}
	}
	return bulkCustomResult{Target: tgt, StartedNanos: startedNanos, DurationMS: durationMS, Status: status, Success: true}
}

func bulkCustomStatusFor(label string) (string, string) {
	switch label {
	case "success":
		return "available", "success"
	case "rate_limited":
		return "rate_limited", "rate_limited"
	case "auth_failure":
		return "unavailable", "auth_failure"
	case "upstream_failure":
		return "unavailable", "upstream_failure"
	case "transport_failure", "inconclusive":
		return "inconclusive", "inconclusive"
	case "parse_error":
		return "inconclusive", "parse_error"
	case "cancelled":
		return "inconclusive", "cancelled"
	case "transient_client":
		return "inconclusive", "transient_client"
	case "client_rejected":
		return "unavailable", "client_rejected"
	default:
		return "inconclusive", label
	}
}

func sortedNoModelList(has map[string]bool) []string {
	out := []string{}
	for k, v := range has {
		if v {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
