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
// lane is used. Anonymous requires a free Zen-servable model; authenticated
// requires a Zen-served model. The legacy Go tier never yields a model.
func (g *Gateway) bulkProbeModel(tier Tier, public bool) (string, Protocol, bool) {
	if g == nil || g.catalog == nil {
		return "", "", false
	}
	if tier == TierGo {
		return "", "", false
	}
	for _, model := range g.catalog.List() {
		if public {
			if !g.catalog.anonymousDecision(model).Allowed {
				continue
			}
			route, err := g.catalog.Route(model, false, true)
			if err != nil || !route.Anonymous || route.Tier != TierZen {
				continue
			}
			return model, route.ProtocolFor(TierZen), true
		}
		route, err := g.catalog.Route(model, true, false)
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
	return "", "", false
}

// bulkProbeRequestBody builds minimal-inference bodies for custom fallback
// probes only. Custom probes reuse the unified canonical OpenCode wire
// identity through the shared custom request construction path while keeping
// scheduler/transport state isolation and minimal-inference semantics. Native
// Zen anonymous/authenticated probes use bulkProbeCanonicalBody plus
// newUpstreamRequest so they share the gateway preparation/header path.
// The configured channel reasoning effort is overlaid with the existing
// fallback helper: supplier-default ("") strips target strength and
// "inherit" preserves the converted strength, both no-ops on the minimal
// body that carries no strength; explicit low/medium/high inject the same
// target-protocol strength the normal fallback path sends. The target-bound
// custom route session is stamped via the shared applyRouteSessionToBody path
// so session-requiring compatible upstreams do not return a false 400.
func bulkProbeRequestBody(model string, protocol Protocol, effort string) ([]byte, error) {
	return bulkProbeRequestBodyWithSession(model, protocol, effort, "")
}

func bulkProbeRequestBodyWithSession(model string, protocol Protocol, effort string, routeSession string) ([]byte, error) {
	var payload map[string]any
	switch protocol {
	case ProtocolResponses:
		// Compatible minimal output limit: Zen-compatible Responses
		// upstreams reject max_output_tokens=1 with 400
		// (param=max_output_tokens) when the full OpenCode fingerprint is
		// present, while 16 returns 200. Session headers must stay intact
		// (removing them causes MissingSessionID), so only raise the limit.
		payload = map[string]any{"model": model, "input": "hi", "max_output_tokens": 16, "stream": false}
	case ProtocolAnthropic:
		payload = map[string]any{"model": model, "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "max_tokens": 1}
	default:
		payload = map[string]any{"model": model, "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "max_tokens": 1, "stream": false}
	}
	applyFallbackReasoningEffort(payload, effort, protocol)
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(routeSession) == "" {
		return encoded, nil
	}
	return applyRouteSessionToBody(encoded, routeSession, protocol, false)
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
	ID       string
	Name     string // display snapshot only
	BaseURL  string
	Model    string
	APIKey   string
	Protocol Protocol
	// Effort is the configured channel reasoning effort ("" / inherit /
	// low / medium / high), overlaid on the minimal probe body with the
	// same helper the normal fallback path uses.
	Effort string
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
	proto := fallbackChannelProtocol(FallbackChannelConfig{Protocol: tgt.Protocol})
	started := time.Now()
	startedNanos := started.UnixNano()
	if strings.TrimSpace(tgt.APIKey) == "" || strings.TrimSpace(tgt.Model) == "" {
		return bulkCustomResult{Target: tgt, StartedNanos: startedNanos, DurationMS: 0, Cancelled: parent.Err() != nil}
	}
	// Canonical stateless probe identity shared with native probes, bound to
	// the custom target scope. No scheduler reads/writes, no binding, no
	// product-level persistent affinity: each probe is one independent
	// minimal inference whose header/body carry the same pseudonymous triple.
	ids := bulkProbeIDs()
	scope := customRouteScopeFor(tgt.ID, tgt.BaseURL, tgt.APIKey, proto)
	routeSession := deriveFirstRouteSession(ids.Session, scope)
	body, err := bulkProbeRequestBodyWithSession(strings.TrimSpace(tgt.Model), proto, tgt.Effort, routeSession)
	if err != nil {
		return bulkCustomResult{Target: tgt, StartedNanos: startedNanos, Cancelled: parent.Err() != nil}
	}
	sendCtx, cancel := context.WithTimeout(parent, bulkPerSendTimeout)
	defer cancel()
	// Shared custom request construction (endpoint, Content-Type,
	// non-streaming Accept, canonical OpenCode wire headers, Bearer auth).
	// Probes write no binding/scheduler/health/metrics/history state.
	req, err := newCustomChannelRequest(sendCtx, tgt.BaseURL, proto, body, tgt.APIKey, false, ids, routeSession)
	if err != nil {
		return bulkCustomResult{Target: tgt, StartedNanos: startedNanos, DurationMS: max(time.Since(started).Milliseconds(), 0), TransportErr: err, Cancelled: parent.Err() != nil}
	}
	resp, err := g.fallbackCustomClient().Do(req)
	durationMS := max(time.Since(started).Milliseconds(), 0)
	if err != nil {
		return bulkCustomResult{Target: tgt, StartedNanos: startedNanos, DurationMS: durationMS, TransportErr: err, Cancelled: parent.Err() != nil}
	}
	defer resp.Body.Close()
	status := resp.StatusCode
	// Exactly HTTP 200 with a valid body is success; other HTTP statuses
	// (including other 2xx) remain their real outcomes.
	if status != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return bulkCustomResult{Target: tgt, StartedNanos: startedNanos, DurationMS: durationMS, Status: status}
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return bulkCustomResult{Target: tgt, StartedNanos: startedNanos, DurationMS: durationMS, Status: status, ParseError: true}
	}
	if !bulkProbeSuccessBody(proto, raw) {
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
