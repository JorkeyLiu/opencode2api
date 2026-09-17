package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"
)

type FallbackChannelView struct {
	ID              string     `json:"id"`
	Name            string     `json:"name"`
	BaseURL         string     `json:"base_url"`
	APIKey          SecretView `json:"api_key"`
	Model           string     `json:"model"`
	Protocol        Protocol   `json:"protocol"`
	ReasoningEffort string     `json:"reasoning_effort"`
}

type FallbackView struct {
	Active   string                `json:"active"`
	Channels []FallbackChannelView `json:"channels"`
}

type FallbackChannelInput struct {
	ID              string      `json:"id"`
	Name            string      `json:"name"`
	BaseURL         string      `json:"base_url"`
	APIKey          SecretInput `json:"api_key"`
	Model           string      `json:"model"`
	Protocol        Protocol    `json:"protocol"`
	ReasoningEffort string      `json:"reasoning_effort"`
}

type FallbackInput struct {
	Active   string                 `json:"active"`
	Channels []FallbackChannelInput `json:"channels"`
}

func fallbackViewFromConfig(cfg Config) FallbackView {
	view := FallbackView{Active: cfg.Fallback.Active, Channels: []FallbackChannelView{}}
	for _, ch := range cfg.Fallback.Channels {
		view.Channels = append(view.Channels, FallbackChannelView{
			ID:              ch.ID,
			Name:            ch.Name,
			BaseURL:         ch.BaseURL,
			APIKey:          SecretView{ID: secretFingerprint(ch.APIKey), Display: maskValue(ch.APIKey)},
			Model:           ch.Model,
			Protocol:        fallbackChannelProtocol(ch),
			ReasoningEffort: fallbackChannelEffort(ch),
		})
	}
	if view.Channels == nil {
		view.Channels = []FallbackChannelView{}
	}
	return view
}

func resolveFallbackInput(input FallbackInput, current FallbackConfig) (FallbackConfig, error) {
	known := map[string]string{}
	ambiguous := map[string]bool{}
	for _, ch := range current.Channels {
		fp := secretFingerprint(ch.APIKey)
		if ambiguous[fp] {
			continue
		}
		if prev, ok := known[fp]; ok {
			if prev != ch.APIKey {
				ambiguous[fp] = true
				delete(known, fp)
			}
			continue
		}
		known[fp] = ch.APIKey
	}
	// Masked-secret resolution is keyed by stable channel ID. Legacy inputs
	// without an ID derive it deterministically (same rule as config
	// normalization) so old name-only payloads keep resolving.
	sameID := map[string]map[string]string{}
	for _, ch := range current.Channels {
		id := strings.TrimSpace(ch.ID)
		if id == "" {
			id = fallbackDeriveChannelID(ch.Name)
		}
		if id == "" {
			continue
		}
		if _, ok := sameID[id]; !ok {
			sameID[id] = map[string]string{}
		}
		sameID[id][secretFingerprint(ch.APIKey)] = ch.APIKey
	}
	sameName := map[string]map[string]string{}
	for _, ch := range current.Channels {
		if _, ok := sameName[ch.Name]; !ok {
			sameName[ch.Name] = map[string]string{}
		}
		sameName[ch.Name][secretFingerprint(ch.APIKey)] = ch.APIKey
	}
	out := FallbackConfig{Active: strings.TrimSpace(input.Active), Channels: []FallbackChannelConfig{}}
	for _, ch := range input.Channels {
		id := strings.TrimSpace(ch.ID)
		name := strings.TrimSpace(ch.Name)
		base := strings.TrimSpace(ch.BaseURL)
		model := strings.TrimSpace(ch.Model)
		if id == "" {
			// Backward compat: legacy payloads without id derive
			// deterministically from the display name.
			id = fallbackDeriveChannelID(name)
		}
		if name == "" || base == "" || model == "" {
			return FallbackConfig{}, errors.New("fallback channels require name, base_url and model")
		}
		var key string
		switch {
		case strings.TrimSpace(ch.APIKey.Value) != "":
			key = strings.TrimSpace(ch.APIKey.Value)
		case strings.TrimSpace(ch.APIKey.ID) != "":
			sid := strings.TrimSpace(ch.APIKey.ID)
			if m, ok := sameID[id]; ok {
				if v, ok := m[sid]; ok {
					key = v
					break
				}
			}
			// Legacy fallback: same display-name scoping for old drafts.
			if m, ok := sameName[name]; ok {
				if v, ok := m[sid]; ok {
					key = v
					break
				}
			}
			if ambiguous[sid] {
				return FallbackConfig{}, errors.New("unknown or stale secret id")
			}
			v, ok := known[sid]
			if !ok {
				return FallbackConfig{}, errors.New("unknown or stale secret id")
			}
			key = v
		default:
			return FallbackConfig{}, errors.New("fallback channel api_key must contain id or value")
		}
		out.Channels = append(out.Channels, FallbackChannelConfig{ID: id, Name: name, BaseURL: base, APIKey: key, Model: model, Protocol: ch.Protocol, ReasoningEffort: ch.ReasoningEffort})
	}
	if err := validateFallbackConfig(&out); err != nil {
		return FallbackConfig{}, err
	}
	return out, nil
}

const fallbackDiscoverRateLimit = 10

func (a *AdminServer) allowFallbackDiscover(client string) bool {
	return a.allowWindow(&a.fallbackAttempts, client, fallbackDiscoverRateLimit, rateLimitWindowMinute)
}

type fallbackDiscoverRequest struct {
	BaseURL string      `json:"base_url"`
	APIKey  SecretInput `json:"api_key"`
	Channel string      `json:"channel"`
}

type fallbackDiscoverResponse struct {
	Models []string `json:"models"`
}

func (a *AdminServer) logFallbackDiscover(channel, base, result string, success bool) {
	a.logFallbackDiscoverDetailed(channel, base, result, "", 0, 0, success)
}

// logFallbackDiscoverDetailed records the redacted discovery audit: channel
// name, redacted endpoint/base, result enum, safe reason, HTTP status, and
// elapsed_ms. It never logs key material, Authorization values, request
// bodies, upstream body text, or raw transport errors.
func (a *AdminServer) logFallbackDiscoverDetailed(channel, base, result, reason string, httpStatus int, elapsedMS int64, success bool) {
	if a == nil || a.logger == nil {
		return
	}
	fields := []any{
		"component", "fallback", "event", "fallback_discover_completed",
		"channel", channel, "base_url", redactURL(base), "result", result,
	}
	if reason != "" {
		fields = append(fields, "reason", reason)
	}
	if httpStatus != 0 {
		fields = append(fields, "http_status", httpStatus)
	}
	if elapsedMS != 0 {
		fields = append(fields, "elapsed_ms", elapsedMS)
	}
	if success {
		a.logger.Info("fallback model discovery completed", fields...)
	} else {
		a.logger.Warn("fallback model discovery completed", fields...)
	}
}

// writeFallbackDiscoverError keeps code discover_failed for compatibility and
// adds the safe structured fields reason/endpoint/http_status/elapsed_ms.
// Endpoint is already redacted; message stays generic and never echoes key
// material, upstream body, or raw errors.
func writeFallbackDiscoverError(w http.ResponseWriter, detail *fallbackDiscoverError) {
	endpoint := ""
	reason := fallbackDiscoverTransport
	var httpStatus int
	var elapsedMS int64
	if detail != nil {
		endpoint = detail.Endpoint
		if detail.Reason != "" {
			reason = detail.Reason
		}
		httpStatus = detail.HTTPStatus
		elapsedMS = detail.ElapsedMS
	}
	payload := map[string]any{"code": "discover_failed", "message": "model discovery failed", "reason": reason, "endpoint": endpoint, "elapsed_ms": elapsedMS}
	if httpStatus != 0 {
		payload["http_status"] = httpStatus
	}
	writeJSON(w, http.StatusBadGateway, map[string]any{"error": payload})
}

func (a *AdminServer) handleFallbackDiscover(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	client := clientIP(r)
	if !a.allowFallbackDiscover(client) {
		writeAdminError(w, http.StatusTooManyRequests, "discover_rate_limited", "too many model discovery requests; retry in one minute")
		return
	}
	var input fallbackDiscoverRequest
	if err := decodeAdminJSON(w, r, &input); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cfg := a.manager.Config()
	baseInput := strings.TrimSpace(input.BaseURL)
	value := strings.TrimSpace(input.APIKey.Value)
	id := strings.TrimSpace(input.APIKey.ID)
	channelRef := strings.TrimSpace(input.Channel)
	base := ""
	key := ""
	channelName := channelRef
	if channelRef != "" {
		// Channel reference is the stable ID; legacy display names still
		// resolve for backward compatibility.
		ch, ok := fallbackChannelLookup(cfg, channelRef)
		if !ok {
			a.logFallbackDiscover(channelRef, baseInput, "unknown_channel", false)
			writeAdminError(w, http.StatusBadRequest, "unknown_channel", "fallback channel does not exist")
			return
		}
		channelName = ch.Name
		savedBaseNorm := normalizeFallbackBaseURL(ch.BaseURL)
		if baseInput == "" {
			base = ch.BaseURL
		} else {
			base = baseInput
		}
		if err := validateFallbackBaseURL(base); err != nil {
			a.logFallbackDiscover(channelName, base, "invalid_base_url", false)
			writeAdminError(w, http.StatusBadRequest, "invalid_base_url", err.Error())
			return
		}
		effBaseNorm := normalizeFallbackBaseURL(base)
		switch {
		case value != "":
			// Explicit caller-supplied key: allowed for any base (no
			// stored-key disclosure).
			key = value
		case id != "":
			// Saved-key reuse only when the effective base equals the
			// saved channel's normalized base AND the id matches this
			// channel's saved key. Base changes, cross-channel ids, and
			// unknown/stale ids all require an explicit value.
			if effBaseNorm != savedBaseNorm {
				a.logFallbackDiscover(channelName, base, "base_mismatch_requires_value", false)
				writeAdminError(w, http.StatusBadRequest, "invalid_request", "api_key value is required when base_url differs from the saved channel")
				return
			}
			if secretFingerprint(ch.APIKey) != id {
				a.logFallbackDiscover(channelName, base, "invalid_api_key", false)
				writeAdminError(w, http.StatusBadRequest, "invalid_api_key", "unknown or stale secret id")
				return
			}
			key = ch.APIKey
		default:
			// Omitted api_key: only a saved-channel refresh on the same
			// normalized base may reuse the stored key.
			if effBaseNorm != savedBaseNorm {
				a.logFallbackDiscover(channelName, base, "base_mismatch_requires_value", false)
				writeAdminError(w, http.StatusBadRequest, "invalid_request", "api_key value is required when base_url differs from the saved channel")
				return
			}
			key = ch.APIKey
		}
	} else {
		// No channel: stored keys are never eligible. Only an explicit
		// value may be sent to an ad-hoc base URL.
		if baseInput == "" {
			writeAdminError(w, http.StatusBadRequest, "invalid_request", "base_url or channel is required")
			return
		}
		base = baseInput
		if err := validateFallbackBaseURL(base); err != nil {
			a.logFallbackDiscover("", base, "invalid_base_url", false)
			writeAdminError(w, http.StatusBadRequest, "invalid_base_url", err.Error())
			return
		}
		if value == "" {
			if id != "" {
				a.logFallbackDiscover("", base, "invalid_api_key", false)
				writeAdminError(w, http.StatusBadRequest, "invalid_api_key", "api_key value is required for a new base_url")
			} else {
				a.logFallbackDiscover("", base, "missing_api_key", false)
				writeAdminError(w, http.StatusBadRequest, "invalid_request", "api_key value or id is required for a new base_url")
			}
			return
		}
		key = value
	}
	if key == "" {
		a.logFallbackDiscover(channelName, base, "invalid_api_key", false)
		writeAdminError(w, http.StatusBadRequest, "invalid_api_key", "api_key must not be empty")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
	defer cancel()
	models, err := fetchFallbackModels(ctx, nil, base, key)
	if err != nil {
		// Never leak key material or upstream body text.
		if detail, ok := asFallbackDiscoverError(err); ok {
			a.logFallbackDiscoverDetailed(channelName, detail.Endpoint, "discover_failed", detail.Reason, detail.HTTPStatus, detail.ElapsedMS, false)
			writeFallbackDiscoverError(w, detail)
			return
		}
		a.logFallbackDiscover(channelName, base, "discover_failed", false)
		writeAdminError(w, http.StatusBadGateway, "discover_failed", "model discovery failed")
		return
	}
	a.logFallbackDiscover(channelName, base, "success", true)
	writeJSON(w, http.StatusOK, fallbackDiscoverResponse{Models: models})
}
