package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

var allowedConfigKeys = map[string]bool{
	"listen": true, "server_keys": true, "keys": true, "zen_keys": true, "go_keys": true,
	"anonymous": true, "proxies": true, "proxyfile": true,
	"proxy_pools": true, "proxy_routing": true,
	"upstream": true, "retry": true, "models": true, "performance": true,
	"logging": true, "webui": true, "prefer": true, "history": true,
	"fallback": true,
}

type ProxyPoolConfig struct {
	Proxies   []string `json:"proxies"`
	ProxyFile string   `json:"proxyfile"`
	effective []string
}

type ProxyRoutingConfig struct {
	Anonymous     string `json:"anonymous"`
	Authenticated string `json:"authenticated"`
	// Legacy compat inputs only: accepted on load, never emitted.
	Zen string `json:"zen,omitempty"`
	Go  string `json:"go,omitempty"`
}

type Config struct {
	Listen       string                     `json:"listen"`
	ServerKeys   []string                   `json:"server_keys"`
	Keys         []string                   `json:"keys"`
	ZenKeys      []string                   `json:"zen_keys,omitempty"`
	GoKeys       []string                   `json:"go_keys,omitempty"`
	Anonymous    bool                       `json:"anonymous"`
	ProxyPools   map[string]ProxyPoolConfig `json:"proxy_pools"`
	ProxyRouting ProxyRoutingConfig         `json:"proxy_routing"`
	Fallback     FallbackConfig             `json:"fallback"`
	Upstream     UpstreamConfig             `json:"upstream"`
	Retry        RetryConfig                `json:"retry"`
	Models       ModelsConfig               `json:"models"`
	Performance  PerformanceConfig          `json:"performance"`
	Logging      LoggingConfig              `json:"logging"`
	WebUI        WebUIConfig                `json:"webui"`
	Prefer       Tier                       `json:"prefer,omitempty"`
	History      HistoryConfig              `json:"history"`
	Proxies      []string                   `json:"proxies,omitempty"`
	ProxyFile    string                     `json:"proxyfile,omitempty"`

	effectivePools         map[string][]string
	legacyProxiesPresent   bool
	legacyProxyFilePresent bool
	proxyPoolsPresent      bool
	proxyRoutingPresent    bool
}

type UpstreamConfig struct {
	Zen string `json:"zen"`
	// Legacy compat input only: accepted on load, ignored at runtime, never emitted.
	Go string `json:"go,omitempty"`
}

type RetryConfig struct {
	MaxAttempts                   int `json:"max_attempts"`
	TimeoutSeconds                int `json:"timeout_seconds"`
	TransientMaxAttempts          int `json:"transient_max_attempts"`
	TransientRetryIntervalSeconds int `json:"transient_retry_interval_seconds"`
}

type ModelsConfig struct {
	RefreshSeconds int               `json:"refresh_seconds"`
	Protocols      map[string]string `json:"protocols"`
}

type LoggingConfig struct {
	Level    string `json:"level"`
	RingSize int    `json:"ring_size"`
}

type WebUIConfig struct {
	Enabled           bool   `json:"enabled"`
	Listen            string `json:"listen"`
	Username          string `json:"username"`
	Password          string `json:"password,omitempty"`
	PasswordHash      string `json:"password_hash,omitempty"`
	SessionTTLMinutes int    `json:"session_ttl_minutes"`
}

type PerformanceConfig struct {
	MaxIdleConns             int `json:"max_idle_conns"`
	MaxIdleConnsPerHost      int `json:"max_idle_conns_per_host"`
	MaxConnsPerHost          int `json:"max_conns_per_host"`
	IdleConnTimeoutSeconds   int `json:"idle_conn_timeout_seconds"`
	ConnectTimeoutSeconds    int `json:"connect_timeout_seconds"`
	FailureCooldownSeconds   int `json:"failure_cooldown_seconds"`
	RateLimitCooldownSeconds int `json:"rate_limit_cooldown_seconds"`
	// Legacy compat input only: accepted on load, ignored at runtime, never emitted.
	// The 429 maximum is the fixed 3600s backoff cap.
	RateLimitCooldownMaxSeconds int `json:"rate_limit_cooldown_max_seconds,omitempty"`
}

// HistoryConfig is a bounded, redacted, embedded projection of recent
// inference metadata. It never affects routing, readiness, or inference.
type HistoryConfig struct {
	Enabled       bool   `json:"enabled"`
	Directory     string `json:"directory"`
	RetentionDays int    `json:"retention_days"`
	MaxBytesMB    int    `json:"max_bytes_mb"`
}

func defaultConfig() Config {
	return Config{
		Listen:      "127.0.0.1:8080",
		Upstream:    UpstreamConfig{Zen: "https://opencode.ai/zen"},
		Retry:       RetryConfig{MaxAttempts: 3, TimeoutSeconds: 300, TransientMaxAttempts: 3, TransientRetryIntervalSeconds: 3},
		Models:      ModelsConfig{RefreshSeconds: 300, Protocols: map[string]string{}},
		Performance: PerformanceConfig{MaxIdleConns: 2048, MaxIdleConnsPerHost: 256, MaxConnsPerHost: 0, IdleConnTimeoutSeconds: 120, ConnectTimeoutSeconds: 5, FailureCooldownSeconds: 15, RateLimitCooldownSeconds: 300},
		Logging:     LoggingConfig{Level: "info", RingSize: 2000},
		WebUI:       WebUIConfig{Listen: "0.0.0.0:8081", SessionTTLMinutes: 720},
		History:     HistoryConfig{Enabled: true, Directory: "", RetentionDays: 7, MaxBytesMB: 128},
	}
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read %s: %w", path, err)
	}
	data, err = stripJSONComments(data)
	if err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}
	cfg := defaultConfig()
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := ensureJSONEOF(dec); err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return NormalizeConfig(path, cfg)
}

// MarshalJSON emits only the canonical shape: keys, anonymous+authenticated
// routing, Zen upstream only, and the single 429 base. Legacy inputs
// (zen_keys/go_keys, prefer, proxy_routing.zen/go, upstream.go,
// performance.rate_limit_cooldown_max_seconds, top-level proxies/proxyfile)
// are load-time compat and never persist.
func (cfg Config) MarshalJSON() ([]byte, error) {
	type diskRouting struct {
		Anonymous     string `json:"anonymous"`
		Authenticated string `json:"authenticated"`
	}
	type diskUpstream struct {
		Zen string `json:"zen"`
	}
	type diskPerformance struct {
		MaxIdleConns             int `json:"max_idle_conns"`
		MaxIdleConnsPerHost      int `json:"max_idle_conns_per_host"`
		MaxConnsPerHost          int `json:"max_conns_per_host"`
		IdleConnTimeoutSeconds   int `json:"idle_conn_timeout_seconds"`
		ConnectTimeoutSeconds    int `json:"connect_timeout_seconds"`
		FailureCooldownSeconds   int `json:"failure_cooldown_seconds"`
		RateLimitCooldownSeconds int `json:"rate_limit_cooldown_seconds"`
	}
	type diskConfig struct {
		Listen       string                     `json:"listen"`
		ServerKeys   []string                   `json:"server_keys"`
		Keys         []string                   `json:"keys"`
		Anonymous    bool                       `json:"anonymous"`
		ProxyPools   map[string]ProxyPoolConfig `json:"proxy_pools"`
		ProxyRouting diskRouting                `json:"proxy_routing"`
		Fallback     FallbackConfig             `json:"fallback"`
		Upstream     diskUpstream               `json:"upstream"`
		Retry        RetryConfig                `json:"retry"`
		Models       ModelsConfig               `json:"models"`
		Performance  diskPerformance            `json:"performance"`
		Logging      LoggingConfig              `json:"logging"`
		WebUI        WebUIConfig                `json:"webui"`
		History      HistoryConfig              `json:"history"`
	}
	pools := cfg.ProxyPools
	if pools == nil {
		pools = map[string]ProxyPoolConfig{}
	}
	fb := cfg.Fallback
	if fb.Channels == nil {
		fb.Channels = []FallbackChannelConfig{}
	}
	keys := cfg.Keys
	if keys == nil {
		keys = []string{}
	}
	return json.Marshal(diskConfig{
		Listen: cfg.Listen, ServerKeys: cfg.ServerKeys, Keys: keys,
		Anonymous: cfg.Anonymous, ProxyPools: pools,
		ProxyRouting: diskRouting{Anonymous: cfg.ProxyRouting.Anonymous, Authenticated: cfg.ProxyRouting.Authenticated},
		Fallback:     fb,
		Upstream:     diskUpstream{Zen: cfg.Upstream.Zen},
		Retry:        cfg.Retry, Models: cfg.Models,
		Performance: diskPerformance{
			MaxIdleConns: cfg.Performance.MaxIdleConns, MaxIdleConnsPerHost: cfg.Performance.MaxIdleConnsPerHost,
			MaxConnsPerHost: cfg.Performance.MaxConnsPerHost, IdleConnTimeoutSeconds: cfg.Performance.IdleConnTimeoutSeconds,
			ConnectTimeoutSeconds: cfg.Performance.ConnectTimeoutSeconds, FailureCooldownSeconds: cfg.Performance.FailureCooldownSeconds,
			RateLimitCooldownSeconds: cfg.Performance.RateLimitCooldownSeconds,
		},
		Logging: cfg.Logging, WebUI: cfg.WebUI, History: cfg.History,
	})
}

// UnmarshalJSON enforces strict unknown-field rejection while recording
// whether legacy or new proxy fields were explicitly present, so
// legacy+new conflicts are reported instead of silently prioritized.
func (cfg *Config) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	for key := range raw {
		if !allowedConfigKeys[key] {
			return fmt.Errorf("json: unknown field %q", key)
		}
	}
	def := defaultConfig()
	*cfg = def
	cfg.legacyProxiesPresent = hasKey(raw, "proxies")
	cfg.legacyProxyFilePresent = hasKey(raw, "proxyfile")
	cfg.proxyPoolsPresent = hasKey(raw, "proxy_pools")
	cfg.proxyRoutingPresent = hasKey(raw, "proxy_routing")
	decodeStrict := func(key string, target any) error {
		rawValue, ok := raw[key]
		if !ok {
			return nil
		}
		dec := json.NewDecoder(bytes.NewReader(rawValue))
		dec.DisallowUnknownFields()
		if err := dec.Decode(target); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		if err := ensureJSONEOF(dec); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		return nil
	}
	if err := decodeStrict("listen", &cfg.Listen); err != nil {
		return err
	}
	if err := decodeStrict("server_keys", &cfg.ServerKeys); err != nil {
		return err
	}
	if err := decodeStrict("keys", &cfg.Keys); err != nil {
		return err
	}
	if err := decodeStrict("zen_keys", &cfg.ZenKeys); err != nil {
		return err
	}
	if err := decodeStrict("go_keys", &cfg.GoKeys); err != nil {
		return err
	}
	if err := decodeStrict("anonymous", &cfg.Anonymous); err != nil {
		return err
	}
	if err := decodeStrict("proxies", &cfg.Proxies); err != nil {
		return err
	}
	if err := decodeStrict("proxyfile", &cfg.ProxyFile); err != nil {
		return err
	}
	if err := decodeStrict("proxy_pools", &cfg.ProxyPools); err != nil {
		return err
	}
	if err := decodeStrict("proxy_routing", &cfg.ProxyRouting); err != nil {
		return err
	}
	if err := decodeStrict("fallback", &cfg.Fallback); err != nil {
		return err
	}
	if err := decodeStrict("upstream", &cfg.Upstream); err != nil {
		return err
	}
	if err := decodeStrict("retry", &cfg.Retry); err != nil {
		return err
	}
	if err := decodeStrict("models", &cfg.Models); err != nil {
		return err
	}
	if err := decodeStrict("performance", &cfg.Performance); err != nil {
		return err
	}
	if err := decodeStrict("logging", &cfg.Logging); err != nil {
		return err
	}
	if err := decodeStrict("webui", &cfg.WebUI); err != nil {
		return err
	}
	if err := decodeStrict("prefer", &cfg.Prefer); err != nil {
		return err
	}
	if err := decodeStrict("history", &cfg.History); err != nil {
		return err
	}
	return nil
}

func hasKey(raw map[string]json.RawMessage, key string) bool {
	_, ok := raw[key]
	return ok
}

// NormalizeConfig resolves external inputs and validates a Config supplied by
// either the JSON file or the authenticated management API.
// Canonical keys live in Keys; zen_keys/go_keys merge in as one-time compat.
// Canonical routing is anonymous+authenticated; proxy_routing.zen (then go)
// maps to authenticated as compat. Upstream uses zen only; upstream.go and
// prefer are accepted but ignored.
func NormalizeConfig(path string, cfg Config) (Config, error) {
	trimList(&cfg.ServerKeys)
	trimList(&cfg.Keys)
	trimList(&cfg.ZenKeys)
	trimList(&cfg.GoKeys)
	cfg.Keys = mergeKeys(cfg.Keys, cfg.ZenKeys, cfg.GoKeys)
	cfg.ZenKeys = nil
	cfg.GoKeys = nil
	// Prefer is a legacy compat input: accepted, ignored at runtime, never emitted.
	cfg.Prefer = ""
	legacyExplicit := cfg.legacyProxiesPresent || cfg.legacyProxyFilePresent || len(cfg.Proxies) > 0 || strings.TrimSpace(cfg.ProxyFile) != ""
	newExplicit := cfg.proxyPoolsPresent || cfg.proxyRoutingPresent || len(cfg.ProxyPools) > 0 ||
		strings.TrimSpace(cfg.ProxyRouting.Anonymous) != "" || strings.TrimSpace(cfg.ProxyRouting.Authenticated) != "" || strings.TrimSpace(cfg.ProxyRouting.Zen) != "" || strings.TrimSpace(cfg.ProxyRouting.Go) != ""
	if legacyExplicit && newExplicit {
		return Config{}, errors.New("proxies/proxyfile (legacy) cannot be combined with proxy_pools/proxy_routing; remove the legacy fields to use named pools")
	}
	switch {
	case legacyExplicit:
		trimList(&cfg.Proxies)
		cfg.ProxyFile = strings.TrimSpace(cfg.ProxyFile)
		effective, err := resolvePoolProxies(path, cfg.Proxies, cfg.ProxyFile)
		if err != nil {
			return Config{}, err
		}
		if cfg.ProxyPools == nil {
			cfg.ProxyPools = map[string]ProxyPoolConfig{}
		}
		cfg.ProxyPools["shared"] = ProxyPoolConfig{Proxies: append([]string(nil), cfg.Proxies...), ProxyFile: cfg.ProxyFile, effective: effective}
		cfg.ProxyRouting = ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"}
		cfg.Proxies = nil
		cfg.ProxyFile = ""
		cfg.legacyProxiesPresent = false
		cfg.legacyProxyFilePresent = false
		cfg.proxyPoolsPresent = true
		cfg.proxyRoutingPresent = true
	case !newExplicit:
		cfg.ProxyPools = map[string]ProxyPoolConfig{
			"shared": {Proxies: []string{"direct"}, ProxyFile: "", effective: []string{"direct"}},
		}
		cfg.ProxyRouting = ProxyRoutingConfig{Anonymous: "shared", Authenticated: "shared"}
		cfg.Proxies = nil
		cfg.ProxyFile = ""
		cfg.proxyPoolsPresent = true
		cfg.proxyRoutingPresent = true
	default:
		// New-only: legacy inputs must already be absent.
		cfg.Proxies = nil
		cfg.ProxyFile = ""
		cfg.legacyProxiesPresent = false
		cfg.legacyProxyFilePresent = false
		// Map authenticated pool from legacy zen when present, else legacy go.
		if strings.TrimSpace(cfg.ProxyRouting.Authenticated) == "" {
			if strings.TrimSpace(cfg.ProxyRouting.Zen) != "" {
				cfg.ProxyRouting.Authenticated = strings.TrimSpace(cfg.ProxyRouting.Zen)
			} else if strings.TrimSpace(cfg.ProxyRouting.Go) != "" {
				cfg.ProxyRouting.Authenticated = strings.TrimSpace(cfg.ProxyRouting.Go)
			}
		}
		cfg.ProxyRouting.Zen = ""
		cfg.ProxyRouting.Go = ""
	}
	if err := normalizeNamedPools(path, &cfg); err != nil {
		return Config{}, err
	}
	if cfg.Listen == "" {
		return Config{}, errors.New("listen must not be empty")
	}
	cfg.Upstream.Zen = strings.TrimSpace(cfg.Upstream.Zen)
	cfg.Upstream.Go = strings.TrimSpace(cfg.Upstream.Go)
	{
		u, err := url.Parse(strings.TrimSpace(cfg.Upstream.Zen))
		if err != nil || u.Host == "" || (strings.ToLower(u.Scheme) != "http" && strings.ToLower(u.Scheme) != "https") {
			return Config{}, fmt.Errorf("upstream.zen must be an http or https URL")
		}
	}
	// Legacy upstream.go is ignored for runtime.
	cfg.Upstream.Go = ""
	if len(cfg.ServerKeys) == 0 {
		return Config{}, errors.New("server_keys must contain at least one local key")
	}
	if !cfg.Anonymous && len(cfg.Keys) == 0 {
		return Config{}, errors.New("keys must contain at least one upstream key unless anonymous is enabled")
	}
	if cfg.Retry.MaxAttempts < 1 {
		return Config{}, errors.New("retry.max_attempts must be at least 1")
	}
	if cfg.Retry.TimeoutSeconds < 1 {
		return Config{}, errors.New("retry.timeout_seconds must be at least 1")
	}
	if cfg.Retry.TransientMaxAttempts < 1 || cfg.Retry.TransientMaxAttempts > 10 {
		return Config{}, errors.New("retry.transient_max_attempts must be between 1 and 10")
	}
	if cfg.Retry.TransientRetryIntervalSeconds < 0 || cfg.Retry.TransientRetryIntervalSeconds > 30 {
		return Config{}, errors.New("retry.transient_retry_interval_seconds must be between 0 and 30")
	}
	if cfg.Models.RefreshSeconds < 1 {
		return Config{}, errors.New("models.refresh_seconds must be at least 1")
	}
	if cfg.Performance.MaxIdleConns < 1 || cfg.Performance.MaxIdleConnsPerHost < 1 || cfg.Performance.MaxConnsPerHost < 0 || cfg.Performance.IdleConnTimeoutSeconds < 1 || cfg.Performance.ConnectTimeoutSeconds < 1 || cfg.Performance.FailureCooldownSeconds < 1 {
		return Config{}, errors.New("performance values must be positive (max_conns_per_host may be zero for unlimited)")
	}
	if cfg.Performance.RateLimitCooldownSeconds == 0 {
		cfg.Performance.RateLimitCooldownSeconds = 300
	}
	// Legacy max is accepted as compat input but ignored; the 429 maximum is
	// the fixed 3600s backoff cap.
	cfg.Performance.RateLimitCooldownMaxSeconds = 0
	if cfg.Performance.RateLimitCooldownSeconds < 300 || cfg.Performance.RateLimitCooldownSeconds > 3600 {
		return Config{}, errors.New("performance.rate_limit_cooldown_seconds must be between 300 and 3600")
	}
	if cfg.Logging.Level != "debug" && cfg.Logging.Level != "info" && cfg.Logging.Level != "warn" && cfg.Logging.Level != "error" {
		return Config{}, errors.New("logging.level must be debug, info, warn, or error")
	}
	if cfg.Logging.RingSize < 100 || cfg.Logging.RingSize > 50000 {
		return Config{}, errors.New("logging.ring_size must be between 100 and 50000")
	}
	if cfg.WebUI.Password != "" && len(cfg.WebUI.Password) < 10 {
		return Config{}, errors.New("webui.password must contain at least 10 characters")
	}
	if cfg.WebUI.Enabled {
		cfg.WebUI.Listen = strings.TrimSpace(cfg.WebUI.Listen)
		cfg.WebUI.Username = strings.TrimSpace(cfg.WebUI.Username)
		if cfg.WebUI.Listen == "" {
			return Config{}, errors.New("webui.listen must not be empty when webui is enabled")
		}
		if cfg.WebUI.Username == "" {
			return Config{}, errors.New("webui.username must not be empty when webui is enabled")
		}
		if cfg.WebUI.Password == "" && cfg.WebUI.PasswordHash == "" {
			return Config{}, errors.New("webui.password is required for first-time setup")
		}
		if cfg.WebUI.SessionTTLMinutes < 5 || cfg.WebUI.SessionTTLMinutes > 10080 {
			return Config{}, errors.New("webui.session_ttl_minutes must be between 5 and 10080")
		}
	}
	for model, protocol := range cfg.Models.Protocols {
		if model == "" || !validProtocol(Protocol(protocol)) {
			return Config{}, fmt.Errorf("models.protocols contains invalid mapping %q: %q", model, protocol)
		}
	}
	if err := validateFallbackConfig(&cfg.Fallback); err != nil {
		return Config{}, err
	}
	if cfg.History.RetentionDays < 1 || cfg.History.RetentionDays > 90 {
		return Config{}, errors.New("history.retention_days must be between 1 and 90")
	}
	if cfg.History.MaxBytesMB < 16 || cfg.History.MaxBytesMB > 2048 {
		return Config{}, errors.New("history.max_bytes_mb must be between 16 and 2048")
	}
	cfg.History.Directory = strings.TrimSpace(cfg.History.Directory)
	return cfg, nil
}

// normalizeNamedPools validates pool names, resolves each pool's
// proxies+proxyfile with the legacy trim/relative/comment/dedup/direct/scheme
// semantics, and validates that every routing reference exists.
// Unreferenced pools are fully validated but need not be built at runtime.
func normalizeNamedPools(path string, cfg *Config) error {
	if len(cfg.ProxyPools) == 0 {
		return errors.New("proxy_pools must contain at least one pool")
	}
	for name := range cfg.ProxyPools {
		if err := validatePoolName(name); err != nil {
			return err
		}
	}
	routing := &cfg.ProxyRouting
	routing.Anonymous = strings.TrimSpace(routing.Anonymous)
	routing.Authenticated = strings.TrimSpace(routing.Authenticated)
	routing.Zen = strings.TrimSpace(routing.Zen)
	routing.Go = strings.TrimSpace(routing.Go)
	// Legacy zen/go were already mapped to authenticated before this call for
	// the default path; tolerate stray legacy values by mapping them here as
	// well so old in-memory configs still normalize.
	if routing.Authenticated == "" {
		if routing.Zen != "" {
			routing.Authenticated = routing.Zen
		} else if routing.Go != "" {
			routing.Authenticated = routing.Go
		}
	}
	routing.Zen = ""
	routing.Go = ""
	if routing.Anonymous == "" || routing.Authenticated == "" {
		return errors.New("proxy_routing.anonymous and proxy_routing.authenticated must each reference an existing pool")
	}
	for _, ref := range []struct {
		field string
		name  string
	}{{field: "proxy_routing.anonymous", name: routing.Anonymous}, {field: "proxy_routing.authenticated", name: routing.Authenticated}} {
		if _, ok := cfg.ProxyPools[ref.name]; !ok {
			return fmt.Errorf("%s references unknown pool %q", ref.field, ref.name)
		}
	}
	if cfg.effectivePools == nil {
		cfg.effectivePools = map[string][]string{}
	}
	for name, pool := range cfg.ProxyPools {
		proxies := append([]string(nil), pool.Proxies...)
		trimList(&proxies)
		proxyfile := strings.TrimSpace(pool.ProxyFile)
		effective, err := resolvePoolProxies(path, proxies, proxyfile)
		if err != nil {
			return fmt.Errorf("proxy_pools[%q]: %w", name, err)
		}
		for _, raw := range effective {
			if err := validateProxyURL(raw); err != nil {
				return fmt.Errorf("proxy_pools[%q]: %w", name, err)
			}
		}
		cfg.ProxyPools[name] = ProxyPoolConfig{Proxies: proxies, ProxyFile: proxyfile, effective: effective}
		cfg.effectivePools[name] = effective
	}
	return nil
}

func validatePoolName(name string) error {
	if name == "" {
		return errors.New("proxy pool name must not be empty")
	}
	if len(name) > 64 {
		return fmt.Errorf("invalid proxy pool name %q: must be 1-64 characters", name)
	}
	if strings.EqualFold(name, "direct") {
		return fmt.Errorf("invalid proxy pool name %q: reserved word", name)
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
			continue
		}
		return fmt.Errorf("invalid proxy pool name %q: use letters, digits, '_', '-' or '.'", name)
	}
	return nil
}

// remapProxyRoutingForRename maps routing references from an old pool name to
// a new one. It mirrors the WebUI rename behavior and is kept as a pure,
// unit-testable helper.
func remapProxyRoutingForRename(routing ProxyRoutingConfig, oldName, newName string) ProxyRoutingConfig {
	if routing.Anonymous == oldName {
		routing.Anonymous = newName
	}
	if routing.Authenticated == oldName {
		routing.Authenticated = newName
	}
	if routing.Zen == oldName {
		routing.Zen = newName
	}
	if routing.Go == oldName {
		routing.Go = newName
	}
	return routing
}

func validateProxyURL(raw string) error {
	if raw == "direct" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("invalid proxy URL %q", redactURL(raw))
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https", "socks5", "socks5h":
		return nil
	default:
		return fmt.Errorf("unsupported proxy scheme %q", u.Scheme)
	}
}

// RuntimeProxiesFor returns the resolved proxies (config + proxyfile,
// deduped, direct default) for one named pool.
func (cfg Config) RuntimeProxiesFor(pool string) []string {
	if cfg.effectivePools != nil {
		if effective, ok := cfg.effectivePools[pool]; ok && len(effective) > 0 {
			return effective
		}
	}
	if cfg.ProxyPools != nil {
		if entry, ok := cfg.ProxyPools[pool]; ok {
			if len(entry.effective) > 0 {
				return entry.effective
			}
			if len(entry.Proxies) > 0 {
				return entry.Proxies
			}
		}
	}
	return []string{"direct"}
}

// RuntimeProxies preserves the legacy single-pool accessor for shared-pool
// callers that have not migrated yet. New code should use RuntimeProxiesFor.
func (cfg Config) RuntimeProxies() []string {
	if name := cfg.ProxyRouting.Authenticated; name != "" {
		return cfg.RuntimeProxiesFor(name)
	}
	if name := cfg.ProxyRouting.Zen; name != "" {
		return cfg.RuntimeProxiesFor(name)
	}
	return cfg.RuntimeProxiesFor("shared")
}

// ReferencedPools returns routing references in anonymous/authenticated order.
func (cfg Config) ReferencedPools() []string {
	return []string{cfg.ProxyRouting.Anonymous, cfg.ProxyRouting.Authenticated}
}

// UniqueActivePools returns deduplicated referenced pool names in first-use
// order (anonymous, authenticated), skipping empty references.
func (cfg Config) UniqueActivePools() []string {
	seen := map[string]bool{}
	out := []string{}
	for _, name := range cfg.ReferencedPools() {
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	return out
}

func ensureJSONEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

// stripJSONComments removes // and /* */ comments without changing newlines,
// so syntax errors still point at the correct line in config.json. Comment
// markers inside JSON strings (for example, https:// URLs) are preserved.
func stripJSONComments(data []byte) ([]byte, error) {
	data = bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf})
	out := make([]byte, 0, len(data))
	inString := false
	escaped := false
	lineComment := false
	blockComment := false

	for i := 0; i < len(data); i++ {
		current := data[i]
		if lineComment {
			if current == '\n' || current == '\r' {
				lineComment = false
				out = append(out, current)
			} else {
				out = append(out, ' ')
			}
			continue
		}
		if blockComment {
			if current == '*' && i+1 < len(data) && data[i+1] == '/' {
				out = append(out, ' ', ' ')
				i++
				blockComment = false
			} else if current == '\n' || current == '\r' {
				out = append(out, current)
			} else {
				out = append(out, ' ')
			}
			continue
		}
		if inString {
			out = append(out, current)
			if escaped {
				escaped = false
			} else if current == '\\' {
				escaped = true
			} else if current == '"' {
				inString = false
			}
			continue
		}

		switch {
		case current == '"':
			inString = true
			out = append(out, current)
		case current == '/' && i+1 < len(data) && data[i+1] == '/':
			lineComment = true
			out = append(out, ' ', ' ')
			i++
		case current == '/' && i+1 < len(data) && data[i+1] == '*':
			blockComment = true
			out = append(out, ' ', ' ')
			i++
		default:
			out = append(out, current)
		}
	}
	if blockComment {
		return nil, errors.New("unterminated block comment")
	}
	return out, nil
}

func resolvePoolProxies(configPath string, proxies []string, proxyfile string) ([]string, error) {
	effective := append([]string(nil), proxies...)
	if proxyfile != "" {
		resolved := proxyfile
		if !filepath.IsAbs(resolved) {
			resolved = filepath.Join(filepath.Dir(configPath), resolved)
		}
		loaded, err := readProxyFile(resolved)
		if err != nil {
			return nil, fmt.Errorf("load proxy file %s: %w", resolved, err)
		}
		effective = append(effective, loaded...)
	}
	effective = uniqueStrings(effective)
	if len(effective) == 0 {
		effective = []string{"direct"}
	}
	return effective, nil
}

// SaveConfigAtomic writes normalized JSON and keeps the preceding file as
// config.json.bak. The temporary file is created beside the target so the
// final rename stays on the same filesystem.
func SaveConfigAtomic(path string, cfg Config) error {
	cfg.PasswordForSave()
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("encode config: %w", err)
	}
	data = append(data, '\n')
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, ".config-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary config: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if _, err = temp.Write(data); err == nil {
		err = temp.Sync()
	}
	closeErr := temp.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write temporary config: %w", err)
	}
	if info, statErr := os.Stat(path); statErr == nil {
		_ = os.Chmod(tempPath, info.Mode().Perm())
	}

	backup := path + ".bak"
	if runtime.GOOS == "windows" {
		_ = os.Remove(backup)
		if _, statErr := os.Stat(path); statErr == nil {
			if err := os.Rename(path, backup); err != nil {
				return fmt.Errorf("backup config: %w", err)
			}
		}
		if err := os.Rename(tempPath, path); err != nil {
			_ = os.Rename(backup, path)
			return fmt.Errorf("replace config: %w", err)
		}
		return nil
	}
	if _, statErr := os.Stat(path); statErr == nil {
		if err := copyFile(path, backup); err != nil {
			return fmt.Errorf("backup config: %w", err)
		}
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace config: %w", err)
	}
	return nil
}

// PasswordForSave ensures resolved-only data is excluded. The method is kept
// separate to make accidental persistence of effective proxy values obvious.
func (cfg *Config) PasswordForSave() {
	cfg.effectivePools = nil
	cfg.Proxies = nil
	cfg.ProxyFile = ""
	for name, pool := range cfg.ProxyPools {
		pool.effective = nil
		cfg.ProxyPools[name] = pool
	}
	cfg.legacyProxiesPresent = false
	cfg.legacyProxyFilePresent = false
}

func copyFile(source, target string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	mode := os.FileMode(0600)
	if info, statErr := in.Stat(); statErr == nil {
		mode = info.Mode().Perm()
	}
	out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	_ = out.Chmod(mode)
	_, copyErr := io.Copy(out, in)
	syncErr := out.Sync()
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func readProxyFile(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var proxies []string
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		value := strings.TrimSpace(strings.TrimPrefix(scanner.Text(), "\ufeff"))
		value = strings.TrimSpace(stripProxyLineComment(value))
		if value != "" {
			proxies = append(proxies, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return proxies, nil
}

func stripProxyLineComment(line string) string {
	for i := 0; i < len(line); i++ {
		if i > 0 && line[i-1] != ' ' && line[i-1] != '\t' {
			continue
		}
		if line[i] == '#' || line[i] == ';' || (line[i] == '/' && i+1 < len(line) && line[i+1] == '/') {
			return line[:i]
		}
	}
	return line
}

func uniqueStrings(items []string) []string {
	out := items[:0]
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func trimList(items *[]string) {
	out := (*items)[:0]
	for _, item := range *items {
		if value := strings.TrimSpace(item); value != "" {
			out = append(out, value)
		}
	}
	*items = out
}

// mergeKeys dedupes the canonical keys with legacy zen/go inputs, preserving
// first-seen order (keys, then zen_keys, then go_keys).
func mergeKeys(groups ...[]string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, group := range groups {
		for _, key := range group {
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, key)
		}
	}
	return out
}

// ResolveHistoryDir maps the history.directory field to an absolute path.
// Empty or relative values resolve against the config file directory;
// absolute paths are cleaned and used directly.
func ResolveHistoryDir(configPath, dir string) string {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return filepath.Join(filepath.Dir(configPath), "history")
	}
	if filepath.IsAbs(dir) {
		return filepath.Clean(dir)
	}
	return filepath.Join(filepath.Dir(configPath), dir)
}

func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid>"
	}
	if u.User != nil {
		u.User = url.User("***")
	}
	return u.String()
}
