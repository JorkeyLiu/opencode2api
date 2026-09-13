package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type gatewayRuntime struct {
	config  Config
	gateway *Gateway
	handler http.Handler
	cancel  context.CancelFunc
}

type ApplyResult struct {
	Applied         bool     `json:"applied"`
	RestartRequired bool     `json:"restart_required"`
	RestartFields   []string `json:"restart_fields,omitempty"`
}

type RuntimeManager struct {
	configPath string
	root       context.Context
	logger     *slog.Logger
	monitor    *Monitor
	hub        *LogHub
	redactor   *SecretRedactor
	level      *slog.LevelVar
	current    atomic.Pointer[gatewayRuntime]
	updateMu   sync.Mutex
	effective  effectiveListeners
	metadata   *modelMetadataStore
}

type effectiveListeners struct {
	API          string
	WebUI        string
	WebUIEnabled bool
}

func NewRuntimeManager(root context.Context, configPath string, cfg Config, logger *slog.Logger, monitor *Monitor, hub *LogHub, redactor *SecretRedactor, level *slog.LevelVar) (*RuntimeManager, error) {
	manager := &RuntimeManager{
		configPath: configPath, root: root, logger: logger, monitor: monitor, hub: hub, redactor: redactor, level: level,
		effective: effectiveListeners{API: cfg.Listen, WebUI: cfg.WebUI.Listen, WebUIEnabled: cfg.WebUI.Enabled},
	}
	manager.metadata = newModelMetadataStore(configPath, logger)
	// models.dev refreshes ride the active runtime's healthy proxy transports
	// and fall back to the store's direct client when none are available.
	manager.metadata.SetClientProvider(func() []*http.Client {
		runtime := manager.current.Load()
		if runtime == nil || runtime.gateway == nil {
			return nil
		}
		return runtime.gateway.healthyClients()
	})
	if cfg.WebUI.Password != "" {
		hash, err := hashPassword(cfg.WebUI.Password)
		if err != nil {
			return nil, fmt.Errorf("hash webui password: %w", err)
		}
		cfg.WebUI.PasswordHash = hash
		cfg.WebUI.Password = ""
		if err := SaveConfigAtomic(configPath, cfg); err != nil {
			return nil, fmt.Errorf("persist webui password migration: %w", err)
		}
		// The automatically-created backup contains the one-time plaintext
		// bootstrap password and must not be retained.
		_ = os.Remove(configPath + ".bak")
	}
	runtime, err := manager.build(cfg)
	if err != nil {
		return nil, err
	}
	if err := runtime.gateway.catalog.LoadCache(modelCatalogCachePath(configPath)); err != nil && !os.IsNotExist(err) {
		if manager.logger != nil {
			manager.logger.Warn("model catalog cache ignored", "component", "models", "event", "catalog_cache_load_failed", "path", modelCatalogCachePath(configPath), "error", err)
		}
	}
	manager.current.Store(runtime)
	manager.redactor.Replace(cfg)
	setLogLevel(manager.level, cfg.Logging.Level)
	manager.start(runtime)
	manager.metadata.Start(root)
	return manager, nil
}

func (m *RuntimeManager) build(cfg Config) (*gatewayRuntime, error) {
	gateway, err := NewGateway(cfg, m.logger, m.monitor)
	if err != nil {
		return nil, err
	}
	gateway.catalog.metadata = m.metadata
	gateway.catalog.SetCachePath(modelCatalogCachePath(m.configPath))
	return &gatewayRuntime{config: cfg, gateway: gateway, handler: gateway.Handler(), cancel: func() {}}, nil
}

func (m *RuntimeManager) start(runtime *gatewayRuntime) {
	// Store the context through a lightweight wrapper so cancel stops catalog
	// and proxy checks while in-flight HTTP requests continue on the old pools.
	runtimeCtx, cancel := context.WithCancel(m.root)
	runtime.cancel = cancel
	runtime.gateway.StartProxyHealthChecks(runtimeCtx)
	runtime.gateway.StartModelRefresh(runtimeCtx)
}

func (m *RuntimeManager) Handler() http.Handler {
	dynamic := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runtime := m.current.Load()
		if runtime == nil {
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
		runtime.handler.ServeHTTP(w, r)
	})
	return monitorMiddleware(m.monitor, m.logger, dynamic)
}

func (m *RuntimeManager) Config() Config {
	runtime := m.current.Load()
	if runtime == nil {
		return Config{}
	}
	return cloneConfig(runtime.config)
}

func (m *RuntimeManager) RestartStatus() (effectiveListeners, []string) {
	cfg := m.Config()
	fields := make([]string, 0, 3)
	if cfg.Listen != m.effective.API {
		fields = append(fields, "listen")
	}
	if cfg.WebUI.Listen != m.effective.WebUI {
		fields = append(fields, "webui.listen")
	}
	if cfg.WebUI.Enabled != m.effective.WebUIEnabled {
		fields = append(fields, "webui.enabled")
	}
	return m.effective, fields
}

func cloneConfig(cfg Config) Config {
	cfg.ServerKeys = append([]string(nil), cfg.ServerKeys...)
	cfg.ZenKeys = append([]string(nil), cfg.ZenKeys...)
	cfg.GoKeys = append([]string(nil), cfg.GoKeys...)
	cfg.Proxies = append([]string(nil), cfg.Proxies...)
	if cfg.ProxyPools != nil {
		pools := make(map[string]ProxyPoolConfig, len(cfg.ProxyPools))
		for name, pool := range cfg.ProxyPools {
			pools[name] = ProxyPoolConfig{
				Proxies:   append([]string(nil), pool.Proxies...),
				ProxyFile: pool.ProxyFile,
				effective: append([]string(nil), pool.effective...),
			}
		}
		cfg.ProxyPools = pools
	}
	if cfg.effectivePools != nil {
		effective := make(map[string][]string, len(cfg.effectivePools))
		for name, proxies := range cfg.effectivePools {
			effective[name] = append([]string(nil), proxies...)
		}
		cfg.effectivePools = effective
	}
	if cfg.Models.Protocols != nil {
		protocols := make(map[string]string, len(cfg.Models.Protocols))
		for key, value := range cfg.Models.Protocols {
			protocols[key] = value
		}
		cfg.Models.Protocols = protocols
	}
	return cfg
}

func (m *RuntimeManager) Apply(candidate Config, persist bool) (ApplyResult, error) {
	m.updateMu.Lock()
	defer m.updateMu.Unlock()

	current := m.current.Load()
	hadPlaintextPassword := candidate.WebUI.Password != ""
	if hadPlaintextPassword {
		hash, err := hashPassword(candidate.WebUI.Password)
		if err != nil {
			return ApplyResult{}, err
		}
		candidate.WebUI.PasswordHash = hash
		candidate.WebUI.Password = ""
	}
	normalized, err := NormalizeConfig(m.configPath, candidate)
	if err != nil {
		return ApplyResult{}, err
	}
	next, err := m.build(normalized)
	if err != nil {
		return ApplyResult{}, fmt.Errorf("initialize runtime: %w", err)
	}
	if current != nil {
		next.gateway.catalog.CopyState(current.gateway.catalog)
	}
	if persist || hadPlaintextPassword {
		if err := SaveConfigAtomic(m.configPath, normalized); err != nil {
			next.cancel()
			return ApplyResult{}, err
		}
	}

	result := ApplyResult{Applied: true}
	if normalized.Listen != m.effective.API {
		result.RestartFields = append(result.RestartFields, "listen")
	}
	if normalized.WebUI.Listen != m.effective.WebUI {
		result.RestartFields = append(result.RestartFields, "webui.listen")
	}
	if normalized.WebUI.Enabled != m.effective.WebUIEnabled {
		result.RestartFields = append(result.RestartFields, "webui.enabled")
	}
	result.RestartRequired = len(result.RestartFields) > 0
	m.redactor.Replace(normalized)
	setLogLevel(m.level, normalized.Logging.Level)
	if current == nil || normalized.Logging.RingSize != current.config.Logging.RingSize {
		m.hub.Resize(normalized.Logging.RingSize)
	}
	m.start(next)
	previous := m.current.Swap(next)
	if previous != nil {
		previous.cancel()
	}
	m.logger.Info("configuration applied", "component", "config", "event", "config_applied", "restart_required", result.RestartRequired, "restart_fields", result.RestartFields)
	return result, nil
}

func (m *RuntimeManager) Reload() (ApplyResult, error) {
	cfg, err := LoadConfig(m.configPath)
	if err != nil {
		return ApplyResult{}, err
	}
	hadPlaintextPassword := cfg.WebUI.Password != ""
	result, err := m.Apply(cfg, false)
	if err == nil && hadPlaintextPassword {
		_ = os.Remove(m.configPath + ".bak")
	}
	return result, err
}

func (m *RuntimeManager) Shutdown() {
	if current := m.current.Load(); current != nil {
		current.cancel()
	}
}

type ResourceSnapshot struct {
	Models           modelCatalogSnapshot   `json:"models"`
	Keys             []KeyStatus            `json:"keys"`
	Proxies          []ProxyStatus          `json:"proxies"`
	Anonymous        bool                   `json:"anonymous"`
	AnonymousProxies []AnonymousProxyStatus `json:"anonymous_proxies,omitempty"`
	Metadata         MetadataSnapshot       `json:"metadata"`
}

type KeyStatus struct {
	ID                       string     `json:"id"`
	Tier                     string     `json:"tier"`
	Index                    int        `json:"index"`
	ProxyIndex               int        `json:"proxy_index"`
	Proxy                    string     `json:"proxy,omitempty"`
	ProxyPool                string     `json:"proxy_pool,omitempty"`
	Failures                 uint32     `json:"failures"`
	CooldownUntil            *time.Time `json:"cooldown_until,omitempty"`
	CooldownRemainingSeconds *int64     `json:"cooldown_remaining_seconds,omitempty"`
}

// AnonymousProxyStatus exposes the per-proxy anonymous cooldown state.
// Proxy health still means transport connectivity only; failures and
// cooldown here are credential (business) state, never proxy health.
type AnonymousProxyStatus struct {
	Index                    int        `json:"index"`
	Pool                     string     `json:"proxy_pool,omitempty"`
	Address                  string     `json:"address"`
	Healthy                  bool       `json:"healthy"`
	Checking                 bool       `json:"checking"`
	Failures                 uint32     `json:"failures"`
	CooldownUntil            *time.Time `json:"cooldown_until,omitempty"`
	CooldownRemainingSeconds *int64     `json:"cooldown_remaining_seconds,omitempty"`
}

type ProxyStatus struct {
	Index     int      `json:"index"`
	Pool      string   `json:"proxy_pool,omitempty"`
	Address   string   `json:"address"`
	Healthy   bool     `json:"healthy"`
	Checking  bool     `json:"checking"`
	ZenKeys   int      `json:"zen_keys"`
	GoKeys    int      `json:"go_keys"`
	Anonymous bool     `json:"anonymous"`
	Routing   []string `json:"routing,omitempty"`
}

func (m *RuntimeManager) Resources() ResourceSnapshot {
	runtime := m.current.Load()
	if runtime == nil {
		return ResourceSnapshot{}
	}
	gateway := runtime.gateway
	result := ResourceSnapshot{Models: gateway.catalog.Snapshot(), Anonymous: gateway.cfg.Anonymous}
	if gateway.catalog.metadata != nil {
		result.Metadata = gateway.catalog.metadata.Snapshot()
	}
	result.Keys = append(result.Keys, keyStatuses("zen", gateway.zenNodes)...)
	result.Keys = append(result.Keys, keyStatuses("go", gateway.goNodes)...)
	bindingsFor := func(pool *nodePool) []int {
		if pool == nil {
			return nil
		}
		pool.bindingsMu.Lock()
		defer pool.bindingsMu.Unlock()
		return append([]int(nil), pool.bindingCount...)
	}
	zenBindings := bindingsFor(gateway.zenNodes)
	goBindings := bindingsFor(gateway.goNodes)
	routingFor := func(poolName string) []string {
		out := []string{}
		if gateway.cfg.ProxyRouting.Anonymous == poolName {
			out = append(out, "anonymous")
		}
		if gateway.cfg.ProxyRouting.Zen == poolName {
			out = append(out, "zen")
		}
		if gateway.cfg.ProxyRouting.Go == poolName {
			out = append(out, "go")
		}
		return out
	}
	anonPool := gateway.pools[gateway.cfg.ProxyRouting.Anonymous]
	for _, pool := range gateway.uniquePools() {
		// Each tier counts only its own pool binding. A shared pool reports
		// both tiers over the same index space; an isolated pool reports
		// only the tier bound to it.
		isZen := gateway.zenNodes != nil && gateway.zenNodes.transports == pool
		isGo := gateway.goNodes != nil && gateway.goNodes.transports == pool
		for _, proxy := range pool.items {
			status := ProxyStatus{
				Index: proxy.index, Pool: pool.name, Address: redactURL(proxy.name),
				Healthy: proxy.healthy.Load(), Checking: proxy.checking.Load(),
				Anonymous: gateway.cfg.Anonymous && anonPool == pool,
				Routing:   routingFor(pool.name),
			}
			if isZen && proxy.index < len(zenBindings) {
				status.ZenKeys = zenBindings[proxy.index]
			}
			if isGo && proxy.index < len(goBindings) {
				status.GoKeys = goBindings[proxy.index]
			}
			result.Proxies = append(result.Proxies, status)
		}
	}
	result.AnonymousProxies = anonymousProxyStatuses(gateway.anonymous)
	return result
}

func (m *RuntimeManager) DebugModels() ([]ModelRouteDiagnostic, MetadataSnapshot) {
	runtime := m.current.Load()
	if runtime == nil {
		return nil, MetadataSnapshot{}
	}
	gateway := runtime.gateway
	models := gateway.catalog.List()
	result := make([]ModelRouteDiagnostic, 0, len(models))
	for _, model := range models {
		result = append(result, gateway.catalog.Diagnostic(model, "", len(gateway.cfg.ZenKeys) > 0, len(gateway.cfg.GoKeys) > 0, gateway.cfg.Anonymous))
	}
	metadata := MetadataSnapshot{}
	if gateway.catalog.metadata != nil {
		metadata = gateway.catalog.metadata.Snapshot()
	}
	return result, metadata
}

func (m *RuntimeManager) DebugRoute(model string, requested Protocol) ModelRouteDiagnostic {
	runtime := m.current.Load()
	if runtime == nil {
		return ModelRouteDiagnostic{Model: model, RequestedProtocol: requested, RouteError: "gateway runtime is unavailable"}
	}
	gateway := runtime.gateway
	return gateway.catalog.Diagnostic(model, requested, len(gateway.cfg.ZenKeys) > 0, len(gateway.cfg.GoKeys) > 0, gateway.cfg.Anonymous)
}

func keyStatuses(tier string, pool *nodePool) []KeyStatus {
	if pool == nil {
		return nil
	}
	now := time.Now()
	result := make([]KeyStatus, 0, len(pool.nodes))
	for _, node := range pool.nodes {
		proxyIndex := int(node.proxyIndex.Load())
		status := KeyStatus{ID: keyDisplayID(node.key), Tier: tier, Index: node.index, ProxyIndex: proxyIndex, Failures: node.failures.Load()}
		// Current proxy name is redacted; the pool slice is immutable after
		// build and atomics are lock-free, so no long lock is introduced.
		// ProxyIndex is pool-local; ProxyPool names the owning pool.
		if pool.transports != nil {
			status.ProxyPool = pool.transports.name
			if proxyIndex >= 0 && proxyIndex < len(pool.transports.items) && pool.transports.items[proxyIndex] != nil {
				status.Proxy = redactURL(pool.transports.items[proxyIndex].name)
			}
		}
		if until := node.cooldownUntil.Load(); until > now.UnixNano() {
			value := time.Unix(0, until).UTC()
			status.CooldownUntil = &value
			status.CooldownRemainingSeconds = cooldownRemainingSeconds(until, now)
		}
		result = append(result, status)
	}
	return result
}

// anonymousProxyStatuses snapshots the per-proxy anonymous credential state
// without taking pool or binding locks: nodes are immutable after build and
// all counters are atomics.
func anonymousProxyStatuses(pool *anonymousPool) []AnonymousProxyStatus {
	if pool == nil || len(pool.nodes) == 0 {
		return nil
	}
	now := time.Now()
	result := make([]AnonymousProxyStatus, 0, len(pool.nodes))
	for _, node := range pool.nodes {
		if node == nil || node.proxy == nil {
			continue
		}
		status := AnonymousProxyStatus{
			Index: node.proxy.index, Pool: node.proxy.pool, Address: redactURL(node.proxy.name),
			Healthy: node.proxy.healthy.Load(), Checking: node.proxy.checking.Load(),
			Failures: node.failures.Load(),
		}
		if until := node.cooldownUntil.Load(); until > now.UnixNano() {
			value := time.Unix(0, until).UTC()
			status.CooldownUntil = &value
			status.CooldownRemainingSeconds = cooldownRemainingSeconds(until, now)
		}
		result = append(result, status)
	}
	return result
}

func cooldownRemainingSeconds(untilUnixNano int64, now time.Time) *int64 {
	remaining := untilUnixNano - now.UnixNano()
	if remaining <= 0 {
		return nil
	}
	seconds := (remaining + int64(time.Second) - 1) / int64(time.Second)
	return &seconds
}

func secretFingerprint(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:10]
}

// keyDisplayID is intentionally separate from secretFingerprint. The latter
// is an internal stable identifier used by session/config bookkeeping; this
// value is safe for logs and the operator UI and shows only the key suffix.
func keyDisplayID(value string) string {
	runes := []rune(value)
	if len(runes) <= 5 {
		return string(runes)
	}
	return string(runes[len(runes)-5:])
}
