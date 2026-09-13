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
		migrateGatewaySchedulerState(current.gateway, next.gateway)
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

// migrateGatewaySchedulerState moves scheduler and proxy-transport state
// from the old Gateway to the newly built one before the atomic swap:
// credential state matches by tier+full key, proxy health by (pool name,
// raw proxy URL), and target state by full identity. Only still-future
// cooldowns migrate (remaining capped at 5 minutes); new resources start at
// zero state and removed identities are dropped. Checking flags never
// migrate. In-flight requests keep using the old Gateway and its state.
func migrateGatewaySchedulerState(oldGateway, newGateway *Gateway) {
	if oldGateway == nil || newGateway == nil || oldGateway.scheduler == nil || newGateway.scheduler == nil {
		return
	}
	for name, newPool := range newGateway.pools {
		oldPool := oldGateway.pools[name]
		if oldPool == nil || newPool == nil {
			continue
		}
		healthByRaw := make(map[string]bool, len(oldPool.items))
		for _, proxy := range oldPool.items {
			if proxy != nil {
				healthByRaw[proxy.name] = proxy.healthy.Load()
			}
		}
		for _, proxy := range newPool.items {
			if proxy == nil {
				continue
			}
			if healthy, ok := healthByRaw[proxy.name]; ok {
				proxy.healthy.Store(healthy)
			}
		}
	}
	newGateway.scheduler.migrateFrom(oldGateway.scheduler)
	validCreds := make(map[string]bool, len(newGateway.zenCreds)+len(newGateway.goCreds)+1)
	for _, cred := range newGateway.zenCreds {
		validCreds[cred.id] = true
	}
	for _, cred := range newGateway.goCreds {
		validCreds[cred.id] = true
	}
	if newGateway.cfg.Anonymous {
		validCreds[anonymousSchedulerCredentialID] = true
	}
	validPoolProxy := make(map[string]map[string]bool, len(newGateway.pools))
	for name, pool := range newGateway.pools {
		set := make(map[string]bool, len(pool.items))
		for _, proxy := range pool.items {
			if proxy != nil {
				set[proxy.name] = true
			}
		}
		validPoolProxy[name] = set
	}
	newGateway.scheduler.retainOnly(validCreds, validPoolProxy)
}

type ResourceSnapshot struct {
	Models           modelCatalogSnapshot   `json:"models"`
	Keys             []KeyStatus            `json:"keys"`
	Proxies          []ProxyStatus          `json:"proxies"`
	Anonymous        bool                   `json:"anonymous"`
	AnonymousProxies []AnonymousProxyStatus `json:"anonymous_proxies,omitempty"`
	Targets          []TargetStatus         `json:"targets,omitempty"`
	TargetsTotal     int                    `json:"targets_total,omitempty"`
	TargetsTruncated bool                   `json:"targets_truncated,omitempty"`
	Metadata         MetadataSnapshot       `json:"metadata"`
}

type KeyStatus struct {
	ID    string `json:"id"`
	Tier  string `json:"tier"`
	Index int    `json:"index"`
	// Deprecated: the static key->proxy binding no longer exists.
	// ProxyIndex is always zero and Proxy always empty; ProxyPool names the
	// assigned named pool (config identity, not a binding).
	ProxyIndex int    `json:"proxy_index,omitempty"`
	Proxy      string `json:"proxy,omitempty"`
	ProxyPool  string `json:"proxy_pool,omitempty"`
	// Failures/CooldownUntil describe the global credential (401) state.
	Failures                 uint32     `json:"failures"`
	CooldownUntil            *time.Time `json:"cooldown_until,omitempty"`
	CooldownRemainingSeconds *int64     `json:"cooldown_remaining_seconds,omitempty"`
	// AvailableTargets/TotalTargets are transport+credential level counts
	// over the assigned pool (model-agnostic); per-model target cooldowns
	// live in the targets list.
	AvailableTargets int `json:"available_targets,omitempty"`
	TotalTargets     int `json:"total_targets,omitempty"`
}

// AnonymousProxyStatus is the per-proxy anonymous target summary. Transport
// fields (Healthy/Checking) still mean proxy connectivity only; the cooldown
// fields summarize per-(proxy, model) target state across models.
// Failures/CooldownUntil/CooldownRemainingSeconds are a deprecated
// model-agnostic aggregate (sums / latest deadline); prefer ActiveCooldowns,
// NextAvailableAt, and LastFailureClass.
type AnonymousProxyStatus struct {
	Index                    int        `json:"index"`
	Pool                     string     `json:"proxy_pool,omitempty"`
	Address                  string     `json:"address"`
	Healthy                  bool       `json:"healthy"`
	Checking                 bool       `json:"checking"`
	ActiveCooldowns          int        `json:"active_cooldowns,omitempty"`
	NextAvailableAt          *time.Time `json:"next_available_at,omitempty"`
	LastFailureClass         string     `json:"last_failure_class,omitempty"`
	Failures                 uint32     `json:"failures"`
	CooldownUntil            *time.Time `json:"cooldown_until,omitempty"`
	CooldownRemainingSeconds *int64     `json:"cooldown_remaining_seconds,omitempty"`
}

type ProxyStatus struct {
	Index    int    `json:"index"`
	Pool     string `json:"proxy_pool,omitempty"`
	Address  string `json:"address"`
	Healthy  bool   `json:"healthy"`
	Checking bool   `json:"checking"`
	// ZenKeys/GoKeys are pool-level routed config credential counts: the
	// number of configured keys of that tier whose assigned pool is this
	// pool. Every proxy row in the same pool shows the same pool-level
	// count. They are NOT bindings; no key is bound to any single proxy.
	ZenKeys   int      `json:"zen_keys"`
	GoKeys    int      `json:"go_keys"`
	Anonymous bool     `json:"anonymous"`
	Routing   []string `json:"routing,omitempty"`
	// AvailableCredentials is the pool-level routed config credential count:
	// zen keys + go keys routed to this pool, plus one when the anonymous
	// credential is routed here. Same value as ZenKeys+GoKeys(+1); kept for
	// compatibility with consumers reading a single field.
	AvailableCredentials int `json:"available_credentials,omitempty"`
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
	result.Keys = append(result.Keys, keyStatusesForTier(gateway, "zen", gateway.zenCreds, gateway.cfg.ProxyRouting.Zen)...)
	result.Keys = append(result.Keys, keyStatusesForTier(gateway, "go", gateway.goCreds, gateway.cfg.ProxyRouting.Go)...)
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
	credsForPool := func(poolName string) int {
		count := 0
		if gateway.cfg.ProxyRouting.Zen == poolName {
			count += len(gateway.zenCreds)
		}
		if gateway.cfg.ProxyRouting.Go == poolName {
			count += len(gateway.goCreds)
		}
		if gateway.cfg.Anonymous && gateway.cfg.ProxyRouting.Anonymous == poolName {
			count++
		}
		return count
	}
	for _, pool := range gateway.uniquePools() {
		zenRouted, goRouted := 0, 0
		if gateway.cfg.ProxyRouting.Zen == pool.name {
			zenRouted = len(gateway.zenCreds)
		}
		if gateway.cfg.ProxyRouting.Go == pool.name {
			goRouted = len(gateway.goCreds)
		}
		for _, proxy := range pool.items {
			status := ProxyStatus{
				Index: proxy.index, Pool: pool.name, Address: redactURL(proxy.name),
				Healthy: proxy.healthy.Load(), Checking: proxy.checking.Load(),
				ZenKeys: zenRouted, GoKeys: goRouted,
				Anonymous: gateway.cfg.Anonymous && anonPool == pool,
				Routing:   routingFor(pool.name), AvailableCredentials: credsForPool(pool.name),
			}
			result.Proxies = append(result.Proxies, status)
		}
	}
	result.AnonymousProxies = gateway.anonymousTargetSummaries()
	targets, total := gateway.scheduler.snapshotTargets()
	result.Targets = targets
	result.TargetsTotal = total
	result.TargetsTruncated = total > len(targets)
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

// keyStatusesForTier snapshots credential-level state for one tier without
// taking pool locks: credential lists are immutable after build and all
// scheduler counters are mutex-guarded snapshots.
func keyStatusesForTier(gateway *Gateway, tier string, creds []credentialRef, poolName string) []KeyStatus {
	now := time.Now()
	var total, healthy int
	if pool := gateway.pools[poolName]; pool != nil {
		total = len(pool.items)
		for _, proxy := range pool.items {
			if proxy != nil && proxy.healthy.Load() {
				healthy++
			}
		}
	}
	result := make([]KeyStatus, 0, len(creds))
	for _, cred := range creds {
		failures, until := gateway.scheduler.credentialSnapshot(cred.id)
		status := KeyStatus{
			ID: cred.display, Tier: tier, Index: cred.index,
			ProxyPool: poolName, Failures: failures,
			TotalTargets: total,
		}
		// Only an unexpired credential (401) cooldown hides targets. An
		// expired cooldown with failures>0 (backoff memory) must report the
		// same transport-level availability as buildAuthCandidates sees.
		if until > now.UnixNano() {
			status.AvailableTargets = 0
		} else {
			status.AvailableTargets = healthy
		}
		if until > now.UnixNano() {
			value := time.Unix(0, until).UTC()
			status.CooldownUntil = &value
			status.CooldownRemainingSeconds = cooldownRemainingSeconds(until, now)
		}
		result = append(result, status)
	}
	return result
}

// anonymousTargetSummaries snapshots the per-proxy anonymous target state
// without taking pool locks: transports are immutable after build and all
// scheduler counters are mutex-guarded snapshots.
func (g *Gateway) anonymousTargetSummaries() []AnonymousProxyStatus {
	if !g.cfg.Anonymous {
		return nil
	}
	pool := g.pools[g.cfg.ProxyRouting.Anonymous]
	if pool == nil || len(pool.items) == 0 {
		return nil
	}
	now := time.Now()
	result := make([]AnonymousProxyStatus, 0, len(pool.items))
	for _, proxy := range pool.items {
		if proxy == nil {
			continue
		}
		active, nextAvailable, lastClass, failures := g.scheduler.proxyTargetSummary(pool.name, proxy.name)
		status := AnonymousProxyStatus{
			Index: proxy.index, Pool: pool.name, Address: redactURL(proxy.name),
			Healthy: proxy.healthy.Load(), Checking: proxy.checking.Load(),
			ActiveCooldowns: active, NextAvailableAt: nextAvailable,
			LastFailureClass: lastClass, Failures: failures,
		}
		if nextAvailable != nil {
			value := *nextAvailable
			status.CooldownUntil = &value
			status.CooldownRemainingSeconds = cooldownRemainingSeconds(value.UnixNano(), now)
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
