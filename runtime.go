package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
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
	history    atomic.Pointer[HistoryStore]
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
	manager.openHistory(cfg.History)
	return manager, nil
}

// openHistory builds the durable history projection. Failures only warn and
// leave a memory-only disabled status; Gateway startup always continues.
func (m *RuntimeManager) openHistory(cfg HistoryConfig) {
	store := OpenHistoryStore(m.configPath, cfg, m.logger, m.redactor)
	m.history.Store(store)
	m.monitor.SetHistorySink(store)
}

// History returns the active history store (possibly disabled).
func (m *RuntimeManager) History() *HistoryStore {
	if m == nil {
		return nil
	}
	return m.history.Load()
}

// swapHistoryForApply opens the new history store when config changed,
// reuses the old store when unchanged, and atomically switches the Monitor
// sink before draining the old store. Failures degrade to disabled status
// and never fail Apply.
func (m *RuntimeManager) swapHistoryForApply(oldCfg, newCfg HistoryConfig) {
	previous := m.history.Load()
	if previous != nil && historyEqual(oldCfg, newCfg) {
		return
	}
	next := OpenHistoryStore(m.configPath, newCfg, m.logger, m.redactor)
	m.history.Store(next)
	m.monitor.SetHistorySink(next)
	if previous != nil {
		previous.Close()
	}
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
	cfg.Fallback.Active = strings.TrimSpace(cfg.Fallback.Active)
	if cfg.Fallback.Channels != nil {
		channels := make([]FallbackChannelConfig, len(cfg.Fallback.Channels))
		copy(channels, cfg.Fallback.Channels)
		cfg.Fallback.Channels = channels
	}
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
		summary := migrateGatewaySchedulerState(current.gateway, next.gateway)
		m.logger.Info("scheduler state migrated", "component", "scheduler", "event", "scheduler_state_migrated",
			"credentials", summary.Credentials, "credential_rate_limits", summary.Credential429, "targets", summary.Targets, "proxy_rate_limits", summary.Proxy429, "channel_availability", summary.Channel, "proxies", summary.Proxies, "pins", summary.Pins, "fallbacks", summary.Fallbacks)
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
	// History is hot-applied: never a restart field.
	if current != nil {
		m.swapHistoryForApply(current.config.History, normalized.History)
	} else {
		m.openHistory(normalized.History)
	}
	setLogLevel(m.level, normalized.Logging.Level)
	if current == nil || normalized.Logging.RingSize != current.config.Logging.RingSize {
		m.hub.Resize(normalized.Logging.RingSize)
	}
	m.start(next)
	previous := m.current.Swap(next)
	if previous != nil {
		previous.cancel()
		// Release idle connections held by the old Gateway pools. Only
		// idle connections are closed; in-flight active connections on the
		// old pools continue until their requests finish.
		if previous.gateway != nil {
			previous.gateway.CloseIdleConnections()
		}
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
	m.ShutdownWithTimeout(historyShutdownCloseTimeout)
}

// ShutdownWithTimeout stops background refresh, closes idle connections on
// the current Gateway, and drains the history store within timeout. Only
// idle connections are closed; in-flight requests are not interrupted.
// History uses best-effort semantics: on timeout a history_shutdown_timeout
// warn is emitted and the writer continues in the background.
func (m *RuntimeManager) ShutdownWithTimeout(timeout time.Duration) {
	if current := m.current.Load(); current != nil {
		current.cancel()
		if current.gateway != nil {
			current.gateway.CloseIdleConnections()
		}
	}
	if store := m.history.Load(); store != nil {
		store.CloseWithTimeout(timeout)
	}
}

// ShutdownWithContext drains history within the context deadline (capped at
// the shutdown budget) so main can fit history drain inside the 15s server
// shutdown budget. A nil context or no deadline falls back to the default
// shutdown budget.
func (m *RuntimeManager) ShutdownWithContext(ctx context.Context) {
	timeout := historyShutdownCloseTimeout
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			remaining := time.Until(deadline)
			if remaining < timeout {
				timeout = remaining
			}
			if timeout < 0 {
				timeout = 0
			}
		}
	}
	m.ShutdownWithTimeout(timeout)
}

// gatewayMigrationSummary counts migrated state for the Apply log summary.
// No per-identity detail is included.
type gatewayMigrationSummary struct {
	Credentials   int
	Credential429 int
	Targets       int
	Proxy429      int
	Channel       int
	Proxies       int
	Pins          int
	Fallbacks     int
}

// migrateGatewaySchedulerState moves scheduler and proxy-transport state
// from the old Gateway to the newly built one before the atomic swap:
// credential state matches by tier+full key, credential429 by tier+key,
// proxy health by (pool name, raw proxy URL), target state by full identity,
// proxy429 and channel state by (tier, pool, raw proxy) identity only, and
// route-session overrides by target scope (client dimension excluded from
// validity).
// Authenticated route-session scopes are proxy-independent; only anonymous
// scopes require proxy validity, and legacy auth overrides with proxy-bound
// keys are dropped (new requests derive the proxy-independent session
// statelessly). Only still-future cooldowns and still-fresh route overrides
// migrate (remaining capped at 5 minutes for cooldowns, idle TTL for
// sessions); new resources start at zero/stateless state and removed
// identities are dropped. Session-affinity pins migrate without validity
// filtering up to the pin cap as tombstone-like bindings: authenticated pins
// validate proxy-independently (binding without proxy) while anonymous pins
// remain full-target; removed/changed bindings still resolve to the pinned
// path and fail locally with 502. Checking flags never migrate.
// Route-session overrides and pins are in-memory authority (not a
// projection): they never persist across restarts and never enter logs,
// metrics, history, or admin output. In-flight requests keep using the old
// Gateway and its state.
func migrateGatewaySchedulerState(oldGateway, newGateway *Gateway) gatewayMigrationSummary {
	var summary gatewayMigrationSummary
	if oldGateway == nil || newGateway == nil || oldGateway.scheduler == nil || newGateway.scheduler == nil {
		return summary
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
				summary.Proxies++
			}
		}
	}
	migrated := newGateway.scheduler.migrateFrom(oldGateway.scheduler)
	summary.Credentials = migrated.Credentials
	summary.Credential429 = migrated.Credential429
	summary.Targets = migrated.Targets
	summary.Proxy429 = migrated.Proxy429
	summary.Channel = migrated.Channel
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
	validTierPool := map[string]map[string]bool{
		string(TierZen): {newGateway.cfg.ProxyRouting.Zen: true, newGateway.cfg.ProxyRouting.Anonymous: true},
		string(TierGo):  {newGateway.cfg.ProxyRouting.Go: true},
	}
	newGateway.scheduler.retainOnly(validCreds, validPoolProxy, validTierPool)
	zenAuthority := normalizeRouteAuthority(newGateway.cfg.Upstream.Zen)
	goAuthority := normalizeRouteAuthority(newGateway.cfg.Upstream.Go)
	if oldGateway.scheduler.routeSessions != nil && newGateway.scheduler.routeSessions != nil {
		validScope := func(scope routeSessionScope) bool {
			if !validCreds[scope.CredID] {
				return false
			}
			// Authenticated scopes are proxy-independent: only pool validity
			// matters; legacy proxy-bound auth overrides (ProxyRaw != "")
			// are dropped and re-derived statelessly.
			if scope.CredID != anonymousSchedulerCredentialID {
				if scope.ProxyRaw != "" {
					return false
				}
				if _, ok := validPoolProxy[scope.Pool]; !ok {
					return false
				}
			} else {
				proxies, ok := validPoolProxy[scope.Pool]
				if !ok || !proxies[scope.ProxyRaw] {
					return false
				}
			}
			if scope.Protocol != ProtocolChat && scope.Protocol != ProtocolResponses && scope.Protocol != ProtocolAnthropic {
				return false
			}
			// Target scope must still be routable: authority matches the tier
			// upstream and the pool is still the assigned pool for that
			// channel. Indexing by target scope (not client) keeps migration
			// bounded.
			switch {
			case scope.CredID == anonymousSchedulerCredentialID:
				if scope.Tier != TierZen || scope.Authority != zenAuthority {
					return false
				}
				return scope.Pool == newGateway.cfg.ProxyRouting.Anonymous
			case scope.Tier == TierZen:
				if scope.Authority != zenAuthority {
					return false
				}
				return scope.Pool == newGateway.cfg.ProxyRouting.Zen
			case scope.Tier == TierGo:
				if scope.Authority != goAuthority {
					return false
				}
				return scope.Pool == newGateway.cfg.ProxyRouting.Go
			default:
				return false
			}
		}
		newGateway.scheduler.routeSessions.migrateRouteSessionsFrom(oldGateway.scheduler.routeSessions, validScope)
	}
	if oldGateway.scheduler.pins != nil && newGateway.scheduler.pins != nil {
		summary.Pins = newGateway.scheduler.pins.migratePinsFrom(oldGateway.scheduler.pins)
	}
	if oldGateway.scheduler.fallbacks != nil && newGateway.scheduler.fallbacks != nil {
		summary.Fallbacks = newGateway.scheduler.fallbacks.migrateFallbackFrom(oldGateway.scheduler.fallbacks)
	}
	return summary
}

type ResourceSnapshot struct {
	Models                   modelCatalogSnapshot        `json:"models"`
	Keys                     []KeyStatus                 `json:"keys"`
	Proxies                  []ProxyStatus               `json:"proxies"`
	Anonymous                bool                        `json:"anonymous"`
	Targets                  []TargetStatus              `json:"targets,omitempty"`
	TargetsTotal             int                         `json:"targets_total,omitempty"`
	TargetsTruncated         bool                        `json:"targets_truncated,omitempty"`
	ProxyRateLimits          []ProxyRateLimitStatus      `json:"proxy_rate_limits,omitempty"`
	ProxyRateLimitsTotal     int                         `json:"proxy_rate_limits_total,omitempty"`
	ProxyRateLimitsTruncated bool                        `json:"proxy_rate_limits_truncated,omitempty"`
	ChannelCooldowns         []ChannelAvailabilityStatus `json:"channel_cooldowns,omitempty"`
	ChannelCooldownsTotal    int                         `json:"channel_cooldowns_total,omitempty"`
	ChannelTruncated         bool                        `json:"channel_truncated,omitempty"`
	AvailabilityCheckedAt    *time.Time                  `json:"availability_checked_at,omitempty"`
	AvailabilityTruncated    bool                        `json:"availability_truncated,omitempty"`
	AvailabilityPartial      bool                        `json:"availability_partial,omitempty"`
	Metadata                 MetadataSnapshot            `json:"metadata"`
}

// KeyStatus is the per-credential availability row (凭证可用性). ID is the
// key tail (never full secrets); Tier names the channel; ProxyPool is the
// assigned pool identity. Status is available/unavailable/rate_limited;
// Reason is the short machine reason (auth_failure, rate_limited,
// no_healthy_proxies, all_proxies_cooling, success, untested, ...).
// LastChecked is the admin bulk-check projection (never routing authority).
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
	// Credential availability (operator-facing): status/reason/last checked.
	Status      string     `json:"status,omitempty"`
	Reason      string     `json:"reason,omitempty"`
	LastChecked *time.Time `json:"last_checked,omitempty"`
}

// ProxyStatus is one pool-qualified proxy row in the unified 代理可用性
// view. The same raw URL in different pools appears as independent rows
// (pool-qualified); the UI may visually group by redacted node but must
// never merge state across pools. Zen/Go are channel-specific availability
// labels for that tier+pool+proxy (available, rate_limited,
// channel_unavailable, transport_unavailable, untested). CooldownReason is
// the active Zen/Go cooldown reason (if any); LastChecked is the bulk-check
// projection.
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
	AvailableCredentials int        `json:"available_credentials,omitempty"`
	Zen                  string     `json:"zen,omitempty"`
	Go                   string     `json:"go,omitempty"`
	CooldownReason       string     `json:"cooldown_reason,omitempty"`
	LastChecked          *time.Time `json:"last_checked,omitempty"`
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
	bulkSnap := gateway.bulkSnapshot.Load()
	credLastChecked := map[string]*time.Time{}
	if bulkSnap != nil {
		for _, c := range bulkSnap.Credentials {
			// Lookup by internal credential ID (tier-qualified); tail-only
			// keys would collapse collisions. Fall back to tail key for
			// snapshots written before CredID existed.
			if c.CredID != "" {
				credLastChecked[string(c.Tier)+"\x00"+c.CredID+"\x00"+c.Pool] = c.LastChecked
			} else {
				credLastChecked[string(c.Tier)+"\x00"+c.KeyTail+"\x00"+c.Pool] = c.LastChecked
			}
		}
		if bulkSnap.CheckedAt.IsZero() == false {
			value := bulkSnap.CheckedAt.UTC()
			result.AvailabilityCheckedAt = &value
			result.AvailabilityTruncated = bulkSnap.Truncated
			result.AvailabilityPartial = bulkSnap.Partial
		}
	}
	result.Keys = append(result.Keys, keyStatusesForTier(gateway, "zen", gateway.zenCreds, gateway.cfg.ProxyRouting.Zen, credLastChecked)...)
	result.Keys = append(result.Keys, keyStatusesForTier(gateway, "go", gateway.goCreds, gateway.cfg.ProxyRouting.Go, credLastChecked)...)
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
	// Bulk projection lookup for per-proxy last-checked (pool-qualified).
	nodeLastChecked := map[string]*time.Time{}
	if bulkSnap != nil {
		for _, n := range bulkSnap.Nodes {
			nodeLastChecked[n.Pool+"\x00"+n.ProxyNode] = n.LastChecked
		}
	}
	now := time.Now()
	for _, pool := range gateway.uniquePools() {
		zenRouted, goRouted := 0, 0
		if gateway.cfg.ProxyRouting.Zen == pool.name {
			zenRouted = len(gateway.zenCreds)
		}
		if gateway.cfg.ProxyRouting.Go == pool.name {
			goRouted = len(gateway.goCreds)
		}
		for _, proxy := range pool.items {
			healthy := proxy.healthy.Load()
			zenLabel, zenReason := proxyChannelLabel(gateway, TierZen, pool.name, proxy.name, healthy, now)
			goLabel, goReason := proxyChannelLabel(gateway, TierGo, pool.name, proxy.name, healthy, now)
			reason := ""
			if zenReason != "" {
				reason = "zen:" + zenReason
			}
			if goReason != "" {
				if reason != "" {
					reason += ";"
				}
				reason += "go:" + goReason
			}
			// Preserve anonymous Zen 403/5xx + proxy429 context inside the
			// Zen reason column: channel/proxy429 already encode it, and the
			// dedicated anonymous summary table is removed.
			status := ProxyStatus{
				Index: proxy.index, Pool: pool.name, Address: redactURL(proxy.name),
				Healthy: healthy, Checking: proxy.checking.Load(),
				ZenKeys: zenRouted, GoKeys: goRouted,
				Anonymous: gateway.cfg.Anonymous && anonPool == pool,
				Routing:   routingFor(pool.name), AvailableCredentials: credsForPool(pool.name),
				Zen: zenLabel, Go: goLabel, CooldownReason: reason,
			}
			if lc, ok := nodeLastChecked[pool.name+"\x00"+redactURL(proxy.name)]; ok {
				status.LastChecked = lc
			} else if bulkSnap != nil && !bulkSnap.CheckedAt.IsZero() {
				// Fall back to the run timestamp so every row shows a
				// last-checked projection after at least one bulk run.
				value := bulkSnap.CheckedAt.UTC()
				status.LastChecked = &value
			}
			result.Proxies = append(result.Proxies, status)
		}
	}
	targets, total := gateway.scheduler.snapshotTargets()
	result.Targets = targets
	result.TargetsTotal = total
	result.TargetsTruncated = total > len(targets)
	limits, limitsTotal := gateway.scheduler.snapshotProxy429()
	result.ProxyRateLimits = limits
	result.ProxyRateLimitsTotal = limitsTotal
	result.ProxyRateLimitsTruncated = limitsTotal > len(limits)
	channels, channelsTotal := gateway.scheduler.snapshotChannel()
	result.ChannelCooldowns = channels
	result.ChannelCooldownsTotal = channelsTotal
	result.ChannelTruncated = channelsTotal > len(channels)
	return result
}

// proxyChannelLabel resolves one tier's operator-facing availability for a
// pool-qualified proxy: transport first, then proxy429, then channel.
// It returns the compact label plus the active cooldown reason (if any).
// Zen and Go are evaluated independently so the same node can differ by tier.
func proxyChannelLabel(gateway *Gateway, tier Tier, pool, raw string, healthy bool, now time.Time) (string, string) {
	if !healthy {
		return "transport_unavailable", "transport_unavailable"
	}
	if until, _, ok := gateway.scheduler.proxy429CooldownStatus(tier, pool, raw); ok && until > now.UnixNano() {
		return "rate_limited", "rate_limited"
	}
	if until, _, ok := gateway.scheduler.channelCooldownStatus(tier, pool, raw); ok && until > now.UnixNano() {
		return "channel_unavailable", "channel_unavailable"
	}
	return "available", ""
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
// scheduler counters are mutex-guarded snapshots. Status/reason form the
// operator-facing 凭证可用性 view: tier, key tail, assigned pool, status,
// availability counts, reason, last checked. Full secrets never appear.
func keyStatusesForTier(gateway *Gateway, tier string, creds []credentialRef, poolName string, lastChecked map[string]*time.Time) []KeyStatus {
	now := time.Now()
	nowNanos := now.UnixNano()
	var total, healthy int
	if pool := gateway.pools[poolName]; pool != nil {
		total = len(pool.items)
		for _, proxy := range pool.items {
			if proxy != nil && proxy.healthy.Load() {
				healthy++
			}
		}
	}
	tierTyped := Tier(tier)
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
		if until > nowNanos {
			status.AvailableTargets = 0
		} else {
			status.AvailableTargets = healthy
		}
		if until > nowNanos {
			value := time.Unix(0, until).UTC()
			status.CooldownUntil = &value
			status.CooldownRemainingSeconds = cooldownRemainingSeconds(until, now)
		}
		// Operator status: 401 cooling wins, then credential429 cooling, then
		// transport (no healthy proxy), then all-proxies tier cooling
		// (proxy429/channel covering every healthy proxy), else available.
		// healthz readiness is unchanged: channel and credential429 never
		// affect it; this display-only status may still report them.
		if until > nowNanos {
			status.Status = "unavailable"
			status.Reason = "auth_failure"
		} else if cUntil, _, ok := gateway.scheduler.credential429CooldownStatus(cred.id); ok && cUntil > nowNanos {
			status.Status = "rate_limited"
			status.Reason = "rate_limited"
		} else if healthy == 0 {
			status.Status = "unavailable"
			status.Reason = "no_healthy_proxies"
		} else {
			cooling := 0
			if pool := gateway.pools[poolName]; pool != nil {
				for _, proxy := range pool.items {
					if proxy == nil || !proxy.healthy.Load() {
						continue
					}
					if pUntil, _, ok := gateway.scheduler.proxy429CooldownStatus(tierTyped, poolName, proxy.name); ok && pUntil > nowNanos {
						cooling++
						continue
					}
					if cUntil, _, ok := gateway.scheduler.channelCooldownStatus(tierTyped, poolName, proxy.name); ok && cUntil > nowNanos {
						cooling++
					}
				}
			}
			if total > 0 && cooling >= healthy {
				status.Status = "unavailable"
				status.Reason = "all_proxies_cooling"
			} else {
				status.Status = "available"
				status.Reason = "success"
			}
		}
		if lc, ok := lastChecked[tier+"\x00"+cred.id+"\x00"+poolName]; ok {
			status.LastChecked = lc
		} else if lc, ok := lastChecked[tier+"\x00"+cred.display+"\x00"+poolName]; ok {
			status.LastChecked = lc
		} else if snap := gateway.bulkSnapshot.Load(); snap != nil && !snap.CheckedAt.IsZero() {
			value := snap.CheckedAt.UTC()
			status.LastChecked = &value
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
