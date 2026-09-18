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
	cfg.Keys = append([]string(nil), cfg.Keys...)
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
// validity). Both native route-session scopes are proxy-independent; legacy
// proxy-bound overrides (ProxyRaw != "") are dropped and re-derived
// statelessly. Only still-future cooldowns and still-fresh route overrides
// migrate (non-429 remaining capped at the generic 5 minutes, proxy429 and
// credential429 remaining capped at the NEW configured 429 max, idle TTL
// for sessions); new resources start at zero/stateless state and removed
// identities are dropped. Session-affinity pins migrate without validity
// filtering up to the pin cap as tombstone-like bindings: both channels
// validate proxy-independently (binding without proxy); removed/changed
// bindings still resolve to the pinned path and fail locally with 502.
// Checking flags never migrate.
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
	validCreds := make(map[string]bool, len(newGateway.credentials())+1)
	for _, cred := range newGateway.credentials() {
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
		string(TierZen): {newGateway.authPoolName(): true, newGateway.cfg.ProxyRouting.Anonymous: true},
	}
	newGateway.scheduler.retainOnly(validCreds, validPoolProxy, validTierPool)
	zenAuthority := normalizeRouteAuthority(newGateway.cfg.Upstream.Zen)
	if oldGateway.scheduler.routeSessions != nil && newGateway.scheduler.routeSessions != nil {
		validScope := func(scope routeSessionScope) bool {
			if !validCreds[scope.CredID] {
				return false
			}
			// Both native scopes are proxy-independent: only pool validity
			// matters; legacy proxy-bound overrides (ProxyRaw != "") are
			// dropped and re-derived statelessly so an old proxy-bound
			// anonymous override never hits the new proxy-free scope.
			if scope.ProxyRaw != "" {
				return false
			}
			if _, ok := validPoolProxy[scope.Pool]; !ok {
				return false
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
				return scope.Pool == newGateway.authPoolName()
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
	Custom                   []bulkCustomAvailability    `json:"custom,omitempty"`
	Metadata                 MetadataSnapshot            `json:"metadata"`
}

// KeyStatus is the per-credential availability row (凭证可用性). ID is the
// key tail (never full secrets); Fingerprint is the stable redacted
// credential identity (SHA-256 prefix of tier+key, never raw key material and
// never tail-only) used by the per-row 检测 endpoint; Tier names the channel;
// ProxyPool is the assigned pool identity. Status is available/unavailable/rate_limited;
// Reason is the short machine reason (auth_failure, rate_limited,
// no_healthy_proxies, all_proxies_cooling, success, untested, ...).
// LastChecked is the admin bulk-check projection (never routing authority).
type KeyStatus struct {
	ID          string `json:"id"`
	Fingerprint string `json:"fingerprint,omitempty"`
	Tier        string `json:"tier"`
	Index       int    `json:"index"`
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
	// Status/Reason describe scheduler-derived transport availability and
	// remain for counts; Probe* carry the latest real minimal-inference
	// observation for this credential (status/reason/HTTP), empty when never
	// probed. LastChecked is the probe observation time, nil when unprobed.
	Status          string     `json:"status,omitempty"`
	Reason          string     `json:"reason,omitempty"`
	ProbeStatus     string     `json:"probe_status,omitempty"`
	ProbeReason     string     `json:"probe_reason,omitempty"`
	ProbeHTTPStatus int        `json:"probe_http_status,omitempty"`
	LastChecked     *time.Time `json:"last_checked,omitempty"`
}

// ProxyStatus is one pool-qualified proxy row. The same raw URL in different
// pools appears as independent rows (pool-qualified). Anonymous and
// Authenticated carry the latest real probe observations for that lane
// (success only on exact HTTP 200; 429 stays rate_limited; other HTTP and
// transport outcomes stay real; no_model/unconfigured/untested are machine
// outcomes, never a generic available verdict). HTTP statuses are present
// whenever an HTTP response exists. This view is observation-driven; scheduler
// cooldown state lives in the dedicated cooling tables, never here.
type ProxyStatus struct {
	Index    int    `json:"index"`
	Pool     string `json:"proxy_pool,omitempty"`
	Address  string `json:"address"`
	Healthy  bool   `json:"healthy"`
	Checking bool   `json:"checking"`
	// AuthKeys is the pool-level routed credential count for the single
	// authenticated lane. It is NOT a binding.
	AuthKeys int `json:"auth_keys"`
	// Deprecated aliases kept for compilation; never serialized.
	ZenKeys   int      `json:"-"`
	GoKeys    int      `json:"-"`
	Anonymous bool     `json:"anonymous"`
	Routing   []string `json:"routing,omitempty"`
	// AvailableCredentials is the pool-level credential count: auth keys
	// routed here, plus one when the anonymous credential is routed here.
	AvailableCredentials int `json:"available_credentials,omitempty"`
	// Transport is the independent transport observation for this node from
	// the latest availability probe (healthy/unhealthy/inconclusive), empty
	// when never probed. It never derives from Healthy: Healthy stays the
	// internal routing transport signal (defaults true) while Transport is
	// display-only probe evidence. Reason preserves the bulk node reason.
	Transport                string `json:"transport,omitempty"`
	Reason                   string `json:"reason,omitempty"`
	AnonymousObservation     string `json:"anonymous_observation,omitempty"`
	AnonymousHTTPStatus      int    `json:"anonymous_http_status,omitempty"`
	AuthenticatedObservation string `json:"authenticated_observation,omitempty"`
	AuthenticatedHTTPStatus  int    `json:"authenticated_http_status,omitempty"`
	// Deprecated scheduler-derived aliases; never serialized.
	Zen string `json:"-"`
	Go  string `json:"-"`
	// Deprecated scheduler-derived reason; always empty in the observation model.
	CooldownReason string     `json:"-"`
	LastChecked    *time.Time `json:"last_checked,omitempty"`
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
	credProbe := map[string]bulkCredentialAvailability{}
	if bulkSnap != nil {
		for _, c := range bulkSnap.Credentials {
			// Lookup by internal credential ID (tier-qualified); tail-only
			// keys would collapse collisions. Fall back to tail key for
			// snapshots written before CredID existed.
			if c.CredID != "" {
				credProbe[string(c.Tier)+"\x00"+c.CredID+"\x00"+c.Pool] = c
			} else {
				credProbe[string(c.Tier)+"\x00"+c.KeyTail+"\x00"+c.Pool] = c
			}
		}
		if bulkSnap.CheckedAt.IsZero() == false {
			value := bulkSnap.CheckedAt.UTC()
			result.AvailabilityCheckedAt = &value
			result.AvailabilityTruncated = bulkSnap.Truncated
			result.AvailabilityPartial = bulkSnap.Partial
		}
	}
	// Configured fallback channels always appear: project every configured
	// channel as untested, then overlay the latest stored custom observation.
	// Restart/Apply may clear observations but never drops configured rows.
	result.Custom = projectCustomResources(gateway.cfg, bulkSnap)
	authPoolName := gateway.authPoolName()
	result.Keys = append(result.Keys, keyStatusesForTier(gateway, "zen", gateway.credentials(), authPoolName, credProbe)...)
	routingFor := func(poolName string) []string {
		out := []string{}
		if gateway.cfg.ProxyRouting.Anonymous == poolName {
			out = append(out, "anonymous")
		}
		if authPoolName == poolName {
			out = append(out, "authenticated")
		}
		return out
	}
	anonPool := gateway.pools[gateway.cfg.ProxyRouting.Anonymous]
	credsForPool := func(poolName string) int {
		count := 0
		if authPoolName == poolName {
			count += len(gateway.credentials())
		}
		if gateway.cfg.Anonymous && gateway.cfg.ProxyRouting.Anonymous == poolName {
			count++
		}
		return count
	}
	// Observation snapshot lookup per pool-qualified proxy. The resource view
	// is observation-driven, never inferred from scheduler cooldowns.
	// Transport is the independent probe observation (healthy/unhealthy/
	// inconclusive), never derived from Healthy. Healthy stays the internal
	// routing signal; Transport is display-only and empty when unprobed.
	type nodeObs struct {
		transport, anon, auth, reason string
		anonCode, authCode            int
		checked                       *time.Time
	}
	nodeObsMap := map[string]*nodeObs{}
	if bulkSnap != nil {
		for _, n := range bulkSnap.Nodes {
			key := n.Pool + "\x00" + n.ProxyNode
			// Keep the latest row per pool-qualified node; snapshot rows are
			// already latest-merged, so first wins deterministically.
			if _, ok := nodeObsMap[key]; ok {
				continue
			}
			anon, anonCode := n.Anonymous, n.AnonymousHTTPStatus
			if anon == "" {
				anon = n.Zen
			}
			auth, authCode := n.Authenticated, n.AuthenticatedHTTPStatus
			if auth == "" {
				auth = n.Go
			}
			nodeObsMap[key] = &nodeObs{transport: n.Transport, anon: anon, auth: auth, anonCode: anonCode, authCode: authCode, reason: n.Reason, checked: n.LastChecked}
		}
	}
	for _, pool := range gateway.uniquePools() {
		authRouted := 0
		if authPoolName == pool.name {
			authRouted = len(gateway.credentials())
		}
		for _, proxy := range pool.items {
			healthy := proxy.healthy.Load()
			redacted := redactURL(proxy.name)
			obs := nodeObsMap[pool.name+"\x00"+redacted]
			anonObs, authObs := "untested", "untested"
			var anonHTTP, authHTTP int
			var lastChecked *time.Time
			var transport, reason string
			if obs != nil {
				if obs.anon != "" {
					anonObs = obs.anon
				}
				if obs.auth != "" {
					authObs = obs.auth
				}
				anonHTTP, authHTTP = obs.anonCode, obs.authCode
				transport = obs.transport
				reason = obs.reason
				lastChecked = obs.checked
			}
			// Unprobed lanes stay untested; lanes that never apply to this
			// pool stay untested as well. Authenticated without keys is
			// unconfigured rather than untested.
			if authObs == "untested" && pool.name == authPoolName && len(gateway.credentials()) == 0 {
				authObs = "unconfigured"
			}
			status := ProxyStatus{
				Index: proxy.index, Pool: pool.name, Address: redacted,
				Healthy: healthy, Checking: proxy.checking.Load(),
				AuthKeys: authRouted, ZenKeys: authRouted,
				Anonymous: gateway.cfg.Anonymous && anonPool == pool,
				Routing:   routingFor(pool.name), AvailableCredentials: credsForPool(pool.name),
				Transport: transport, Reason: reason,
				AnonymousObservation: anonObs, AnonymousHTTPStatus: anonHTTP,
				AuthenticatedObservation: authObs, AuthenticatedHTTPStatus: authHTTP,
				Zen: anonObs, Go: authObs,
			}
			// Per-node probe time only; never fall back to the global batch
			// timestamp so unprobed nodes stay empty (display "—").
			if lastChecked != nil {
				status.LastChecked = lastChecked
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

// proxyChannelLabel is a deprecated scheduler-derived helper kept for test
// compilation. Resource status is observation-driven; new code must read the
// bulkSnapshot observation instead.
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

// projectCustomResources projects every configured fallback channel as a row.
// Configured channels always appear, even before any probe (as untested);
// the latest stored custom observation overlays the matching row by stable
// channel ID (legacy name-only rows match by name). The row carries both the
// stable ID (machine key) and the display name (operator copy).
func projectCustomResources(cfg Config, snap *bulkAvailabilitySnapshot) []bulkCustomAvailability {
	stored := map[string]bulkCustomAvailability{}
	if snap != nil {
		for _, c := range snap.Custom {
			if key := customAvailabilityKey(c); key != "" {
				if _, ok := stored[key]; !ok {
					stored[key] = c
				}
			}
		}
	}
	out := make([]bulkCustomAvailability, 0, len(cfg.Fallback.Channels))
	for _, ch := range cfg.Fallback.Channels {
		if row, ok := stored[strings.TrimSpace(ch.ID)]; ok {
			// Refresh operator copy to the current display name/endpoint.
			row.ID = ch.ID
			row.Name = ch.Name
			row.BaseURL = redactURL(ch.BaseURL)
			row.Model = ch.Model
			out = append(out, row)
			continue
		}
		if row, ok := stored[strings.TrimSpace(ch.Name)]; ok {
			row.ID = ch.ID
			row.Name = ch.Name
			row.BaseURL = redactURL(ch.BaseURL)
			row.Model = ch.Model
			out = append(out, row)
			continue
		}
		out = append(out, bulkCustomAvailability{
			ID: ch.ID, Name: ch.Name, BaseURL: redactURL(ch.BaseURL), Model: ch.Model,
			Status: "untested", Reason: "untested",
		})
	}
	return out
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
		result = append(result, gateway.catalog.Diagnostic(model, "", len(gateway.cfg.Keys) > 0, gateway.cfg.Anonymous))
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
	return gateway.catalog.Diagnostic(model, requested, len(gateway.cfg.Keys) > 0, gateway.cfg.Anonymous)
}

// keyStatusesForTier snapshots credential-level state for one tier without
// taking pool locks: credential lists are immutable after build and all
// scheduler counters are mutex-guarded snapshots. Status/reason form the
// scheduler-derived transport view; Probe* carry the latest real probe
// observation (empty when never probed) with LastChecked as probe time only.
// Full secrets never appear.
func keyStatusesForTier(gateway *Gateway, tier string, creds []credentialRef, poolName string, probe map[string]bulkCredentialAvailability) []KeyStatus {
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
			ID: cred.display, Fingerprint: credentialFingerprint(cred.tier, cred.key), Tier: tier, Index: cred.index,
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
		// Probe observation overlay only; never fall back to the global batch
		// timestamp so unprobed credentials stay empty (display "—").
		if p, ok := probe[tier+"\x00"+cred.id+"\x00"+poolName]; ok {
			status.ProbeStatus = p.Status
			status.ProbeReason = p.Reason
			status.ProbeHTTPStatus = p.HTTPStatus
			status.LastChecked = p.LastChecked
		} else if p, ok := probe[tier+"\x00"+cred.display+"\x00"+poolName]; ok {
			status.ProbeStatus = p.Status
			status.ProbeReason = p.Reason
			status.ProbeHTTPStatus = p.HTTPStatus
			status.LastChecked = p.LastChecked
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

// credentialFingerprint is the stable redacted per-credential identity used
// by the per-row credential availability endpoint. It hashes tier+key so the
// same raw key text on zen and go yields distinct identities, never exposes
// raw key material, and is never tail-only (tails collide by design).
func credentialFingerprint(tier Tier, key string) string {
	return secretFingerprint(string(tier) + ":" + key)
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
