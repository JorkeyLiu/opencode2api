package main

import (
	"crypto/sha256"
	"encoding/hex"
	"hash/fnv"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Unified credential x proxy target scheduler.
//
// Six state layers with separate ownership:
//   - proxyTransport.healthy: transport connectivity only. Only isProxyFailure
//     (timeout/deadline/refused) may set unhealthy; HTTP statuses never do.
//   - proxy429State: per (tier/channel, pool, raw proxy) rate-limit cooldown,
//     429 only. Anonymous Zen and authenticated Zen share the Zen channel;
//     Go is isolated even for the same pool+URL. Pool isolation is preserved.
//     Model, credential, and client session never participate.
//   - channelState: per (tier/channel, pool, raw proxy) channel-availability
//     cooldown, 403/5xx only. Written only by comparative management probes
//     (same tier+credential success on one node plus 403/5xx on another);
//     lone 403/5xx without comparative success is display-only. Anonymous Zen
//     and authenticated Zen share the Zen channel; Go is isolated. Pool
//     isolation is preserved. Model, credential, and client session never
//     participate.
//   - credentialState: global per-credential cooldown, 401 only.
//   - credential429State: per (tier, credential) rate-limit cooldown, 429
//     only. Set only when one established/unbound authenticated request
//     observes 429 from two distinct eligible proxies in sequence with the
//     same credential. Single-proxy 429 never sets it.
//   - targetState: per (tier, credential, pool, proxy, model) cooldown for
//     403/5xx only. Transport errors, 408/425, and ordinary 4xx (including
//     400) are neutral no-ops and 2xx clears only the single target (plus the
//     credential 401 state and, when the send started at or after the latest
//     429, the tier-qualified proxy429/channel state and the credential429 state).
//
// Target identity uses the raw configured proxy URL string qualified by pool
// name for internal matching; external output always uses redactURL.
//
// Authenticated session+model bindings are proxy-independent: the durable
// identity fixes tier, credential, pool, model, protocol, and authority but
// not the proxy node. The pin's ProxyRaw is the mutable current/preferred
// proxy with generation fencing. Anonymous bindings remain full-target
// (proxy is identity) with no cross-proxy recovery.

const (
	// anonymousSchedulerCredentialID is the internal scheduler identity for the
	// shared public credential. It is distinct from anonymousCredentialID
	// ("anonymous"), which remains the observability display literal.
	anonymousSchedulerCredentialID = "zen:anonymous"

	// maxTargetSnapshotEntries bounds the resources targets list so K x P x M
	// state can never explode an admin response.
	maxTargetSnapshotEntries = 512

	// maxTargetStates bounds the live targetState map (credential x pool x
	// proxy x model). Creation at the cap first prunes expired-stale entries
	// (cooldown expired and last failure older than targetStaleRetention),
	// then deterministically evicts the oldest idle (not in active cooldown)
	// entry by (lastFailureAt, identity). If every entry is in active
	// cooldown, the new entry is still inserted and the map temporarily
	// exceeds the cap rather than breaking an active cooldown. No randomness.
	maxTargetStates = 4096

	// targetStaleRetention is how long an expired target failure memory is
	// kept for exponential-backoff escalation after its cooldown ends. Past
	// this retention and out of cooldown the entry is pruned.
	targetStaleRetention = targetBackoffCap

	// targetBackoffCap caps every non-429 computed cooldown (credential 401,
	// target 403/5xx, channel 403/5xx). Proxy429 and credential429 use the
	// configured 429 max (default 1h) instead; see rateLimitMax.
	targetBackoffCap = 5 * time.Minute

	// defaultRateLimitBaseSeconds is the 429 minimum/start/base when the
	// config field is missing or zero. Explicit nonzero values are kept.
	defaultRateLimitBaseSeconds = 300

	// defaultRateLimitMaxSeconds is the 429 maximum clamp when the config
	// field is missing or zero.
	defaultRateLimitMaxSeconds = 3600

	// maxProxy429States bounds the live proxy429State map (pool x proxy).
	// Each pool-qualified proxy holds at most one entry, so the live size is
	// proportional to proxy resources. Creation at the cap first prunes
	// expired-stale entries (cooldown expired and last failure older than
	// proxy429StaleRetention), then deterministically evicts the oldest idle
	// (not in active cooldown) entry by (lastFailureAt, identity). If every
	// entry is in active cooldown, the new entry is still inserted and the
	// map temporarily exceeds the cap rather than breaking an active
	// cooldown. No randomness. This cap is independent of sessionPinStoreCap.
	maxProxy429States = 1024

	// maxProxy429SnapshotEntries bounds the admin proxy rate-limit list so
	// proxy state can never explode an admin response.
	maxProxy429SnapshotEntries = 256

	// proxy429StaleRetention mirrors targetStaleRetention for the proxy429
	// layer: expired failure memory is kept for backoff escalation, then
	// pruned once out of cooldown and past retention.
	proxy429StaleRetention = targetBackoffCap

	// maxChannelStates bounds the live channel-availability map
	// (tier x pool x proxy). Each pool-qualified proxy holds at most one
	// entry per tier, so live size stays proportional to proxy resources.
	// Creation at the cap first prunes expired-stale entries, then
	// deterministically evicts the oldest idle entry. If every entry is in
	// active cooldown the map temporarily exceeds the cap rather than
	// breaking active state. No randomness.
	maxChannelStates = 1024

	// maxChannelSnapshotEntries bounds the admin channel-availability list.
	maxChannelSnapshotEntries = 256

	// channelStaleRetention mirrors targetStaleRetention for the channel
	// layer: expired failure memory is kept for backoff escalation, then
	// pruned once out of cooldown and past retention.
	channelStaleRetention = targetBackoffCap
)

// credentialIDForKey returns the internal credential identity:
// tier + SHA-256 fingerprint of the full key. The fingerprint never leaves
// the process; logs and APIs use keyDisplayID or the anonymous literal.
func credentialIDForKey(tier Tier, key string) string {
	sum := sha256.Sum256([]byte(key))
	return string(tier) + ":" + hex.EncodeToString(sum[:])
}

// targetCandidate is one frozen (credential, proxy, model) combination.
type targetCandidate struct {
	Tier        Tier
	CredKey     string // full key material ("public" for anonymous)
	CredID      string // internal credential identity
	CredDisplay string // safe display: key suffix or "anonymous"
	CredIndex   int    // config order, -1 for anonymous
	PoolName    string
	Proxy       *proxyTransport
	ProxyRaw    string // stable identity: raw configured URL string
	Model       string
	Identity    string // full target identity string
}

func targetIdentity(tier Tier, credID, pool, proxyRaw, model string) string {
	return string(tier) + "\x00" + credID + "\x00" + pool + "\x00" + proxyRaw + "\x00" + model
}

type credentialEntry struct {
	failures      uint32
	cooldownUntil int64 // unix nanos
	lastStatus    int
}

type targetEntry struct {
	failures         uint32
	cooldownUntil    int64 // unix nanos
	lastFailureAt    int64 // unix nanos, last noteTargetFailure time; drives retention/eviction
	lastFailureClass string
	lastStatus       int
	retryAfterUntil  int64 // unix nanos
}

// proxy429Entry is the global per-(pool, raw proxy) 429 rate-limit state.
// It never keys by model, tier, credential, channel, or client session.
type proxy429Entry struct {
	failures         uint32
	cooldownUntil    int64 // unix nanos
	lastFailureAt    int64 // unix nanos, last noteProxy429Failure wall time; drives retention/eviction
	lastStartedNanos int64 // send-start nanos of the latest recorded 429; guards 2xx clears
	lastFailureClass string
	lastStatus       int
	retryAfterUntil  int64 // unix nanos
}

func proxy429Identity(tier Tier, pool, proxyRaw string) string {
	return string(tier) + "\x00" + pool + "\x00" + proxyRaw
}

func parseProxy429Identity(identity string) (tier Tier, pool, proxyRaw string) {
	parts := splitNul3(identity)
	if len(parts) != 3 {
		return "", "", ""
	}
	return Tier(parts[0]), parts[1], parts[2]
}

func splitNul3(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}

// channelEntry is the per-(tier, pool, raw proxy) channel-availability
// cooldown for 403/5xx only. It filters one proxy for one tier across all
// models and credentials using that tier+pool+proxy. It is distinct from
// transport health (connectivity only), proxy429 (429 only),
// credential401/429 (per-credential global), and model-specific target
// 403/5xx (per credential x pool x proxy x model). Zen anonymous and
// authenticated Zen share the Zen tier entry; Go is isolated even for the
// same raw URL. Pool qualification is preserved: the same raw URL in
// different pools holds independent entries.
type channelEntry struct {
	failures         uint32
	cooldownUntil    int64 // unix nanos
	lastFailureAt    int64 // unix nanos, last noteChannelFailure wall time; drives retention/eviction
	lastStartedNanos int64 // send-start nanos of latest recorded failure; guards success clears
	lastFailureClass string
	lastStatus       int
	retryAfterUntil  int64 // unix nanos
}

func channelIdentity(tier Tier, pool, proxyRaw string) string {
	return string(tier) + "\x00" + pool + "\x00" + proxyRaw
}

func parseChannelIdentity(identity string) (tier Tier, pool, proxyRaw string) {
	return parseProxy429Identity(identity)
}

type targetScheduler struct {
	mu                sync.Mutex
	baseCooldown      time.Duration
	rateLimitCooldown time.Duration
	rateLimitMaxDur   time.Duration
	credState         map[string]*credentialEntry
	cred429State      map[string]*credential429Entry
	targetState       map[string]*targetEntry
	proxy429State     map[string]*proxy429Entry
	channelState      map[string]*channelEntry
	credDisplay       map[string]string
	roundRobin        atomic.Uint64
	routeSessions     *routeSessionStore
	pins              *sessionPinStore
	fallbacks         *fallbackTakeoverStore
}

func newTargetScheduler(baseCooldown time.Duration, rateLimitBases ...time.Duration) *targetScheduler {
	if baseCooldown <= 0 {
		baseCooldown = 15 * time.Second
	}
	rateLimitCooldown := baseCooldown
	if len(rateLimitBases) > 0 && rateLimitBases[0] > 0 {
		rateLimitCooldown = rateLimitBases[0]
	} else if len(rateLimitBases) == 0 {
		// Single-arg legacy construction keeps both bases equal.
		rateLimitCooldown = baseCooldown
	} else if rateLimitCooldown <= 0 {
		rateLimitCooldown = defaultRateLimitBaseSeconds * time.Second
	}
	rateLimitMax := defaultRateLimitMaxSeconds * time.Second
	if len(rateLimitBases) > 1 && rateLimitBases[1] > 0 {
		rateLimitMax = rateLimitBases[1]
	}
	if rateLimitMax < rateLimitCooldown {
		rateLimitMax = rateLimitCooldown
	}
	return &targetScheduler{
		baseCooldown:      baseCooldown,
		rateLimitCooldown: rateLimitCooldown,
		rateLimitMaxDur:   rateLimitMax,
		credState:         make(map[string]*credentialEntry),
		cred429State:      make(map[string]*credential429Entry),
		targetState:       make(map[string]*targetEntry),
		proxy429State:     make(map[string]*proxy429Entry),
		channelState:      make(map[string]*channelEntry),
		credDisplay:       make(map[string]string),
		routeSessions:     newRouteSessionStore(),
		pins:              newSessionPinStore(),
		fallbacks:         newFallbackTakeoverStore(),
	}
}

// credential429Entry is the per-(tier, credential) rate-limit cooldown. The
// map key is the internal credential ID (already tier-qualified via
// credentialIDForKey), so Zen and Go stay isolated even for identical key
// text. Only two-distinct-proxy 429 evidence sets it; single-proxy 429 never
// does. Success started at/after the latest credential-429 clears it.
type credential429Entry struct {
	failures         uint32
	cooldownUntil    int64 // unix nanos
	lastFailureAt    int64 // unix nanos, last noteCredential429Failure time
	lastStartedNanos int64 // send-start nanos of latest recorded 429
	lastFailureClass string
	lastStatus       int
	retryAfterUntil  int64 // unix nanos
}

// Route-session layer: client session vs upstream route session.
//
// The client session (ids.Session) is only affinity input for HRW ordering.
// The upstream route session is what leaves the process in
// x-opencode-session / x-session-affinity / X-Session-Id and in the prepared
// body session fields. Its scope is target-bound: upstream authority
// (normalized base URL), tier, internal credential identity, proxy pool, raw
// proxy identity, and target protocol. Model is intentionally excluded so a
// rotation does not fragment per model. Raw key material never enters the
// scope; only the internal credential ID does. Scope keys, tokens, and raw
// identities never enter projections, logs, metrics, or admin output.

// routeSessionStoreCap bounds the in-memory 400-rotation override map.
// First-generation sessions are stateless derivations and never stored;
// only post-rotation overrides occupy entries.
const routeSessionStoreCap = 4096

// routeSessionIdleTTL expires an override that has not been used recently.
// Expiry is checked on access and during insert/migration pruning.
const routeSessionIdleTTL = 30 * time.Minute

// routeSessionScope is the target-bound identity for one upstream route
// session. It deliberately excludes model and any secret material.
type routeSessionScope struct {
	Authority string
	Tier      Tier
	CredID    string
	Pool      string
	ProxyRaw  string
	Protocol  Protocol
}

func normalizeRouteAuthority(baseURL string) string {
	return strings.TrimRight(strings.TrimSpace(baseURL), "/")
}

func routeScopeForCandidate(authority string, cand targetCandidate, protocol Protocol) routeSessionScope {
	// Authenticated sessions are NOT proxy-affine: the route-session scope
	// excludes the proxy node so moving within the bound pool preserves the
	// same upstream session value. Anonymous Zen remains exactly proxy-affine.
	proxyRaw := cand.ProxyRaw
	if cand.CredID != anonymousSchedulerCredentialID {
		proxyRaw = ""
	}
	return routeSessionScope{
		Authority: normalizeRouteAuthority(authority),
		Tier:      cand.Tier,
		CredID:    cand.CredID,
		Pool:      cand.PoolName,
		ProxyRaw:  proxyRaw,
		Protocol:  protocol,
	}
}

func (s routeSessionScope) key() string {
	return s.Authority + "\x00" + string(s.Tier) + "\x00" + s.CredID + "\x00" + s.Pool + "\x00" + s.ProxyRaw + "\x00" + string(s.Protocol)
}

// deriveFirstRouteSession is the stable, target-bound first generation:
// SHA-256 over an independent domain, the client session key, and the target
// scope. The rss_ prefix keeps it disjoint from the ses_ client namespace.
// The same client+target is stable across requests; different targets always
// differ.
func deriveFirstRouteSession(clientSession string, scope routeSessionScope) string {
	sum := sha256.Sum256([]byte("route-session-v1\x00" + clientSession + "\x00" + scope.key()))
	return "rss_" + hex.EncodeToString(sum[:12])
}

func newRouteSessionToken() string {
	return randomID("rss", 12)
}

type routeSessionEntry struct {
	token    string
	lastUsed int64 // unix nanos
	scope    routeSessionScope
}

type routeSessionStore struct {
	mu      sync.Mutex
	entries map[string]*routeSessionEntry
}

func newRouteSessionStore() *routeSessionStore {
	return &routeSessionStore{entries: make(map[string]*routeSessionEntry)}
}

func routeSessionMapKey(clientSession string, scope routeSessionScope) string {
	return clientSession + "\x00" + scope.key()
}

// routeSessionFor returns the current upstream route session for one client
// session + target scope: the stored override when present and fresh,
// otherwise the stable first generation. Access refreshes idle TTL.
func (s *targetScheduler) routeSessionFor(clientSession string, scope routeSessionScope) string {
	if s == nil || s.routeSessions == nil {
		return deriveFirstRouteSession(clientSession, scope)
	}
	return s.routeSessions.get(clientSession, scope)
}

// rotateRouteSession implements compare-and-rotate: when the stored token
// still equals the observed token (or no override exists yet), it installs a
// fresh cryptographically random token; when a concurrent request already
// rotated, it returns the current token without generating a second
// generation.
func (s *targetScheduler) rotateRouteSession(clientSession string, scope routeSessionScope, observed string) string {
	if s == nil || s.routeSessions == nil {
		return newRouteSessionToken()
	}
	return s.routeSessions.rotate(clientSession, scope, observed)
}

func (st *routeSessionStore) get(clientSession string, scope routeSessionScope) string {
	if st == nil {
		return deriveFirstRouteSession(clientSession, scope)
	}
	key := routeSessionMapKey(clientSession, scope)
	now := time.Now().UnixNano()
	st.mu.Lock()
	defer st.mu.Unlock()
	if entry, ok := st.entries[key]; ok {
		if now-entry.lastUsed > int64(routeSessionIdleTTL) {
			delete(st.entries, key)
		} else {
			entry.lastUsed = now
			return entry.token
		}
	}
	return deriveFirstRouteSession(clientSession, scope)
}

// rotate generates the candidate token before taking the lock so crypto/rand
// never blocks the critical section, then confirms inside the lock
// (CAS-like): only the holder of the observed generation installs the new
// one.
func (st *routeSessionStore) rotate(clientSession string, scope routeSessionScope, observed string) string {
	candidate := newRouteSessionToken()
	if st == nil {
		return candidate
	}
	key := routeSessionMapKey(clientSession, scope)
	now := time.Now().UnixNano()
	st.mu.Lock()
	defer st.mu.Unlock()
	if entry, ok := st.entries[key]; ok {
		if now-entry.lastUsed > int64(routeSessionIdleTTL) {
			entry.token = candidate
			entry.lastUsed = now
			entry.scope = scope
			return candidate
		}
		if entry.token != observed {
			entry.lastUsed = now
			return entry.token
		}
		entry.token = candidate
		entry.lastUsed = now
		return candidate
	}
	if len(st.entries) >= routeSessionStoreCap {
		st.pruneExpiredLocked(now)
		if len(st.entries) >= routeSessionStoreCap {
			st.evictOldestLocked()
		}
	}
	if st.entries == nil {
		st.entries = make(map[string]*routeSessionEntry)
	}
	st.entries[key] = &routeSessionEntry{token: candidate, lastUsed: now, scope: scope}
	return candidate
}

func (st *routeSessionStore) pruneExpiredLocked(now int64) {
	for key, entry := range st.entries {
		if entry == nil || now-entry.lastUsed > int64(routeSessionIdleTTL) {
			delete(st.entries, key)
		}
	}
}

// evictOldestLocked deterministically removes the least-recently-used entry
// (smallest lastUsed, tie-break smallest key). Route overrides carry no
// active-cooldown obligation, so eviction always applies at the cap.
func (st *routeSessionStore) evictOldestLocked() {
	var victim string
	var victimAt int64
	found := false
	for key, entry := range st.entries {
		at := int64(0)
		if entry != nil {
			at = entry.lastUsed
		}
		if !found || at < victimAt || (at == victimAt && key < victim) {
			victim, victimAt, found = key, at, true
		}
	}
	if found {
		delete(st.entries, victim)
	}
}

func (st *routeSessionStore) count() int {
	if st == nil {
		return 0
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.entries)
}

// migrateRouteSessionsFrom copies still-fresh overrides whose target scope
// still exists in the new Gateway (checked via valid). Expired entries never
// cross Apply; new scopes start stateless and removed scopes are dropped.
// Insertion respects the cap with the same prune-then-evict policy.
func (st *routeSessionStore) migrateRouteSessionsFrom(old *routeSessionStore, valid func(routeSessionScope) bool) int {
	if st == nil || old == nil || st == old {
		return 0
	}
	now := time.Now().UnixNano()
	old.mu.Lock()
	type copied struct {
		key   string
		entry routeSessionEntry
	}
	staged := make([]copied, 0, len(old.entries))
	for key, entry := range old.entries {
		if entry == nil || entry.token == "" {
			continue
		}
		if now-entry.lastUsed > int64(routeSessionIdleTTL) {
			continue
		}
		staged = append(staged, copied{key: key, entry: *entry})
	}
	old.mu.Unlock()
	sort.Slice(staged, func(i, j int) bool { return staged[i].key < staged[j].key })
	migrated := 0
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, item := range staged {
		if valid != nil && !valid(item.entry.scope) {
			continue
		}
		if _, ok := st.entries[item.key]; ok {
			continue
		}
		if len(st.entries) >= routeSessionStoreCap {
			st.pruneExpiredLocked(now)
			if len(st.entries) >= routeSessionStoreCap {
				st.evictOldestLocked()
			}
		}
		fresh := item.entry
		fresh.lastUsed = now
		st.entries[item.key] = &fresh
		migrated++
	}
	return migrated
}

// maxDurationValue is the saturation ceiling for duration arithmetic so
// absurd config values never wrap negative through time.Duration overflow.
const maxDurationValue = time.Duration(math.MaxInt64)

// secondsToDuration converts whole seconds to a duration, saturating at
// MaxInt64 instead of overflowing for absurd values.
func secondsToDuration(seconds int) time.Duration {
	if seconds <= 0 {
		return 0
	}
	if int64(seconds) > int64(maxDurationValue)/int64(time.Second) {
		return maxDurationValue
	}
	return time.Duration(seconds) * time.Second
}

// cooldownDeadline returns nowNanos+delay, saturating at MaxInt64 so absurd
// configured maxima never wrap negative through int64 overflow.
func cooldownDeadline(nowNanos int64, delay time.Duration) int64 {
	if delay <= 0 {
		return nowNanos
	}
	if int64(delay) > math.MaxInt64-nowNanos {
		return math.MaxInt64
	}
	return nowNanos + int64(delay)
}

// saturatingShiftLeft returns base*2^shift, saturating at MaxInt64.
func saturatingShiftLeft(base time.Duration, shift uint32) time.Duration {
	if base <= 0 {
		return 0
	}
	delay := base
	for i := uint32(0); i < shift; i++ {
		if delay > maxDurationValue/2 {
			return maxDurationValue
		}
		delay *= 2
	}
	return delay
}

// deterministicJitter returns delay scaled by +/-20%, derived from
// FNV-1a(identity + failure count) so tests are stable and concurrent
// targets do not expire simultaneously. No global rand is used. The float
// multiply is pre-clamped so huge delays saturate instead of overflowing
// the float-to-int conversion.
func deterministicJitter(delay time.Duration, identity string, failures uint32) time.Duration {
	if delay <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(identity))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strconv.FormatUint(uint64(failures), 10)))
	frac := float64(h.Sum64()%4001) / 4000.0
	factor := 0.8 + 0.4*frac
	scaled := float64(delay) * factor
	if scaled >= float64(maxDurationValue) {
		return maxDurationValue
	}
	out := time.Duration(scaled)
	if out < 0 {
		return maxDurationValue
	}
	return out
}

// backoffDelay computes base*2^min(failures-1,3) with deterministic jitter,
// taking the larger of the jittered backoff and retryAfter, capped at 5min.
// It preserves the non-429 (credential 401 / target / channel) behavior.
func (s *targetScheduler) backoffDelay(failures uint32, identity string, retryAfter time.Duration) time.Duration {
	return backoffDelayForBase(s.failureBase(), failures, identity, retryAfter)
}

// rateLimitBackoffDelay uses the 429 minimum/start/base with the same
// exponential/jitter/Retry-After conventions as backoffDelay, but clamps at
// the configured 429 maximum (default 1h), not the generic 5-minute cap.
func (s *targetScheduler) rateLimitBackoffDelay(failures uint32, identity string, retryAfter time.Duration) time.Duration {
	return backoffDelayForBaseWithCap(s.rateLimitBase(), s.rateLimitMax(), failures, identity, retryAfter)
}

func backoffDelayForBase(base time.Duration, failures uint32, identity string, retryAfter time.Duration) time.Duration {
	return backoffDelayForBaseWithCap(base, targetBackoffCap, failures, identity, retryAfter)
}

func backoffDelayForBaseWithCap(base, cap time.Duration, failures uint32, identity string, retryAfter time.Duration) time.Duration {
	if base <= 0 {
		base = 15 * time.Second
	}
	if cap <= 0 {
		cap = targetBackoffCap
	}
	if cap > maxDurationValue {
		cap = maxDurationValue
	}
	if failures < 1 {
		failures = 1
	}
	shift := failures - 1
	if shift > 3 {
		shift = 3
	}
	delay := saturatingShiftLeft(base, shift)
	if delay > cap {
		delay = cap
	}
	delay = deterministicJitter(delay, identity, failures)
	if delay > cap {
		delay = cap
	}
	if retryAfter < 0 {
		retryAfter = 0
	}
	if retryAfter > delay {
		delay = retryAfter
	}
	if delay > cap {
		delay = cap
	}
	if delay < 0 {
		delay = 0
	}
	return delay
}

func (s *targetScheduler) failureBase() time.Duration {
	if s == nil || s.baseCooldown <= 0 {
		return 15 * time.Second
	}
	return s.baseCooldown
}

func (s *targetScheduler) rateLimitBase() time.Duration {
	if s == nil || s.rateLimitCooldown <= 0 {
		if s == nil {
			return defaultRateLimitBaseSeconds * time.Second
		}
		return s.failureBase()
	}
	return s.rateLimitCooldown
}

func (s *targetScheduler) rateLimitMax() time.Duration {
	if s == nil || s.rateLimitMaxDur <= 0 {
		return defaultRateLimitMaxSeconds * time.Second
	}
	return s.rateLimitMaxDur
}

// credentialCoolUntil returns the credential cooldown deadline (nanos), or 0.
func (s *targetScheduler) credentialCoolUntil(credID string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry := s.credState[credID]; entry != nil {
		return entry.cooldownUntil
	}
	return 0
}

// targetCoolUntil returns the target cooldown deadline (nanos), or 0.
func (s *targetScheduler) targetCoolUntil(identity string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry := s.targetState[identity]; entry != nil {
		return entry.cooldownUntil
	}
	return 0
}

// credentialChange is a scheduler-owned state delta snapshot. It carries only
// counts and deadlines; key material, fingerprints, and proxy URLs never
// appear here. The Gateway owns log emission from these snapshots so the
// scheduler never depends on a logger.
type credentialChange struct {
	Changed       bool
	Cleared       bool
	Failures      uint32
	CooldownUntil int64
	PreviousUntil int64
}

// targetChange is the per-target equivalent of credentialChange. ProxyRaw is
// intentionally absent: callers already hold the redacted node label.
type targetChange struct {
	Changed       bool
	Cleared       bool
	Failures      uint32
	CooldownUntil int64
	PreviousUntil int64
	FailureClass  string
	Status        int
}

// noteCredentialAuthFailure applies the global 401 credential cooldown.
func (s *targetScheduler) noteCredentialAuthFailure(credID string) credentialChange {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.credState[credID]
	var previous int64
	if entry == nil {
		entry = &credentialEntry{}
		s.credState[credID] = entry
	} else {
		previous = entry.cooldownUntil
	}
	entry.failures++
	delay := s.backoffDelayLocked(entry.failures, credID, 0)
	entry.cooldownUntil = cooldownDeadline(now.UnixNano(), delay)
	entry.lastStatus = 401
	return credentialChange{
		Changed: entry.cooldownUntil > previous, Failures: entry.failures,
		CooldownUntil: entry.cooldownUntil, PreviousUntil: previous,
	}
}

// credentialCooldownStatus reports an active credential cooldown with its
// sanitized HTTP status (401; legacy entries without a stored status also
// report 401). It returns ok=false when no cooldown is active.
func (s *targetScheduler) credentialCooldownStatus(credID string) (until int64, status int, ok bool) {
	now := time.Now().UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.credState[credID]
	if entry == nil || entry.cooldownUntil <= now {
		return 0, 0, false
	}
	status = entry.lastStatus
	if status == 0 {
		status = 401
	}
	return entry.cooldownUntil, status, true
}

// targetCooldownStatus reports an active per-target cooldown with its stored
// sanitized HTTP status. It returns ok=false when no cooldown is active.
func (s *targetScheduler) targetCooldownStatus(identity string) (until int64, status int, ok bool) {
	now := time.Now().UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.targetState[identity]
	if entry == nil || entry.cooldownUntil <= now {
		return 0, 0, false
	}
	status = entry.lastStatus
	if status == 0 {
		status = 502
	}
	return entry.cooldownUntil, status, true
}

// noteTargetFailure cools one target identity for 403/5xx only. 429 never
// reaches this helper: it cools the pool-qualified proxy globally via
// noteProxy429Failure. Transport errors never reach this helper: they stay
// neutral and only trigger the async proxy health verification. retryAfter
// (from a Retry-After header) takes effect only when larger, and the total
// is capped at 5 minutes. The failure count is retained after cooldown
// expiry for targetStaleRetention so the next failure escalates; success
// deletes the entry.
func (s *targetScheduler) noteTargetFailure(identity, failureClass string, status int, retryAfter time.Duration) targetChange {
	now := time.Now()
	nowNanos := now.UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.targetState[identity]
	var previous int64
	if entry == nil {
		if len(s.targetState) >= maxTargetStates {
			s.pruneStaleTargetsLocked(nowNanos)
			if len(s.targetState) >= maxTargetStates {
				// Deterministic eviction of the oldest idle entry; when all
				// entries are in active cooldown the map temporarily exceeds
				// the cap instead of breaking active state.
				s.evictOldestIdleTargetLocked(nowNanos)
			}
		}
		entry = &targetEntry{}
		s.targetState[identity] = entry
	} else {
		previous = entry.cooldownUntil
	}
	entry.failures++
	delay := s.backoffDelayLocked(entry.failures, identity, retryAfter)
	entry.cooldownUntil = cooldownDeadline(now.UnixNano(), delay)
	entry.lastFailureAt = nowNanos
	entry.lastFailureClass = failureClass
	entry.lastStatus = status
	if retryAfter > 0 {
		entry.retryAfterUntil = cooldownDeadline(now.UnixNano(), retryAfter)
	}
	return targetChange{
		Changed: entry.cooldownUntil > previous, Failures: entry.failures,
		CooldownUntil: entry.cooldownUntil, PreviousUntil: previous,
		FailureClass: failureClass, Status: status,
	}
}

// pruneStaleTargetsLocked deletes entries whose cooldown has expired and whose
// last failure is older than targetStaleRetention. Caller holds s.mu. Only
// called from low-frequency paths (creation at cap, snapshot, migration
// helpers), never from the per-candidate hot loop.
func (s *targetScheduler) pruneStaleTargetsLocked(nowNanos int64) {
	for identity, entry := range s.targetState {
		if entry == nil {
			delete(s.targetState, identity)
			continue
		}
		if entry.cooldownUntil > nowNanos {
			continue
		}
		if entry.lastFailureAt == 0 || nowNanos-entry.lastFailureAt > int64(targetStaleRetention) {
			delete(s.targetState, identity)
		}
	}
}

// evictOldestIdleTargetLocked deterministically deletes the oldest entry that
// is not in active cooldown (smallest lastFailureAt, tie-break smallest
// identity). Returns true when an entry was evicted. Caller holds s.mu.
func (s *targetScheduler) evictOldestIdleTargetLocked(nowNanos int64) bool {
	var victim string
	var victimAt int64
	found := false
	for identity, entry := range s.targetState {
		if entry == nil || entry.cooldownUntil > nowNanos {
			continue
		}
		at := entry.lastFailureAt
		if !found || at < victimAt || (at == victimAt && identity < victim) {
			victim, victimAt, found = identity, at, true
		}
	}
	if !found {
		return false
	}
	delete(s.targetState, victim)
	return true
}

// proxy429Change is the pool-qualified proxy rate-limit delta snapshot. It
// carries only counts and deadlines; pool names are operator identities and
// the redacted node label is resolved by callers. Raw proxy URLs never
// appear here.
type proxy429Change struct {
	Changed       bool
	Cleared       bool
	Failures      uint32
	CooldownUntil int64
	PreviousUntil int64
	FailureClass  string
	Status        int
}

// noteTargetSuccess clears only the single target identity.
func (s *targetScheduler) noteTargetSuccess(identity string) targetChange {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.targetState[identity]
	if entry == nil {
		return targetChange{}
	}
	failures := entry.failures
	delete(s.targetState, identity)
	return targetChange{Cleared: true, Changed: true, Failures: failures}
}

// noteCredentialSuccess clears the credential 401 cooldown/failures.
func (s *targetScheduler) noteCredentialSuccess(credID string) credentialChange {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.credState[credID]
	if entry == nil {
		return credentialChange{}
	}
	failures := entry.failures
	delete(s.credState, credID)
	return credentialChange{Cleared: true, Changed: true, Failures: failures}
}

// noteCredential429Failure sets the per-(tier, credential) rate-limit
// cooldown. Callers must only invoke it after observing 429 from two distinct
// eligible proxies in sequence with the same credential in one request; a
// single-proxy 429 must never reach here. Retry-After uses the second 429
// with deterministic capped backoff conventions. The key is the internal
// credential ID (already tier-qualified), so identical key text on Zen vs Go
// stays isolated.
func (s *targetScheduler) noteCredential429Failure(credID, failureClass string, status int, retryAfter time.Duration, startedNanos int64) credentialChange {
	now := time.Now()
	nowNanos := now.UnixNano()
	if startedNanos == 0 {
		startedNanos = nowNanos
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.cred429State[credID]
	var previous int64
	if entry == nil {
		entry = &credential429Entry{}
		if s.cred429State == nil {
			s.cred429State = make(map[string]*credential429Entry)
		}
		s.cred429State[credID] = entry
	} else {
		previous = entry.cooldownUntil
	}
	entry.failures++
	delay := s.rateLimitDelayLocked(entry.failures, "cred429\x00"+credID, retryAfter)
	entry.cooldownUntil = cooldownDeadline(now.UnixNano(), delay)
	entry.lastFailureAt = nowNanos
	if startedNanos > entry.lastStartedNanos {
		entry.lastStartedNanos = startedNanos
	}
	entry.lastFailureClass = failureClass
	entry.lastStatus = status
	if retryAfter > 0 {
		entry.retryAfterUntil = cooldownDeadline(now.UnixNano(), retryAfter)
	}
	return credentialChange{
		Changed: entry.cooldownUntil > previous, Failures: entry.failures,
		CooldownUntil: entry.cooldownUntil, PreviousUntil: previous,
	}
}

// credential429CooldownStatus reports an active per-credential 429 cooldown.
func (s *targetScheduler) credential429CooldownStatus(credID string) (until int64, status int, ok bool) {
	now := time.Now().UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.cred429State[credID]
	if entry == nil || entry.cooldownUntil <= now {
		return 0, 0, false
	}
	status = entry.lastStatus
	if status == 0 {
		status = 429
	}
	return entry.cooldownUntil, status, true
}

// noteCredential429Success clears the credential429 state only when the
// successful send started at or after the latest recorded credential-429.
// Stale in-flight success must not clear a newer cooldown.
func (s *targetScheduler) noteCredential429Success(credID string, startedNanos int64) credentialChange {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.cred429State[credID]
	if entry == nil {
		return credentialChange{}
	}
	if startedNanos == 0 {
		startedNanos = time.Now().UnixNano()
	}
	if startedNanos < entry.lastStartedNanos {
		return credentialChange{}
	}
	failures := entry.failures
	delete(s.cred429State, credID)
	return credentialChange{Cleared: true, Changed: true, Failures: failures}
}

// credential429Snapshot returns failures/cooldown for one credential429 identity.
func (s *targetScheduler) credential429Snapshot(credID string) (failures uint32, cooldownUntil int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry := s.cred429State[credID]; entry != nil {
		return entry.failures, entry.cooldownUntil
	}
	return 0, 0
}

// proxy429CooldownStatus reports an active tier-qualified proxy 429 cooldown
// with its stored sanitized HTTP status (429). It returns ok=false when no
// cooldown is active. The identity is (tier/channel, pool, raw proxy) only:
// model, credential, and client session never participate. Anonymous and
// authenticated Zen share the Zen channel; Go is isolated.
func (s *targetScheduler) proxy429CooldownStatus(tier Tier, pool, proxyRaw string) (until int64, status int, ok bool) {
	now := time.Now().UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.proxy429State[proxy429Identity(tier, pool, proxyRaw)]
	if entry == nil || entry.cooldownUntil <= now {
		return 0, 0, false
	}
	status = entry.lastStatus
	if status == 0 {
		status = 429
	}
	return entry.cooldownUntil, status, true
}

// proxy429CoolUntil returns the proxy429 cooldown deadline (nanos), or 0.
func (s *targetScheduler) proxy429CoolUntil(tier Tier, pool, proxyRaw string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry := s.proxy429State[proxy429Identity(tier, pool, proxyRaw)]; entry != nil {
		return entry.cooldownUntil
	}
	return 0
}

// noteProxy429Failure records a live 429 on one tier-qualified proxy. The
// cooldown is shared across models, credentials, and client sessions using
// that tier/channel+pool+proxy; other tiers (Zen vs Go) stay isolated even
// for the same pool+URL. Anonymous and authenticated Zen share the Zen
// channel. It uses the existing deterministic exponential backoff with
// Retry-After max/cap behavior. startedNanos is the send-start time of the
// failing send; a zero value falls back to now. The stored start clock only
// moves forward so multiple newer failures remain authoritative while a stale
// late-arriving failure still escalates backoff without moving the clear
// watermark backwards. No same-target retry is implied here; callers decide
// retry behavior separately.
func (s *targetScheduler) noteProxy429Failure(tier Tier, pool, proxyRaw, failureClass string, status int, retryAfter time.Duration, startedNanos int64) proxy429Change {
	now := time.Now()
	nowNanos := now.UnixNano()
	if startedNanos == 0 {
		startedNanos = nowNanos
	}
	identity := proxy429Identity(tier, pool, proxyRaw)
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.proxy429State[identity]
	var previous int64
	if entry == nil {
		if len(s.proxy429State) >= maxProxy429States {
			s.pruneStaleProxy429Locked(nowNanos)
			if len(s.proxy429State) >= maxProxy429States {
				s.evictOldestIdleProxy429Locked(nowNanos)
			}
		}
		entry = &proxy429Entry{}
		if s.proxy429State == nil {
			s.proxy429State = make(map[string]*proxy429Entry)
		}
		s.proxy429State[identity] = entry
	} else {
		previous = entry.cooldownUntil
	}
	entry.failures++
	delay := s.rateLimitDelayLocked(entry.failures, identity, retryAfter)
	entry.cooldownUntil = cooldownDeadline(now.UnixNano(), delay)
	entry.lastFailureAt = nowNanos
	if startedNanos > entry.lastStartedNanos {
		entry.lastStartedNanos = startedNanos
	}
	entry.lastFailureClass = failureClass
	entry.lastStatus = status
	if retryAfter > 0 {
		entry.retryAfterUntil = cooldownDeadline(now.UnixNano(), retryAfter)
	}
	return proxy429Change{
		Changed: entry.cooldownUntil > previous, Failures: entry.failures,
		CooldownUntil: entry.cooldownUntil, PreviousUntil: previous,
		FailureClass: failureClass, Status: status,
	}
}

// noteProxy429Success clears the tier-qualified proxy429 state, but only when
// the successful send started at or after the latest recorded 429 failure.
// A 2xx from a request already in flight before a newer 429 must not clear
// it. A zero startedNanos is treated as now (newest) to preserve the
// historical clear path for callers without send-start tracking.
func (s *targetScheduler) noteProxy429Success(tier Tier, pool, proxyRaw string, startedNanos int64) proxy429Change {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity := proxy429Identity(tier, pool, proxyRaw)
	entry := s.proxy429State[identity]
	if entry == nil {
		return proxy429Change{}
	}
	if startedNanos == 0 {
		startedNanos = time.Now().UnixNano()
	}
	if startedNanos < entry.lastStartedNanos {
		return proxy429Change{}
	}
	failures := entry.failures
	delete(s.proxy429State, identity)
	return proxy429Change{Cleared: true, Changed: true, Failures: failures}
}

// pruneStaleProxy429Locked deletes entries whose cooldown has expired and
// whose last failure is older than proxy429StaleRetention. Caller holds s.mu.
func (s *targetScheduler) pruneStaleProxy429Locked(nowNanos int64) {
	for identity, entry := range s.proxy429State {
		if entry == nil {
			delete(s.proxy429State, identity)
			continue
		}
		if entry.cooldownUntil > nowNanos {
			continue
		}
		if entry.lastFailureAt == 0 || nowNanos-entry.lastFailureAt > int64(proxy429StaleRetention) {
			delete(s.proxy429State, identity)
		}
	}
}

// evictOldestIdleProxy429Locked deterministically deletes the oldest entry
// that is not in active cooldown (smallest lastFailureAt, tie-break smallest
// identity). Returns true when an entry was evicted. Caller holds s.mu.
func (s *targetScheduler) evictOldestIdleProxy429Locked(nowNanos int64) bool {
	var victim string
	var victimAt int64
	found := false
	for identity, entry := range s.proxy429State {
		if entry == nil || entry.cooldownUntil > nowNanos {
			continue
		}
		at := entry.lastFailureAt
		if !found || at < victimAt || (at == victimAt && identity < victim) {
			victim, victimAt, found = identity, at, true
		}
	}
	if !found {
		return false
	}
	delete(s.proxy429State, victim)
	return true
}

// channelChange is the pool-qualified channel-availability delta snapshot.
// It carries only counts and deadlines; pool names are operator identities
// and the redacted node label is resolved by callers. Raw proxy URLs never
// appear here.
type channelChange struct {
	Changed       bool
	Cleared       bool
	Failures      uint32
	CooldownUntil int64
	PreviousUntil int64
	FailureClass  string
	Status        int
}

// channelCooldownStatus reports an active tier-qualified channel cooldown
// with its stored sanitized HTTP status (403/5xx). It returns ok=false when
// no cooldown is active. The identity is (tier, pool, raw proxy) only:
// model, credential, and client session never participate. Anonymous and
// authenticated Zen share the Zen tier entry; Go is isolated.
func (s *targetScheduler) channelCooldownStatus(tier Tier, pool, proxyRaw string) (until int64, status int, ok bool) {
	now := time.Now().UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.channelState[channelIdentity(tier, pool, proxyRaw)]
	if entry == nil || entry.cooldownUntil <= now {
		return 0, 0, false
	}
	status = entry.lastStatus
	if status == 0 {
		status = 502
	}
	return entry.cooldownUntil, status, true
}

// channelCoolUntil returns the channel cooldown deadline (nanos), or 0.
func (s *targetScheduler) channelCoolUntil(tier Tier, pool, proxyRaw string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry := s.channelState[channelIdentity(tier, pool, proxyRaw)]; entry != nil {
		return entry.cooldownUntil
	}
	return 0
}

// noteChannelFailure records a comparative 403/5xx on one tier-qualified
// proxy. Callers must only invoke it after observing success on another
// node with the same credential (or public credential) in the same tier:
// a lone 403/5xx without comparative success is display-only and must never
// reach here. It uses deterministic exponential backoff with Retry-After
// max/cap conventions. startedNanos is the send-start time; zero falls back
// to now. The stored start clock only moves forward so newer failures stay
// authoritative while stale late arrivals still escalate backoff without
// moving the clear watermark backwards.
func (s *targetScheduler) noteChannelFailure(tier Tier, pool, proxyRaw, failureClass string, status int, retryAfter time.Duration, startedNanos int64) channelChange {
	now := time.Now()
	nowNanos := now.UnixNano()
	if startedNanos == 0 {
		startedNanos = nowNanos
	}
	identity := channelIdentity(tier, pool, proxyRaw)
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.channelState[identity]
	var previous int64
	if entry == nil {
		if len(s.channelState) >= maxChannelStates {
			s.pruneStaleChannelLocked(nowNanos)
			if len(s.channelState) >= maxChannelStates {
				s.evictOldestIdleChannelLocked(nowNanos)
			}
		}
		entry = &channelEntry{}
		if s.channelState == nil {
			s.channelState = make(map[string]*channelEntry)
		}
		s.channelState[identity] = entry
	} else {
		previous = entry.cooldownUntil
	}
	entry.failures++
	delay := s.backoffDelayLocked(entry.failures, "channel\x00"+identity, retryAfter)
	entry.cooldownUntil = cooldownDeadline(now.UnixNano(), delay)
	entry.lastFailureAt = nowNanos
	if startedNanos > entry.lastStartedNanos {
		entry.lastStartedNanos = startedNanos
	}
	entry.lastFailureClass = failureClass
	entry.lastStatus = status
	if retryAfter > 0 {
		entry.retryAfterUntil = cooldownDeadline(now.UnixNano(), retryAfter)
	}
	return channelChange{
		Changed: entry.cooldownUntil > previous, Failures: entry.failures,
		CooldownUntil: entry.cooldownUntil, PreviousUntil: previous,
		FailureClass: failureClass, Status: status,
	}
}

// noteChannelSuccess clears the tier-qualified channel state, but only when
// the successful send started at or after the latest recorded channel
// failure. A stale in-flight success must not clear a newer cooldown.
func (s *targetScheduler) noteChannelSuccess(tier Tier, pool, proxyRaw string, startedNanos int64) channelChange {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity := channelIdentity(tier, pool, proxyRaw)
	entry := s.channelState[identity]
	if entry == nil {
		return channelChange{}
	}
	if startedNanos == 0 {
		startedNanos = time.Now().UnixNano()
	}
	if startedNanos < entry.lastStartedNanos {
		return channelChange{}
	}
	failures := entry.failures
	delete(s.channelState, identity)
	return channelChange{Cleared: true, Changed: true, Failures: failures}
}

// pruneStaleChannelLocked deletes entries whose cooldown has expired and
// whose last failure is older than channelStaleRetention. Caller holds s.mu.
func (s *targetScheduler) pruneStaleChannelLocked(nowNanos int64) {
	for identity, entry := range s.channelState {
		if entry == nil {
			delete(s.channelState, identity)
			continue
		}
		if entry.cooldownUntil > nowNanos {
			continue
		}
		if entry.lastFailureAt == 0 || nowNanos-entry.lastFailureAt > int64(channelStaleRetention) {
			delete(s.channelState, identity)
		}
	}
}

// evictOldestIdleChannelLocked deterministically deletes the oldest entry
// that is not in active cooldown. Returns true when evicted. Caller holds s.mu.
func (s *targetScheduler) evictOldestIdleChannelLocked(nowNanos int64) bool {
	var victim string
	var victimAt int64
	found := false
	for identity, entry := range s.channelState {
		if entry == nil || entry.cooldownUntil > nowNanos {
			continue
		}
		at := entry.lastFailureAt
		if !found || at < victimAt || (at == victimAt && identity < victim) {
			victim, victimAt, found = identity, at, true
		}
	}
	if !found {
		return false
	}
	delete(s.channelState, victim)
	return true
}

func (s *targetScheduler) backoffDelayLocked(failures uint32, identity string, retryAfter time.Duration) time.Duration {
	// Caller holds s.mu; baseCooldown is immutable after construction.
	return backoffDelayForBase(s.failureBase(), failures, identity, retryAfter)
}

func (s *targetScheduler) rateLimitDelayLocked(failures uint32, identity string, retryAfter time.Duration) time.Duration {
	// Caller holds s.mu; rate-limit base/max are immutable after construction.
	return backoffDelayForBaseWithCap(s.rateLimitBase(), s.rateLimitMax(), failures, identity, retryAfter)
}

// hrwScore is a pure Rendezvous/HRW score over the full target identity plus
// the already-hashed session. Higher scores sort first; ties break on the
// identity string so ordering is fully deterministic.
func hrwScore(session, credID, pool, proxyRaw, model string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(session))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(credID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(pool))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(proxyRaw))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(model))
	return h.Sum64()
}

// proxyAffinityScore is the deterministic soft-affinity score for one
// (credential, pool, proxy) triple. Higher sorts first; no session, model,
// tier-beyond-credID, or pool-contents beyond the triple participates. Same
// key+pool prefers the same proxy across sessions/models; different pools
// choose independently. Rendezvous hashing preserves fair dispersion across
// credentials and minimal disruption when pool contents change. No persisted
// key->proxy map exists; the order derives from current pool contents.
func proxyAffinityScore(credID, pool, proxyRaw string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(credID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(pool))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(proxyRaw))
	return h.Sum64()
}

// credOrderScore orders credential groups for unbound establishment: session
// HRW over credential identity only (pool is constant within one tier build).
// Sessions disperse across credentials; proxies within each credential stay
// session-independent via proxyAffinityScore.
func credOrderScore(session, credID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(session))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(credID))
	return h.Sum64()
}

// preferredProxyRaw returns the affinity-preferred proxy raw identity for one
// credential+pool from the current pool contents. Empty when no proxies.
func preferredProxyRaw(credID, poolName string, proxies []*proxyTransport) string {
	best := ""
	var bestScore uint64
	found := false
	for _, proxy := range proxies {
		if proxy == nil {
			continue
		}
		score := proxyAffinityScore(credID, poolName, proxy.name)
		if !found || score > bestScore || (score == bestScore && proxy.name < best) {
			best, bestScore, found = proxy.name, score, true
		}
	}
	return best
}

// orderCandidates sorts a frozen candidate slice. Anonymous candidates keep
// session-stable HRW over the full target (proxy-affine). Authenticated
// candidates use credential-soft-affinity ordering: credential groups ordered
// by session HRW, proxies within each credential ordered by deterministic
// affinity (session/model independent). Empty session uses round-robin. No
// randomness is used.
func (s *targetScheduler) orderCandidates(cands []targetCandidate, session string) []targetCandidate {
	if len(cands) < 2 {
		return cands
	}
	isAnon := true
	for _, c := range cands {
		if c.CredID != anonymousSchedulerCredentialID {
			isAnon = false
			break
		}
	}
	if session == "" {
		start := int((s.roundRobin.Add(1) - 1) % uint64(len(cands)))
		if start == 0 {
			return cands
		}
		rotated := make([]targetCandidate, 0, len(cands))
		rotated = append(rotated, cands[start:]...)
		rotated = append(rotated, cands[:start]...)
		return rotated
	}
	if isAnon {
		ordered := append([]targetCandidate(nil), cands...)
		sort.SliceStable(ordered, func(i, j int) bool {
			si := hrwScore(session, ordered[i].CredID, ordered[i].PoolName, ordered[i].ProxyRaw, ordered[i].Model)
			sj := hrwScore(session, ordered[j].CredID, ordered[j].PoolName, ordered[j].ProxyRaw, ordered[j].Model)
			if si != sj {
				return si > sj
			}
			return ordered[i].Identity < ordered[j].Identity
		})
		return ordered
	}
	return s.orderAuthCandidates(cands, session)
}

// orderAuthCandidates implements credential-soft-affinity ordering for
// authenticated candidates: group by credential, order groups by session HRW,
// order proxies within each group by affinity. Deterministic, session
// disperses across credentials, proxy choice is session/model stable.
func (s *targetScheduler) orderAuthCandidates(cands []targetCandidate, session string) []targetCandidate {
	groups := make(map[string][]targetCandidate)
	credOrder := []string{}
	for _, c := range cands {
		if _, ok := groups[c.CredID]; !ok {
			credOrder = append(credOrder, c.CredID)
		}
		groups[c.CredID] = append(groups[c.CredID], c)
	}
	sort.SliceStable(credOrder, func(i, j int) bool {
		si := credOrderScore(session, credOrder[i])
		sj := credOrderScore(session, credOrder[j])
		if si != sj {
			return si > sj
		}
		return credOrder[i] < credOrder[j]
	})
	for credID := range groups {
		list := groups[credID]
		sort.SliceStable(list, func(i, j int) bool {
			si := proxyAffinityScore(list[i].CredID, list[i].PoolName, list[i].ProxyRaw)
			sj := proxyAffinityScore(list[j].CredID, list[j].PoolName, list[j].ProxyRaw)
			if si != sj {
				return si > sj
			}
			return list[i].Identity < list[j].Identity
		})
		groups[credID] = list
	}
	out := make([]targetCandidate, 0, len(cands))
	for _, credID := range credOrder {
		out = append(out, groups[credID]...)
	}
	return out
}

// affinityProxyOrder returns eligible proxies for one authenticated binding
// in try order: current/preferred first when eligible, then remaining healthy
// proxies in affinity order. Callers filter by health/cooldowns before or via
// the eligible set.
func affinityProxyOrder(pool *transportPool, credID, currentRaw string) []*proxyTransport {
	if pool == nil {
		return nil
	}
	others := make([]*proxyTransport, 0, len(pool.items))
	var current *proxyTransport
	for _, proxy := range pool.items {
		if proxy == nil {
			continue
		}
		if proxy.name == currentRaw {
			current = proxy
			continue
		}
		others = append(others, proxy)
	}
	sort.SliceStable(others, func(i, j int) bool {
		si := proxyAffinityScore(credID, pool.name, others[i].name)
		sj := proxyAffinityScore(credID, pool.name, others[j].name)
		if si != sj {
			return si > sj
		}
		return others[i].name < others[j].name
	})
	if current != nil {
		return append([]*proxyTransport{current}, others...)
	}
	return others
}

// buildAuthCandidates returns every (credential x healthy proxy) combination
// for one tier and model, filtered by credential 401/429, tier-qualified
// proxy429, tier-qualified channel-availability, and target cooldowns. A
// proxy under active tier-qualified 429 or channel cooldown is excluded for
// every model/credential/client using that tier+pool+proxy; the other tier
// stays available. The result is in config order (credential index, then
// proxy index); callers freeze it with orderCandidates (affinity ordering
// for auth).
func (s *targetScheduler) buildAuthCandidates(tier Tier, creds []credentialRef, pool *transportPool, model string, now int64) []targetCandidate {
	if len(creds) == 0 || pool == nil || len(pool.items) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cred := range creds {
		if _, ok := s.credDisplay[cred.id]; !ok {
			s.credDisplay[cred.id] = cred.display
		}
	}
	out := make([]targetCandidate, 0, len(creds)*len(pool.items))
	for _, cred := range creds {
		if entry := s.credState[cred.id]; entry != nil && entry.cooldownUntil > now {
			continue
		}
		if entry := s.cred429State[cred.id]; entry != nil && entry.cooldownUntil > now {
			continue
		}
		for _, proxy := range pool.items {
			if proxy == nil || !proxy.healthy.Load() {
				continue
			}
			if entry := s.proxy429State[proxy429Identity(tier, pool.name, proxy.name)]; entry != nil && entry.cooldownUntil > now {
				continue
			}
			if entry := s.channelState[channelIdentity(tier, pool.name, proxy.name)]; entry != nil && entry.cooldownUntil > now {
				continue
			}
			identity := targetIdentity(tier, cred.id, pool.name, proxy.name, model)
			if entry := s.targetState[identity]; entry != nil && entry.cooldownUntil > now {
				continue
			}
			out = append(out, targetCandidate{
				Tier: tier, CredKey: cred.key, CredID: cred.id, CredDisplay: cred.display,
				CredIndex: cred.index, PoolName: pool.name, Proxy: proxy,
				ProxyRaw: proxy.name, Model: model, Identity: identity,
			})
		}
	}
	return out
}

// buildAnonymousCandidates returns the fixed anonymous credential x every
// healthy, target-, proxy429-, and channel-available proxy in the assigned
// pool. The tier-qualified (Zen channel) proxy429 and channel filters match
// the authenticated Zen path: an actively cooling Zen proxy is excluded for
// every model and credential on Zen; Go stays isolated.
func (s *targetScheduler) buildAnonymousCandidates(pool *transportPool, model string, now int64) []targetCandidate {
	if pool == nil || len(pool.items) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.credDisplay[anonymousSchedulerCredentialID]; !ok {
		s.credDisplay[anonymousSchedulerCredentialID] = anonymousCredentialID
	}
	if entry := s.credState[anonymousSchedulerCredentialID]; entry != nil && entry.cooldownUntil > now {
		return nil
	}
	out := make([]targetCandidate, 0, len(pool.items))
	for _, proxy := range pool.items {
		if proxy == nil || !proxy.healthy.Load() {
			continue
		}
		if entry := s.proxy429State[proxy429Identity(TierZen, pool.name, proxy.name)]; entry != nil && entry.cooldownUntil > now {
			continue
		}
		if entry := s.channelState[channelIdentity(TierZen, pool.name, proxy.name)]; entry != nil && entry.cooldownUntil > now {
			continue
		}
		identity := targetIdentity(TierZen, anonymousSchedulerCredentialID, pool.name, proxy.name, model)
		if entry := s.targetState[identity]; entry != nil && entry.cooldownUntil > now {
			continue
		}
		out = append(out, targetCandidate{
			Tier: TierZen, CredKey: anonymousZenKey, CredID: anonymousSchedulerCredentialID,
			CredDisplay: anonymousCredentialID, CredIndex: -1, PoolName: pool.name,
			Proxy: proxy, ProxyRaw: proxy.name, Model: model, Identity: identity,
		})
	}
	return out
}

// credentialRef is a config-order credential handle. Full key material stays
// in the Gateway; only the display suffix ever leaves the process.
type credentialRef struct {
	tier    Tier
	key     string
	id      string
	display string
	index   int
}

func credentialsForKeys(tier Tier, keys []string) []credentialRef {
	refs := make([]credentialRef, 0, len(keys))
	for i, key := range keys {
		refs = append(refs, credentialRef{
			tier: tier, key: key, id: credentialIDForKey(tier, key),
			display: keyDisplayID(key), index: i,
		})
	}
	return refs
}

// TargetStatus is the bounded admin view of stateful targets. Only targets
// with recorded failures or an active cooldown are listed. It describes only
// 403/5xx target cooldowns; HTTP 429 lives in the dedicated proxy429 layer
// (ProxyRateLimitStatus) and never appears here.
type TargetStatus struct {
	Tier             string     `json:"tier"`
	Credential       string     `json:"credential"`
	ProxyPool        string     `json:"proxy_pool"`
	ProxyNode        string     `json:"proxy_node"`
	Model            string     `json:"model"`
	Failures         uint32     `json:"failures"`
	CooldownUntil    *time.Time `json:"cooldown_until,omitempty"`
	RemainingSeconds *int64     `json:"remaining_seconds,omitempty"`
	LastFailureClass string     `json:"last_failure_class,omitempty"`
	LastStatus       int        `json:"last_status,omitempty"`
}

// ProxyRateLimitStatus is the bounded additive admin view of the
// tier-qualified proxy429 layer: one row per (tier/channel, pool, proxy) with
// recorded 429 state. Tier names the channel (zen shared by anonymous and
// authenticated Zen; go isolated). Pool is the operator pool identity;
// ProxyNode is the redacted proxy node (never raw credentials). Active
// reports whether the cooldown is currently in the future; Failures counts
// recorded 429s (including retained backoff memory);
// RemainingSeconds/CooldownUntil/NextAvailableAt describe the active
// deadline; LastFailureClass/LastStatus describe the latest 429.
type ProxyRateLimitStatus struct {
	Tier             string     `json:"tier,omitempty"`
	ProxyPool        string     `json:"proxy_pool"`
	ProxyNode        string     `json:"proxy_node"`
	Active           bool       `json:"active"`
	Failures         uint32     `json:"failures"`
	CooldownUntil    *time.Time `json:"cooldown_until,omitempty"`
	RemainingSeconds *int64     `json:"remaining_seconds,omitempty"`
	NextAvailableAt  *time.Time `json:"next_available_at,omitempty"`
	LastFailureClass string     `json:"last_failure_class,omitempty"`
	LastStatus       int        `json:"last_status,omitempty"`
}

// snapshotTargets returns at most maxTargetSnapshotEntries stateful targets
// plus the total count so callers can report truncation. Expired-stale entries
// (out of cooldown and past targetStaleRetention) are pruned under the same
// lock before the snapshot; the per-candidate hot loop never scans the map.
func (s *targetScheduler) snapshotTargets() (entries []TargetStatus, total int) {
	now := time.Now()
	nowNanos := now.UnixNano()
	type parsed struct {
		status   TargetStatus
		identity string
	}
	s.mu.Lock()
	s.pruneStaleTargetsLocked(nowNanos)
	all := make([]parsed, 0, len(s.targetState))
	for identity, entry := range s.targetState {
		if entry == nil || (entry.failures == 0 && entry.cooldownUntil <= nowNanos) {
			continue
		}
		tier, credID, pool, proxyRaw, model := parseTargetIdentity(identity)
		credDisplay := s.credDisplay[credID]
		if credDisplay == "" {
			credDisplay = credentialDisplayForID(credID)
		}
		st := TargetStatus{
			Tier: tier, Credential: credDisplay, ProxyPool: pool,
			ProxyNode: redactURL(proxyRaw), Model: model, Failures: entry.failures,
			LastFailureClass: entry.lastFailureClass, LastStatus: entry.lastStatus,
		}
		if entry.cooldownUntil > now.UnixNano() {
			value := time.Unix(0, entry.cooldownUntil).UTC()
			st.CooldownUntil = &value
			st.RemainingSeconds = cooldownRemainingSeconds(entry.cooldownUntil, now)
		}
		all = append(all, parsed{status: st, identity: identity})
	}
	s.mu.Unlock()
	sort.Slice(all, func(i, j int) bool { return all[i].identity < all[j].identity })
	total = len(all)
	if len(all) > maxTargetSnapshotEntries {
		all = all[:maxTargetSnapshotEntries]
	}
	entries = make([]TargetStatus, 0, len(all))
	for _, p := range all {
		entries = append(entries, p.status)
	}
	return entries, total
}

// snapshotProxy429 returns at most maxProxy429SnapshotEntries stateful
// tier-qualified proxies plus the total count so callers can report
// truncation. Expired-stale entries are pruned under the same lock before
// the snapshot; the per-candidate hot loop never scans the map. The view is
// additive and redacted: pool names are operator identities, proxy nodes are
// redacted, and raw credentials never appear.
func (s *targetScheduler) snapshotProxy429() (entries []ProxyRateLimitStatus, total int) {
	now := time.Now()
	nowNanos := now.UnixNano()
	type parsed struct {
		status   ProxyRateLimitStatus
		identity string
	}
	s.mu.Lock()
	s.pruneStaleProxy429Locked(nowNanos)
	all := make([]parsed, 0, len(s.proxy429State))
	for identity, entry := range s.proxy429State {
		if entry == nil || (entry.failures == 0 && entry.cooldownUntil <= nowNanos) {
			continue
		}
		tier, pool, proxyRaw := parseProxy429Identity(identity)
		st := ProxyRateLimitStatus{
			Tier: string(tier), ProxyPool: pool, ProxyNode: redactURL(proxyRaw), Failures: entry.failures,
			LastFailureClass: entry.lastFailureClass, LastStatus: entry.lastStatus,
		}
		if entry.cooldownUntil > nowNanos {
			value := time.Unix(0, entry.cooldownUntil).UTC()
			st.Active = true
			st.CooldownUntil = &value
			st.NextAvailableAt = &value
			st.RemainingSeconds = cooldownRemainingSeconds(entry.cooldownUntil, now)
		}
		all = append(all, parsed{status: st, identity: identity})
	}
	s.mu.Unlock()
	sort.Slice(all, func(i, j int) bool { return all[i].identity < all[j].identity })
	total = len(all)
	if len(all) > maxProxy429SnapshotEntries {
		all = all[:maxProxy429SnapshotEntries]
	}
	entries = make([]ProxyRateLimitStatus, 0, len(all))
	for _, p := range all {
		entries = append(entries, p.status)
	}
	return entries, total
}

// ChannelAvailabilityStatus is the bounded additive admin view of the
// tier-qualified channel-availability layer: one row per (tier/channel,
// pool, proxy) with recorded 403/5xx channel state. Tier names the channel
// (zen shared by anonymous and authenticated Zen; go isolated). Pool is the
// operator pool identity; ProxyNode is the redacted proxy node (never raw
// credentials). Active reports whether the cooldown is currently in the
// future; Failures counts recorded channel failures (including retained
// backoff memory); RemainingSeconds/CooldownUntil/NextAvailableAt describe
// the active deadline; LastFailureClass/LastStatus describe the latest
// channel failure.
type ChannelAvailabilityStatus struct {
	Tier             string     `json:"tier,omitempty"`
	ProxyPool        string     `json:"proxy_pool"`
	ProxyNode        string     `json:"proxy_node"`
	Active           bool       `json:"active"`
	Failures         uint32     `json:"failures"`
	CooldownUntil    *time.Time `json:"cooldown_until,omitempty"`
	RemainingSeconds *int64     `json:"remaining_seconds,omitempty"`
	NextAvailableAt  *time.Time `json:"next_available_at,omitempty"`
	LastFailureClass string     `json:"last_failure_class,omitempty"`
	LastStatus       int        `json:"last_status,omitempty"`
}

// snapshotChannel returns at most maxChannelSnapshotEntries stateful
// tier-qualified proxies plus the total count so callers can report
// truncation. Expired-stale entries are pruned under the same lock before
// the snapshot; the per-candidate hot loop never scans the map.
func (s *targetScheduler) snapshotChannel() (entries []ChannelAvailabilityStatus, total int) {
	now := time.Now()
	nowNanos := now.UnixNano()
	type parsed struct {
		status   ChannelAvailabilityStatus
		identity string
	}
	s.mu.Lock()
	s.pruneStaleChannelLocked(nowNanos)
	all := make([]parsed, 0, len(s.channelState))
	for identity, entry := range s.channelState {
		if entry == nil || (entry.failures == 0 && entry.cooldownUntil <= nowNanos) {
			continue
		}
		tier, pool, proxyRaw := parseChannelIdentity(identity)
		st := ChannelAvailabilityStatus{
			Tier: string(tier), ProxyPool: pool, ProxyNode: redactURL(proxyRaw), Failures: entry.failures,
			LastFailureClass: entry.lastFailureClass, LastStatus: entry.lastStatus,
		}
		if entry.cooldownUntil > nowNanos {
			value := time.Unix(0, entry.cooldownUntil).UTC()
			st.Active = true
			st.CooldownUntil = &value
			st.NextAvailableAt = &value
			st.RemainingSeconds = cooldownRemainingSeconds(entry.cooldownUntil, now)
		}
		all = append(all, parsed{status: st, identity: identity})
	}
	s.mu.Unlock()
	sort.Slice(all, func(i, j int) bool { return all[i].identity < all[j].identity })
	total = len(all)
	if len(all) > maxChannelSnapshotEntries {
		all = all[:maxChannelSnapshotEntries]
	}
	entries = make([]ChannelAvailabilityStatus, 0, len(all))
	for _, p := range all {
		entries = append(entries, p.status)
	}
	return entries, total
}

func parseTargetIdentity(identity string) (tier, credID, pool, proxyRaw, model string) {
	parts := splitNul5(identity)
	if len(parts) != 5 {
		return "", "", "", "", ""
	}
	return parts[0], parts[1], parts[2], parts[3], parts[4]
}

func splitNul5(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}

// migrateFrom copies still-future credential, credential429, target,
// proxy429, and channel cooldowns from the old scheduler. Non-429 remaining
// (credential 401, target, channel) is capped at the generic 5 minutes;
// proxy429 and credential429 remaining is capped at the NEW configured 429
// max. Expired cooldowns never migrate, including expired failure memory
// (failures>0 out of cooldown): the new instance restarts backoff from
// zero. New resources start at zero state and removed identities are
// dropped. Proxy429 and channel migrate by (tier, pool, proxy) identity
// only and preserve backoff/Retry-After remaining within the cap; old
// pool-only identities (no tier separator count) are dropped.
// Credential429 migrates by tier+key (internal credential ID). Migrated
// targets respect maxTargetStates and migrated proxy429/channel entries
// respect maxProxy429States/maxChannelStates with the same deterministic
// policy as creation: prune expired-stale first, evict oldest idle next,
// allow temporary overflow only when every entry is in active cooldown.
// migrationSummary counts migrated still-future state for the Apply log
// (aggregate counts only). No per-identity detail is included.
type migrationSummary struct {
	Credentials   int
	Credential429 int
	Targets       int
	Proxy429      int
	Channel       int
}

func (s *targetScheduler) migrateFrom(old *targetScheduler) migrationSummary {
	var summary migrationSummary
	if old == nil || old == s {
		return summary
	}
	now := time.Now().UnixNano()
	old.mu.Lock()
	creds := make(map[string]credentialEntry, len(old.credState))
	for id, entry := range old.credState {
		if entry == nil {
			continue
		}
		creds[id] = *entry
	}
	targets := make(map[string]targetEntry, len(old.targetState))
	for id, entry := range old.targetState {
		if entry == nil {
			continue
		}
		targets[id] = *entry
	}
	proxy429 := make(map[string]proxy429Entry, len(old.proxy429State))
	for id, entry := range old.proxy429State {
		if entry == nil {
			continue
		}
		proxy429[id] = *entry
	}
	cred429 := make(map[string]credential429Entry, len(old.cred429State))
	for id, entry := range old.cred429State {
		if entry == nil {
			continue
		}
		cred429[id] = *entry
	}
	channel := make(map[string]channelEntry, len(old.channelState))
	for id, entry := range old.channelState {
		if entry == nil {
			continue
		}
		channel[id] = *entry
	}
	displays := make(map[string]string, len(old.credDisplay))
	for id, display := range old.credDisplay {
		displays[id] = display
	}
	old.mu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	for id, entry := range creds {
		if entry.cooldownUntil <= now {
			continue
		}
		if remaining := time.Duration(entry.cooldownUntil - now); remaining > targetBackoffCap {
			entry.cooldownUntil = cooldownDeadline(now, targetBackoffCap)
		}
		fresh := entry
		s.credState[id] = &fresh
		summary.Credentials++
	}
	for id, display := range displays {
		if _, ok := s.credDisplay[id]; !ok {
			s.credDisplay[id] = display
		}
	}
	for id, entry := range targets {
		// Only still-future target cooldowns migrate; expired failure
		// memory does not cross Apply.
		if entry.cooldownUntil <= now {
			continue
		}
		if remaining := time.Duration(entry.cooldownUntil - now); remaining > targetBackoffCap {
			entry.cooldownUntil = cooldownDeadline(now, targetBackoffCap)
		}
		if len(s.targetState) >= maxTargetStates {
			s.pruneStaleTargetsLocked(now)
			if len(s.targetState) >= maxTargetStates {
				s.evictOldestIdleTargetLocked(now)
			}
		}
		fresh := entry
		s.targetState[id] = &fresh
		summary.Targets++
	}
	rateCap := s.rateLimitMax()
	for id, entry := range cred429 {
		if entry.cooldownUntil <= now {
			continue
		}
		if remaining := time.Duration(entry.cooldownUntil - now); remaining > rateCap {
			entry.cooldownUntil = cooldownDeadline(now, rateCap)
		}
		if s.cred429State == nil {
			s.cred429State = make(map[string]*credential429Entry)
		}
		fresh := entry
		s.cred429State[id] = &fresh
		summary.Credential429++
	}
	for id, entry := range proxy429 {
		// Only still-future tier-qualified proxy429 cooldowns migrate;
		// legacy pool-only identities (no tier separator count) are dropped.
		// Expired memory does not cross Apply. Backoff and Retry-After
		// remaining are preserved within the cap.
		if len(splitNul3(id)) != 3 {
			continue
		}
		if entry.cooldownUntil <= now {
			continue
		}
		if remaining := time.Duration(entry.cooldownUntil - now); remaining > rateCap {
			entry.cooldownUntil = cooldownDeadline(now, rateCap)
		}
		if s.proxy429State == nil {
			s.proxy429State = make(map[string]*proxy429Entry)
		}
		if len(s.proxy429State) >= maxProxy429States {
			s.pruneStaleProxy429Locked(now)
			if len(s.proxy429State) >= maxProxy429States {
				s.evictOldestIdleProxy429Locked(now)
			}
		}
		fresh := entry
		s.proxy429State[id] = &fresh
		summary.Proxy429++
	}
	for id, entry := range channel {
		if len(splitNul3(id)) != 3 {
			continue
		}
		if entry.cooldownUntil <= now {
			continue
		}
		if remaining := time.Duration(entry.cooldownUntil - now); remaining > targetBackoffCap {
			entry.cooldownUntil = cooldownDeadline(now, targetBackoffCap)
		}
		if s.channelState == nil {
			s.channelState = make(map[string]*channelEntry)
		}
		if len(s.channelState) >= maxChannelStates {
			s.pruneStaleChannelLocked(now)
			if len(s.channelState) >= maxChannelStates {
				s.evictOldestIdleChannelLocked(now)
			}
		}
		fresh := entry
		s.channelState[id] = &fresh
		summary.Channel++
	}
	return summary
}

// retainOnly drops credential, credential429, target, proxy429, and channel
// state that no longer matches live resources: unknown credential IDs,
// unknown (tier, pool, proxy) triples, and their display entries. Tier
// qualification is enforced: a proxy429/channel entry whose tier channel no
// longer routes that pool is dropped even if the pool+URL still exists
// elsewhere. New resources start at zero state.
func (s *targetScheduler) retainOnly(validCreds map[string]bool, validPoolProxy map[string]map[string]bool, validTierPool map[string]map[string]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.credState {
		if !validCreds[id] {
			delete(s.credState, id)
		}
	}
	for id := range s.cred429State {
		if !validCreds[id] {
			delete(s.cred429State, id)
		}
	}
	for id := range s.credDisplay {
		if !validCreds[id] {
			delete(s.credDisplay, id)
		}
	}
	for identity, entry := range s.proxy429State {
		tier, pool, proxyRaw := parseProxy429Identity(identity)
		if entry == nil {
			delete(s.proxy429State, identity)
			continue
		}
		proxies, ok := validPoolProxy[pool]
		if !ok || !proxies[proxyRaw] {
			delete(s.proxy429State, identity)
			continue
		}
		if pools, ok := validTierPool[string(tier)]; !ok || !pools[pool] {
			delete(s.proxy429State, identity)
		}
	}
	for identity := range s.targetState {
		_, credID, pool, proxyRaw, _ := parseTargetIdentity(identity)
		if !validCreds[credID] {
			delete(s.targetState, identity)
			continue
		}
		proxies, ok := validPoolProxy[pool]
		if !ok || !proxies[proxyRaw] {
			delete(s.targetState, identity)
		}
	}
	for identity, entry := range s.channelState {
		tier, pool, proxyRaw := parseChannelIdentity(identity)
		if entry == nil {
			delete(s.channelState, identity)
			continue
		}
		proxies, ok := validPoolProxy[pool]
		if !ok || !proxies[proxyRaw] {
			delete(s.channelState, identity)
			continue
		}
		if pools, ok := validTierPool[string(tier)]; !ok || !pools[pool] {
			delete(s.channelState, identity)
		}
	}
}

// credentialSnapshot returns failures/cooldown for one credential identity.
func (s *targetScheduler) credentialSnapshot(credID string) (failures uint32, cooldownUntil int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry := s.credState[credID]; entry != nil {
		return entry.failures, entry.cooldownUntil
	}
	return 0, 0
}

// proxy429EntrySnapshot returns failures/cooldown for one tier-qualified proxy.
func (s *targetScheduler) proxy429EntrySnapshot(tier Tier, pool, proxyRaw string) (failures uint32, cooldownUntil int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry := s.proxy429State[proxy429Identity(tier, pool, proxyRaw)]; entry != nil {
		return entry.failures, entry.cooldownUntil
	}
	return 0, 0
}

// channelEntrySnapshot returns failures/cooldown for one tier-qualified proxy.
func (s *targetScheduler) channelEntrySnapshot(tier Tier, pool, proxyRaw string) (failures uint32, cooldownUntil int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry := s.channelState[channelIdentity(tier, pool, proxyRaw)]; entry != nil {
		return entry.failures, entry.cooldownUntil
	}
	return 0, 0
}

// proxyTargetSummary aggregates per-proxy 403/5xx target state across models
// for one (pool, raw proxy URL), restricted to the anonymous credential
// only. Auth credentials sharing the same pool/proxy never pollute the
// anonymous view: callers pass pool+proxyRaw from the anonymous assigned
// pool and only identities with the anonymous credential ID are counted.
// HTTP 429 never appears here; it lives in the global proxy429 layer.
func (s *targetScheduler) proxyTargetSummary(pool, proxyRaw string) (active int, nextAvailable *time.Time, lastClass string, failures uint32) {
	now := time.Now()
	nowNanos := now.UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	var latest int64
	var lastNanos int64
	for identity, entry := range s.targetState {
		tier, credID, poolName, raw, _ := parseTargetIdentity(identity)
		_ = tier
		if poolName != pool || raw != proxyRaw || entry == nil {
			continue
		}
		if credID != anonymousSchedulerCredentialID {
			continue
		}
		failures += entry.failures
		if entry.cooldownUntil > nowNanos {
			active++
			if entry.cooldownUntil > latest {
				latest = entry.cooldownUntil
			}
		}
		if entry.lastFailureClass != "" {
			// Track the most recently written class by cooldown deadline as
			// a stable proxy for recency without extra clocks.
			if entry.cooldownUntil >= lastNanos {
				lastNanos = entry.cooldownUntil
				lastClass = entry.lastFailureClass
			}
		}
	}
	if latest > 0 {
		value := time.Unix(0, latest).UTC()
		nextAvailable = &value
	}
	return active, nextAvailable, lastClass, failures
}

// credentialDisplayForID maps an internal credential ID to its safe display:
// the anonymous literal, or the key suffix when the ID embeds no display.
// The full fingerprint never leaves the scheduler; for auth credentials the
// stored target identity alone cannot recover the suffix, so callers that
// need exact suffixes should track displays separately. This fallback keeps
// the admin view redacted rather than leaking identity material.
func credentialDisplayForID(credID string) string {
	if credID == anonymousSchedulerCredentialID {
		return anonymousCredentialID
	}
	return "•••••"
}

// Session-affinity pin layer: derived client session + model ID binds to one
// target. Anonymous bindings are full-target (proxy is identity, no moves).
// Authenticated bindings are proxy-independent: the durable identity fixes
// tier, credential, pool, model, protocol, and authority but not the proxy
// node; ProxyRaw is the mutable current/preferred proxy with generation
// fencing. In-memory Gateway authority only: no persistence, no
// log/admin/history projection. Strict process-lifetime semantics: once
// pinned, a binding never expires and is never evicted during the process
// lifetime. Memory stays bounded by sessionPinStoreCap entries; at the cap a
// new unpinned session+model fails closed locally before any upstream send
// and existing pins keep serving. Restart clears pins (map lives in the
// Gateway); no disk persistence.

const sessionPinStoreCap = 4096

// sessionPin is the bound target for one derived session + model. It carries
// the binding identity plus the mutable current proxy and generation. For
// anonymous bindings ProxyRaw is identity; for authenticated bindings it is
// the current/preferred selection fenced by Generation. Raw client signals,
// key material, and pin state never leave this struct.
type sessionPin struct {
	Tier      Tier
	CredID    string
	Pool      string
	ProxyRaw  string
	Model     string
	Protocol  Protocol
	Authority string
	// Generation fences concurrent proxy moves for authenticated bindings.
	// Anonymous bindings never move; generation stays zero.
	Generation uint64
}

// isAnonymousPin reports whether the pin uses the shared public credential.
func (p sessionPin) isAnonymousPin() bool { return p.CredID == anonymousSchedulerCredentialID }

// bindingEqual reports durable identity equality: full-target for anonymous
// (including proxy), proxy-independent for authenticated (excluding proxy
// and generation).
func (p sessionPin) bindingEqual(other sessionPin) bool {
	if p.Tier != other.Tier || p.CredID != other.CredID || p.Pool != other.Pool ||
		p.Model != other.Model || p.Protocol != other.Protocol || p.Authority != other.Authority {
		return false
	}
	if p.isAnonymousPin() {
		return p.ProxyRaw == other.ProxyRaw
	}
	return true
}

func sessionPinKey(session, model string) string {
	return session + "\x00" + model
}

type sessionPinStore struct {
	mu      sync.Mutex
	entries map[string]*sessionPin
	// claimMu guards the bounded in-flight establishment map and the reserved
	// capacity count. It is never held over network I/O, so pin lookup/bind,
	// migration, and Apply cannot deadlock against establishment waits.
	// Lock order when both mutexes are needed is always claimMu -> mu; no
	// path takes them in the opposite order.
	claimMu  sync.Mutex
	inflight map[string]*pinEstablishmentClaim
	reserved int
}

// pinEstablishmentClaim is the single-owner gate for one session+model key.
// done is closed exactly once when the owner releases; followers wait on it
// without retaining any map state after release. reserved reports whether
// this owner holds one capacity reservation against sessionPinStoreCap.
type pinEstablishmentClaim struct {
	done     chan struct{}
	reserved bool
}

func newSessionPinStore() *sessionPinStore {
	return &sessionPinStore{entries: make(map[string]*sessionPin)}
}

// pinGet returns a copy of the pinned target for session+model. Pins never
// expire: lookup is read-only and never mutates or prunes state.
func (s *targetScheduler) pinGet(session, model string) (sessionPin, bool) {
	if s == nil || s.pins == nil || session == "" || model == "" {
		return sessionPin{}, false
	}
	return s.pins.get(session, model)
}

func (st *sessionPinStore) get(session, model string) (sessionPin, bool) {
	if st == nil {
		return sessionPin{}, false
	}
	key := sessionPinKey(session, model)
	st.mu.Lock()
	defer st.mu.Unlock()
	entry, ok := st.entries[key]
	if !ok || entry == nil {
		return sessionPin{}, false
	}
	return *entry, true
}

// pinBind binds session+model to the successful target on first success.
// The first pin wins and is never overwritten, never expires, and is never
// evicted. A new key at the cap is dropped without evicting an existing pin.
// Callers establishing via pinClaim already hold a capacity reservation, so
// their bind always has space; the drop path only triggers for unreserved
// inserts at a full store.
func (s *targetScheduler) pinBind(session, model string, pin sessionPin) {
	if s == nil || s.pins == nil || session == "" || model == "" {
		return
	}
	pin.Model = model
	s.pins.bind(session, model, pin)
}

func (st *sessionPinStore) bind(session, model string, pin sessionPin) {
	if st == nil {
		return
	}
	key := sessionPinKey(session, model)
	if pin.Model == "" {
		pin.Model = model
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if existing, ok := st.entries[key]; ok && existing != nil {
		return
	}
	if len(st.entries) >= sessionPinStoreCap {
		return
	}
	if st.entries == nil {
		st.entries = make(map[string]*sessionPin)
	}
	fresh := pin
	st.entries[key] = &fresh
}

func (st *sessionPinStore) count() int {
	if st == nil {
		return 0
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.entries)
}

// pinClaim attempts to become the establishment owner for session+model.
// It returns the live claim and owned=true for the single owner, or the
// owner's claim and owned=false for a follower. The third return reports
// capacity: false means the store plus reservations are at the cap and a new
// unpinned session+model must fail closed before any send. Different keys
// never block each other except through the shared cap. Empty session/model
// never coordinates: callers treat a nil claim with owned=true and ok=true
// as an immediate unguarded owner. A nil claim with owned=false and ok=true
// means the pin appeared concurrently: the caller must re-check pinGet.
func (s *targetScheduler) pinClaim(session, model string) (*pinEstablishmentClaim, bool, bool) {
	if s == nil || s.pins == nil || session == "" || model == "" {
		return nil, true, true
	}
	return s.pins.claim(session, model)
}

func (st *sessionPinStore) claim(session, model string) (*pinEstablishmentClaim, bool, bool) {
	if st == nil {
		return nil, true, true
	}
	key := sessionPinKey(session, model)
	st.claimMu.Lock()
	defer st.claimMu.Unlock()
	if st.inflight == nil {
		st.inflight = make(map[string]*pinEstablishmentClaim)
	}
	if existing, ok := st.inflight[key]; ok && existing != nil {
		return existing, false, true
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if _, ok := st.entries[key]; ok {
		return nil, false, true
	}
	if len(st.entries)+st.reserved >= sessionPinStoreCap {
		return nil, false, false
	}
	claim := &pinEstablishmentClaim{done: make(chan struct{}), reserved: true}
	st.inflight[key] = claim
	st.reserved++
	return claim, true, true
}

// pinRelease removes the claim and notifies followers. It runs on every
// owner return path via defer, so it is panic-safe and never retains waiter
// state: the map entry is deleted and only the closed channel remains
// briefly in follower locals. A reserved owner releases its capacity slot:
// on success the slot is consumed by the new pin entry (entries grew by one
// while reserved shrinks by one, net unchanged); on failure without a pin
// the slot is freed so a later request can retry establishment.
func (s *targetScheduler) pinRelease(session, model string, claim *pinEstablishmentClaim) {
	if s == nil || s.pins == nil || claim == nil || session == "" || model == "" {
		return
	}
	s.pins.release(session, model, claim)
}

func (st *sessionPinStore) release(session, model string, claim *pinEstablishmentClaim) {
	if st == nil || claim == nil {
		return
	}
	key := sessionPinKey(session, model)
	st.claimMu.Lock()
	defer st.claimMu.Unlock()
	if stored, ok := st.inflight[key]; ok && stored == claim {
		delete(st.inflight, key)
		if stored.reserved {
			st.reserved--
			if st.reserved < 0 {
				st.reserved = 0
			}
		}
		close(stored.done)
	}
}

func (st *sessionPinStore) inflightCount() int {
	if st == nil {
		return 0
	}
	st.claimMu.Lock()
	defer st.claimMu.Unlock()
	return len(st.inflight)
}

func (st *sessionPinStore) reservedCount() int {
	if st == nil {
		return 0
	}
	st.claimMu.Lock()
	defer st.claimMu.Unlock()
	return st.reserved
}

// pinMoveCurrent performs generation-fenced current-proxy update for an
// established authenticated binding. It succeeds only when the stored binding
// identity still matches (proxy-independent) and the generation equals the
// observed generation; stale in-flight outcomes cannot move the pin back and
// competing moves cannot overwrite a newer selection or split one session.
// Anonymous pins never move. Returns the new generation on success.
func (s *targetScheduler) pinMoveCurrent(session, model string, expectedGen uint64, newProxyRaw string) (uint64, bool) {
	if s == nil || s.pins == nil || session == "" || model == "" || newProxyRaw == "" {
		return 0, false
	}
	return s.pins.moveCurrent(session, model, expectedGen, newProxyRaw)
}

func (st *sessionPinStore) moveCurrent(session, model string, expectedGen uint64, newProxyRaw string) (uint64, bool) {
	if st == nil {
		return 0, false
	}
	key := sessionPinKey(session, model)
	st.mu.Lock()
	defer st.mu.Unlock()
	entry, ok := st.entries[key]
	if !ok || entry == nil || entry.isAnonymousPin() {
		return 0, false
	}
	if entry.Generation != expectedGen {
		return entry.Generation, false
	}
	if entry.ProxyRaw == newProxyRaw {
		return entry.Generation, true
	}
	entry.ProxyRaw = newProxyRaw
	entry.Generation++
	return entry.Generation, true
}

// migratePinsFrom carries all existing pin identities up to the cap without
// validity filtering. Removed or changed targets migrate as unresolved
// tombstone-like bindings: the pinned resolver still matches them and fails
// locally with 502 rather than re-establishing or falling back. Authenticated
// pins migrate with their current proxy and generation intact; validity is
// proxy-independent (binding without proxy) while anonymous remains full
// target. Insertion is deterministic key order and stops at the cap without
// evicting. Restart remains the only clearing boundary (fresh store starts
// empty).
func (st *sessionPinStore) migratePinsFrom(old *sessionPinStore) int {
	if st == nil || old == nil || st == old {
		return 0
	}
	old.mu.Lock()
	type copied struct {
		key   string
		entry sessionPin
	}
	staged := make([]copied, 0, len(old.entries))
	for key, entry := range old.entries {
		if entry == nil {
			continue
		}
		staged = append(staged, copied{key: key, entry: *entry})
	}
	old.mu.Unlock()
	sort.Slice(staged, func(i, j int) bool { return staged[i].key < staged[j].key })
	migrated := 0
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, item := range staged {
		if _, ok := st.entries[item.key]; ok {
			continue
		}
		if len(st.entries) >= sessionPinStoreCap {
			break
		}
		fresh := item.entry
		st.entries[item.key] = &fresh
		migrated++
	}
	return migrated
}
