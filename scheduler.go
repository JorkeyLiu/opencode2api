package main

import (
	"crypto/sha256"
	"encoding/hex"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Unified credential x proxy target scheduler.
//
// Three state layers with separate ownership:
//   - proxyTransport.healthy: transport connectivity only. Only isProxyFailure
//     (timeout/deadline/refused) may set unhealthy; HTTP statuses never do.
//   - credentialState: global per-credential cooldown, 401 only.
//   - targetState: per (tier, credential, pool, proxy, model) cooldown for
//     403/429/5xx only. Transport errors and ordinary 4xx (including 400)
//     are neutral no-ops and 2xx clears only the single target.
//
// Target identity uses the raw configured proxy URL string qualified by pool
// name for internal matching; external output always uses redactURL.

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

	// targetBackoffCap caps every computed cooldown (credential and target).
	targetBackoffCap = 5 * time.Minute
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

type targetScheduler struct {
	mu            sync.Mutex
	baseCooldown  time.Duration
	credState     map[string]*credentialEntry
	targetState   map[string]*targetEntry
	credDisplay   map[string]string
	roundRobin    atomic.Uint64
	routeSessions *routeSessionStore
	pins          *sessionPinStore
}

func newTargetScheduler(baseCooldown time.Duration) *targetScheduler {
	if baseCooldown <= 0 {
		baseCooldown = 15 * time.Second
	}
	return &targetScheduler{
		baseCooldown:  baseCooldown,
		credState:     make(map[string]*credentialEntry),
		targetState:   make(map[string]*targetEntry),
		credDisplay:   make(map[string]string),
		routeSessions: newRouteSessionStore(),
		pins:          newSessionPinStore(),
	}
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
	return routeSessionScope{
		Authority: normalizeRouteAuthority(authority),
		Tier:      cand.Tier,
		CredID:    cand.CredID,
		Pool:      cand.PoolName,
		ProxyRaw:  cand.ProxyRaw,
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

// deterministicJitter returns delay scaled by +/-20%, derived from
// FNV-1a(identity + failure count) so tests are stable and concurrent
// targets do not expire simultaneously. No global rand is used.
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
	return time.Duration(float64(delay) * factor)
}

// backoffDelay computes base*2^min(failures-1,3) with deterministic jitter,
// taking the larger of the jittered backoff and retryAfter, capped at 5min.
func (s *targetScheduler) backoffDelay(failures uint32, identity string, retryAfter time.Duration) time.Duration {
	if failures < 1 {
		failures = 1
	}
	shift := failures - 1
	if shift > 3 {
		shift = 3
	}
	delay := s.baseCooldown * time.Duration(1<<shift)
	if delay > targetBackoffCap {
		delay = targetBackoffCap
	}
	delay = deterministicJitter(delay, identity, failures)
	if retryAfter > delay {
		delay = retryAfter
	}
	if delay > targetBackoffCap {
		delay = targetBackoffCap
	}
	if delay < 0 {
		delay = 0
	}
	return delay
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
	entry.cooldownUntil = now.Add(delay).UnixNano()
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

// noteTargetFailure cools one target identity for 403/429/5xx only.
// Transport errors never reach this helper: they stay neutral and only
// trigger the async proxy health verification. retryAfter (from a
// Retry-After header) takes effect only when larger, and the total is capped
// at 5 minutes. The failure count is retained after cooldown expiry for
// targetStaleRetention so the next failure escalates; success deletes the
// entry.
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
	entry.cooldownUntil = now.Add(delay).UnixNano()
	entry.lastFailureAt = nowNanos
	entry.lastFailureClass = failureClass
	entry.lastStatus = status
	if retryAfter > 0 {
		entry.retryAfterUntil = now.Add(retryAfter).UnixNano()
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

func (s *targetScheduler) backoffDelayLocked(failures uint32, identity string, retryAfter time.Duration) time.Duration {
	// Caller holds s.mu; baseCooldown is immutable after construction.
	base := s.baseCooldown
	if base <= 0 {
		base = 15 * time.Second
	}
	if failures < 1 {
		failures = 1
	}
	shift := failures - 1
	if shift > 3 {
		shift = 3
	}
	delay := base * time.Duration(1<<shift)
	if delay > targetBackoffCap {
		delay = targetBackoffCap
	}
	delay = deterministicJitter(delay, identity, failures)
	if retryAfter > delay {
		delay = retryAfter
	}
	if delay > targetBackoffCap {
		delay = targetBackoffCap
	}
	if delay < 0 {
		delay = 0
	}
	return delay
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

// orderCandidates sorts a frozen candidate slice: session-stable HRW
// descending when a session is present, otherwise an atomic round-robin
// start offset over config order. No randomness is used.
func (s *targetScheduler) orderCandidates(cands []targetCandidate, session string) []targetCandidate {
	if len(cands) < 2 {
		return cands
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

// buildAuthCandidates returns every (credential x healthy proxy) combination
// for one tier and model, filtered by credential and target cooldowns.
// The result is in config order (credential index, then proxy index);
// callers freeze it with orderCandidates.
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
		for _, proxy := range pool.items {
			if proxy == nil || !proxy.healthy.Load() {
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
// healthy, target-available proxy in the assigned pool.
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
// with recorded failures or an active cooldown are listed.
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

// migrateFrom copies still-future credential and target cooldowns from the
// old scheduler. Remaining time is capped at 5 minutes. Expired cooldowns
// never migrate, including expired failure memory (failures>0 out of
// cooldown): the new instance restarts backoff from zero. New resources start
// at zero state and removed identities are dropped. Migrated targets respect
// maxTargetStates with the same deterministic policy as creation: prune
// expired-stale first, evict oldest idle next, allow temporary overflow only
// when every entry is in active cooldown.
// migrationSummary counts migrated still-future state for the Apply log.
// No per-identity detail is included.
type migrationSummary struct {
	Credentials int
	Targets     int
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
			entry.cooldownUntil = now + int64(targetBackoffCap)
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
			entry.cooldownUntil = now + int64(targetBackoffCap)
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
	return summary
}

// retainOnly drops credential and target state that no longer matches live
// resources: unknown credential IDs, unknown (pool, proxy) pairs, and their
// display entries. New resources start at zero state.
func (s *targetScheduler) retainOnly(validCreds map[string]bool, validPoolProxy map[string]map[string]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.credState {
		if !validCreds[id] {
			delete(s.credState, id)
		}
	}
	for id := range s.credDisplay {
		if !validCreds[id] {
			delete(s.credDisplay, id)
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

// proxyTargetSummary aggregates per-proxy target state across models for one
// (pool, raw proxy URL), restricted to the anonymous credential only. Auth
// credentials sharing the same pool/proxy never pollute the anonymous view:
// callers pass pool+proxyRaw from the anonymous assigned pool and only
// identities with the anonymous credential ID are counted.
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
// full target identity. In-memory Gateway authority only: no persistence,
// no log/admin/history projection. Strict process-lifetime semantics: once
// pinned, a binding never expires and is never evicted during the process
// lifetime. Memory stays bounded by sessionPinStoreCap entries; at the cap a
// new unpinned session+model fails closed locally before any upstream send
// and existing pins keep serving. Restart clears pins (map lives in the
// Gateway); no disk persistence.

const sessionPinStoreCap = 4096

// sessionPin is the bound target for one derived session + model. It carries
// the full target identity (tier, internal credential identity, proxy pool,
// raw proxy identity, model) plus the target protocol and normalized upstream
// authority needed to validate the pin against the current route/config.
// Raw client signals, key material, and pin state never leave this struct.
type sessionPin struct {
	Tier      Tier
	CredID    string
	Pool      string
	ProxyRaw  string
	Model     string
	Protocol  Protocol
	Authority string
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

// migratePinsFrom carries all existing pin identities up to the cap without
// validity filtering. Removed or changed targets migrate as unresolved
// tombstone-like bindings: the pinned resolver still matches them and fails
// locally with 502 rather than re-establishing or falling back. Insertion is
// deterministic key order and stops at the cap without evicting. Restart
// remains the only clearing boundary (fresh store starts empty).
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
