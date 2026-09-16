# AGENTS.md — opencode2api

> Always-on agent contract for this repository. This file defines what the
> system is, who owns what state, which semantics MUST be preserved, and what
> counts as done. It is not a README, tutorial, or reference dump: for facts,
> follow the links in Canonical References and read the source.

## 1. System Mental Model

- opencode2api is a Go 1.24 protocol gateway for OpenCode Zen / Zen Go. It
  exposes OpenAI-compatible Chat Completions, Responses, and Models APIs plus
  the Anthropic Messages API, and forwards to upstream Zen (`prefer: zen`) or
  Zen Go (`prefer: go`) endpoints.
- The product adds OpenCode client headers (`User-Agent`, `x-opencode-*`),
  performs same-protocol passthrough or cross-protocol conversion
  (text, image, thinking/reasoning, tool definitions/calls/results), and
  tracks routing results.
- Two runtime roles exist and MUST NOT be conflated:
  - **Gateway (inference plane)** on `listen`: local auth, routing, upstream
    fan-out, protocol bridging, streaming.
  - **Admin / WebUI (management plane)** on `webui.listen`: config editing,
    token/upstream metrics, route diagnostics, three-protocol Playground, live
    log tail. It never serves inference traffic.
- This root guide governs the whole repo. A future nested `AGENTS.md` in any
  subdirectory, if created, SHOULD act as a local overlay for that scope only
  and MUST NOT be assumed to override this contract unless it explicitly says so.

## 2. Authority & State

- `config.json` is the writable authority for operator intent. It accepts `//`
  and `/* ... */` comments; saved output is normalized and comments are not
  preserved. The on-disk example shape lives in `config.example.json`.
- The effective Gateway state (named proxy pools with per-pool resolved
  proxies + proxyfile, unified credential×proxy target scheduler state,
  connection pools, model catalog snapshot) is in-memory runtime state built
  from config. NEVER treat
  it as editable directly; change it only by changing config (or the seed/env
  inputs that produce config) and letting the runtime rebuild. Scheduler
  state has six layers: proxy transport health (connectivity only),
  tier-qualified `(tier,pool,proxy)` 429 rate-limit cooldowns, tier-qualified
  `(tier,pool,proxy)` comparative channel-availability cooldowns for 403/5xx,
  per-(tier, credential) 401 cooldowns, per-(tier, credential) 429 cooldowns
  established only by two distinct proxies 429ing in one request, and
  per-(tier, credential, pool, proxy, model) target cooldowns for 403/5xx
  only. Anonymous Zen and authenticated Zen share the Zen channel state (Go
  stays isolated even for identical key text or raw URL); pool qualification
  is preserved throughout. There is no static key→proxy binding: one
  credential+pool deterministically prefers a stable proxy (soft affinity,
  recomputed from current pool contents) without any persisted map. Two
  distinct in-memory affinity authorities sit above these layers:
  route-session rotation overrides (per target-scope recovery string; the
  scope includes the proxy for anonymous Zen and excludes it for
  authenticated sessions) and client-session+model pins (durable binding
  selecting which target an established session+model may use); the former
  rotates the upstream session value, the latter fixes the binding itself.
  Anonymous pins fix the complete target including the proxy; authenticated
  pins fix tier, credential, pool, model, protocol, and authority while the
  proxy remains a mutable current selection under generation fencing.
- Proxy identity is two-level: `proxy_pools` names stable pool identities and
  `proxy_routing` assigns exactly one pool each to the anonymous, zen, and go
  channels (same or different). Top-level `proxies` / `proxyfile` are
  load-time legacy inputs only: they migrate to a `shared` pool on load and
  never persist. Pool names are operator identities, never IPs or URLs.
  Same pool name referenced by multiple channels shares one transport
  instance; different pool names isolate pool-qualified runtime state even for
  the same raw URL. The key↔proxy relation is deterministic soft affinity
  (like a stable user/VPN IP preference), never a hard binding.
  Only referenced pools are runtime resources; unreferenced pools are staged
  config (validated, never built/probed/counted).
- External authorities MUST NOT be duplicated into this guide or hardcoded:
  - Upstream `/v1/models` (Zen and Go) for model existence per tier.
  - The OpenCode capability directory (`models.opencode.ai`) for each model's
    native protocol and unsupported set.
  - `models.dev` for cost/deprecation (zero input+output cost, or
    case-insensitive `free` in the model ID, qualifies for anonymous routing).
- Projections (never authoritative): in-memory request/token/upstream metrics,
  recent attempts, Playground results, stdout JSON logs, the memory log ring,
  the on-disk model/metadata caches (`<config>.models.catalog.json`,
  `models.dev.json` compat cache), and the bounded redacted persistent
  history (`history/*.ndjson`: request/attempt/minute metadata only, never
  body/secrets). History never affects routing or readiness; it MUST NOT be
  edited to change behavior.
- Config parsing MUST use strict validation (unknown fields rejected). Any new
  config surface MUST follow the same rule.

## 3. Request and Change Propagation

- Inference spine (gateway). Every chat/responses/anthropic request follows:
  monitoring context → local `server_keys` auth → routing and per-tier request
  preparation (single shared preparation path, incl. thinking/tool-history
  normalization) → anonymous Zen assigned pool (free models only) → authenticated
  Zen/Go key tiers in `prefer` order → same-protocol passthrough or
  cross-protocol conversion → result recording (metrics, upstream attempts,
  usage when the upstream reports it).
- Session affinity spine: explicit client session headers or
  `metadata.session_id` win; otherwise the first user message derives a stable
  client session hash. The derived client session is the establishment identity
  and never leaves the process raw; upstream receives only the target-bound
  route session (upstream authority, tier, internal credential identity, proxy
  pool, raw proxy identity, target protocol; never the model, never raw
  secrets) in the upstream session headers and in the already-present body
  session fields. The authenticated route session excludes the proxy so a
  within-pool proxy move preserves the same upstream session value; the
  anonymous route session includes it. The first generation is a stable stateless derivation; only
  a 400 rotation stores a bounded in-memory override. Before a pin exists for
  session+model, one establishment owner freezes/HRW-orders the currently
  available targets and may walk proxies/tiers per §4; concurrent followers
  wait cancellably, then adopt the pin or contend to become the next owner.
  The first upstream 2xx, including a successful exact-400 replay, pins
  session+model to the complete target (tier, internal credential identity,
  pool, raw proxy identity, model, protocol/authority validity). After the pin,
  every request uses exactly that target with no cross-proxy/tier fallback and
  no anonymous→authenticated promotion; an active proxy429/credential/target
  cooldown fast-fails locally with the corresponding protocol/status/
  Retry-After without a send, and a removed/unhealthy/unresolvable target fails
  locally with 502. Same-target transient retry and exact-400 replay remain per
  §4. For authenticated pins the binding is proxy-independent (tier,
  credential, pool, model, protocol, authority) while the current proxy is a
  fenced mutable selection: an established session may move within the same
  pool, credential, tier, model, protocol, and authority only after a transport
  failure under the normal send budget or an HTTP 429, before any client bytes
  and never across tiers, keys, or pools; a successful move updates only the
  current proxy. No move occurs on 400/401/403/408/425, ordinary 4xx, or 5xx.
  Anonymous pins stay exactly proxy-affine with no cross-proxy recovery. Pins are process-lifetime and never expire/evict; the store is
  fixed-bounded (see `sessionPinStoreCap` in `scheduler.go`) and a new
  session+model at capacity fails closed locally with 502 before any send
  while existing pins keep serving. Restart is the explicit clearing boundary.
- Config change spine (save / Apply / reload-from-disk): parse and validate
  the full candidate → build new pools and Gateway instance → migrate
  still-future scheduler cooldowns, proxy health, still-fresh
  route-session overrides by identity, and session+model pins without validity
  filtering as tombstone-like identity (credential by tier+key, proxy health
  by pool+URL, target by full identity, proxy429 and channel by
  tier+pool+URL identity with aggregate counts in the migration log, credential
  429 by tier+key, route session by target scope with the client dimension
  excluded from validity — authenticated scopes migrate only when proxy-free
  and pool-routable — and still-fresh/idle-TTL filtering, pins migrated by
  identity up to the pin cap with aggregate count
  only where a removed target remains pinned and fails locally with 502 rather
  than re-establishing; new resources start at
  zero / stateless, removed ones drop except pinned tombstones) → atomically write
  (temp file + `config.json.bak` + replace) → atomically switch new requests
  to the new instance. On write or init failure the old instance MUST keep
  serving; already-started requests MUST NOT be interrupted. Saved JSON is
  normalized.
- Hot vs. restart: keys, proxies (pools, files, and routing references),
  upstream addresses, retry, models,
  performance, preferred tier, and log level take effect immediately.
  `listen`, `webui.listen`, and `webui.enabled` are saved but REQUIRE a
  process restart. NEVER claim a listen-plane edit is live without restart.
- Cache refresh spine: model lists + capability directory refresh
  concurrently every `models.refresh_seconds`; `models.dev` refreshes every
  24h with fixed timeout. Refresh uses a stateless key x healthy-proxy
  traversal that never reads or writes foreground credential/target/proxy429
  cooldowns and never changes proxy healthy/checking (healthy proxies
  observed read-only; no syncProxyResult/verifyProxyAfterError). A refresh
  context deadline/cancel is only a refresh failure, never a proxy signal.
  Refresh failure MUST keep the previous snapshot;
  startup uses valid disk cache before the first live refresh.

## 4. Invariants (MUST Preserve)

- Anonymous channel: fixed Zen credential (`Bearer public` for OpenAI-family
  upstream, `x-api-key: public` for Anthropic upstream); free models try it
  first, non-free models skip it entirely. Proxy/tier fallback below applies
  only to unpinned establishment; once pinned, the pinned target serves alone
  per the Session affinity spine. Dispersion and fallback belong to
  the frozen order only (HRW/round-robin); same-target retry never disperses.
  Each available proxy gets at most one fallback send and is NEVER truncated
  by `retry.max_attempts`; the whole channel additionally owns one shared
  transient token for the first transport error, 408/425, or 5xx (same target,
  same route session, same body). Ordinary 4xx MUST end the anonymous channel
  without scanning remaining proxies, then may enter the authenticated tiers;
  only proxy exhaustion without a 400 enters them otherwise. Client cancel or
  the shared request deadline ends the route immediately with no further
  retry/fallback and no state change. The first exact HTTP 400 on any
  candidate replays exactly once on the same target with a rotated route
  session (same request ID, same credential/proxy/protocol, always before any
  client bytes; Responses replays also drop stale previous_response_id/
  reasoning refs; never consumes the transient token). The replay result is
  final for the whole route: success returns normally, a second 400 returns
  that 400, and any other replay outcome returns as-is without scanning
  remaining proxies or entering the authenticated tiers. A replay 2xx pins the
  session+model when still unbound.
- Authenticated tiers: each tier owns its own `retry.max_attempts` real-send
  budget (each first send plus each same-target transient retry consumes it)
  and its own transient token. Inside a tier, while still unpinned, only transport errors, 408/425,
  401/403, 429, 5xx, and other retryable responses advance the frozen list;
  401/403/429 never retry same-target; transport/408/425/5xx retry same-target
  at most once per tier; any other 4xx MUST end that tier. The first exact
  HTTP 400 on any candidate follows the same same-target one-replay rule as
  anonymous (route-session rotation, Responses stale-ref cleanup, replay is
  the route's last recovery action, always allowed once extra beyond the
  ordinary budget); a second 400 terminates the whole route and MUST NOT fall
  back to the other tier. Only a non-400 tier failure falls back to the other
  tier that actually serves the model and has keys, ordered by `prefer` (`go`
  default: Go → Zen). Cancel/deadline ends the route without further tiers. A
  replay 2xx pins the session+model when still unbound; once pinned, the
  pinned target serves alone per the Session affinity spine.
- Scheduler state: proxy health is transport connectivity only (HTTP statuses
  never change it); a single foreground transport error never cools
  credential/target/proxy429/channel state and never flips proxy healthy directly —
  it only triggers the existing async neutral proxy health verification,
  and only that independent probe on explicit isProxyFailure may flip
  healthy. 401 cools the tier+credential globally; 429 cools the tier-qualified
  `(tier,pool,proxy)` proxy globally across models, credentials, channels, and
  client sessions (anonymous and authenticated Zen share the Zen entry; Go stays
  isolated even for the same raw URL; same raw URL in different pools stays
  isolated), so one proxy's rate limit filters every model/credential using that
  tier+pool+proxy; two distinct proxies 429ing with the same tier+credential in
  one request additionally cool that tier+credential using the second Retry-After
  (Zen and Go stay isolated even for identical key text), while a single proxy429
  never does; 403/5xx cool the single (tier, credential, pool, proxy, model) target,
  so one model's 403/5xx never affects another, and only with comparative success
  (same tier+credential succeeding on another node) cool the tier-qualified
  `(tier,pool,proxy)` channel — a lone 403/5xx without comparative success is
  display-only; 408/425 are transient neutral
  (retryable, never cooling); ordinary 4xx (including exact 400) is neutral
  and 2xx clears this target and this credential's 401 state, plus the
  tier-qualified proxy429/channel and credential429 state only when the send started
  at or after the latest recorded failure (stale in-flight 2xx never clears a newer
  cooldown; newer failures stay authoritative). Proxy429/channel use deterministic exponential
  backoff with Retry-After max/cap, no same-target retry, bounded maps with
  stale prune/eviction proportional to proxy resources, and still-future
  migration by tier+pool+proxy with aggregate counts only. Route-session overrides
  are bounded in-memory Gateway authority (first generation stateless,
  rotation stored with idle TTL and deterministic eviction; never persisted,
  never projected, never logged), in contrast to session+model pins which are
  process-lifetime, never expire/evict, and are fixed-bounded fail-closed.
  Restart clears all in-memory cooldowns/overrides/pins. Model/capability refresh is stateless and
  touches none of these layers. The admin probe is one of the explicit
  transport-health actions: it may flip proxy healthy and nothing else (never
  clears/sets proxy429); manual refresh shares the scheduled stateless path
  and its concurrency gate, so it never reads or writes proxy or foreground
  scheduler state.
- Availability management: `POST /api/availability/check` sends real minimal
  inference (`stream:false`, minimal messages, ~1 token max output) per lane, never
  `GET /v1/models` as an availability verdict. Probe models are directory-driven
  (anonymous: free + Zen-servable; Zen/Go: actually served by that tier; never
  hardcoded IDs); a lane without a directory model reports `no_model`/`inconclusive`
  without fake success. Active nodes only, inherits admin auth/CSRF/Origin checks,
  `no-store`, strict `{}`, rate/concurrency/send caps, partial-result reporting,
  and redaction. Zen public probes the Zen-channel nodes and never affects Go or
  configured credentials; a real-credential 401 cools that tier+credential; success
  clears tier-qualified proxy/channel state (stale-fenced) while success+429 writes
  proxy429 and success+403/5xx writes channel state; two 429s without success write
  credential429 while a single 429 stays display-only; ambiguous outcomes
  (transport-inconclusive, 408/425, ordinary 4xx, parse/empty, timeouts) stay
  display-only. Per-send/admin timeouts are diagnostic only. The single-proxy probe
  stays distinct (one proxy, transport connectivity target). Both update health only
  through the existing independent last-healthy protection. All configured custom
  fallback channels are probed in the same response with their configured model
  plus channel protocol (chat → `/v1/chat/completions`, responses →
  `/v1/responses`, via the unified API-root rule);
  custom results never write Zen/Go scheduler or transport health and never record
  inference metrics/history. Model discovery may still use `GET {root}/v1/models`
  (host, `/v1`, either full inference endpoint all normalize to `/v1/models`)
  but MUST never be called an availability verdict; discovery failures keep code
  `discover_failed` with safe `reason` (`dns_error`/`connect_refused`/`timeout`/
  `tls_error`/`transport_error`/`non_2xx`/`invalid_json`/`empty_list`),
  `endpoint`, `http_status`, `elapsed_ms`, never body/key/Authorization/raw error.
- Custom session fallback: strict `fallback` config (`active` + unique-name
  `channels` with `name`/`base_url` http-https/`api_key`/`model`/`protocol`
  (`chat`=Chat Completions default, `responses`=Responses; empty normalizes to
  `chat`, other values strictly rejected) plus optional per-channel
  `reasoning_effort` (`""`=supplier default, `low`/`medium`/`high`; empty
  normalizes to `""`, other values strictly rejected; never in the binding
  identity, never migrates as identity, never probed); active empty or referencing an
  existing channel) persists via config authority/RuntimeManager.Apply
  with masked GET, authenticated-session reveal (POST-only non-GET behind admin
  session auth + CSRF + Origin, no-store response, and full-chain
  redaction without plaintext logging). Only a request
  entering with an existing anonymous session+model pin that gets HTTP 429 on the
  pinned anonymous path (live 429 or tier-qualified proxy429 local 429) may take
  over; unbound anonymous 429 and authenticated-pin 429 never trigger. Without an
  active channel the original 429 stands; with one, the current request retries at
  once through the then-active custom OpenAI-compatible channel (`{root}/v1/chat/completions`
  for chat, `{root}/v1/responses` for responses via the unified API-root rule,
  Bearer key, client entry converted to the channel protocol via the strict bridge
  with model rewritten to the channel model plus the channel reasoning_effort
  override (chat sets `reasoning_effort`, responses merges/creates
  `reasoning:{effort}`, empty never injects), response/stream transcoded back;
  never listed in public `/v1/models`; no supplier session affinity; never touches
  Zen/Go scheduler layers)
  and the session binds first to that full channel identity (name + normalized base
  URL/authority + key fingerprint/identity + configured model + protocol) so later
  requests for the session serve exclusively there without drifting on active
  switches. Custom errors return as-is with no fallback to native or other custom
  channels. Takeover state is session-keyed (not session+model), process-lifetime,
  unpersisted, unprojected, unexpired, bounded 4096, first-wins, capacity-502
  without claiming unrecorded takeover; hot Apply migrates without validity
  filtering as tombstones and a deleted/identity-mismatched (including protocol
  change) channel fails locally with 502. Restart clears.
- Health readiness: healthz keeps all existing fields and adds additive
  routing readiness (global credential availability plus assigned-pool health;
  per-model target cooldowns, proxy429 cooldowns, channel cooldowns, and credential429
  cooldowns never count). Zero
  globally available channels degrades readiness; one model's targets all
  cooling or one proxy's 429 cooling never does.
- Streaming: once bytes have been written to the client, the Gateway MUST
  NOT switch upstreams or regenerate; error-class upstream stream signals
  MUST surface as structured target-protocol error events, never as clean
  finish or silent truncation.
- Capability gating: models with unknown capability MUST NOT be exposed;
  model IDs MUST NOT be hardcoded — exposure is driven by the capability
  directory (manual `models.protocols` covers experiments only). Same-name
  models report metadata per actually-serving tier.
- Identity hygiene: `proxy_node` names a proxy node, NEVER the egress IP.
  `proxy_pool` names the owning pool; resource identities are pool-qualified.
  Real keys render as last-5-characters (or `anonymous`); config-secret
  fingerprints stay SHA-256 internal. Responses with the same model name in
  different tiers MUST keep their tier attribution.
- Error envelopes: gateway errors MUST keep the target protocol envelope
  (chat / responses / anthropic); diagnostic/admin HTTP semantics
  (e.g. Playground returning admin-200 with embedded upstream status) MUST
  NOT be silently changed.
- Conversion strictness: input content blocks the bridge cannot losslessly
  express MUST fail loudly, NEVER be silently dropped. Reasoning/thinking
  normalization applies only where the target protocol requires it.

## 5. Engineering Rules

- Go module, standard library first. The only direct dependency is
  `golang.org/x/crypto` (see `go.mod`); adding a dependency MUST have an
  explicit, stated reason and updated `go.mod`/`go.sum`.
- WebUI is a single `go:embed` HTML/CSS/JS bundle with no frontend build
  chain. WebUI changes MUST stay dependency-free and MUST render dynamic
  management data as DOM text nodes (no unsanitized HTML injection).
- Container posture MUST be preserved: non-root user, read-only filesystem,
  `no-new-privileges` (see `Dockerfile` / `compose.yaml`).
- Go edits MUST remain `gofmt`-clean. There is no repo lint, typecheck, or
  coverage gate — NEVER invent one or claim one ran.
- Compatibility MUST be capability-directory driven; NEVER hardcode model IDs
  to add or gate a model.
- Config schema changes MUST keep strict unknown-field rejection and the
  save/Apply transactional semantics (validate-first, build-new-runtime,
  atomic switch, old-instance-on-failure). New listeners or planes MUST state
  explicitly whether they are hot-swapped or restart-required.
- All new output (logs, metrics, admin payloads) MUST flow through the
  existing redaction model — no new raw-secret or body paths.

## 6. Security & Observability

- Admin auth: Argon2id password hash (`webui.password_hash`; plaintext
  `webui.password` is bootstrap-only, ≥ 10 chars, deleted after first
  successful start), server-side sessions, HttpOnly Strict cookies, CSRF
  token + Origin check on mutating management calls, login rate limiting.
  NEVER weaken or bypass these; new management endpoints MUST inherit them.
- Sensitive-data handling: NEVER log or return full local keys, upstream
  keys, Authorization/Cookie/password values, or proxy credentials. Request
  message bodies MUST NOT be logged by default. Sensitive admin responses
  MUST carry `no-store` (or `no-cache, no-store`).
- Logging: stdout is single-line JSON (time, level, component, event, plus
  request/model/tier/status/latency/attempts/key-tail where applicable);
  established requests log a routing event at the right level. The memory ring
  (`logging.ring_size`, 100–50000) backs the WebUI live tail only. NEVER add
  body/key/credential fields to either sink.
- Metrics/usage semantics: usage is recorded only when the upstream reports
  it (plain, same-protocol SSE, and cross-protocol SSE alike) — NEVER
  estimate. Lifetime is process-start scoped; last-hour is 60 one-minute
  buckets; bounded retention (recent requests/attempts capped, admin caps
  response size). Restart clearing in-memory history is expected behavior;
  the durable history projection is independent and survives restarts.
- Persistent history is a bounded redacted projection only: NDJSON
  request/attempt/minute metadata, never bodies or secrets; directory 0700
  and files 0600; failures never affect routing or readiness. Container
  read-only posture is unchanged; the history directory must be a writable
  volume when enabled.

## 7. Validation and Done

- Authoritative gate: `go test ./...` MUST pass (this is the CI gate).
  `go build -o opencode2api ./` is the supported build check.
- Go changes MUST be `gofmt`-clean before finishing.
- A task is done only when: the gate above passes for Go-affecting changes
  (or the change is provably not Go-affecting), preserved semantics in
  §4 are unbroken, no new unredacted output exists, and no invented gates
  are claimed.
- Do not duplicate volatile inventories here (model lists, cost tables,
  metric field dumps, per-key configs). Point at the authoritative source
  instead.

## 8. Git & Artifacts

- Git write operations (commit, push, branch, PR, tag, merge) REQUIRE the
  user's explicit request. This contract MUST NOT be read as authorization.
- NEVER commit or submit: real `config.json`, on-disk caches, built binaries,
  logs, credentials, or secrets. `config.example.json` is the only
  config-shaped file safe to share.
- Do not create files outside the requested scope; do not restructure,
  reformat, or "improve" unrelated code.

## 9. Canonical References

- `README.md` — product behavior, API paths, WebUI, config semantics.
- `config.example.json` — authoritative config shape (never paste real secrets).
- `go.mod` / `go.sum` — toolchain (`go 1.24`) and dependency set.
- `Dockerfile`, `compose.yaml`, `docker-entrypoint.sh` — container posture,
  ports, healthcheck, volume/seed wiring.
- `.github/workflows/release.yml` — CI gate (`go test ./...`) and release
  build matrix.
- Source of truth for behavior: `gateway.go`, `scheduler.go`, `availability.go`, `convert.go`, `stream.go`,
  `models.go`, `model_metadata.go`, `pool.go`, `runtime.go`, `config.go`,
  `admin.go`, `observability.go`, `password.go`, `ids.go`, `main.go`,
  `fallback.go`, `availability_probe.go`, `admin_fallback.go`
  (read them; this guide states relationships, not code locations).
- No `CLAUDE.md` exists in this repo and none SHOULD be created; tool-specific
  entries, if ever needed, MUST be pointers to or synchronized copies of this
  file, never independently maintained contracts.
