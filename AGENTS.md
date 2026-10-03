# AGENTS.md — opencode2api

> Always-on agent contract for this repository. This file defines what the
> system is, who owns what state, which semantics MUST be preserved, and what
> counts as done. It is not a README, tutorial, or reference dump: for facts,
> follow the links in Canonical References and read the source.

## 1. System Mental Model

> **Supreme Principle — Session Recovery (会话恢复) is the highest behavioral principle.** The gateway is a *session recovery system*, not an HTTP-status-driven retrier. Purpose: every recovery exists to let the session continue. Closed loop: `observe stability -> resolve stable cause -> continue session or faithfully return` (per target protocol). Unified semantic three layers (not object-queue projection): **L1 / Observe stability** — 观察稳定性，先判断当前状态，本身已稳定则不重试，未稳定则用受约束的多次请求观察至稳定或停止，`retry` 属于这一层的观察手段，不是错误码/对象切换队列，从不立即重发、从不无限；**L2 / Resolve stable cause** — 解决已确认的稳定原因，按 `cause + current object + 当前上下文` 选择最小针对性动作：非法请求清理/修正（同目标 corrective replay）、代理节点不可用时换节点、对象耗尽时选择下一个可用对象、503 不设独立的 L2 持续观察（稳定性观察仅属 L1 并受其既有边界约束），L1-final 503 后仅按既有对象选择/耗尽规则解决，无法解决则进入忠实返回，代理/代理池/凭证/备用渠道/恢复域都只是不同粒度的对象/候选，不是 L2/L3 的固定层级；**L3 / Continue session or faithfully return** — 解决成功则继续同一会话（保持必要的 pin/route-session/协议身份约束），解决失败或达到停止边界则按目标协议忠实返回，`fallback` 只是 L2 中一种对象选择/解决动作，不是固定“最后一级”，“恢复域”只是对象/候选组织与可用性过滤的上下文，不是比对象更高的恢复层级。Stability first. Recovery domain: bound session = that session+model pin's credential+pool sendable proxies; unbound = this request's frozen eligible targets; exhaustion = health/cooldown filtering leaves no sendable object. No single status code alone qualifies for fallback; `retry.max_attempts` is L1 观察层判定稳定性所需的最小请求/观察计数，不是错误码/对象队列的统一配额，预算数值与退避以 ADR 为准且不冻结逐状态码矩阵，本指南仅保留抽象不变式。

- opencode2api is a Go 1.24 protocol gateway for OpenCode Zen. It
  exposes OpenAI-compatible Chat Completions, Responses, and Models APIs plus
  the Anthropic Messages API, and forwards to upstream Zen endpoints.
- Native channels are exactly two, and both use Zen upstream: **anonymous**
  (fixed public credential, free models only) and **authenticated**
  (configured `keys`). There is no Go channel, no Go keys, no `prefer`
  ordering, and no Go pool/upstream/model exposure in canonical behavior.
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
- Canonical config is Zen-only: one `keys` list; `proxy_routing.anonymous`
  and `proxy_routing.authenticated`; Zen upstream only. Legacy `zen` / `go` /
  `prefer` fields are accepted as load-time migration inputs only and are
  normalized away on save.
- The effective Gateway state (named proxy pools with per-pool resolved
  proxies + proxyfile, unified credential×proxy target scheduler state,
  connection pools, model catalog snapshot) is in-memory runtime state built
  from config. NEVER treat
  it as editable directly; change it only by changing config (or the seed/env
  inputs that produce config) and letting the runtime rebuild. Scheduler
  state has seven layers: proxy transport health (connectivity only, distinct
  from bounded temporary transport-suspect candidate filtering),
  channel-qualified `(channel,pool,proxy)` 429 rate-limit cooldowns,
  channel-qualified `(channel,pool,proxy)` comparative channel-availability
  cooldowns for 403/5xx, per-(channel, credential) 401 cooldowns,
  per-(channel, credential) 429 cooldowns established only after every
  currently eligible proxy for that credential has returned live 429 in one
  request (frozen-eligible exhaustion using the last Retry-After; partial 429
  never writes it, pre-cooled skips never count as live evidence, single
  eligible exhausting alone still writes it), per-(channel, credential, pool, proxy,
  model) target cooldowns for 403/5xx only, and per-(channel, pool, proxy)
  short-lived transport-suspect filtering for true transport failures only
  (bounded temporary candidate filtering, never readiness). Unbound
  authenticated establishment additionally keeps request-local suspect
  evidence: a qualifying L1-final true transport error records its
  (channel,pool,proxy) identity for this request only, and a later frozen
  candidate with the same identity skips while that suspect cooldown is
  still active (expired cooldowns never skip; distinct pools never match).
  Suspect skips count as unavailable with the domain marked entered and never
  as live429/credential429 evidence. Scheduler identity has no
  Zen-vs-Go tier separation on live paths: anonymous and authenticated share
  Zen channel-scoped proxy state as appropriate while retaining
  credential/target/session distinctions and the current pin/retry invariants.
  There is no static key→proxy binding: one credential+pool deterministically
  prefers a stable proxy (soft affinity, recomputed from current pool
  contents) without any persisted map. Two distinct in-memory affinity
  authorities sit above these layers: target-bound route sessions (proxy-free
  native scope; first generation is a stable stateless derivation, the store
  keeps only legacy/migrated overrides with idle TTL and no live path creates
  new overrides) and client-session+model pins
  (durable binding selecting which target an established session+model may
  use); the former derives the upstream session value, the latter fixes the
  binding itself. Both anonymous and authenticated pins fix channel,
  credential, pool, model, protocol, and authority while the proxy remains a
  mutable current selection under generation fencing.
- Proxy identity is two-level: `proxy_pools` names stable pool identities and
  `proxy_routing` assigns exactly one pool each to the anonymous and
  authenticated channels (same or different). Top-level `proxies` /
  `proxyfile` are load-time legacy inputs only: they migrate to a `shared`
  pool on load and never persist. Pool names are operator identities, never
  IPs or URLs. Same pool name referenced by multiple channels shares one
  transport instance; different pool names isolate pool-qualified runtime
  state even for the same raw URL. The key↔proxy relation is deterministic
  soft affinity (like a stable user/VPN IP preference), never a hard binding.
  Only referenced pools are runtime resources; unreferenced pools are staged
  config (validated, never built/probed/counted).
- External authorities MUST NOT be duplicated into this guide or hardcoded:
  - Upstream `/v1/models` (Zen) for model existence.
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
  monitoring context → local `server_keys` auth → routing and per-channel
  request preparation (single shared preparation path, incl.
  thinking/tool-history normalization) → anonymous assigned pool (free models
  only) → authenticated keys → same-protocol passthrough or cross-protocol
  conversion → result recording (metrics, upstream attempts, usage when the
  upstream reports it).
- **Supreme recovery closed loop (gateway first) — `observe stability -> resolve stable cause -> continue session or faithfully return`.** The gateway is a *session recovery system*, not an HTTP-status-driven retrier; every recovery exists to let the session continue. The gateway absorbs steady-state fluctuations before exposing them to the client. **Unified semantic three layers (not object queue):** **L1 / Observe stability** — 先判断当前状态，本身已稳定则不重试，未稳定则用受约束的多次请求观察至稳定或停止，`retry` 属于这一层的观察手段，不是错误码/对象切换队列，从不立即重发、从不无限；**L2 / Resolve stable cause** — 解决已确认的稳定原因，按 `cause + current object + 当前上下文` 选择最小针对性动作：非法请求清理/修正（同目标 corrective replay，始终 before any client bytes）、代理节点不可用时换节点、对象耗尽时选择下一个可用对象、503 不设独立的 L2 持续观察（稳定性观察仅属 L1 并受其既有边界约束），L1-final 503 后仅按既有对象选择/耗尽规则解决，无法解决则进入忠实返回，代理/代理池/凭证/备用渠道都只是不同粒度的对象/候选，不是 L2/L3 的固定层级；**L3 / Continue session or faithfully return** — 解决成功则继续同一会话（保持必要的 pin/route-session/协议身份约束），解决失败或达到停止边界则按目标协议忠实返回，`fallback` 只是 L2 中一种对象选择/解决动作，不是固定“最后一级”，“恢复域”只是对象/候选组织与可用性过滤的上下文，不是比对象更高的恢复层级，不能把 proxy/pool/channel/domain 映射成 L2/L3。Recovery domain: bound = that session+model pin's credential+pool sendable proxies; unbound = this request's frozen eligible targets; exhaustion = health/cooldown filtering leaves no sendable object. No single HTTP status alone qualifies for fallback; `retry.max_attempts` is L1 观察层判定稳定性所需的最小请求/观察计数，不是错误码/对象队列的统一配额，预算数值与退避以 ADR 为准且不冻结逐状态码矩阵，本指南仅保留抽象不变式，MUST NOT be invented as a complete matrix here. HTTP 400 is an illegal request and non-fluctuating: its same-target replay after cleaning Responses reasoning/`previous_response_id` 属于 L2 的稳定非法请求解决动作（corrective / policy-removal），不是 L1 retry，完成後按 L3 终止/忠实返回；it MUST keep same target, same route session, and same request identity, and its outcome is always the route's last recovery action with no further candidate, channel, or fallback. `fallback` is an L2 object-selection action taken only when the current object is stably unavailable and recovery-domain availability filtering leaves no sendable object; a single error code MUST NOT mechanically cut to fallback without exhaustion evidence of the current domain's available objects. The existing scheduler / pin / route-session domain ownership and the streaming committed-bytes stop invariant remain unchanged.
- Session affinity spine: explicit client session headers or
  `metadata.session_id` win; otherwise the first user message derives a stable
  client session hash. The derived client session is the establishment identity
  and never leaves the process raw; upstream receives only the target-bound
  route session (upstream authority, channel, internal credential identity,
  proxy pool, raw proxy identity, target protocol; never the model, never raw
  secrets), encoded on the wire as canonical OpenCode-shaped pseudonymous IDs
  (internal `rss_*` stays target-bound/non-raw; headers/body carry its stable
  canonical mapping). OpenCode-provider requests use the official header set
  only (bare `opencode/x.y.z` UA, `x-opencode-client`, `x-opencode-session` /
  `request` / `project`, optional canonical parent; no generic affinity headers);
  Responses `prompt_cache_key` / `store` / session body fields match the same
  wire session. Both native route sessions are proxy-free so a
  within-pool proxy move preserves the same upstream session value and body
  bytes. The first generation is a stable stateless derivation; no live path
  rotates or stores an override (exact-400 replays keep the same session).
  Before a pin exists for
  session+model, one establishment owner freezes/HRW-orders the currently
  available targets and may walk proxies per §4; concurrent followers
  wait cancellably, then adopt the pin or contend to become the next owner.
  The first upstream 2xx, including a successful exact-400 replay, pins
  session+model to the binding (channel, internal credential identity,
  pool, model, protocol/authority validity) with the successful proxy as the
  initial current selection.   After the pin,
  every request stays inside that binding with no cross-credential/pool/channel
  fallback and
  no anonymous→authenticated promotion. Bound anon/auth share one unified walk
  inside the same credential+pool+channel/model/protocol/authority (current
  first, stable affinity order, one shared proxy-free route session and
  identical body bytes, local proxy429 cooldowns skipped without new evidence,
  before any client bytes, never truncated by `retry.max_attempts`): stable
  429 walks all sendable proxies with no L1; stable 403 and L1-final true
  transport / pre-commit startup / 408 / 425 / 5xx walk the frozen sendable
  proxies after the same-target L1 observation; stable 401 is a global
  credential failure with no further sends under the same credential while
  still counting as binding unavailability. Exact-400 same-target corrective
  replay stays route-terminal on any replay result. Ordinary 4xx,
  build/config-identity/tombstone invalid, cancel/deadline, and committed
  bytes stay faithful/local failures with no new credential/channel/pool
  inside the native pin. A successful 2xx on an alternate updates only the
  current proxy under generation fencing (concurrent moves converge to one
  winner), preserving the proxy-free wire session/body identity. A
  removed/unhealthy/unresolvable or identity-mismatched pinned binding fails
  locally with 502 and never falls back on deletion. A valid binding whose
  actual-pool proxies are all filtered by current health/cooldowns may
  zero-send to the active custom channel (including pre-cooled 429) with no
  fabricated live evidence/attempts/`credential429`/metrics; otherwise a valid
  binding reaches the active custom channel after object-unavailable
  exhaustion, including single-proxy L1-final and stable credential-401, with
  no prior-switch/consumed prerequisite. Full live-429 proof stays distinct
  solely for `credential429` writes (mixed/filtered evidence never qualifies).
  Without an active channel the native status/`Retry-After` is kept. Same-target
  L1 observation and exact-400 replay remain per §4.
  Both channels share these pin/move semantics. Pins are process-lifetime and never expire/evict; the store is
  fixed-bounded (see `sessionPinStoreCap` in `scheduler.go`) and a new
  session+model at capacity fails closed locally with 502 before any send
  while existing pins keep serving. Restart is the explicit clearing boundary.
- Config change spine (save / Apply / reload-from-disk): parse and validate
  the full candidate → build new pools and Gateway instance → migrate
  still-future scheduler cooldowns, proxy health, still-fresh
  route-session overrides by identity, and session+model pins without validity
  filtering as tombstone-like identity (credential by channel+key, proxy health
  by pool+URL, target by full identity, proxy429 and channel by
  channel+pool+URL identity with aggregate counts in the migration log,
  credential 429 by channel+key, route session by native target scope with the client
  dimension excluded from validity — both native scopes are proxy-free and
  migrate only when pool-routable; legacy proxy-bound overrides drop and
  re-derive statelessly — and still-fresh/idle-TTL filtering, pins
  migrated by identity up to the pin cap with aggregate count
  only where a removed target remains pinned and fails locally with 502 rather
  than re-establishing; new resources start at
  zero / stateless, removed ones drop except pinned tombstones) → atomically write
  (temp file + `config.json.bak` + replace) → atomically switch new requests
  to the new instance. On write or init failure the old instance MUST keep
  serving; already-started requests MUST NOT be interrupted. Saved JSON is
  normalized.
- Hot vs. restart: keys, proxies (pools, files, and routing references), Zen
  upstream address, retry (including the single 429 base/cooldown), models,
  performance, and log level take effect immediately.
  `listen`, `webui.listen`, and `webui.enabled` are saved but REQUIRE a
  process restart. NEVER claim a listen-plane edit is live without restart.
- Cache refresh spine: model list + capability directory refresh
  concurrently every `models.refresh_seconds`; `models.dev` refreshes every
  24h with fixed timeout. Refresh uses a stateless key x healthy-proxy
  traversal that never reads or writes foreground credential/target/proxy429
  cooldowns and never changes proxy healthy/checking (healthy proxies
  observed read-only; no syncProxyResult/verifyProxyAfterError). A refresh
  context deadline/cancel is only a refresh failure, never a proxy signal.
  Refresh failure MUST keep the previous snapshot;
  startup uses valid disk cache before the first live refresh.

## 4. Invariants (MUST Preserve)

> **Implementation honesty (current baseline).** Invariants below describe the approved completed baseline — 统一会话恢复完整闭环：单候选权威（`classifyStableCause`/`decideCandidateRecovery`/`recoverSingleCandidate`）与单域权威（`decideDomainRecovery`，`AllowCustom` 经 `Recovered400/Cancelled/Committed` 停止门）统一全部重试/回退判定；L1 同目标有界观察仅 `true transport`/`408`/`425`/`500-599` 含 `503`/`pre-commit stream-startup`（`retry.max_attempts` 含首次，`100ms` 下限，`interval`/`Retry-After` 取 max，无 L1 指数/jitter，调度冷却指数退避仍按既有 caps）、L2 `400` 一次同目标 corrective replay 后 route-terminal（任何重放结果）、已绑定 anon/auth 统一漫游（`429`/`403`/`L1-final transport-startup-408-425-5xx` walk、`401` 同凭证停发但计绑定不可用）、pinned 接管为 valid-binding 对象不可用耗尽（含单代理 L1-final 与稳定凭证 401，无 consumed/预切换前提；实际池全过滤时可零发送接管，含预冷 429，无伪造证据；`credential429` 仅全量 live429 写入）及 L3 取消/deadline/committed 停止均已定版；`attempt_timeout_seconds` 仅覆盖单次原生发送启动（响应头至首个可交付 SSE 事件），`timeout_seconds` 为最终预算（客户端流启动期限在可交付事件撤销，非流全请求期限不撤销，成功 SSE 尾流存活于 attempt 计时之外，预算先尽则不保证全耗尽/接管）；详细矩阵正典为 `docs/adr/0001` 与 `recovery.go`/`gateway.go`/`scheduler.go`，本指南仅保留高层不变式。

- Anonymous channel: fixed Zen credential (`Bearer public` for OpenAI-family
  upstream, `x-api-key: public` for Anthropic upstream); free models try it
  first, non-free models skip it entirely. Anonymous free-tier sends go
  upstream as agent-shaped streams with core tools; non-stream callers receive
  collapsed protocol-correct JSON and native anonymous availability uses the
  same path. Unpinned establishment walks the frozen proxy order except that
  the first real stable 401 ends remaining same-credential proxy sends
  (credential-scoped L2 cause, skips count as unavailable without sends) and,
  on full anonymous 429 exhaustion or 401-skipped exhaustion without a 400,
  still enters the authenticated channel; once pinned, the binding walks the
  same credential+pool per the Session affinity spine (not a single target).
  Dispersion and fallback belong to
  the frozen order only (HRW/round-robin); same-target L1 observation never
  disperses. Each same-target transient (transport error, 408/425, 500-599,
  stream startup failure) is observed up to the unique L1 limit (normalized
  `retry.max_attempts`, including the first send); 400/429/ordinary 4xx are
  stable and never enter L1, and candidate traversal is bounded by the frozen
  slice. The 429 chain walks all sendable proxies (no candidate-send budget).
  Ordinary 4xx MUST end the anonymous channel
  without scanning remaining proxies, then may enter the authenticated channel;
  only proxy exhaustion without a 400 enters it otherwise. Client cancel or
  the shared request deadline ends the route immediately with no further
  retry/fallback and no state change. The first exact HTTP 400 on any
  candidate is a corrective / policy-removal action, not a retry: it replays
  exactly once on the same target with the same route session (same request
  ID, same credential/proxy/protocol, same wire session bytes, always before any
  client bytes; Responses replays also drop stale previous_response_id/
   reasoning refs; no override is stored).
   The replay result is
  final for the whole route: success returns normally, a second 400 returns
  that 400, and any other replay outcome returns as-is without scanning
  remaining proxies or entering the authenticated channel. A replay 2xx pins the
  session+model when still unbound.
- Authenticated channel: `retry.max_attempts` is the unique L1 same-target
  stability observation limit, including the first send. It never truncates
  candidate traversal, which is bounded by the frozen eligible/candidate slice;
  the 429 chain walks all currently sendable credential×proxy candidates to
  exhaustion. `retry.max_attempts`
  is the minimum observation/request count to judge stability, not a uniform
  error-count quota; per-status mapping is still governed by the state matrix.
  Inside the channel, while still
  unpinned, only transport errors, 408/425, 401/403, 429, 5xx, and other
  retryable responses advance the frozen list; 401 advances only to the next
  distinct credential (remaining same-credential frozen targets are skipped as
  the credential-scoped L2 cause without sends, still counted as unavailable);
  a live 429 via a real send in this unbound auth request establishes a
  request-local proxy L2 cause for its `(tier/channel,pool,proxy)` identity
  (shared across credentials): later frozen candidates with the same identity
  are skipped without a POST (pre-existing cooldowns alone never trigger this;
  only request-local live-429 evidence does), counted as unavailable+entered
  without attempts, upstream records, or scheduler writes; a qualifying
  L1-final true transport error in this unbound auth request likewise
  establishes a request-local `(channel,pool,proxy)` suspect cause for later
  same-identity frozen candidates, skipped only while that suspect cooldown
  stays active (expired cooldowns never skip; 408/425/5xx/stream-startup/429/
  cancel never establish it), counted as unavailable+entered without
  attempts/records/writes and never as live429/credential429 evidence;
  403/429 never
  observe same-target; transport/408/425/5xx observe same-target
  up to the unique L1 limit; any other
  4xx MUST end the route. Exhaustion of one credential's frozen eligible set
  writes that credential's 429 (last Retry-After) without stopping other
  credential/channel candidates; partial 429 (including partial live429 plus
  request-local skips) and pre-cooled skips never do.
   The first exact HTTP 400 on any candidate is a corrective / policy-removal
  action, not a retry, and follows the same same-target one-replay rule as
  anonymous (same route/wire session, Responses stale-ref cleanup, replay is the
  route's last recovery action; no override is stored);
  a second 400
  terminates the whole route. Cancel/deadline ends the route. A
  replay 2xx pins the session+model when still unbound; once pinned, the
  binding follows the unified bound walk per the Session affinity spine (429 /
  403 / L1-final transport-startup-408-425-5xx walk, 401 credential-failed
  without same-credential resend, not 429-only).
- Scheduler state: proxy health is transport connectivity only (HTTP statuses
  never change it; bounded temporary transport-suspect filtering is a
  separate short-lived candidate filter, never health); a single foreground transport error never cools
  credential/target/proxy429/channel state and never flips proxy healthy directly —
  it only triggers the existing async neutral proxy health verification,
  and only that independent probe on explicit isProxyFailure may flip
  healthy. 401 cools the channel+credential globally; 429 cools the
  channel-qualified `(channel,pool,proxy)` proxy globally across models,
  credentials, channels, and client sessions (anonymous and authenticated share
  the Zen channel-scoped entry as appropriate; same raw URL in different pools
  stays isolated), so one proxy's rate limit filters every model/credential
  using that channel+pool+proxy; two distinct proxies 429ing with the same
  channel+credential in one request additionally cool that channel+credential
  using the last Retry-After only after every currently eligible proxy for
  that credential has returned live 429 in the same request, while a partial
  429 never does (a single eligible proxy exhausting alone still writes it;
  pre-cooled skips never count as live evidence, request-local proxy429
  skips never count as live evidence for credential429); 403/5xx
  cool the single (channel, credential, pool, proxy, model) target,
  so one model's 403/5xx never affects another, and only with comparative success
  (same channel+credential succeeding on another node) cool the
  channel-qualified `(channel,pool,proxy)` channel — a lone 403/5xx without
  comparative success is display-only; 408/425 are transient neutral
  (retryable, never cooling); ordinary 4xx (including exact 400) is neutral
  and   2xx clears this target and this credential's 401 state, plus the
  channel-qualified proxy429/channel and credential429 state only when the send
  started at or after the latest recorded failure (stale in-flight 2xx never
  clears a newer cooldown; newer failures stay authoritative). 429 config is
  a single base/cooldown seconds value default 300, minimum 300, no fixed
  business upper bound; the effective internal backoff ceiling is at least
  the configured base (no second configurable max field).
  `retry.max_attempts` is the unique L1 same-target observation limit (including
  the first send), not a uniform per-status error-count quota.
  Proxy429/channel use deterministic exponential backoff with Retry-After
  max/cap, no same-target retry, bounded maps with stale prune/eviction
  proportional to proxy resources, and still-future migration by
  channel+pool+proxy with aggregate counts only. Route-session overrides
  are bounded in-memory Gateway authority (first generation stateless,
  the store keeps only legacy/migrated overrides with idle TTL and
  deterministic eviction and no live path creates a new override; never persisted,
  never projected, never logged), in contrast to session+model pins which are
  process-lifetime, never expire/evict, and are fixed-bounded fail-closed.
  Restart clears all in-memory cooldowns/overrides/pins. Model/capability refresh is stateless and
  touches none of these layers. The admin probe is one of the explicit
  transport-health actions: it may flip proxy healthy and nothing else (never
  clears/sets proxy429); manual refresh shares the scheduled stateless path
  and its concurrency gate, so it never reads or writes proxy or foreground
  scheduler state.
- Availability management: `POST /api/availability/check` (proxy scope: all
  nodes, anonymous and authenticated lanes) sends real minimal
  inference (minimal messages, ~1 token max output) per lane,
  never `GET /v1/models` as an availability verdict. The native anonymous
  lane uses the same agent-shaped stream path as inference and collapses the
  stream internally for a factual non-stream observation. `POST
  /api/availability/check-credentials` covers all configured authenticated
  credentials only (no anonymous, no custom); `POST
  /api/availability/check-customs` covers every configured custom channel
  exactly once including inactive ones (no native nodes or credentials).
  All three share the bulk rate limit and the single-flight gate (second
  operation gets 409). Availability rows are
  latest real minimal-inference observations: the exact HTTP status is kept
  where present; only 200 is success; the authenticated probe is
  `unconfigured` and sends nothing without keys. UI/resources MUST NOT infer a
  generic availability verdict from scheduler cooldown state. Configured custom
  channels appear before probing as untested. Probe models are
  directory-driven (anonymous: free + Zen-servable; authenticated: actually
  served; never hardcoded IDs); a lane without a directory model reports
  `no_model`/`inconclusive` without fake success. Active nodes only, inherits
  admin auth/CSRF/Origin checks, `no-store`, strict `{}`, rate/concurrency/send
  caps, partial-result reporting, and redaction. Zen public probes the
  Zen-channel nodes and never affects configured credentials; a real-credential
  401 cools that channel+credential; success clears channel-qualified
  proxy/channel state (stale-fenced) while success+429 writes proxy429 and
  success+403/5xx writes channel state; two 429s without success write
  credential429 while a single 429 stays display-only; ambiguous outcomes
  (transport-inconclusive, 408/425, ordinary 4xx, parse/empty, timeouts) stay
  display-only. Per-send/admin timeouts are diagnostic only. The single-proxy
  probe stays distinct (one proxy, transport connectivity target). Both update
  health only through the existing independent last-healthy protection. All
  configured custom fallback channels are probed in the same response with
  their configured model plus channel protocol (chat → `/v1/chat/completions`,
  responses → `/v1/responses`, via the unified API-root rule);
   custom results never write Zen scheduler or transport health and never record
   inference metrics/history, and custom availability rows keep the exact
   `http_status` where an HTTP response exists (same factual-observation rule
   as proxy lanes). The scoped customs batch probes the channels
  under the same per-channel rule while the proxy batch never probes custom
  channels. Model discovery may still use `GET {root}/v1/models`
  (host, `/v1`, either full inference endpoint all normalize to `/v1/models`)
  but MUST never be called an availability verdict; discovery failures keep code
  `discover_failed` with safe `reason` (`dns_error`/`connect_refused`/`timeout`/
  `tls_error`/`transport_error`/`non_2xx`/`invalid_json`/`empty_list`),
  `endpoint`, `http_status`, `elapsed_ms`, never body/key/Authorization/raw error.
- Custom session fallback: strict `fallback` config (`active` + unique-ID
  `channels` with stable `id` (strict 1-64 `[A-Za-z0-9_.-]`, unique machine
  identity)/free-form display `name` (unique display text, spaces allowed)/
  `base_url` http-https/`api_key`/`model`/`protocol`
  (`chat`=Chat Completions default, `responses`=Responses; empty normalizes to
  `chat`, other values strictly rejected) plus optional per-channel
  `reasoning_effort` (`""`=supplier default (strip target strength),
  `"inherit"`=preserve converted strength, `low`/`medium`/`high`; empty
  normalizes to `""`, other values strictly rejected; never in the binding
  identity, never migrates as identity, never probed); active empty or referencing an
  existing channel ID (legacy display-name active normalizes to the ID);
  legacy name-only channels derive their ID deterministically; new WebUI
  channels receive a generated stable ID (never the display name)) persists via config authority/RuntimeManager.Apply
   with masked GET, authenticated-session reveal (POST-only non-GET behind admin
  session auth + CSRF + Origin, no-store response, and full-chain
  redaction without plaintext logging). When `active` names a channel, it is
  the final fallback for eligible native-route exhaustion: unbound (new) requests use per-domain real-request state-agnostic object-unavailable exhaustion per b2c4798 (each domain Entered && Frozen>0 && Unavailable>=Frozen, Unavailable=live 429/401/403/L1-final 408/425/5xx/transport incl. L1-final stream startup per frozen candidate, 400/ordinary 4xx/cancel/deadline/committed/build excluded, state-agnostic, not requiring terminal 429; 401 same-credential skips count by credential cause, request-local proxy429 skips count by this-request live-429 evidence with Entered, request-local suspect skips count by this-request live transport evidence with Entered, none counts as live429 for credential429), while established (pinned) bindings reach the active custom channel after valid-binding object-unavailable exhaustion with no prior-switch/consumed prerequisite (including single-proxy L1-final and stable credential-401); a valid binding whose actual-pool proxies are all filtered by current health/cooldowns may zero-send to the active custom channel (including pre-cooled 429) with no fabricated live evidence/attempts/`credential429`/metrics; exact-400 replay (and its second-400/ordinary 4xx outcome), ordinary 4xx, cancel/deadline, and committed-stream never qualify, and non-429 takeovers never write credential429; unbound and pinned do not decide by single error kind directly and their independent exhaustion proofs differ; other non-429 terminals remain faithful without custom even when 429s were seen earlier on other candidates.
  Without an
  active channel the original envelope is kept (native 429 stays 429, other statuses faithful); with one, the current request retries at
  once through the then-active custom OpenAI-compatible channel (`{root}/v1/chat/completions`
  for chat, `{root}/v1/responses` for responses via the unified API-root rule,
  Bearer key, client entry converted to the channel protocol via the strict bridge
  with model rewritten to the channel model plus the channel reasoning_effort
  control (`inherit` preserves converted strength, `""` strips target strength
  (chat removes top-level strength fields, responses removes the target
  reasoning control; history/content untouched), explicit low/medium/high
  override (chat sets `reasoning_effort`, responses merges/creates
  `reasoning:{effort}`)), response/stream transcoded back;
  never listed in public `/v1/models`; sends canonical pseudonymous OpenCode routing metadata (target-bound custom route session via the shared wire helpers, never raw session/secrets, full history still sent, never relying on supplier-persisted state); never touches
  Zen scheduler layers; the first native→custom crossing strips
  provider-bound Responses refs (`previous_response_id` and input reasoning
  items) while bound follow-ups preserve custom-issued refs)
  and the session binds first to that full channel identity (stable `id` +
  normalized base URL/authority + key fingerprint/identity + configured model +
  protocol; display `name` is snapshot-only and a rename preserves the binding) so later
  requests for the session serve exclusively there without drifting on active
  switches. Custom errors return as-is with no fallback to native or other custom
  channels. Takeover state is session-keyed (not session+model), process-lifetime,
  unpersisted, unprojected, unexpired, bounded 4096, first-wins, capacity-502
  without claiming unrecorded takeover; hot Apply migrates without validity
  filtering as tombstones and a deleted/identity-mismatched (including protocol
  change) channel fails locally with 502. Restart clears. Per-channel
  observability keys use the stable ID (`custom:<id>`); operator copy shows the
  display name. The fallback API-key editor is a non-password text input with
  visual concealment and `autocomplete="off"` (never a password submission);
  the real admin login keeps `username`/`current-password` semantics.
  Custom takeover 并发契约：以 session 级 first-wins 收敛；在请求入口（`doUpstreamTiers`）、未绑定建立环路起点（`doUnboundEstablishment` loop-start）和 pin claim 后首次 native 发送前做有限重查；不提供 fallback/pin store 跨 store 原子线性化；已经发出的 native POST 不撤销，后绑定只影响尚未发送或后续请求；`claim.done` waiter 唤醒后回到入口重查。此为并发边界，不是广义统一恢复完成，也不改变现有 pin 的 claim/waiter 语义。
- Upstream OpenCode client identity: at the upstream boundary the Gateway IS the OpenCode client for every upstream outbound HTTP (native anonymous/authenticated inference, custom fallback sends, availability probes, model/capability discovery). All such egress MUST follow one OpenCode client standard through a single shared, centralized, target-aware request-construction authority. Generation/selection of `User-Agent`, `x-opencode-client`, canonical pseudonymous `x-opencode-session` / `request` / `project` / `parent`, and target-protocol auth headers MUST live in that authority only; call sites MUST NEVER hand-write, copy, trim, or maintain channel/call-site-specific header subsets. Semantically necessary per-category header differences (e.g. inference carries target-bound route-session metadata while pure discovery is sessionless) MUST be explicit request-category policies inside the shared authority, NEVER independent construction logic; this requires one standard plus category policy, NOT literally identical headers on every request. Every session header MUST use the §3 canonical pseudonymous/target-bound mapping; raw client sessions, credentials, or secrets MUST NEVER be passed through. Custom fallback sends full history without relying on supplier-persisted state and MUST still emit the same OpenCode wire routing metadata; `no affinity` MUST NEVER justify omitting it. Any new upstream egress or protocol MUST extend/reuse the shared authority with validation; new functions MUST NEVER directly `Set` the managed headers above.
- Health readiness: healthz keeps all existing fields and adds additive
  routing readiness (global credential availability plus assigned-pool health;
  per-model target cooldowns, proxy429 cooldowns, channel cooldowns, and credential429
  cooldowns never count). Zero
  globally available channels degrades readiness; one model's targets all
  cooling or one proxy's 429 cooling never does.
- Streaming and budgets: once bytes have been written to the client, the Gateway MUST
  NOT switch upstreams or regenerate; error-class upstream stream signals
  MUST surface as structured target-protocol error events, never as clean
  finish or silent truncation. `retry.attempt_timeout_seconds` keeps its strict
  bounds/default and covers only each native send/observation startup (response
  headers through first deliverable SSE event); local startup expiry while the
  parent lives is a `stream_startup_timeout` target cooldown under the bounded
  same-target L1 with existing delays. `retry.timeout_seconds` is the unchanged
  final budget: the client-stream startup deadline is revoked on a deliverable
  event while a non-stream full-request deadline is NOT revoked (including the
  anonymous internal agent-shaped SSE gated before a successful outcome);
  successful native/custom SSE tails live beyond the attempt timer. Caller
  cancel, final deadline, and committed bytes stop absolutely with no fabricated
  cooldown from cancellation. No guaranteed full exhaustion or fallback applies
  once the ultimate budget runs out first.
- Capability gating: models with unknown capability MUST NOT be exposed;
  model IDs MUST NOT be hardcoded — exposure is driven by the capability
  directory (manual `models.protocols` covers experiments only).
- Identity hygiene: `proxy_node` names a proxy node, NEVER the egress IP.
  `proxy_pool` names the owning pool; resource identities are pool-qualified.
  Real keys render as last-5-characters (or `anonymous`); config-secret
  fingerprints stay SHA-256 internal.
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
- WebUI copy is a durable operator-facing contract: user-visible
  labels/captions/statuses/tooltips/toasts MUST express concise
  operator-facing meaning/action; they MUST NOT expose implementation
  instructions, API paths, scheduler tuple/write rules, probe payload
  mechanics, raw backend enum names, or agent/task requirements. Backend
  enums/statuses MUST be deliberately mapped to that operator wording; when
  availability is factual, the UI MUST preserve the actual HTTP/status
  observation rather than inventing a UI availability verdict.
- Container posture MUST be preserved: non-root user, read-only filesystem,
  `no-new-privileges` (see `Dockerfile` / `compose.yaml`).
- Go edits MUST remain `gofmt`-clean. There is no repo lint, typecheck, or
  coverage gate — NEVER invent one or claim one ran.
- Compatibility MUST be capability-directory driven; NEVER hardcode model IDs
  to add or gate a model.
- Config schema changes MUST keep strict unknown-field rejection and the
  save/Apply transactional semantics (validate-first, build-new-runtime,
  atomic switch, old-instance-on-failure). Numeric ranges reject values
  beyond technical representability (Go duration/storage or integer limits)
  with no hidden clamping; the log ring keeps its resident-memory protection
  bound. New listeners or planes MUST state
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
  request/model/channel/status/latency/attempts/key-tail where applicable);
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
  `go build -o opencode2api ./cmd/opencode2api` is the supported build check.
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
- Source of truth for behavior: `cmd/opencode2api/main.go`, `internal/app/app.go`, `internal/app/gateway.go`, `internal/app/scheduler.go`, `internal/app/availability.go`, `internal/app/convert.go`, `internal/app/stream.go`,
  `internal/app/models.go`, `internal/app/model_metadata.go`, `internal/app/pool.go`, `internal/app/runtime.go`, `internal/app/config.go`,
  `internal/app/admin.go`, `internal/app/observability.go`, `internal/app/password.go`, `internal/app/ids.go`,
  `internal/app/fallback.go`, `internal/app/availability_probe.go`, `internal/app/admin_fallback.go`
  (read them; this guide states relationships, not code locations).
- No `CLAUDE.md` exists in this repo and none SHOULD be created; tool-specific
  entries, if ever needed, MUST be pointers to or synchronized copies of this
  file, never independently maintained contracts.
