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
  proxies + proxyfile, key pools with cooldowns, connection pools, model
  catalog snapshot) is in-memory runtime state built from config. NEVER treat
  it as editable directly; change it only by changing config (or the seed/env
  inputs that produce config) and letting the runtime rebuild.
- Proxy identity is two-level: `proxy_pools` names stable pool identities and
  `proxy_routing` assigns exactly one pool each to the anonymous, zen, and go
  channels (same or different). Top-level `proxies` / `proxyfile` are
  load-time legacy inputs only: they migrate to a `shared` pool on load and
  never persist. Pool names are operator identities, never IPs or URLs.
- External authorities MUST NOT be duplicated into this guide or hardcoded:
  - Upstream `/v1/models` (Zen and Go) for model existence per tier.
  - The OpenCode capability directory (`models.opencode.ai`) for each model's
    native protocol and unsupported set.
  - `models.dev` for cost/deprecation (zero input+output cost, or
    case-insensitive `free` in the model ID, qualifies for anonymous routing).
- Projections (never authoritative): in-memory request/token/upstream metrics,
  recent attempts, Playground results, stdout JSON logs, the memory log ring,
  and the on-disk model/metadata caches (`<config>.models.catalog.json`,
  `models.dev.json` compat cache). They reflect or accelerate authority; they
  MUST NOT be edited to change behavior.
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
  session hash. Affinity binds key/proxy; node failure falls back without
  breaking in-flight streams.
- Config change spine (save / Apply / reload-from-disk): parse and validate
  the full candidate → build new pools and Gateway instance → atomically write
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
  24h with fixed timeout. Refresh failure MUST keep the previous snapshot;
  startup uses valid disk cache before the first live refresh.

## 4. Invariants (MUST Preserve)

- Anonymous channel: fixed Zen credential (`Bearer public` for OpenAI-family
  upstream, `x-api-key: public` for Anthropic upstream); free models try it
  first, non-free models skip it entirely. It walks every currently available
  proxy in its assigned pool exactly once and is NEVER truncated by
  `retry.max_attempts`. Any error
  (transport, 4xx, 5xx, non-2xx) advances to the next proxy; only proxy
  exhaustion enters the authenticated tiers.
- Authenticated tiers: each tier owns its own `retry.max_attempts` budget
  (first attempt included). Inside a tier, only network errors, 401/403, 429,
  and 5xx rotate nodes; any other 4xx MUST end that tier. A failed tier falls
  back to the other tier that actually serves the model and has keys,
  ordered by `prefer` (`go` default: Go → Zen).
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
  MUST carry `no-store` (or `no-cache, no-store`); full secret values require
  re-authentication with the admin password, and onboarding examples MUST use
  the `YOUR_API_KEY` placeholder.
- Logging: stdout is single-line JSON (time, level, component, event, plus
  request/model/tier/status/latency/attempts/key-tail where applicable);
  established requests log a routing event at the right level. The memory ring
  (`logging.ring_size`, 100–50000) backs the WebUI live tail only. NEVER add
  body/key/credential fields to either sink.
- Metrics/usage semantics: usage is recorded only when the upstream reports
  it (plain, same-protocol SSE, and cross-protocol SSE alike) — NEVER
  estimate. Lifetime is process-start scoped; last-hour is 60 one-minute
  buckets; bounded retention (recent requests/attempts capped, admin caps
  response size). Restart clearing in-memory history is expected behavior.

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
- Source of truth for behavior: `gateway.go`, `convert.go`, `stream.go`,
  `models.go`, `model_metadata.go`, `pool.go`, `runtime.go`, `config.go`,
  `admin.go`, `observability.go`, `password.go`, `ids.go`, `main.go`
  (read them; this guide states relationships, not code locations).
- No `CLAUDE.md` exists in this repo and none SHOULD be created; tool-specific
  entries, if ever needed, MUST be pointers to or synchronized copies of this
  file, never independently maintained contracts.
