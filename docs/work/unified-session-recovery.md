# Unified session recovery — live work projection

> Live projection, not history. History lives in git log and `docs/adr/0001`. This route can change as reality teaches us; provisional notes never override binding behavior.

## Outcome

- User-observable result: steady-state fluctuation is absorbed, a stable cause gets a context-appropriate resolution, and the session either continues the same session or faithfully returns in the target protocol envelope.

## Reality

- Behaviorally implemented: single-`retry.max_attempts` bounded same-target L1 observation; exact-400 corrective same-target replay that is route-terminal; cancellation/deadline/committed-byte stop boundaries; unbound custom eligibility by independent per-domain object-unavailable exhaustion; unbound 401 credential-scoped L2 cause (first real stable 401 skips remaining same-credential frozen targets, skips count as unavailable without sends, incl. L1-final 401); unbound auth request-local proxy429 L2 cause (live 429 via real send establishes `(tier/channel,pool,proxy)` unavailable for this request, later same-identity frozen candidates skip without POST, skips count as unavailable+entered without attempts/writes and never as live429 for credential429); unbound auth request-local transport-suspect L2 cause (qualifying L1-final true transport establishes `(channel,pool,proxy)` for this request, later same-identity skips only while suspect stays active, skips count as unavailable+entered without attempts/writes and never as live429/credential429; 408/425/5xx/stream-startup/429/cancel never establish it); 403 target-scoped stable cause in the pure decision seam (initial 403 and transient-after final 403 classify as explicit `stableCauseTargetForbidden`, unbound decision stays advance+mark, scheduler/pinned/exhaustion semantics unchanged); pinned full live-429 traversal plus bounded consumption; pinned transport/move/session rules and the 503 no-L2-observation policy. Exact semantics live in `AGENTS.md` §3–4 and `docs/adr/0001`, not here.
- Current state includes the 403 pure-decision explicitation plus focused tests only — not observable progress beyond that single cause.

## Distance

- Accepted principle — cause + context → object action — is the basic idea to develop through production work, not a complete matrix to design first. Broad cause+context → L2 object action is not fully operationalized; 403 is now explicit in the pure seam but its L2 resolution reuses the existing advance+mark/exhaustion path, no complete status matrix or behavior change is claimed.
- No AGENTS-specified implementation mismatch in unbound or pinned paths is currently known beyond the closed 403 explicitation; the gap statement itself is not claimed complete.

## Route / next move

- Best route: develop L2 decisions alongside real gateway behavior — not a full status matrix up front, and not another helper-only centralization. Structural centralization alone does not close the semantic distance above.
- Next move: decide one concrete cause+context → action at the production path, implement it, observe whether the session continues/faithfully returns under pin/session constraints, and revise the working rule from that evidence. No prior full ADR/design adjudication is required; incomplete notes and ordinary reversible decisions resolve along the route, not as blockers. The 403 seam is closed under this rule; the next cause (if any) is selected the same way.
- If no first cause can be responsibly fixed from current evidence (no named user failure scenario, no known normative mismatch), let the next concrete runtime/code observation select it — e.g. which stable cause most often reaches L3 without a principled L2 resolution in live unbound/pinned traffic — and carry that decision into production development rather than waiting or declaring no next move.

## Constraints

- Binding: `AGENTS.md` §3 (recovery closed loop, pin/move, session affinity) and §4 (400-terminal, 429/401/403/5xx/channel, streaming, custom takeover, identity hygiene) plus `docs/adr/0001`. This file never duplicates their matrices; on conflict they win and provisional wording yields.

## Verification

- Tests prove the chosen behavior, not progress by itself. Gate is `go test ./...`; supported build is `go build -o opencode2api ./cmd/opencode2api`.
