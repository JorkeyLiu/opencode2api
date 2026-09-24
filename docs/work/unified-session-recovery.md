# Unified session recovery — live work projection

> Live projection, not history. History lives in git log and `docs/adr/0001`. This route can change as reality teaches us; provisional notes never override binding behavior. Current anchor: HEAD `bcbfb8e`.

## Outcome

- User-observable result: steady-state fluctuation is absorbed, a stable cause gets a context-appropriate resolution, and the session either continues the same session or faithfully returns in the target protocol envelope.

## Reality

- Behaviorally implemented: single-`retry.max_attempts` bounded same-target L1 observation; exact-400 corrective same-target replay that is route-terminal; cancellation/deadline/committed-byte stop boundaries; unbound custom eligibility by independent per-domain object-unavailable exhaustion; pinned full live-429 traversal plus bounded consumption; pinned transport/move/session rules and the 503 no-L2-observation policy. Exact semantics live in `AGENTS.md` §3–4 and `docs/adr/0001`, not here.
- HEAD `bcbfb8e` changed none of the above: it centralized unbound post-L1 lane decisions into the pure `decideUnboundPostL1` seam (consumed by unbound anon/auth walkers) plus its unit test with cancellation-priority rows and 408/425 both-domain custom-takeover end-to-end evidence. Structural centralization plus tests only — not observable recovery-semantic progress.

## Distance

- Accepted principle — cause + context → object action — is the basic idea to develop through production work, not a complete matrix to design first. Broad cause+context → L2 object action is not fully operationalized; no complete status matrix or behavior change is claimed.
- No AGENTS-specified implementation mismatch in unbound or pinned paths is currently known; the gap statement itself is not claimed complete.

## Route / next move

- Best route: develop L2 decisions alongside real gateway behavior — not a full status matrix up front, and not another helper-only centralization. `bcbfb8e` does not close the semantic distance above.
- Next move: decide one concrete cause+context → action at the production path, implement it, observe whether the session continues/faithfully returns under pin/session constraints, and revise the working rule from that evidence. No prior full ADR/design adjudication is required; incomplete notes and ordinary reversible decisions resolve along the route, not as blockers.
- If no first cause can be responsibly fixed from current evidence (no named user failure scenario, no known normative mismatch), let the next concrete runtime/code observation select it — e.g. which stable cause most often reaches L3 without a principled L2 resolution in live unbound/pinned traffic — and carry that decision into production development rather than waiting or declaring no next move.

## Constraints

- Binding: `AGENTS.md` §3 (recovery closed loop, pin/move, session affinity) and §4 (400-terminal, 429/401/403/5xx/channel, streaming, custom takeover, identity hygiene) plus `docs/adr/0001`. This file never duplicates their matrices; on conflict they win and provisional wording yields.

## Verification

- Tests prove the chosen behavior, not progress by itself. Gate is `go test ./...`; supported build is `go build -o opencode2api ./cmd/opencode2api`.
