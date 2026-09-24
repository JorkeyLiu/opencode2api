# Unified session recovery — live work projection

> Live projection, not history. History lives in git log and `docs/adr/0001`. This route can change as reality teaches us; provisional notes never override binding behavior.

## 结构完成结论（Structural completion）

四 walker（`doPinnedAnonymous` / `doPinnedAuth` / `doAnonymousUpstream` / `doKeyUpstream`）单候选恢复已真实替换为共享权威：`initial send → 有限 L1 observation → exact400 一次同目标 corrective replay 终态` 的 single-candidate runner（`recoverSingleCandidate`）+ typed decision（`decideCandidateRecovery`，`cause + lane context -> typed action`）。旧重复 initial/post-L1 决策链已删除，无并行旧权威（含 `decideUnboundPostL1` / `pinnedAuthContextEarlyReturn` 移除）。pinned-auth Final-authority 修复已纳入。frozen 候选、request-local fences、scheduler 证据写入、pin bind/move fencing、route-session/body/attempt metadata、response ownership、custom 门仍归 walker 所有；共享 authority 不建立新状态生命周期。

## 1) 已验证结构交付

- 单候选控制流单一权威落地，行为保持；全 suite、`go build`、gofmt 通过（见本次提交终检）。

## 2) 仍未落地的方向（ADR pending）

- 广义对象选择/策略方向仍未落地，保持 ADR pending：逐状态码广义矩阵、预算数值与退避未冻结。跨 pool / 跨 credential 等受 pin 约束禁止的动作不是既定目标，不得冒充为既定目标。

## 3) 后续整体统一恢复的前提

- 如继续整体统一恢复，必须先对照 ADR 闭环（`observe stability -> resolve stable cause -> continue session or faithfully return`）确定尚未被共享决策承接的实际生产责任/结果差距，再按完整责任边界迁移。
- 不能用逐 cause 无限循环或 helper-only 增量当路线，也不能无证据声称所有语义完成；当前证据无法确定的下一动作不编造成已确定。

## Constraints

- Binding: `AGENTS.md` §3 (recovery closed loop, pin/move, session affinity) and §4 (400-terminal, 429/401/403/5xx/channel, streaming, custom takeover, identity hygiene) plus `docs/adr/0001`. This file never duplicates their matrices; on conflict they win and provisional wording yields.

## Verification

- Tests prove the chosen behavior, not progress by itself. Gate is `go test ./...`; supported build is `go build -o opencode2api ./cmd/opencode2api`.
