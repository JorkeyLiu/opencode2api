# Unified session recovery — live work projection

> Live projection, not history. History lives in git log and `docs/adr/0001`. This route can change as reality teaches us; provisional notes never override binding behavior.

## 结构完成结论（Structural completion）

四 walker（`doPinnedAnonymous` / `doPinnedAuth` / `doAnonymousUpstream` / `doKeyUpstream`）单候选恢复已真实替换为共享权威：`initial send → 有限 L1 observation → exact400 一次同目标 corrective replay 终态` 的 single-candidate runner（`recoverSingleCandidate`）+ typed decision（`decideCandidateRecovery`，`cause + lane context -> typed action`）。旧重复 initial/post-L1 决策链已删除，无并行旧权威（含 `decideUnboundPostL1` / `pinnedAuthContextEarlyReturn` 移除）。pinned-auth Final-authority 修复已纳入。frozen 候选、request-local fences、scheduler 证据写入、pin bind/move fencing、route-session/body/attempt metadata、response ownership、custom 门仍归 walker 所有；共享 authority 不建立新状态生命周期。

## 1) 已验证结构交付

- 单候选控制流单一权威落地，行为保持；全 suite、`go build`、gofmt 通过（见本次提交终检）。
- 单候选结构交付已完成收口：post-L1 context/replay 资格已收归统一纯决策（typed action 决定能否进入 exact400 replay，无并行 context 门）；pinned custom 出口、耗尽证明归属、状态所有权均未动。
- L1 最小观察延迟已落地（窄实现，非广义矩阵）：共享 `transientDelay` 对零/负/低于下限输入取内部命名下限 100ms，再与配置正间隔与解析后 `Retry-After` 取 max；显式 `transient_retry_interval_seconds=0` 仍合法并持久化 0（缺失默认 3），运行时永不立即重发；`sleepWithContext`、exact400 重放延迟、计数/lane/调度/pin/响应归属/遍历均未动。
- 发送前取消检查已落地（窄实现，非广义矩阵）：共享 runner（`recoverSingleCandidate`）入口先判断已取消 context，未发送即返回 typed `recoveryReturnContext`（`ctx.Err` + `transientStopContext`，无 `Started`/初始响应伪造、无 unavailable 证据，不增 attempts、不 sync meta、不 replay）；L1 `observeSameTargetTransient` 在 sleep 成功后、increment/exec 前复查 `ctx.Err`，覆盖 timer/context 同时就绪；通用 `sleepWithContext` 未动，无全局钩子；exact400 语义、四 lane 决策、committed stream、scheduler/pin/custom 门均未动；仅承诺已可观察取消前不发，不承诺与竞态 wire dispatch 的原子性。

## 2) 仍未落地的方向（ADR pending）

- 广义对象选择/策略方向仍未落地，保持 ADR pending：逐状态码广义矩阵、预算数值与退避未冻结。跨 pool / 跨 credential 等受 pin 约束禁止的动作不是既定目标，不得冒充为既定目标。本次 L1 下限与发送前取消检查均为已接受的窄实现，不声称广义退避/矩阵完成。
- 残余未决（未编造下一 helper 任务）：广义逐状态策略/矩阵仍未决，不从 pending 自动派生实现任务。

## 3) 后续整体统一恢复的前提

- 如继续整体统一恢复，必须先对照 ADR 闭环（`observe stability -> resolve stable cause -> continue session or faithfully return`）确定尚未被共享决策承接的实际生产责任/结果差距，再按完整责任边界迁移。
- 不能用逐 cause 无限循环或 helper-only 增量当路线，也不能无证据声称所有语义完成；当前证据无法确定的下一动作不编造成已确定。
- 不同耗尽证明（unbound 逐域状态无关耗尽、pinned 有界 consumption）与 scheduler/pin/route-session 状态所有权仍归 walker 与既有域所有者，不是第二决策权威；未决策略（广义逐状态矩阵/退避/广义 fallback）仍未决，不能从 pending 自动派生实现任务；本收口不声称整个 ADR 目标全部完成。

## Constraints

- Binding: `AGENTS.md` §3 (recovery closed loop, pin/move, session affinity) and §4 (400-terminal, 429/401/403/5xx/channel, streaming, custom takeover, identity hygiene) plus `docs/adr/0001`. This file never duplicates their matrices; on conflict they win and provisional wording yields.

## Verification

- Tests prove the chosen behavior, not progress by itself. Gate is `go test ./...`; supported build is `go build -o opencode2api ./cmd/opencode2api`.
