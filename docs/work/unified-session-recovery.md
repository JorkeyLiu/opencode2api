# 统一会话恢复工作文档

> 持久化工作记录 · 面向运维/开发者 · 仅含当前决策相关状态
> 不复制完整状态矩阵 / 代码清单 / AGENTS.md / README

## 0. 元信息

- 文件：`docs/work/unified-session-recovery.md`
- 目标：统一会话恢复（unified session recovery）
- 基准：当前代码即基线（`jorkey/integration` 最新提交 `566d7b7`），统一恢复尚未实现
- 检验时间：2026-09-22 现场检查

## 1. 结论（Outcome）

- 统一会话恢复的最高原则已于 2026-09-22 按维护者裁决固化为统一语义三层/闭环投影：`observe stability -> resolve stable cause -> continue session or faithfully return`（L1 观察稳定性含 retry、L2 解决稳定原因含 400 修正与所有对象粒度 fallback、L3 继续或返回；代理/池/凭证/备用渠道/恢复域均为对象/候选粒度，非 L2/L3 固定层级；fallback 为 L2 对象选择非固定末级；恢复域为候选组织与可用性过滤上下文），网关是会话恢复系统而非 HTTP 状态码重试器，文档层面已对齐 `AGENTS.md` / ADR；本次裁决已关闭“L2/L3 对象层级 / 固定线性升级 / 对象层级是恢复层级”等疑问。
- 代码实现仅部分落地：Increment 1-4 的结构收敛已闭合，400 同目标修正后终态（L2 动作按 L3 终止）、503 受约束同目标稳定性观察（L2 内受 deadline/cancel/committed bytes/观察策略约束非无限）、取消/deadline/已提交字节后停止均已有基线；custom fallback 仍为 429 耗尽限定（未扩展至广义恢复域可用对象耗尽的 L2 对象选择），广义统一语义三层逐状态码映射仍未实现（`retry.max_attempts` 已收敛为唯一 L1 观察计数）；现有代码/测试为候选序列、特定 429 fallback 基线加单一 L1 计数，不应反推为三层已完全实现。
- 本文档为唯一持久化工作记录载体，原则固化不代表代码已完成广义统一恢复；任何新增 `internal/app/recovery.go`（历史路径 `recovery.go`）并行路径仍属废弃方案；广义三层逐状态矩阵、广义 fallback 与退避数值仍未冻结，不把 503 写成无限 retry；单一 L1 计数已收敛。

## 2. 已提交基线（Committed Baseline）

- 代码基线：工作区现有 Go 代码即权威基线，未引入新模块/新依赖。
- 配置权威：`config.json` + 热生效/重启生效划分保持不变；`AGENTS.md` §3/§4 约束继续有效。
- 构建门槛：`go test ./...` 与 `go build` 为唯一门槛，未新增门槛。

## 3. 现场核验事实（Verified Facts）

- 入口路径：`internal/app/gateway.go:doUpstream` 固定同目标单次重放，注释明确“不重排候选顺序、不重写 client session”。
- 重放实现：`internal/app/gateway.go:replayCandidate400` 保持同一 `routeSession`/同一 `credential/proxy/protocol/requestID`，Responses 场景清理 `previous_response_id` 与 reasoning 输入项；重放结果为路由终态，不再遍历剩余候选或跨 tier。
- 会话标识：`internal/app/scheduler.go` 中 `routeSessionFor` 按 `routeSessionScope` 代理无关推导（`rss_*` 内部态，线上传输为规范 OpenCode 伪名映射）；当前无统一恢复产生的覆盖写入。
- 调度分层：六层状态（代理健康 / proxy429 / channel / credential 401 / credential429 / target）行为与既有注释一致，未引入跨层统一恢复写入。
- 文件检查：仓库不存在 `internal/app/recovery.go`（历史路径 `recovery.go`），`docs/work/` 此前不存在（本次新建）；`docs/adr/` 已承载决策（ADR 0001/0002，不再为空）。

## 4. 已锁定决策（Locked Decisions）

- 基线不变：统一恢复实现前，所有路由/重试/冷却语义以现有代码为准，不以文档描述替代实现。
- 单文件演进：不新增并行 `internal/app/recovery.go`（历史路径 `recovery.go`）；如需统一恢复，在现有 `internal/app/gateway.go`/`internal/app/scheduler.go` 职责内收敛。
- 不虚报完成：未实现的统一行为不得在文档、日志文案或对外说明中表述为已完成。
- 文档边界：本文件仅保留决策相关状态，不展开全量矩阵与清单。

## 5. 需保持不变式（Preserved Invariants）

- 匿名/已认证双通道、Zen-only 上游、代理池两级身份与共享传输语义保持不变。
- 同目标 400 重放为路由最后恢复动作；重放前后 `route session` 字节一致，不创建覆盖。
- 调度六层冷却写入规则、代际围栏、会话绑定（session+model / session 级接管）与迁移语义不变。
- OpenCode 客户端身份统一构造权威不变；新增上游出口必须复用共享 authority。
- 健康就绪、流式“落字节后不切换”与能力目录驱动 gating 不变。

## 6. 当前缺口（Current Gap）

- 原则层面已固化为统一语义三层/闭环投影（L1 观察稳定性 / L2 解决稳定原因 / L3 继续或返回；retry 属 L1、400 属 L2 按 L3 终止、fallback 为 L2 对象选择非固定末级、恢复域为上下文非层级，已关闭对象队列解读）；代码实现仅部分落地，广义统一恢复仍未完成，现有代码/测试仅为候选对象序列与特定 429 fallback 基线，不应反推为三层已实现。
- 已落地基线：400 同目标修正后终态（非法请求、非波动态；重放为 L2 解除策略/修正动作而非 L1 retry，同目标/同会话/同身份，重放后按 L3 终态）与基线一致；503 已有 L2 内受约束持续观察（受 deadline/取消/已提交字节/观察策略约束，非无限，最终仍可按 L3 忠实返回）；取消/deadline/已提交字节后停止（L3 边界）不变式已有；Increment 1-4 结构收敛已闭合（`executeAttempt` 单次发送边界）。
- 未落地缺口：广义统一语义三层（L1 观察稳定性 / L2 解决稳定原因含全部对象粒度 fallback / L3 继续或返回）的逐状态码（429/401/403/5xx/408/425/普通 4xx 等）完整映射、`retry.max_attempts` 已收敛为唯一 L1 观察上限（含首次发送），剩余逐状态矩阵、广义 fallback 与退避仍待后续工作；`fallback` 作为 L2 对象选择的耗尽资格当前仅实现 429 耗尽限定，广义恢复域可用对象耗尽的 L2 选择仍未实现；代理/池/凭证/备用渠道均为对象/候选，恢复域为上下文，不映射为固定 L2/L3。
- 首个有界重构缺口已闭合（Bounded Increment 1）：`internal/app/gateway.go` 已建立结构化单次发送执行器 `executeAttempt` 并已将 `doPinnedAnonymous` 迁移至该边界（行为等价、无恢复语义变更），为后续统一恢复提供了单一收敛边界。
- 第二个有界重构缺口已闭合（Bounded Increment 2）：`internal/app/gateway.go` 内 `attemptOutcome` 已新增 `Started int64`，`executeAttempt` 已对普通与重试发送均返回该 `Started` 并以同一值完成流式成功/启动失败的调度与监控记录，`doPinnedAuth` 两处直连 `sendUpstreamOnce` 块已迁移至该边界（行为等价、无恢复语义变更）；更广义的统一会话恢复仍未落地，当前仍以同目标单次 400 重放为基线。
- 第三个有界重构缺口已闭合（Bounded Increment 3）：`internal/app/gateway.go` 内 `doAnonymousUpstream` 两处直连 `sendUpstreamOnce` + 内联流式门控块已迁移至既有 `executeAttempt` 边界（`TierZen`/`g.cfg.Upstream.Zen`/`route.Protocol`/`candBody`/`ids`/`cand`/`routeSession` + `channel=anonymous`/`credDisplay=anonymous`/`anonymous=true`/`attemptOffset+attempts`），行为等价、无恢复语义变更；更广义的统一会话恢复仍未落地，当前仍以同目标单次 400 重放为基线。
- 第四个有界重构缺口已闭合（Bounded Increment 4）：`internal/app/gateway.go` 内 `doKeyUpstream` 两处直连 `sendUpstreamOnce` + 内联流式门控块已迁移至既有 `executeAttempt` 边界（`route.Tier`/`g.cfg.Upstream.Zen`/`route.Protocol`/`candBody`/`ids`/`cand`/`routeSession` + `channel="key"`/`credDisplay=cand.CredDisplay`/`anonymous=false`/`attemptOffset+attempts`，`firstStarted`/`retryStarted` 取 `out.Started` 并向 `cred429Evidence`/`noteCredential429Failure` 传播以保留 `lastStartedNanos` stale fencing），行为等价、无恢复语义变更；聚焦 `Started` 回归已通过（对标 `TestPinnedAuthStartedCredentialEvidence` 覆盖 unbound 路径的 `TestUnboundAuthStartedCredentialEvidence`，双 429 `Retry-After 9`/`future cooldown`/`lastStartedNanos>1`/`nanos=1` fencing），`gofmt` 干净、`go test ./...` 已通过（聚焦与全量门槛均已执行），`go build -o opencode2api ./cmd/opencode2api` 按执行记录已通过；更广义的统一会话恢复仍未落地，当前仍以同目标单次 400 重放为基线。
- 第五个有界重构缺口已闭合（Bounded Increment 5 · 结构 L1 观察循环收敛）：`internal/app/gateway.go` 内 `transientNextAttempt` 单步机制已提升为结构化同目标观察循环 `observeSameTargetTransient`，`doPinnedAnonymous`/`doPinnedAuth`/`doAnonymousUpstream`/`doKeyUpstream` 四处内层 transient `for` 循环均已迁移至该边界（行为等价、无恢复语义变更）；循环仅拥有 L1 机制（初始 transient 起算、`isSameTargetTransient` 持续、`maxTransient` 含初始发送、调用方普通预算门、`Retry-After`/interval、context、仅排空被重试体、`attempts`/`syncAttemptMeta`/`execute` 闭包，返回 `transientLoopResult`（完整 `Final attemptOutcome` + `stable`/`context`/`observation-limit`/`ordinary-budget` 停止原因 + `InitialResp`/`InitialErr` 历史元数据供调用方精确还原），调用方保留全部策略（`BuildErr`、`2xx` 绑定/移动、`exact400` 同目标终态重放、`429` 退款/证据/`Started`/custom/候选步进、普通 4xx、transport 跨代理规则、流启动哨兵 `local502`/候选行为、调度/pin 状态）；`pinnedAuth` 普通发送先增、`doKey` 仅 `BuildErr==nil` 后增并 `429` 退款、匿名无预算、`maxTransient` 含初始、`monitorAttempt`/`Started`/`Diag` 不变、稳定返回体永不排空、流哨兵原样返回（pinned `502`、unbound 候选行为）、`pinnedAuth` 以 `ordinary-budget` vs `observation-limit` 区分 transport 跨代理（`5xx`/`408`/`425` 永不移动）均已保留；审计差异已修复（`pinnedAuth` 混合 `503->transport-nil` 在观察/预算上限返回陈旧初始 `503` 而非 `nil+transport`、`pinnedAuth` 初末任一流哨兵即 `local502`（初或末、非仅末；不采用 last-wins/去毒化）、`pinnedAuth`/`doKey` 内层预算门先于 context（匿名仍 context-only，睡眠 context 不变）、`doPinnedAnonymous` 预重试取消仅当 `cur!=initial` 排空中体且重试 transport 非空体排空后返回（稳定终态永不排空；`transport+non-nil` 经 `net/http Client` 不可达，仅防御性保留）；未新增 `internal/app/recovery.go`；最终 L1 迁移已收敛单一计数；广义逐状态映射、广义 fallback 与退避仍未冻结。
- 第六个有界重构缺口已闭合（Bounded Increment 6 · L2 custom 429-only 资格集中化 + cancel/no-state-change 边界修正）：`internal/app/gateway.go` 内新增唯一小助手 `customTakeoverEligible(customTakeoverQualification{ObservedLive429/Eligible/TerminalStatus/Recovered400/Cancelled/Committed})`（归属既有 `gateway.go`，未新增 `recovery.go`），集中既有严格 429-only 接管资格——仅当冻结 eligible 全集在本请求/路由内均提供去重 live 429 证据（预冷跳过不计入）、终态为 429、非 400 修正终态、非取消/deadline/已提交时为真；`doPinnedAuth` 两处 in-loop 与终态、`doPinnedAnonymous` 终态（新增 `live429` 计数）、`doKeyUpstream` 两处 per-credential 证据、`doUpstreamTiersUnbound` 外层终态共七处散落门已迁移至该助手（预冷本地 429 快路径、身份/`Started` 围栏/last Retry-After/容量 fail-closed/custom 错误原样/pin/调度写入均保留调用方所有）；边界修正：pinned in-loop `credential429` 写入与 custom 接管均受 cancel-gate 抑制，pinned anonymous/auth 预冷本地 429 快路径在取消时保持原生 429 本地终态（不绑定 custom、不写新状态），非取消路径行为不变；`doUpstreamTiersUnbound` 外层当时为 terminal-only recheck（1/1 简并计数），真实 eligible 耗尽证明仍在冻结 inner walks（`doAnonymousUpstream`/`doKeyUpstream`/pinned bindings），不以谓词夸大证明；广义 L2 fallback（恢复域可用对象耗尽的一般对象选择）仍未实现。
- 第七个有界增量已闭合（Bounded Increment 7 · 非绑定恢复域耗尽证据端到端有界接线）：`internal/app/gateway.go` 内新增 `unboundDomainEvidence{Domain/Entered/Frozen/Live429/Terminal/Recovered400}`（归属既有 `gateway.go`，未新增 `recovery.go`，无全局状态），仅由 `doAnonymousUpstream`（单域）与 `doKeyUpstream`（每凭证一域，永不加总）携带至 `doUpstreamTiersUnbound`；各域在真实游走边界填充（空/预冷为 `Entered=false/Frozen=0` 永不合格、去重 live 429 map、非 429 终态失效、400 重放终态 `Recovered400` 且重放 429 不计入证据、取消/deadline、流提交前返回）；唯一非绑定外层 custom 决策以 `unboundDomainsAllowCustom` 聚合既有 `customTakeoverEligible`（非第二资格权威）：仅当全部收集域均实际进入且各自独立满足 429-only 门、最终路由终态为 429、会话可绑定、非 `recovered400`/取消/已提交时接管，否则保持原生；无 active、容量/tombstone、`Retry-After`、`Started` 围栏、custom 错误原样、匿名→认证顺序、400 终态路由语义、pinned 路径/pin 身份均不变；`TestUnifiedAnonUnboundToAuthAndCustom` 第二阶段改用新鲜网关以保证本请求内真实 live 发送（复用旧网关将因前一请求预冷导致匿名空域而 truthfully 保持原生 429）；广义 fallback 与 pinned 行为未声称。
- 并发边界（custom session takeover，以 session 级 first-wins 收敛）：当前 HEAD 已在请求入口（`doUpstreamTiers`）、未绑定建立环路起点（`doUnboundEstablishment` loop-start）和 pin claim 后首次 native 发送前做有限重查（fallback takeover 为 session-keyed first-wins，fallback store 与 pin store 无跨 store 原子性）；已经发出的 native POST 不撤销，后绑定只影响尚未发送或后续请求；`claim.done` waiter 唤醒后回到入口重查。此为并发边界，不是广义统一恢复完成，也不改变现有 pin 的 claim/waiter 语义；避免把发送前有限窗口误判为缺陷，不声称全局线性化。

## 7. 下一自然工作单元（Bounded Increment 6 · 统一三层语义的最小代码单元待选择）

> 统一三层语义已固化，实现待选取最小行为单元；Increment 5（结构 L1 观察循环收敛）已闭合。下述仅为候选行为域与裁决前提，不构成执行指令，不声称统一恢复已完成。

- 现状：最高原则已于 2026-09-22 固化为统一语义三层/闭环投影 `observe stability -> resolve stable cause -> continue session or faithfully return`（L1 观察稳定性含 retry、L2 解决稳定原因含 400 修正与所有对象粒度 fallback、L3 继续或返回；代理/池/凭证/备用渠道/恢复域均为对象/候选，非固定 L2/L3；已关闭对象队列解读），文档已对齐 `AGENTS.md` / ADR；代码层面 Increment 1-5 结构收敛已闭合（Increment 5 为 `observeSameTargetTransient` 同目标观察循环收敛，四调用方已迁移，行为等价），400 同目标修正后终态（L2 按 L3 终止）、503 受约束观察（L2 内受 deadline/cancel/committed bytes/观察策略约束非无限）、取消/deadline/已提交字节后停止（L3 边界）已有基线，custom fallback 仍为 429 耗尽限定的 L2 保底，广义统一语义三层逐状态码映射仍未落地（单一 L1 计数已于最终迁移收敛）；现有代码/测试仅为候选对象序列与特定 429 fallback 基线，不应反推为三层已实现；`internal/app/gateway.go` 已无内层 transient `for` 直连迁移点，下一行为增量无可直接迁移的技术点。
- 候选行为域（待后续工作、未立项）：
  - 候选 A：`exact-400` 语义——已裁决为终态（同目标修正后即路由最终结果，不回流正常恢复，属 L2 完成后按 L3 终止），本候选不再待裁决；当前代码已符合裁决，无需新实现；
  - 候选 B：统一语义三层恢复——逐状态码（429/401/403/5xx/408/425/普通 4xx 等）在统一语义三层（L1 观察稳定性 / L2 解决稳定原因含全部对象粒度、fallback 为 L2 动作 / L3 继续或返回）下的映射、单一 L1 计数收敛（`retry.max_attempts` 为唯一观察上限含首次发送，无候选发送预算）与停止条件（含 503 在 L2 内受约束持续观察非无限、无法解决时按 L3 忠实返回）仍待定义与落地；`fallback` 广义恢复域可用对象耗尽的 L2 选择仍未实现。
- 裁决前提（未满足则不进入实现）：
  - 需先明确逐状态码在统一语义三层下的映射与优先级、单一预算（`retry.max_attempts` 规范化为 L1 最小观察次数非统一错误额度/对象队列配额）与 429 budget-neutral 的归一方式、停止条件（取消/deadline/已提交字节后不切换为 L3 边界，503 受约束观察为 L2 内非无限）的边界；`fallback` 需以当前对象稳定不可用且恢复域可用性过滤后无可发送对象的 L2 耗尽证据为准（代理/池/凭证/备用渠道均为对象/候选，恢复域为上下文，不映射为固定层级），不因单个错误码机械直切；
  - 已裁决的 400 终态语义（L2 修正按 L3 终止）与现有 `replayCandidate400` / `AGENTS.md` §4 不变式保持一致，后续工作不得将其回流至正常恢复或视为 L1 retry；
  - 已裁决的最高原则与恢复域定义（上下文非层级）保持不变，广义统一语义三层逐状态矩阵、广义 fallback 与退避仍显式待实现，不虚报完成；单一 L1 计数已收敛。
  - 由 Nexus 裁决选定唯一最小行为增量后，方可形成新的“当前自然工作单元”并进入执行；本轮不自行设计或实现上述任一行为。
- 约束重申：不新增 `internal/app/recovery.go`，不拆包，不改 `README`/`AGENTS`/`ADR`/`config.example.json` 以外文件、依赖或配置 schema；未声称统一恢复已完成；已裁决的最高原则不再作为待裁决项；未裁决前不产生新的代码行为变更；不把 503 写成无限 retry，不冻结逐状态码矩阵/预算/退避。

## 8. 验收状态（Acceptance Status · 截至最终 L1 迁移已闭合）

> Increment 1-5 为已闭合的结构化收敛增量，最终 L1 迁移为完整行为闭合（非局部 503 过渡）；本节为历史验收摘要。

- 文档：本文件已同步 Increment 1-5 闭合事实与最终 L1 迁移（`retry.transient_max_attempts` 删除、`retry.max_attempts` 为唯一 L1 同目标观察上限含首次发送、认证普通发送预算删除、候选遍历由冻结切片自然有界、`observeSameTargetTransient` 唯一停止为 stable/context/observation-limit、模型刷新独立全遍历）；400 语义已对齐（L2 修正按 L3 终态）；本次仅完成单一 L1 计数收敛，逐状态矩阵、广义 fallback 与退避数值仍未冻结，不声称三层已完全落地。
- 代码：`internal/app/config.go`、`admin.go`、`gateway.go` 已收敛；未新增 `internal/app/recovery.go`；`gofmt` 干净。
- 验证（已执行且通过）：
  - 新增/改写聚焦 `TestObserveSameTargetTransientStops`（`stable`/`context`/`observation-limit`/`BuildErr`/`429-stable`/流哨兵原样，含 `observationLimitIncludesFirstSend` 与 `observationLimitUsesNormalizedMaxAttempts`）与 `TestPinnedAuthObservationLimitTransport`（观察上限 transport 仍走冻结切片、`5xx` 永不移动）及审计回归 `TestPinnedAuthMixed503TransportStaleInitial` 与 `TestPinnedAuthSentinelPoisoning503`；`TestAuthL1ObservationAndTraversal`（L1=1 仍走全切片、L1=2 先重试后走）、`TestAuthEstablishedTransportExhaustionWalks`、`TestRefreshTraversesAllCandidatesIndependentOfL1`、`TestTransientMaxAttemptsStrictlyRejected` 已通过；`pinnedAnon` 中体取消/`transport+non-nil` 仍为防御性保留；既有 400/429/pin/stream/cancel 证据保留。
  - 全量回归 `go test ./... -count=1` 已通过，`gofmt` 干净；`go build -o opencode2api ./cmd/opencode2api` 已通过。
- 当前状态说明：最终 L1 迁移闭合后，单一 L1 计数已收敛；剩余广义逐状态矩阵、广义 fallback 与退避数值待后续裁决。

## 9. 未决问题（Unresolved Questions）

- 统一恢复是否允许跨代理或跨凭证迁移，抑或仍限定同目标同会话？（注：L2/L3 对象层级疑问已于 2026-09-22 裁决关闭——代理/池/凭证/备用渠道/恢复域均为对象/候选或上下文，不映射为固定 L2/L3；fallback 为 L2 对象选择非固定末级）
- 与 Responses `previous_response_id` 清理及自定义 fallback 接管的交互边界？（400 为 L2 修正按 L3 终态，不回流正常恢复）
- 是否需要新增可观测事件/指标，或复用现有 `route_session_recovery_*` 事件？

## 10. 已否决路径（Rejected Approach）

- 新建并行 `internal/app/recovery.go`（历史路径 `recovery.go`）承载独立恢复流程：与现有 `internal/app/gateway.go:replayCandidate400` 职责重复，易造成双路径分叉与冷却/绑定不一致，已否决。后续工作不得以该文件形式出现。

## 11. 重评估/停止规则（Reassessment / Stopping Rule）

- 当规格证明需跨通道/跨凭证状态写入或打破“重放字节一致/不创建覆盖”不变式时，暂停实现并返回 Nexus 裁决。
- 当验证显示现有 400 重放与统一恢复无法在单一路径内互斥收敛时，停止并重新评估方案，不强行合入。
- 任何偏离本文件锁定决策的实现尝试均视为需重评估事件。

## 12. 现状重评估（Current-State Reassessment · 2026-09-22）

- **源码布局重构为独立有界结构化工单元：** 仓库扁平 `package main` 拓扑 → `cmd/opencode2api` 薄入口 + `internal/app` 单包内聚（含白盒测试）+ `webui` 就近嵌入的布局调整，已由 `docs/adr/0002-go-source-layout.md` 承载为独立 ADR；其范围仅为文件物理位置与构建/CI/Docker 路径（`./cmd/opencode2api`）切换，**与本文件的统一会话恢复行为域正交**。
- **恢复行为保持不变：** 该结构重构不改变任何路由/重试/冷却/会话亲和语义，不触及 `AGENTS.md` §4 不变式；本文件 §1–§11 所锁定的“同目标单次 400 重放为终态、不创建覆盖、六层冷却与绑定语义不变”等结论继续有效。
- **文档边界：** 后续布局实现验证（`go test ./...` / `go build ./cmd/opencode2api` / `gofmt` / Docker）归 ADR 0002 追踪，不在本文件验收证据内重复判定。
