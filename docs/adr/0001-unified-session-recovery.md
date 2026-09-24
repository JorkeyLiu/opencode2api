# ADR 0001: 统一会话恢复（Unified Session Recovery）

- **状态：** 原则已接受 / 分层实现待验证（Principle accepted / Layered implementation pending validation）
- **日期：** 2026-09-22
- **作者：** opencode2api maintainers
- **关联范围：** `gateway.go` / `scheduler.go` / `fallback.go` / `stream.go` / `availability*.go` / 会话亲和与路由层

---

## 1. 背景（Context）

当前网关存在多套重叠的重试/回退路径：匿名与已认证通道的 429 处理、同目标瞬态重试、精确 400 重放、代理内回退、自定义备用通道（fallback）接管。其语义分散在 `retry.max_attempts`、通道内预算、代理 429 链、以及与 HTTP 状态强绑定的触发条件中，导致：

- 重试预算语义不唯一，出现并行预算解释；
- 备用路由触发条件与单一 HTTP 错误（尤其是 429）强耦合，而非与恢复域内可用性耗尽对齐；
- 400 重放的终端性与恢复流程的衔接不清晰；
- 取消 / deadline / 已向客户端提交字节后的继续重试边界需明确。

需要一个稳定的会话级恢复模型，统一上述路径的分层与归属，同时为后续实现保留足够的待定空间，不提前锁定逐状态码矩阵。

## 2. 决策（Decision）

确立**会话恢复（Session Recovery）**为本项目的最高行为原则（统一语义三层/闭环投影：`observe stability -> resolve stable cause -> continue session or faithfully return`），已接受原则如下：

- **目的：** 一切恢复都是为了让会话继续；网关是会话恢复系统，不是 HTTP 状态码驱动的重试器。
- **语义三层/闭环投影（非固定线性对象队列）：** 单次请求的恢复按语义分层投影，非按 proxy/pool/channel/domain 的固定对象层级升级。代理、代理池、凭证、备用渠道、恢复域都只是不同粒度的对象/候选，不是 L2/L3 的固定层级；`fallback` 只是 L2 对象选择而非固定“最后一级”，“恢复域”只是对象/候选组织与可用性过滤的上下文，不是比对象更高的恢复层级：
  - **L1 / Observe stability — 观察稳定性：** 先判断当前状态；本身已稳定则不重试；未稳定则用受约束的多次请求观察至稳定或停止。`retry` 属于这一层的观察手段，不是错误码/对象切换队列，从不立即重发、从不无限。
  - **L2 / Resolve stable cause — 解决已确认的稳定原因：** 按 `cause + current object + 当前上下文` 选择最小针对性动作：非法请求清理/修正（同目标 corrective replay，始终 before any client bytes，完成后按 L3 终止/忠实返回，不是 L1 retry）；代理节点不可用时换节点；对象耗尽时选择下一个可用对象；503 不设独立的 L2 持续观察/重试循环（稳定性观察仅属 L1 并受其既有边界约束）；L1-final 503 后 L2 仅按既有对象选择/耗尽规则解决，无法解决则进入忠实返回。备用渠道等不同粒度对象均属此层候选决策。
  - **L3 / Continue session or faithfully return — 继续会话或忠实返回：** 解决成功则继续同一会话（保持必要的 pin/route-session/协议身份约束），解决失败或达到停止边界则按目标协议忠实返回。
- **恢复域定义（上下文而非层级）：** 已绑定会话为该 session+model 绑定的 credential+pool 可用代理集；未绑定会话为本次冻结的 eligible targets；耗尽是健康/冷却过滤后没有可发送对象。恢复域是对象/候选组织与可用性过滤的上下文，不产生比对象更高的恢复层级。
- **无单状态码自动资格：** 任何单个状态码都不自动等同于 fallback 资格；逐状态码在 L1/L2/L3 语义投影下的映射、预算数值、退避细节仍由后续实现决定，本 ADR 不将其完整矩阵写成已完成，MUST NOT 冻结逐状态码矩阵、预算或退避。

已接受的分层与不变式如下：

1. **定性：** 恢复是会话恢复系统，而非 HTTP 状态码驱动的重试系统。HTTP 状态仅作为输入信号，恢复判决以会话与当前恢复域的可用性为中心；分层为语义投影（observe/resolve/continue-or-return），非对象队列投影。

2. **语义三层/闭环投影（Semantic Three Layers — observe/resolve/continue-or-return）：** 统一三层为语义归属，非固定线性对象升级：
   - **L1 Observe stability — 观察稳定性：** 见上，`retry` 是此层观察手段。
   - **L2 Resolve stable cause — 解决已确认的稳定原因：** 见上，所有对象粒度决策在此层；代理/代理池/凭证/备用渠道仅为不同粒度的候选对象，恢复域为上下文。
   - **L3 Continue session or faithfully return — 继续会话或忠实返回：** 见上，fallback 属于 L2，不作为独立层级。

3. **备用路由资格（Fallback eligibility — L2 对象选择）：** `fallback` 是 L2 中一种对象选择/解决动作，不是固定“最后一级”L3。触发条件为当前对象已稳定不可用且经恢复域可用性过滤后无可发送对象；而非单一 HTTP 状态码机械触发。不能因单个错误码直切 fallback；必须有当前恢复域的可用对象耗尽证据。

4. **精确 400 重放的终态定位（Corrective replay, not retry — L2 动作按 L3 终态）：** HTTP 400 属于非法请求、非波动态。精确 HTTP 400 的一次同目标重放（exact 400 replay）是对 `reasoning`/`previous_response_id` 等策略引用清理后的“解除策略/请求修正动作”，属于 L2 的稳定非法请求解决动作，不是 L1 retry；必须保持同目标、同 route session、同请求身份（same target / same route session / same request ID）约束，始终在向客户端提交任何字节之前执行。重放结果无论成功、二次 400 或其他结果，都是整条路由的最后恢复动作（route-terminal），按 L3 终止/忠实返回，不再继续候选、通道或 fallback 恢复，不进入进一步评估。当前代码与 `AGENTS.md` §4 的 400 终态基线是正确的，以此为准。

5. **停止条件与忠实返回（Stop conditions & Faithful return — L3 边界）：** 客户端取消（cancellation）、请求 deadline 超时、已向客户端提交字节（committed bytes / streaming started）后，立即停止恢复（L3 停止边界），不再切换上游或重新生成；稳定性观察仅属 L1（受单一 `retry.max_attempts` 计数、interval/Retry-After、deadline/cancel、提交前边界约束），L1-final 503 后仅按既有对象选择/耗尽规则解决，无法解决时按 L3 忠实返回。

6. **最大重试语义（稳定性观察计数 — L1 观察层）：** `max retry` / `retry.max_attempts` 的规范语义是 L1 观察层判定稳定性所需的“最小请求次数/观察次数”（minimum observation/request count to judge stability），不是对所有错误统一发放的重试额度或对象队列配额（not a uniform error-count quota）；具体状态在统一语义三层下的映射与是否计入该观察预算，仍由既有/后续实现定义，本 ADR 不将其完整矩阵写成已决定。该语义已**规范化迁移至单一权威定义（canonical migration，已落地单一 L1 计数）**：`retry.transient_max_attempts` 已删除（严格 unknown-field 拒绝，无值迁移），`retry.max_attempts` 为唯一 L1 同目标观察上限（含首次发送），认证普通发送预算已删除（不再截断 L1 与候选遍历，遍历由冻结切片自然有界），模型刷新改用独立全遍历；避免被误读为并行预算或按错误数额度的配额；现有分散预算已收敛到 L1 稳定性观察，但不预先冻结数值与逐状态退避，不写成 L1/L2/L3 固定队列。本次仅完成单一 L1 计数收敛，逐状态矩阵、广义 fallback 与退避数值仍未冻结。

7. **领域归属不变：** `scheduler` / `pin`（会话绑定）/ `route-session`（路由会话）保持现有领域归属（domain owners），统一恢复不改变其所有权与职责边界。

> **实现细节待定标记：** 除上述已接受方向外，逐 HTTP 状态码（429/401/403/5xx/408/425/普通 4xx 等）在统一语义三层（L1 观察 / L2 解决 / L3 继续或返回）下的具体映射、精确计数与退避参数，属于实现待验证细节，本 ADR 不作最终规定，需由后续实现与验证确定，不冻结矩阵与数值。

## 3. 范围与边界（Scope / Boundaries）

**在范围内（In scope）：**

- 统一会话恢复的语义三层/闭环投影与闭环条件（observe/resolve/continue-or-return，非固定线性对象队列）；
- 网关对瞬态波动的吸收职责（L1 观察稳定性）；
- 备用路由作为 L2 对象选择（耗尽后下一可用对象）的定位，非固定 L3；
- 400 重放的终态定位（L2 稳定非法请求解决动作，完成后按 L3 终止/忠实返回，不是 L1 retry）；
- 停止信号与 L3 忠实返回的一致处理（cancel/deadline/committed bytes 边界）；
- 重试预算向 L1 观察层单一权威的规范化方向。

**不在范围内（Out of scope）：**

- 逐状态码的最终行为矩阵（per-status matrix）—— 显式待定；
- 具体的退避算法、冷却时长、并发与预算数值；
- 模型能力目录、成本与匿名路由资格的判定；
- 管理面（Admin/WebUI）与可观测性投影的细节变更。

**边界约束：**

- 不改变双通道（anonymous / authenticated）与 Zen-only 上游的既有产品定位；
- 不改变网关作为 OpenCode 客户端在上游边界的身份与请求构造权威；
- 不引入新的持久化状态或跨重启的恢复状态。

## 4. 保留不变式（Preserved Invariants）

本决策不削弱以下既有不变式（以 `AGENTS.md` §4 为准）：

- 匿名通道固定凭证与免费模型优先语义；已建立会话绑定后的无跨域回退约束；
- 流式提交后不切换上游、不重新生成；错误以目标协议信封呈现；
- 能力门控由能力目录驱动，不硬编码模型 ID；
- 调度器分层（proxy 健康仅表示连通性、proxy429/channel/credential/target 冷却分离等）与迁移语义；
- 会话亲和与 pin/route-session 的生成与绑定规则；
- 配置严格校验与保存/应用的事务语义（validate-first, build-new-runtime, atomic switch）；
- 日志与指标的脱敏与投影边界（不记录请求体/密钥）。

## 5. 已拒绝的替代方案（Alternatives Rejected）

- **HTTP 状态码驱动的重试系统：** 以状态码为中心串联重试逻辑。拒绝原因：与会话可用性解耦，易将单一错误直接等同于回退资格，违背“基于可用目标耗尽可能进入备份路由”的方向。
- **并行预算（Parallel budgets per channel/layer）：** 各通道/层各自持有独立 max_attempts 预算。拒绝原因：语义分裂，需收敛为单一权威预算以保证可解释性与可验证性。
- **备用路由作为特定错误（尤其 429）的直接分支：** 见到 429 即切 fallback。拒绝原因：与“耗尽判定”原则冲突，忽略当前域内仍有可用代理的事实，且违背 fallback 为 L2 对象选择（需耗尽证据）而非单状态码直切 L3 的原则，与已建立绑定的域内语义不一致。
- **400 重放结果回流至正常恢复（已废弃方向）：** 重放后按统一分层继续评估候选/通道/fallback。拒绝原因：HTTP 400 为非法请求、非波动态，重放是解除策略的修正动作而非 retry；当前代码与 `AGENTS.md` §4 已确立“重放后终态、不再继续恢复”的正确基线，回流会扩大恢复域、违背同目标/同会话/同身份约束并引入不必要的域移动。已接受方向为终态分支（重放结果即路由最后恢复动作）。
- **网关不吸收瞬态波动、直接进入 L2 对象选择：** 拒绝原因：放大瞬态噪声，增加不必要的域移动，与 L1 稳定性观察（受约束观察至稳定）目标相悖。

## 6. 后果（Consequences）

- **正向：** 恢复路径可解释性提升；备用路由的触发条件与会话域可用性对齐，减少误触发；预算语义澄清为 L1 观察层稳定性观察计数并向单一权威定义收敛，便于审计与测试；400 终态（L2 修正后按 L3 终止）与现有代码/`AGENTS.md` 对齐，减少歧义；明确代理/池/凭证/备用渠道均为对象粒度、恢复域为上下文，避免把 proxy/pool/channel/domain 误映射为 L2/L3。
- **负向/成本：** `retry` 与通道预算的规范化迁移已完成（明确为 L1 最小观察次数而非统一错误额度/对象队列配额；`transient_max_attempts` 删除、严格拒绝、无值迁移）；现有测试与文档需按统一语义三层（observe/resolve/continue-or-return）与 400 终态重新对齐。
- **中性：** 逐状态码在统一语义三层下的映射、具体退避与数值预算仍待定（本 ADR 不将其写成已决定），单一 L1 计数已收敛（`transient_max_attempts` 删除、`max_attempts` 为唯一 L1 上限、无候选发送预算、刷新独立全遍历），其余保持现状行为（候选序列与特定 429 fallback 基线），仅以本 ADR 的统一三层闭环、耗尽资格与 400 终态原则作为评判后续改动的依据；调度器/pin/route-session 领域边界与流式提交后停止不变式保持不变，广义逐状态矩阵、广义 fallback 与退避数值仍未冻结。

## 7. 迁移与实现约束（Migration / Implementation Constraints）

- `max retry` 已收敛至 L1 观察层“最小观察次数/稳定性观察计数”的单一权威语义（not a uniform error-count quota / 对象队列配额）：`retry.max_attempts` 为唯一 L1 同目标观察上限（含首次发送），已删除的 `retry.transient_max_attempts` 严格 unknown-field 拒绝、无值迁移；不保留多预算并行解释，也不将逐状态数值预算与退避写成已决定或冻结矩阵。
- 实现必须保持 `scheduler` / `pin` / `route-session` 的领域归属，不将冷却、绑定、会话生成职责外移或合并；流式提交后不切换上游/不重新生成的不变式保持不变；代理/池/凭证/备用渠道/恢复域仅为对象/候选上下文，不映射为固定 L2/L3。
- 400 重放的实现需保证同目标、同路由会话、同一请求身份的约束，属于 L2 稳定非法请求解决动作，且其结果为整条路由的最后恢复动作（route-terminal，按 L3 终止/忠实返回），不再继续候选/通道/fallback 评估；不得将重放结果回流至正常恢复或视为 L1 retry，不得把 400 写成 L1 行为。
- 取消 / deadline / 已提交字节的停止语义（L3 边界）需在网关入口与流式网关处一致执行，不产生新的后台重试；503 不设 L2 持续观察（稳定性观察仅属 L1），L1-final 503 后仅按既有对象选择/耗尽规则解决、否则按 L3 忠实返回，不写成无限 retry。
- 配置变更保持严格未知字段拒绝与原子切换语义；新增恢复相关配置需显式说明是否热生效或需重启（参考 `AGENTS.md` §3）。
- 不引入新依赖（`golang.org/x/crypto` 以外需显式理由），保持 `gofmt` 清洁与容器姿态。
- custom session takeover 并发边界：以 session 级 first-wins 收敛；在请求入口（`doUpstreamTiers`）、未绑定建立环路起点（`doUnboundEstablishment` loop-start）和 pin claim 后首次 native 发送前做有限重查；不提供 fallback/pin store 跨 store 原子线性化；已经发出的 native POST 不撤销，后绑定只影响尚未发送或后续请求；`claim.done` waiter 唤醒后回到入口重查。此为并发边界，不是广义统一恢复完成，也不改变现有 pin 的 claim/waiter 语义。

## 8. 验证要求（Validation Required）

以下为验证要求分项现状（锚点说明：下述“已执行”覆盖截至 HEAD `488f3c9`（当时工作区干净）的已实现切片验证，以及 `488f3c9..HEAD 2463f1c`（`419d5ac` native pin+stream cancel、`4958720` custom cancel、`2463f1c` gateway 取消资格门 + 两 unbound auth 用例，均已提交）生产增量（含本文档/ADR 同步更新）的静态输出审查与 `go test ./... -count=1`/gofmt/build/diff --check 验证，另加 HEAD `2463f1c` 之上本增量（随本提交合入：`gateway.go` 共享纯未绑定 post-L1 决策接缝，行为保持 + 新 `gateway_unbound_decision_test.go` 决策测试 + `unbound_exhaustion_stateagnostic_test.go` 表驱动 `TestUnboundExhaustion408425BothDomainsAllowCustom` 测试证据；其中 408/425 的聚焦/包级/`go test ./... -count=1`/gofmt/`git diff --check`/临时构建按委托记录通过，本轮新增接缝聚焦 `TestDecideUnboundPostL1`/gofmt/`git diff --check`已执行、全量与构建随本提交前终检执行见终检记录；广义矩阵外不假称为已整体审查）。其中锚点内已实现切片——单一 L1 观察、400 route-terminal、unbound 逐域状态无关耗尽、pinned 全量 live429 + 有界 consumption、cancel/deadline/committed、503 无独立 L2 循环、retry 旧字段拒绝——对应的验证已执行且通过（见各项“已执行”标注）；广义 cause→action 矩阵/退避/广义 fallback 仍未决定，对应验证保持 pending，不宣称完整统一三层完成：

- `go test ./... -count=1` 通过（CI 门禁）——已执行（现有验证，全量回归通过；见工作文档 §8）。
- `go build -o opencode2api ./cmd/opencode2api` 构建通过——已执行（现有验证）。
- L1 受约束观察至稳定的测试（`TestObserveSameTargetTransientStops` 覆盖 stable/context/observation-limit、`TestAuthL1ObservationAndTraversal`、`TestPinnedAuthObservationLimitTransport`、`TestRefreshTraversesAllCandidatesIndependentOfL1`）——已执行。
- L2 当前双门/503 的测试：unbound 逐域状态无关耗尽（`TestUnboundExhaustion503BothDomainsAllowCustom` / `TestUnboundExhaustionAuth429And503AllowCustom` / `TestUnboundExhaustion503PartialOrdinaryNoCustom` / `TestUnboundExhaustion503NoActivePreserves503` / `TestUnboundDomainsExhaustedAllowCustom` 系列）与 pinned 429-only 加有界 consumption（`TestPinnedConsumptionAuth429ThenL1Final408Takeover` / `TestPinnedConsumptionAuth429ThenL1Final425Takeover` / `TestPinnedConsumptionAuth429ThenStartupTakeover` / `TestPinnedConsumptionResponses429Then403Takeover` / `TestPinnedAuthFull429StillCustom`）；503 仅 L1 同目标有界观察、无 L2 持续观察循环（`TestUnboundAnonymous503RetrySuccessAfterMultipleFailures` / `TestPinned503RemainsSameTarget` / `TestPinned5xxRetryOnly`）——已执行。
- 400 同目标修正重放后终态的路径测试（L2 修正动作完成后按 L3 终止/忠实返回，重放后不进入正常恢复/不触发后续候选、通道或 fallback，重放结果即路由最终结果，不是 L1 retry；`TestAnonymous400ReplaysSameTarget` / `TestAuthZen400ReplaySuccess` / `TestAuth400RecoveryRouteTerminal` / `TestCustomTakeover400ReplayFinalNoCustom` / `TestMaybeReplayCandidate400Entry` / `TestClassifyStableCause`）——已执行。
- 取消 / deadline / 已提交字节后停止恢复的测试（L3 边界；`TestCancelStopsRetryFallback` / `CommittedStream_*` 路由层回归 / `TestPinnedConsumptionAuthDeadlineNoCustom` / `TestPinnedConsumptionAuthCancelDuringSecondProxyNoCustom`）——已执行。
- 预算收敛的拒绝语义测试（`retry.max_attempts` 为唯一 L1 最小观察计数，含首次发送，非统一错误额度/对象队列额度；已删除 `transient_max_attempts` 严格 unknown-field 拒绝、无值迁移；`TestTransientMaxAttemptsStrictlyRejected` / `TestObserveSameTargetTransientStops` 计数语义）——已执行。
- 未绑定 408/425 双域 L1-final 耗尽接管的测试证据（同属本增量，随本提交合入，仅测试证据，无生产语义变更：`TestUnboundExhaustion408425BothDomainsAllowCustom` 表驱动 408/425，anon/auth 两域均进入、各单冻结候选、`MaxAttempts=3` 各 3 次同目标 L1 观察后按域耗尽→active custom 接管一次并绑定 session、不建 pin，`attempts=7`；408/425 调度中性：不写 proxy429/channel/target/credential401/credential429；不声称广义矩阵完成）——已执行（聚焦测试、包级测试、`go test ./... -count=1`、gofmt 干净、`git diff --check`、仓库外临时构建按委托记录通过，本文件内不重跑、不代验）。
- 未绑定 post-L1 共享纯决策接缝与取消优先测试证据（本增量，随本提交合入，行为保持，无原则变更：`gateway.go` `decideUnboundPostL1` 为匿名/已认证 walker 共用的 `cause + lane context -> action` 纯函数——lane 区分匿名 `Cause==context||Cancelled` 与已认证 `Cancelled` 门，其余 build/success/exact400/ordinary/live429/L1-final 映射为当前行为，不选响应、不排空、不触碰调度/pin/custom/attempt 状态，发送/L1/重放执行/调度写入/credential429 证据/pin/body/custom 聚合仍归既有调用方；`gateway_unbound_decision_test.go` `TestDecideUnboundPostL1` 覆盖 exact400+Cancelled/ordinary+Cancelled/L1Final+Cancelled → return-context 双 lane、Success+Cancelled → return-success、Build+Cancelled → return-build 及既有 lane/mark 行；无生产语义变更，不冻结广义 cause→L2 矩阵/fallback/退避，不声称三层已完全落地）——已执行（本轮聚焦 `TestDecideUnboundPostL1` 通过、gofmt 干净、`git diff --check` 通过；全量回归与构建随本提交前终检执行，见终检记录）。
- 未引入新的未脱敏日志/指标输出的人工审查（历史区间 `b2c4798..488f3c9` 内生产 Go 变更仅 `internal/app/gateway.go`，无新增 logger/metrics/admin/history 输出，未见新的未脱敏路径；该结论仅适用于该旧区间，不得扩张到之后变更）——已执行（区间内）。`488f3c9..HEAD 2463f1c`（`419d5ac` 的 `scheduler.go`+`gateway.go` 取消边界、`4958720` 的 `fallback.go`+`gateway.go` custom 绑定、`2463f1c` 的 `gateway.go` 一行取消资格门，均已提交）生产增量（含两 unbound auth 用例与文档同步）的全部生产 Go diff（`fallback.go`/`gateway.go`/`scheduler.go`）静态人工审查已执行：新增 logger/metrics/admin/history/body/key/session/proxy 输出 sink 0 个，流式 `logTargetCooldownSet` 为旧调用移入 cancel guard 内，monitor record 口径不变；限此范围未见新增未脱敏输出，不声称全库安全审计。本轮 `go test ./... -count=1`/gofmt/build/diff --check 已执行且通过。
- 广义 cause→action 矩阵 / 退避 / 广义 fallback 与对象策略的验证——pending（对应行为仍未决定，不宣称完成）。

> 历史记录：本文档创建时（2026-09-22）上述验证尚未执行；现行状态见上——当前已实现切片的对应验证已执行且通过，广义矩阵/对象策略验证仍 pending。

## 9. 修订与过期条件（Revision / Expiry）

- **修订触发：** 当实现验证对分层、耗尽可能、或 400 终态语义提出与本 ADR 冲突的证据时，或 `AGENTS.md` 中调度/亲和/可用性不变式发生变更时，必须修订本 ADR。
- **过期条件：** 若自 2026-09-22 起 6 个月内未进入实现验证，或上游协议/产品定位发生根本变化导致会话恢复模型不再适用，则重新评估本决策是否继续有效。
- **版本记录：** 后续修订需在文末追加修订历史，保留原始决策与变更理由。

---

**附注：** 本 ADR 仅记录已稳定的方向性决策，所有逐状态码行为矩阵、阈值、退避与计数器等实现细节均显式标记为待定，不得视为已接受决策；不擅自冻结所有状态码矩阵。

### 修订历史（Revision History）

- **2026-09-24 — 未绑定认证 request-local proxy429 L2 原因（已实现/已验证切片，无广义矩阵）：**
  - 未绑定认证建立内，真实发送的 live 429 即时建立本请求内 `(tier/channel,pool,proxy)` 稳定不可用原因：同身份后续冻结候选跳过无 POST（仅请求内真实 live 429 证据触发，预冷/并发调度状态不触发；身份含现有 channel/tier+pool+raw proxy，认证单 tier/pool 不弱化）；跳过按本请求证据计入该凭证域 `Unavailable` 并标记 `Entered`，不计入该凭证 live429/`credential429` 证据、不增 `attempts`、不记 upstream attempt、不写任何调度状态（`credential429` 仍仅当该凭证全部冻结 eligible 经真实发送 live 429 时写入，partial live429+skip 永不写入）；401 凭证围栏、exact400 终态、ordinary4xx、403/5xx target 范围、L1、匿名/已绑定、cancel/deadline/committed、pin/route-session、custom first-wins 均不变。
  - 验证：新增 `unbound_proxy429_scope_test.go`（跨凭证跳过同代理并在另一代理成功/pin 且 attempts 仅计实发；后凭证全跳过按域耗尽接管 active custom 且 skips 非 attempts；1 实发 429+1 skip 不写 `credential429`；池限定身份单元）；既有 `TestUnboundExhaustionCredentialDomainsNotMerged` 改为双代理夹具以在新跳过语义下保持凭证域不合并证明（单共享代理下后凭证全跳过即耗尽接管为新正确行为）；聚焦/相关 `unbound/401/400/pinned/custom/429` 回归通过、gofmt 干净；全量门禁随后续终检，不声称广义矩阵完成。
  - 语义边界：仅未绑定认证；预冷冻结排除不变；不同池/代理身份不跳过；不冻结广义逐状态矩阵/退避/广义 fallback。

- **2026-09-24 — 未绑定 401 凭证级 L2 原因（已实现/已验证切片，无广义矩阵）：**
  - 未绑定建立（匿名/认证共享冻结遍历，含空会话直达路径）将首个真实稳定 401（`err==nil && 401` 且非取消；含 L1 同目标观察后稳定 401）视为凭证级 L2 原因：同请求内不再发送同 `CredID` 剩余冻结候选，直接进入下一 distinct credential（匿名单凭证则结束剩余代理遍历并按既有外层进入认证 lane）；跳过目标按该凭证级原因计入该域 `Unavailable`，无伪发送/`attempts`/upstream attempts/调度写入（401 本体既有 `credential401` 写入保留，403/429/408/425/5xx/transport/400/ordinary/cancel/deadline/committed/pin/custom 门不变）。
  - 验证：新增 `unbound_401_credential_scope_test.go`（auth 首 401 跳过同凭证并进下一凭证成功/pin、transient→L1 稳定 401 同跳过且 attempts=2+1、匿名首 401 单发送后进认证、双域 401 跳过仍按域耗尽接管 active custom 且 attempts=实发+custom），既有 `TestServerRetryAndNoRetryClasses/Unauthorized_no_retry` 按新语义更新为 401 跳过匿名次代理并进认证（429/403 行不变），`go test ./... -count=1` 通过、gofmt 干净。
  - 语义边界：仅未绑定；已绑定 401 不移动/不跨域规则不变；预冷冻结排除不变；不冻结广义逐状态矩阵/退避/广义 fallback。

- **2026-09-24 — 未绑定 post-L1 共享纯决策接缝（未提交行为保持增量，无原则变更）：**
  - HEAD `2463f1c` 之上工作树未提交：`internal/app/gateway.go` 新增 `decideUnboundPostL1`（`cause + lane context -> action` 纯函数，匿名/已认证 walker 消费，行为保持；发送/L1/重放执行/调度写入/credential429 证据/pin/body/custom 聚合仍归既有调用方） + 新 `internal/app/gateway_unbound_decision_test.go`（`TestDecideUnboundPostL1` 含取消优先行：exact400/ordinary/L1Final+Cancelled → return-context 双 lane、Success+Cancelled → return-success、Build+Cancelled → return-build） + 408/425 双域测试证据（同属本增量，见下条）。
  - 语义边界：纯决策接缝，不选响应、不排空、不触碰状态；不冻结广义 cause→L2 矩阵/fallback/退避，不声称三层已完全落地。
  - §8 新增对应“已执行”验证证据行（本轮聚焦/gofmt/`git diff --check` 通过，全量与构建本轮未执行）；最高原则、503 关闭口径、400 终态与已锁定 pins 不变；广义逐状态矩阵、广义 fallback 与退避仍未冻结、验证保持 pending。

- **2026-09-24 — 未绑定 408/425 双域测试追加（同属当前未提交增量，仅测试证据，无原则/生产语义变更）：**
  - HEAD `2463f1c` 之上工作树未提交：`internal/app/unbound_exhaustion_stateagnostic_test.go` 表驱动 `TestUnboundExhaustion408425BothDomainsAllowCustom`（408/425 各一子用例），无生产 Go 变更，不 commit。
  - 语义边界：anon/auth 两域均进入、各单冻结候选、`MaxAttempts=3` 各 3 次同目标 L1 观察后按域耗尽→active custom 接管一次并绑定 session、不建 pin；408/425 调度中性（不写 proxy429/channel/target/credential401/credential429）；不声称生产行为变更或广义 cause→L2/fallback/backoff 矩阵完成。
  - §8 新增对应“已执行”验证证据行（聚焦/包级/`go test ./... -count=1`/gofmt/`git diff --check`/临时构建按委托记录通过）；最高原则、503 关闭口径、400 终态与已锁定 pins 不变；广义逐状态矩阵、广义 fallback 与退避仍未冻结、验证保持 pending。

- **2026-09-24 — `2463f1c` 取消后不写 credential cooldown 已提交（文档事实更新，无原则变更）：**
  - HEAD `2463f1c`（`fix(recovery): suppress credential cooldown after cancellation`）已提交，工作树干净；含 `gateway.go` 一行（`doKeyUpstream` 全耗尽 `noteCredential429Failure` 前资格改走 `Cancelled: isContextCancelled(ctx)`，取消中 live 429 不写 `credential429`）与 `unbound_auth_started_test.go` 两用例（取消不写 / no-cancel 对照写）及文档同步更新；此前“HEAD `4958720` + 未提交”表述已 superseded。
  - 最高原则、503 关闭口径、400 终态与已锁定 pins 不变；广义 cause→L2 映射、广义 fallback 与退避仍未冻结，不声称三层已完全落地。

- **2026-09-24 — §8 验证锚点收束到区间覆盖（文档事实更新，无原则变更）：**
  - §8“已执行”明确仅覆盖截至 `488f3c9` 的已实现切片验证；当前 HEAD `4958720`（`419d5ac`/`4958720` 生产增量）与工作树未提交改动不在该锚点覆盖内，不假称为已整体审查。
  - “生产 Go 变更仅 `gateway.go`”限定为历史区间 `b2c4798..488f3c9`，不得扩张；之后增量与未提交改动的新增输出路径核验保持 pending。
  - 最高原则、503 关闭口径、400 终态与已锁定 pins 不变；广义逐状态矩阵、广义 fallback 与退避仍未冻结。

- **2026-09-23 — §8 验证要求分项现状收束（文档事实更新，无原则变更）：**
  - §8 改为分项现状：当前已实现切片（单一 L1 观察、400 route-terminal、unbound 逐域状态无关耗尽、pinned 全量 live429 + 有界 consumption、cancel/deadline/committed、503 无独立 L2 循环、retry 旧字段拒绝）对应的 `go test ./... -count=1`、构建、L1/L2 双门与 503/400/L3/预算直接测试覆盖、人工审查（`b2c4798..488f3c9` 生产 Go 仅 `gateway.go`、无新 logger/metrics/admin/history 输出）已执行且通过；广义 cause→action 矩阵/退避/广义 fallback 验证保持 pending。
  - 此前修订条中“§8 验证要求保持待执行，不标记完成”为当时历史快照，现已被本条覆盖，不再为现状；正文不再保留“均待执行”表述。最高原则、503 关闭口径、400 终态与已锁定 pins 不变。

- **2026-09-23 — L1 后稳定原因分类接缝收敛（实现事实，无原则变更）：**
  - `gateway.go` 内 L1 观察之后收敛为单一分类权威 `classifyStableCause`（仅七种：context / build / 2xx / exact400 / live429 / ordinary4xx / 其余 L1-final；late-cancel 不折入原因，`doPinnedAuth` 保留旧门原样）；四条 native walker 消费分类，各路径 L2 对象决策与 scheduler / pin / route-session / custom 门保留，行为等价。
  - 本次为行为保持的结构收敛，不更改已接受的统一语义三层原则，不冻结新的逐状态码矩阵、预算或退避；广义 cause→L2 action / object selection 映射仍待定。

- **2026-09-23 — 关闭 503 观察策略问题（Nexus 裁决：无 L2 持续观察）：**
  - 无独立的 HTTP-503 L2 持续观察/重试循环：稳定性观察仅属 L1，受单一规范 `retry.max_attempts` 计数（含首次发送）、既有 interval/Retry-After 延迟、请求 deadline、取消与提交前边界约束；新增 post-L1 503 观察循环将重复 L1、形成并行/隐藏观察预算，与已接受的单一 L1 权威矛盾，故不设。
  - L1-final 503 后，L2 仅按既有对象选择/耗尽规则解决稳定原因：未绑定建立沿用当前冻结目标遍历与分域耗尽规则；已绑定会话保持 5xx 不移动不变式，仅已满足条件的既有有界 consumption/custom 规则可适用，否则按 L3 忠实返回目标协议 503；单个 503 不直接构成 fallback 资格。
  - 本次仅关闭 503 观察策略问题；广义逐状态码矩阵与统一三层完整实现仍待定，不冻结无关退避/预算细节；正文 §2/§5/§7/§8 中的 L2 503 持续观察表述已同步修正为上述关闭口径（历史修订条保留原样，以下条目为准）；§8 验证要求保持待执行，不标记完成。

- **2026-09-23 — 当前 fallback 事实状态澄清（无规范变更，仅消除现状误读）：**
  - §6“候选序列与特定 429 fallback 基线”与 2026-09-22 修订条“当前代码仍为历史候选/429 fallback 基线”均为当时快照，现已由 `b2c4798`（未绑定按域状态无关耗尽：每域独立 `Entered && Frozen>0 && Unavailable>=Frozen`，允许集 live 429 / 401 / 403 / L1-final 408-425-5xx-transport 含 L1-final stream startup，400 / ordinary 4xx / cancel-deadline / committed / build 不计，状态无关）与 `1e4536c`（已绑定保留全量 live 429 并新增有界 consumption 门：已实际切换到下一冻结 eligible、全部冻结 eligible 真实尝试、末位对象不可用；预冷零发送保持原生；单代理首发非 429 / 400 重放 / ordinary 4xx / cancel-deadline / committed 不接管）取代；`977790a` / `1a5904f` / `8a43803` 为纯测试提交，无生产语义变更。
  - 503 当前实现仅为同目标有界 L1 观察，已绑定 5xx 不移动；本 ADR 所述 L2 持续观察为已接受原则方向，未另行规定/实现，不视为已落地。§8 验证要求保持待执行，不标记完成。

- **2026-09-23 — 收敛单一 L1 观察计数（最终迁移）：**
  - 删除 `retry.transient_max_attempts`（配置/管理面/运行时 helper/持久化兼容全部移除，严格 unknown-field 拒绝，无值迁移）；`retry.max_attempts` 为唯一 L1 同目标观察上限（含首次发送）；认证 `ordinarySends`/`budgetOK`/候选发送预算全部删除，候选遍历由冻结切片自然有界；`observeSameTargetTransient` 唯一停止为 stable/context/observation-limit；模型刷新改用冻结 `keys × healthy proxies` 全遍历，不再复用推理 L1 值。
  - 本次仅完成单一 L1 计数与观察预算收敛；逐状态矩阵、广义 fallback 与退避数值仍未冻结，不声称三层已完全落地。
- **2026-09-22 — 修正 400 语义并统一 retry/fallback 定义（维护者裁决）：**
  - 将 §2.4 从“400 重放结果进入正常恢复流程（feed into normal recovery）”修正为“400 属于非法请求、非波动态；重放是解除策略/请求修正动作（not retry），必须同目标/同会话/同身份且始终在提交字节前，重放后即为整条路由最后恢复动作（route-terminal），不进入 L1/L2/L3 进一步评估”；以当前代码与 `AGENTS.md` §4 的终态基线为准。
  - 同步修正 §5（拒绝“400 回流至正常恢复”方向，改为拒绝其并接受终态）、§6（预算语义澄清为稳定性观察计数、保留矩阵待定、保持领域边界）、§7（400 约束改为终态、`max retry` 明确为最小观察次数且不预先冻结数值）、§8（400 验证改为终态路径测试）及附注措辞。
  - 明确 `retry` 为稳定性观察、仅返回稳定结果，`max retry` 为最小观察次数而非统一错误额度，`fallback` 需当前恢复域可用对象耗尽证据且对象层级随域变化（代理耗尽可能进下一代理，池/域耗尽可能进备用渠道），不因单个错误码机械直切；不改变 scheduler/pin/route-session 领域归属与流式提交后停止不变式。
  - 旧方向“400 回流至正常恢复”仅作为历史修订记录中的被废弃方向保留，正文不再表述为现行规范。
- **2026-09-22 — 确立“会话恢复”为最高行为原则并对齐低熵闭环（维护者裁决）：**
  - 明确最高原则闭环为 `observe stability -> resolve stable cause -> continue session or faithfully return`；网关是会话恢复系统而非 HTTP 状态码驱动的重试器，一切恢复为让会话继续；先判断稳定性（稳态不重试、未稳态受约束多次观察），稳态后按错误原因/当前对象/恢复域取最小针对性解决动作（非法请求同目标修正、节点不可用域内换节点、域耗尽进下一对象/备用渠道、503 受 deadline/取消/已提交字节/观察策略约束的持续稳定性观察，非无限请求）。
  - 补充对象/恢复域定义：已绑定 = 同 session+model 绑定的 credential+pool 可用代理集，未绑定 = 本次冻结 eligible targets，耗尽 = 健康/冷却过滤后无可发送对象；单状态码不自动获得 fallback 资格，备用路由仅在当前恢复域可用目标耗尽后作为下一可用目标。
  - 明确无法解决时按目标协议忠实返回；保留 400 同目标修正重放 route-terminal 定位与既有代码/`AGENTS.md` §4 基线；逐状态码 L1/L2/L3 矩阵、预算数值、退避细节仍显式待定，不视为已完成。
   - 状态由“已接受方向 / 实现待验证”调整为“原则已接受 / 分层实现待验证”。
- **2026-09-22 — 将三层从对象队列投影修正为 observe/resolve/continue-or-return 语义投影（维护者裁决）：**
  - 将 §2 的“有序分层（Ordered Layers）：L1 同目标→ L2 域内代理→ L3 备用路由”对象队列描述修正为统一语义三层/闭环投影：L1 / Observe stability（观察稳定性，先判断已稳定则不重试，未稳定受约束多次请求观察，retry 属此层）、L2 / Resolve stable cause（按 cause+current object+上下文选最小针对性动作：非法请求同目标修正、节点不可用换节点、对象耗尽选下一可用对象、503 受 deadline/cancel/committed bytes/观察策略约束持续观察，无法解决则忠实返回）、L3 / Continue session or faithfully return（成功继续会话保持 pin/route-session 约束，失败或止边界按目标协议忠实返回）；代理/代理池/凭证/备用渠道/恢复域均为不同粒度的对象/候选，不是 L2/L3 的固定层级；fallback 为 L2 对象选择/解决动作，不是固定 L3；恢复域为对象/候选组织与可用性过滤的上下文，不是比对象更高的恢复层级，不能把 proxy/pool/channel/domain 映射成 L2/L3；400 重放属 L2 完成后按 L3 终止，不是 L1 retry。
  - 同步修正 §2 备用路由资格、§3 范围、§5 拒绝项、§6 后果、§7 迁移约束、§8 验证要求中的 L1/L2/L3 引用与 400/503/停止条件/max retry 表述，避免把 fallback 定为 L3、避免冻结逐状态码矩阵/预算/退避、不把 503 写成无限 retry、不改变当前 400 终态与 scheduler/pin/route-session/流式提交后停止等实现不变式。
  - 当前代码仍为历史候选/429 fallback 基线，三层统一模型尚未完全落地，本次仅修正文档投影，不声称 Go 代码已实现；`retry.max_attempts` 明确为 L1 观察层最小观察计数，不写成对象队列配额。
