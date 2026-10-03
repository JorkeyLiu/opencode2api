# ADR 0001: 统一会话恢复（Unified Session Recovery）

- **状态：** 已接受并已落地（Accepted+Implemented）— 统一会话恢复完整闭环已实现，详细矩阵以本文与 `recovery.go`/`gateway.go`/`scheduler.go` 为准
- **日期：** 2026-09-22（2026-09-24 定版 Final）
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
- **恢复域定义（上下文而非层级；2026-10-03 current-pool 自然恢复修订，见文末修订历史）：** 已绑定会话为该 session+model 绑定的 credential + CURRENT 通道所属池可发送代理集（当前资源选择；establishment pool 仅为 wire 兼容派生来源，不入 pin 身份；旧池名移除不是 tombstone）；未绑定会话为本次冻结的 eligible targets；耗尽是健康/冷却过滤后没有可发送对象。恢复域是对象/候选组织与可用性过滤的上下文，不产生比对象更高的恢复层级。旧文“credential+pool 可用代理集/pool 入 pin 身份/pool-routable 才迁移”已废弃。
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

6. **最大重试语义（稳定性观察计数 — L1 观察层）：** `max retry` / `retry.max_attempts` 的规范语义是 L1 观察层判定稳定性所需的“最小请求次数/观察次数”（minimum observation/request count to judge stability），不是对所有错误统一发放的重试额度或对象队列配额（not a uniform error-count quota）；具体状态在统一语义三层下的映射与是否计入该观察预算，仍由既有/后续实现定义，本 ADR 不将其完整矩阵写成已决定。该语义已**规范化迁移至单一权威定义（canonical migration，已落地单一 L1 计数）**：`retry.transient_max_attempts` 已删除（严格 unknown-field 拒绝，无值迁移），`retry.max_attempts` 为唯一 L1 同目标观察上限（含首次发送），认证普通发送预算已删除（不再截断 L1 与候选遍历，遍历由冻结切片自然有界），模型刷新改用独立全遍历；避免被误读为并行预算或按错误数额度的配额；现有分散预算已收敛到 L1 稳定性观察，但不预先冻结数值与逐状态退避，不写成 L1/L2/L3 固定队列。单一 L1 计数已收敛；逐状态矩阵、广义 fallback 与退避数值仍未冻结。

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
- **中性：** 逐状态码在统一语义三层下的映射、具体退避与数值预算仍待定（本 ADR 不将其写成已决定），单一 L1 计数已收敛（`transient_max_attempts` 删除、`max_attempts` 为唯一 L1 上限、无候选发送预算、刷新独立全遍历），其余保持既有行为（以本 ADR 修订历史中的现行耗尽规则为准），仅以本 ADR 的统一三层闭环、耗尽资格与 400 终态原则作为评判后续改动的依据；调度器/pin/route-session 领域边界与流式提交后停止不变式保持不变，广义逐状态矩阵、广义 fallback 与退避数值仍未冻结。

## 7. 迁移与实现约束（Migration / Implementation Constraints）

- `max retry` 已收敛至 L1 观察层“最小观察次数/稳定性观察计数”的单一权威语义（not a uniform error-count quota / 对象队列配额）：`retry.max_attempts` 为唯一 L1 同目标观察上限（含首次发送），已删除的 `retry.transient_max_attempts` 严格 unknown-field 拒绝、无值迁移；不保留多预算并行解释，也不将逐状态数值预算与退避写成已决定或冻结矩阵。
- 实现必须保持 `scheduler` / `pin` / `route-session` 的领域归属，不将冷却、绑定、会话生成职责外移或合并；流式提交后不切换上游/不重新生成的不变式保持不变；代理/池/凭证/备用渠道/恢复域仅为对象/候选上下文，不映射为固定 L2/L3。
- 400 重放的实现需保证同目标、同路由会话、同一请求身份的约束，属于 L2 稳定非法请求解决动作，且其结果为整条路由的最后恢复动作（route-terminal，按 L3 终止/忠实返回），不再继续候选/通道/fallback 评估；不得将重放结果回流至正常恢复或视为 L1 retry，不得把 400 写成 L1 行为。
- 取消 / deadline / 已提交字节的停止语义（L3 边界）需在网关入口与流式网关处一致执行，不产生新的后台重试；503 不设 L2 持续观察（稳定性观察仅属 L1），L1-final 503 后仅按既有对象选择/耗尽规则解决、否则按 L3 忠实返回，不写成无限 retry。
- 配置变更保持严格未知字段拒绝与原子切换语义；新增恢复相关配置需显式说明是否热生效或需重启（参考 `AGENTS.md` §3）。
- 不引入新依赖（`golang.org/x/crypto` 以外需显式理由），保持 `gofmt` 清洁与容器姿态。
- custom session takeover 并发边界：以 session 级 first-wins 收敛；在请求入口（`doUpstreamTiers`）、未绑定建立环路起点（`doUnboundEstablishment` loop-start）和 pin claim 后首次 native 发送前做有限重查；不提供 fallback/pin store 跨 store 原子线性化；已经发出的 native POST 不撤销，后绑定只影响尚未发送或后续请求；`claim.done` waiter 唤醒后回到入口重查。此为并发边界，不是广义统一恢复完成，也不改变现有 pin 的 claim/waiter 语义。

## 8. 验证要求（Validation Required）

以下为验证要求分项现状：已实现切片——单一 L1 观察、400 route-terminal、unbound 逐域状态无关耗尽、已绑定统一漫游与 valid-binding 对象不可用耗尽（无 consumed/预切换前提，含单代理 L1-final 与稳定凭证 401，实际池全过滤时可零发送接管含预冷 429、无伪造证据，`credential429` 仅全量 live429 写入）、cancel/deadline/committed 停止、503 无独立 L2 循环、retry 旧字段拒绝、`attempt_timeout_seconds` 启动覆盖与 `timeout_seconds` 最终预算——对应的 `go test ./... -count=1`、构建、gofmt 与直接测试覆盖已执行且通过（见各项“已执行”标注）；有界 consumption/预切换旧门相关证据行仅为历史证据（见修订历史 2026-10-03 条）。广义 cause→action 矩阵/退避/广义 fallback 仍未决定（本 ADR 范围外），对应验证保持 pending，不宣称完整统一三层完成：

- `go test ./... -count=1` 通过（CI 门禁）——已执行（现有验证，全量回归通过；见工作文档 §8）。
- `go build -o opencode2api ./cmd/opencode2api` 构建通过——已执行（现有验证）。
- L1 受约束观察至稳定的测试（`TestObserveSameTargetTransientStops` 覆盖 stable/context/observation-limit、`TestAuthL1ObservationAndTraversal`、`TestPinnedAuthObservationLimitTransport`、`TestRefreshTraversesAllCandidatesIndependentOfL1`）——已执行。
- L2 当前双门/503 的测试：unbound 逐域状态无关耗尽（`TestUnboundExhaustion503BothDomainsAllowCustom` / `TestUnboundExhaustionAuth429And503AllowCustom` / `TestUnboundExhaustion503PartialOrdinaryNoCustom` / `TestUnboundExhaustion503NoActivePreserves503` / `TestUnboundDomainsExhaustedAllowCustom` 系列）与 pinned 全量 live429（`TestPinnedAuthFull429StillCustom`）；`TestPinnedConsumption*` 系列为有界 consumption 旧门历史证据（见修订历史 2026-10-03 条），现行 pinned 非 429 接管为 valid-binding 对象不可用耗尽，新门验证已随批准实现落地；503 仅 L1 同目标有界观察、无 L2 持续观察循环（`TestUnboundAnonymous503RetrySuccessAfterMultipleFailures` / `TestPinned503RemainsSameTarget` / `TestPinned5xxRetryOnly`）——已执行。
- 400 同目标修正重放后终态的路径测试（L2 修正动作完成后按 L3 终止/忠实返回，重放后不进入正常恢复/不触发后续候选、通道或 fallback，重放结果即路由最终结果，不是 L1 retry；`TestAnonymous400ReplaysSameTarget` / `TestAuthZen400ReplaySuccess` / `TestAuth400RecoveryRouteTerminal` / `TestCustomTakeover400ReplayFinalNoCustom` / `TestMaybeReplayCandidate400Entry` / `TestClassifyStableCause`）——已执行。
- 取消 / deadline / 已提交字节后停止恢复的测试（L3 边界；`TestCancelStopsRetryFallback` / `CommittedStream_*` 路由层回归 / `TestPinnedConsumptionAuthDeadlineNoCustom` / `TestPinnedConsumptionAuthCancelDuringSecondProxyNoCustom`）——已执行。
- 预算收敛的拒绝语义测试（`retry.max_attempts` 为唯一 L1 最小观察计数，含首次发送，非统一错误额度/对象队列额度；已删除 `transient_max_attempts` 严格 unknown-field 拒绝、无值迁移；`TestTransientMaxAttemptsStrictlyRejected` / `TestObserveSameTargetTransientStops` 计数语义）——已执行。
- 未绑定 408/425 双域 L1-final 耗尽接管的测试证据（`TestUnboundExhaustion408425BothDomainsAllowCustom` 表驱动 408/425：anon/auth 两域均进入、各单冻结候选、`MaxAttempts=3` 各 3 次同目标 L1 观察后按域耗尽→active custom 接管一次并绑定 session、不建 pin，`attempts=7`；408/425 调度中性：不写 proxy429/channel/target/credential401/credential429；不声称广义矩阵完成）——已执行。
- 未绑定 post-L1 共享纯决策接缝与取消优先测试证据（历史中间增量，已被单候选统一决策 `decideCandidateRecovery`/`recoverSingleCandidate` 取代；`decideUnboundPostL1` 全套已删除，无并行权威；`TestDecideUnboundPostL1` 覆盖的 exact400/ordinary/L1Final+Cancelled 双 lane 取消优先语义由 `TestDecideCandidateRecovery` 四 lane 差异表继承）——当时已执行，现仅为历史证据。
- 未引入新的未脱敏日志/指标输出的人工审查：批准实现范围内的生产 Go diff 静态人工审查已执行，新增 logger/metrics/admin/history/body/key/session/proxy 输出 sink 0 个；限此范围未见新增未脱敏输出，不声称全库安全审计——已执行。
- 未绑定认证 request-local transport-suspect L2 原因（已实现/已验证切片，无广义矩阵）：`gateway.go` 未绑定认证建立内，真实 L1-final true transport（`isTrueTransportError`，含 L1 final/advance 门；408/425/5xx/stream-startup/429/cancel 不建证据）建立本请求内 `(tier/channel,pool,proxy)` 身份，同身份后续冻结候选在 suspect 仍 active 时跳过（过期不跳、跨池不命中、不播种预冷、匿名域证据不借用、已绑定不变）；跳过无 POST/attempts/上游记录/调度写入，按域计入 `Unavailable` 并标记 `Entered`，永不计入 live429/`credential429`；`unbound_transport_suspect_scope_test.go`（跨凭证跳过同代理并在另一代理成功/pin 且 attempts 仅计实发、后凭证全跳过按域耗尽接管 active custom 且 skips 非 attempts、408 代表不建证据且同代理仍发送、过期/缺席/跨池谓词加受控调度状态无睡眠、池限定身份单元）；401/proxy429/unbound 耗尽/transport-suspect/pinned/custom 聚焦回归与 `go test ./... -count=1` 通过、gofmt 干净——已执行。
- 四 walker 单候选恢复控制流结构迁移（`recovery.go` runner + 统一纯决策，四生产路径消费；行为保持，无原则变更）：`decideCandidateRecovery`（`cause + lane context -> typed action`，`recoveryLane{Bound,Anonymous}` 四 lane：匿名广义/未绑定认证 Cancelled-only/已绑定认证 strict 三套 context 门，pinned-auth initial-or-final vs pinned-anon final-only 双哨兵身份，pinned-auth 非哨兵 transport walk vs 他 lane 忠实，429/403 bound/unbound 分流，unbound Stop 门控 mark；不选响应、不排空、不触碰状态）+ `recoverSingleCandidate`（唯一 initial send、唯一 L1 有界观察、唯一 exact400 corrective replay，post-L1 context/replay 资格复用同一纯决策的 typed action（能否 replay 由决策决定，无并行 context 门）；Final/Stop/Cancelled/ReplayTerminal/StreamSentinel/Started typed 结果）；`gateway_unbound_decision_test.go` 改写为 `TestDecideCandidateRecovery` 四 lane 差异表，`TestPinnedAuthContextEarlyReturn` 改写为统一决策 strict 门断言，新增 `recovery_runner_test.go` 跨 lane 生命周期证明（单发送/L1 有界计数/replay 恰一次终态/同 transport 终态三动作分流/Started 保留/取消单发送停止）；`decideUnboundPostL1` 全套与 `pinnedAuthContextEarlyReturn` 已删除，无并行权威；pinned-auth Final-authority 修复已纳入（429 耗尽优先于哨兵毒化，partial initial-sentinel 才 502）；`go test ./... -count=1` 通过、gofmt 干净、仓库外临时构建通过——已执行。
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
  - 验证：新增 `unbound_proxy429_scope_test.go`（跨凭证跳过同代理并在另一代理成功/pin 且 attempts 仅计实发；后凭证全跳过按域耗尽接管 active custom 且 skips 非 attempts；1 实发 429+1 skip 不写 `credential429`；池限定身份单元）；既有 `TestUnboundExhaustionCredentialDomainsNotMerged` 改为双代理夹具以在新跳过语义下保持凭证域不合并证明（单共享代理下后凭证全跳过即耗尽接管为新正确行为）；聚焦/相关 `unbound/401/400/pinned/custom/429` 回归通过、gofmt 干净；全量门禁已随批准实现落地通过，不声称广义矩阵完成。
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
  - §6“候选序列与特定 429 fallback 基线”与 2026-09-22 修订条“当前代码仍为历史候选/429 fallback 基线”均为当时快照，现已由 `b2c4798`（未绑定按域状态无关耗尽：每域独立 `Entered && Frozen>0 && Unavailable>=Frozen`，允许集 live 429 / 401 / 403 / L1-final 408-425-5xx-transport 含 L1-final stream startup，400 / ordinary 4xx / cancel-deadline / committed / build 不计，状态无关）与 `1e4536c`（历史快照，已被 2026-10-03 最优坏节点修复取代；当时已绑定保留全量 live 429 并新增有界 consumption 门：已实际切换到下一冻结 eligible、全部冻结 eligible 真实尝试、末位对象不可用；预冷零发送保持原生；单代理首发非 429 / 400 重放 / ordinary 4xx / cancel-deadline / committed 不接管）取代；`977790a` / `1a5904f` / `8a43803` 为纯测试提交，无生产语义变更。
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

- **2026-09-24 — 未绑定认证 request-local transport-suspect L2 原因（已实现/已验证切片，无广义矩阵）：**
  - 未绑定认证建立内，真实 L1-final true transport（`isTrueTransportError` 且 L1 已达 final/advance 点）建立本请求内 `(tier/channel,pool,proxy)` 稳定不可用原因：同身份后续冻结候选在 suspect 仍 active 时跳过无 POST（仅请求内真实 transport 证据触发，预冷不播种、过期不延长、跨池不命中、匿名域证据不借用）；跳过按本请求证据计入该凭证域 `Unavailable` 并标记 `Entered`，不增 `attempts`、不记 upstream attempt、不写任何调度状态、不计入 live429/`credential429`；401 凭证围栏、request-local proxy429 围栏、exact400 终态、ordinary4xx、target 403/5xx、L1、cancel/deadline/committed、route-session/pin/custom first-wins 均不变；已绑定行为不变。
  - 语义边界：仅未绑定认证；代理健康仍为连通性语义，suspect 为有界临时候选过滤；不冻结广义逐状态矩阵/退避/广义 fallback。

- **2026-09-24 — 403 target-scoped stable cause 纯决策显式化（行为保持，无广义矩阵）：**
  - `gateway.go` 纯决策接缝内将 HTTP 403 提升为独立显式原因 `stableCauseTargetForbidden`（`isStableTargetForbidden`：`err==nil && 403`，不进 L1、不属 ordinary、不再落入通用 L1-final）：`classifyStableCause` 在 live429 之后/ordinary 之前分类，initial 403 与 transient 后 final 403 均归此原因；`decideUnboundPostL1` 对此原因双 lane 均返回 `advanceNext+markUnavailable`（取消门优先：`Cancelled` 仍按既有 lane 门返回 context），与既有可观测 walk 一致——未绑定当前 target 标记不可用并推进下一冻结 target，逐域真实耗尽后才可能 custom；`finalNon429ObjectUnavailable`/`unboundObjectUnavailable` 的 403 叶语义、scheduler target-cooldown、已绑定不移动/不新增 custom/冷却语义、普通 4xx/401/429/transport/503/fallback/pin/session/stream 语义均不变，预算/退避/冷却数值不变。
  - 验证：`stable_cause_test.go` 新增 initial 403 / transient-后-final 403 / late-cancel 保持 403 断言（含非 transient、非 ordinary、仍计入 object-unavailable）与既有 401-仍-L1Final 回归；`gateway_unbound_decision_test.go` 新增双 lane 403 advance+mark 与 cancelled-override 行；聚焦与 `go test ./...` 通过、gofmt 干净。
  - 语义边界：仅纯决策分类显式化；不展开其他状态码，不冻结广义逐状态矩阵/退避/广义 fallback。

- **2026-09-24 — 四 walker 单候选恢复控制流迁移（结构收敛，行为保持；含 pinned-auth Final-authority 修复的完整纳入）：**
  - 新增 `internal/app/recovery.go`：`recoveryLane{Bound,Anonymous}` 上下文、`decideCandidateRecovery`（`cause + lane context -> typed action` 纯决策：匿名广义 `Cause==context||Cancelled` / 未绑定认证 `Cancelled`-only / 已绑定认证 strict `stop==context||stable+cancelled` 三套 context 门；pinned-auth initial-or-final vs pinned-anon final-only 双哨兵身份，未绑定无哨兵分支；pinned-auth 非哨兵 transport 终态 `WalkNext` vs 他 lane 忠实；live429 bound 暂存 vs unbound 推进+mark；403 bound 忠实 vs unbound 推进+mark；unbound L1-final 仅 stable/limit mark；不选响应、不排空、不触碰调度/pin/custom/attempt 状态）、`recoverSingleCandidate`（单候选全生命周期：唯一 initial send、唯一 L1 有界观察 authority、唯一 exact400 corrective replay authority；`Final/Stop/Cancelled/ReplayTerminal/StreamSentinel/Started` typed 表达，`final.Started` 不丢失；取消/截止无后续发送；503 无 L2 循环；429 不进 L1）。
  - 四生产 walker 改走共享 runner + 执行 typed action 的短 switch：`doPinnedAnonymous`（bound+anon：429 暂存/哨兵 final-only 502/其余 consumption 门忠实）、`doPinnedAuth`（bound+auth：429 证据+耗尽 custom/pinMove fencing/transport walk/consumption 门忠实）、`doAnonymousUpstream`（unbound+anon：401 围栏/mark/bind）、`doKeyUpstream`（unbound+auth：401 围栏/request-local proxy429 与 suspect 证据/credential429 证据门/bind）；删除四路径重复 initial/post-L1 决策链；删除 `decideUnboundPostL1` 全套与 `pinnedAuthContextEarlyReturn`（语义折入统一决策），无并行权威、无死兼容。
  - 保留边界：walker 继续拥有 frozen 候选、三围栏、scheduler 证据写入（含 credential429 429-only 门与 Started stale fencing）、pin bind/move fencing、route-session/body/attempt metadata、response ownership（含 HEAD 排空归属：pinned-anon context 中间态排空、transport 非初态排空）、custom 双门（当时 pinned 全 live429 + bounded consumption 不合并，未绑定逐域耗尽不变，partial429 不写 credential429，custom first-wins 不变；bounded consumption 旧门已被 2026-10-03 取代，见该条）；pinned-auth 429 耗尽优先于哨兵毒化（full exhaustion precedes poisoning，partial initial-sentinel 才 502、无 custom/credential429）；exact400 同目标/同请求/同会话只一次（含 L1 后 400），replay 结果全 route terminal（成功正确 pin，二次 400/其他忠实，无新 candidate/custom）；取消/截止/committed 停止无后续发送；503 无 L2 观察；429 遍历不受 `max_attempts` 截断；匿名普通 4xx 按当前外层进入认证；共享 authority 不建立新状态生命周期，不改变 scheduler/pin/session store，不新增依赖。
  - 验证：`gateway_unbound_decision_test.go` 改写为 `TestDecideCandidateRecovery`（四 lane 差异：context 三门、429/403 分流、哨兵双身份、transport walk/忠实分流、unbound Stop 门控 mark；exact400+Cancelled 四 lane `Stop==stable` 均 `returnContext`、ordinary+Cancelled 未绑定双 lane `returnContext`，均用合法稳定 Stop，不断言 pinned-auth `observationLimit+Cancelled` 早停以保留 strict 窄门），`stable_cause_test.go` `TestPinnedAuthContextEarlyReturn` 改写为统一决策 strict 门断言，新增 `recovery_runner_test.go`（单稳定发送、L1 恰 `maxObservation` 计数、exact400 恰一次终态、同 transport 终态三 lane 三动作、`Started` 保留、取消单发送停止、`TestRunnerCancelledStableNoMarkOrAdvance` 证明已取消 initial 429/401/403 单发送无 mark 不推进）；既有 403/401/proxy429/suspect/pinned-consumption/400/custom/429 聚集回归与 `go test ./... -count=1` 通过、gofmt 干净、仓库外临时构建通过。
  - 可观察取消差异（精确 HEAD/当前对照，不恢复旧行为，不引入额外状态）：initial 429/401/403 时已 cancelled，新 runner 在 walker mark 之前 `returnContext`，HEAD 先走 walker mark 路径再 `return`。精确区分两层——request-local 耗尽/证据 marks 与 scheduler writes：scheduler 写入两侧均无（`sendUpstreamOnce→applyAttemptOutcome` 遇 `ctx.Err()!=nil` 直接返回分类、不写任何 scheduler 层）；`markAnonUnavailable`/`markCredUnavailable`、401 围栏、`reqProxy429` 插入本身均带 `isContextCancelled` 门，两侧均不产生 `Unavailable` 增量/围栏/request-local 证据。唯一 HEAD-only 残留是未绑定认证 initial 429 无 cancel 门的本地 `cred429Evidence` 表项（scheduler 写仍被 `Cancelled` 门拦截），return 后即丢弃，不影响 `Unavailable`/耗尽/custom。保留新取消早停（符合取消边界：无后续发送、无后续 local recovery evidence、无错误 custom），不恢复旧 cancel 后 mark。
  - 语义边界：仅单候选控制流结构收敛；广义 cause→L2 矩阵/退避/广义 fallback 仍未冻结，不声称三层已完全落地。

- **2026-09-24 — pinned-auth L1 Final observation authority 全路径修复（根因修复，无新矩阵）：**
  - `doPinnedAuth` 同目标 L1 执行后，所有 transport next-proxy、`pinnedConsumptionAllowCustom` 与 L3 决议一律使用 `final.Resp`/`final.Err`；`Initial` 仅保留排空/归属指针比较与既有 initial-or-final stream 哨兵身份，不得覆盖 `Final`。此前残留的 `sendErr`/`resp`（initial）分支在 mixed 503→bare-transport 下错误抑制了既有 auth pinned transport 规则；本次将其收敛到 final authority，删除 stale-initial 分支与相关注释（`transientLoopResult.InitialResp/InitialErr` 保留仅为哨兵毒化与排空归属，中间哨兵不毒化不变）。
  - 完整不变量（与 `AGENTS.md` 已有 auth transport move 合同一致）：单代理 mixed final 为忠实 502（非 stale 503），无 custom、无 binding、pin 不动、不写 `credential429`；多代理同终态按既有规则尝试下一 eligible proxy，下一代理 2xx 则成功并 generation-fenced move current；已 consumed 且全部 eligible 真实尝试、末位 final transport 时按当时既有 bounded consumption 门可 custom（非 429 不写 `credential429`；历史快照，已被 2026-10-03 最优坏节点修复取代）；反向 initial transport→final 稳定 HTTP（如 403）以 Final 为准忠实返回，无错误 transport walk。
  - 验证：`transient_observation_loop_test.go` 新增/重写四用例——`TestPinnedAuthMixed503TransportFinalObservation`（单代理 502、无 custom/binding、pin 不动）、`TestPinnedAuthMixedFinalTransportWalksNextProxy`（pinned 2 次 + walk 1 次、attempts 3、move 到 other 且 generation+1）、`TestPinnedAuthMixedFinalTransportConsumedTakeoverCustom`（429 消耗后 mixed final 接管 custom 200、attempts 4、binding 落定）、`TestPinnedAuthReverseTransportToFinal403`（Final 403 忠实、other 0 hit、pin 不动）；mixed 聚焦 + pinned503/pinned consumption/unbound503/哨兵相关回归与 `go test ./...` 通过、gofmt 干净。
  - 语义边界：仅已绑定认证 L1 后决议；anonymous pinned、未绑定、哨兵毒化、cancel/deadline、429、预算/退避/冷却不变；不冻结广义逐状态矩阵/退避/广义 fallback。

- **2026-09-24 — L1 最小观察延迟窄实现（已接受生产边界，无广义矩阵）：**
  - 共享 `transientDelay` 增加命名内部下限 `minL1ObservationDelay=100ms`：零/负/低于下限输入先取下限，再与配置正间隔与解析后 `Retry-After` 取 max；显式 `retry.transient_retry_interval_seconds=0` 仍合法并持久化 0（缺失默认 3），运行时永不立即重发，符合已接受 ADR “L1 从不立即重发”；`sleepWithContext`、exact400 修正重放延迟、计数/lane/调度/pin/响应归属/遍历均未动。
  - 验证：新增 `transient_delay_floor_test.go`（`TestTransientDelayFloor` 精确下限/正间隔/`Retry-After` 取 max；`TestObserveIntervalZeroHasMinimumDelay` 以配置 0 的真实 `transientInterval()` 走真实 L1 环证明非立即重发，稳健下限 50ms、无窄上界；`TestObserveIntervalZeroFloorCancellable` 证明 floor 等待中 cancel/deadline 无再次发送）；transient/runner/400/耗尽/配置往返聚焦回归通过、gofmt 干净。
  - 语义边界：仅 L1 floor；不构成广义退避矩阵，不冻结逐状态矩阵/预算/广义 fallback。

- **2026-09-24 — 发送前取消零发送窄实现（已接受停止边界，无广义矩阵）：**
  - `recovery.go` `recoverSingleCandidate` 入口先查已取消 context：未做 initial increment/meta/exec 即返回 typed `recoveryReturnContext`（`ctx.Err` + `transientStopContext`，无 `Started`/初始响应伪造、无 unavailable 证据，保留既有 attempts/meta，不 replay）；`gateway.go` `observeSameTargetTransient` 在 sleep 成功后、increment/exec 前复查 `ctx.Err`，覆盖 timer/context 同时就绪；通用 `sleepWithContext` 未动，无全局钩子；exact400 语义、四 lane 决策、committed stream、scheduler/pin/custom 门均未动；仅承诺已可观察取消前不发，不承诺与竞态 wire dispatch 的原子性，不新增子系统。
  - 调用方安全：四 walker 既有 `recoveryReturnContext` 分支均可安全处理 nil 响应 + `ctx.Err`（无 `Header`/排空空指针，`mark*Unavailable` 与 401/proxy429/suspect 证据本就带 cancel 门，外层 custom 门在 cancelled 下恒否），无需拓宽 walker 变更。
  - 验证：`recovery_runner_test.go` 原 pre-cancel 单发送期望更新为零发送（`TestRunnerCancelledStableNoMarkOrAdvance` 非零初值保持 + `TestRunnerStartedPreservedAndCancelStops` 后半），新增 `TestRunnerCancelDuringExecPreservesResponseNoAdvance`（执行中取消保留真实响应/`Started` 身份、无 mark/advance/replay）与 `TestRunnerPreSendCancelZeroSendFourLanes`（四 lane × pre-cancel/expired-deadline，非零初值、无 replay、无 meta 副作用），`transient_observation_loop_test.go` 新增 L1 `contextExpiredDeadlineAtEntry`（初始态无发送、保留初始未排空）与既有 `contextPreCancel`/`contextSleepInterrupt` 回归；runner/transient 决策、cancel/deadline、gateway cancel+stream+400 聚焦与 `go test ./... -count=1` 通过、gofmt 干净。
  - 语义边界：仅发送前可观察取消零发送与 L1 睡后复查；不冻结广义逐状态矩阵/退避/广义 fallback；残余广义策略仍未决，不编造下一 helper 任务。

- **2026-09-24 — FINAL 统一会话恢复定版（Accepted+Implemented，完整矩阵落地）：**
  - **L1 最终映射（定版，行为保持）：** 同目标有界观察仅 `true transport`、`408`、`425`、`500-599`（含 `503`）与 `pre-commit stream-startup failure`；`retry.max_attempts` 含首次发送；`min delay 100ms`（`transientDelay` 命名下限，`interval`/`Retry-After` 取 max）；`400/401/403/429/ordinary4xx` 永不 L1；L1 无指数退避/jitter，调度冷却指数退避+jitter 与 caps 保持不变（`scheduler.go`）。
  - **L2 修正重放（定版）：** 精确 `400` 一次同目标同凭证/池/代理/协议/路由会话/请求身份 corrective replay（Responses 陈旧 `previous_response_id`/`reasoning` 清理），后按 L3 route-terminal 终态返回，永不进入候选/通道/custom。
  - **未绑定域（定版）：** 请求冻结 eligible 域的稳定不可用证据完整集为 `live429`、`stable401`（同凭证剩余跳过计 Unavailable 无发送）、`403`、`L1-final 408/425/5xx/true transport/stream-startup`；`auth request-local live429/suspect` 跳过计 `Unavailable+Entered` 但永不计 `live429/credential429`；`Build/400/ordinary4xx/cancel/deadline/committed` 不计；每已进入冻结域须独立耗尽（`Entered && Frozen>0 && Unavailable>=Frozen`）才可 custom，且为 state-agnostic。
  - **已绑定域（历史快照，已被 2026-10-03 最优坏节点修复取代，现行见该条）：** pin 固定 `credential+pool+channel/model/protocol/authority`，永不跨域；代理移动仅 `pinned-anon/auth` 在 `live429` 时 walk，`pinned-auth` 额外在 `L1-final true transport` 时 walk，其余 `400/401/403/408/425/ordinary4xx/5xx/stream-startup` 永不移动；`generation fencing` 保留。
  - **已绑定 custom 双证明（历史快照，已被 2026-10-03 最优坏节点修复取代，现行见该条；单域权威下双路径，不合并证据）：** `(a)` 全量 `live429` 发送（`pre-cooled zero-send` 排除，`credential429` 仅此路径写入，`partial` 永不写）；`(b)` 有界 consumption：已真实 `move/send` 到另一冻结 eligible、`Attempted>=Eligible` 且 `Final` 为非 429 的 `object-unavailable`（`401/403/L1-final 408/425/5xx/true transport/stream-startup`），单代理首发非 429、`400/replay/ordinary4xx/cancel/deadline/committed/build` 不触发，非 429 接管永不写 `credential429`；`429` 仅为证据种类非自动直切。
  - **L3 停止与忠实返回（定版）：** `cancellation/deadline/committed` 一旦可观察立即停止（`recoverSingleCandidate` 入口与 L1 睡后复查），`success` 继续/绑定/移动，否则按目标协议忠实返回；不承诺与竞态 wire dispatch 的原子性。
  - **结构落地（单域权威）：** 保留 `classifyStableCause`/`decideCandidateRecovery`/`recoverSingleCandidate` 为单候选权威；新增 `recovery.go` 单域权威 `decideDomainRecovery`（`domainRecoveryInput{Recovered400,Cancelled,Committed,UnboundDomains, Pinned}` → `domainRecoveryResult{Kind: domainNone|domainUnboundExhausted|domainPinnedFullLive429|domainPinnedConsumption, AllowCustom}`），统一接管 `customTakeoverEligible`/`pinnedConsumptionAllowCustom`/`unboundDomainExhausted`/`unboundDomainsExhaustedAllowCustom` 的全部 `fallback/exhaustion` 判定，叶证据分类仍留 `gateway.go`，无第二判定；未绑定外层与双 pinned 游走器已迁移至该权威（仅保留 `draining/ownership/scheduler writes/pin fencing/request-local evidence/custom` 调用），旧判定与死分支已删，保持可观察行为仅在取消时 `credential429` 抑制与 lock 一致。
  - **详细矩阵正典：** 本 ADR 与 `recovery.go`（`decideDomainRecovery`/`pinnedDomainEvidence`/`unboundDomainEvidence`）、`gateway.go`（`finalNon429ObjectUnavailable`/`unboundObjectUnavailable`/`transientDelay`/`isSameTargetTransient`）为正典；`AGENTS.md` 仅保留高层不变式并链接至此。
  - **验证：** 新增 `domain_recovery_decision_test.go` 正典表驱动 `TestDecideDomainRecovery`（unbound 全域 AND、unentered/zero/recovered400/stops 否决；pinned full-live429 允许 vs partial/precooled/terminal 否决；pinned consumption 允许 vs 未 consumed/attempted<eligible/非 unavailable/stops 否决（旧门历史证据，已被 2026-10-03 取代）；证明种类互异；无状态直切），既有 `TestDecideCandidateRecovery`/`recovery_runner_test.go` 保持正典；`go test ./... -count=1` 通过、`go build`/`gofmt`/`git diff --check` 干净。

- **2026-10-03 — 最优坏节点修复（Optimal bad-node fix，已接受并已落地；当前行为以本条为准）：**
  - **超时/启动预算（新增明确，与旧条无冲突）：** `retry.attempt_timeout_seconds` 严格字段边界与默认值不变，仅覆盖每次原生发送/观察启动（响应头至首个可交付 SSE 事件），不是长流上限；父请求存活下的本地启动超时为 `stream_startup_timeout` target 冷却，走有界同目标 L1（`max_attempts` 含首次/既有延迟）。`retry.timeout_seconds` 最终预算不变：客户端流启动期限在可交付事件撤销，非流全请求期限不撤销（含匿名内部 agent 形态 SSE，需在成功 outcome 前收敛才算可交付）；成功原生/自定义 SSE 尾流存活于 attempt 计时之外。调用方取消/最终 deadline/已提交字节绝对停止且不伪造冷却；最终预算先尽则不保证全耗尽/接管。
  - **已绑定统一漫游（取代 2026-09-24 FINAL 条“已绑定域”旧表述）：** 旧文“代理移动仅 `pinned-anon/auth` 在 `live429` 时 walk、`pinned-auth` 额外在 `L1-final true transport` 时 walk，其余 `400/401/403/408/425/ordinary4xx/5xx/stream-startup` 永不移动”为历史已废弃。现行：bound anon/auth 统一在同一 credential+channel/model/protocol/authority 内漫游（当前池恒为 CURRENT 通道所属池，pool 不入 pin 身份，见本文件 2026-10-03 current-pool 条）——稳定 `429` 走尽可发送代理（无 L1），稳定 `403` 与 L1-final 真传输/预提交启动/`408`/`425`/`5xx` 在同目标 L1 观察后走冻结可发送代理；稳定 `401` 为全局凭证失败，同凭证不再发送但计绑定不可用。精确 `400` 同目标 corrective replay 任何重放结果均为 route-terminal。ordinary 4xx/build/配置身份/tombstone 无效/cancel/deadline/committed 按目标协议本地忠实返回，绑定内不产生新凭证/通道/池。alternate `2xx` 仅代际 fencing 更新 current，保持 proxy-free wire session/body 身份。被删除/身份失配的绑定本地 `502` 且永不回退。
  - **Pinned custom 接管（取代“已绑定 custom 双证明”中 bounded consumption 旧门）：** 旧门“已真实 `move/send` 到另一冻结 eligible、`Attempted>=Eligible`、末位非 429 对象不可用；单代理首发非 429 不触发；`pre-cooled zero-send` 永不接管”为历史已废弃，不再适用。现行：有效绑定对象不可用耗尽即可接管 active custom，含单代理 L1-final 与稳定凭证 `401`，无 consumed/预切换/全部真实尝试前提；有效绑定实际池代理被当前健康/冷却全过滤时可零发送接管（含预冷 `429`），无伪造 live 证据/attempts/`credential429`/metrics。`credential429` 仍仅全量 live429 写入（mixed/filtered 证据永不合格）。无 active 则保持原生状态/`Retry-After`。未绑定语义、custom 接管身份/first-wins/绑定后无原生回退/安全语义均不变。
  - **历史标记：** 本条之上的 `2026-09-24 FINAL` 条中“已绑定域”“已绑定 custom 双证明”内与本条冲突的行（bound-only-429、pre-cooled 永不接管、单代理 consumed 排除）保留为历史快照，不得再引用为现行规范；§8 中 `TestPinnedConsumption*`/`domain_recovery_decision_test.go` pinned-consumption 证据行同为旧门历史证据；新门验证已随批准实现落地（`go test ./...`、`gofmt`、构建通过）。

- **2026-10-03 — Current-pool 自然恢复（Current-pool natural recovery，已接受；固定新契约，仅取代不可变 assigned-pool-as-pin-validity）：**
  - **现行 pin 身份（取代 pool 入绑定身份旧表述）：** 原生 pin 保留 session+model 的 channel/credential/model/protocol/authority 与 ORIGINAL 上游 route-session 身份；establishment pool 仅保留为 wire 兼容的不可变派生来源（derivation-origin），不是路由许可，也不要求该池继续被配置。§2“已绑定=该 session+model 绑定的 credential+pool 可用代理集”与 2026-09-24 FINAL 条“pin 固定 credential+pool+channel/…”中把 pool 作为 pin 有效性/身份失配 tombstone 的行（及“被删除/变化的 target 仍命中原绑定并本地 502”“migrate only when pool-routable”派生表述）为历史已废弃，不得再引用为现行规范；现行恢复域为 bound=该 pin 的 credential + CURRENT 通道所属池可发送代理（当前资源选择），unbound=本次冻结 eligible targets，耗尽=健康/冷却过滤后无可发送对象。
  - **现行候选上下文（当前资源选择+稳定身份，无新迁移子系统）：** 当前候选恒为运行时 CURRENT 通道所属池；热 Apply 池 A->B 时既有原生 pin 自然选 B 节点，无池名失配人工本地 502、无强制旧节点失败 POST、无额外跨池迁移事件/子系统。CURRENT 池限定选择内原节点仍在则优先，否则按当前亲和；当前选择=（实际池,代理）二元组可变，受既有 generation fencing 收敛；全部节点健康/suspect/429/channel/target 冷却与可观测均按实际发送池判定，同 URL 不同池隔离；成功仅更新当前二元组，保持 ORIGINAL route-session/wire/body 身份。旧池名单独移除/重指派不是 tombstone；被删除/身份变化的 credential/provider authority/model/protocol 身份仍本地 502（原生/custom 发送前），不重建不回退。两原生通道共享规则，pin 内无凭证/通道提升，不把已配置 B 误作任意池尝试、不切换凭证/供应商。
  - **恢复/停止沿用（无矩阵变更）：** L1 有界同目标观察与 L2 统一漫游走当前池：稳定 403、稳定 429（无 L1）、L1-final 真传输/预提交启动/408/425/5xx；稳定 401 同凭证停发但计绑定不可用；有效绑定当前池对象不可用耗尽证明（含单代理 L1-final 与过滤零发送，含预冷 429）按既有 custom 规则接管，`credential429` 仅全部实际 eligible 真实 live429 写入、无伪造证据；cancel/总 deadline/committed/精确 400 任何重放结果/ordinary 4xx/build 停止均不变。
  - **热 Apply 沿用既有运行时重建/迁移（非新迁移特性）：** 保留 pins/当前选择/代际来源；仍新鲜的 proxy-free route-session override 只要支撑结构有效原生 pin 即保留，即使原 scope 池已不再被引用/移除，既有 idle TTL 与 legacy proxy-bound 丢弃/无效凭证-供应商-协议丢弃保留；未绑定 route-session scope 仍按 CURRENT 原生候选解析；仅被引用池为运行时资源，暂存池仍不构建/不探测/不计容量；切换前已开始请求走旧 Gateway，新请求走当前配置；首代仍为稳定无状态派生，无新存储/持久化/日志投影。
