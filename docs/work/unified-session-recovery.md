# 统一会话恢复工作文档

> 持久化工作记录 · 面向运维/开发者 · 仅含当前决策相关状态
> 不复制完整状态矩阵 / 代码清单 / AGENTS.md / README

## 0. 元信息

- 文件：`docs/work/unified-session-recovery.md`
- 目标：统一会话恢复（unified session recovery）
- 基准：当前代码即基线（`jorkey/integration` 最新提交 `566d7b7`），统一恢复尚未实现
- 检验时间：2026-09-22 现场检查

## 1. 结论（Outcome）

- 统一会话恢复未落地，当前实现保持原有按候选同目标单次 400 重放语义。
- 本文档为唯一持久化决策载体，不代表功能已完成。
- 任何新增 `internal/app/recovery.go`（历史路径 `recovery.go`）并行路径均视为偏离基线的废弃方案，已否决。

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

- 统一会话恢复的触发条件、跨候选/跨通道统一策略、与现有 400 重放及 transient 重试的优先级仍未定义并落地。
- 现有代码仍为“同目标单次重放”基线，统一恢复逻辑尚未编码、未验证。
- 首个有界重构缺口已闭合（Bounded Increment 1）：`internal/app/gateway.go` 已建立结构化单次发送执行器 `executeAttempt` 并已将 `doPinnedAnonymous` 迁移至该边界（行为等价、无恢复语义变更），为后续统一恢复提供了单一收敛边界。
- 第二个有界重构缺口已闭合（Bounded Increment 2）：`internal/app/gateway.go` 内 `attemptOutcome` 已新增 `Started int64`，`executeAttempt` 已对普通与重试发送均返回该 `Started` 并以同一值完成流式成功/启动失败的调度与监控记录，`doPinnedAuth` 两处直连 `sendUpstreamOnce` 块已迁移至该边界（行为等价、无恢复语义变更）；更广义的统一会话恢复仍未落地，当前仍以同目标单次 400 重放为基线。
- 第三个有界重构缺口已闭合（Bounded Increment 3）：`internal/app/gateway.go` 内 `doAnonymousUpstream` 两处直连 `sendUpstreamOnce` + 内联流式门控块已迁移至既有 `executeAttempt` 边界（`TierZen`/`g.cfg.Upstream.Zen`/`route.Protocol`/`candBody`/`ids`/`cand`/`routeSession` + `channel=anonymous`/`credDisplay=anonymous`/`anonymous=true`/`attemptOffset+attempts`），行为等价、无恢复语义变更；更广义的统一会话恢复仍未落地，当前仍以同目标单次 400 重放为基线。
- 第四个有界重构缺口已闭合（Bounded Increment 4）：`internal/app/gateway.go` 内 `doKeyUpstream` 两处直连 `sendUpstreamOnce` + 内联流式门控块已迁移至既有 `executeAttempt` 边界（`route.Tier`/`g.cfg.Upstream.Zen`/`route.Protocol`/`candBody`/`ids`/`cand`/`routeSession` + `channel="key"`/`credDisplay=cand.CredDisplay`/`anonymous=false`/`attemptOffset+attempts`，`firstStarted`/`retryStarted` 取 `out.Started` 并向 `cred429Evidence`/`noteCredential429Failure` 传播以保留 `lastStartedNanos` stale fencing），行为等价、无恢复语义变更；聚焦 `Started` 回归已补齐（对标 `TestPinnedAuthStartedCredentialEvidence` 覆盖 unbound 路径，双 429 `Retry-After 9`/`future cooldown`/`lastStartedNanos>1`/`nanos=1` fencing），门槛待重跑；更广义的统一会话恢复仍未落地，当前仍以同目标单次 400 重放为基线。

## 7. 当前自然工作单元（Current Work Unit · Bounded Increment 4）

> 已明确为本增量的唯一执行范围；不存在“下一单元未指定”的开放状态。以内容定位待迁移点，不依赖陈旧行号（Increment 3 已位移）。

- 目标：在现有 `internal/app/gateway.go` 内**仅迁移 `doKeyUpstream`** 两处直连发送/门控块至已建立的 `executeAttempt` 边界，行为等价、无恢复语义变更；保持代码仅在 `gateway.go`，不新增 `internal/app/recovery.go`、不拆包、不改 `README/AGENTS/ADR`/依赖/配置 schema。
- 设计边界：
  - 精确替换两处（按内容定位，不按行号）：首发 `sendUpstreamOnce` + 内联 `verifyStreamGate`/`applyStreamSuccess`/`noteStreamStartupFailure`/`record` 与 transient 重试 `sendUpstreamOnce` + 同样门控块，改为 `executeAttempt(ctx, route, route.Tier, baseURL, route.Protocol, candBody, ids, cand, routeSession, "key", cand.CredDisplay, false, attemptOffset+attempts)`；其中 `route.Tier`/`baseURL=g.cfg.Upstream.Zen`/`route.Protocol`/`candBody`/`ids`/`cand`/`routeSession`/`channel="key"`/`credDisplay=cand.CredDisplay`/`anonymous=false`/`monitorAttempt=attemptOffset+attempts` 与现有 `sendUpstreamOnce` 实参精确一致；`executeAttempt` 已完整拥有流式门控与记录，外层不再重复。`firstStarted`/`retryStarted` 均取 `out.Started` 赋值，保留 `Started` 向 `cred429Evidence` 与 `noteCredential429Failure` 的传播与 stale fencing。
  - `executeAttempt` 仍为单次发送执行器，不拥有：冻结已认证候选顺序（`buildAuthCandidates`/`orderCandidates`）、`ordinarySends` 预算与 429 `ordinarySends--` 退款（429 budget-neutral）、`cred429Evidence` 累积与 `noteCredential429Failure` 的全量 eligible 计数门控（以 last `Retry-After` 与 `Started` 的 stale fencing 为准）、单 transient token/delay（`maxTransient`/`transientDelay`/`sleepWithContext`）、pinHit 早退、`exact-400` 同目标单次重放终态（`replayCandidate400` 与 `recovered=true` 超预算最终性）、`isOrdinaryClientRejection` 判定、`lastResponse`/`lastErr` 保持、`bindSessionPin`、上下文取消（`isContextCancelled`/`ctx.Err()`）与 `upstream stream startup failure` sentinel 的同目标 transient 重试语义、attempt 编号（`attempts`/`syncAttemptMeta`/`attemptOffset+attempts`）、drain 语义、日志与协议 envelope。以上仍由 `doKeyUpstream` 及其外层 `doUpstreamTiersUnbound`/custom fallback 负责。
  - 自定义 fallback 与 tier 迁移仍在外层调用方，不在本增量执行器内。
  - 精确保留：build 错误直接返回、`Resp/Err/Diag/Started` 映射、取消与流启动 sentinel 行为、429 `Retry-After` 末值与 `credential429` 写入的 `future cooldown`、attempt 连续性、discard/drain 语义、与语义相关的日志与所有协议 envelope；不改变当前 exact-400 终态语义。
  - 不触及 `doAnonymousUpstream`/`doPinnedAnonymous`/`doPinnedAuth` 及其测试。
- 保持不变：
  - `executeAttempt` 仍为单次发送边界，不引入恢复策略；`doKeyUpstream` 的冻结顺序、`ordinarySends` 预算与 429 退款、`cred429Evidence` 全量门控与 `Started` 传播、transient 预算、400 重放、pinHit、`lastResponse`/`drain`/`bind` 均保持不变。
  - 流式语义：`executeAttempt` 内部仍以 `Started` 完成 `applyStreamSuccess`/`noteStreamStartupFailure` 与 `recordUpstreamAttemptWithClass`，外层仅保留成功日志与 `bindSessionPin`；启动失败以 `upstream stream startup failure` 同目标重试，不推进候选。
  - 统一会话恢复仍未实现：本增量仅为结构化收敛，不引入跨候选/跨通道统一恢复策略。

## 8. 验收证据（Acceptance Evidence · Increment 4）

- 文档：本文件已更新，当前自然工作单元与设计边界明确为 Bounded Increment 4 已闭合，阐明仅迁移 `doKeyUpstream` 两处直连块至 `executeAttempt`（已认证元数据 `route.Tier`/`g.cfg.Upstream.Zen`/`route.Protocol`/`candBody`/`ids`/`cand`/`routeSession`/`"key"/cand.CredDisplay/false`/`attemptOffset+attempts` 精确一致，`firstStarted`/`retryStarted` 取 `out.Started`），且 `frozen ordering/ordinarySends+429 refund/cred429Evidence+Started fencing/transient/400 replay/pinHit/lastResponse/bind/fallback` 仍在执行器外；未声称更广义的统一恢复已实现。
- 代码：`internal/app/gateway.go` 内 `doKeyUpstream` 仅两处直连发送块已替换为 `executeAttempt`，保留 `buildErr`/`Diag`/`Started`/`isContextCancelled`/`startup failure` sentinel、`ordinarySends`/`cred429Evidence`/`attempts`/`lastResponse/lastErr`/`bindSessionPin`/日志 envelope 语义；未新增 `internal/app/recovery.go`；`gofmt` 干净。
- 验证：
  - 新增聚焦回归（对标 `TestPinnedAuthStartedCredentialEvidence` 覆盖 unbound 路径，`internal/app/unbound_auth_started_test.go:TestUnboundAuthStartedCredentialEvidence`）：`doKeyUpstream` 双 eligible 代理各 live 429（`Retry-After` 如 `4` 与 `9`，冻结全量）→ 断言 `credential429` 以末个 `Retry-After=9` 写入、`future cooldown` 生效、`lastStartedNanos` 为真实传播值（`non-zero` 且 `>1` 且 `<` 末次 429 到达时 `time.Now().UnixNano()`，非 `0` 回退）且 `nanos=1` 陈旧成功不能清除。
  - 既有 key/auth/429/stream/400/pinned 429、stale429 与匿名 transport/429 用例保持权威回归；已执行 `gofmt`、聚焦 `go test ./internal/app -run TestUnboundAuthStartedCredentialEvidence` 与全量 `go test ./...`，门槛待重跑（执行结果见本次报告）；`go build -o opencode2api ./cmd/opencode2api` 按需通过。

## 9. 未决问题（Unresolved Questions）

- 统一恢复是否允许跨代理或跨凭证迁移，抑或仍限定同目标同会话？
- 与 Responses `previous_response_id` 清理及自定义 fallback 接管的交互边界？
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
