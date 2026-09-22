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
- 首个有界重构缺口已闭合（Bounded Increment 1）：`internal/app/gateway.go` 已建立结构化单次发送执行器 `executeAttempt` 并已将 `doPinnedAnonymous` 迁移至该边界（行为等价、无恢复语义变更），为后续统一恢复提供了单一收敛边界；更广义的统一会话恢复仍未落地，当前仍以同目标单次 400 重放为基线。

## 7. 当前自然工作单元（Current Work Unit · Bounded Increment 1）

> 已明确为本增量的唯一执行范围；不存在“下一单元未指定”的开放状态。

- 目标：在现有 `internal/app/gateway.go` 内建立真实的结构化单次尝试执行器 `executeAttempt`，并**仅迁移 `doPinnedAnonymous`** 至该边界，行为等价、无恢复语义变更。
- 设计边界：
  - 代码保留在现有 `gateway.go`；不新增 `internal/app/recovery.go` 或拆包。
  - 新/重命名 `executeAttempt` 边界仅拥有单次发送事实：调用现有请求发送/状态分类权威、流式启动门控（落字节前）、流成功调度/监控记录、启动失败分类/记录，并返回足以支撑外层循环的结构化结果。
  - 不拥有候选选择/推进、代理回退决策、ordinary-send 预算、credential429 证据累积、custom fallback 调用、exact 400 重放、pin 绑定/移动、或路由会话/体构造。以上仍由 `doPinnedAnonymous`/相邻所有者负责。
  - 仅迁移 `doPinnedAnonymous`；其他 native 循环保持现行路径，不引入第二套策略表或未使用的策略抽象。
- 保持不变：
  - attempts 编号与监控记录完全一致，含流式启动失败与上下文取消路径。
  - `doPinnedAnonymous` 行为：仅 429 可遍历代理；同目标 transient 观测保留；400 重放保持同目标且在当前基线下为终态；401/403/普通 4xx/408/425/5xx 在现行同目标规则后不移动；live/local 429 证据、custom fallback、相同体/会话、pinMoveCurrent 代际围栏与最终 envelope 保持不变。

## 8. 验收证据（Acceptance Evidence · Increment 1）

- 文档：本文件已更新，当前自然工作单元与验收标准明确为本增量，不再表述为“下一单元未指定”。
- 代码：`internal/app/gateway.go` 内新增 `executeAttempt` 结构化结果与边界；`doPinnedAnonymous` 唯一迁移至该边界；未新增 `internal/app/recovery.go`；`gofmt` 干净。
- 验证：
  - 新增聚焦回归覆盖 pinned anonymous 401、403 与普通 4xx（404 或 422）：对当前代理单次 POST、对端/auth/custom 零次、原状态原样返回、pin generation/current 未变、无 proxy429 写入（表格测试位于既有相关 `*_test.go`，复用既有 helper）。
  - 既有 pinned anonymous 429/transient/400/stream 用例保持权威回归；执行最聚焦的相关包/用例并报告命令与结果。
  - `go test ./...` / `go build -o opencode2api ./cmd/opencode2api` 按需通过。

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
