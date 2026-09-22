# ADR 0002: Go 源码布局重构（Go Source Layout）

- **状态：** 已接受方向 / 实现待验证（Accepted direction / Implementation pending validation）
- **日期：** 2026-09-22
- **作者：** opencode2api maintainers
- **关联范围：** `main.go` / `gateway.go` / `scheduler.go` / `admin.go` / `webui/*` / `Dockerfile` / `compose.yaml` / `.github/workflows/release.yml` / `go.mod`

---

## 1. 背景（Context）

当前仓库采用**扁平 `package main` 拓扑**：

- 约 30+ 个 `*.go` 文件全部位于仓库根目录，统一声明 `package main`；`main.go` 为进程入口，其余文件（`gateway.go`、`scheduler.go`、`fallback.go`、`admin.go`、`availability*.go`、`convert.go`、`stream.go`、`models.go`、`config.go`、`runtime.go` 等）与白盒测试（`*_test.go`）同处同包、直接访问未导出标识。
- `admin.go: //go:embed webui/*` 将 `webui/` 作为根目录静态资源嵌入管理面，无独立包边界。
- 构建与发布路径均为根包：`go build -o opencode2api ./`、`go test ./...`、`Dockerfile`/`compose.yaml`/`release.yml` 均以 `.` 或 `./...` 为上下文。

该布局在早期可减少包间循环依赖成本，但随着网关、调度、可用性探测、自定义 fallback 等职责增长，根目录文件数与职责耦合度持续上升；同时缺少 `cmd/` 约束使得入口与库代码边界不清晰。需在不拆分紧密耦合实现的前提下，建立符合 Go 惯例的最小结构化布局，为后续可证伪的包拆分准备前提。

## 2. 决策（Decision）

确立**最小结构化布局**为已接受方向，约束如下：

1. **薄入口迁移（Thin entrypoint）：** 将当前 `main.go` 的进程入口逻辑迁移至 `cmd/opencode2api/main.go`，保持为薄包装（仅参数解析、配置加载、进程启动与信号处理），不承载业务逻辑。

2. **单包内聚（Single-package cohesion）：** 将根目录下所有紧密耦合的实现文件与白盒测试作为**一个包**整体迁移至 `internal/app`（包名保持 `main` 或统一为 `app`，以实现为准），不进行子包拆分。`gateway` / `scheduler` / `fallback` / `stream` / `pool` / `models` 等保持同包内直接协作，白盒测试随实现一并迁移，保持对未导出标识的覆盖能力。

3. **嵌入资源就近迁移（Embed co-location）：** 将 `webui/` 从仓库根目录迁移至 `internal/app/webui/`（或 `internal/app/admin/webui`，以实现为准），与 `admin.go` 的 `//go:embed` 声明保持同包就近关系，嵌入路径更新为对应该包的相对路径（如 `//go:embed webui/*` 在 `internal/app` 包内，或 `//go:embed admin/webui/*` 如置于子目录），不在根目录保留副本。

4. **暂缓进一步拆包（Deferred decomposition）：** 在依赖边界被验证为可证伪之前，**不**将 `internal/app` 进一步拆分为 `internal/gateway`、`internal/scheduler`、`internal/admin` 等子包；任何子包化需以明确的依赖方向与循环依赖消除证据为前提（见 §5 已拒绝方案）。

5. **构建/CI/Docker 路径切换（Build path retargeting）：** 所有构建、测试与镜像构建路径必须指向新入口：
   - 本地构建：`go build -o opencode2api ./cmd/opencode2api`
   - 全量测试：`go test ./...`（覆盖 `internal/app` 与 `cmd/opencode2api`）
   - `Dockerfile` / `compose.yaml` / `.github/workflows/release.yml` 中的 `go build`、`COPY`、`go test` 路径更新至 `./cmd/opencode2api`

> **实现细节待定标记：** `internal/app` 的包名（`main` vs `app`）、`webui` 在 `internal/app` 内的精确子路径（`webui/` vs `admin/webui/`）、以及 `cmd/opencode2api` 是否保留 `go:embed` 转发，均属实现待验证细节，本 ADR 仅锁定上述 5 条结构约束。

## 3. 范围与边界（Scope / Boundaries）

**在范围内（In scope）：**

- 文件物理位置迁移：`*.go` + `*_test.go` → `internal/app`，`main.go` → `cmd/opencode2api/main.go`，`webui/` → `internal/app/webui`（或等效就近路径）；
- `//go:embed` 路径与 `Dockerfile`/`release.yml`/`compose.yaml`/`README`/`AGENTS.md` 中构建路径的同步更新；
- `go.mod` 模块名保持 `opencode2api` 不变，`internal/` 约束由 Go 工具链强制。

**不在范围内（Out of scope）：**

- 业务语义变更：网关路由、调度六层冷却、会话亲和、可用性探测、自定义 fallback、配置热切换等行为均不在本 ADR 范围内；
- 子包拆分与接口抽象：不引入 `internal/gateway` 等新包，不新增接口层或依赖注入框架；
- 依赖变更：不新增 `golang.org/x/crypto` 以外的依赖；
- 持久化状态或跨重启行为变更。

**边界约束：**

- 本次重构为**纯结构移动**，不得改变对外 API、配置 schema、日志/指标脱敏与投影边界；
- `config.json` 权威与保存/Apply 事务语义保持不变；
- 不改变容器姿态（非 root、只读文件系统、`no-new-privileges`）。

## 4. 保留不变式（Preserved Invariants）

本决策不削弱 `AGENTS.md` §4 既有不变式，特别强调：

- 双通道（anonymous / authenticated）与 Zen-only 上游定位不变；
- 调度器六层状态（proxy 健康 / proxy429 / channel / credential 401 / credential429 / target）与迁移语义不变；
- 会话亲和与 `pin` / `route-session` 生成、绑定与代际围栏规则不变；
- 流式提交后不切换上游、不重新生成的不变式不变；
- 能力目录驱动 gating，不硬编码模型 ID；
- OpenCode 客户端身份统一构造权威不变；
- 配置严格校验与原子切换语义不变。

## 5. 已拒绝的替代方案（Alternatives Rejected）

- **保持根目录扁平 `package main`：** 拒绝原因：文件数持续增长导致职责边界模糊，`cmd/` 约束缺失使入口与库代码无法分离，不利于后续可验证的模块化。
- **立即拆分为多子包（`internal/gateway` / `internal/scheduler` / `internal/admin` 等）：** 拒绝原因：当前实现高度耦合，循环依赖风险高；在依赖方向未被证伪前拆包会引入不必要的接口与转发成本，违背“延迟分解直到边界被证明”原则。
- **仅移动 `main.go` 至 `cmd/` 而实现仍留根目录：** 拒绝原因：根目录仍为事实上的实现包，`internal/` 约束未生效，未解决扁平拓扑的根本问题。
- **将 `webui/` 保留在根目录、跨包 `go:embed`：** 拒绝原因：破坏嵌入资源与使用方的就近原则，增加构建上下文对根目录的隐式依赖。
- **将白盒测试转为黑盒 `*_test` 外部包测试：** 拒绝原因：现有测试深度依赖未导出标识，转黑盒将大幅降低覆盖有效性且无业务收益。

## 6. 后果（Consequences）

- **正向：** 布局符合 Go 惯例（`cmd/` + `internal/`），入口与实现边界清晰；`internal/` 强制封装防止外部误导入；为后续基于真实依赖边界的子包拆分建立最小前提。
- **负向/成本：** 所有构建/CI/Docker 路径需同步更新；`git log --follow` 与历史追溯需适应文件移动；未及时更新本地脚本或外部引用会导致构建失败。
- **中性：** 包内调用关系、测试形态与运行时行为保持不变；短期内除路径外的可观测行为无变化。

## 7. 迁移与实现约束（Migration / Implementation Constraints）

- 迁移必须为**原子移动 + 路径重写**：一次性完成 `*.go` / `webui/` 位置迁移与 `//go:embed`、`Dockerfile`、`release.yml`、`compose.yaml` 路径更新，不保留根目录副本或双路径兼容。
- `go.mod` 不更名；`internal/app` 不对外暴露为可导入公共 API。
- 构建与镜像必须以 `./cmd/opencode2api` 为唯一入口；`go test ./...` 必须覆盖新布局。
- 保持 `gofmt` 清洁与容器姿态；不引入新依赖。
- 文档侧仅更新构建路径相关描述，不改变行为语义描述。

## 8. 验证要求（Validation Required）

在实现被视为完成前，必须完成以下验证（均为待执行项，本 ADR 仅作要求声明）：

- `go test ./...` 通过（CI 门禁，覆盖 `internal/app` 与 `cmd/opencode2api`）；
- `go build -o opencode2api ./cmd/opencode2api` 本地构建通过；
- `gofmt` 检查通过，无格式漂移；
- `Dockerfile` 构建上下文与 `docker compose` 启动验证通过（健康检查 `healthz` 返回预期）；
- `.github/workflows/release.yml` 在新路径下可正常执行 `go test` / `go build` 矩阵；
- `//go:embed webui/*` 在新包路径下可正常嵌入，WebUI 静态资源可访问；
- 未引入新的未脱敏日志/指标输出的人工审查。

> 本文档创建时上述验证尚未执行，状态保持“实现待验证”。

## 9. 修订与过期条件（Revision / Expiry）

- **修订触发：** 当实现验证发现 `internal/app` 单包假设导致不可接受的循环依赖、测试覆盖显著下降、或构建/容器姿态需非平凡调整时，或 `AGENTS.md` 中监听平面/管理平面划分发生变更时，必须修订本 ADR。
- **过期条件：** 若自 2026-09-22 起 6 个月内未进入实现验证，或 Go 工具链对 `internal/` 约束语义发生根本变化，则重新评估本决策是否继续有效。
- **版本记录：** 后续修订需在文末追加修订历史，保留原始决策与变更理由。

---

**附注：** 本 ADR 仅记录已稳定的结构方向，所有子包拆分、接口抽象与依赖注入等设计均显式标记为暂缓，不得视为已接受决策。与 ADR 0001（统一会话恢复）的行为域正交，互不影响。
