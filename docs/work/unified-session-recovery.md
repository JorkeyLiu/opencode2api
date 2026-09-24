# Unified session recovery — final state

> Final state. History lives in git log and `docs/adr/0001-unified-session-recovery.md`. No remaining route.

## Outcome complete

统一会话恢复已定版并落地完毕，无剩余路线。详细矩阵正典为 `docs/adr/0001` 与 `internal/app/recovery.go`/`gateway.go`/`scheduler.go`；本文件不再承载 live 策略。

## Current authority

- **Single-candidate authority**（`recovery.go`）：`classifyStableCause` + `decideCandidateRecovery`（`cause + lane context -> typed action`）+ `recoverSingleCandidate`（`initial send → 有界 L1 observation → exact400 一次同目标 corrective replay 终态`）。四 walker（`doPinnedAnonymous`/`doPinnedAuth`/`doAnonymousUpstream`/`doKeyUpstream`）均消费该 runner，旧重复决策链已删。
- **Domain authority**（`recovery.go`）：`decideDomainRecovery`（`domainRecoveryInput{Recovered400,Cancelled,Committed,UnboundDomains, Pinned}` → `domainRecoveryResult{Kind: domainNone|domainUnboundExhausted|domainPinnedFullLive429|domainPinnedConsumption, AllowCustom}`），统一接管全部 `fallback/exhaustion` 判定（原 `customTakeoverEligible`/`pinnedConsumptionAllowCustom`/`unboundDomainExhausted`/`unboundDomainsExhaustedAllowCustom` 已删，叶分类 `finalNon429ObjectUnavailable`/`unboundObjectUnavailable` 保留）。未绑定外层与双 pinned 游走器已迁移至该权威，仅保留 `draining/ownership/scheduler writes/pin fencing/request-local evidence/custom` 调用。
- **L1/L2/L3 闭环完整**：L1 同目标有界观察（`true transport/408/425/500-599` 含 `503`/`stream-startup`，`retry.max_attempts` 含首次，`100ms` 下限，`interval`/`Retry-After` 取 max，无指数/jitter）、L2 `400` 一次同目标 corrective replay 后 route-terminal、未绑定/已绑定双域独立耗尽与双 pinned 证明（`full live429` 与 `bounded consumption` 不合并）、L3 取消/deadline/committed 立即停止与忠实返回；无第二判定，无广义 pending。

## No remaining route

无待实现 helper、无 pending 矩阵；后续变更以 `AGENTS.md` §3/§4 与 ADR 为合同，任何偏离须先修订 ADR。

## Constraints

- Binding: `AGENTS.md` §3/§4 + `docs/adr/0001`. On conflict they win.

## Verification

- Gate: `go test ./... -count=1`, `go build -o opencode2api ./cmd/opencode2api`, `gofmt`, `git diff --check`.
