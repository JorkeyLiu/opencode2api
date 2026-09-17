# opencode2api

`opencode2api` 是一个使用 Go 编写的 OpenCode Zen 协议代理。它对外提供标准 OpenAI 与 Anthropic API，并自动添加 OpenCode 客户端请求头。

主要功能：

- 支持 OpenAI Chat Completions、Responses 和 Models API
- 支持 Anthropic Messages API
- 支持普通响应和 SSE 流式响应
- 支持文本、图片、thinking/reasoning、工具定义、工具调用和工具结果转换
- 配置单个 Zen key 列表（`keys`）
- 支持无需上游 key 的 Zen 匿名模式，免费模型先走匿名通道，失败后进入认证通道（单个 Zen lane）
- 按配置周期同步 Zen `/v1/models`，并从 OpenCode `models.opencode.ai/api.json` 自动同步原生协议与不支持模型；不把模型 ID 硬编码在程序中
- 每 24 小时从 models.dev 更新 OpenCode 成本与弃用信息；models.dev 零成本或名称含 `free` 任一条件即可判定免费
- 认证通道为单个 Zen lane：同一 credential+pool 按确定性软亲和稳定首选同一代理
- 支持直连、HTTP、HTTPS、SOCKS5 和 SOCKS5H 代理
- 支持从文本文件读取代理池，并与配置内的代理合并、去重
- `config.json` 支持 `//` 和 `/* ... */` 注释
- 将 key 与代理组织为统一的 credential×proxy×model target 调度，同一 credential+pool 按确定性软亲和稳定首选同一代理，无静态绑定、无持久映射
- 使用稳定客户端会话建立一次 target 绑定：未绑定建立时对完整 target 做 HRW 排序并按冻结顺序 fallback；匿名会话+模型固定到含代理的完整 target，认证会话+模型固定到 tier/凭证/池/模型/协议/上游地址（代理为带代际 fencing 的当前选择，可在同池同凭证同模型同协议同地址内移动）；首次成功后不再跨 Tier/跨凭证/跨池 fallback，也不再从匿名升级到认证；绑定后失败按目标协议本地失败，避免把已建立上下文换 target 重放
- 代理传输故障只影响传输健康；401 按 tier+凭证全局冷却，429 按（通道，代理池，代理节点）全局冷却（匿名与认证共享 Zen 通道；同 URL 不同池隔离），同一 tier+凭证在一次请求内两个不同代理先后 429 才额外冷却该凭证（取第二个 Retry-After），单个 429 不冷却凭证；403/5xx 只冷却单个 (tier, credential, pool, proxy, model) target，且仅在同凭证另有节点成功作比较时才写入通道级 403/5xx 冷却，否则仅展示；408/425/400 与普通 4xx 中性
- 根据真实上游流量识别代理传输故障，并每 15 分钟通过 Cloudflare trace 并行复查异常代理
- 为不同会话生成不同的 OpenCode 会话 ID，并支持 `x-opencode-session`、`x-session-id` 和 `conversation-id` 显式指定会话
- 内置独立端口 Field Manual WebUI，可管理配置、查看 Token/上游指标、诊断路由、运行三协议 Playground 与订阅实时日志
- WebUI 使用账号密码、服务端 session、HttpOnly Cookie、CSRF 与登录限速保护
- WebUI 保存后原子写入配置并热切换 Gateway；无效配置不会影响当前流量
- stdout 输出结构化 JSON 日志；请求、Token、上游尝试与最近一小时滚动指标仅保存在进程内存中

## API 路径

| 方法 | 路径 | 协议 |
| --- | --- | --- |
| `GET` | `/v1/models` | OpenAI 模型列表 |
| `POST` | `/v1/chat/completions` | OpenAI Chat Completions |
| `POST` | `/v1/responses` | OpenAI Responses |
| `POST` | `/v1/messages` | Anthropic Messages |
| `GET` | `/healthz` | 健康检查 |

`/healthz` 无需 API key，返回服务版本以及模型目录、认证 key、匿名开关和代理池的汇总状态，不会暴露 key 或代理地址。模型目录尚未完成首次刷新、没有可暴露模型、没有健康代理或全局可用路由为零（`routing.channels_available==0`）时返回 HTTP `503`；使用过期磁盘快取时仍返回 `200`，但 `models.status` 为 `stale`。新增 `routing` 对象为 additive 全局路由可用性（匿名可用性、认证可用 credential 数、冷却 credential 数、可用通道数：匿名与认证两个通道，均为 Zen 上游），只读全局 401 冷却与代理传输健康，不读取单模型 target 冷却、代理 429 限流冷却、通道可用性冷却与凭证 429 冷却；单模型 target 全部冷却、单代理 429/通道冷却或单凭证 429 冷却不会触发全局 `503`，原有字段保持不变。

模型目录的过期阈值为 `models.refresh_seconds` 的两倍，且不低于 60 秒。刚启动时短暂返回 `503 starting` 属于正常现象，模型列表首次刷新成功后会变为 `200 ok`。

健康检查不会计入请求指标。它也不会触发 models.dev 刷新或写入任何监控记录。

## WebUI

示例配置会在独立的 `8081` 端口启动管理界面：

```text
http://服务器地址:8081
```

首次账号为 `admin`，密码来自 `webui.password`。服务第一次成功启动时会使用 Argon2id 将密码转换为带盐哈希，写入 `webui.password_hash`，并从配置中删除明文密码。请在首次登录后立即修改示例密码。

Field Manual WebUI 包含运行桌面、六步首次运行检查、接入手册、Token 用量、三协议 Playground、路由诊断、配置、事件日志和账号安全页面。接入手册会生成 Chat、Responses、Anthropic、Python 和 JavaScript 示例，但始终使用 `YOUR_API_KEY` 占位符，不把真实 Server Key 写入页面。

Token 页面展示用量覆盖率、每分钟趋势、模型排行与 Tier 分布。诊断页展示 models.dev 状态、模型原生协议与匿名判断来源、Key/代理状态（资源行为实际探测状态：精确 HTTP 200 为成功，429 保持 429，其他 HTTP/传输/no_model/unconfigured 保持实际观察）、逐次上游尝试以及最近一次 Playground 追踪；实时日志通过 SSE 推送。所有动态管理数据都以 DOM 文本节点渲染。

健康页为统一视图：全局就绪、`代理可用性`（传输连通性与匿名/认证通道合一，同一 URL 在不同池为独立成员，仅视觉分组不合并状态；匿名与认证按通道分别展示实际探测状态，匿名 403/5xx 与限流并入匿名列与原因）、`备用模型渠道可用性`（全部自定义渠道真实最小推理，不写调度）、`凭证可用性`（按通道的密钥状态），以及`代理限流冷却`、`通道可用性冷却`、`活跃目标冷却`三张冷却表，不再有独立匿名目标表。`代理可用性`页提供`批量检测`按钮，调用下述可用性接口；未被引用的暂存池不运行、不可检测。顶栏指标为 `Active`（活跃请求）与 `Streaming`（活跃流）。

自定义 session fallback（OpenAI-compatible chat/responses）：配置 `fallback: {"active":"id","channels":[{"id":"c1","name":"c1","base_url":"https://...","api_key":"...","model":"...","protocol":"chat","reasoning_effort":""}]}`，`id` 为稳定机器身份（严格 1-64 `[A-Za-z0-9_.-]`、唯一），`name` 为展示文本（空格如 `OpenCode Go` 允许，仅拒绝空/超长/控制字符），展示名唯一，active 可空、非空必须引用现有渠道 ID（旧名自动规范化为 ID），URL 仅 http/https；旧无 `id` 配置按展示名确定性派生 ID 并规范化保存；`protocol` 为 `chat`（Chat Completions，缺省）或 `responses`（Responses），缺省旧配置规范化为 `chat`，其他值严格拒绝；`reasoning_effort` 为 `""`（供应商默认：转换后剥离目标强度控制，由供应商默认，缺省）、`"inherit"`（继承转换后请求强度，无强度则不注入）、`low`、`medium` 或 `high`，其他值严格拒绝，不进入接管绑定身份、不参与可用性探测。base 支持 host、host/v1 与所选协议一致的完整 endpoint；若 base 带有另一协议的完整 endpoint，按统一 API root 识别后依所选协议生成 endpoint，不产生重复坏路径（chat→`{root}/v1/chat/completions`，responses→`{root}/v1/responses`；模型发现对 host、/v1、/v1/chat/completions、/v1/responses 一律生成 `{root}/v1/models`）。仅当请求进入时已存在匿名 session+model pin 且在 pinned 匿名路径得 HTTP 429（含本地 proxy429 冷却 429）时触发；未绑定匿名 429 与认证 pin 429 不触发。无 active 时保持原 429；有 active 时当次经当时 active 渠道重试（客户端入口按渠道协议统一转换并改写为渠道模型与渠道思考强度：`inherit` 保留转换强度，`""` 剥离目标强度（chat 删除顶层强度字段、responses 删除目标 reasoning 控制，历史/内容不动），显式 low/medium/high 覆盖：chat 设置 `reasoning_effort`，responses 合并/新建 `reasoning:{effort}` 并保留其他键，经对应 endpoint Bearer 发送，不依赖供应商会话亲和，每次带完整历史；响应/流按渠道协议转回客户端协议；自定义模型不进公开 `/v1/models`；不碰 Zen 调度冷却），且无论成功与否先将会话固化到该完整渠道身份（id+规范化 base URL/authority+key 指纹/身份+配置模型+protocol，展示名仅快照、不参与匹配，重命名不失效），此后该会话仅走该固化渠道、不随 active 切换漂移，协议变更后旧会话本地 502；自定义错误原样返回不回退。接管键为 session，有界 4096 first-wins，容量满本地 502；热 Apply 无过滤迁移为 tombstone，删除/身份（含协议）变化后旧会话本地 502；进程期、无持久化/投影/过期，重启清空。WebUI 支持增删/改渠道（含协议选择：Chat Completions/Responses，每渠道思考强度：供应商默认/继承自请求/Low/Medium/High，API 密钥为文本输入+视觉遮蔽、autocomplete=off，非密码提交）、模型仅从下拉选择（无自由文本，已有模型预置，发现后保留或切换）并一键激活，激活经配置保存/应用持久化（新渠道生成稳定 ID，展示名重命名不漂移绑定）；模型发现失败保持 `discover_failed` code 并附加安全结构化 `reason`（dns_error/connect_refused/timeout/tls_error/transport_error/non_2xx/invalid_json/empty_list）、`endpoint`、`http_status`、`elapsed_ms`，不返回上游 body/key/Authorization 或原始错误全文。观测通道键为 `custom:<id>`（旧 `custom` 合并行保留，展示取 `name`）。

监控、上游尝试、最近 Playground 结果和日志仅保存在内存。**进程重启会清空全部内存监控与诊断历史，包括 lifetime 累计。** stdout 日志仍可由 Docker 或日志平台收集；模型目录快取与 models.dev 价格快取是独立的磁盘兼容资料，不属于监控历史。脱敏持久历史（`history`）独立于内存统计，重启后仍可查询 24h / 7d 请求、attempt 与分钟趋势。

### 监控字段

登录 WebUI 后，`GET /api/monitor` 返回以下顶层字段；该管理 API 只在 `webui.listen` 上提供，并受 Session 保护：

| 字段 | 内容 |
| --- | --- |
| `version` | 当前程序版本。 |
| `metrics` | 原有请求统计：进程启动时间、Active/Streaming（活跃请求/流）、lifetime 与最近一小时成功率/延迟、端点/模型/Tier/状态码聚合，以及 60 个每分钟序列点。 |
| `usage` | Token 的 `lifetime` 与 `last_hour` 统计。包含 `requests`、`reported`、`coverage`、总 Token 和按模型/Tier 聚合；另有 additive `channels` 按通道聚合（自定义渠道为 `custom:<渠道ID>`，展示取渠道 `name`，旧 `custom` 合并行保留，`tiers.custom` 仍为总量）。 |
| `upstream` | 上游请求级路由 `requests`、尝试级 `lifetime`、`last_hour` 与 `recent`。按 Tier、匿名/Key/自定义通道、Key 尾码聚合；自定义 Tier 保持 `custom`，通道为 `custom:<渠道ID>`（`channels` 按此分裂，`tiers` 仍聚合）。 |
| `resources` | 模型目录、Key 冷却、脱敏代理节点、匿名开关和 models.dev metadata 状态。 |

`usage.*.tokens` 与模型/Tier 项均包含 `input_tokens`、`output_tokens`、`cached_tokens`、`reasoning_tokens`、`total_tokens`。其中 `input_tokens` 统一表示包含缓存读写的总输入，`cached_tokens` 单独表示缓存读取量。`metrics.series` 的每分钟点也包含这些 Token 字段和 `usage_reported`。普通 JSON、同协议 SSE 与跨协议 SSE 响应都会解析上游 usage；`coverage` 是收到 usage 的推理请求数除以已建立上游路由的推理请求数。上游未提供 usage 时不会估算 Token。

每个 `upstream.requests` 项对应一个已完成的推理请求，包含最终实际使用的 Tier、通道（自定义为 `custom:<渠道ID>`，展示取渠道 `name`，Tier 仍为 `custom`，密钥显示仍为“自定义”）、Key 尾码（或 `anonymous`）、尝试次数、HTTP 状态、耗时、成功标记和结果分类。每个 `upstream.recent` 项则对应一次上游尝试，包含时间、Request ID、模型、Tier、尝试序号、匿名标记、通道、Key 尾码、`proxy_node`、HTTP 状态、耗时、成功标记和结果分类。`metrics.channels` 为最近一小时按通道调用数；历史 `channel=custom` 兼容匹配全部自定义前缀，`channel=custom:<ID>` 精确匹配指定渠道，旧 `custom` 记录仍归旧合并行。真实 Key 在日志和 WebUI 中只显示最后 5 个字符；配置接口的内部 secret ID 仍使用 SHA-256 稳定指纹。代理 URL 的认证信息会被移除，字段名称明确为代理节点而非出口 IP。

lifetime 从当前进程启动开始；last hour 使用 60 个一分钟 Bucket。进程内固定保留最近 10,000 个请求路由和 20,000 次上游尝试，管理响应最多返回其中最近 500 个，WebUI 默认各显示最后 100 条。`resources.models` 额外显示模型目录来源（`none`、`disk` 或 `live`）及 `stale` 状态。数据不会写入配置、metadata 快取或其他数据库。

### 持久历史

`history` 为轻量内嵌脱敏投影（标准库 NDJSON，无新依赖、无独立部署），只保存请求/attempt/分钟元数据，不保存 body、headers、query、session、完整 key、指纹、原始代理 URL、错误文本或 Playground 载荷。目录为空时为配置目录下 `history`，相对路径相对配置目录，绝对路径允许；目录 `0700`、文件 `0600`。分文件 `requests-YYYYMMDD-NNN.ndjson`、`attempts-…`、`minutes-…`，单段 32MB 或跨 UTC 日轮转；按 `retention_days`（1–90）与 `max_bytes_mb`（16–2048）清理最老已封段，当前写入段不删。初始化/写入/读取失败只 warn 并退化为 memory-only，不影响 Gateway 启动、Apply、healthz 或推理；`history` 热生效。`GET /api/monitor` 增加 `history` 状态（`enabled_config/active/directory[basename]/retention_days/max_bytes_mb/dropped/gap/last_error/oldest_at/newest_at/size_bytes`）。

历史查询（管理 Session，`no-store`，GET 无 CSRF，每 IP 每分钟 30 次，单请求 5s 超时）：

| 方法 | 路径 | 默认范围 |
| --- | --- | --- |
| `GET` | `/api/history/requests?from=&to=&limit=&cursor=&model=&tier=&channel=&success=&proxy_pool=` | 24h，limit 100 cap 200 |
| `GET` | `/api/history/attempts?…&request_id=&failure_class=` | 24h，limit 100 cap 200 |
| `GET` | `/api/history/series?from=&to=&limit=&cursor=` | 7d，limit/cap 10080 |
| `GET` | `/api/history/proxy-stats?from=&to=&limit=` | 24h，limit 200 cap 500 |
| `GET` | `/api/history/usage-aggregate?from=&to=&limit=` | 24h，limit 200 cap 500 |

范围上限为 `retention_days`；`cursor` 为不透明 base64（稳定倒序分页）；禁用时返回 200 空 items 与 `active:false`；参数错 400。`usage-aggregate` 单遍扫描所选范围的 `requests-*.ndjson`，按模型与上游聚合（上游为 `tier`，`tier=custom` 时为完整渠道：旧 `custom` 保留合并行，新 `custom:<ID>` 分行）：每项含 `calls`（范围内全部请求数）、`usage_calls`（已上报数）与仅已上报累计的 `input/output/cached/reasoning/total_tokens`，另有行级 `total_complete`/`reasoning_complete`（任一已上报旧行缺 `usage_detail_complete` 时该行记 `false`，不从 input+output 合成总量；全完整行含合法零记 `true`，无已上报行亦记 `true`），按 `total_tokens` 倒序、limit 截断并返回 `total_models/total_upstreams/truncated/gap/dropped/active/last_error/from/to`；旧历史用量行缺 `reasoning/total` 时计零且置全局 `legacy_incomplete:true`，行级总量/推理展示为 `—` 而非数字 0 或部分和，不做估算，新请求已持久化真实上游上报的 `reasoning_tokens`/`total_tokens`（additive v1，旧行仍可读）。WebUI 使用统计页提供`今天|最近 24 小时|最近 7 天|本月`切换，按模型/按上游表均来自所选范围的 `usage-aggregate`（不再使用内存最近一小时），行级不完整总量/推理渲染为 `—`，趋势/KPI/代理统计来源不变。

### Playground 与诊断 API

以下接口受管理 Session 保护；POST 还要求当前 CSRF Token，并按客户端每分钟最多 12 次限制：

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/api/debug/models` | 模型路由、原生协议、协议来源、Zen 可用性、匿名资格/来源、成本与 metadata 状态。 |
| `POST` | `/api/debug/inference` | 通过真实 Gateway 发起 Chat、Responses 或 Anthropic 非流式诊断请求。 |

请求格式：

```json
{
  "protocol": "chat",
  "request": {
    "model": "example-model",
    "messages": [{"role": "user", "content": "Hello"}]
  }
}
```

`protocol` 可为 `chat`、`responses` 或 `anthropic`。服务端无条件把内层 `stream` 改为 `false`。诊断结果统一包含 `ok`、真实 `http_status`、`duration_ms`、`request_id`、`route` 和原始协议 `response`。一旦诊断调用已执行，管理接口本身返回 HTTP `200`，即使上游结果是 4xx/5xx；因此错误正文、路由和 Request ID 仍可一起查看。外层请求格式、Session、CSRF 或限速错误仍使用相应管理 HTTP 状态。

诊断响应不会包含本地 Server Key、上游 Key、Cookie、密码、Authorization 或代理凭据；响应中同名敏感字段及已配置敏感值会在返回浏览器前清除。最近一次结果只保存在当前管理进程内。

### 管理探针与手动刷新

以下接口同样只在 `webui.listen` 上提供，受管理 Session 与 CSRF（含 Origin 校验）保护，响应禁止缓存：

| 方法 | 路径 | 用途与限速 |
| --- | --- | --- |
| `POST` | `/api/proxies/probe` | 按具名 pool 与池内 index 探测单个 proxy 的传输健康；每客户端每分钟最多 10 次。 |
| `POST` | `/api/availability/check` | 批量可用性检测（全部渠道真实最小推理）：原生按目录实际可服务模型与原生协议 POST `stream:false` 约 1 token，自定义渠道按其配置协议（Chat Completions/Responses）用配置模型测对应 endpoint；请求体必须为严格空对象 `{}`；每客户端每分钟最多 3 次，同时只允许一次运行（忙时 `409`）。 |
| `POST` | `/api/fallback/discover` | 自定义渠道模型发现（GET `{root}/v1/models` 仅下拉，host//v1/完整推理 endpoint 统一归一）：支持未保存新 base_url+明文 key 与已保存 masked secret id；限流 10/分钟，限大小/超时，验证模型 id，`no-store` 不泄露 key；失败保持 `discover_failed` 并附加 `reason`/`endpoint`/`http_status`/`elapsed_ms` 安全分类。 |
| `POST` | `/api/models/refresh` | 手动刷新模型目录（`catalog`）、models.dev metadata（`metadata`）或两者（`all`）；每客户端每分钟最多 3 次。 |

请求格式（严格 JSON，不接受多余字段）：

```json
{"pool": "shared", "index": 0}
{"scope": "all"}
```

 `scope` 只能是 `catalog`、`metadata` 或 `all`。探针只接受已被 anonymous/authenticated 引用的运行时池与合法池内 index，不接受 URL 或敏感值，因此无法被指向任意目标；未被引用的暂存池不可探测（`unknown_pool`）。探针是显式的管理健康动作：它可以改变该 proxy 的传输健康（`healthy`），但绝不读写 credential / target / 代理限流调度状态，也不清理或设置 429 冷却；WebUI 资源页每行 proxy 的“探测”按钮即调用此接口，目录/metadata 快照旁的“刷新目录 / 刷新 metadata / 全部刷新”按钮调用刷新接口。手动刷新复用定时刷新的无状态逻辑（失败保留旧快照），不触碰 proxy 健康/检查状态与前台 credential / target / 代理限流冷却；与定时刷新共享去重门，所需组件正忙时返回 HTTP `409` 而不叠加工作。批量检测对活跃节点发真实最小推理（目录驱动选模型：匿名选免费且 Zen 可服务，认证选 Zen 实际可服务；无模型时 `no_model`/`inconclusive`，不用 `/v1/models` 冒充成功；模型发现仍可用 GET 但绝不作为可用性判定）：匿名探测只影响匿名通道、从不影响认证与配置凭证；真实凭证 401 冷却该凭证；成功仅清理通道/限流（带陈旧发送保护），成功+429 写代理限流、成功+403/5xx 写通道可用性，无比较成功时 403/5xx 与单个 429 仅展示不写入，无比较成功的双 429 写凭证限流；传输不定性、408/425、普通 4xx、解析/空结果与超时/取消仅展示；单次与整批超时只用于诊断。全部自定义渠道在同一响应内按各自协议真实探测（配置模型+对应 endpoint 最小请求），结果不写 Zen 调度与传输健康、不记推理指标/历史。探针与刷新统一返回 HTTP `200` 并内嵌结果（`result` / `refreshed` 与快照摘要，不含全量模型列表）；参数错误返回相应 `4xx`。成功与失败分别以 `info` / `warn` 记录完成事件，错误文本经过脱敏。

## 编译

需要 Go 1.24 或更高版本。

```bash
go build -o opencode2api ./
```

## 下载

预编译的 Windows、Linux 和 macOS 可执行文件可从 [GitHub Releases](https://github.com/JorkeyLiu/opencode2api/releases) 下载。

## GHCR / Docker Compose 部署

正式镜像发布在 `ghcr.io/jorkeyliu/opencode2api`。

- 推送到 `jorkey/integration` 会自动构建并发布不可变 `sha-<short>` 与滚动 `dev`（同一多架构 digest，可手动 `workflow_dispatch` 补构建）。
- `latest` 只代表手动 promotion：从已验证的 `sha-*` 按 digest 复制而来，不重构建，不会自动产生。
- `v*` 语义版本 Release 只发布预编译二进制包，不再发布任何 Docker 镜像 tag。
- 生产建议 pin 具体的 `sha-*` 或 `@sha256:` digest；`latest`/`dev` 只用于愿意跟随浮动的跟踪环境。


```bash
git clone https://github.com/JorkeyLiu/opencode2api.git
cd opencode2api
cp config.example.json config.json
# 编辑 server_keys、keys，并修改 webui.password
docker compose pull
docker compose up -d
```

首次启动会把 `config.json` 导入 `opencode2api-state` 命名卷。之后应通过 WebUI 修改配置；如需再次从宿主机导入配置，可执行：

```bash
docker compose cp config.json opencode2api:/var/lib/opencode2api/config.json
docker compose restart
```

健康检查、WebUI 和日志：

```bash
curl http://127.0.0.1:8080/healthz
# 浏览器打开 http://127.0.0.1:8081
docker compose logs -f
```

可通过环境变量固定镜像版本和修改宿主机端口（生产请使用已验证的 `sha-*`，`latest` 只是最近一次手动 promotion 的可用版本）：

```bash
OPENCODE2API_VERSION=sha-a1b2c3d OPENCODE2API_PORT=18080 OPENCODE2API_WEBUI_PORT=18081 docker compose up -d
```

Compose 默认使用 `latest` 并始终 `pull_policy: always` 拉取；生产如需完全 pin，可把 `image:` 改写为 `ghcr.io/jorkeyliu/opencode2api:sha-a1b2c3d` 或 `ghcr.io/jorkeyliu/opencode2api@sha256:<digest>`。

不使用 Compose 时也可直接运行 GHCR 镜像（示例为 `latest`，生产请换成已验证的 `sha-*` 或 `@sha256:` digest）：

```bash
docker volume create opencode2api-state
docker run -d --name opencode2api --restart unless-stopped \
  -p 8080:8080 -p 8081:8081 \
  -e CONFIG_SEED_PATH=/run/config/opencode2api.json \
  -v "$(pwd)/config.json:/run/config/opencode2api.json:ro" \
  -v opencode2api-state:/var/lib/opencode2api \
  ghcr.io/jorkeyliu/opencode2api:latest
```

## 配置

复制示例配置：

```bash
cp config.example.json config.json
```

然后编辑 `config.json`：

```json
{
  "listen": "127.0.0.1:8080",
  "server_keys": ["change-this-local-key"],
  "keys": ["sk-your-zen-key"],
  "anonymous": false,
  "proxy_pools": {
    "shared": {
      "proxies": ["direct"],
      "proxyfile": ""
    }
  },
  "proxy_routing": {
    "anonymous": "shared",
    "authenticated": "shared"
  },
  "upstream": {
    "zen": "https://opencode.ai/zen"
  },
  "retry": {
    "max_attempts": 3,
    "timeout_seconds": 300
  },
  "models": {
    "refresh_seconds": 300,
    "protocols": {}
  },
  "performance": {
    "max_idle_conns": 2048,
    "max_idle_conns_per_host": 256,
    "max_conns_per_host": 0,
    "idle_conn_timeout_seconds": 120,
    "connect_timeout_seconds": 5,
    "failure_cooldown_seconds": 15,
    "rate_limit_cooldown_seconds": 300
  },
  "logging": {
    "level": "info",
    "ring_size": 2000
  },
  "history": {
    "enabled": true,
    "directory": "",
    "retention_days": 7,
    "max_bytes_mb": 128
  },
  "webui": {
    "enabled": true,
    "listen": "0.0.0.0:8081",
    "username": "admin",
    "password": "change-this-admin-password",
    "session_ttl_minutes": 720
  }
}
```

### 基础字段

| 字段 | 含义 |
| --- | --- |
| `listen` | 本地监听地址。默认建议使用 `127.0.0.1:8080`，避免服务直接暴露到公网。 |
| `server_keys` | 调用本代理时使用的本地 API key 列表。它们只用于本地鉴权，不会发送给 OpenCode。 |
| `keys` | OpenCode Zen API key 列表。允许配置多个 key。 |
| `anonymous` | 是否启用 Zen 匿名模式，默认 `false`。models.dev 判定为零成本，或模型名称包含 `free`，任一条件成立即可进入匿名通道。 |
| `proxy_pools` | 具名代理池映射。每个 pool 有 `proxies`（上游代理列表，支持 `direct`、`http://`、`https://`、`socks5://` 和 `socks5h://`，URL 可含认证信息）与 `proxyfile`（可选代理文件路径，相对路径以 `config.json` 所在目录为基准）。pool 名是稳定 identity，只能使用字母、数字、`_`、`-`、`.`（1–64 字符），禁止用 IP 或 URL 命名，`direct` 为保留字。 |
| `proxy_routing` | `anonymous` / `authenticated` 两个通道各引用一个已存在的 pool 名（均为 Zen 上游），允许相同或不同。`anonymous: false` 只关闭匿名凭证，两个引用仍必须全部合法。未被 anonymous/authenticated 引用的池为暂存配置，不运行、不可探测、不计容量；仅被引用的池为运行时资源（构建传输、参与健康与容量）。暂存池仍完整校验，可随时被路由引用后热生效。 |

`server_keys` 至少需要一个值。`anonymous` 为 `false` 时，`keys` 不能为空；启用匿名模式后上游 key 列表可以为空。

### Zen 匿名模式

OpenCode 客户端在没有配置 Zen key 时使用固定的 `public` 凭证；Zen 服务端将它转换为匿名请求，并按出口 IP 对允许匿名访问的模型限流。本项目使用相同协议：OpenAI/Responses 上游请求发送 `Authorization: Bearer public`，Anthropic 上游请求发送 `x-api-key: public`。

启用 `anonymous` 后，以下任一条件成立即视为免费模型：

1. models.dev 已知输入、输出成本都为 `0`，且模型未弃用；不要求名称包含 `free`。
2. 模型 ID 大小写不敏感地包含 `free`；即使 metadata 尚未就绪、缺少该模型或显示为付费，也仍按名称条件视为免费。

免费模型先走匿名 Zen，非免费模型完全跳过匿名通道。未绑定建立阶段候选分散与 fallback 由冻结顺序负责（HRW/round-robin 决定 target 顺序，fallback 按顺序走下一个）；同 target retry 只负责临时性验证，从不负责分散。匿名通道每个可用 target 最多一次 fallback 发送，整个通道另有唯一共享 transient token：首次传输错误、408/425 或 5xx 在同一 target、同 Route Session、同 body 上追加一次 transient retry；401/403/429 无同 target retry，直接按冻结顺序 fallback，其中 429 按代理池 + 代理节点全局冷却并过滤所有模型/凭证的后续候选；普通 4xx（404/422 等，排除 400/401/403/429/408/425）直接结束匿名通道（不再扫剩余代理），随后可进入认证 Key 阶段。任意 candidate 首次精确 HTTP 400 走专用恢复：同一 candidate 轮换 Route Session 后精确重放一次（同 request ID、attempt +1、同 target/协议/credential/proxy，始终在向客户端写任何字节之前；Responses 重放同时清理 `previous_response_id` 与 `input[]` 中的 `reasoning` 旧引用；不消耗 transient token），重放结果即整条路由最终结果，不再扫剩余代理、不进入认证 Tier；该重放是同 target 陈旧会话/引用恢复，不是换 target 的许可，重放成功若仍未绑定则建立会话绑定。anonymous 不受 `retry.max_attempts` 截断。认证 lane 使用独立 transient token 与真实发送预算；精确 400 同样单次重放且终结整条路由，普通 4xx 结束当前 lane（无跨 Tier 回退）。已绑定会话 pin 永不跨 target：429 冷却期间本地返回目标协议 429 + 剩余 Retry-After，不发送不 fallback，过期后仍只发原 pin 代理。客户端取消或请求总 deadline 立即终止整条路由，不再 retry/fallback，不改变任何状态。监控中的 `proxy_node` 表示所选代理节点、`proxy_pool` 表示其所属池，两者都不代表或推断实际出口 IP。

只有匿名通道、且 `keys` 为空时，`/v1/models` 只展示按上述规则可匿名使用的模型。只要配置了任一真实上游 Key，模型列表仍展示该 Key 路由可用的完整模型集合。

models.dev 使用固定 30 秒超时，每 24 小时刷新一次。标准地址为 `https://models.dev/api.json`；规范化后的 OpenCode 模型成本缓存在 `config.json.models.dev.json`，以 `0600` 权限和同目录临时文件原子替换。启动会先读取可用快取，再尝试联网更新；更新失败不会丢弃旧资料，错误、更新时间与过期状态可在监控资源和诊断页查看。

### key、代理与 target 调度规则

代理以具名池组织，`proxy_routing` 决定 anonymous / authenticated 各自使用哪个池（均为 Zen 上游）。相同池名引用共享同一传输池实例，不同池名即使 URL 相同也完全隔离运行时状态：认证 credential 只使用 `proxy_routing.authenticated` 指向的池，匿名凭证只走 `.anonymous`；隔离模式下各通道的候选互不相见。key 与代理之间不存在静态绑定：同一 credential+pool 按确定性软亲和（类似稳定用户/VPN IP 偏好，按当前池内容 recompute）稳定首选同一代理，不同池独立选择，池内容变化只做最小扰动。

未绑定建立时每次请求按 credential×proxy×模型展开 target 候选并冻结顺序：匿名按会话对完整 target 做 HRW（Rendezvous）降序排列，保证同会话同模型同资源下顺序稳定；认证按“凭证组会话 HRW + 组内代理软亲和”排序，保证同凭证+池稳定首选同一代理、会话分散到不同凭证；无会话的后台路径使用原子 round-robin 起始偏移，不使用随机。分散与 fallback 只由该冻结顺序决定，同请求内不重排；同 target retry 永远不改变顺序，只在同一 target 上做临时性验证。同一会话+模型首次上游成功（含精确 400 同 target 重放成功）后即建立绑定：匿名绑定到含代理的完整 target，之后只用该 target；认证绑定到 tier/凭证/池/模型/协议/上游地址（代理为带代际 fencing 的当前选择），之后不再跨 Tier/跨凭证/跨池，也不再从匿名升级到认证；已建立认证可在同池同凭证同模型同协议同地址内移动：仅在传输失败（走完同 target transient retry 后、仍在发送预算内）或 HTTP 429 后、向客户端写任何字节之前尝试同池下一个健康代理，400/401/403/408/425/普通 4xx/5xx 从不移动，成功则只更新当前代理；target 侧的上游状态（prompt 缓存、Responses/reasoning 引用、路由状态）未必可跨 target 携带，因此网关取 fail-closed 防御兼容：绑定后其他失败按目标协议本地返回，而不是把已建立上下文换 target 重放。绑定为进程生命周期，不过期不淘汰，有界存储，容量满时新会话在发送前本地 502，已有绑定继续服务；进程重启清空全部绑定并重新建立。

状态分层：单次前台传输错误为中性，不写 target/credential/限流冷却，也不会立即因 timeout/refused 将 proxy 标 unhealthy；它只触发现有异步中性 proxy 健康 verification，只有该独立 probe 明确得到连通性失败（timeout/refused 等）时才允许翻转 proxy healthy。HTTP 状态从不直接改变 proxy 健康；HTTP 401 按 tier+凭证全局冷却该 credential；HTTP 429 按（通道，代理池，代理原始身份）全局冷却该通道限定代理（跨模型/凭证/匿名/认证通道与客户端会话共享，匿名与认证共享 Zen 通道；同 URL 不同池隔离），未绑定建立时直接过滤该代理的全部候选，匿名已绑定 pin 在冷却期间本地返回目标协议 429 + 剩余 Retry-After（不发送不 fallback），认证已绑定则在预算与候选允许时尝试同池下一代理；同一 tier+凭证在一次请求内两个不同代理先后 429 才额外冷却该凭证（取第二个 Retry-After），单个代理 429 从不冷却凭证；403/5xx 只冷却命中的单个 (tier, credential, pool, proxy, model) target，同 credential 同 proxy 的其他模型不受影响，且仅在同 tier+凭证另有节点成功作比较时才写入（通道，池，代理）通道可用性冷却，无比较成功的单个 403/5xx 仅展示不写入通道层；408/425 为 transient 中性（可 retry/fallback，不冷却）；普通 4xx（含精确 400 与 404/422）中性，既不冷却也不清理已有状态（其中精确 400 另有同 target 单次会话重放语义，见上）；2xx 只清理本 target 与本 credential 的 401 状态，并仅当本次发送起始时间不早于最新失败时清理本通道代理的 429/通道状态与本凭证的 429 状态（已在途中早发的 2xx 不得清除更新的冷却，多个更新的失败保持权威）。Route Session override 是内存 Gateway authority 的一部分（首代无状态派生，仅 400 轮换后存储，有界、idle TTL、确定性淘汰；认证 scope 与代理无关因而移动不换会话值，匿名 scope 含代理；不持久化、不进投影/日志/metrics/history/admin），与会话+模型 target 绑定不同：后者是进程生命周期 durable 绑定，不过期不淘汰，有界 fail-closed。进程重启清空全部内存冷却/override/绑定。冷却按双基准指数退避（确定性 ±20% 抖动）：非 429 用 `performance.failure_cooldown_seconds`、总封顶 5 分钟（`Retry-After` 取更大值同样封顶 5 分钟）；429 proxy/credential 用 `performance.rate_limit_cooldown_seconds` 起算（可配 300..3600，默认 300）、固定 3600s 封顶（`Retry-After` 取 `max(退避, Retry-After)` 后同样 clamp 到 429 最大值）；429 仍只触发中性异步 proxy verification，从不直接改变 healthy。代理限流/通道表有界（与代理资源成比例，过期修剪 + 最老空闲确定性淘汰，全活跃时允许临时超出不断活跃冷却）。下游请求取消不更新任何状态。模型/能力目录刷新使用独立的无状态 key×healthy proxy 遍历，只读 healthy 代理顺序，不读写前台 credential/target/限流/通道/route-session 状态，也不改变 proxy healthy/checking；刷新 context deadline/cancel 只是刷新失败，失败保留旧快照。健康诊断另设“代理限流冷却”表（通道、池、脱敏节点、活跃/失败数/剩余/下次可用/分类/状态，有界截断）与“通道可用性冷却”表（同维，仅比较探测写入）；目标表仅描述按模型 403/5xx，不再解释 429。

共享单池示例（默认，行为与旧版单代理池一致）：

```json
"proxy_pools": {
  "shared": { "proxies": ["direct"], "proxyfile": "" }
},
"proxy_routing": { "anonymous": "shared", "authenticated": "shared" }
```

隔离示例（匿名、认证各走不同出口）：

```json
"proxy_pools": {
  "anon": { "proxies": ["socks5://127.0.0.1:1080"], "proxyfile": "" },
  "zen": { "proxies": ["http://user:password@127.0.0.1:7890"], "proxyfile": "" },
  "go": { "proxies": ["direct"], "proxyfile": "" }
},
"proxy_routing": { "anonymous": "anon", "authenticated": "zen" }
```

池内条目示例：

```json
"proxies": [
  "http://user:password@127.0.0.1:7890",
  "socks5://127.0.0.1:1080"
]
```

也可以从文本文件加载单个池：

```json
"shared": { "proxyfile": "proxies.txt", "proxies": ["direct"] }
```

`proxies.txt` 每行填写一个代理。支持空行、以 `#`、`;` 或 `//` 开头的整行注释，也支持在代理后使用空格加这些标记写行尾注释：

```text
# HTTP 代理
http://user:password@127.0.0.1:7890
socks5://127.0.0.1:1080  # 备用代理
```

每个池的 `proxies` 先加载，随后加载该池的 `proxyfile`，重复项只保留第一次出现的位置。如果两个来源都为空，则该池仍使用 `direct`。`config.json` 本身支持 `//` 单行注释和 `/* ... */` 块注释；引号内的 `https://` 等内容不会被当作注释。

旧版顶层 `proxies` / `proxyfile` 仍可在启动与重载时读取：仅旧字段出现时会自动迁移为 `shared` 池并由三个路由引用（清除旧字段，行为与旧版一致）；完全没有代理字段时同样规范化为 `shared`/`direct`。旧字段与任何新字段同时出现会返回明确的校验错误。保存与 Apply 只写新格式。监控中的 `proxy_node` 表示所选代理节点，`proxy_pool` 表示其所属池，两者都不代表或推断实际出口 IP。

### `upstream`

| 字段 | 含义 |
| --- | --- |
| `upstream.zen` | Zen 上游根地址，通常保持为 `https://opencode.ai/zen`。 |


### `retry`

| 字段 | 含义 |
| --- | --- |
| `retry.max_attempts` | 每个认证 Key Tier 的真实发送预算，含首次发送与同 target transient retry（仅未绑定建立阶段按冻结顺序 fallback 有效）。同一 Tier 内首次传输错误、408/425 或 5xx 共享一次同 target transient retry（同 Route Session、同 body）；401/403/429 无同 target retry，直接按冻结顺序 fallback，其中 429 在记录全局代理限流后 fallback（同池同代理的后续候选已被过滤，未绑定新会话直接过滤该代理全部候选）；普通 4xx 结束当前 Tier。任意 candidate 首次精确 400 走专用 Route Session 重放一次，该次固定额外允许、不受本预算截断，重放结果即路由最终结果、不再走剩余候选或回退另一 Tier，重放成功若仍未绑定则建立绑定。已绑定会话不再跨 target：只保留同 target transient retry 与精确 400 同 target 重放，429/credential/target 冷却按目标协议本地失败。单认证 lane 内无跨 Tier 回退；精确 400 重放终结整条路由。anonymous 不使用此上限，每个可用 target 最多一次 fallback 发送，另加全通道一次 transient retry 与可选一次 400 重放；普通 4xx 直接结束匿名通道。 |
| `retry.timeout_seconds` | 单个客户端请求的总超时时间，同时用于限制上游响应头等待时间。 |

流式响应一旦已经向客户端输出数据，就不会切换节点重新生成，避免拼接两个不同的响应。

### `models`

| 字段 | 含义 |
| --- | --- |
| `models.refresh_seconds` | 重新读取 Zen 模型列表及 OpenCode 能力目录的间隔秒数。模型列表与能力目录会并发刷新。 |
| `models.protocols` | 手动指定模型的原生协议。值只能是 `chat`、`responses` 或 `anthropic`，并覆盖自动同步结果。通常保持为空。 |


模型协议覆盖示例：

```json
"protocols": {
  "custom-model": "chat"
}
```

自动协议来源是 `https://models.opencode.ai/api.json`，并用 OpenCode 官方 Zen endpoint 文档补充具体路径：模型的 `provider.npm`（或 Tier 默认 `npm`）及文档 endpoint 会映射为 OpenAI Responses、Anthropic Messages 或 OpenAI-compatible Chat。模型列表与协议能力会写入 `<config path>.models.catalog.json`，使用同目录临时文件原子替换并限制为 `0600`；启动时会先使用有效的旧快取，背景刷新成功后再更新。若能力目录暂时无法更新，服务会保留进程内上一份能力快照；首次启动且没有能力快照时，不会暴露能力未知的模型，避免把不支持的模型误路由到 Chat。手动 `models.protocols` 可用于上游实验模型。

认证 lane 为单个 Zen 通道，无跨 Tier 排序与回退。免费模型在认证顺序之前额外尝试匿名 Zen。

### Thinking 工具历史兼容

所有请求都会经过同一个上游请求准备流程，同协议转发和跨协议转换不再使用两套分支。通过 Chat Completions 或 Anthropic Messages API 调用 DeepSeek、Kimi/Moonshot 或 MiMo 模型时，代理会按上游的目标协议规范化 assistant 工具历史：Chat 补全缺失或空的 `reasoning_content`；Anthropic 保留有效 thinking 文本、为缺失或空的 thinking 补充兼容占位内容、将 `redacted_thinking` 转为普通 thinking，并移除这些兼容端点不接受的 `signature`。显式启用 reasoning/thinking 的别名模型也会启用该处理，普通非 reasoning 请求不会被修改。

跨协议桥接会区分 Chat/Responses 的 system 与 developer 指令，在 Anthropic 目标中按顺序合并为 system 内容；reasoning effort 会转换为兼容 thinking 预算。工具选择、空参数 `{}`、停止原因，以及 SSE 中延迟到达的工具名称、参数分片和完成事件也会转换到目标协议的对应形态。

流式响应会兼容 Chat 上游的 `delta.reasoning_content` 与 `delta.reasoning`。Anthropic 上游的 `event: error`、Responses 上游的 `response.failed` 以及 Chat 的错误类 `finish_reason` 会转换为目标协议的结构化错误事件，不会伪装成正常结束或静默断流。`redacted_thinking` 在支持加密 reasoning 的 Responses 目标中保留 `encrypted_content`，在无法表达加密块的 Chat/Anthropic 目标中使用 `[redacted thinking]` 明确占位。

协议文档未定义或当前 bridge 无法无损表达的输入 content block（例如未实现的音频/文件类型）会返回明确的转换错误，不再静默丢弃内容。

### `performance`

| 字段 | 含义 |
| --- | --- |
| `performance.max_idle_conns` | 所有上游连接池允许保留的最大空闲连接数。 |
| `performance.max_idle_conns_per_host` | 每个上游主机允许保留的最大空闲连接数。 |
| `performance.max_conns_per_host` | 每个主机的最大并发连接数。`0` 表示不设置上限。 |
| `performance.idle_conn_timeout_seconds` | 空闲连接在连接池中保留的时间。 |
| `performance.connect_timeout_seconds` | 与上游或代理建立 TCP 连接的超时时间。 |
| `performance.failure_cooldown_seconds` | 非 429 冷却的基础时间。401 按此对 credential 全局指数退避，403/5xx 按此对单个 target 与通道可用性指数退避，均带确定性 ±20% 抖动、总封顶 5 分钟；403 的 `Retry-After` 取更大值同样封顶 5 分钟。 |
| `performance.rate_limit_cooldown_seconds` | 429 冷却基准（秒），默认 300，可配 300..3600。429 proxy 与 credential429 连续打击按此起算指数退避（`base*2^min(n-1,3)`，确定性 ±20% 抖动），`Retry-After` 取 `max(退避, Retry-After)` 后统一 clamp 到固定 3600s 最大值；缺失或显式 0 时兼容为 300。非 429 冷却仍用通用 5 分钟封顶与 `failure_cooldown_seconds` 行为。时长换算与连续翻倍均为饱和安全，极端取值不会回绕为负。 |

### `logging`

| 字段 | 含义 |
| --- | --- |
| `logging.level` | 日志级别，支持 `debug`、`info`、`warn` 和 `error`，可通过 WebUI 热切换。 |
| `logging.ring_size` | WebUI 最近日志环容量，范围 100–50000，默认 2000。stdout 不受此容量限制。 |

每条 stdout 日志都是单行 JSON，包含时间、级别、组件、事件以及适用的 request ID、模型、tier、状态码、耗时、重试次数和实际使用的 Key 尾码。已建立上游路由的请求会以 `info` 级别记录 `request_routed` 事件；真实 Key 只显示最后 5 个字符，anonymous 请求显示为 `anonymous`。普通“请求完成”事件仍使用 `debug` 级别；警告和错误按原级别输出。日志不会输出完整上游 key、本地 key、Authorization、Cookie、代理认证信息或请求消息正文。

### `webui`

| 字段 | 含义 |
| --- | --- |
| `webui.enabled` | 是否在独立端口启动管理服务。旧配置未包含该段时默认关闭。 |
| `webui.listen` | 管理服务监听地址，示例为 `0.0.0.0:8081`。 |
| `webui.username` | 单一管理员账号。 |
| `webui.password` | 仅用于首次初始化的明文密码，至少 10 个字符；启动后自动删除。 |
| `webui.password_hash` | 自动生成的 Argon2id 哈希，不应手动编辑，也不会由 WebUI API 返回。 |
| `webui.session_ttl_minutes` | 登录 session 有效时间，范围 5–10080 分钟。 |

WebUI 中普通配置响应只包含 key 尾码/指纹及脱敏 proxy；运行桌面和路由诊断会显示每个请求最终使用的 Key 最后 5 个字符或 `anonymous`。需要查看完整值时必须再次输入管理密码，敏感响应禁止浏览器缓存。

### 配置保存与热重载

WebUI 保存时先解析并验证完整候选配置、创建新的连接池和 Gateway，然后将旧 Gateway 状态按 identity 迁移到新实例：仍在未来的 credential/target/代理限流/通道/凭证限流冷却（credential 按 tier+key、target 按完整 identity、代理限流与通道按 tier+池+URL、凭证限流按 tier+key）与 proxy 传输健康（按池+URL）迁移，其中非 429（credential 401、target、通道）剩余封顶通用 5 分钟，proxy429 与 credential429 剩余按新配置的 429 最大值 clamp（不按通用 5 分钟）；仍新鲜的 route-session override 仅当 target scope（不含 client 维度）仍有效时迁移，其中认证 scope 要求 proxy-free 且池仍为该通道所属池，新鲜度按 idle TTL 判断；会话+模型 pin 按 identity 无有效性过滤迁移至 cap 上限（含当前代理与代际，认证按绑定判有效、匿名按完整 target），呈 tombstone 绑定，被删除/变化的 target 仍命中原绑定并本地 502，不重建不 fallback；其余删除身份丢弃、新增资源按新最小值/最大值从零开始，迁移日志仅聚合计数。再写入临时文件、保留 `config.json.bak` 并替换 `config.json`，最后原子切换新请求使用的运行实例。写入或初始化失败时旧实例继续工作；切换前已开始的请求不会中断，仍使用旧状态。

keys、代理池（含 `proxy_pools` 与 `proxy_routing` 的新增/修改/引用切换）、上游、重试、模型、性能（含 `failure_cooldown_seconds` 与 `rate_limit_cooldown_seconds`，新 scheduler 按新基准、新资源用新值、已有 429 冷却仅保留剩余并按固定 3600s 最大值 clamp 而不重算）和日志级别会立即生效。`listen`、`webui.listen` 与 `webui.enabled` 会保存但需要重启进程。WebUI 也提供“从磁盘重载”，外部编辑后的配置仍会经过相同的验证与回滚流程。保存后的 JSON 会被规范化为新格式（旧顶层 `proxies` / `proxyfile` 不保留），原有注释不会保留。旧字段（`zen_keys`/`go_keys`、`prefer`、`proxy_routing.zen/go`、`upstream.go`、`performance.rate_limit_cooldown_max_seconds`）仅加载兼容：按 keys→zen→go 合并去重、authenticated 取显式值否则 legacy zen 否则 legacy go、其余忽略，保存/API 仅输出 canonical（`keys`、`proxy_routing.anonymous/authenticated`、Zen 上游、单个 429 字段）；真正未知字段仍拒绝。


## 会话 ID

代理会为上游添加 OpenCode 使用的 `User-Agent`（`opencode/1.18.31` 裸版本）、`x-opencode-client`、`x-opencode-session`、`x-opencode-request`、`x-opencode-project` 和可选的 `x-parent-session-id` 请求头（仅官方集合，不发送 `x-session-affinity` / `X-Session-Id`）。

- 每个请求使用不同的 `x-opencode-request`（`msg_` 规范形状），同一次请求的重试保持不变。
- 优先使用客户端提供的 `x-opencode-session`、`x-session-affinity`、`X-Session-Id`、`x-session-id`、`conversation-id`、`conversation_id` 或 `metadata.session_id` 生成客户端会话 ID（入站解析不变）。
- 没有显式会话标识时，使用第一条用户消息生成稳定客户端会话 ID，使同一段多轮对话保持一致；客户端会话是建立身份，从不原文出进程：未绑定时用于稳定排序并按冻结顺序 fallback，400 恢复不修改它、不重排已冻结候选；首次成功后同一会话+模型建立绑定，匿名绑定完整 target，认证绑定 tier/凭证/池/模型/协议/上游地址并允许同池内移动，之后不再跨 Tier/跨凭证/跨池回退。
- 如果两个独立会话的第一条消息完全相同，建议由客户端发送不同的 `x-session-id`，以确保两个会话严格分离。
- 上游实际发送的是按 route target scope 分离的 Route Session（上游 authority、tier、credential 内部身份、proxy pool、target protocol，不含 model、不存原始 credential；认证 scope 与代理无关，匿名 scope 另含 proxy 原始身份）：内部仍为 target-bound 非原文的 `rss_*`（首代稳定无状态派生，仅 400 轮换后保存有界内存 override），线上编码为与之稳定对应的规范 OpenCode 形伪名 wire ID（`ses_` 规范形状、`msg_` 请求、`40hex` 项目、父会话亦为规范伪名），写入 `x-opencode-session` / `x-opencode-request` / `x-opencode-project` / 规范父会话，并同步覆盖已存在的 body `conversation_id`、`metadata.session_id` 为同一 wire 会话（缺失不新增，类型不符明确报错）；Responses 还会将 `prompt_cache_key` 置为同一 wire 会话、`store` 缺失时默认 `false`（显式值保留）。Apply 会按 target scope 有效性/新鲜度过滤迁移 route override，重启丢失；override 不进日志/metrics/history/admin。会话+模型 target 绑定与此不同：它是 durable 建立绑定，Apply 无有效性过滤迁移为 tombstone 身份（被删除/变化的 target 仍保持绑定并本地 502，不重建），重启清空；新会话与已存在会话的 429 行为不同——前者在建立时过滤该代理全部候选，匿名已存在会话在冷却期间本地返回目标协议 429 + 剩余 Retry-After，认证已存在会话在预算与候选允许时可移向同池下一代理，否则同样本地失败。
- 可用性探测使用同样的官方集合与确定性规范会话/请求/项目值，无状态且不写 scheduler；自定义 fallback 通道不带供应商会话亲和语义。

## 致谢

感谢 [LINUX DO](https://linux.do) 社区一直以来的支持。
