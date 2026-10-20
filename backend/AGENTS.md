# MediaFlow Backend — Go 工程与执行系统规范

本文件适用于 `backend/` 下的所有 Go 代码、SQL、脚本、测试和服务配置，并与仓库根目录 `AGENTS.md` 共同生效。

## 1. 架构与职责

- 采用 **Go 模块化单体控制面 + 独立 Worker 执行面**；在证据充分前不要擅自拆分微服务。
- 按领域模块组织代码，避免所有业务堆进 `handler/service/repository` 三个全局目录。
- `domain` 承担实体、值对象、业务不变量和领域规则，不依赖 HTTP、pgx、Redis 或特定 AI SDK。
- `application` 组织用例、授权校验、事务协调和跨模块协作。
- `infrastructure` 实现 PostgreSQL、对象存储、消息以及第三方模型适配。
- HTTP Handler 负责协议解析、校验、认证上下文、调用应用服务和响应映射，禁止绕过应用服务直接修改核心业务状态。

### 执行子系统的责任边界

- **Workflow Engine**：DAG 校验、NodeRun 状态、依赖决议、失败传播与工作流推进。
- **Scheduler**：READY Task 发现、资源/配额检查、原子 Claim 与执行授权。
- **Task Execution**：Task、TaskAttempt、Lease、ResultCommit 的有效状态转换。
- **Worker**：领取授权、执行 Handler、上报进度、续租和提交结果；不得直接修改 WorkflowRun/NodeRun。
- **Model Gateway**：供应商差异、能力适配、错误映射及外部 Job 状态管理。
- **Asset**：文件元数据、不可变版本和已验证产物的引用。

## 2. 现有数据库与兼容性

在任何数据库任务开始前，查找仓库中的实际 Schema、测试脚本以及 `docs/Database/` 和 `docs/Execution/`；优先核对**真实字段、约束、函数签名**，不能仅凭设计文档猜测其存在。

- PostgreSQL 是核心执行状态的事实来源，Redis/消息通知不能取代持久化任务状态。
- 用户当前采用单文件 Schema 与独立 Seed SQL；尊重仓库现有文件位置及命名，不要自动拆成编号迁移文件。
- 每张新增表的 `CREATE TABLE` 后**紧跟**其 `COMMENT ON TABLE` 和所有 `COMMENT ON COLUMN`；每个字段须有中文、准确、说明语义的注释，再开始下一张表。
- Schema 与初始化数据分别维护；测试数据必须独立，不得混入生产 Seed。
- DDL 必须包含适当的 PK、FK、UNIQUE、CHECK 和索引，并用 SQL 注释说明非显而易见的约束设计。
- 不得未经授权清空、覆盖或重建现有数据库；测试必须连接明确指定的独立测试库。
- `lock_version` 用于乐观并发控制，`lease_epoch`/fencing 用于防止过期执行结果被接受，两者不能混用。
- 核心状态更新采用有条件的原子更新或事务，并明确锁顺序与冲突语义。

## 3. 核心执行协议不变量

- WorkflowDefinition/WorkflowVersion 与 WorkflowRun 分离；已发布版本不可变。
- WorkflowRun、NodeRun、Task、TaskAttempt 独立建模和管理状态。
- 同一逻辑任务通过确定的 `logical_task_key` 或等价唯一约束去重。
- Claim 必须保证有效执行授权唯一，并协调创建 Attempt、Lease 与资源预留。
- 续租和结果提交需要校验当前 Task、Attempt、Worker Session、租约与执行代数，按实际协议检查必需字段。
- 重复提交同一结果应具备幂等行为；不同结果使用同一提交键时应被拒绝。
- 旧 Attempt 迟到不能覆盖已确认结果；租约过期并不证明远程 AI 操作已终止或不会计费。
- ResultCommit 与 DAG 后继推进之间使用已定义的事务/事件协议；Worker 不直接推进工作流。
- Outbox 与重复消费必须支持可恢复、幂等的处理；不将“消息只会收到一次”作为假设。
- 对象存储与 PostgreSQL 之间不存在天然原子事务，必须执行产物校验、正式确认及孤儿清理规则。

## 4. Go 实现规范

### 4.1 基础工程规范

- 优先使用 Go 标准库；Gin、pgx/v5、sqlc 等已确定的技术选型不受此限制。
- 遵循 Go 官方代码规范，保持代码简洁、清晰、可维护，避免不必要的抽象和框架封装。
- `context.Context` 应贯穿 HTTP API、Application、Repository、数据库及外部服务调用，正确处理超时、取消和请求生命周期。
- 使用 `%w` 包装底层错误，通过 `errors.Is` / `errors.As` 区分领域拒绝、状态冲突、并发竞争、暂时性失败和系统错误。
- 正常业务失败不得使用 `panic`；不允许静默忽略错误。
- 正确关闭 `Rows`、回滚未提交事务、释放资源，并处理进程退出及优雅关闭。
- 不使用无限 Goroutine、无界 Channel 或无界重试；并发数量、队列容量及重试次数必须受到控制。
- 依赖方向必须清晰，接口应根据实际调用者需求定义，禁止提前设计没有消费者的大型通用接口。
- 使用显式 DTO 和 Mapper 隔离 HTTP 请求响应、数据库记录、领域实体及外部供应商数据结构。
- 使用结构化日志，保留 `workflow_run_id`、`node_run_id`、`task_id`、`attempt_id` 等关联标识；不得记录密钥、访问令牌或用户敏感输入全文。

### 4.2 PostgreSQL 数据访问技术选型

- 数据库访问层统一采用 **pgx/v5 + sqlc**。
- `pgx/v5` 负责 PostgreSQL 连接管理、事务执行、底层 SQL 操作及 PostgreSQL 特性支持。
- 使用 `pgxpool.Pool` 管理数据库连接池，并配置合理的连接数量、连接超时、空闲时间及健康检查策略。
- `sqlc` 根据显式维护的 SQL 查询文件生成类型安全的 Go 数据访问代码。
- 禁止未经架构决策引入 GORM、Ent 或其他 ORM，不得同时维护多套职责重叠的数据库访问框架。
- `sqlc` 不是 ORM，不负责数据库 Schema 迁移、业务事务协调或领域模型管理。
- 现有 PostgreSQL Schema、约束、索引和数据库函数仍是实际数据库契约的重要组成部分，不得因为使用 sqlc 而重写已验证的核心执行协议。
- 所有数据库查询必须以仓库中实际存在的表、字段、约束和函数签名为依据，不得凭设计文档猜测数据库结构。

### 4.3 sqlc 使用规范

- SQL 查询必须显式维护在指定的 SQL Query 文件中，不允许以生成代码代替 SQL 源文件。
- 使用项目统一的 `sqlc.yaml` 配置，明确 SQL Schema、Query 路径、Go Package、生成目录及 pgx/v5 代码生成目标。
- 生成代码统一归属 Infrastructure，不允许 Domain 直接导入或依赖 sqlc 生成的类型。
- 不得手动修改 sqlc 生成代码；修改 SQL 后必须重新运行代码生成命令。
- 新增或修改 SQL Query 时，应同步检查生成代码、类型映射和调用方兼容性。
- 对常规 CRUD、明确参数的查询及可由 sqlc 正确解析的 PostgreSQL 函数调用，优先使用 sqlc。
- 对动态 SQL、特殊 PostgreSQL 操作或 sqlc 无法合理表达的查询，允许直接使用 pgx，但必须说明原因，并保留在 Infrastructure 层。
- 不得为了让 sqlc 成功生成代码而擅自修改数据库业务约束、状态机或核心事务函数。
- sqlc 的 Schema 输入应与仓库真实数据库结构保持一致，避免维护与实际数据库不一致的影子 Schema。
- 不要求在工程初始化阶段提前实现所有 SQL Query 或 Repository；只建立当前任务实际需要的配置与生成机制。

### 4.4 Repository 与领域模型边界

- Repository 接口应根据 Domain 或 Application 的实际需要定义，不得直接暴露数据库驱动或 sqlc 生成类型。
- Repository 的具体实现位于 Infrastructure，负责调用 sqlc 生成代码或必要的原生 pgx 操作。
- 数据库 Record、领域 Entity 和 HTTP DTO 必须按实际边界进行转换，不允许将 sqlc 数据结构直接作为对外 API 响应。
- Repository 方法应表达明确的业务语义，避免把数据库表操作机械地暴露给 Application。
- 不允许 Handler 绕过 Application 和 Repository 直接修改核心执行数据。
- 应保持领域模型不依赖 pgx、sqlc、Gin、Redis 或外部 AI SDK。
- 不得为了抽象而抽象；简单查询不必强制建立多层无业务价值的包装。

### 4.5 数据库事务与并发控制

- PostgreSQL 是 WorkflowRun、NodeRun、Task、TaskAttempt、Lease 和资源预留等核心执行状态的权威数据源。
- 事务边界由 Application Service 或明确的事务协调组件管理，Repository 必须能够在同一个业务事务中协同执行。
- 使用 `pgx.Tx` 或适当的事务抽象明确管理事务，禁止一个需要原子性的业务操作被意外拆分为多个独立提交。
- 所有事务必须正确处理 Commit、Rollback、超时、取消和数据库连接异常。
- 事务中不得执行不必要的长时间外部 HTTP 请求、AI 模型调用或媒体处理操作。
- 核心状态更新必须遵循数据库已定义的唯一约束、状态条件、锁顺序及并发冲突规则。
- 优先复用现有 `mf_claim_task`、`mf_renew_lease`、`mf_commit_result` 和 `mf_recover_expired` 等数据库函数，并以实际函数签名和返回语义为准。
- Task Claim、Lease、ResultCommit、故障恢复及资源预留不得被普通 CRUD 操作替代。
- 正确区分 `lock_version` 乐观锁和 `lease_epoch` Fencing 机制，禁止混用。
- 对死锁、序列化失败、唯一约束冲突及数据库暂时不可用等情况，必须建立明确的错误分类和有限重试策略。
- 对提交结果未知的事务，不得盲目重试可能产生重复业务副作用的操作；必须结合幂等键及数据库状态进行确认。
- 不得将 PostgreSQL 事务与 Redis、对象存储或外部 AI API 调用视为天然原子操作。

### 4.6 配置、连接与安全

- 数据库连接信息通过环境变量或统一配置系统提供，不得硬编码用户名、密码及生产数据库地址。
- PostgreSQL 连接池应在应用启动阶段初始化，并在服务退出时正确关闭。
- `/health/live` 用于判断进程是否存活，不应因数据库短暂不可用而直接失败。
- `/health/ready` 应根据服务依赖情况检查 PostgreSQL 等必要组件的可用性。
- 日志不得输出完整 PostgreSQL DSN、数据库密码、访问令牌或其他敏感凭据。
- SQL 参数必须使用参数化查询，不得将用户输入直接拼接进 SQL 字符串。
- 动态 SQL 中不可避免的标识符必须使用安全的白名单或正确的标识符处理机制。
- 数据库连接数、查询超时、事务超时等参数必须可配置，且具有合理默认值。

### 4.7 测试与代码质量

- Domain 层优先使用表驱动单元测试覆盖状态转换、业务规则和非法操作。
- Repository、SQL Query、数据库函数及事务行为必须使用真实 PostgreSQL 集成测试验证。
- sqlc 生成成功不代表 SQL 业务语义正确，必须验证查询结果、事务回滚和并发条件。
- 数据库并发测试应使用独立连接，覆盖双重 Claim、资源超售、Lease 竞争、过期 Attempt 及重复 ResultCommit 等场景。
- HTTP Handler 测试应使用 `httptest`，覆盖正常响应、参数错误、权限拒绝、状态冲突及内部错误。
- 新增或修改 SQL Query 后，必须验证 sqlc 代码生成，并运行相关 Go 测试。
- 根据任务实际范围执行 `gofmt`、`go vet ./...`、`go test ./...` 和 `go build ./...`。
- 对数据库集成测试无法执行的情况，应说明缺失的环境条件，不得将跳过测试描述为测试通过。
- 不允许通过弱化数据库约束、跳过错误检查或删除已有测试来使 CI 通过。

## 5. HTTP API 与 Worker 通信规范

### 5.1 HTTP 技术选型

- 使用 Gin 作为 HTTP API 框架，底层使用标准库 `http.Server` 管理超时、启动及优雅退出。
- Gin 是已确定的技术选型，不应擅自替换为其他 HTTP 框架。
- 对外 API 采用 RESTful 风格，统一使用 `/api/v1` 前缀。
- Worker 内部通信使用 HTTP/JSON，统一使用 `/internal/v1` 前缀。
- 请求与响应默认使用 JSON，大文件通过对象存储传递。
- 对外 API 使用 OpenAPI 3.1 描述契约。

### 5.2 RESTful API 设计

- API 以资源为中心设计，资源路径使用名词，优先使用复数形式。
- 使用 GET、POST、PUT、PATCH、DELETE 表达标准 HTTP 语义。
- GET 不得产生业务状态变更。
- PUT 应具有幂等语义；PATCH 是否幂等取决于具体操作。
- 资源创建通常使用 POST，成功时根据实际语义返回 201 或 202。
- 工作流启动、取消、任务重试等操作优先采用资源化建模，不强制将所有内部操作机械地转换为 CRUD。
- 正确使用 HTTP 状态码，禁止所有业务错误一律返回 200。
- 统一分页、过滤和排序规则；高数据量列表应考虑游标分页。
- 所有写操作执行参数校验、身份认证、权限检查及必要的业务状态校验。
- 对可能重复提交的关键操作提供明确的幂等策略。
- 错误响应优先采用 RFC 9457 Problem Details，并允许扩展稳定的业务错误码。
- 新增或修改对外 API 时，同步维护 OpenAPI 3.1 契约及接口测试。

### 5.3 HTTP 架构边界

- Gin 仅用于 HTTP Transport 层。
- Handler 负责请求绑定、协议级校验、提取认证上下文、调用应用服务和响应映射。
- 应用服务负责用例协调、业务授权及事务边界。
- Domain、Application 和 Repository 接口不得依赖 `gin.Context`。
- 跨层使用标准库 `context.Context` 传递取消信号、截止时间和必要的请求上下文。
- 不允许 Handler 直接修改核心业务数据库状态。

### 5.4 Worker HTTP/JSON 通信

- Worker 采用主动 Pull 模式，通过控制面 API 注册、领取任务、续租、上报进度及提交结果。
- Worker 不直接访问控制面的 PostgreSQL 执行状态表，不直接修改 WorkflowRun、NodeRun、Task 或 TaskAttempt。
- Task Claim 由控制面通过 PostgreSQL 原子事务完成，包括必要的 Attempt、Lease 和资源预留操作。
- Worker 获取有效 TaskEnvelope 后方可执行任务。
- 续租和结果提交必须按照实际执行协议校验 Task、Attempt、Worker Session、LeaseEpoch/Fencing 及适用的授权凭据。
- 过期 Attempt、失效 Session 或不匹配的执行代数不得覆盖当前有效执行结果。
- HTTP 超时、断线或响应丢失不能直接判定任务没有执行；调用方必须支持重试、幂等及状态查询。
- Worker 通信的错误语义必须明确区分：可重试错误、执行授权失效、状态冲突和永久拒绝。
- 图片、音频、视频等大文件使用对象存储引用传递，不直接嵌入 JSON。
- Worker 内部 API 必须进行服务身份认证、授权和传输安全保护；不得因为属于内部接口而默认可信。
- 暂不引入 gRPC；如需变更通信协议，先通过架构决策评审。

## 6. 测试与验证

每项核心变更应覆盖适用的：成功、非法状态、重复请求、并发竞争、超时、取消、资源不足、事务回滚及故障恢复。

- 领域状态机优先表驱动单元测试。
- 数据库事务必须使用**真实 PostgreSQL 的集成测试**；Mock 不证明数据库并发正确性。
- 多连接并发验证至少包含：双 Claim、资源不超售、续租/恢复竞争、提交/恢复竞争、重复 ResultCommit、迟到 Attempt 拒绝。
- 并发测试优先使用受控屏障、独立连接和实际状态断言，不把固定 `sleep` 作为唯一同步手段。
- `go test ./...`、`go vet ./...`、`gofmt` 应按任务情况实际运行；数据库环境不可用时明确 Skip 原因。
- 不能将 `go test -race` 视为数据库事务一致性的替代验证。
- 对数据库函数的修改需补充回归测试，不允许为通过测试而弱化已有业务不变量。

## 7. 本阶段开发顺序

按照任务范围分批交付：

1. Go 工程初始化、配置、健康检查、数据库连接及测试基础设施。
2. 数据库多连接并发测试与已有 SQL 验证。
3. Task/TaskAttempt 领域模型和存储适配。
4. 工作流最小 DAG 执行与依赖推进。
5. Scheduler/Worker 执行链路及故障恢复。
6. AI Provider、资产与媒体生产的端到端整合。

不得仅因这个顺序而主动实施尚未授权的后续阶段。

## 8. 任务完成报告

输出以下内容：变更文件、对应设计依据、实现要点、实际执行的命令和结果、未验证/遗留问题、建议 Gitmoji 提交信息。未经要求不执行 Git Commit、Push、数据库清空或生产部署。
