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

- 优先标准库，数据库客户端按既有项目选型使用 `pgx/v5`，避免不必要地同时引入多个 ORM/SQL 访问栈。
- `context.Context` 应贯穿 API、应用服务、数据库和外部调用，超时及取消必须明确处理。
- 使用 `%w` 包装底层错误，按照 `errors.Is`/`errors.As` 区分领域拒绝、并发冲突、暂时性失败和系统错误。
- 正常业务失败不得使用 `panic`；不得忽略 `Rows.Close`、事务 `Rollback`、资源释放和进程退出状态。
- 不使用无限 Goroutine、无界 Channel 或无界重试；并发度必须受控。
- 依赖方向清晰，接口按调用者需要定义；不创建无实际消费者的“大而全”接口。
- 通过显式 DTO/Mapper 隔离数据库记录、领域实体和外部 API 格式。
- 使用结构化日志和稳定关联标识：`workflow_run_id`、`node_run_id`、`task_id`、`attempt_id` 等；不记录密钥或用户敏感输入全文。

## 5. 测试与验证

每项核心变更应覆盖适用的：成功、非法状态、重复请求、并发竞争、超时、取消、资源不足、事务回滚及故障恢复。

- 领域状态机优先表驱动单元测试。
- 数据库事务必须使用**真实 PostgreSQL 的集成测试**；Mock 不证明数据库并发正确性。
- 多连接并发验证至少包含：双 Claim、资源不超售、续租/恢复竞争、提交/恢复竞争、重复 ResultCommit、迟到 Attempt 拒绝。
- 并发测试优先使用受控屏障、独立连接和实际状态断言，不把固定 `sleep` 作为唯一同步手段。
- `go test ./...`、`go vet ./...`、`gofmt` 应按任务情况实际运行；数据库环境不可用时明确 Skip 原因。
- 不能将 `go test -race` 视为数据库事务一致性的替代验证。
- 对数据库函数的修改需补充回归测试，不允许为通过测试而弱化已有业务不变量。

## 6. 本阶段开发顺序

按照任务范围分批交付：

1. Go 工程初始化、配置、健康检查、数据库连接及测试基础设施。
2. 数据库多连接并发测试与已有 SQL 验证。
3. Task/TaskAttempt 领域模型和存储适配。
4. 工作流最小 DAG 执行与依赖推进。
5. Scheduler/Worker 执行链路及故障恢复。
6. AI Provider、资产与媒体生产的端到端整合。

不得仅因这个顺序而主动实施尚未授权的后续阶段。

## 7. 任务完成报告

输出以下内容：变更文件、对应设计依据、实现要点、实际执行的命令和结果、未验证/遗留问题、建议 Gitmoji 提交信息。未经要求不执行 Git Commit、Push、数据库清空或生产部署。
