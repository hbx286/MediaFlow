# MediaFlow Backend

MediaFlow 后端采用 Go 模块化单体结构。HTTP API 提供服务健康检查，Go module 位于本目录。

## 技术栈

- Go 1.24+
- Gin、标准库 `http.Server`
- PostgreSQL 16+、pgx/v5（`pgxpool`）

## 目录结构

```text
backend/
├── cmd/api/                     API 启动入口
├── internal/health/             HTTP 健康检查
├── internal/platform/config/   环境配置
├── internal/platform/database/ PostgreSQL 连接池
├── internal/platform/lifecycle/ HTTP 服务生命周期
├── migration/postgres/          数据库 Schema
└── tests/                       Go 测试
```

## 环境要求

需要 Go 1.24+ 和 PostgreSQL 16+。

## 环境变量配置

`DATABASE_URL` 必填；其他变量、默认值及用途见 [.env.example](.env.example)。时间参数使用 Go duration 格式，例如 `2s`、`30m`。程序只读取环境变量，不自动加载 `.env` 文件。

## 本地启动

在 `backend/` 目录运行：

```sh
cp -n .env.example .env
# 编辑 .env，填入本地 PostgreSQL 连接信息
set -a
. ./.env
set +a
go run ./cmd/api
```

API 默认监听 `:8080`。可使用以下端点检查服务：

| 路径 | 含义 |
| --- | --- |
| `GET /health/live` | 进程可响应时返回 200，不依赖数据库。 |
| `GET /health/ready` | PostgreSQL 可连接时返回 200，否则返回 503。 |

## 测试与构建

在 `backend/` 目录运行：

```sh
go test ./...
go vet ./...
go build ./...
```

数据库 Schema 文件位于 [`migration/postgres/ddl_1.0.sql`](migration/postgres/ddl_1.0.sql)，仅适用于全新空库，不应在已初始化的数据库中重复执行。
