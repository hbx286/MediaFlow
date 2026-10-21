package database

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"mediaflow/backend/internal/infrastructure/postgres/sqlcgen"
)

func TestGetWorkspaceNameWithPostgreSQL(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("未设置 TEST_DATABASE_URL；未执行真实 PostgreSQL 查询")
	}
	testConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal("TEST_DATABASE_URL 无效")
	}
	if appURL := os.Getenv("DATABASE_URL"); appURL != "" {
		appConfig, err := pgxpool.ParseConfig(appURL)
		if err != nil {
			t.Fatal("DATABASE_URL 无效，无法确认测试数据库隔离")
		}
		if testConfig.ConnConfig.Host == appConfig.ConnConfig.Host &&
			testConfig.ConnConfig.Port == appConfig.ConnConfig.Port &&
			testConfig.ConnConfig.Database == appConfig.ConnConfig.Database {
			t.Fatal("TEST_DATABASE_URL 与 DATABASE_URL 指向相同的 Host、Port 和 Database")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := NewPool(ctx, Options{
		URL: url, ConnectTimeout: 3 * time.Second, MaxConns: 2,
		MaxConnIdleTime: time.Minute, HealthCheckPeriod: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("测试数据库不可用: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer rollbackCancel()
		if err := tx.Rollback(rollbackCtx); err != nil {
			t.Errorf("回滚测试事务失败: %v", err)
		}
	}()
	id := pgtype.UUID{Valid: true}
	if _, err := rand.Read(id.Bytes[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO workspaces (id, name) VALUES ($1, $2)", id, "sqlc 测试工作空间"); err != nil {
		t.Fatalf("插入测试工作空间失败（测试库需先执行 ddl_1.0.sql）: %v", err)
	}
	q := sqlcgen.New(tx)
	name, err := q.GetWorkspaceName(ctx, id)
	if err != nil || name != "sqlc 测试工作空间" {
		t.Fatalf("查询结果 = %q, %v", name, err)
	}
	missing := id
	missing.Bytes[15] ^= 0xff
	if _, err := q.GetWorkspaceName(ctx, missing); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("不存在的工作空间应返回 ErrNoRows，实际为 %v", err)
	}
}
