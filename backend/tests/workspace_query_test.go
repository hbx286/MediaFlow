//go:build integration

package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"mediaflow/backend/internal/infrastructure/postgres/sqlcgen"
)

func TestGetWorkspaceNameWithPostgreSQL(t *testing.T) {
	pool := newIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTestTx(t, tx)
	id := testUUID(t)
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
