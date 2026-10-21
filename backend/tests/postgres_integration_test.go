//go:build integration

package tests

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"mediaflow/backend/internal/platform/database"
)

func newIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	testURL := os.Getenv("TEST_DATABASE_URL")
	if testURL == "" {
		t.Skip("未设置 TEST_DATABASE_URL；未执行真实 PostgreSQL 集成测试")
	}
	testConfig, err := pgxpool.ParseConfig(testURL)
	if err != nil {
		t.Fatal("TEST_DATABASE_URL 无效")
	}
	if appURL := os.Getenv("DATABASE_URL"); appURL != "" {
		appConfig, err := pgxpool.ParseConfig(appURL)
		if err != nil {
			t.Fatal("DATABASE_URL 无效，无法确认测试库隔离")
		}
		if testConfig.ConnConfig.Host == appConfig.ConnConfig.Host &&
			testConfig.ConnConfig.Port == appConfig.ConnConfig.Port &&
			testConfig.ConnConfig.Database == appConfig.ConnConfig.Database {
			t.Fatal("TEST_DATABASE_URL 与 DATABASE_URL 指向相同的 Host、Port 和 Database")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := database.NewPool(ctx, database.Options{
		URL: testURL, ConnectTimeout: 3 * time.Second, MaxConns: 2,
		MaxConnIdleTime: time.Minute, HealthCheckPeriod: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("测试数据库不可用: %v", err)
	}
	var databaseName, schemaName string
	var hasWorkspaces, hasVersions, hasRuns, hasNodes, hasTasks, hasRenewLease bool
	err = pool.QueryRow(ctx, `SELECT current_database(), current_schema(),
		to_regclass('public.workspaces') IS NOT NULL,
		to_regclass('public.workflow_versions') IS NOT NULL,
		to_regclass('public.workflow_runs') IS NOT NULL,
		to_regclass('public.node_runs') IS NOT NULL,
		to_regclass('public.tasks') IS NOT NULL,
		to_regprocedure('public.mf_renew_lease(uuid,uuid,bigint,text,integer)') IS NOT NULL`).Scan(
		&databaseName, &schemaName, &hasWorkspaces, &hasVersions, &hasRuns, &hasNodes, &hasTasks, &hasRenewLease,
	)
	if err != nil {
		t.Fatalf("检查测试库 Schema 失败: %v", err)
	}
	if databaseName != testConfig.ConnConfig.Database || schemaName != "public" {
		t.Fatal("测试库实际名称或当前 Schema 与预期不符，拒绝写入")
	}
	if !hasWorkspaces || !hasVersions || !hasRuns || !hasNodes || !hasTasks || !hasRenewLease {
		t.Fatal("测试库缺少当前 Schema；请先按 backend/README.md 在经确认的独立空库中初始化 ddl_1.0.sql")
	}
	return pool
}

func testUUID(t *testing.T) pgtype.UUID {
	t.Helper()
	id := pgtype.UUID{Valid: true}
	if _, err := rand.Read(id.Bytes[:]); err != nil {
		t.Fatal(err)
	}
	return id
}

func rollbackTestTx(t *testing.T, tx pgx.Tx) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		t.Errorf("回滚测试事务失败: %v", err)
	}
}

func TestPostgreSQLConnection(t *testing.T) {
	pool := newIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("Ping 失败: %v", err)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conn.Release()
	if pool.Stat().AcquiredConns() != 0 {
		t.Fatal("数据库连接未释放")
	}
	expiredCtx, cancelExpired := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancelExpired()
	if err := pool.Ping(expiredCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("过期上下文应返回 DeadlineExceeded，实际为 %v", err)
	}
	var result int
	err = pool.QueryRow(ctx, "SELECT 1 / 0").Scan(&result)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "22012" {
		t.Fatalf("预期 PostgreSQL 除零错误 22012，实际为 %v", err)
	}
}

func TestPostgreSQLTransactionCommitAndRollback(t *testing.T) {
	pool := newIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	committedID := testUUID(t)
	insertSucceeded := false
	commitSucceeded := false
	t.Cleanup(func() {
		if !insertSucceeded {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		tag, err := pool.Exec(cleanupCtx, "DELETE FROM public.workspaces WHERE id = $1", committedID)
		if err != nil {
			t.Errorf("按主键清理测试数据失败，可能有遗留记录: %v", err)
			return
		}
		if commitSucceeded && tag.RowsAffected() != 1 {
			t.Errorf("已提交的测试数据应清理 1 行，实际清理 %d 行", tag.RowsAffected())
		} else if !commitSucceeded && tag.RowsAffected() == 1 {
			t.Log("提交未确认的测试数据已按主键清理")
		}
	})
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTestTx(t, tx)
	if _, err := tx.Exec(ctx, "INSERT INTO public.workspaces (id, name) VALUES ($1, $2)", committedID, "事务提交测试"); err != nil {
		t.Fatal(err)
	}
	insertSucceeded = true
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("提交测试事务失败，提交结果可能不确定: %v", err)
	}
	commitSucceeded = true
	var name string
	if err := pool.QueryRow(ctx, "SELECT name FROM public.workspaces WHERE id = $1", committedID).Scan(&name); err != nil || name != "事务提交测试" {
		t.Fatalf("提交后数据不可见或内容错误: %q, %v", name, err)
	}
	rolledBackID := testUUID(t)
	rollbackTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTestTx(t, rollbackTx)
	if _, err := rollbackTx.Exec(ctx, "INSERT INTO public.workspaces (id, name) VALUES ($1, $2)", rolledBackID, "事务回滚测试"); err != nil {
		t.Fatal(err)
	}
	if err := rollbackTx.Rollback(ctx); err != nil {
		t.Fatalf("回滚测试事务失败: %v", err)
	}
	if err := pool.QueryRow(ctx, "SELECT name FROM public.workspaces WHERE id = $1", rolledBackID).Scan(&name); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("回滚后应无测试数据，实际为 %v", err)
	}
}

func TestRenewLeaseRejectsInvalidAttempt(t *testing.T) {
	pool := newIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTestTx(t, tx)
	workspaceID, versionID, runID := testUUID(t), testUUID(t), testUUID(t)
	nodeID, taskID, absentAttemptID := testUUID(t), testUUID(t), testUUID(t)
	statements := []struct {
		query string
		args  []any
	}{
		{"INSERT INTO public.workspaces (id, name) VALUES ($1, $2)", []any{workspaceID, "函数测试工作空间"}},
		{"INSERT INTO public.workflow_versions (id) VALUES ($1)", []any{versionID}},
		{"INSERT INTO public.workflow_runs (id, workspace_id, workflow_version_id, status) VALUES ($1, $2, $3, 'RUNNING')", []any{runID, workspaceID, versionID}},
		{"INSERT INTO public.node_runs (id, workflow_run_id, node_definition_id) VALUES ($1, $2, $3)", []any{nodeID, runID, "test-node"}},
		{"INSERT INTO public.tasks (id, workflow_run_id, node_run_id, workspace_id, logical_task_key, task_type, capability, status) VALUES ($1, $2, $3, $4, $5, $6, $7, 'RUNNING')", []any{taskID, runID, nodeID, workspaceID, "test-task", "test-type", "test-capability"}},
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement.query, statement.args...); err != nil {
			t.Fatalf("准备函数测试数据失败: %v", err)
		}
	}
	var renewed bool
	if err := tx.QueryRow(ctx, "SELECT public.mf_renew_lease($1, $2, $3, $4, $5)", taskID, absentAttemptID, int64(1), "test-token-hash", 60).Scan(&renewed); err != nil {
		t.Fatalf("调用 mf_renew_lease 失败: %v", err)
	}
	if renewed {
		t.Fatal("不存在的当前 Attempt 不应获得续租")
	}
	var status string
	var leaseEpoch int64
	if err := tx.QueryRow(ctx, "SELECT status, lease_epoch FROM public.tasks WHERE id = $1", taskID).Scan(&status, &leaseEpoch); err != nil {
		t.Fatal(err)
	}
	if status != "RUNNING" || leaseEpoch != 0 {
		t.Fatalf("拒绝续租后任务状态被修改: status=%s lease_epoch=%d", status, leaseEpoch)
	}
}
