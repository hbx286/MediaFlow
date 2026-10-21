//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Each fixture owns its rows. It is committed so two connections can compete for them.
type concurrencyFixture struct {
	workspace, version, worker, pool pgtype.UUID
	sessions                         [2]pgtype.UUID
	runs, nodes, tasks               [2]pgtype.UUID
	count                            int
}

func newConcurrencyFixture(t *testing.T, pool *pgxpool.Pool, count int) concurrencyFixture {
	t.Helper()
	f := concurrencyFixture{workspace: testUUID(t), version: testUUID(t), worker: testUUID(t), pool: testUUID(t), count: count}
	for i := range f.sessions {
		f.sessions[i] = testUUID(t)
	}
	for i := 0; i < count; i++ {
		f.runs[i], f.nodes[i], f.tasks[i] = testUUID(t), testUUID(t), testUUID(t)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTestTx(t, tx)
	statements := []struct {
		query string
		args  []any
	}{
		{"INSERT INTO public.workspaces(id,name) VALUES($1,$2)", []any{f.workspace, "并发测试工作空间"}},
		{"INSERT INTO public.workflow_versions(id) VALUES($1)", []any{f.version}},
		{"INSERT INTO public.workers(id,name) VALUES($1,$2)", []any{f.worker, "并发测试 Worker"}},
		{"INSERT INTO public.resource_pools(id,capability) VALUES($1,$2)", []any{f.pool, "concurrency-test"}},
		{"INSERT INTO public.resource_pool_counters(pool_id,capacity_slots) VALUES($1,1)", []any{f.pool}},
	}
	for _, s := range statements {
		if _, err := tx.Exec(ctx, s.query, s.args...); err != nil {
			t.Fatalf("创建并发测试数据失败: %v", err)
		}
	}
	for _, session := range f.sessions {
		if _, err := tx.Exec(ctx, "INSERT INTO public.worker_sessions(id,worker_id,status) VALUES($1,$2,'ACTIVE')", session, f.worker); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < count; i++ {
		for _, s := range []struct {
			query string
			args  []any
		}{
			{"INSERT INTO public.workflow_runs(id,workspace_id,workflow_version_id,status) VALUES($1,$2,$3,'RUNNING')", []any{f.runs[i], f.workspace, f.version}},
			{"INSERT INTO public.node_runs(id,workflow_run_id,node_definition_id) VALUES($1,$2,$3)", []any{f.nodes[i], f.runs[i], fmt.Sprintf("node-%d", i)}},
			{"INSERT INTO public.tasks(id,workflow_run_id,node_run_id,workspace_id,logical_task_key,task_type,capability) VALUES($1,$2,$3,$4,$5,$6,$7)", []any{f.tasks[i], f.runs[i], f.nodes[i], f.workspace, fmt.Sprintf("task-%d", i), "test", "concurrency-test"}},
		} {
			if _, err := tx.Exec(ctx, s.query, s.args...); err != nil {
				t.Fatalf("创建并发测试数据失败: %v", err)
			}
		}
	}
	// Register before Commit: even an ambiguous commit result gets an exact-row cleanup attempt.
	t.Cleanup(func() { cleanupConcurrencyFixture(t, pool, f) })
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("提交并发测试数据失败，提交结果可能不确定: %v", err)
	}
	return f
}

func cleanupConcurrencyFixture(t *testing.T, pool *pgxpool.Pool, f concurrencyFixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Errorf("清理并发测试数据无法开始事务: %v", err)
		return
	}
	defer rollbackTestTx(t, tx)
	for i := 0; i < f.count; i++ {
		for _, query := range []string{
			"DELETE FROM public.outbox_events WHERE aggregate_type='Task' AND aggregate_id=$1",
			"DELETE FROM public.attempt_result_submissions WHERE task_id=$1",
			"DELETE FROM public.resource_reservations WHERE task_id=$1",
			"UPDATE public.tasks SET current_attempt_id=NULL WHERE id=$1",
			"DELETE FROM public.task_attempts WHERE task_id=$1",
			"DELETE FROM public.tasks WHERE id=$1",
		} {
			if _, err := tx.Exec(ctx, query, f.tasks[i]); err != nil {
				t.Errorf("按任务主键清理并发测试数据失败: %v", err)
				return
			}
		}
		if _, err := tx.Exec(ctx, "DELETE FROM public.node_runs WHERE id=$1", f.nodes[i]); err != nil {
			t.Errorf("清理 NodeRun 失败: %v", err)
			return
		}
		if _, err := tx.Exec(ctx, "DELETE FROM public.workflow_runs WHERE id=$1", f.runs[i]); err != nil {
			t.Errorf("清理 WorkflowRun 失败: %v", err)
			return
		}
	}
	for _, session := range f.sessions {
		if _, err := tx.Exec(ctx, "DELETE FROM public.worker_sessions WHERE id=$1", session); err != nil {
			t.Errorf("清理 WorkerSession 失败: %v", err)
			return
		}
	}
	for _, s := range []struct {
		query string
		id    pgtype.UUID
	}{
		{"DELETE FROM public.resource_pool_counters WHERE pool_id=$1", f.pool},
		{"DELETE FROM public.resource_pools WHERE id=$1", f.pool},
		{"DELETE FROM public.workers WHERE id=$1", f.worker},
		{"DELETE FROM public.workflow_versions WHERE id=$1", f.version},
		{"DELETE FROM public.workspaces WHERE id=$1", f.workspace},
	} {
		if _, err := tx.Exec(ctx, s.query, s.id); err != nil {
			t.Errorf("清理并发测试基础数据失败: %v", err)
			return
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Errorf("提交并发测试数据清理失败: %v", err)
	}
}

// compete holds a row lock on connection 1, observes connection 2 waiting in
// PostgreSQL, then runs the first operation and commits or rolls it back.
func compete[T any](t *testing.T, pool *pgxpool.Pool, lockQuery string, lockID pgtype.UUID,
	firstQuery string, firstArgs []any, secondQuery string, secondArgs []any, commitFirst bool) (T, T) {
	t.Helper()
	var first, second T
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn1, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Release()
	conn2, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	hijacked := false
	defer func() {
		if !hijacked {
			conn2.Release()
		}
	}()
	secondConn := conn2.Conn()
	secondNetworkConn := secondConn.PgConn().Conn()
	tx, err := conn1.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTestTx(t, tx)
	var lockedID pgtype.UUID
	if err := tx.QueryRow(ctx, lockQuery, lockID).Scan(&lockedID); err != nil {
		t.Fatal(err)
	}
	var pid1, pid int32
	if err := tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid1); err != nil {
		t.Fatal(err)
	}
	if err := conn2.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	if pid1 == pid {
		t.Fatal("竞争操作必须使用两个独立的 PostgreSQL 后端连接")
	}
	secondCtx, secondCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer secondCancel()
	type outcome struct {
		value T
		err   error
	}
	done := make(chan outcome, 1)
	received := false
	go func() {
		var value T
		err := secondConn.QueryRow(secondCtx, secondQuery, secondArgs...).Scan(&value)
		done <- outcome{value, err}
	}()
	defer func() {
		// A failed assertion must release the lock before waiting for the other call.
		rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = tx.Rollback(rollbackCtx)
		rollbackCancel()
		if !received {
			secondCancel()
			select {
			case <-done:
				return
			case <-time.After(2 * time.Second):
			}
			// Keep the connection out of the pool before interrupting its socket.
			// The goroutine uses secondConn, not the pgxpool wrapper.
			hijackedConn := conn2.Hijack()
			hijacked = true
			if err := secondNetworkConn.Close(); err != nil {
				t.Errorf("强制关闭竞争连接失败: %v", err)
			}
			select {
			case <-done:
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
				if err := hijackedConn.Close(closeCtx); err != nil && !errors.Is(err, net.ErrClosed) {
					t.Errorf("关闭已脱离连接池的 PostgreSQL 连接失败: %v", err)
				}
				closeCancel()
			case <-time.After(2 * time.Second):
				t.Error("强制关闭后竞争查询仍未退出；连接已脱离连接池，不能安全地并发关闭 pgx.Conn，拒绝复用")
			}
		}
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting bool
		if err := tx.QueryRow(ctx, `SELECT COALESCE(wait_event_type='Lock' AND
			pg_backend_pid()=ANY(pg_blocking_pids($1)),false)
			FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&waiting); err != nil {
			t.Fatalf("观察 PostgreSQL 锁等待失败: %v", err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("第二个物理连接没有进入 PostgreSQL 锁等待")
		}
		select {
		case result := <-done:
			received = true
			t.Fatalf("第二个操作未等待锁即返回: %v", result.err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := tx.QueryRow(ctx, firstQuery, firstArgs...).Scan(&first); err != nil {
		t.Fatalf("持锁操作失败: %v", err)
	}
	if commitFirst {
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("持锁操作提交失败: %v", err)
		}
	} else if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("持锁操作回滚失败: %v", err)
	}
	select {
	case result := <-done:
		received = true
		second = result.value
		if result.err != nil {
			t.Fatalf("等待锁的操作失败: %v", result.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("释放锁后竞争操作未完成")
	}
	return first, second
}

const claimSQL = "SELECT public.mf_claim_task($1,$2,$3,$4,$5,$6,$7)"
const renewSQL = "SELECT public.mf_renew_lease($1,$2,$3,$4,$5)"
const recoverSQL = "SELECT public.mf_recover_expired($1,$2)"
const commitSQL = "SELECT public.mf_commit_result($1,$2,$3,$4,$5,$6,$7,$8,$9)"
const runLockSQL = "SELECT id FROM public.workflow_runs WHERE id=$1 FOR UPDATE"
const poolLockSQL = "SELECT pool_id FROM public.resource_pool_counters WHERE pool_id=$1 FOR UPDATE"

func claimArgs(f concurrencyFixture, task, session int, attempt, reservation pgtype.UUID, seconds int) []any {
	return []any{f.tasks[task], attempt, f.sessions[session], f.pool, reservation, "test-token-hash", seconds}
}

func claimFixtureTask(t *testing.T, pool *pgxpool.Pool, f concurrencyFixture, task, session int, seconds int) pgtype.UUID {
	t.Helper()
	attempt, reservation := testUUID(t), testUUID(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var claimed bool
	if err := pool.QueryRow(ctx, claimSQL, claimArgs(f, task, session, attempt, reservation, seconds)...).Scan(&claimed); err != nil || !claimed {
		t.Fatalf("准备已领取任务失败: claimed=%v err=%v", claimed, err)
	}
	return attempt
}

func assertTaskState(t *testing.T, pool *pgxpool.Pool, task pgtype.UUID, status string, attempt pgtype.UUID, epoch int64, attemptCount int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var gotStatus string
	var gotAttempt pgtype.UUID
	var gotEpoch int64
	var gotCount int32
	if err := pool.QueryRow(ctx, "SELECT status,current_attempt_id,lease_epoch,attempt_count FROM public.tasks WHERE id=$1", task).
		Scan(&gotStatus, &gotAttempt, &gotEpoch, &gotCount); err != nil {
		t.Fatal(err)
	}
	if gotStatus != status || gotAttempt != attempt || gotEpoch != epoch || gotCount != attemptCount {
		t.Fatalf("任务最终状态不符: status=%s attempt=%v epoch=%d count=%d", gotStatus, gotAttempt, gotEpoch, gotCount)
	}
}

func assertPoolState(t *testing.T, pool *pgxpool.Pool, f concurrencyFixture, reserved, active int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var capacity, gotReserved, gotActive int32
	if err := pool.QueryRow(ctx, `SELECT c.capacity_slots,c.reserved_slots,
		(SELECT count(*) FROM public.resource_reservations r WHERE r.pool_id=c.pool_id AND r.status='ACTIVE')
		FROM public.resource_pool_counters c WHERE c.pool_id=$1`, f.pool).Scan(&capacity, &gotReserved, &gotActive); err != nil {
		t.Fatal(err)
	}
	if capacity != 1 || gotReserved != reserved || gotActive != active {
		t.Fatalf("资源最终状态不符: capacity=%d reserved=%d active=%d", capacity, gotReserved, gotActive)
	}
}

func assertEventCount(t *testing.T, pool *pgxpool.Pool, task pgtype.UUID, eventType string, want int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var count int32
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM public.outbox_events WHERE aggregate_type='Task' AND aggregate_id=$1 AND event_type=$2", task, eventType).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("Outbox %s 事件数=%d，期望 %d", eventType, count, want)
	}
}

func TestConcurrentClaimSameTask(t *testing.T) {
	pool := newIntegrationPool(t)
	f := newConcurrencyFixture(t, pool, 1)
	winnerAttempt, winnerReservation := testUUID(t), testUUID(t)
	loserAttempt, loserReservation := testUUID(t), testUUID(t)
	first, second := compete[bool](t, pool, runLockSQL, f.runs[0],
		claimSQL, claimArgs(f, 0, 0, winnerAttempt, winnerReservation, 60),
		claimSQL, claimArgs(f, 0, 1, loserAttempt, loserReservation, 60), true)
	if !first || second {
		t.Fatalf("双 Claim 结果不符: 持锁方=%v 等待方=%v", first, second)
	}
	assertTaskState(t, pool, f.tasks[0], "RUNNING", winnerAttempt, 1, 1)
	assertPoolState(t, pool, f, 1, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var attempts, loserRows, reservations int32
	if err := pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM public.task_attempts WHERE task_id=$1 AND status='RUNNING'),
		(SELECT count(*) FROM public.task_attempts WHERE id=$2),
		(SELECT count(*) FROM public.resource_reservations WHERE task_id=$1 AND status='ACTIVE')`, f.tasks[0], loserAttempt).
		Scan(&attempts, &loserRows, &reservations); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || loserRows != 0 || reservations != 1 {
		t.Fatalf("双 Claim 最终记录不符: active_attempts=%d loser_rows=%d reservations=%d", attempts, loserRows, reservations)
	}
	assertEventCount(t, pool, f.tasks[0], "TaskSucceeded", 0)
}

func TestClaimRollbackAllowsWaitingClaim(t *testing.T) {
	pool := newIntegrationPool(t)
	f := newConcurrencyFixture(t, pool, 1)
	rolledBackAttempt, rolledBackReservation := testUUID(t), testUUID(t)
	winnerAttempt, winnerReservation := testUUID(t), testUUID(t)
	first, second := compete[bool](t, pool, runLockSQL, f.runs[0],
		claimSQL, claimArgs(f, 0, 0, rolledBackAttempt, rolledBackReservation, 60),
		claimSQL, claimArgs(f, 0, 1, winnerAttempt, winnerReservation, 60), false)
	if !first || !second {
		t.Fatalf("回滚后等待方应成功领取: rolled_back=%v winner=%v", first, second)
	}
	assertTaskState(t, pool, f.tasks[0], "RUNNING", winnerAttempt, 1, 1)
	assertPoolState(t, pool, f, 1, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var abandonedAttempts, abandonedReservations int32
	if err := pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM public.task_attempts WHERE id=$1),
		(SELECT count(*) FROM public.resource_reservations WHERE id=$2)`, rolledBackAttempt, rolledBackReservation).
		Scan(&abandonedAttempts, &abandonedReservations); err != nil {
		t.Fatal(err)
	}
	if abandonedAttempts != 0 || abandonedReservations != 0 {
		t.Fatalf("回滚的 Claim 留下数据: attempts=%d reservations=%d", abandonedAttempts, abandonedReservations)
	}
}

func TestConcurrentClaimRespectsPoolCapacity(t *testing.T) {
	pool := newIntegrationPool(t)
	f := newConcurrencyFixture(t, pool, 2)
	firstAttempt, firstReservation := testUUID(t), testUUID(t)
	secondAttempt, secondReservation := testUUID(t), testUUID(t)
	// mf_claim_task locks run, task, and session before updating the counter.
	// Those rows differ across claims; the shared resource_pools row is only read.
	// Thus the waiting claim holds no row the first claim needs before the counter.
	first, second := compete[bool](t, pool, poolLockSQL, f.pool,
		claimSQL, claimArgs(f, 0, 0, firstAttempt, firstReservation, 60),
		claimSQL, claimArgs(f, 1, 1, secondAttempt, secondReservation, 60), true)
	if !first || second {
		t.Fatalf("容量 1 的双 Claim 结果不符: 持锁方=%v 等待方=%v", first, second)
	}
	assertTaskState(t, pool, f.tasks[0], "RUNNING", firstAttempt, 1, 1)
	assertTaskState(t, pool, f.tasks[1], "READY", pgtype.UUID{}, 0, 0)
	assertPoolState(t, pool, f, 1, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var failedAttempts, failedReservations int32
	if err := pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM public.task_attempts WHERE task_id=$1),
		(SELECT count(*) FROM public.resource_reservations WHERE task_id=$1)`, f.tasks[1]).
		Scan(&failedAttempts, &failedReservations); err != nil {
		t.Fatal(err)
	}
	if failedAttempts != 0 || failedReservations != 0 {
		t.Fatalf("失败 Claim 遗留数据: attempts=%d reservations=%d", failedAttempts, failedReservations)
	}
	var commitResult string
	if err := pool.QueryRow(ctx, commitSQL, commitArgs(f.tasks[0], firstAttempt, 1, testUUID(t), testUUID(t))...).Scan(&commitResult); err != nil || commitResult != "COMMITTED" {
		t.Fatalf("释放已占用资源失败: result=%s err=%v", commitResult, err)
	}
	assertPoolState(t, pool, f, 0, 0)
	var retried bool
	if err := pool.QueryRow(ctx, claimSQL, claimArgs(f, 1, 1, secondAttempt, secondReservation, 60)...).Scan(&retried); err != nil || !retried {
		t.Fatalf("资源释放后重试 Claim 失败: claimed=%v err=%v", retried, err)
	}
	assertTaskState(t, pool, f.tasks[0], "SUCCEEDED", firstAttempt, 1, 1)
	assertTaskState(t, pool, f.tasks[1], "RUNNING", secondAttempt, 1, 1)
	assertPoolState(t, pool, f, 1, 1)
	assertEventCount(t, pool, f.tasks[0], "TaskSucceeded", 1)
}

func expireAttempt(t *testing.T, pool *pgxpool.Pool, attempt pgtype.UUID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tag, err := pool.Exec(ctx, "UPDATE public.task_attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", attempt)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("设置测试 Attempt 过期失败: affected=%d err=%v", tag.RowsAffected(), err)
	}
}

func assertAttemptAndReservation(t *testing.T, pool *pgxpool.Pool, attempt pgtype.UUID, attemptStatus, reservationStatus string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var gotAttempt, gotReservation string
	if err := pool.QueryRow(ctx, `SELECT a.status,r.status FROM public.task_attempts a
		JOIN public.resource_reservations r ON r.attempt_id=a.id WHERE a.id=$1`, attempt).
		Scan(&gotAttempt, &gotReservation); err != nil {
		t.Fatal(err)
	}
	if gotAttempt != attemptStatus || gotReservation != reservationStatus {
		t.Fatalf("Attempt/预留状态不符: attempt=%s reservation=%s", gotAttempt, gotReservation)
	}
}

func assertOutboxEvent(t *testing.T, pool *pgxpool.Pool, task, attempt, event pgtype.UUID, eventType string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var gotType, gotAggregate string
	var gotTask, gotAttempt pgtype.UUID
	if err := pool.QueryRow(ctx, `SELECT aggregate_type,event_type,aggregate_id,payload->>'attempt_id'
		FROM public.outbox_events WHERE id=$1`, event).
		Scan(&gotAggregate, &gotType, &gotTask, &gotAttempt); err != nil {
		t.Fatal(err)
	}
	if gotAggregate != "Task" || gotType != eventType || gotTask != task || gotAttempt != attempt {
		t.Fatalf("Outbox 事件关联不符: aggregate=%s type=%s task=%v attempt=%v", gotAggregate, gotType, gotTask, gotAttempt)
	}
}

func TestConcurrentRenewAndRecover(t *testing.T) {
	t.Run("renew wins", func(t *testing.T) {
		pool := newIntegrationPool(t)
		f := newConcurrencyFixture(t, pool, 1)
		attempt := claimFixtureTask(t, pool, f, 0, 0, 60)
		event := testUUID(t)
		renewed, recovered := compete[bool](t, pool, runLockSQL, f.runs[0],
			renewSQL, []any{f.tasks[0], attempt, int64(1), "test-token-hash", 60},
			recoverSQL, []any{f.tasks[0], event}, true)
		if !renewed || recovered {
			t.Fatalf("续租先取得锁时结果不符: renewed=%v recovered=%v", renewed, recovered)
		}
		assertTaskState(t, pool, f.tasks[0], "RUNNING", attempt, 1, 1)
		assertAttemptAndReservation(t, pool, attempt, "RUNNING", "ACTIVE")
		assertPoolState(t, pool, f, 1, 1)
		assertEventCount(t, pool, f.tasks[0], "TaskLeaseExpired", 0)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var leaseValid bool
		if err := pool.QueryRow(ctx, "SELECT lease_expires_at>clock_timestamp() FROM public.task_attempts WHERE id=$1", attempt).Scan(&leaseValid); err != nil || !leaseValid {
			t.Fatalf("续租后租约应有效: valid=%v err=%v", leaseValid, err)
		}
	})
	t.Run("recovery wins", func(t *testing.T) {
		pool := newIntegrationPool(t)
		f := newConcurrencyFixture(t, pool, 1)
		oldAttempt := claimFixtureTask(t, pool, f, 0, 0, 60)
		expireAttempt(t, pool, oldAttempt)
		event := testUUID(t)
		recovered, renewed := compete[bool](t, pool, runLockSQL, f.runs[0],
			recoverSQL, []any{f.tasks[0], event},
			renewSQL, []any{f.tasks[0], oldAttempt, int64(1), "test-token-hash", 60}, true)
		if !recovered || renewed {
			t.Fatalf("恢复先取得锁时结果不符: recovered=%v renewed=%v", recovered, renewed)
		}
		assertTaskState(t, pool, f.tasks[0], "READY", pgtype.UUID{}, 1, 1)
		assertAttemptAndReservation(t, pool, oldAttempt, "TIMED_OUT", "RELEASED")
		assertPoolState(t, pool, f, 0, 0)
		assertEventCount(t, pool, f.tasks[0], "TaskLeaseExpired", 1)
		assertOutboxEvent(t, pool, f.tasks[0], oldAttempt, event, "TaskLeaseExpired")
		newAttempt := claimFixtureTask(t, pool, f, 0, 1, 60)
		assertTaskState(t, pool, f.tasks[0], "RUNNING", newAttempt, 2, 2)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var oldResult string
		if err := pool.QueryRow(ctx, commitSQL, commitArgs(f.tasks[0], oldAttempt, 1, testUUID(t), testUUID(t))...).Scan(&oldResult); err != nil || oldResult != "REJECTED" {
			t.Fatalf("旧 Attempt 绕过 fencing: result=%s err=%v", oldResult, err)
		}
		assertTaskState(t, pool, f.tasks[0], "RUNNING", newAttempt, 2, 2)
		assertPoolState(t, pool, f, 1, 1)
		assertEventCount(t, pool, f.tasks[0], "TaskSucceeded", 0)
	})
}

func commitArgs(task, attempt pgtype.UUID, epoch int64, submission, event pgtype.UUID) []any {
	return []any{task, attempt, epoch, "test-token-hash", submission, "submission-key", "verified-manifest-hash",
		json.RawMessage(`{"asset":"verified-test-ref"}`), event}
}

func TestConcurrentCommitAndRecover(t *testing.T) {
	t.Run("commit wins and replay", func(t *testing.T) {
		pool := newIntegrationPool(t)
		f := newConcurrencyFixture(t, pool, 1)
		attempt := claimFixtureTask(t, pool, f, 0, 0, 60)
		submission, event, ignoredEvent := testUUID(t), testUUID(t), testUUID(t)
		committed, recovered := compete[string](t, pool, runLockSQL, f.runs[0],
			commitSQL, commitArgs(f.tasks[0], attempt, 1, submission, event),
			"SELECT public.mf_recover_expired($1,$2)::text", []any{f.tasks[0], ignoredEvent}, true)
		if committed != "COMMITTED" || recovered != "false" {
			t.Fatalf("提交先取得锁时结果不符: committed=%s recovered=%s", committed, recovered)
		}
		assertTaskState(t, pool, f.tasks[0], "SUCCEEDED", attempt, 1, 1)
		assertAttemptAndReservation(t, pool, attempt, "SUCCEEDED", "RELEASED")
		assertPoolState(t, pool, f, 0, 0)
		assertOutboxEvent(t, pool, f.tasks[0], attempt, event, "TaskSucceeded")
		assertEventCount(t, pool, f.tasks[0], "TaskSucceeded", 1)
		assertEventCount(t, pool, f.tasks[0], "TaskLeaseExpired", 0)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var replay string
		if err := pool.QueryRow(ctx, commitSQL, commitArgs(f.tasks[0], attempt, 1, testUUID(t), testUUID(t))...).Scan(&replay); err != nil || replay != "REPLAY" {
			t.Fatalf("重复提交结果不符: result=%s err=%v", replay, err)
		}
		conflictArgs := commitArgs(f.tasks[0], attempt, 1, testUUID(t), testUUID(t))
		conflictArgs[6] = "different-manifest-hash"
		var conflictResult string
		var pgErr *pgconn.PgError
		if err := pool.QueryRow(ctx, commitSQL, conflictArgs...).Scan(&conflictResult); !errors.As(err, &pgErr) || pgErr.Message != "submission key conflict" {
			t.Fatalf("同提交键不同摘要应冲突，实际结果=%s err=%v", conflictResult, err)
		}
		var submissionCount int32
		var resultMatches bool
		if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM public.attempt_result_submissions WHERE attempt_id=$1 AND status='COMMITTED'),
			result_ref=$3::jsonb FROM public.tasks WHERE id=$2`, attempt, f.tasks[0], json.RawMessage(`{"asset":"verified-test-ref"}`)).Scan(&submissionCount, &resultMatches); err != nil {
			t.Fatal(err)
		}
		if submissionCount != 1 || !resultMatches {
			t.Fatalf("重复提交后结果记录不符: committed_submissions=%d result_matches=%v", submissionCount, resultMatches)
		}
		assertEventCount(t, pool, f.tasks[0], "TaskSucceeded", 1)
		assertPoolState(t, pool, f, 0, 0)
	})
	t.Run("recovery wins", func(t *testing.T) {
		pool := newIntegrationPool(t)
		f := newConcurrencyFixture(t, pool, 1)
		attempt := claimFixtureTask(t, pool, f, 0, 0, 60)
		expireAttempt(t, pool, attempt)
		event, submission, rejectedEvent := testUUID(t), testUUID(t), testUUID(t)
		recovered, result := compete[string](t, pool, runLockSQL, f.runs[0],
			"SELECT public.mf_recover_expired($1,$2)::text", []any{f.tasks[0], event},
			commitSQL, commitArgs(f.tasks[0], attempt, 1, submission, rejectedEvent), true)
		if recovered != "true" || result != "REJECTED" {
			t.Fatalf("恢复先取得锁时结果不符: recovered=%s result=%s", recovered, result)
		}
		assertTaskState(t, pool, f.tasks[0], "READY", pgtype.UUID{}, 1, 1)
		assertAttemptAndReservation(t, pool, attempt, "TIMED_OUT", "RELEASED")
		assertPoolState(t, pool, f, 0, 0)
		assertOutboxEvent(t, pool, f.tasks[0], attempt, event, "TaskLeaseExpired")
		assertEventCount(t, pool, f.tasks[0], "TaskLeaseExpired", 1)
		assertEventCount(t, pool, f.tasks[0], "TaskSucceeded", 0)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var submissions int32
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM public.attempt_result_submissions WHERE attempt_id=$1", attempt).Scan(&submissions); err != nil || submissions != 0 {
			t.Fatalf("失败提交不应留下结果记录: count=%d err=%v", submissions, err)
		}
	})
}
