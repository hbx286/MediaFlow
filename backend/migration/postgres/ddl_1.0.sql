-- MediaFlow 执行子系统：Schema 定义（PostgreSQL 16+）
-- 每张表的 CREATE TABLE 后紧跟本表和字段的 COMMENT ON；索引与函数在相应定义之后。
-- 本文件仅包含 DDL 和数据库函数，不含业务初始化数据。
-- 注意：这不是 MediaFlow 所有产品领域的完整表结构，仅涵盖目前设计的执行子系统。
-- 仅用于全新空库，不能重复在已初始化数据库中执行。

BEGIN;

CREATE TABLE workspaces (id uuid PRIMARY KEY, name text NOT NULL);
COMMENT ON TABLE public.workspaces IS '工作空间：用户与项目资源的租户隔离边界';
COMMENT ON COLUMN public.workspaces.id IS '主键标识（UUID）';
COMMENT ON COLUMN public.workspaces.name IS '名称';

CREATE TABLE workflow_versions (id uuid PRIMARY KEY, definition jsonb NOT NULL DEFAULT '{}'::jsonb);
COMMENT ON TABLE public.workflow_versions IS '已发布工作流的不可变版本及DAG定义';
COMMENT ON COLUMN public.workflow_versions.id IS '主键标识（UUID）';
COMMENT ON COLUMN public.workflow_versions.definition IS '工作流版本定义快照（JSON）';

CREATE TABLE workflow_runs (
 id uuid PRIMARY KEY, workspace_id uuid NOT NULL REFERENCES workspaces(id),
 workflow_version_id uuid NOT NULL REFERENCES workflow_versions(id),
 status text NOT NULL DEFAULT 'CREATED' CHECK(status IN ('CREATED','RUNNING','WAITING','PAUSED','CANCELING','CANCELED','SUCCEEDED','FAILED')),
 input_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb, pause_requested_at timestamptz, cancel_requested_at timestamptz,
 lock_version bigint NOT NULL DEFAULT 0, created_at timestamptz NOT NULL DEFAULT clock_timestamp(), UNIQUE(id,workspace_id)
);
COMMENT ON TABLE public.workflow_runs IS '工作流单次执行实例，保存输入快照与总体状态';
COMMENT ON COLUMN public.workflow_runs.id IS '主键标识（UUID）';
COMMENT ON COLUMN public.workflow_runs.workspace_id IS '所属工作空间ID，用于租户归属和隔离';
COMMENT ON COLUMN public.workflow_runs.workflow_version_id IS '本次运行绑定的工作流定义版本ID';
COMMENT ON COLUMN public.workflow_runs.status IS '工作流整体状态；由Workflow Engine负责推进';
COMMENT ON COLUMN public.workflow_runs.input_snapshot IS '本次运行所使用的不可变输入快照';
COMMENT ON COLUMN public.workflow_runs.pause_requested_at IS '用户申请暂停的时间；不代表任务已经暂停';
COMMENT ON COLUMN public.workflow_runs.cancel_requested_at IS '用户申请取消的时间；不代表任务已经终止';
COMMENT ON COLUMN public.workflow_runs.lock_version IS '乐观锁版本号，写入时用于并发冲突检测';
COMMENT ON COLUMN public.workflow_runs.created_at IS '记录创建时间（带时区）';

CREATE TABLE node_runs (
 id uuid PRIMARY KEY, workflow_run_id uuid NOT NULL REFERENCES workflow_runs(id),
 node_definition_id text NOT NULL, expansion_key text NOT NULL DEFAULT '',
 status text NOT NULL DEFAULT 'PENDING' CHECK(status IN ('PENDING','READY','RUNNING','WAITING','SUCCEEDED','FAILED','SKIPPED','BLOCKED','CANCELED')),
 lock_version bigint NOT NULL DEFAULT 0, output_ref jsonb,
 UNIQUE(workflow_run_id,node_definition_id,expansion_key), UNIQUE(id,workflow_run_id)
);
COMMENT ON TABLE public.node_runs IS '工作流运行中的节点实例及其执行状态';
COMMENT ON COLUMN public.node_runs.id IS '主键标识（UUID）';
COMMENT ON COLUMN public.node_runs.workflow_run_id IS '所属工作流运行实例ID';
COMMENT ON COLUMN public.node_runs.node_definition_id IS '工作流版本中的逻辑节点标识';
COMMENT ON COLUMN public.node_runs.expansion_key IS '动态展开项的稳定标识；普通节点使用空字符串';
COMMENT ON COLUMN public.node_runs.status IS '节点运行状态；依据依赖决议和执行结果变更';
COMMENT ON COLUMN public.node_runs.lock_version IS '乐观锁版本号，写入时用于并发冲突检测';
COMMENT ON COLUMN public.node_runs.output_ref IS '节点输出引用；大型产物应引用稳定资产版本';

CREATE TABLE workers (id uuid PRIMARY KEY, name text NOT NULL);
COMMENT ON TABLE public.workers IS 'Worker稳定身份';
COMMENT ON COLUMN public.workers.id IS '主键标识（UUID）';
COMMENT ON COLUMN public.workers.name IS '名称';

CREATE TABLE worker_sessions (
 id uuid PRIMARY KEY, worker_id uuid NOT NULL REFERENCES workers(id),
 status text NOT NULL CHECK(status IN ('ACTIVE','DRAINING','EXPIRED','CLOSED')),
 heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(), capabilities jsonb NOT NULL DEFAULT '[]'::jsonb
);
COMMENT ON TABLE public.worker_sessions IS 'Worker单次启动会话，记录存活状态与执行能力';
COMMENT ON COLUMN public.worker_sessions.id IS '主键标识（UUID）';
COMMENT ON COLUMN public.worker_sessions.worker_id IS 'Worker稳定身份ID';
COMMENT ON COLUMN public.worker_sessions.status IS 'Worker会话状态：ACTIVE、DRAINING、EXPIRED、CLOSED';
COMMENT ON COLUMN public.worker_sessions.heartbeat_at IS '最近Worker会话心跳时间';
COMMENT ON COLUMN public.worker_sessions.capabilities IS 'Worker会话声明的可执行能力集合';

CREATE TABLE tasks (
 id uuid PRIMARY KEY, workflow_run_id uuid NOT NULL, node_run_id uuid NOT NULL,
 workspace_id uuid NOT NULL, logical_task_key text NOT NULL CHECK(length(logical_task_key)>0),
 task_type text NOT NULL, capability text NOT NULL,
 status text NOT NULL DEFAULT 'READY' CHECK(status IN ('PENDING','READY','QUEUED','RUNNING','RETRY_WAIT','SUCCEEDED','FAILED','CANCELED')),
 priority integer NOT NULL DEFAULT 0, next_run_at timestamptz, timeout_ms bigint NOT NULL DEFAULT 60000 CHECK(timeout_ms>0),
 attempt_count integer NOT NULL DEFAULT 0 CHECK(attempt_count>=0), lease_epoch bigint NOT NULL DEFAULT 0 CHECK(lease_epoch>=0),
 current_attempt_id uuid, result_ref jsonb, lock_version bigint NOT NULL DEFAULT 0,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(node_run_id,logical_task_key), UNIQUE(id,workflow_run_id),
 FOREIGN KEY(node_run_id,workflow_run_id) REFERENCES node_runs(id,workflow_run_id),
 FOREIGN KEY(workflow_run_id,workspace_id) REFERENCES workflow_runs(id,workspace_id)
);
COMMENT ON TABLE public.tasks IS '可调度任务及当前有效执行授权';
COMMENT ON COLUMN public.tasks.id IS '主键标识（UUID）';
COMMENT ON COLUMN public.tasks.workflow_run_id IS '所属工作流运行实例ID';
COMMENT ON COLUMN public.tasks.node_run_id IS '所属节点运行实例ID';
COMMENT ON COLUMN public.tasks.workspace_id IS '所属工作空间ID，用于租户归属和隔离';
COMMENT ON COLUMN public.tasks.logical_task_key IS '节点范围内的逻辑任务去重键';
COMMENT ON COLUMN public.tasks.task_type IS '业务任务类型，用于选择Handler';
COMMENT ON COLUMN public.tasks.capability IS '执行所需能力标识，用于匹配资源池和Worker';
COMMENT ON COLUMN public.tasks.status IS '任务生命周期状态；变更由Task Execution负责，禁止Worker直接修改';
COMMENT ON COLUMN public.tasks.priority IS '调度优先级；数值越大通常优先级越高';
COMMENT ON COLUMN public.tasks.next_run_at IS '任务下次允许调度的时间，空值代表无需延迟';
COMMENT ON COLUMN public.tasks.timeout_ms IS '任务超时配置，单位毫秒';
COMMENT ON COLUMN public.tasks.attempt_count IS '累计已经创建的任务尝试次数';
COMMENT ON COLUMN public.tasks.lease_epoch IS '单调递增的执行代数，用于拒绝过期Attempt';
COMMENT ON COLUMN public.tasks.current_attempt_id IS '当前被认可的执行尝试ID；未分配时为空';
COMMENT ON COLUMN public.tasks.result_ref IS '任务已确认的最终输出引用';
COMMENT ON COLUMN public.tasks.lock_version IS '乐观锁版本号，写入时用于并发冲突检测';
COMMENT ON COLUMN public.tasks.created_at IS '记录创建时间（带时区）';
COMMENT ON COLUMN public.tasks.updated_at IS '记录最后更新时间（带时区）';

CREATE INDEX ix_tasks_ready ON tasks(priority DESC,created_at,id) WHERE status IN ('READY','QUEUED');
CREATE TABLE task_attempts (
 id uuid PRIMARY KEY, task_id uuid NOT NULL REFERENCES tasks(id), attempt_no integer NOT NULL CHECK(attempt_no>0),
 worker_session_id uuid NOT NULL REFERENCES worker_sessions(id), lease_epoch bigint NOT NULL CHECK(lease_epoch>0),
 lease_token_hash text NOT NULL CHECK(length(lease_token_hash)>0), lease_expires_at timestamptz NOT NULL,
 status text NOT NULL CHECK(status IN ('CREATED','RUNNING','SUCCEEDED','FAILED','TIMED_OUT','ABANDONED')),
 result_manifest jsonb, started_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
 UNIQUE(task_id,attempt_no), UNIQUE(task_id,lease_epoch), UNIQUE(task_id,id)
);
COMMENT ON TABLE public.task_attempts IS '任务的具体执行尝试，保存租约与执行结果';
COMMENT ON COLUMN public.task_attempts.id IS '主键标识（UUID）';
COMMENT ON COLUMN public.task_attempts.task_id IS '所属任务ID';
COMMENT ON COLUMN public.task_attempts.attempt_no IS 'Task内从1递增的尝试序号';
COMMENT ON COLUMN public.task_attempts.worker_session_id IS '执行任务的Worker会话ID';
COMMENT ON COLUMN public.task_attempts.lease_epoch IS '单调递增的执行代数，用于拒绝过期Attempt';
COMMENT ON COLUMN public.task_attempts.lease_token_hash IS '租约令牌的哈希值；不存储原始令牌';
COMMENT ON COLUMN public.task_attempts.lease_expires_at IS '当前Attempt租约有效截止时间';
COMMENT ON COLUMN public.task_attempts.status IS '单次执行尝试状态；与Task总体状态独立';
COMMENT ON COLUMN public.task_attempts.result_manifest IS 'Attempt执行输出清单，必须按提交协议校验';
COMMENT ON COLUMN public.task_attempts.started_at IS '实际开始时间（带时区）';
COMMENT ON COLUMN public.task_attempts.finished_at IS '终止时间（带时区）';

CREATE UNIQUE INDEX ux_attempt_active ON task_attempts(task_id) WHERE status IN ('CREATED','RUNNING');
CREATE INDEX ix_attempt_expiry ON task_attempts(lease_expires_at) WHERE status IN ('CREATED','RUNNING');
ALTER TABLE tasks ADD CONSTRAINT fk_current_attempt FOREIGN KEY(id,current_attempt_id) REFERENCES task_attempts(task_id,id);
CREATE TABLE resource_pools (id uuid PRIMARY KEY, capability text NOT NULL);
COMMENT ON TABLE public.resource_pools IS '按能力划分的执行资源池';
COMMENT ON COLUMN public.resource_pools.id IS '主键标识（UUID）';
COMMENT ON COLUMN public.resource_pools.capability IS '执行所需能力标识，用于匹配资源池和Worker';

CREATE TABLE resource_pool_counters (
 pool_id uuid PRIMARY KEY REFERENCES resource_pools(id),
 capacity_slots integer NOT NULL CHECK(capacity_slots>=0), reserved_slots integer NOT NULL DEFAULT 0 CHECK(reserved_slots>=0),
 lock_version bigint NOT NULL DEFAULT 0, CHECK(reserved_slots<=capacity_slots)
);
COMMENT ON TABLE public.resource_pool_counters IS '资源池总槽位与已预留槽位计数';
COMMENT ON COLUMN public.resource_pool_counters.pool_id IS '资源池ID';
COMMENT ON COLUMN public.resource_pool_counters.capacity_slots IS '资源池可分配的总并发槽数';
COMMENT ON COLUMN public.resource_pool_counters.reserved_slots IS '已被有效任务尝试预留的槽数';
COMMENT ON COLUMN public.resource_pool_counters.lock_version IS '乐观锁版本号，写入时用于并发冲突检测';

CREATE TABLE resource_reservations (
 id uuid PRIMARY KEY, task_id uuid NOT NULL, attempt_id uuid NOT NULL,
 worker_session_id uuid NOT NULL REFERENCES worker_sessions(id), pool_id uuid NOT NULL REFERENCES resource_pool_counters(pool_id),
 slots integer NOT NULL CHECK(slots>0), status text NOT NULL CHECK(status IN ('ACTIVE','RELEASED')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), released_at timestamptz,
 UNIQUE(attempt_id,pool_id), FOREIGN KEY(task_id,attempt_id) REFERENCES task_attempts(task_id,id),
 CHECK ((status='RELEASED')=(released_at IS NOT NULL))
);
COMMENT ON TABLE public.resource_reservations IS '任务尝试持有的资源预留及释放记录';
COMMENT ON COLUMN public.resource_reservations.id IS '主键标识（UUID）';
COMMENT ON COLUMN public.resource_reservations.task_id IS '所属任务ID';
COMMENT ON COLUMN public.resource_reservations.attempt_id IS '执行尝试ID';
COMMENT ON COLUMN public.resource_reservations.worker_session_id IS '执行任务的Worker会话ID';
COMMENT ON COLUMN public.resource_reservations.pool_id IS '资源池ID';
COMMENT ON COLUMN public.resource_reservations.slots IS '本次预留占用的资源槽数量';
COMMENT ON COLUMN public.resource_reservations.status IS '资源预留状态：ACTIVE在用、RELEASED已释放';
COMMENT ON COLUMN public.resource_reservations.created_at IS '记录创建时间（带时区）';
COMMENT ON COLUMN public.resource_reservations.released_at IS '资源预留实际释放的时间';

CREATE INDEX ix_reservations_active ON resource_reservations(pool_id) WHERE status='ACTIVE';
CREATE TABLE attempt_result_submissions (
 id uuid PRIMARY KEY, task_id uuid NOT NULL, attempt_id uuid NOT NULL, submission_key text NOT NULL,
 manifest_hash text NOT NULL, status text NOT NULL CHECK(status IN ('RECEIVED','COMMITTED','REJECTED')),
 committed_result_ref jsonb, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(attempt_id,submission_key), FOREIGN KEY(task_id,attempt_id) REFERENCES task_attempts(task_id,id)
);
COMMENT ON TABLE public.attempt_result_submissions IS '任务结果提交请求及幂等确认记录';
COMMENT ON COLUMN public.attempt_result_submissions.id IS '主键标识（UUID）';
COMMENT ON COLUMN public.attempt_result_submissions.task_id IS '所属任务ID';
COMMENT ON COLUMN public.attempt_result_submissions.attempt_id IS '执行尝试ID';
COMMENT ON COLUMN public.attempt_result_submissions.submission_key IS '调用方提供的幂等结果提交键';
COMMENT ON COLUMN public.attempt_result_submissions.manifest_hash IS '结果清单摘要，用于识别同键不同内容冲突';
COMMENT ON COLUMN public.attempt_result_submissions.status IS '结果请求状态：RECEIVED、COMMITTED或REJECTED';
COMMENT ON COLUMN public.attempt_result_submissions.committed_result_ref IS '通过校验并正式提交的结果引用';
COMMENT ON COLUMN public.attempt_result_submissions.created_at IS '记录创建时间（带时区）';

CREATE TABLE node_dependency_decisions (
 workflow_run_id uuid NOT NULL REFERENCES workflow_runs(id), target_node_run_id uuid NOT NULL,
 dependency_key text NOT NULL, source_node_run_id uuid NOT NULL, source_outcome_version bigint NOT NULL CHECK(source_outcome_version>=0),
 outcome text NOT NULL CHECK(outcome IN ('SATISFIED','NOT_SELECTED','IMPOSSIBLE')),
 PRIMARY KEY(target_node_run_id,dependency_key),
 FOREIGN KEY(target_node_run_id,workflow_run_id) REFERENCES node_runs(id,workflow_run_id),
 FOREIGN KEY(source_node_run_id,workflow_run_id) REFERENCES node_runs(id,workflow_run_id)
);
COMMENT ON TABLE public.node_dependency_decisions IS '节点依赖决议，区分满足、未选中及不可能满足';
COMMENT ON COLUMN public.node_dependency_decisions.workflow_run_id IS '所属工作流运行实例ID';
COMMENT ON COLUMN public.node_dependency_decisions.target_node_run_id IS '依赖关系的目标节点运行实例ID';
COMMENT ON COLUMN public.node_dependency_decisions.dependency_key IS '目标节点内唯一的依赖边标识';
COMMENT ON COLUMN public.node_dependency_decisions.source_node_run_id IS '依赖来源节点运行实例ID';
COMMENT ON COLUMN public.node_dependency_decisions.source_outcome_version IS '依赖来源结果的版本号，用于防重复和追踪';
COMMENT ON COLUMN public.node_dependency_decisions.outcome IS '依赖决议：满足、分支未选择或不可满足';

CREATE TABLE node_expansions (
 id uuid PRIMARY KEY, workflow_run_id uuid NOT NULL REFERENCES workflow_runs(id), parent_node_run_id uuid NOT NULL,
 expansion_key text NOT NULL, source_fingerprint text NOT NULL, expansion_spec jsonb NOT NULL,
 status text NOT NULL CHECK(status IN ('PENDING','APPLIED','FAILED')),
 UNIQUE(parent_node_run_id,expansion_key), FOREIGN KEY(parent_node_run_id,workflow_run_id) REFERENCES node_runs(id,workflow_run_id)
);
COMMENT ON TABLE public.node_expansions IS '工作流动态节点展开的持久化记录';
COMMENT ON COLUMN public.node_expansions.id IS '主键标识（UUID）';
COMMENT ON COLUMN public.node_expansions.workflow_run_id IS '所属工作流运行实例ID';
COMMENT ON COLUMN public.node_expansions.parent_node_run_id IS '动态展开来源的父节点实例ID';
COMMENT ON COLUMN public.node_expansions.expansion_key IS '动态展开项的稳定标识；普通节点使用空字符串';
COMMENT ON COLUMN public.node_expansions.source_fingerprint IS '展开输入与参数指纹，用于识别重复或冲突展开';
COMMENT ON COLUMN public.node_expansions.expansion_spec IS '动态展开的具体定义快照（JSON）';
COMMENT ON COLUMN public.node_expansions.status IS '动态展开状态：PENDING、APPLIED或FAILED';

CREATE TABLE outbox_events (
 id uuid PRIMARY KEY, aggregate_type text NOT NULL, aggregate_id uuid NOT NULL, event_type text NOT NULL,
 payload jsonb NOT NULL, occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(), published_at timestamptz
);
COMMENT ON TABLE public.outbox_events IS '本地事务内写入的待发布领域事件';
COMMENT ON COLUMN public.outbox_events.id IS '主键标识（UUID）';
COMMENT ON COLUMN public.outbox_events.aggregate_type IS '产生事件的聚合类型';
COMMENT ON COLUMN public.outbox_events.aggregate_id IS '产生事件的聚合ID';
COMMENT ON COLUMN public.outbox_events.event_type IS '领域事件类型';
COMMENT ON COLUMN public.outbox_events.payload IS '事件载荷（JSON）';
COMMENT ON COLUMN public.outbox_events.occurred_at IS '事件实际产生时间';
COMMENT ON COLUMN public.outbox_events.published_at IS '事件成功发布的时间；空值表示未确认发布';

CREATE TABLE inbox_receipts (consumer_name text NOT NULL, event_id uuid NOT NULL, processed_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(consumer_name,event_id));
COMMENT ON TABLE public.inbox_receipts IS '事件消费者已处理事件的去重凭据';
COMMENT ON COLUMN public.inbox_receipts.consumer_name IS '事件消费者的稳定名称';
COMMENT ON COLUMN public.inbox_receipts.event_id IS '被消费的领域事件ID';
COMMENT ON COLUMN public.inbox_receipts.processed_at IS '消费者成功处理事件的时间';


-- Atomic scheduling primitives; worker network/AI/FFmpeg calls MUST remain outside DB transactions.

-- Caller provides UUIDs (generated by Go); token HASH only, never store plaintext lease token.
CREATE FUNCTION mf_claim_task(p_task uuid,p_attempt uuid,p_session uuid,p_pool uuid,p_reservation uuid,
 p_token_hash text,p_lease_seconds integer DEFAULT 60)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE v_run uuid; v_space uuid; v_epoch bigint; v_count integer; v_cap text;
BEGIN
 IF p_lease_seconds<=0 THEN RAISE EXCEPTION 'invalid lease duration'; END IF;
 -- Lock global run first, then task (consistent with cancel/recovery).
 SELECT workflow_run_id INTO v_run FROM tasks WHERE id=p_task;
 IF v_run IS NULL THEN RETURN false; END IF;
 PERFORM 1 FROM workflow_runs WHERE id=v_run AND status='RUNNING' AND cancel_requested_at IS NULL AND pause_requested_at IS NULL FOR UPDATE;
 IF NOT FOUND THEN RETURN false; END IF;
 PERFORM 1 FROM tasks WHERE id=p_task FOR UPDATE;
 IF NOT FOUND THEN RETURN false; END IF;
 SELECT lease_epoch,attempt_count,capability INTO v_epoch,v_count,v_cap FROM tasks
 WHERE id=p_task AND status IN ('READY','QUEUED') AND (next_run_at IS NULL OR next_run_at<=clock_timestamp());
 IF NOT FOUND THEN RETURN false; END IF;
 PERFORM 1 FROM worker_sessions WHERE id=p_session AND status='ACTIVE' FOR UPDATE;
 IF NOT FOUND THEN RETURN false; END IF;
 PERFORM 1 FROM resource_pools WHERE id=p_pool AND capability=v_cap;
 IF NOT FOUND THEN RETURN false; END IF;
 -- Atomically reserve the slot; pool counter row lock serializes competing Claims.
 UPDATE resource_pool_counters SET reserved_slots=reserved_slots+1,lock_version=lock_version+1
 WHERE pool_id=p_pool AND reserved_slots<capacity_slots;
 IF NOT FOUND THEN RETURN false; END IF;
 INSERT INTO task_attempts(id,task_id,attempt_no,worker_session_id,lease_epoch,lease_token_hash,lease_expires_at,status)
 VALUES(p_attempt,p_task,v_count+1,p_session,v_epoch+1,p_token_hash,clock_timestamp()+make_interval(secs=>p_lease_seconds),'RUNNING');
 INSERT INTO resource_reservations(id,task_id,attempt_id,worker_session_id,pool_id,slots,status)
 VALUES(p_reservation,p_task,p_attempt,p_session,p_pool,1,'ACTIVE');
 UPDATE tasks SET status='RUNNING',current_attempt_id=p_attempt,lease_epoch=v_epoch+1,attempt_count=v_count+1,
 lock_version=lock_version+1,updated_at=clock_timestamp() WHERE id=p_task;
 RETURN true;
END $$;

CREATE FUNCTION mf_renew_lease(p_task uuid,p_attempt uuid,p_epoch bigint,p_token_hash text,p_extend_seconds integer)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE v_run uuid;
BEGIN
 IF p_extend_seconds<=0 THEN RETURN false; END IF;
 SELECT workflow_run_id INTO v_run FROM tasks WHERE id=p_task;
 PERFORM 1 FROM workflow_runs WHERE id=v_run AND status='RUNNING' AND cancel_requested_at IS NULL FOR UPDATE;
 IF NOT FOUND THEN RETURN false; END IF;
 PERFORM 1 FROM tasks WHERE id=p_task AND status='RUNNING' AND current_attempt_id=p_attempt AND lease_epoch=p_epoch FOR UPDATE;
 IF NOT FOUND THEN RETURN false; END IF;
 UPDATE task_attempts a SET lease_expires_at=greatest(a.lease_expires_at,clock_timestamp())+make_interval(secs=>p_extend_seconds)
 FROM worker_sessions s WHERE a.id=p_attempt AND a.task_id=p_task AND a.lease_epoch=p_epoch
 AND a.lease_token_hash=p_token_hash AND a.status='RUNNING' AND a.lease_expires_at>clock_timestamp()
 AND a.worker_session_id=s.id AND s.status='ACTIVE';
 RETURN FOUND;
END $$;

-- Result refs must refer to PRE-VERIFIED stable content. No external IO in this function.
-- Returns COMMITTED, REPLAY, or REJECTED. Same submission key + different hash raises conflict.
CREATE FUNCTION mf_commit_result(p_task uuid,p_attempt uuid,p_epoch bigint,p_token_hash text,
 p_submission uuid,p_submission_key text,p_manifest_hash text,p_result jsonb,p_event uuid)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE v_run uuid; v_status text; v_old_hash text; v_old_ref jsonb; v_res record;
BEGIN
 SELECT workflow_run_id INTO v_run FROM tasks WHERE id=p_task;
 IF v_run IS NULL THEN RETURN 'REJECTED'; END IF;
 PERFORM 1 FROM workflow_runs WHERE id=v_run FOR UPDATE;
 PERFORM 1 FROM tasks WHERE id=p_task FOR UPDATE;
 SELECT status,manifest_hash,committed_result_ref INTO v_status,v_old_hash,v_old_ref
 FROM attempt_result_submissions WHERE attempt_id=p_attempt AND submission_key=p_submission_key;
 IF FOUND THEN
   IF v_old_hash<>p_manifest_hash THEN RAISE EXCEPTION 'submission key conflict'; END IF;
   IF v_status='COMMITTED' THEN RETURN 'REPLAY'; END IF;
   RETURN 'REJECTED';
 END IF;
 PERFORM 1 FROM tasks WHERE id=p_task AND status='RUNNING' AND current_attempt_id=p_attempt AND lease_epoch=p_epoch;
 IF NOT FOUND THEN RETURN 'REJECTED'; END IF;
 PERFORM 1 FROM task_attempts a JOIN worker_sessions s ON s.id=a.worker_session_id
 WHERE a.id=p_attempt AND a.task_id=p_task AND a.lease_epoch=p_epoch AND a.status='RUNNING'
 AND a.lease_token_hash=p_token_hash AND a.lease_expires_at>clock_timestamp() AND s.status='ACTIVE';
 IF NOT FOUND THEN RETURN 'REJECTED'; END IF;
 INSERT INTO attempt_result_submissions(id,task_id,attempt_id,submission_key,manifest_hash,status,committed_result_ref)
 VALUES(p_submission,p_task,p_attempt,p_submission_key,p_manifest_hash,'COMMITTED',p_result);
 UPDATE task_attempts SET status='SUCCEEDED',result_manifest=p_result,finished_at=clock_timestamp() WHERE id=p_attempt;
 UPDATE tasks SET status='SUCCEEDED',result_ref=p_result,lock_version=lock_version+1,updated_at=clock_timestamp() WHERE id=p_task;
 -- Lock reservation rows in stable pool ordering. Claim locks pool counter but not reservation (new row).
 FOR v_res IN SELECT id,pool_id,slots FROM resource_reservations
 WHERE attempt_id=p_attempt AND status='ACTIVE' ORDER BY pool_id FOR UPDATE LOOP
   UPDATE resource_reservations SET status='RELEASED',released_at=clock_timestamp() WHERE id=v_res.id AND status='ACTIVE';
   IF FOUND THEN
     UPDATE resource_pool_counters SET reserved_slots=reserved_slots-v_res.slots,lock_version=lock_version+1 WHERE pool_id=v_res.pool_id;
   END IF;
 END LOOP;
 INSERT INTO outbox_events(id,aggregate_type,aggregate_id,event_type,payload)
 VALUES(p_event,'Task',p_task,'TaskSucceeded',jsonb_build_object('task_id',p_task,'attempt_id',p_attempt));
 RETURN 'COMMITTED';
END $$;

CREATE FUNCTION mf_recover_expired(p_task uuid,p_event uuid)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE v_run uuid; v_attempt uuid; v_res record;
BEGIN
 SELECT workflow_run_id INTO v_run FROM tasks WHERE id=p_task;
 PERFORM 1 FROM workflow_runs WHERE id=v_run FOR UPDATE;
 PERFORM 1 FROM tasks WHERE id=p_task AND status='RUNNING' FOR UPDATE;
 IF NOT FOUND THEN RETURN false; END IF;
 SELECT current_attempt_id INTO v_attempt FROM tasks WHERE id=p_task;
 UPDATE task_attempts SET status='TIMED_OUT',finished_at=clock_timestamp()
 WHERE id=v_attempt AND status='RUNNING' AND lease_expires_at<=clock_timestamp();
 IF NOT FOUND THEN RETURN false; END IF;
 UPDATE tasks SET status='READY',current_attempt_id=NULL,lock_version=lock_version+1,updated_at=clock_timestamp() WHERE id=p_task;
 FOR v_res IN SELECT id,pool_id,slots FROM resource_reservations
 WHERE attempt_id=v_attempt AND status='ACTIVE' ORDER BY pool_id FOR UPDATE LOOP
   UPDATE resource_reservations SET status='RELEASED',released_at=clock_timestamp() WHERE id=v_res.id AND status='ACTIVE';
   IF FOUND THEN
     UPDATE resource_pool_counters SET reserved_slots=reserved_slots-v_res.slots,lock_version=lock_version+1 WHERE pool_id=v_res.pool_id;
   END IF;
 END LOOP;
 INSERT INTO outbox_events(id,aggregate_type,aggregate_id,event_type,payload)
 VALUES(p_event,'Task',p_task,'TaskLeaseExpired',jsonb_build_object('attempt_id',v_attempt));
 RETURN true;
END $$;


COMMENT ON FUNCTION public.mf_claim_task(uuid,uuid,uuid,uuid,uuid,text,integer) IS '原子领取任务：检查工作流状态与执行能力，创建Attempt和租约，并预留资源槽位；返回是否成功。';
COMMENT ON FUNCTION public.mf_renew_lease(uuid,uuid,bigint,text,integer) IS '续租当前有效TaskAttempt；校验Task、LeaseEpoch、令牌哈希、会话和原租约时效。';
COMMENT ON FUNCTION public.mf_commit_result(uuid,uuid,bigint,text,uuid,text,text,jsonb,uuid) IS '幂等提交任务结果：校验有效执行权，确认结果，释放资源并写入Outbox事件。';
COMMENT ON FUNCTION public.mf_recover_expired(uuid,uuid) IS '回收租约已过期的任务尝试，释放预留资源，使任务重新进入READY并写入Outbox。';

COMMIT;
