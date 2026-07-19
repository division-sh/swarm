package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func TestSQLiteWorkflowInstanceStore_PreservesCreateEntityInitialValueMutationRows(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	ensurePipelineTestRun(t, store, runID)
	ctx = withWorkflowCreateEntityInitialValues(ctx, map[string]any{
		"region": "west",
		"tier":   float64(1),
	})
	storageRef := "root/acme"
	entityID := FlowInstanceEntityID(storageRef)

	if err := store.Create(ctx, WorkflowInstance{
		InstanceID:      "acme",
		StorageRef:      storageRef,
		WorkflowName:    "root",
		WorkflowVersion: "v1",
		CurrentState:    "created",
		EnteredStageAt:  time.Now().UTC(),
		Metadata: map[string]any{
			"flow_path": storageRef,
			"region":    "west",
			"tier":      float64(2),
		},
	}); err != nil {
		t.Fatalf("Create workflow instance: %v", err)
	}

	assertSQLiteMutationCount(t, db, entityID, "region", "entity_initial_value", "create_entity", "null", `"west"`, 1)
	assertSQLiteMutationCount(t, db, entityID, "region", "workflow_instance_store", "create", "", "", 0)
	assertSQLiteMutationCount(t, db, entityID, "tier", "entity_initial_value", "create_entity", "null", "1", 1)
	assertSQLiteMutationCount(t, db, entityID, "tier", "workflow_instance_store", "create", "1", "2", 1)
}

func TestSQLiteWorkflowInstanceStore_PreservesParentRouteControlMetadata(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	ensurePipelineTestRun(t, store, runID)
	storageRef := "review/inst-1"

	if err := store.Create(ctx, WorkflowInstance{
		InstanceID:      "inst-1",
		StorageRef:      storageRef,
		WorkflowName:    "review",
		WorkflowVersion: "v1",
		CurrentState:    "created",
		EnteredStageAt:  time.Now().UTC(),
		Metadata: map[string]any{
			"flow_path":            storageRef,
			"parent_flow_id":       "operating",
			"parent_flow_instance": "operating/root",
			"parent_entity_id":     "parent-ent",
		},
	}); err != nil {
		t.Fatalf("Create workflow instance: %v", err)
	}

	loaded, ok, err := store.Load(ctx, storageRef)
	if err != nil {
		t.Fatalf("Load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	for key, want := range map[string]string{
		"parent_flow_id":       "operating",
		"parent_flow_instance": "operating/root",
		"parent_entity_id":     "parent-ent",
	} {
		if got := strings.TrimSpace(asString(loaded.Metadata[key])); got != want {
			t.Fatalf("loaded.Metadata[%s] = %#v, want %q", key, loaded.Metadata[key], want)
		}
	}
	identity, err := workflowInstancePersistedIdentity(nil, loaded)
	if err != nil {
		t.Fatalf("workflowInstancePersistedIdentity: %v", err)
	}
	if identity.ParentRoute.FlowID != "operating" || identity.ParentRoute.FlowInstance != "operating/root" || identity.ParentRoute.EntityID != "parent-ent" {
		t.Fatalf("ParentRoute = %#v, want operating/operating/root/parent-ent", identity.ParentRoute)
	}
}

func TestSQLiteWorkflowInstanceStore_MarkTerminatedUsesRuntimeMutationRunner(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	runner := &recordingRuntimeMutationRunner{db: db}
	store := NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, runner)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	ensurePipelineTestRun(t, store, runID)
	storageRef := "root/terminated"
	terminatedAt := time.Now().UTC().Truncate(time.Millisecond)
	if _, err := db.Exec(`
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES (?, 'root', 'workflow', '{}', 'running', ?)
	`, storageRef, time.Now().UTC()); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}

	if err := store.MarkTerminated(ctx, storageRef, terminatedAt); err != nil {
		t.Fatalf("MarkTerminated: %v", err)
	}
	if got := atomic.LoadInt32(&runner.calls); got != 1 {
		t.Fatalf("runtime mutation calls = %d, want 1", got)
	}

	var status string
	var hasTerminatedAt int
	if err := db.QueryRow(`
		SELECT COALESCE(status, ''), terminated_at IS NOT NULL
		FROM flow_instances
		WHERE instance_id = ?
	`, storageRef).Scan(&status, &hasTerminatedAt); err != nil {
		t.Fatalf("load terminated flow instance: %v", err)
	}
	if status != "terminated" || hasTerminatedAt != 1 {
		t.Fatalf("flow instance status=%q hasTerminatedAt=%d, want terminated/1", status, hasTerminatedAt)
	}
}

func TestSQLiteWorkflowInstanceStore_RunPipelineMutationRequiresRuntimeMutationRunner(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := NewSQLiteWorkflowInstanceStore(db)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), uuid.NewString())
	called := false

	err := store.RunPipelineMutation(ctx, func(context.Context) error {
		called = true
		return nil
	})
	if !errors.Is(err, errSQLiteWorkflowInstanceStoreRuntimeMutationRunnerRequired) {
		t.Fatalf("RunPipelineMutation error = %v, want runtime mutation runner required", err)
	}
	if called {
		t.Fatal("RunPipelineMutation callback ran without runtime mutation runner")
	}
}

func TestSQLiteWorkflowInstanceStore_RunPipelineMutationUsesRuntimeMutationRunner(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	runner := &recordingRuntimeMutationRunner{db: db}
	store := NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, runner)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	ensurePipelineTestRun(t, store, runID)
	var postCommitActions int32

	err := store.RunPipelineMutation(ctx, func(txctx context.Context) error {
		tx, ok := PipelineSQLTxFromContext(txctx)
		if !ok || tx == nil {
			return errors.New("pipeline transaction is required")
		}
		if !QueuePipelinePostCommitAction(txctx, func() {
			atomic.AddInt32(&postCommitActions, 1)
		}) {
			return errors.New("queue pipeline post-commit action")
		}
		_, err := tx.ExecContext(txctx, `
			INSERT INTO runs (run_id, status, started_at)
			VALUES (?, 'running', ?)
		`, uuid.NewString(), time.Now().UTC())
		return err
	})
	if err != nil {
		t.Fatalf("RunPipelineMutation with runtime mutation runner: %v", err)
	}
	if got := atomic.LoadInt32(&runner.calls); got != 1 {
		t.Fatalf("runtime mutation calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&postCommitActions); got != 1 {
		t.Fatalf("post-commit actions = %d, want 1", got)
	}
}

func TestSQLiteWorkflowInstanceStore_MutateERollsBackCallbackFailure(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	runner := &recordingRuntimeMutationRunner{db: db}
	store := NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, runner)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	ensurePipelineTestRun(t, store, runID)
	instance := WorkflowInstance{InstanceID: "root/item", StorageRef: "root/item", WorkflowName: "root", WorkflowVersion: "1.0.0", CurrentState: "queued", Metadata: map[string]any{}}
	if err := store.Upsert(ctx, instance); err != nil {
		t.Fatalf("seed: %v", err)
	}
	sentinel := errors.New("supersession failed")
	if err := store.MutateE(ctx, instance.InstanceID, func(item *WorkflowInstance) error {
		item.CurrentState = "must_not_commit"
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("MutateE error = %v, want sentinel", err)
	}
	loaded, ok, err := store.Load(ctx, instance.InstanceID)
	if err != nil || !ok {
		t.Fatalf("Load = found %v err %v", ok, err)
	}
	if loaded.CurrentState != "queued" {
		t.Fatalf("CurrentState = %q, want queued", loaded.CurrentState)
	}
}

func TestSQLiteWorkflowInstanceStore_RunPipelineMutationDoesNotRetryActiveTransaction(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := NewSQLiteWorkflowInstanceStore(db)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), uuid.NewString())
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin active tx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })

	busyErr := errors.New("SQLITE_BUSY: database is locked")
	var attempts int32
	txctx, err := runtimeauthoractivity.Begin(WithPipelineSQLTxContext(ctx, tx), tx, runtimeauthoractivity.DialectSQLite)
	if err != nil {
		t.Fatalf("begin author activity story: %v", err)
	}
	err = store.RunPipelineMutation(txctx, func(txctx context.Context) error {
		atomic.AddInt32(&attempts, 1)
		gotTx, ok := PipelineSQLTxFromContext(txctx)
		if !ok {
			t.Fatal("active transaction missing from pipeline mutation context")
		}
		if gotTx != tx {
			t.Fatalf("transaction = %#v, want active transaction", gotTx)
		}
		return busyErr
	})
	if !errors.Is(err, busyErr) {
		t.Fatalf("RunPipelineMutation error = %v, want sentinel busy error", err)
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Fatalf("attempts = %d, want no retry inside active transaction", got)
	}
}

func TestSQLiteWorkflowInstanceStore_RunPipelineMutationRejectsUnownedRawTransaction(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := NewSQLiteWorkflowInstanceStore(db)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), uuid.NewString())
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin raw tx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })

	err = store.RunPipelineMutation(WithPipelineSQLTxContext(ctx, tx), func(context.Context) error {
		t.Fatal("raw transaction callback must not run")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "raw transaction without author activity ownership") {
		t.Fatalf("RunPipelineMutation error = %v, want unowned raw transaction rejection", err)
	}
}

func TestWorkflowInstanceStore_RunPipelineMutationDoesNotRetryPostgresDialect(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := NewWorkflowInstanceStore(db)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), uuid.NewString())
	busyErr := errors.New("SQLITE_BUSY: database is locked")
	var attempts int32

	err := store.RunPipelineMutation(ctx, func(context.Context) error {
		atomic.AddInt32(&attempts, 1)
		return busyErr
	})
	if !errors.Is(err, busyErr) {
		t.Fatalf("RunPipelineMutation error = %v, want sentinel busy error", err)
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Fatalf("attempts = %d, want no retry for postgres dialect", got)
	}
}

type recordingRuntimeMutationRunner struct {
	db    *sql.DB
	mu    sync.Mutex
	calls int32
}

func (r *recordingRuntimeMutationRunner) RunRuntimeMutationContext(ctx context.Context, fn func(context.Context) error) error {
	atomic.AddInt32(&r.calls, 1)
	r.mu.Lock()
	defer r.mu.Unlock()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	postCommit := make([]func(), 0, 4)
	txctx := withPipelinePostCommitActions(WithPipelineSQLTxContext(ctx, tx), &postCommit)
	storyctx, err := runtimeauthoractivity.Begin(txctx, tx, runtimeauthoractivity.DialectSQLite)
	if err != nil {
		return err
	}
	if err := fn(storyctx); err != nil {
		return err
	}
	if err := runtimeauthoractivity.Finalize(storyctx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	flushPipelinePostCommitActions(postCommit)
	return nil
}

func newSQLiteWorkflowInstanceStoreForTest(t *testing.T, db *sql.DB) *WorkflowInstanceStore {
	t.Helper()
	return NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, &recordingRuntimeMutationRunner{db: db})
}

func newSQLiteWorkflowInstanceStoreTestDB(t *testing.T) *sql.DB {
	t.Helper()
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	db, err := sql.Open("sqlite", "file:"+name+"?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	createSQLiteWorkflowInstanceStoreTestSchema(t, db)
	return db
}

func createSQLiteWorkflowInstanceStoreTestSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, stmt := range []string{
		`CREATE TABLE runs (
				run_id TEXT PRIMARY KEY,
				status TEXT,
				bundle_hash TEXT,
				bundle_source TEXT,
				started_at TIMESTAMP
		)`,
		`CREATE TABLE flow_instances (
			instance_id TEXT PRIMARY KEY,
			flow_template TEXT,
			mode TEXT,
			config TEXT,
			status TEXT,
			terminated_at TIMESTAMP,
			created_at TIMESTAMP
		)`,
		`CREATE TABLE entity_state (
			run_id TEXT,
			entity_id TEXT,
			flow_instance TEXT,
			entity_type TEXT,
			slug TEXT,
			name TEXT,
			current_state TEXT,
			gates TEXT,
			fields TEXT,
			accumulator TEXT,
			revision INTEGER,
			entered_state_at TIMESTAMP,
			created_at TIMESTAMP,
			updated_at TIMESTAMP,
			PRIMARY KEY (run_id, entity_id)
		)`,
		`CREATE TABLE timers (
			timer_id TEXT PRIMARY KEY,
			run_id TEXT,
			timer_name TEXT,
			source_timer_id TEXT,
			forked_from_run_id TEXT,
			forked_from_event_id TEXT,
			reconstruction_owner TEXT,
			entity_id TEXT,
			flow_instance TEXT,
			fire_event TEXT,
			fire_payload TEXT,
			fire_at TIMESTAMP,
			recurring BOOLEAN,
			recurrence_cron TEXT,
			recurrence_interval TEXT,
			owner_node TEXT,
			owner_agent TEXT,
			reply_context_id TEXT,
			task_type TEXT,
			status TEXT,
			fired_at TIMESTAMP,
			created_at TIMESTAMP
		)`,
		`CREATE TABLE entity_mutations (
			mutation_id TEXT PRIMARY KEY,
			run_id TEXT,
			entity_id TEXT,
			field TEXT,
			old_value TEXT,
			new_value TEXT,
			caused_by_event TEXT,
			writer_type TEXT,
			writer_id TEXT,
			handler_step TEXT,
			created_at TIMESTAMP
		)`,
		`CREATE TABLE events (
			event_class TEXT NOT NULL CHECK (event_class IN ('root_ingress', 'operator_injected', 'child', 'replay', 'selected_fork_replay', 'runtime_control', 'runtime_diagnostic', 'diagnostic_direct')),
			event_id TEXT PRIMARY KEY,
			run_id TEXT REFERENCES runs(run_id),
			event_name TEXT NOT NULL CHECK (NULLIF(TRIM(event_name), '') IS NOT NULL),
			task_id TEXT,
			entity_id TEXT,
			flow_instance TEXT,
			scope TEXT NOT NULL CHECK (scope IN ('entity', 'flow', 'global')),
			payload TEXT NOT NULL CHECK (json_valid(payload)),
			execution_mode TEXT NOT NULL CHECK (execution_mode IN ('live', 'mock')),
			chain_depth INTEGER NOT NULL CHECK (chain_depth >= 0),
			produced_by TEXT NOT NULL CHECK (NULLIF(TRIM(produced_by), '') IS NOT NULL),
			produced_by_type TEXT NOT NULL CHECK (produced_by_type IN ('node', 'agent', 'platform', 'external')),
			source_event_id TEXT,
			created_at TEXT NOT NULL,
			routing_source_kind TEXT NOT NULL CHECK (routing_source_kind IN ('', 'declared_ingress', 'runtime_instance')),
			routing_source_authority TEXT,
			source_route TEXT NOT NULL CHECK (json_valid(source_route)),
			target_route TEXT NOT NULL CHECK (json_valid(target_route)),
			target_set TEXT NOT NULL CHECK (json_valid(target_set)),
			operator_reference_event_id TEXT,
			handler_node TEXT,
			idempotency_key TEXT,
			CHECK ((event_class IN ('child', 'replay') AND source_event_id IS NOT NULL AND run_id IS NOT NULL) OR (event_class NOT IN ('child', 'replay') AND source_event_id IS NULL) OR (event_class IN ('runtime_control', 'runtime_diagnostic', 'diagnostic_direct') AND source_event_id IS NOT NULL AND run_id IS NOT NULL)),
			CHECK ((event_class = 'operator_injected') OR operator_reference_event_id IS NULL),
			CHECK ((routing_source_kind = '' AND source_route = '{}' AND NULLIF(TRIM(COALESCE(routing_source_authority, '')), '') IS NULL) OR (routing_source_kind = 'declared_ingress' AND source_route <> '{}' AND NULLIF(TRIM(COALESCE(routing_source_authority, '')), '') IS NOT NULL) OR (routing_source_kind = 'runtime_instance' AND source_route <> '{}' AND NULLIF(TRIM(COALESCE(routing_source_authority, '')), '') IS NULL))
		)`,
		`CREATE TABLE event_receipts (
			receipt_id TEXT PRIMARY KEY,
			event_id TEXT,
			subscriber_type TEXT,
			subscriber_id TEXT,
			entity_id TEXT,
			flow_instance TEXT,
			outcome TEXT,
			reason_code TEXT,
			side_effects TEXT,
			failure TEXT,
			idempotency_key TEXT,
			processed_at TIMESTAMP,
			UNIQUE(event_id, subscriber_type, subscriber_id)
		)`,
		`CREATE TABLE event_deliveries (
			delivery_id TEXT PRIMARY KEY,
			run_id TEXT,
			event_id TEXT,
			subscriber_type TEXT,
			subscriber_id TEXT,
			delivery_target_route TEXT,
			status TEXT,
			retry_count INTEGER,
			reason_code TEXT,
			failure TEXT,
			active_session_id TEXT,
			started_at TIMESTAMP,
			delivered_at TIMESTAMP,
			created_at TIMESTAMP
		)`,
		`CREATE TABLE run_fork_selected_contract_executions (
			execution_id TEXT PRIMARY KEY,
			fork_run_id TEXT NOT NULL REFERENCES runs(run_id),
			source_run_id TEXT NOT NULL REFERENCES runs(run_id),
			source_event_id TEXT NOT NULL REFERENCES events(event_id),
			fork_event_id TEXT NOT NULL REFERENCES events(event_id),
			event_name TEXT NOT NULL CHECK (NULLIF(TRIM(event_name), '') IS NOT NULL),
			selection_authority TEXT NOT NULL CHECK (NULLIF(TRIM(selection_authority), '') IS NOT NULL),
			created_at TEXT NOT NULL,
			UNIQUE (fork_run_id, source_event_id),
			UNIQUE (fork_event_id)
		)`,
		`CREATE TABLE run_fork_delivery_event_replays (
			replay_id TEXT PRIMARY KEY,
			fork_run_id TEXT NOT NULL REFERENCES runs(run_id),
			source_run_id TEXT NOT NULL REFERENCES runs(run_id),
			source_event_id TEXT NOT NULL REFERENCES events(event_id),
			source_delivery_id TEXT NOT NULL REFERENCES event_deliveries(delivery_id),
			fork_event_id TEXT NOT NULL REFERENCES events(event_id),
			fork_delivery_id TEXT NOT NULL REFERENCES event_deliveries(delivery_id),
			subscriber_type TEXT NOT NULL CHECK (subscriber_type IN ('node', 'agent')),
			subscriber_id TEXT NOT NULL CHECK (NULLIF(TRIM(subscriber_id), '') IS NOT NULL),
			selection_authority TEXT NOT NULL CHECK (NULLIF(TRIM(selection_authority), '') IS NOT NULL),
			created_at TEXT NOT NULL,
			UNIQUE (fork_run_id, source_delivery_id),
			UNIQUE (fork_delivery_id)
		)`,
		`CREATE TABLE activity_attempts (
			request_event_id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL,
			execution_mode TEXT NOT NULL CHECK (execution_mode IN ('live', 'mock')),
			source_event_id TEXT,
			parent_event_id TEXT,
			entity_id TEXT,
			flow_instance TEXT,
			node_id TEXT NOT NULL,
			handler_event_key TEXT NOT NULL,
			activity_id TEXT NOT NULL,
			tool TEXT NOT NULL,
			effect_class TEXT NOT NULL,
			attempt INTEGER NOT NULL DEFAULT 1,
			status TEXT NOT NULL,
			success_event TEXT NOT NULL,
			failure_event TEXT NOT NULL,
			result_event_id TEXT,
			result_event_type TEXT,
			result_payload TEXT,
			failure TEXT,
			input_hash TEXT NOT NULL,
			loop_generation TEXT NOT NULL DEFAULT '{}',
			loop_stage TEXT,
			reply_context_id TEXT,
			started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			completed_at TEXT,
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE author_activity_order (
			singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
			last_sequence BIGINT NOT NULL CHECK (last_sequence >= 0)
		)`,
		`CREATE TABLE author_activity_occurrences (
			occurrence_id TEXT PRIMARY KEY,
			sequence BIGINT NOT NULL UNIQUE CHECK (sequence > 0),
			kind TEXT NOT NULL,
			version INTEGER NOT NULL CHECK (version = 2),
			transition TEXT NOT NULL,
			source_owner TEXT NOT NULL,
			source_identity TEXT NOT NULL,
			dedup_key TEXT NOT NULL UNIQUE,
			run_id TEXT,
			entity_id TEXT,
			agent_id TEXT,
			flow_id TEXT,
			scope_kind TEXT NOT NULL,
			runtime_instance_id TEXT,
			bundle_hash TEXT,
			author_safe_summary TEXT,
			projection TEXT NOT NULL DEFAULT '{}',
			failure TEXT,
			occurred_at TIMESTAMP NOT NULL
		)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create sqlite test schema: %v", err)
		}
	}
}

func assertSQLiteMutationCount(t *testing.T, db *sql.DB, entityID, field, writerID, handlerStep, oldValue, newValue string, want int) {
	t.Helper()
	query := `
		SELECT COUNT(*)
		FROM entity_mutations
		WHERE entity_id = ?
		  AND field = ?
		  AND writer_id = ?
		  AND handler_step = ?
	`
	args := []any{entityID, field, writerID, handlerStep}
	if oldValue != "" {
		query += ` AND old_value = ?`
		args = append(args, oldValue)
	}
	if newValue != "" {
		query += ` AND new_value = ?`
		args = append(args, newValue)
	}
	var got int
	if err := db.QueryRow(query, args...).Scan(&got); err != nil {
		t.Fatalf("count sqlite mutation rows: %v", err)
	}
	if got != want {
		t.Fatalf("mutation count for field=%s writer=%s step=%s old=%s new=%s = %d, want %d", field, writerID, handlerStep, oldValue, newValue, got, want)
	}
}
