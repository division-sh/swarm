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

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func TestSQLiteWorkflowInstanceStore_PreservesCreateEntityInitialValueMutationRows(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
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
	ctx := runtimecorrelation.WithRunID(context.Background(), uuid.NewString())
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
	storageRef := "root/terminated"
	terminatedAt := time.Now().UTC().Truncate(time.Millisecond)
	if _, err := db.Exec(`
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES (?, 'root', 'workflow', '{}', 'running', ?)
	`, storageRef, time.Now().UTC()); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}

	if err := store.MarkTerminated(context.Background(), storageRef, terminatedAt); err != nil {
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
	ctx := runtimecorrelation.WithRunID(context.Background(), uuid.NewString())
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
	ctx := runtimecorrelation.WithRunID(context.Background(), uuid.NewString())
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

func TestSQLiteWorkflowInstanceStore_RunPipelineMutationDoesNotRetryActiveTransaction(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := NewSQLiteWorkflowInstanceStore(db)
	ctx := runtimecorrelation.WithRunID(context.Background(), uuid.NewString())
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin active tx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })

	busyErr := errors.New("SQLITE_BUSY: database is locked")
	var attempts int32
	err = store.RunPipelineMutation(WithPipelineSQLTxContext(ctx, tx), func(txctx context.Context) error {
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

func TestWorkflowInstanceStore_RunPipelineMutationDoesNotRetryPostgresDialect(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := NewWorkflowInstanceStore(db)
	ctx := runtimecorrelation.WithRunID(context.Background(), uuid.NewString())
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
	if err := fn(txctx); err != nil {
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
			entity_id TEXT,
			flow_instance TEXT,
			fire_event TEXT,
			fire_payload TEXT,
			fire_at TIMESTAMP,
			recurring BOOLEAN,
			owner_node TEXT,
			owner_agent TEXT,
			task_type TEXT,
			status TEXT,
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
			event_id TEXT PRIMARY KEY,
			run_id TEXT,
			event_name TEXT,
			entity_id TEXT,
			flow_instance TEXT,
			scope TEXT,
			payload TEXT,
			chain_depth INTEGER,
			produced_by_type TEXT,
			created_at TIMESTAMP
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
			status TEXT,
			retry_count INTEGER,
			reason_code TEXT,
			last_error TEXT,
			active_session_id TEXT,
			started_at TIMESTAMP,
			delivered_at TIMESTAMP,
			created_at TIMESTAMP
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
