package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
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
	store := NewSQLiteWorkflowInstanceStore(db)
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
	store := NewSQLiteWorkflowInstanceStore(db)
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

func TestSQLiteWorkflowInstanceStore_RunInPipelineTransactionRetriesBusyAndFlushesPostCommitOnce(t *testing.T) {
	db, lockDB := newSQLiteWorkflowInstanceStoreBusyTestDBs(t)
	store := NewSQLiteWorkflowInstanceStore(db)
	ctx, cancel := context.WithTimeout(runtimecorrelation.WithRunID(context.Background(), uuid.NewString()), 2*time.Second)
	defer cancel()

	lockTx, err := lockDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin locking tx: %v", err)
	}
	lockCommitted := false
	t.Cleanup(func() {
		if !lockCommitted {
			_ = lockTx.Rollback()
		}
	})
	if _, err := lockTx.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES (?, 'running', ?)
	`, uuid.NewString(), time.Now().UTC()); err != nil {
		t.Fatalf("hold sqlite write lock: %v", err)
	}

	busySeen := make(chan struct{})
	var closeBusy sync.Once
	var attempts int32
	var postCommitActions int32
	done := make(chan error, 1)
	go func() {
		done <- store.RunInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			if tx == nil {
				return errors.New("pipeline transaction is required")
			}
			atomic.AddInt32(&attempts, 1)
			if !QueuePipelinePostCommitAction(txctx, func() {
				atomic.AddInt32(&postCommitActions, 1)
			}) {
				return errors.New("queue pipeline post-commit action")
			}
			_, err := tx.ExecContext(txctx, `
				INSERT INTO runs (run_id, status, started_at)
				VALUES (?, 'running', ?)
			`, uuid.NewString(), time.Now().UTC())
			if sqlitePipelineBusyError(err) {
				closeBusy.Do(func() { close(busySeen) })
			}
			return err
		})
	}()

	select {
	case <-busySeen:
	case <-ctx.Done():
		t.Fatalf("wait for deterministic sqlite busy attempt: %v", ctx.Err())
	}
	if err := lockTx.Commit(); err != nil {
		t.Fatalf("release sqlite write lock: %v", err)
	}
	lockCommitted = true

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunInPipelineTransaction after lock release: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for retried pipeline transaction: %v", ctx.Err())
	}
	if got := atomic.LoadInt32(&attempts); got < 2 {
		t.Fatalf("attempts = %d, want retry after busy", got)
	}
	if got := atomic.LoadInt32(&postCommitActions); got != 1 {
		t.Fatalf("post-commit actions = %d, want only successful attempt flushed", got)
	}
}

func TestSQLiteWorkflowInstanceStore_RunInPipelineTransactionStopsRetryOnContextDeadline(t *testing.T) {
	db, lockDB := newSQLiteWorkflowInstanceStoreBusyTestDBs(t)
	store := NewSQLiteWorkflowInstanceStore(db)
	baseCtx := runtimecorrelation.WithRunID(context.Background(), uuid.NewString())

	lockTx, err := lockDB.BeginTx(baseCtx, nil)
	if err != nil {
		t.Fatalf("begin locking tx: %v", err)
	}
	t.Cleanup(func() { _ = lockTx.Rollback() })
	if _, err := lockTx.ExecContext(baseCtx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES (?, 'running', ?)
	`, uuid.NewString(), time.Now().UTC()); err != nil {
		t.Fatalf("hold sqlite write lock: %v", err)
	}

	ctx, cancel := context.WithTimeout(baseCtx, 35*time.Millisecond)
	defer cancel()
	var attempts int32
	err = store.RunInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		atomic.AddInt32(&attempts, 1)
		_, err := tx.ExecContext(txctx, `
			INSERT INTO runs (run_id, status, started_at)
			VALUES (?, 'running', ?)
		`, uuid.NewString(), time.Now().UTC())
		return err
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunInPipelineTransaction error = %v, want context deadline", err)
	}
	if got := atomic.LoadInt32(&attempts); got == 0 {
		t.Fatal("expected at least one busy attempt before context deadline")
	}
}

func TestSQLiteWorkflowInstanceStore_RunInPipelineTransactionDoesNotRetryActiveTransaction(t *testing.T) {
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
	err = store.RunInPipelineTransaction(WithPipelineSQLTxContext(ctx, tx), func(txctx context.Context, gotTx *sql.Tx) error {
		atomic.AddInt32(&attempts, 1)
		if gotTx != tx {
			t.Fatalf("transaction = %#v, want active transaction", gotTx)
		}
		return busyErr
	})
	if !errors.Is(err, busyErr) {
		t.Fatalf("RunInPipelineTransaction error = %v, want sentinel busy error", err)
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Fatalf("attempts = %d, want no retry inside active transaction", got)
	}
}

func TestWorkflowInstanceStore_RunInPipelineTransactionDoesNotRetryPostgresDialect(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := NewWorkflowInstanceStore(db)
	ctx := runtimecorrelation.WithRunID(context.Background(), uuid.NewString())
	busyErr := errors.New("SQLITE_BUSY: database is locked")
	var attempts int32

	err := store.RunInPipelineTransaction(ctx, func(context.Context, *sql.Tx) error {
		atomic.AddInt32(&attempts, 1)
		return busyErr
	})
	if !errors.Is(err, busyErr) {
		t.Fatalf("RunInPipelineTransaction error = %v, want sentinel busy error", err)
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Fatalf("attempts = %d, want no retry for postgres dialect", got)
	}
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

func newSQLiteWorkflowInstanceStoreBusyTestDBs(t *testing.T) (*sql.DB, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "workflow.db")
	workflowDB := openSQLiteWorkflowInstanceStoreBusyTestDB(t, path)
	lockDB := openSQLiteWorkflowInstanceStoreBusyTestDB(t, path)
	createSQLiteWorkflowInstanceStoreTestSchema(t, workflowDB)
	return workflowDB, lockDB
}

func openSQLiteWorkflowInstanceStoreBusyTestDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite file db: %v", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	t.Cleanup(func() { _ = db.Close() })
	for _, stmt := range []string{
		`PRAGMA foreign_keys = ON`,
		`PRAGMA journal_mode = WAL`,
		`PRAGMA busy_timeout = 1`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("configure sqlite file db: %v", err)
		}
	}
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
