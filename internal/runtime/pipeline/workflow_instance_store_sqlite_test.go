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

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func TestSQLiteWorkflowInstanceStore_PreservesCreateEntityInitialValueMutationRows(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID)
	ensurePipelineTestRun(t, store, runID)
	storageRef := "root/acme"
	entityID := FlowInstanceEntityID(storageRef)

	if err := store.create(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      "acme",
		StorageRef:      storageRef,
		EntityID:        entityID,
		WorkflowName:    "root",
		WorkflowVersion: "v1",
		CurrentState:    "created",
		EnteredStageAt:  time.Now().UTC(),
		Fields: map[string]any{
			"region": "west",
			"tier":   float64(2),
		},
		InitialFieldValues: map[string]any{
			"region": "west",
			"tier":   float64(1),
		},
		EntityType: "test_entity",
	})); err != nil {
		t.Fatalf("Create workflow instance: %v", err)
	}

	assertSQLiteMutationCount(t, db, entityID, "region", "entity_initial_value", "create_entity", "null", `"west"`, 1)
	assertSQLiteMutationCount(t, db, entityID, "region", "workflow_instance_store", "create", "", "", 0)
	assertSQLiteMutationCount(t, db, entityID, "tier", "entity_initial_value", "create_entity", "null", "1", 1)
	assertSQLiteMutationCount(t, db, entityID, "tier", "workflow_engine", "create", "1", "2", 1)
}

func TestSQLiteEntityStateDiffRequiresExistingCanonicalRunBeforeMutation(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID)
	ctx, err = authoractivityfixture.Begin(ctx, tx, authoractivityfixture.DialectSQLite)
	if err != nil {
		t.Fatalf("begin author activity: %v", err)
	}
	err = insertSQLiteEntityStateDiff(
		ctx,
		tx,
		testRunLifecycleMutation{tx: tx, dialect: workflowStoreDialectSQLite},
		uuid.NewString(),
		runtimemutationlog.EntityStateProjection{},
		runtimemutationlog.EntityStateProjection{Fields: map[string]any{"status": "ready"}},
		runtimemutationlog.Writer{Type: "platform", ID: "hostile-proof", HandlerStep: "diff"},
	)
	if !errors.Is(err, storerunlifecycle.ErrRunNotFound) {
		t.Fatalf("insertSQLiteEntityStateDiff error = %v, want ErrRunNotFound", err)
	}
	assertSQLiteTxTableCount(t, tx, "runs", 0)
	assertSQLiteTxTableCount(t, tx, "entity_mutations", 0)
}

func TestSQLiteInitialValueMutationRequiresExistingCanonicalRunBeforeMutation(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID)
	ctx, err = authoractivityfixture.Begin(ctx, tx, authoractivityfixture.DialectSQLite)
	if err != nil {
		t.Fatalf("begin author activity: %v", err)
	}
	_, err = insertSQLiteWorkflowCreateEntityInitialValueMutations(
		ctx,
		tx,
		testRunLifecycleMutation{tx: tx, dialect: workflowStoreDialectSQLite},
		uuid.NewString(),
		runtimemutationlog.EntityStateProjection{},
		runtimemutationlog.EntityStateProjection{Fields: map[string]any{"region": "west"}},
		map[string]any{"region": "west"},
	)
	if !errors.Is(err, storerunlifecycle.ErrRunNotFound) {
		t.Fatalf("insertSQLiteWorkflowCreateEntityInitialValueMutations error = %v, want ErrRunNotFound", err)
	}
	assertSQLiteTxTableCount(t, tx, "runs", 0)
	assertSQLiteTxTableCount(t, tx, "entity_mutations", 0)
}

func TestSQLiteWorkflowInstanceStore_PreservesParentRouteControlMetadata(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID)
	ensurePipelineTestRun(t, store, runID)
	storageRef := "review/inst-1"

	if err := store.create(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:         "inst-1",
		StorageRef:         storageRef,
		ParentFlowID:       "operating",
		ParentFlowInstance: "operating/root",
		ParentEntityID:     "parent-ent",
		WorkflowName:       "review",
		WorkflowVersion:    "v1",
		CurrentState:       "created",
		EnteredStageAt:     time.Now().UTC(),
		Fields:             map[string]any{},
		EntityType:         "test_entity",
	})); err != nil {
		t.Fatalf("Create workflow instance: %v", err)
	}

	loaded, ok, err := store.Load(ctx, testWorkflowInstanceRoute(storageRef))
	if err != nil {
		t.Fatalf("Load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	if loaded.ParentFlowID != "operating" || loaded.ParentFlowInstance != "operating/root" || loaded.ParentEntityID != "parent-ent" {
		t.Fatalf("loaded parent identity = %q/%q/%q", loaded.ParentFlowID, loaded.ParentFlowInstance, loaded.ParentEntityID)
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
	runner := &recordingRuntimeMutationRunner{db: db, dialect: workflowStoreDialectSQLite}
	store := newTestSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, runner)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID)
	ensurePipelineTestRun(t, store, runID)
	storageRef := "root/terminated"
	entityID := uuid.NewString()
	terminatedAt := time.Now().UTC().Truncate(time.Millisecond)
	if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: "terminated", StorageRef: storageRef, EntityID: entityID, WorkflowName: "root", WorkflowVersion: "1",
		CurrentState: "running", EnteredStageAt: time.Now().UTC(), Fields: map[string]any{},
		EntityType: "test_entity",
	})); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}
	atomic.StoreInt32(&runner.calls, 0)

	if err := store.MarkTerminated(ctx, testWorkflowInstanceRoute(storageRef), identity.NormalizeEntityID(entityID), terminatedAt); err != nil {
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

func TestSQLiteWorkflowInstanceStore_runPipelineMutationUsesRuntimeMutationRunner(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	runner := &recordingRuntimeMutationRunner{db: db}
	store := newTestSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, runner)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID)
	ensurePipelineTestRun(t, store, runID)
	var postCommitActions int32

	err := store.runPipelineMutation(ctx, func(txctx context.Context) error {
		tx, ok := PipelineSQLTxFromContext(txctx)
		if !ok || tx == nil {
			return errors.New("pipeline transaction is required")
		}
		if !QueuePipelinePostCommitAction(txctx, func(context.Context) {
			atomic.AddInt32(&postCommitActions, 1)
		}) {
			return errors.New("queue pipeline post-commit action")
		}
		source, err := runtimecorrelation.NewEphemeralBundleSourceFact(
			"bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		)
		if err != nil {
			return err
		}
		_, err = (testRunLifecycleMutation{tx: tx, dialect: workflowStoreDialectSQLite}).CreateRun(txctx, storerunlifecycle.CreateRequest{
			RunID: uuid.NewString(), Origin: storerunlifecycle.ScenarioSetupRunOrigin(),
			Source: source, StartedAt: time.Now().UTC(),
		})
		return err
	})
	if err != nil {
		t.Fatalf("runPipelineMutation with runtime mutation runner: %v", err)
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
	store := newTestSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, runner)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID)
	ensurePipelineTestRun(t, store, runID)
	instance := materializedWorkflowInstanceForTest(WorkflowInstance{InstanceID: "item", StorageRef: "root/item", WorkflowName: "root", WorkflowVersion: "1.0.0", CurrentState: "queued", Fields: map[string]any{},
		EntityType: "test_entity"})
	if err := store.upsert(ctx, instance); err != nil {
		t.Fatalf("seed: %v", err)
	}
	sentinel := errors.New("supersession failed")
	if err := store.mutateE(ctx, testWorkflowInstanceRoute(instance.StorageRef), func(item *WorkflowInstance) error {
		item.CurrentState = "must_not_commit"
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("MutateE error = %v, want sentinel", err)
	}
	loaded, ok, err := store.Load(ctx, testWorkflowInstanceRoute(instance.StorageRef))
	if err != nil || !ok {
		t.Fatalf("Load = found %v err %v", ok, err)
	}
	if loaded.CurrentState != "queued" {
		t.Fatalf("CurrentState = %q, want queued", loaded.CurrentState)
	}
}

func TestSQLiteWorkflowInstanceStore_runPipelineMutationDoesNotRetryActiveTransaction(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newTestSQLiteWorkflowInstanceStore(db)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), uuid.NewString())
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin active tx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })

	busyErr := errors.New("SQLITE_BUSY: database is locked")
	var attempts int32
	txctx, err := authoractivityfixture.Begin(WithPipelineSQLTxContext(ctx, tx), tx, authoractivityfixture.DialectSQLite)
	if err != nil {
		t.Fatalf("begin author activity story: %v", err)
	}
	err = store.runPipelineMutation(txctx, func(txctx context.Context) error {
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
		t.Fatalf("runPipelineMutation error = %v, want sentinel busy error", err)
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Fatalf("attempts = %d, want no retry inside active transaction", got)
	}
}

func TestSQLiteWorkflowInstanceStore_runPipelineMutationRejectsUnownedRawTransaction(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newTestSQLiteWorkflowInstanceStore(db)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), uuid.NewString())
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin raw tx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })

	err = store.runPipelineMutation(WithPipelineSQLTxContext(ctx, tx), func(context.Context) error {
		t.Fatal("raw transaction callback must not run")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "raw transaction without author activity ownership") {
		t.Fatalf("runPipelineMutation error = %v, want unowned raw transaction rejection", err)
	}
}

func TestWorkflowInstanceStore_runPipelineMutationDoesNotRetryPostgresDialect(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := newTestWorkflowInstanceStore(db)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), uuid.NewString())
	busyErr := errors.New("SQLITE_BUSY: database is locked")
	var attempts int32

	err := store.runPipelineMutation(ctx, func(context.Context) error {
		atomic.AddInt32(&attempts, 1)
		return busyErr
	})
	if !errors.Is(err, busyErr) {
		t.Fatalf("runPipelineMutation error = %v, want sentinel busy error", err)
	}
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Fatalf("attempts = %d, want no retry for postgres dialect", got)
	}
}

type recordingRuntimeMutationRunner struct {
	db                                    *sql.DB
	dialect                               workflowStoreDialect
	decisionCards                         decisioncard.Store
	mu                                    sync.Mutex
	calls                                 int32
	committedGenericScheduleActivations   []runtimegenericschedule.Activation
	committedGenericScheduleCancellations []runtimegenericschedule.Activation
}

func (r *recordingRuntimeMutationRunner) lifecycleMutation(ctx context.Context) (testRunLifecycleMutation, error) {
	tx, ok := PipelineSQLTxFromContext(ctx)
	if !ok || tx == nil {
		return testRunLifecycleMutation{}, errors.New("test run lifecycle transaction is required")
	}
	dialect := r.dialect
	if dialect == "" {
		dialect = workflowStoreDialectSQLite
	}
	return testRunLifecycleMutation{tx: tx, dialect: dialect}, nil
}

func (r *recordingRuntimeMutationRunner) RequirePresentRun(ctx context.Context, runID string) error {
	m, err := r.lifecycleMutation(ctx)
	if err != nil {
		return err
	}
	return m.RequirePresentRun(ctx, runID)
}

func (r *recordingRuntimeMutationRunner) RequireActiveRun(ctx context.Context, runID string) error {
	m, err := r.lifecycleMutation(ctx)
	if err != nil {
		return err
	}
	return m.RequireActiveRun(ctx, runID)
}

func (r *recordingRuntimeMutationRunner) RequirePresentRunSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	m, err := r.lifecycleMutation(ctx)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	return m.RequirePresentRunSource(ctx, runID)
}

func (r *recordingRuntimeMutationRunner) RequireActiveRunSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	m, err := r.lifecycleMutation(ctx)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	return m.RequireActiveRunSource(ctx, runID)
}

func (r *recordingRuntimeMutationRunner) CreateRun(ctx context.Context, request storerunlifecycle.CreateRequest) (storerunlifecycle.MutationDisposition, error) {
	m, err := r.lifecycleMutation(ctx)
	if err != nil {
		return "", err
	}
	return m.CreateRun(ctx, request)
}

func (r *recordingRuntimeMutationRunner) RequestCompletionCandidate(ctx context.Context, request storerunlifecycle.CandidateRequest) (storerunlifecycle.CandidateRequestDisposition, error) {
	if err := r.RequirePresentRun(ctx, request.RunID); err != nil {
		return "", err
	}
	return storerunlifecycle.CandidateRequested, nil
}

func (r *recordingRuntimeMutationRunner) TransitionActiveRun(ctx context.Context, request storerunlifecycle.ActiveTransitionRequest) (storerunlifecycle.MutationDisposition, error) {
	m, err := r.lifecycleMutation(ctx)
	if err != nil {
		return "", err
	}
	return m.TransitionActiveRun(ctx, request)
}

func (r *recordingRuntimeMutationRunner) MarkTerminalRun(ctx context.Context, request storerunlifecycle.TerminalRequest) (storerunlifecycle.Snapshot, storerunlifecycle.MutationDisposition, error) {
	m, err := r.lifecycleMutation(ctx)
	if err != nil {
		return storerunlifecycle.Snapshot{}, "", err
	}
	return m.MarkTerminalRun(ctx, request)
}

func (r *recordingRuntimeMutationRunner) ForkRunSource(ctx context.Context, request storerunlifecycle.ForkSourceRequest) (storerunlifecycle.Snapshot, storerunlifecycle.MutationDisposition, error) {
	m, err := r.lifecycleMutation(ctx)
	if err != nil {
		return storerunlifecycle.Snapshot{}, "", err
	}
	return m.ForkRunSource(ctx, request)
}

func (r *recordingRuntimeMutationRunner) ReviseRunSource(ctx context.Context, request storerunlifecycle.SourceRevisionRequest) (storerunlifecycle.MutationDisposition, error) {
	m, err := r.lifecycleMutation(ctx)
	if err != nil {
		return "", err
	}
	return m.ReviseRunSource(ctx, request)
}

func (r *recordingRuntimeMutationRunner) SyncRunCounters(ctx context.Context, runID string) error {
	m, err := r.lifecycleMutation(ctx)
	if err != nil {
		return err
	}
	return m.SyncRunCounters(ctx, runID)
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
	postCommit := make([]OwnerAction, 0, 4)
	rollbackActions := make([]OwnerAction, 0, 4)
	txctx := withPipelinePostCommitActions(WithPipelineSQLTxContext(ctx, tx), &postCommit)
	txctx = withPipelineRollbackActions(txctx, &rollbackActions)
	dialect := r.dialect
	authorDialect := authoractivityfixture.DialectSQLite
	if dialect == workflowStoreDialectPostgres {
		authorDialect = authoractivityfixture.DialectPostgres
	} else {
		dialect = workflowStoreDialectSQLite
	}
	storyctx, err := authoractivityfixture.Begin(txctx, tx, authorDialect)
	if err != nil {
		flushPipelineRollbackActions(rollbackActions)
		return err
	}
	if err := fn(storyctx); err != nil {
		flushPipelineRollbackActions(rollbackActions)
		return err
	}
	if err := authoractivityfixture.Finalize(storyctx); err != nil {
		flushPipelineRollbackActions(rollbackActions)
		return err
	}
	if err := tx.Commit(); err != nil {
		flushPipelineRollbackActions(rollbackActions)
		return err
	}
	committed = true
	flushPipelinePostCommitActions(postCommit)
	return nil
}

func newSQLiteWorkflowInstanceStoreForTest(t *testing.T, db *sql.DB) *workflowInstanceStore {
	t.Helper()
	store := newTestSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, &recordingRuntimeMutationRunner{db: db, dialect: workflowStoreDialectSQLite})
	store.deliveryStore = newPipelineTestDeliveryOwner(t, db, true)
	return store
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
				origin_kind TEXT NOT NULL,
				trigger_event_id TEXT,
				trigger_event_type TEXT,
				origin_service_id TEXT,
				origin_generation INTEGER,
				forked_from_run_id TEXT,
				forked_from_event_id TEXT,
				continued_as_run_id TEXT,
				event_count INTEGER NOT NULL DEFAULT 0,
				entity_count INTEGER NOT NULL DEFAULT 0,
				failure TEXT,
				started_at TIMESTAMP NOT NULL,
				ended_at TIMESTAMP
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
		`CREATE TABLE flow_instance_runtime_readiness (
			run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
			instance_id TEXT NOT NULL REFERENCES flow_instances(instance_id) ON DELETE CASCADE,
			plan TEXT NOT NULL,
			topology_ready_at TIMESTAMP,
			creation_event_emitted_at TIMESTAMP,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			PRIMARY KEY (run_id, instance_id)
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
			bookkeeping TEXT,
			accumulator TEXT,
			revision INTEGER,
			entered_state_at TIMESTAMP,
			created_at TIMESTAMP,
			updated_at TIMESTAMP,
			PRIMARY KEY (run_id, entity_id)
		)`,
		`CREATE TABLE workflow_instance_initial_materializations (
			run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
			entity_id TEXT NOT NULL,
			instance_id TEXT NOT NULL REFERENCES flow_instances(instance_id) ON DELETE CASCADE,
			projection_version INTEGER NOT NULL CHECK (projection_version = 2),
			projection TEXT NOT NULL,
			occurred_at TIMESTAMP NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (run_id, entity_id),
			UNIQUE (run_id, instance_id),
			FOREIGN KEY (run_id, entity_id) REFERENCES entity_state(run_id, entity_id) ON DELETE CASCADE
		)`,
		`CREATE TABLE timers (
			timer_id TEXT PRIMARY KEY,
			run_id TEXT,
			timer_name TEXT,
			schedule_scope TEXT,
			schedule_key TEXT,
			immutable_hash TEXT,
			source_timer_id TEXT,
			forked_from_run_id TEXT,
			forked_from_event_id TEXT,
			reconstruction_owner TEXT,
			entity_id TEXT,
			flow_scope_key TEXT,
			flow_instance_id TEXT,
			flow_instance TEXT,
			fire_event TEXT,
			fire_payload TEXT,
			routing_source TEXT NOT NULL,
			execution_mode TEXT NOT NULL CHECK (execution_mode IN ('live', 'mock')),
			fire_at TIMESTAMP,
			initial_fire_at TIMESTAMP,
			recurring BOOLEAN,
			recurrence_interval TEXT,
			owner_node TEXT,
			owner_agent TEXT,
			owner_kind TEXT NOT NULL,
			agent_name_owner TEXT,
			agent_name_source TEXT,
			agent_route_presence TEXT,
			agent_flow_scope_key TEXT,
			agent_flow_instance_id TEXT,
			reply_context_id TEXT,
			task_id TEXT,
			due_basis_kind TEXT,
			due_basis_absolute TIMESTAMP,
			due_basis_duration TEXT,
			due_basis_cron TEXT,
			occurrence_event_id TEXT,
			occurrence_admitted_at TIMESTAMP,
			accepted_at TIMESTAMP,
			cancel_cause TEXT,
			cancelled_at TIMESTAMP,
			failure_code TEXT,
			failure_message TEXT,
			failed_at TIMESTAMP,
			task_type TEXT,
			status TEXT,
			fired_at TIMESTAMP,
			created_at TIMESTAMP,
			UNIQUE (schedule_scope, schedule_key)
		)`,
		`CREATE TABLE entity_mutations (
			mutation_id TEXT PRIMARY KEY,
			run_id TEXT,
			entity_id TEXT,
			domain TEXT NOT NULL,
			path TEXT NOT NULL,
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
			payload_bytes BLOB NOT NULL,
			execution_mode TEXT NOT NULL CHECK (execution_mode IN ('live', 'mock')),
			chain_depth INTEGER NOT NULL CHECK (chain_depth >= 0),
			produced_by TEXT NOT NULL CHECK (NULLIF(TRIM(produced_by), '') IS NOT NULL),
			produced_by_type TEXT NOT NULL CHECK (produced_by_type IN ('node', 'agent', 'platform', 'external')),
			source_event_id TEXT,
			created_at TEXT NOT NULL,
			routing_source_kind TEXT NOT NULL CHECK (routing_source_kind IN ('absent', 'external_ingress', 'root', 'static_flow', 'concrete_template_instance', 'flow_owned_control', 'platform_control')),
			routing_source_authority TEXT,
			source_route TEXT NOT NULL CHECK (json_valid(source_route)),
			target_route TEXT NOT NULL CHECK (json_valid(target_route)),
			target_set TEXT NOT NULL CHECK (json_valid(target_set)),
			route_settlement TEXT NOT NULL CHECK (json_valid(route_settlement)),
			operator_reference_event_id TEXT,
			handler_node TEXT,
			idempotency_key TEXT,
			CHECK ((event_class IN ('child', 'replay') AND source_event_id IS NOT NULL AND run_id IS NOT NULL) OR (event_class NOT IN ('child', 'replay') AND source_event_id IS NULL) OR (event_class IN ('runtime_control', 'runtime_diagnostic', 'diagnostic_direct') AND source_event_id IS NOT NULL AND run_id IS NOT NULL)),
			CHECK ((event_class = 'operator_injected') OR operator_reference_event_id IS NULL),
			CHECK ((routing_source_kind IN ('absent', 'platform_control') AND source_route = '{}' AND NULLIF(TRIM(COALESCE(routing_source_authority, '')), '') IS NULL) OR (routing_source_kind = 'external_ingress' AND source_route <> '{}' AND NULLIF(TRIM(COALESCE(routing_source_authority, '')), '') IS NOT NULL) OR (routing_source_kind IN ('root', 'static_flow', 'concrete_template_instance', 'flow_owned_control') AND source_route <> '{}' AND NULLIF(TRIM(COALESCE(routing_source_authority, '')), '') IS NULL))
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
			event_id TEXT NOT NULL,
			route_identity TEXT NOT NULL,
			subscriber_type TEXT NOT NULL,
			subscriber_id TEXT NOT NULL,
			agent_name_owner TEXT NOT NULL,
			agent_name_source TEXT NOT NULL,
			agent_route_presence TEXT NOT NULL,
			agent_flow_scope_key TEXT NOT NULL,
			agent_flow_instance_id TEXT NOT NULL,
			agent_flow_instance_path TEXT NOT NULL,
			delivery_target_route TEXT NOT NULL,
			delivery_context TEXT NOT NULL,
			delivery_payload_projection TEXT NOT NULL,
			connect_execution_claim TEXT NOT NULL,
			execution_authority_kind TEXT NOT NULL,
			authority_bundle_hash TEXT NOT NULL,
			authority_bundle_source TEXT NOT NULL,
			execution_authority_id TEXT NOT NULL,
			execution_authority_generation INTEGER NOT NULL,
			selected_execution_id TEXT,
			selected_fork_run_id TEXT,
			selected_execution_generation INTEGER,
			continuation_handoff_at TIMESTAMP,
			status TEXT NOT NULL,
			retry_count INTEGER NOT NULL,
			max_retries INTEGER NOT NULL,
			next_eligible_at TIMESTAMP,
			claim_version INTEGER NOT NULL,
			current_attempt_version INTEGER,
			current_attempt_open BOOLEAN,
			reason_code TEXT,
			failure TEXT,
			started_at TIMESTAMP,
			settled_at TIMESTAMP,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			UNIQUE(event_id, route_identity)
		)`,
		`CREATE TABLE event_delivery_handler_rule_selections (
			delivery_id TEXT PRIMARY KEY REFERENCES event_deliveries(delivery_id),
			selection_context TEXT NOT NULL CHECK (selection_context IN ('none', 'handler_rules', 'handler_on_complete', 'join_on_complete', 'join_timeout')),
			disposition TEXT NOT NULL CHECK (disposition IN ('selected', 'no_match', 'evaluation_failed', 'not_applicable')),
			package_coordinate TEXT,
			element_id TEXT,
			display_label TEXT NOT NULL DEFAULT '',
			CHECK ((disposition = 'selected' AND selection_context <> 'none' AND NULLIF(TRIM(COALESCE(package_coordinate, '')), '') IS NOT NULL AND element_id IS NOT NULL) OR (disposition = 'evaluation_failed' AND selection_context IN ('handler_rules', 'handler_on_complete') AND NULLIF(TRIM(COALESCE(package_coordinate, '')), '') IS NOT NULL AND element_id IS NOT NULL) OR (disposition = 'no_match' AND selection_context IN ('handler_rules', 'handler_on_complete') AND package_coordinate IS NULL AND element_id IS NULL AND display_label = '') OR (disposition = 'not_applicable' AND selection_context = 'none' AND package_coordinate IS NULL AND element_id IS NULL AND display_label = ''))
		)`,
		`CREATE TABLE event_delivery_attempts (
			delivery_id TEXT NOT NULL,
			claim_version INTEGER NOT NULL,
			claim_token TEXT NOT NULL UNIQUE,
			started_at TIMESTAMP NOT NULL,
			lease_expires_at TIMESTAMP NOT NULL,
			current_delivery_id TEXT,
			active_session_id TEXT,
			session_delivery_id TEXT,
			session_run_id TEXT,
			session_subscriber_type TEXT,
			session_agent_id TEXT,
			session_agent_name_owner TEXT,
			session_agent_name_source TEXT,
			session_agent_route_presence TEXT,
			session_agent_flow_scope_key TEXT,
			session_agent_flow_instance_id TEXT,
			session_agent_flow_instance_path TEXT,
			open_marker BOOLEAN NOT NULL,
			outcome TEXT,
			reason_code TEXT,
			failure TEXT,
			side_effects TEXT NOT NULL DEFAULT '[]',
			duration_ms INTEGER,
			completed_at TIMESTAMP,
			PRIMARY KEY(delivery_id, claim_version)
		)`,
		`CREATE TABLE event_delivery_outcomes (
			delivery_id TEXT NOT NULL,
			claim_version INTEGER NOT NULL,
			outcome TEXT NOT NULL,
			reason_code TEXT,
			failure TEXT,
			side_effects TEXT NOT NULL DEFAULT '[]',
			duration_ms INTEGER NOT NULL,
			settled_at TIMESTAMP NOT NULL,
			PRIMARY KEY(delivery_id, claim_version)
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
			  AND domain = 'authored_field'
			  AND path = ?
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

func assertSQLiteTxTableCount(t *testing.T, tx *sql.Tx, table string, want int) {
	t.Helper()
	var query string
	switch table {
	case "runs":
		query = `SELECT COUNT(*) FROM runs`
	case "entity_mutations":
		query = `SELECT COUNT(*) FROM entity_mutations`
	default:
		t.Fatalf("unsupported sqlite table %q", table)
	}
	var got int
	if err := tx.QueryRow(query).Scan(&got); err != nil {
		t.Fatalf("count sqlite %s rows: %v", table, err)
	}
	if got != want {
		t.Fatalf("sqlite %s rows = %d, want %d", table, got, want)
	}
}
