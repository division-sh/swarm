package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	timerobligationadapter "github.com/division-sh/swarm/internal/store/timerobligationadapter"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

func newTestWorkflowInstanceStore(db *sql.DB) *workflowInstanceStore {
	store := NewPostgresWorkflowPersistence(db, &recordingRuntimeMutationRunner{db: db, dialect: workflowStoreDialectPostgres}).store
	store.runLifecycle = &unavailablePipelineTestRunLifecycle{}
	store.timerObligations = pipelineTestTimerObligationReader{db: db, dialect: timerobligationadapter.DialectPostgres}
	return store
}

func newTestSQLiteWorkflowInstanceStore(db *sql.DB) *workflowInstanceStore {
	reader := &recordingRuntimeMutationRunner{db: db, dialect: workflowStoreDialectSQLite}
	return &workflowInstanceStore{
		db:               db,
		dialect:          workflowStoreDialectSQLite,
		entityQuery:      reader,
		runLifecycle:     &unavailablePipelineTestRunLifecycle{},
		timerObligations: pipelineTestTimerObligationReader{db: db, dialect: timerobligationadapter.DialectSQLite},
	}
}

func newTestSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db *sql.DB, runner runtimeMutationRunner) *workflowInstanceStore {
	store := NewSQLiteWorkflowPersistence(db, runner).store
	store.timerObligations = pipelineTestTimerObligationReader{db: db, dialect: timerobligationadapter.DialectSQLite}
	if owner, ok := runner.(runtimerunlifecycle.OperationOwner); ok {
		store.runLifecycle = owner
	} else {
		store.runLifecycle = &unavailablePipelineTestRunLifecycle{}
	}
	return store
}

type pipelineTestTimerObligationReader struct {
	db      *sql.DB
	dialect timerobligationadapter.Dialect
}

func (r pipelineTestTimerObligationReader) ReadTimerObligations(ctx context.Context, scope runtimetimerobligation.Scope, observedAt time.Time) (runtimetimerobligation.Snapshot, error) {
	if tx, ok := PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return timerobligationadapter.Read(ctx, tx, r.dialect, scope, observedAt)
	}
	return timerobligationadapter.Read(ctx, r.db, r.dialect, scope, observedAt)
}

func workflowPersistenceForTest(store *workflowInstanceStore) WorkflowPersistence {
	return WorkflowPersistence{store: store}
}

const testPipelineRunID = "77777777-7777-7777-7777-777777777777"

func seedPipelineEventRecord(t testing.TB, ctx context.Context, db *sql.DB, event events.Event) {
	seedPipelineEventRecordForDialect(t, ctx, db, runtimeauthoractivity.DialectPostgres, event)
}

func seedPipelineEventRecordForDialect(t testing.TB, ctx context.Context, db *sql.DB, dialect runtimeauthoractivity.Dialect, event events.Event) {
	t.Helper()
	if runID := strings.TrimSpace(event.RunID()); runID != "" {
		switch dialect {
		case runtimeauthoractivity.DialectPostgres:
			runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
		case runtimeauthoractivity.DialectSQLite:
			runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
		default:
			t.Fatalf("seed canonical pipeline event %s: unsupported dialect %q", event.ID(), dialect)
		}
	}
	if err := eventfixture.Insert(ctx, db, dialect, event); err != nil {
		t.Fatalf("seed canonical pipeline event %s: %v", event.ID(), err)
	}
}

func testPipelineRunContext(t *testing.T, db *sql.DB) context.Context {
	t.Helper()
	if db == nil {
		t.Fatal("test pipeline run context requires db")
	}
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), testPipelineRunID)
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: testPipelineRunID})
	return ctx
}

func testPipelineRunContextNoSeed(t *testing.T) context.Context {
	t.Helper()
	return runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), testPipelineRunID)
}

func testWorkflowStoreRunContext(t *testing.T, store *workflowInstanceStore) context.Context {
	t.Helper()
	if store == nil {
		t.Fatal("test workflow store run context requires store")
	}
	if store.db == nil {
		return testPipelineRunContextNoSeed(t)
	}
	return testPipelineRunContext(t, store.db)
}

func materializedWorkflowInstanceForTest(instance WorkflowInstance) WorkflowInstance {
	occurredAt := time.Date(2026, time.July, 25, 12, 0, 0, 0, time.UTC)
	if instance.EnteredStageAt.IsZero() {
		instance.EnteredStageAt = occurredAt
	}
	if instance.CreatedAt.IsZero() {
		instance.CreatedAt = occurredAt
	}
	return instance
}

func configureWorkflowLifecycleForTest(t testing.TB, pc *PipelineCoordinator) {
	t.Helper()
	if pc == nil || pc.workflowStore == nil {
		return
	}
	if pc.workflowTimers == nil {
		pc.workflowTimers = newWorkflowTimerLifecycle(pc.workflowStore, pc.SemanticSource(), pc.bus, pc.gatePublisher, pc.workOwner, pc.timerScheduler)
	}
	if pc.workflowStore.lifecycleOwner == nil {
		pc.workflowStore.lifecycleOwner = pipelineWorkflowLifecycleOwner{coordinator: pc}
	}
}

func testPipelineCoordinatorRunContext(t *testing.T, pc *PipelineCoordinator) context.Context {
	t.Helper()
	if pc == nil {
		t.Fatal("test pipeline coordinator run context requires coordinator")
	}
	configureWorkflowLifecycleForTest(t, pc)
	if pc.workflowStore != nil && pc.workflowStore.db != nil {
		configurePipelineTestDeliveryOwner(t, pc)
	}
	var ctx context.Context
	if pc.workflowStore != nil {
		ctx = testWorkflowStoreRunContext(t, pc.workflowStore)
	} else {
		ctx = testPipelineRunContextNoSeed(t)
	}
	if !pc.runtimeReceiver {
		return ctx
	}
	bound, err := pc.receiverExecution.Bind(ctx, executionmode.Live)
	if err != nil {
		t.Fatalf("bind test Pipeline receiver execution: %v", err)
	}
	return bound
}

func testWorkflowStateTransitionContext(ctx context.Context, entityID, eventType string) context.Context {
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(), events.EventType(strings.TrimSpace(eventType)), "test", "", []byte(`{}`), 0,
		runtimecorrelation.RunIDFromContext(ctx), "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC(),
	)
	return runtimecorrelation.WithInboundEvent(ctx, evt)
}

func testPersistedWorkflowStateTransitionContext(t *testing.T, store *workflowInstanceStore, ctx context.Context, entityID, eventType string) context.Context {
	t.Helper()
	transitionCtx := testWorkflowStateTransitionContext(ctx, entityID, eventType)
	evt, ok := runtimecorrelation.InboundEventFromContext(transitionCtx)
	if !ok {
		t.Fatal("test workflow transition context has no inbound event")
	}
	seedExactOnceEvent(t, store, ctx, evt)
	return transitionCtx
}

func seedPipelineNodeDeliveryAuthority(t *testing.T, db *sql.DB, evt events.Event, nodeID string) events.DeliveryRoute {
	t.Helper()
	return seedPipelineNodeDeliveryRouteAuthority(t, db, evt, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(strings.TrimSpace(nodeID)), Target: evt.TargetRoute()})
}

func seedPipelineNodeDeliveryRouteAuthority(t *testing.T, db *sql.DB, evt events.Event, route events.DeliveryRoute) events.DeliveryRoute {
	t.Helper()
	if db == nil {
		t.Fatal("seed pipeline node delivery authority requires db")
	}
	route = route.Normalized()
	if !route.Recipient.IsNode() {
		t.Fatal("seed pipeline node delivery authority requires nodeID")
	}
	eventID := strings.TrimSpace(evt.ID())
	if _, err := uuid.Parse(eventID); err != nil {
		t.Fatalf("seed pipeline node delivery authority event id = %q: %v", eventID, err)
	}
	runID := strings.TrimSpace(evt.RunID())
	if runID == "" {
		runID = testPipelineRunID
	}
	if _, err := uuid.Parse(runID); err != nil {
		t.Fatalf("seed pipeline node delivery authority run id = %q: %v", runID, err)
	}
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID)
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
	entityID := ""
	if raw := strings.TrimSpace(evt.EntityID()); raw != "" {
		if _, err := uuid.Parse(raw); err == nil {
			entityID = raw
		}
	}
	createdAt := evt.CreatedAt()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE event_id = $1::uuid)`, eventID).Scan(&exists); err != nil {
		t.Fatalf("load pipeline node delivery authority event: %v", err)
	}
	deliveryEvent := evt
	if !exists || strings.TrimSpace(evt.RunID()) != runID {
		envelope := events.EventEnvelope{Scope: events.EventScopeGlobal}
		if entityID != "" {
			envelope = events.EnvelopeForEntityID(events.EventEnvelope{}, entityID)
			if flowInstance := strings.TrimSpace(evt.FlowInstance()); flowInstance != "" {
				envelope = events.EnvelopeForFlowInstance(envelope, flowInstance)
			}
		}
		fixture := eventtest.PersistedProjectionForProducer(
			eventID, evt.Type(), evt.Producer(), evt.TaskID(), evt.Payload(), evt.ChainDepth(), runID, evt.ParentEventID(), envelope, createdAt,
		)
		deliveryEvent = fixture
		if !exists {
			seedPipelineEventRecord(t, ctx, db, fixture)
		}
	}
	owner := newPipelineTestDeliveryOwnerForDB(t, db)
	if err := owner.commitInitial(ctx, deliveryEvent, route); err != nil {
		t.Fatalf("seed pipeline node delivery authority: %v", err)
	}
	return route
}
