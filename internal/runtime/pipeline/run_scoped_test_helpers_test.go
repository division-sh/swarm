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
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

func newTestWorkflowInstanceStore(db *sql.DB) *workflowInstanceStore {
	return NewPostgresWorkflowPersistence(db, &recordingRuntimeMutationRunner{db: db, dialect: workflowStoreDialectPostgres}).store
}

func newTestSQLiteWorkflowInstanceStore(db *sql.DB) *workflowInstanceStore {
	return &workflowInstanceStore{db: db, dialect: workflowStoreDialectSQLite}
}

func newTestSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db *sql.DB, runner runtimeMutationRunner) *workflowInstanceStore {
	store := NewSQLiteWorkflowPersistence(db, runner).store
	if owner, ok := runner.(runtimerunlifecycle.OperationOwner); ok {
		store.runLifecycle = owner
	}
	return store
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
	if pc.db != nil {
		return testPipelineRunContext(t, pc.db)
	}
	if pc.workflowStore != nil {
		return testWorkflowStoreRunContext(t, pc.workflowStore)
	}
	return testPipelineRunContextNoSeed(t)
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
	if db == nil {
		t.Fatal("seed pipeline node delivery authority requires db")
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
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
	route := events.DeliveryRoute{SubscriberType: "node", SubscriberID: nodeID, Target: evt.TargetRoute()}
	if err := owner.commitInitial(ctx, deliveryEvent, route); err != nil {
		t.Fatalf("seed pipeline node delivery authority: %v", err)
	}
	return route
}
