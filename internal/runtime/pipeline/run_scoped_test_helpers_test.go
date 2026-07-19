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
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/google/uuid"
)

const testPipelineRunID = "77777777-7777-7777-7777-777777777777"

func seedPipelineEventRecord(t testing.TB, ctx context.Context, db *sql.DB, event events.Event) {
	seedPipelineEventRecordForDialect(t, ctx, db, runtimeauthoractivity.DialectPostgres, event)
}

func seedPipelineEventRecordForDialect(t testing.TB, ctx context.Context, db *sql.DB, dialect runtimeauthoractivity.Dialect, event events.Event) {
	t.Helper()
	if runID := strings.TrimSpace(event.RunID()); runID != "" {
		var (
			query string
			args  []any
		)
		switch dialect {
		case runtimeauthoractivity.DialectPostgres:
			query = `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running') ON CONFLICT (run_id) DO NOTHING`
			args = []any{runID}
		case runtimeauthoractivity.DialectSQLite:
			query = `INSERT INTO runs (run_id, status) VALUES (?, 'running') ON CONFLICT (run_id) DO NOTHING`
			args = []any{runID}
		default:
			t.Fatalf("seed canonical pipeline event %s: unsupported dialect %q", event.ID(), dialect)
		}
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("seed canonical pipeline run %s: %v", runID, err)
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
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), testPipelineRunID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, testPipelineRunID); err != nil {
		t.Fatalf("seed pipeline test run: %v", err)
	}
	return ctx
}

func testPipelineRunContextNoSeed() context.Context {
	return runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), testPipelineRunID)
}

func testWorkflowStoreRunContext(t *testing.T, store *WorkflowInstanceStore) context.Context {
	t.Helper()
	if store == nil {
		t.Fatal("test workflow store run context requires store")
	}
	if store.db == nil {
		return testPipelineRunContextNoSeed()
	}
	return testPipelineRunContext(t, store.db)
}

func testPipelineCoordinatorRunContext(t *testing.T, pc *PipelineCoordinator) context.Context {
	t.Helper()
	if pc == nil {
		t.Fatal("test pipeline coordinator run context requires coordinator")
	}
	if pc.db != nil {
		return testPipelineRunContext(t, pc.db)
	}
	if pc.workflowStore != nil {
		return testWorkflowStoreRunContext(t, pc.workflowStore)
	}
	return testPipelineRunContextNoSeed()
}

func testWorkflowStateTransitionContext(ctx context.Context, entityID, eventType string) context.Context {
	evt := eventtest.RunCreatingRootIngress(
		uuid.NewString(), events.EventType(strings.TrimSpace(eventType)), "test", "", []byte(`{}`), 0,
		runtimecorrelation.RunIDFromContext(ctx), "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC(),
	)
	return runtimecorrelation.WithInboundEvent(ctx, evt)
}

func testPersistedWorkflowStateTransitionContext(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, entityID, eventType string) context.Context {
	t.Helper()
	transitionCtx := testWorkflowStateTransitionContext(ctx, entityID, eventType)
	evt, ok := runtimecorrelation.InboundEventFromContext(transitionCtx)
	if !ok {
		t.Fatal("test workflow transition context has no inbound event")
	}
	seedExactOnceEvent(t, store, ctx, evt)
	return transitionCtx
}

func seedPipelineNodeDeliveryAuthority(t *testing.T, db *sql.DB, evt events.Event, nodeID string) {
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
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, runID); err != nil {
		t.Fatalf("seed pipeline node delivery authority run: %v", err)
	}
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
	if !exists {
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
		seedPipelineEventRecord(t, ctx, db, fixture)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, reason_code, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'node', $3, 'pending', 'test_node_delivery_authority', now()
		)
		ON CONFLICT DO NOTHING
	`, runID, eventID, nodeID); err != nil {
		t.Fatalf("seed pipeline node delivery authority row: %v", err)
	}
}
