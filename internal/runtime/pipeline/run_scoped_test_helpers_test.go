package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

const testPipelineRunID = "77777777-7777-7777-7777-777777777777"

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
	evt := eventtest.RootIngress(
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
	var entityID any
	if raw := strings.TrimSpace(evt.EntityID()); raw != "" {
		if _, err := uuid.Parse(raw); err == nil {
			entityID = raw
		}
	}
	payload := strings.TrimSpace(string(evt.Payload()))
	if payload == "" {
		payload = "{}"
	}
	eventType := strings.TrimSpace(string(evt.Type()))
	if eventType == "" {
		eventType = "test.event"
	}
	flowInstance := strings.TrimSpace(evt.FlowInstance())
	if flowInstance == "" {
		flowInstance = "runtime"
	}
	scope := strings.TrimSpace(string(evt.Scope()))
	if scope == "" {
		scope = "entity"
	}
	sourceAgent := strings.TrimSpace(evt.SourceAgent())
	if sourceAgent == "" {
		sourceAgent = "test"
	}
	createdAt := evt.CreatedAt()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			event_id, run_id, event_name, entity_id, flow_instance, scope, payload,
			produced_by, produced_by_type, created_at
		) VALUES ('live',
			$1::uuid, $2::uuid, $3, $4::uuid, $5, $6, $7::jsonb,
			$8, 'agent', $9
		)
		ON CONFLICT (event_id) DO NOTHING
	`, eventID, runID, eventType, entityID, flowInstance, scope, payload, sourceAgent, createdAt); err != nil {
		t.Fatalf("seed pipeline node delivery authority event: %v", err)
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
