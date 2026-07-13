package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	canonicalrouting "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestCreateEntityHandlerEffectsAreExactOnceAcrossStoreMutations(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/pipeline/create_entity_exact_once_test.go:newExactOnceCoordinator"))
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T) (*PipelineCoordinator, context.Context, *recordingPipelineBus, *recordingScheduleStore, *recordingMailboxWriteMaterializer)
	}{
		{
			name: "sqlite",
			setup: func(t *testing.T) (*PipelineCoordinator, context.Context, *recordingPipelineBus, *recordingScheduleStore, *recordingMailboxWriteMaterializer) {
				db := newSQLiteWorkflowInstanceStoreTestDB(t)
				ctx := sqliteExactOnceRunContext(t, db)
				return newExactOnceCoordinator(t, db, newSQLiteWorkflowInstanceStoreForTest(t, db)), ctx, nil, nil, nil
			},
		},
		{
			name: "postgres",
			setup: func(t *testing.T) (*PipelineCoordinator, context.Context, *recordingPipelineBus, *recordingScheduleStore, *recordingMailboxWriteMaterializer) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				pc := newExactOnceCoordinator(t, db, NewWorkflowInstanceStore(db))
				return pc, testPipelineCoordinatorRunContext(t, pc), nil, nil, nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pc, ctx, _, _, _ := tc.setup(t)
			bus := pc.bus.(*recordingPipelineBus)
			schedules := pc.timerScheduleStore.(*recordingScheduleStore)
			mailbox := pc.mailboxMaterializer.(*recordingMailboxWriteMaterializer)
			eventID := uuid.NewString()
			evt := eventtest.RootIngress(eventID,
				events.EventType("thing.created"), "", "", mustJSON(map[string]any{"amount": 250, "who": "alice"}), 0, runtimecorrelation.RunIDFromContext(ctx), "", events.EventEnvelope{}, time.Now().UTC())

			seedExactOnceEvent(t, pc.workflowStore, ctx, evt)

			result, err := pc.executeNodeContractHandler(ctx, "w-node", exactOnceCreateEntityHandler(), workflowTriggerContext{
				Event:           evt,
				HandlerEventKey: "thing.created",
				State: WorkflowState{
					Stage:    WorkflowStateID("new"),
					Metadata: map[string]any{},
				},
			}, false)
			if err != nil {
				t.Fatalf("executeNodeContractHandler: %v", err)
			}
			if !result.Handled {
				t.Fatal("expected handler to run")
			}
			if got := bus.publishedCount(); got != 1 {
				t.Fatalf("published event count = %d, want 1", got)
			}
			if got := bus.outboxCount(); got != 1 {
				t.Fatalf("outbox intent count = %d, want 1", got)
			}
			if got := mailbox.calls; got != 1 {
				t.Fatalf("mailbox materialization calls = %d, want 1", got)
			}
			if got := len(mailbox.rows()); got != 1 {
				t.Fatalf("mailbox rows = %d, want 1", got)
			}
			if got := len(schedules.upserts); got != 1 {
				t.Fatalf("timer schedule upserts = %d, want 1", got)
			}
			if got := strings.TrimSpace(schedules.upserts[0].EventType); got != "timer.check" {
				t.Fatalf("timer schedule event = %q, want timer.check", got)
			}

			entityID := bus.publishedEvent(0).EntityID()
			if entityID == "" {
				t.Fatal("expected emitted event to carry created entity id")
			}
			instance, ok, err := pc.workflowStore.Load(ctx, entityID)
			if err != nil {
				t.Fatalf("load created entity: %v", err)
			}
			if !ok {
				t.Fatal("created entity missing")
			}
			if got := strings.TrimSpace(instance.CurrentState); got != "done" {
				t.Fatalf("current state = %q, want done", got)
			}
			assertMetadataNumber(t, instance.Metadata, "amount", 250)
			assertMetadataString(t, instance.Metadata, "who", "alice")
			assertMetadataNumber(t, instance.Metadata, "counter", 1)
			assertGateSet(t, instance.Metadata, "ready")

			assertMutationCount(t, pc.workflowStore, ctx, eventID, "amount", "entity_initial_value", "create_entity", 1)
			assertMutationCount(t, pc.workflowStore, ctx, eventID, "who", "entity_initial_value", "create_entity", 1)
			assertMutationCount(t, pc.workflowStore, ctx, eventID, "counter", "entity_initial_value", "create_entity", 1)
			assertMutationCount(t, pc.workflowStore, ctx, eventID, "amount", "workflow_instance_store", "upsert", 1)
			assertMutationCount(t, pc.workflowStore, ctx, eventID, "who", "workflow_instance_store", "upsert", 1)
			assertMutationCount(t, pc.workflowStore, ctx, eventID, "counter", "workflow_instance_store", "upsert", 1)
		})
	}
}

func TestDispatchWorkflowNodeEventSkipsAlreadyProcessedCreateEntityHandler(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T) (*PipelineCoordinator, context.Context)
	}{
		{
			name: "sqlite",
			setup: func(t *testing.T) (*PipelineCoordinator, context.Context) {
				db := newSQLiteWorkflowInstanceStoreTestDB(t)
				pc := newExactOnceCoordinator(t, db, newSQLiteWorkflowInstanceStoreForTest(t, db))
				return pc, sqliteExactOnceRunContext(t, db)
			},
		},
		{
			name: "postgres",
			setup: func(t *testing.T) (*PipelineCoordinator, context.Context) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				pc := newExactOnceCoordinator(t, db, NewWorkflowInstanceStore(db))
				return pc, testPipelineCoordinatorRunContext(t, pc)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pc, ctx := tc.setup(t)
			bus := pc.bus.(*recordingPipelineBus)
			mailbox := pc.mailboxMaterializer.(*recordingMailboxWriteMaterializer)
			eventID := uuid.NewString()
			evt := eventtest.RootIngress(eventID,
				events.EventType("thing.created"), "", "", mustJSON(map[string]any{"amount": 250, "who": "alice"}), 0, runtimecorrelation.RunIDFromContext(ctx), "", events.EventEnvelope{}, time.Now().UTC())

			seedExactOnceEventDelivery(t, pc.workflowStore, ctx, evt, "w-node")

			handled, err := pc.dispatchWorkflowNodeEventResult(ctx, evt)
			if err != nil {
				t.Fatalf("first dispatchWorkflowNodeEventResult: %v", err)
			}
			if !handled {
				t.Fatal("first dispatch handled = false, want true")
			}
			handled, err = pc.dispatchWorkflowNodeEventResult(ctx, evt)
			if err != nil {
				t.Fatalf("second dispatchWorkflowNodeEventResult: %v", err)
			}
			if !handled {
				t.Fatal("second dispatch handled = false, want true for already processed node event")
			}

			if got := bus.publishedCount(); got != 1 {
				t.Fatalf("published event count after duplicate dispatch = %d, want 1", got)
			}
			if got := mailbox.calls; got != 1 {
				t.Fatalf("mailbox materialization calls after duplicate dispatch = %d, want 1", got)
			}
			assertReceiptCount(t, pc.workflowStore, ctx, eventID, "w-node", 1)
			assertDeliveryStatusCount(t, pc.workflowStore, ctx, eventID, "w-node", "delivered", 1)
			assertMutationCount(t, pc.workflowStore, ctx, eventID, "amount", "entity_initial_value", "create_entity", 1)
			assertMutationCount(t, pc.workflowStore, ctx, eventID, "amount", "workflow_instance_store", "upsert", 1)
		})
	}
}

func newExactOnceCoordinator(t *testing.T, db *sql.DB, store *WorkflowInstanceStore) *PipelineCoordinator {
	// routing-example-census: different-concept issue=none owner=pipeline.create_entity_exact_once proof=internal/runtime/pipeline/create_entity_exact_once_test.go:TestCreateEntityHandlerEffectsAreExactOnceAcrossStoreMutations
	t.Helper()
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml": `
name: exact-once-test
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: validation
    flow: validation
    mode: static
`,
		"schema.yaml": "name: exact-once-test\n",
		"flows/validation/schema.yaml": `
name: validation
mode: static
initial_state: new
terminal_states: [done]
states: [new, done]
pins:
  inputs:
    events: [thing.created, timer.check]
  outputs:
    events: [thing.emitted]
`,
		"flows/validation/entities.yaml": `
widget:
  amount:
    type: integer
    initial: 0
  who:
    type: text
    initial: ""
  counter:
    type: integer
    initial: 0
`,
		"flows/validation/events.yaml": `
thing.created:
  swarm:
    source: external
  amount: integer
  who: text
thing.emitted:
  amount: integer
  who: text
timer.check: {}
`,
		"flows/validation/nodes.yaml": `
w-node:
  id: w-node
  execution_type: system_node
  subscribes_to: [thing.created, timer.check]
  produces: [thing.emitted, timer.check]
  timers:
    - id: check_timer
      event: timer.check
      delay: 1h
      start_on: event:thing.created
  event_handlers:
    thing.created:
      create_entity: true
      data_accumulation:
        source_event: thing.created
        writes:
          - source_field: amount
            target_field: amount
          - source_field: who
            target_field: who
          - target_field: counter
            value:
              cel: entity.counter + 1
      sets_gate: ready
      advances_to: done
      emit:
        event: thing.emitted
        broadcast: true
        fields:
          amount:
            cel: entity.amount
          who:
            cel: entity.who
      action:
        id: mailbox_write
        mailbox:
          item_type:
            literal: approval
          severity:
            literal: normal
          summary:
            literal: created
          payload:
            amount:
              ref: payload.amount
`,
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected exact-once workflow bundle")
	}
	nodes, err := LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("load workflow nodes: %v", err)
	}
	bus := &recordingPipelineBus{}
	return NewPipelineCoordinatorWithOptions(bus, db, PipelineCoordinatorOptions{
		WorkflowStore:       store,
		TimerScheduleStore:  &recordingScheduleStore{},
		MailboxMaterializer: &recordingMailboxWriteMaterializer{},
		EventReceiptsCapability: func(context.Context) (bool, error) {
			return true, nil
		},
		Module: &previewWorkflowModule{
			bundle:        bundle,
			workflowNodes: nodes,
			workflow: NewWorkflowDefinition("validation", []WorkflowStage{
				{Name: "new"},
				{Name: "done", Terminal: true},
			}, []WorkflowTransition{
				{Name: "complete", From: []WorkflowStateID{"new"}, To: "done", Trigger: "thing.created", Node: "w-node"},
			}),
		},
	})
}

func exactOnceCreateEntityHandler() runtimecontracts.SystemNodeEventHandler {
	return runtimecontracts.SystemNodeEventHandler{
		CreateEntity: true,
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			SourceEvent: "thing.created",
			Writes: []runtimecontracts.WorkflowDataWrite{
				{SourceField: "amount", TargetField: "amount"},
				{SourceField: "who", TargetField: "who"},
				{TargetField: "counter", Value: runtimecontracts.CELExpression("entity.counter + 1")},
			},
		},
		SetsGate:   &runtimecontracts.GateSpec{Name: "ready"},
		AdvancesTo: "done",
		Emit: runtimecontracts.EmitSpec{
			Event:     "thing.emitted",
			Broadcast: true,
			Fields: map[string]runtimecontracts.ExpressionValue{
				"amount": runtimecontracts.CELExpression("entity.amount"),
				"who":    runtimecontracts.CELExpression("entity.who"),
			},
		},
		Action: runtimecontracts.ActionSpec{
			ID: "mailbox_write",
			Mailbox: &runtimecontracts.MailboxWriteSpec{
				ItemType: runtimecontracts.LiteralExpression("approval"),
				Severity: runtimecontracts.LiteralExpression("normal"),
				Summary:  runtimecontracts.LiteralExpression("created"),
				Payload: map[string]runtimecontracts.ExpressionValue{
					"amount": runtimecontracts.RefExpression("payload.amount"),
				},
			},
		},
	}
}

func sqliteExactOnceRunContext(t *testing.T, db *sql.DB) context.Context {
	t.Helper()
	ctx := runtimecorrelation.WithRunID(context.Background(), testPipelineRunID)
	if _, err := db.ExecContext(ctx, `
		INSERT OR IGNORE INTO runs (run_id, status, started_at)
		VALUES (?, 'running', ?)
	`, testPipelineRunID, time.Now().UTC()); err != nil {
		t.Fatalf("seed sqlite run: %v", err)
	}
	return ctx
}

func seedExactOnceEventDelivery(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, evt events.Event, nodeID string) {
	t.Helper()
	seedExactOnceEvent(t, store, ctx, evt)
	runID := runtimecorrelation.RunIDFromContext(ctx)
	if store.isSQLite() {
		if _, err := store.db.ExecContext(ctx, `
			INSERT INTO event_deliveries (delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, retry_count, created_at)
			VALUES (?, ?, ?, 'node', ?, 'pending', 0, ?)
		`, uuid.NewString(), runID, evt.ID(), nodeID, evt.CreatedAt()); err != nil {
			t.Fatalf("seed sqlite delivery: %v", err)
		}
		return
	}
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO event_deliveries (run_id, event_id, subscriber_type, subscriber_id, status, retry_count, created_at)
		VALUES ($1::uuid, $2::uuid, 'node', $3, 'pending', 0, $4)
	`, runID, evt.ID(), nodeID, evt.CreatedAt()); err != nil {
		t.Fatalf("seed postgres delivery: %v", err)
	}
}

func seedExactOnceEvent(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, evt events.Event) {
	t.Helper()
	runID := runtimecorrelation.RunIDFromContext(ctx)
	if store.isSQLite() {
		if _, err := store.db.ExecContext(ctx, `
			INSERT OR IGNORE INTO events (event_id, run_id, event_name, scope, payload, chain_depth, produced_by_type, created_at)
			VALUES (?, ?, ?, 'global', ?, ?, 'agent', ?)
		`, evt.ID(), runID, strings.TrimSpace(string(evt.Type())), string(evt.Payload()), evt.ChainDepth(), evt.CreatedAt()); err != nil {
			t.Fatalf("seed sqlite event: %v", err)
		}
		return
	}
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO events (event_id, run_id, event_name, scope, payload, chain_depth, produced_by_type, created_at)
		VALUES ($1::uuid, $2::uuid, $3, 'global', $4::jsonb, $5, 'agent', $6)
		ON CONFLICT (event_id) DO NOTHING
	`, evt.ID(), runID, strings.TrimSpace(string(evt.Type())), string(evt.Payload()), evt.ChainDepth(), evt.CreatedAt()); err != nil {
		t.Fatalf("seed postgres event: %v", err)
	}
}

func assertMutationCount(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, eventID, field, writerID, handlerStep string, want int) {
	t.Helper()
	var (
		got int
		err error
	)
	if store.isSQLite() {
		err = store.db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM entity_mutations
			WHERE caused_by_event = ?
			  AND field = ?
			  AND writer_id = ?
			  AND handler_step = ?
		`, eventID, field, writerID, handlerStep).Scan(&got)
	} else {
		err = store.db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM entity_mutations
			WHERE caused_by_event = $1::uuid
			  AND field = $2
			  AND writer_id = $3
			  AND handler_step = $4
		`, eventID, field, writerID, handlerStep).Scan(&got)
	}
	if err != nil {
		t.Fatalf("count mutation rows: %v", err)
	}
	if got != want {
		t.Fatalf("mutation count event=%s field=%s writer=%s step=%s = %d, want %d", eventID, field, writerID, handlerStep, got, want)
	}
}

func assertReceiptCount(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, eventID, nodeID string, want int) {
	t.Helper()
	var got int
	var err error
	if store.isSQLite() {
		err = store.db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM event_receipts
			WHERE event_id = ?
			  AND subscriber_type = 'node'
			  AND subscriber_id = ?
		`, eventID, nodeID).Scan(&got)
	} else {
		err = store.db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM event_receipts
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = $2
		`, eventID, nodeID).Scan(&got)
	}
	if err != nil {
		t.Fatalf("count event_receipts: %v", err)
	}
	if got != want {
		t.Fatalf("event receipt count = %d, want %d", got, want)
	}
}

func assertDeliveryStatusCount(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, eventID, nodeID, status string, want int) {
	t.Helper()
	var got int
	var err error
	if store.isSQLite() {
		err = store.db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM event_deliveries
			WHERE event_id = ?
			  AND subscriber_type = 'node'
			  AND subscriber_id = ?
			  AND status = ?
		`, eventID, nodeID, status).Scan(&got)
	} else {
		err = store.db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM event_deliveries
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = $2
			  AND status = $3
		`, eventID, nodeID, status).Scan(&got)
	}
	if err != nil {
		t.Fatalf("count event_deliveries: %v", err)
	}
	if got != want {
		t.Fatalf("delivery status %s count = %d, want %d", status, got, want)
	}
}

func assertMetadataNumber(t *testing.T, metadata map[string]any, key string, want float64) {
	t.Helper()
	got := metadata[key]
	switch v := got.(type) {
	case int:
		if float64(v) == want {
			return
		}
	case int64:
		if float64(v) == want {
			return
		}
	case float64:
		if v == want {
			return
		}
	}
	t.Fatalf("metadata[%s] = %#v (%T), want %v", key, got, got, want)
}

func assertMetadataString(t *testing.T, metadata map[string]any, key, want string) {
	t.Helper()
	if got := strings.TrimSpace(asString(metadata[key])); got != want {
		t.Fatalf("metadata[%s] = %#v, want %q", key, metadata[key], want)
	}
}

func assertGateSet(t *testing.T, metadata map[string]any, gate string) {
	t.Helper()
	gates := payloadMap(metadata["gates"])
	if truthyMetadataFlag(gates[gate]) {
		return
	}
	for key, value := range gates {
		key = strings.TrimSpace(key)
		if (strings.HasSuffix(key, "."+gate) || strings.HasSuffix(key, "/"+gate)) && truthyMetadataFlag(value) {
			return
		}
	}
	t.Fatalf("gates = %#v, want %s true", gates, gate)
}
