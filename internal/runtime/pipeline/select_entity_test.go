package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/testutil"
)

func TestExecuteNodeContractHandlerSelectEntityUpdatesTargetOwnedEntity(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	budgetEntityID := seedSelectEntityBudget(t, pc.workflowStore, ctx, source, "vertical-1", 0)

	result, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectEntitySpendHandler(), workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("opco.spend_recorded"),
			Payload: mustJSON(map[string]any{"vertical_id": "vertical-1", "amount_usd": 42}),
		}.WithEntityID("22222222-2222-2222-2222-222222222222"),
		State: WorkflowState{},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected selected handler to run")
	}

	instance, ok, err := pc.workflowStore.Load(ctx, budgetEntityID)
	if err != nil {
		t.Fatalf("workflowStore.Load: %v", err)
	}
	if !ok {
		t.Fatal("expected budget entity to exist")
	}
	if got := instance.Metadata["spent_usd"]; got != float64(42) && got != 42 {
		t.Fatalf("spent_usd = %#v, want 42", got)
	}
	if got := FlowInstanceEntityID(instance.StorageRef); got != budgetEntityID {
		t.Fatalf("selected entity storage identity = %q, want %q", got, budgetEntityID)
	}
	assertEntityStateRowCount(t, db, 1)
}

func TestExecuteNodeContractHandlerSelectEntityReplayUsesSameTargetEntity(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	budgetEntityID := seedSelectEntityBudget(t, pc.workflowStore, ctx, source, "vertical-1", 0)

	for _, amount := range []int{42, 99} {
		result, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectEntitySpendHandler(), workflowTriggerContext{
			Event: events.Event{
				Type:    events.EventType("opco.spend_recorded"),
				Payload: mustJSON(map[string]any{"vertical_id": "vertical-1", "amount_usd": amount}),
			}.WithEntityID("22222222-2222-2222-2222-222222222222"),
			State: WorkflowState{},
		}, false)
		if err != nil {
			t.Fatalf("executeNodeContractHandler amount %d: %v", amount, err)
		}
		if !result.Handled {
			t.Fatalf("expected selected handler to run for amount %d", amount)
		}
		assertEntityStateRowCount(t, db, 1)
	}

	instance, ok, err := pc.workflowStore.Load(ctx, budgetEntityID)
	if err != nil {
		t.Fatalf("workflowStore.Load: %v", err)
	}
	if !ok {
		t.Fatal("expected budget entity to exist")
	}
	if got := instance.Metadata["spent_usd"]; got != float64(99) && got != 99 {
		t.Fatalf("spent_usd after replay = %#v, want 99", got)
	}
}

func TestBackgroundWorkflowNodeSelectEntityDuplicateSameEventIsReceiptIdempotent(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	pc.eventReceiptsCapability = eventReceiptsCapabilityStub{enabled: true}.resolve
	ctx := testPipelineCoordinatorRunContext(t, pc)
	budgetEntityID := seedSelectEntityBudget(t, pc.workflowStore, ctx, source, "vertical-1", 0)
	evt := seedSelectEntitySpendEvent(t, db, ctx, map[string]any{"vertical_id": "vertical-1", "amount_usd": 42})
	runner := newSelectEntityBackgroundNode(t, pc, source, db)
	runner.SetRetryPolicyForTest(1, func(int) time.Duration { return 0 })

	runner.ProcessEventForTest(ctx, evt)
	instance, ok, err := pc.workflowStore.Load(ctx, budgetEntityID)
	if err != nil {
		t.Fatalf("workflowStore.Load after first delivery: %v", err)
	}
	if !ok {
		t.Fatal("expected budget entity to exist after first delivery")
	}
	if got := instance.Metadata["spent_usd"]; got != float64(42) && got != 42 {
		t.Fatalf("spent_usd after first delivery = %#v, want 42", got)
	}

	if err := pc.workflowStore.Mutate(ctx, budgetEntityID, func(instance *WorkflowInstance) {
		if instance.Metadata == nil {
			instance.Metadata = map[string]any{}
		}
		instance.Metadata["spent_usd"] = 7
	}); err != nil {
		t.Fatalf("workflowStore.Mutate between duplicate deliveries: %v", err)
	}

	runner.ProcessEventForTest(ctx, evt)
	instance, ok, err = pc.workflowStore.Load(ctx, budgetEntityID)
	if err != nil {
		t.Fatalf("workflowStore.Load after duplicate delivery: %v", err)
	}
	if !ok {
		t.Fatal("expected budget entity to exist after duplicate delivery")
	}
	if got := instance.Metadata["spent_usd"]; got != float64(7) && got != 7 {
		t.Fatalf("spent_usd after duplicate same-event delivery = %#v, want unchanged 7", got)
	}
	assertSelectEntityReceiptRow(t, db, evt.ID, "treasury-orchestrator")
	assertEntityStateRowCount(t, db, 1)
}

func TestExecuteNodeContractHandlerSelectEntityIgnoresTerminalAndTerminatedMatches(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	activeBudgetID := seedSelectEntityBudgetWithState(t, pc.workflowStore, ctx, source, "budget-active", "vertical-1", 0, "active")
	terminalBudgetID := seedSelectEntityBudgetWithState(t, pc.workflowStore, ctx, source, "budget-archived", "vertical-1", 10, "archived")
	terminatedBudgetID := seedSelectEntityBudgetWithState(t, pc.workflowStore, ctx, source, "budget-terminated", "vertical-1", 20, "active")
	terminated, ok, err := pc.workflowStore.Load(ctx, terminatedBudgetID)
	if err != nil {
		t.Fatalf("workflowStore.Load terminated: %v", err)
	}
	if !ok {
		t.Fatal("expected terminated budget entity to exist")
	}
	if err := pc.workflowStore.MarkTerminated(ctx, terminated.StorageRef, time.Now().UTC()); err != nil {
		t.Fatalf("MarkTerminated: %v", err)
	}

	result, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectEntitySpendHandler(), workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("opco.spend_recorded"),
			Payload: mustJSON(map[string]any{"vertical_id": "vertical-1", "amount_usd": 42}),
		},
		State: WorkflowState{},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected selected handler to run")
	}

	active, ok, err := pc.workflowStore.Load(ctx, activeBudgetID)
	if err != nil {
		t.Fatalf("workflowStore.Load active: %v", err)
	}
	if !ok {
		t.Fatal("expected active budget entity to exist")
	}
	if got := active.Metadata["spent_usd"]; got != float64(42) && got != 42 {
		t.Fatalf("active spent_usd = %#v, want 42", got)
	}
	terminal, ok, err := pc.workflowStore.Load(ctx, terminalBudgetID)
	if err != nil {
		t.Fatalf("workflowStore.Load terminal: %v", err)
	}
	if !ok {
		t.Fatal("expected terminal budget entity to exist")
	}
	if got := terminal.Metadata["spent_usd"]; got != float64(10) && got != 10 {
		t.Fatalf("terminal spent_usd = %#v, want unchanged 10", got)
	}
	reloadedTerminated, ok, err := pc.workflowStore.Load(ctx, terminatedBudgetID)
	if err != nil {
		t.Fatalf("workflowStore.Load terminated after select: %v", err)
	}
	if !ok {
		t.Fatal("expected terminated budget entity to exist")
	}
	if got := reloadedTerminated.Metadata["spent_usd"]; got != float64(20) && got != 20 {
		t.Fatalf("terminated spent_usd = %#v, want unchanged 20", got)
	}
	assertEntityStateRowCount(t, db, 3)
}

func TestExecuteNodeContractHandlerSelectEntityFailsClosedOnNoMatch(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	seedSelectEntityBudget(t, pc.workflowStore, ctx, source, "vertical-1", 0)

	_, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectEntitySpendHandler(), workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("opco.spend_recorded"),
			Payload: mustJSON(map[string]any{"vertical_id": "missing", "amount_usd": 42}),
		},
	}, false)
	if err == nil || !strings.Contains(err.Error(), "select_entity_no_match") {
		t.Fatalf("executeNodeContractHandler error = %v, want select_entity_no_match", err)
	}
}

func TestExecuteNodeContractHandlerSelectEntityFailsClosedOnMissingPayloadRef(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	seedSelectEntityBudget(t, pc.workflowStore, ctx, source, "vertical-1", 0)

	_, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectEntitySpendHandler(), workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("opco.spend_recorded"),
			Payload: mustJSON(map[string]any{"amount_usd": 42}),
		},
	}, false)
	if err == nil || !strings.Contains(err.Error(), "missing required payload ref") {
		t.Fatalf("executeNodeContractHandler error = %v, want missing payload ref", err)
	}
}

func TestExecuteNodeContractHandlerSelectEntityFailsClosedOnAmbiguousMatch(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	seedSelectEntityBudgetWithInstance(t, pc.workflowStore, ctx, source, "budget-1", "vertical-1", 0)
	seedSelectEntityBudgetWithInstance(t, pc.workflowStore, ctx, source, "budget-2", "vertical-1", 0)

	_, err := pc.executeNodeContractHandler(ctx, "treasury-orchestrator", selectEntitySpendHandler(), workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("opco.spend_recorded"),
			Payload: mustJSON(map[string]any{"vertical_id": "vertical-1", "amount_usd": 42}),
		},
	}, false)
	if err == nil || !strings.Contains(err.Error(), "select_entity_ambiguous") {
		t.Fatalf("executeNodeContractHandler error = %v, want select_entity_ambiguous", err)
	}
}

func newSelectEntityTestCoordinator(t *testing.T, db *sql.DB) (*PipelineCoordinator, semanticview.Source) {
	t.Helper()
	source := loadWorkflowTempSource(t, map[string]string{
		"package.yaml": `
name: runtime-test
version: "1.0.0"
platform_version: ">=1.0.0"
flows:
  - id: treasury
    flow: treasury
    mode: static
`,
		"schema.yaml": "name: runtime-test\n",
		"flows/treasury/schema.yaml": `
name: treasury
mode: static
initial_state: active
states: [active, archived]
terminal_states: [archived]
pins:
  inputs:
    events: [opco.spend_recorded]
`,
		"flows/treasury/events.yaml": `
opco.spend_recorded:
  vertical_id: string
  amount_usd: number
`,
		"flows/treasury/entities.yaml": `
opco_budget:
  vertical_id:
    type: text
  spent_usd:
    type: number
    initial: 0
`,
		"flows/treasury/nodes.yaml": `
treasury-orchestrator:
  id: treasury-orchestrator
  execution_type: system_node
  subscribes_to: [opco.spend_recorded]
  event_handlers:
    opco.spend_recorded:
      select_entity:
        by:
          vertical_id: payload.vertical_id
      data_accumulation:
        writes:
          - source_field: amount_usd
            target_field: spent_usd
`,
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected temp workflow bundle")
	}
	pc := &PipelineCoordinator{
		bus:            &recordingPipelineBus{},
		db:             db,
		workflowStore:  NewWorkflowInstanceStore(db),
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
		module: &previewWorkflowModule{
			bundle: bundle,
			workflow: NewWorkflowDefinition("treasury", []WorkflowStage{
				{Name: "active"},
			}, nil),
		},
	}
	return pc, source
}

func newSelectEntityBackgroundNode(t *testing.T, pc *PipelineCoordinator, source semanticview.Source, db *sql.DB) *backgroundWorkflowNode {
	t.Helper()
	contract, ok := source.NodeEntries()["treasury-orchestrator"]
	if !ok {
		t.Fatal("expected treasury-orchestrator node contract")
	}
	executor := NewNode(contract, pc.SemanticSource(), newCoordinatorHandlerExecutionEngine(pc, "treasury-orchestrator"), nil)
	if executor == nil {
		t.Fatal("expected treasury-orchestrator node executor")
	}
	runner := newBackgroundWorkflowNodeWithRetryBase(executor, &recordingPipelineBus{}, db, pc.eventReceiptsCapability, 0)
	if runner == nil {
		t.Fatal("expected treasury-orchestrator background runner")
	}
	return runner
}

func selectEntitySpendHandler() runtimecontracts.SystemNodeEventHandler {
	return runtimecontracts.SystemNodeEventHandler{
		SelectEntity: &runtimecontracts.SelectEntitySpec{
			By: map[string]string{"vertical_id": "payload.vertical_id"},
			Bindings: []runtimecontracts.SelectEntityKeyBinding{{
				Field:   "vertical_id",
				Ref:     "payload.vertical_id",
				RefPath: runtimecontracts.RefExpression("payload.vertical_id").RefPath,
			}},
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{
				SourceField: "amount_usd",
				TargetField: "spent_usd",
			}},
		},
	}
}

func seedSelectEntitySpendEvent(t *testing.T, db *sql.DB, ctx context.Context, payload map[string]any) events.Event {
	t.Helper()
	entityID := uuid.NewString()
	evt := (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("opco.spend_recorded"),
		SourceAgent: "opco",
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(entityID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at)
		VALUES ($1::uuid, $2, $3::uuid, 'opco/vertical-1', 'entity', $4::jsonb, 'opco', 'node', now())
	`, evt.ID, string(evt.Type), entityID, string(evt.Payload)); err != nil {
		t.Fatalf("seed select_entity spend event: %v", err)
	}
	return evt
}

func seedSelectEntityBudget(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, source semanticview.Source, verticalID string, spent any) string {
	t.Helper()
	return seedSelectEntityBudgetWithInstance(t, store, ctx, source, "budget-1", verticalID, spent)
}

func seedSelectEntityBudgetWithInstance(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, source semanticview.Source, instanceID, verticalID string, spent any) string {
	t.Helper()
	return seedSelectEntityBudgetWithState(t, store, ctx, source, instanceID, verticalID, spent, "active")
}

func seedSelectEntityBudgetWithState(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, source semanticview.Source, instanceID, verticalID string, spent any, currentState string) string {
	t.Helper()
	identity := DeriveFlowInstanceIdentity(source, "treasury", instanceID)
	instance := WorkflowInstance{
		InstanceID:      identity.EntityID,
		StorageRef:      identity.InstancePath,
		WorkflowName:    "treasury",
		WorkflowVersion: "1.0.0",
		CurrentState:    strings.TrimSpace(currentState),
		Metadata: map[string]any{
			"flow_path":   identity.InstancePath,
			"instance_id": identity.InstanceID,
			"vertical_id": verticalID,
			"spent_usd":   spent,
			"storage_ref": identity.InstancePath,
			"entity_type": "opco_budget",
		},
	}
	if err := store.Upsert(ctx, instance); err != nil {
		t.Fatalf("seed budget entity: %v", err)
	}
	return identity.EntityID
}

func assertSelectEntityReceiptRow(t *testing.T, db *sql.DB, eventID, nodeID string) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
		  AND idempotency_key = $2 || ':' || $1
	`, eventID, nodeID).Scan(&count); err != nil {
		t.Fatalf("count select_entity event_receipts: %v", err)
	}
	if count != 1 {
		t.Fatalf("select_entity event_receipts rows = %d, want 1", count)
	}
}

func assertEntityStateRowCount(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM entity_state`).Scan(&got); err != nil {
		t.Fatalf("count entity_state: %v", err)
	}
	if got != want {
		t.Fatalf("entity_state row count = %d, want %d", got, want)
	}
}
