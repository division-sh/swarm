package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimerunbundle "github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestExecuteNodeContractHandlerSelectEntityUpdatesTargetOwnedEntity(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	budgetEntityID := seedSelectEntityBudget(t, pc.workflowStore, ctx, source, "vertical-1", 0)

	result, err := pc.executeNodeContractHandler(ctx, pipelineNode(t, "treasury", "treasury-orchestrator"), selectEntitySpendHandler(), workflowTriggerContext{
		Event: persistedSelectEntityIngress(t, ctx, db,
			map[string]any{"vertical_id": "vertical-1", "amount_usd": 42},
			events.EnvelopeForEntityID(events.EventEnvelope{}, "22222222-2222-2222-2222-222222222222")),

		State: WorkflowState{},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected selected handler to run")
	}

	instance, ok, err := pc.workflowStore.Load(ctx, DeriveFlowInstanceIdentity(source, "treasury", "budget-1").Route())
	if err != nil {
		t.Fatalf("workflowStore.Load: %v", err)
	}
	if !ok {
		t.Fatal("expected budget entity to exist")
	}
	if got := instance.Fields["spent_usd"]; got != float64(42) && got != 42 {
		t.Fatalf("spent_usd = %#v, want 42", got)
	}
	if got := FlowInstanceEntityID(instance.StorageRef); got != budgetEntityID {
		t.Fatalf("selected entity storage identity = %q, want %q", got, budgetEntityID)
	}
	assertEntityStateRowCount(t, db, 1)
}

func TestMatchHandlerEntitiesForFlowFiltersDescendantsBeforeContractValidation(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, store := openHandlerEntityRequirementStore(t, backend)
			pc, source := newNestedSelectEntityTestCoordinatorWithStore(t, store)
			var ctx context.Context
			if store.isSQLite() {
				ctx = sqliteExactOnceRunContext(t, db)
			} else {
				ctx = testPipelineRunContext(t, db)
			}

			seedSelectEntityBudget(t, store, ctx, source, "vertical-1", 0)
			descendantIdentity := DeriveFlowInstanceIdentity(source, "treasury/detail", "detail-1")
			descendant := materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID:      descendantIdentity.InstanceID,
				StorageRef:      descendantIdentity.InstancePath,
				EntityID:        descendantIdentity.EntityID,
				EntityType:      "detail_record",
				WorkflowName:    "treasury/detail",
				WorkflowVersion: "1.0.0",
				CurrentState:    "active",
				Fields:          map[string]any{"vertical_id": "vertical-1"},
			})
			if err := store.upsert(ctx, descendant); err != nil {
				t.Fatalf("seed descendant entity: %v", err)
			}
			if workflowInstanceOwnedByFlow(source, descendant, "treasury", testPipelineRunID) {
				t.Fatal("descendant fixture unexpectedly belongs to the parent flow")
			}

			matches, err := pc.matchHandlerEntitiesForFlow(ctx, "treasury", testPipelineRunID, map[string]any{"vertical_id": "vertical-1"})
			if err != nil {
				t.Fatalf("match parent entities: %v", err)
			}
			if len(matches) != 1 || matches[0].EntityType != "opco_budget" {
				t.Fatalf("parent matches = %#v, want only opco_budget", matches)
			}

			ownedInvalid := matches[0]
			ownedInvalid.EntityType = "detail_record"
			if err := store.upsert(ctx, ownedInvalid); err != nil {
				t.Fatalf("seed invalid parent contract: %v", err)
			}
			_, err = pc.matchHandlerEntitiesForFlow(ctx, "treasury", testPipelineRunID, map[string]any{"vertical_id": "vertical-1"})
			if err == nil || !strings.Contains(err.Error(), "select_entity_invalid_persisted_contract") {
				t.Fatalf("invalid owned contract error = %v, want select_entity_invalid_persisted_contract", err)
			}
		})
	}
}

func TestExecuteNodeContractHandlerSelectEntityReplayUsesSameTargetEntity(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	seedSelectEntityBudget(t, pc.workflowStore, ctx, source, "vertical-1", 0)

	for _, amount := range []int{42, 99} {
		result, err := pc.executeNodeContractHandler(ctx, pipelineNode(t, "treasury", "treasury-orchestrator"), selectEntitySpendHandler(), workflowTriggerContext{
			Event: persistedSelectEntityIngress(t, ctx, db,
				map[string]any{"vertical_id": "vertical-1", "amount_usd": amount},
				events.EnvelopeForEntityID(events.EventEnvelope{}, "22222222-2222-2222-2222-222222222222")),

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

	instance, ok, err := pc.workflowStore.Load(ctx, DeriveFlowInstanceIdentity(source, "treasury", "budget-1").Route())
	if err != nil {
		t.Fatalf("workflowStore.Load: %v", err)
	}
	if !ok {
		t.Fatal("expected budget entity to exist")
	}
	if got := instance.Fields["spent_usd"]; got != float64(99) && got != 99 {
		t.Fatalf("spent_usd after replay = %#v, want 99", got)
	}
}

func TestExecuteNodeContractHandlerSelectEntityMatchesTypedStatusField(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	budgetEntityID := seedSelectEntityBudgetWithMetadata(t, pc.workflowStore, ctx, source, "budget-1", map[string]any{
		"vertical_id":      "vertical-1",
		"status":           "pending",
		"business_status":  "approved",
		"spent_usd":        0,
		"domain_status_id": "status-field-regression",
	}, map[string]any{"status": "waiting"})

	result, err := pc.executeNodeContractHandler(ctx, pipelineNode(t, "treasury", "treasury-orchestrator"), runtimecontracts.SystemNodeEventHandler{
		SelectEntity: &runtimecontracts.SelectEntitySpec{
			By: map[string]string{
				"vertical_id": "payload.vertical_id",
				"status":      "payload.status",
			},
			Bindings: []runtimecontracts.SelectEntityKeyBinding{
				{
					Field:   "vertical_id",
					Ref:     "payload.vertical_id",
					RefPath: runtimecontracts.RefExpression("payload.vertical_id").RefPath,
				},
				{
					Field:   "status",
					Ref:     "payload.status",
					RefPath: runtimecontracts.RefExpression("payload.status").RefPath,
				},
			},
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{
				SourceField: "amount_usd",
				TargetField: "spent_usd",
			}},
		},
	}, workflowTriggerContext{
		Event: persistedSelectEntityIngress(t, ctx, db,
			map[string]any{"vertical_id": "vertical-1", "status": "pending", "amount_usd": 42},
			events.EnvelopeForEntityID(events.EventEnvelope{}, "22222222-2222-2222-2222-222222222222")),

		State: WorkflowState{},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected selected handler to run")
	}

	instance, ok, err := pc.workflowStore.Load(ctx, DeriveFlowInstanceIdentity(source, "treasury", "budget-1").Route())
	if err != nil {
		t.Fatalf("workflowStore.Load: %v", err)
	}
	if !ok {
		t.Fatal("expected budget entity to exist")
	}
	if got := instance.Fields["spent_usd"]; got != float64(42) && got != 42 {
		t.Fatalf("spent_usd = %#v, want 42", got)
	}
	if got := strings.TrimSpace(asString(instance.Fields["status"])); got != "pending" {
		t.Fatalf("typed status metadata = %q, want pending", got)
	}
	assertEntityStateField(t, db, budgetEntityID, "status", "pending")
	assertFlowInstanceControlConfig(t, db, instance.StorageRef, "status", "waiting")
}

func TestExecuteNodeContractHandlerSelectOrCreateEntityCreatesTargetOwnedEntity(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, _ := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)

	result, err := pc.executeNodeContractHandler(ctx, pipelineNode(t, "treasury", "treasury-orchestrator"), selectOrCreateEntitySpendHandler(), workflowTriggerContext{
		Event: persistedSelectEntityIngress(t, ctx, db, map[string]any{"vertical_id": "vertical-1", "amount_usd": 42}, events.EventEnvelope{}),

		State: WorkflowState{},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected selected-or-created handler to run")
	}

	assertEntityStateRowCount(t, db, 1)
	instance := loadSelectOrCreateBudgetByKey(t, pc.workflowStore, ctx, pc.SemanticSource(), "vertical-1")
	if got := instance.Fields["vertical_id"]; got != "vertical-1" {
		t.Fatalf("vertical_id = %#v, want vertical-1", got)
	}
	if got := instance.Fields["spent_usd"]; got != float64(42) && got != 42 {
		t.Fatalf("spent_usd = %#v, want 42", got)
	}
	if got := strings.TrimSpace(instance.EntityType); got != "opco_budget" {
		t.Fatalf("entity_type metadata = %q, want opco_budget", got)
	}
	assertEntityStateEntityType(t, db, FlowInstanceEntityID(instance.StorageRef), "opco_budget")
}

func TestExecuteNodeContractHandlerSelectOrCreateEntityReplayUsesSameDeclaredKey(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, _ := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)

	for _, amount := range []int{42, 99} {
		result, err := pc.executeNodeContractHandler(ctx, pipelineNode(t, "treasury", "treasury-orchestrator"), selectOrCreateEntitySpendHandler(), workflowTriggerContext{
			Event: persistedSelectEntityIngress(t, ctx, db, map[string]any{"vertical_id": "vertical-1", "amount_usd": amount}, events.EventEnvelope{}),

			State: WorkflowState{},
		}, false)
		if err != nil {
			t.Fatalf("executeNodeContractHandler amount %d: %v", amount, err)
		}
		if !result.Handled {
			t.Fatalf("expected selected-or-created handler to run for amount %d", amount)
		}
		assertEntityStateRowCount(t, db, 1)
	}

	instance := loadSelectOrCreateBudgetByKey(t, pc.workflowStore, ctx, pc.SemanticSource(), "vertical-1")
	if got := instance.Fields["spent_usd"]; got != float64(99) && got != 99 {
		t.Fatalf("spent_usd after replay = %#v, want 99", got)
	}
}

func TestExecuteNodeContractHandlerSelectOrCreateEntityFailsClosedOnAmbiguousMatch(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	seedSelectEntityBudgetWithInstance(t, pc.workflowStore, ctx, source, "budget-1", "vertical-1", 0)
	seedSelectEntityBudgetWithInstance(t, pc.workflowStore, ctx, source, "budget-2", "vertical-1", 0)

	_, err := pc.executeNodeContractHandler(ctx, pipelineNode(t, "treasury", "treasury-orchestrator"), selectOrCreateEntitySpendHandler(), workflowTriggerContext{
		Event: persistedSelectEntityIngress(t, ctx, db, map[string]any{"vertical_id": "vertical-1", "amount_usd": 42}, events.EventEnvelope{}),
	}, false)
	if err == nil || !strings.Contains(err.Error(), "select_or_create_entity_ambiguous") {
		t.Fatalf("executeNodeContractHandler error = %v, want select_or_create_entity_ambiguous", err)
	}
}

func TestExecuteNodeContractHandlerSelectOrCreateEntityFailsClosedOnDeterministicIDConflict(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	instanceID, err := selectOrCreateEntityInstanceID(source, "treasury", map[string]any{"vertical_id": "vertical-1"})
	if err != nil {
		t.Fatalf("selectOrCreateEntityInstanceID: %v", err)
	}
	identity := DeriveFlowInstanceIdentity(source, "treasury", instanceID)
	if err := pc.workflowStore.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      identity.InstanceID,
		StorageRef:      identity.InstancePath,
		EntityID:        identity.EntityID,
		EntityType:      "opco_budget",
		WorkflowName:    "treasury",
		WorkflowVersion: "1.0.0",
		CurrentState:    "active",
		Fields:          map[string]any{"vertical_id": "other-key"},
	})); err != nil {
		t.Fatalf("seed conflicting entity: %v", err)
	}

	_, err = pc.executeNodeContractHandler(ctx, pipelineNode(t, "treasury", "treasury-orchestrator"), selectOrCreateEntitySpendHandler(), workflowTriggerContext{
		Event: persistedSelectEntityIngress(t, ctx, db, map[string]any{"vertical_id": "vertical-1", "amount_usd": 42}, events.EventEnvelope{}),
	}, false)
	if err == nil || !strings.Contains(err.Error(), "select_or_create_entity_conflict") {
		t.Fatalf("executeNodeContractHandler error = %v, want select_or_create_entity_conflict", err)
	}
}

func TestExecuteNodeContractHandlerSelectOrCreateEntityConcurrentDuplicateCreatesOneEntity(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, _ := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	triggers := []events.Event{
		persistedSelectEntityIngress(t, ctx, db, map[string]any{"vertical_id": "vertical-1", "amount_usd": 42}, events.EventEnvelope{}),
		persistedSelectEntityIngress(t, ctx, db, map[string]any{"vertical_id": "vertical-1", "amount_usd": 42}, events.EventEnvelope{}),
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(trigger events.Event) {
			defer wg.Done()
			result, err := pc.executeNodeContractHandler(ctx, pipelineNode(t, "treasury", "treasury-orchestrator"), selectOrCreateEntitySpendHandler(), workflowTriggerContext{
				Event: trigger,

				State: WorkflowState{},
			}, false)
			if err != nil {
				errs <- err
				return
			}
			if !result.Handled {
				errs <- fmt.Errorf("handler was not handled")
			}
		}(triggers[i])
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent select_or_create handler: %v", err)
		}
	}
	assertEntityStateRowCount(t, db, 1)
}

func TestExecuteNodeContractHandlerSelectOrCreateEntityFeedsEntityIDToArtifactRepoCommit(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, _ := newSelectEntityTestCoordinator(t, db)
	pc.artifactRoot = t.TempDir()
	ctx := testPipelineCoordinatorRunContext(t, pc)
	sourceEventID := "33333333-3333-3333-3333-333333333333"
	payload := map[string]any{"artifact_key": "case-1", "request_id": "44444444-4444-4444-4444-444444444444", "namespace": "tenant-alpha", "partition_key": "project-42", "display_slug": "Demo Artifact", "mvp_yaml": "name: Demo\n"}
	sourceEvent := handlerTestRootIngress(sourceEventID,
		events.EventType("spec_repo.commit_requested"), "test", "", mustJSON(payload), 0, testPipelineRunID, "",
		events.EnvelopeForSourceRoute(
			events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, "22222222-2222-2222-2222-222222222222"), "treasury/case-1"),
			events.RouteIdentity{FlowID: "treasury", FlowInstance: "treasury/case-1", EntityID: "22222222-2222-2222-2222-222222222222"},
		),
		time.Unix(1_700_000_000, 0).UTC())
	seedPipelineEventRecord(t, ctx, db, sourceEvent)

	result, err := pc.executeNodeContractHandler(ctx, pipelineNode(t, "treasury", "treasury-orchestrator"), selectOrCreateArtifactRepoCommitHandler(), workflowTriggerContext{
		Event: sourceEvent,

		State: WorkflowState{},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected artifact_repo_commit handler to run")
	}

	instance := loadSelectOrCreateBudgetByKey(t, pc.workflowStore, ctx, pc.SemanticSource(), "case-1")
	entityID := FlowInstanceEntityID(instance.StorageRef)
	if got := strings.TrimSpace(asString(instance.Fields["repo_url"])); got != "swarm-artifact://repos/"+entityID {
		t.Fatalf("repo_url = %q, want repo url derived from entity id %q", got, entityID)
	}
	if ref := strings.TrimSpace(asString(instance.Fields["current_ref"])); len(ref) != 40 {
		t.Fatalf("current_ref length = %d ref=%q", len(ref), ref)
	}
}

func TestExecuteNodeContractHandlerSelectEntityIgnoresTerminalAndTerminatedMatches(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pc, source := newSelectEntityTestCoordinator(t, db)
	ctx := testPipelineCoordinatorRunContext(t, pc)
	seedSelectEntityBudgetWithState(t, pc.workflowStore, ctx, source, "budget-active", "vertical-1", 0, "active")
	seedSelectEntityBudgetWithState(t, pc.workflowStore, ctx, source, "budget-archived", "vertical-1", 10, "archived")
	seedSelectEntityBudgetWithState(t, pc.workflowStore, ctx, source, "budget-terminated", "vertical-1", 20, "active")
	_, ok, err := pc.workflowStore.Load(ctx, DeriveFlowInstanceIdentity(source, "treasury", "budget-terminated").Route())
	if err != nil {
		t.Fatalf("workflowStore.Load terminated: %v", err)
	}
	if !ok {
		t.Fatal("expected terminated budget entity to exist")
	}
	terminatedIdentity := DeriveFlowInstanceIdentity(source, "treasury", "budget-terminated")
	if err := pc.workflowStore.MarkTerminated(ctx, terminatedIdentity.Route(), identity.NormalizeEntityID(terminatedIdentity.EntityID), time.Now().UTC()); err != nil {
		t.Fatalf("MarkTerminated: %v", err)
	}

	result, err := pc.executeNodeContractHandler(ctx, pipelineNode(t, "treasury", "treasury-orchestrator"), selectEntitySpendHandler(), workflowTriggerContext{
		Event: persistedSelectEntityIngress(t, ctx, db, map[string]any{"vertical_id": "vertical-1", "amount_usd": 42}, events.EventEnvelope{}),

		State: WorkflowState{},
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected selected handler to run")
	}

	active, ok, err := pc.workflowStore.Load(ctx, DeriveFlowInstanceIdentity(source, "treasury", "budget-active").Route())
	if err != nil {
		t.Fatalf("workflowStore.Load active: %v", err)
	}
	if !ok {
		t.Fatal("expected active budget entity to exist")
	}
	if got := active.Fields["spent_usd"]; got != float64(42) && got != 42 {
		t.Fatalf("active spent_usd = %#v, want 42", got)
	}
	terminal, ok, err := pc.workflowStore.Load(ctx, DeriveFlowInstanceIdentity(source, "treasury", "budget-archived").Route())
	if err != nil {
		t.Fatalf("workflowStore.Load terminal: %v", err)
	}
	if !ok {
		t.Fatal("expected terminal budget entity to exist")
	}
	if got := terminal.Fields["spent_usd"]; got != float64(10) && got != 10 {
		t.Fatalf("terminal spent_usd = %#v, want unchanged 10", got)
	}
	reloadedTerminated, ok, err := pc.workflowStore.Load(ctx, DeriveFlowInstanceIdentity(source, "treasury", "budget-terminated").Route())
	if err != nil {
		t.Fatalf("workflowStore.Load terminated after select: %v", err)
	}
	if !ok {
		t.Fatal("expected terminated budget entity to exist")
	}
	if got := reloadedTerminated.Fields["spent_usd"]; got != float64(20) && got != 20 {
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

	_, err := pc.executeNodeContractHandler(ctx, pipelineNode(t, "treasury", "treasury-orchestrator"), selectEntitySpendHandler(), workflowTriggerContext{
		Event: persistedSelectEntityIngress(t, ctx, db, map[string]any{"vertical_id": "missing", "amount_usd": 42}, events.EventEnvelope{}),
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

	_, err := pc.executeNodeContractHandler(ctx, pipelineNode(t, "treasury", "treasury-orchestrator"), selectEntitySpendHandler(), workflowTriggerContext{
		Event: persistedSelectEntityIngress(t, ctx, db, map[string]any{"amount_usd": 42}, events.EventEnvelope{}),
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

	_, err := pc.executeNodeContractHandler(ctx, pipelineNode(t, "treasury", "treasury-orchestrator"), selectEntitySpendHandler(), workflowTriggerContext{
		Event: persistedSelectEntityIngress(t, ctx, db, map[string]any{"vertical_id": "vertical-1", "amount_usd": 42}, events.EventEnvelope{}),
	}, false)
	if err == nil || !strings.Contains(err.Error(), "select_entity_ambiguous") {
		t.Fatalf("executeNodeContractHandler error = %v, want select_entity_ambiguous", err)
	}
}

const selectEntityTestNodes = `
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
`

func newSelectEntityTestCoordinator(t *testing.T, db *sql.DB) (*PipelineCoordinator, semanticview.Source) {
	t.Helper()
	return newSelectEntityTestCoordinatorWithNodes(t, db, selectEntityTestNodes)
}

func newSelectOrCreateEntityTestCoordinator(t *testing.T, db *sql.DB) (*PipelineCoordinator, semanticview.Source) {
	t.Helper()
	return newSelectEntityTestCoordinatorWithNodes(t, db, `
treasury-orchestrator:
  id: treasury-orchestrator
  execution_type: system_node
  subscribes_to: [opco.spend_recorded]
  event_handlers:
    opco.spend_recorded:
      select_or_create_entity:
        by:
          vertical_id: payload.vertical_id
      data_accumulation:
        writes:
          - source_field: amount_usd
            target_field: spent_usd
`)
}

func persistedSelectEntityIngress(t *testing.T, ctx context.Context, db *sql.DB, payload map[string]any, envelope events.EventEnvelope) events.Event {
	t.Helper()
	const sourceEntityID = "22222222-2222-2222-2222-222222222222"
	entityID := strings.TrimSpace(envelope.EntityID)
	if entityID == "" {
		entityID = sourceEntityID
	}
	envelope = events.EnvelopeForFlowInstance(envelope, "treasury/orchestrator")
	envelope = events.EnvelopeForEntityID(envelope, entityID)
	envelope = events.EnvelopeForSourceRoute(envelope, events.RouteIdentity{
		FlowID: "treasury", FlowInstance: "treasury/orchestrator", EntityID: entityID,
	})
	event := handlerTestRootIngress(
		"",
		events.EventType("opco.spend_recorded"),
		"",
		"",
		mustJSON(payload),
		0,
		testPipelineRunID,
		"",
		envelope,
		time.Time{},
	)
	seedPipelineEventRecord(t, ctx, db, event)
	return event
}

func newSelectEntityTestCoordinatorWithNodes(t *testing.T, db *sql.DB, treasuryNodes string) (*PipelineCoordinator, semanticview.Source) {
	t.Helper()
	return newSelectEntityTestCoordinatorWithStoreAndNodes(t, newPostgresWorkflowInstanceStoreForTest(db), treasuryNodes)
}

func newSelectEntityTestCoordinatorWithStoreAndNodes(t *testing.T, store *workflowInstanceStore, treasuryNodes string) (*PipelineCoordinator, semanticview.Source) {
	t.Helper()
	return newSelectEntityTestCoordinatorFixture(t, store, treasuryNodes, false)
}

func newNestedSelectEntityTestCoordinatorWithStore(t *testing.T, store *workflowInstanceStore) (*PipelineCoordinator, semanticview.Source) {
	t.Helper()
	return newSelectEntityTestCoordinatorFixture(t, store, selectEntityTestNodes, true)
}

func newSelectEntityTestCoordinatorFixture(t *testing.T, store *workflowInstanceStore, treasuryNodes string, nested bool) (*PipelineCoordinator, semanticview.Source) {
	t.Helper()
	files := map[string]string{
		"package.yaml": `
name: runtime-test
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: treasury
    flow: treasury
    mode: template
`,
		"schema.yaml": "name: runtime-test\n",
		"flows/treasury/schema.yaml": `
name: treasury
mode: template
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
  repo_url:
    type: text
  current_ref:
    type: text
  file_manifest:
    type: text
  status:
    type: text
  failure:
    type: text
  last_request_id:
    type: text
  last_source_event_id:
    type: text
`,
		"flows/treasury/nodes.yaml": treasuryNodes,
	}
	if nested {
		files["flows/treasury/package.yaml"] = `
name: treasury
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: detail
    flow: detail
    mode: template
`
		files["flows/treasury/flows/detail/package.yaml"] = `
name: detail
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`
		files["flows/treasury/flows/detail/schema.yaml"] = `
name: detail
mode: template
initial_state: active
states: [active, archived]
terminal_states: [archived]
`
		files["flows/treasury/flows/detail/entities.yaml"] = `
detail_record:
  vertical_id:
    type: text
`
	}
	source := loadWorkflowTempSource(t, files)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected temp workflow bundle")
	}
	pc := &PipelineCoordinator{
		bus:            &recordingPipelineBus{},
		workflowStore:  store,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
		module: &previewWorkflowModule{
			bundle: bundle,
			workflow: NewWorkflowDefinition("treasury", []WorkflowStage{
				{Name: "active"},
			}, nil),
		},
		runBundleAvailability: selectEntityTestRunBundleAvailability{},
	}
	if !store.isSQLite() {
		pc.workflowStore.deliveryStore = newPipelineTestDeliveryOwnerForDB(t, store.testDB())
	}
	return pc, source
}

type selectEntityTestRunBundleAvailability struct{}

func (a selectEntityTestRunBundleAvailability) LoadRunBundleAvailability(ctx context.Context, runID string) (runtimerunbundle.Availability, error) {
	return runtimerunbundle.Availability{
		RunID: runID, Status: "running",
		BundleHash:       "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111",
		BundleSource:     runtimerunbundle.AvailabilitySourcePersisted,
		BundleRowPresent: true,
	}, nil
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

func selectOrCreateEntitySpendHandler() runtimecontracts.SystemNodeEventHandler {
	return runtimecontracts.SystemNodeEventHandler{
		SelectOrCreateEntity: &runtimecontracts.SelectOrCreateEntitySpec{
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

func selectOrCreateArtifactRepoCommitHandler() runtimecontracts.SystemNodeEventHandler {
	return runtimecontracts.SystemNodeEventHandler{
		SelectOrCreateEntity: &runtimecontracts.SelectOrCreateEntitySpec{
			By: map[string]string{"vertical_id": "payload.artifact_key"},
			Bindings: []runtimecontracts.SelectEntityKeyBinding{{
				Field:   "vertical_id",
				Ref:     "payload.artifact_key",
				RefPath: runtimecontracts.RefExpression("payload.artifact_key").RefPath,
			}},
		},
		Action: runtimecontracts.ActionSpec{
			ID: "artifact_repo_commit",
			ArtifactRepo: &runtimecontracts.ArtifactRepoSpec{
				Provider:     "local_git",
				RepoID:       runtimecontracts.RefExpression("_entity.id"),
				Namespace:    runtimecontracts.RefExpression("payload.namespace"),
				PartitionKey: runtimecontracts.RefExpression("payload.partition_key"),
				DisplaySlug:  runtimecontracts.RefExpression("payload.display_slug"),
				RequestID:    runtimecontracts.RefExpression("payload.request_id"),
				Author:       runtimecontracts.LiteralExpression("artifact-writer"),
				Provenance: map[string]runtimecontracts.ExpressionValue{
					"artifact_type": runtimecontracts.LiteralExpression("fixture"),
				},
				AllowedPaths: []string{"specs/mvp.yaml"},
				Files: []runtimecontracts.ArtifactRepoFileSpec{{
					Path:        runtimecontracts.LiteralExpression("specs/mvp.yaml"),
					Content:     runtimecontracts.RefExpression("payload.mvp_yaml"),
					ContentType: "yaml",
					Schema: runtimecontracts.ArtifactRepoSchemaSpec{
						Type:           "object",
						RequiredFields: []string{"name"},
					},
					MaxBytes: 4096,
				}},
				Output: runtimecontracts.ArtifactRepoOutputSpec{
					RepoURL:           "repo_url",
					CurrentRef:        "current_ref",
					FileManifest:      "file_manifest",
					Status:            "status",
					Failure:           "failure",
					LastRequestID:     "last_request_id",
					LastSourceEventID: "last_source_event_id",
				},
				Limits: runtimecontracts.ArtifactRepoLimitsSpec{
					MaxYAMLBytes: 4096,
					MaxRepoBytes: 1048576,
				},
				FailureEvent: "artifact_repo.commit_failed",
			},
		},
	}
}

func loadSelectOrCreateBudgetByKey(t *testing.T, store *workflowInstanceStore, ctx context.Context, source semanticview.Source, verticalID string) WorkflowInstance {
	t.Helper()
	instanceID, err := selectOrCreateEntityInstanceID(source, "treasury", map[string]any{"vertical_id": verticalID})
	if err != nil {
		t.Fatalf("selectOrCreateEntityInstanceID: %v", err)
	}
	identity := DeriveFlowInstanceIdentity(source, "treasury", instanceID)
	instance, ok, err := store.Load(ctx, identity.Route())
	if err != nil {
		t.Fatalf("workflowStore.Load: %v", err)
	}
	if !ok {
		t.Fatalf("expected select_or_create entity %s to exist", identity.EntityID)
	}
	return instance
}

func seedSelectEntityBudget(t *testing.T, store *workflowInstanceStore, ctx context.Context, source semanticview.Source, verticalID string, spent any) string {
	t.Helper()
	return seedSelectEntityBudgetWithInstance(t, store, ctx, source, "budget-1", verticalID, spent)
}

func seedSelectEntityBudgetWithInstance(t *testing.T, store *workflowInstanceStore, ctx context.Context, source semanticview.Source, instanceID, verticalID string, spent any) string {
	t.Helper()
	return seedSelectEntityBudgetWithState(t, store, ctx, source, instanceID, verticalID, spent, "active")
}

func seedSelectEntityBudgetWithState(t *testing.T, store *workflowInstanceStore, ctx context.Context, source semanticview.Source, instanceID, verticalID string, spent any, currentState string) string {
	t.Helper()
	return seedSelectEntityBudgetWithMetadataAndState(t, store, ctx, source, instanceID, map[string]any{
		"vertical_id": verticalID,
		"spent_usd":   spent,
	}, nil, currentState)
}

func seedSelectEntityBudgetWithMetadata(t *testing.T, store *workflowInstanceStore, ctx context.Context, source semanticview.Source, instanceID string, metadata map[string]any, config map[string]any) string {
	t.Helper()
	return seedSelectEntityBudgetWithMetadataAndState(t, store, ctx, source, instanceID, metadata, config, "active")
}

func seedSelectEntityBudgetWithMetadataAndState(t *testing.T, store *workflowInstanceStore, ctx context.Context, source semanticview.Source, instanceID string, metadata map[string]any, config map[string]any, currentState string) string {
	t.Helper()
	identity := DeriveFlowInstanceIdentity(source, "treasury", instanceID)
	metadata = cloneStringAnyMap(metadata)
	if metadata == nil {
		metadata = map[string]any{}
	}
	config = cloneStringAnyMap(config)
	instance := materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      identity.InstanceID,
		StorageRef:      identity.InstancePath,
		EntityID:        identity.EntityID,
		EntityType:      "opco_budget",
		WorkflowName:    "treasury",
		WorkflowVersion: "1.0.0",
		CurrentState:    strings.TrimSpace(currentState),
		Config:          config,
		Fields:          metadata,
	})
	if err := store.upsert(ctx, instance); err != nil {
		t.Fatalf("seed budget entity: %v", err)
	}
	return identity.EntityID
}

func assertEntityStateField(t *testing.T, db *sql.DB, entityID, field string, want any) {
	t.Helper()
	var gotRaw []byte
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `
		SELECT fields -> $3
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, testPipelineRunID, entityID, field).Scan(&gotRaw); err != nil {
		t.Fatalf("load entity_state fields for %s: %v", entityID, err)
	}
	wantRaw, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal wanted entity_state field %s: %v", field, err)
	}
	if string(gotRaw) != string(wantRaw) {
		t.Fatalf("entity_state.fields[%q] = %s, want %s", field, gotRaw, wantRaw)
	}
}

func assertFlowInstanceControlConfig(t *testing.T, db *sql.DB, storageRef, field string, want any) {
	t.Helper()
	var gotRaw []byte
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `
		SELECT config -> $2
		FROM flow_instances
		WHERE instance_id = $1
	`, storageRef, field).Scan(&gotRaw); err != nil {
		t.Fatalf("load flow_instances config for %s: %v", storageRef, err)
	}
	wantRaw, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal wanted flow instance config %s: %v", field, err)
	}
	if string(gotRaw) != string(wantRaw) {
		t.Fatalf("flow_instances.config[%q] = %s, want %s", field, gotRaw, wantRaw)
	}
}

func assertEntityStateRowCount(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `SELECT COUNT(*) FROM entity_state`).Scan(&got); err != nil {
		t.Fatalf("count entity_state: %v", err)
	}
	if got != want {
		t.Fatalf("entity_state row count = %d, want %d", got, want)
	}
}

func assertEntityStateEntityType(t *testing.T, db *sql.DB, entityID, want string) {
	t.Helper()
	assertEntityStateEntityTypeForRun(t, db, testPipelineRunID, entityID, want)
}

func assertEntityStateEntityTypeForRun(t *testing.T, db *sql.DB, runID, entityID, want string) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `
		SELECT COALESCE(entity_type, '')
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, runID, entityID).Scan(&got); err != nil {
		t.Fatalf("load entity_state entity_type for %s: %v", entityID, err)
	}
	if got != want {
		t.Fatalf("entity_state.entity_type = %q, want %q", got, want)
	}
}
