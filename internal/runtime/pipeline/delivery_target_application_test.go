package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/google/uuid"
)

func TestDeliveryTargetApplicationPreservesCommittedSelectOrCreateTargetOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, store := openHandlerEntityRequirementStore(t, backend)
			source := deliveryTargetOwnershipSource()
			pc := newDurablePipelineCoordinatorForTest(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
				Module:              staticSemanticWorkflowModule{source: source},
				Persistence:         workflowPersistenceForTest(store),
				PipelineObligations: unavailablePipelineTestObligationOwner{},
			})
			var ctx context.Context
			if backend == "sqlite" {
				ctx = sqliteExactOnceRunContext(t, db)
			} else {
				ctx = testPipelineRunContext(t, db)
			}

			node := pipelineNode(t, "review", "key-upserter")
			handlerFact, err := AdmitDeliveryTargetHandler(source, node)
			if err != nil {
				t.Fatal(err)
			}
			handlerFact = handlerFact.ForEvent("work.keyed")
			handler, ok := handlerFact.resolve(source, "work.keyed")
			if !ok {
				t.Fatal("resolve select-or-create handler")
			}
			evt := handlerTestRootIngress(
				uuid.NewString(), "work.keyed", "", "", mustJSON(map[string]any{"account_id": "account-1"}), 0,
				testPipelineRunID, "", events.EventEnvelope{}, time.Now().UTC(),
			)
			expected := map[string]any{"account_id": "account-1"}
			instanceID, err := selectOrCreateEntityInstanceID(source, "review", expected)
			if err != nil {
				t.Fatal(err)
			}
			identity := deriveFlowInstanceIdentity(source, "review", instanceID)
			target := events.RouteIdentity{FlowID: "review", FlowInstance: identity.InstancePath, EntityID: identity.EntityID}
			owner := events.MustMaterializingEntityTarget(target)

			application, err := pc.prepareDeliveryTargetApplication(ctx, node.Key(), handlerFact, handler, evt, owner)
			if err != nil {
				t.Fatalf("prepare zero-match materializing application: %v", err)
			}
			if !application.Owner().MaterializingEntity() || application.EntityID() != identity.EntityID || application.State().Metadata["account_id"] != "account-1" {
				t.Fatalf("zero-match application = owner:%#v entity:%q state:%#v", application.Owner(), application.EntityID(), application.State())
			}

			exact := materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: instanceID, StorageRef: identity.InstancePath, EntityID: identity.EntityID,
				WorkflowName: "review", WorkflowVersion: "1", CurrentState: "active", Fields: expected,
			})
			if err := store.upsert(ctx, exact); err != nil {
				t.Fatalf("seed exact appearing target: %v", err)
			}
			application, err = pc.prepareDeliveryTargetApplication(ctx, node.Key(), handlerFact, handler, evt, owner)
			if err != nil {
				t.Fatalf("prepare exact same-key appearance: %v", err)
			}
			if application.State().EntityID != identity.EntityID {
				t.Fatalf("appearing exact target state = %#v", application.State())
			}

			siblingID := "later-matching-sibling"
			siblingPath := "review/" + siblingID
			siblingEntityID := eventtest.UUID("later-matching-sibling")
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: siblingID, StorageRef: siblingPath, EntityID: siblingEntityID,
				WorkflowName: "review", WorkflowVersion: "1", CurrentState: "active", Fields: expected,
			})); err != nil {
				t.Fatalf("seed later matching sibling: %v", err)
			}
			restarted := newDurablePipelineCoordinatorForTest(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
				Module:              staticSemanticWorkflowModule{source: source},
				Persistence:         workflowPersistenceForTest(store),
				PipelineObligations: unavailablePipelineTestObligationOwner{},
			})
			application, err = restarted.prepareDeliveryTargetApplication(ctx, node.Key(), handlerFact, handler, evt, owner)
			if err != nil {
				t.Fatalf("prepare committed target after restart and later match: %v", err)
			}
			if application.Route().InstancePath != identity.InstancePath || application.EntityID() != identity.EntityID {
				t.Fatalf("committed target rerouted after restart: route=%#v entity=%q", application.Route(), application.EntityID())
			}

			exact.Fields = map[string]any{"account_id": "conflict"}
			if err := store.upsert(ctx, exact); err != nil {
				t.Fatalf("seed exact target conflict: %v", err)
			}
			if _, err := restarted.prepareDeliveryTargetApplication(ctx, node.Key(), handlerFact, handler, evt, owner); err == nil || !strings.Contains(err.Error(), "select_or_create_entity_conflict") {
				t.Fatalf("conflicting exact target error = %v", err)
			}
			sibling, exists, err := store.Load(ctx, testWorkflowInstanceRoute(siblingPath))
			if err != nil || !exists || sibling.EntityID != siblingEntityID || sibling.Fields["account_id"] != "account-1" {
				t.Fatalf("hostile conflict mutated sibling: found=%t err=%v instance=%#v", exists, err, sibling)
			}
		})
	}
}

func TestDeliveryTargetApplicationRejectsMissingExactExistingTargetWithoutMutation(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, store := openHandlerEntityRequirementStore(t, backend)
			source := handlerEntityRequirementExecutionSource()
			pc := newDurablePipelineCoordinatorForTest(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
				Module:              staticSemanticWorkflowModule{source: source},
				Persistence:         workflowPersistenceForTest(store),
				PipelineObligations: unavailablePipelineTestObligationOwner{},
			})
			var ctx context.Context
			if backend == "sqlite" {
				ctx = sqliteExactOnceRunContext(t, db)
			} else {
				ctx = testPipelineRunContext(t, db)
			}
			node := pipelineNode(t, "review", "node-a")
			handlerFact := MustDeliveryTargetHandler(node).ForEvent("work.ready")
			handler := runtimecontracts.SystemNodeEventHandler{Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"}}
			target := events.RouteIdentity{FlowID: "review", FlowInstance: "review/missing", EntityID: eventtest.UUID("missing-existing-target")}
			evt := handlerTestRootIngress(uuid.NewString(), "work.ready", "", "", nil, 0, testPipelineRunID, "", events.EventEnvelope{}, time.Now().UTC())
			if _, err := pc.prepareDeliveryTargetApplication(ctx, node.Key(), handlerFact, handler, evt, events.MustExistingEntityTarget(target)); err == nil || !strings.Contains(err.Error(), "is missing at execution") {
				t.Fatalf("missing exact target error = %v", err)
			}
			instances, err := pc.ListWorkflowInstances(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(instances) != 0 {
				t.Fatalf("missing exact target materialized state: %#v", instances)
			}
		})
	}
}

func TestDeliveryTargetApplicationCarriesScenarioPreStateThroughFirstMutationOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, store := openHandlerEntityRequirementStore(t, backend)
			source := handlerEntityRequirementExecutionSource()
			pc := newDurablePipelineCoordinatorForTest(&recordingPipelineBus{}, db, PipelineCoordinatorOptions{
				Module:              staticSemanticWorkflowModule{source: source},
				Persistence:         workflowPersistenceForTest(store),
				PipelineObligations: unavailablePipelineTestObligationOwner{},
			})
			configureWorkflowLifecycleForTest(t, pc)
			configurePipelineTestDeliveryOwner(t, pc)
			var ctx context.Context
			if backend == "sqlite" {
				ctx = sqliteExactOnceRunContext(t, db)
			} else {
				ctx = testPipelineRunContext(t, db)
			}

			instancePath := "review/scenario-seeded"
			entityID := eventtest.UUID("scenario-seeded-existing-target")
			occurredAt := time.Date(2026, time.January, 4, 12, 0, 0, 0, time.UTC)
			query := `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, bookkeeping, accumulator, revision, entered_state_at, created_at, updated_at) VALUES (?, ?, ?, 'review_item', 'active', '{"approved":true}', '{"marker":"preserved"}', '{}', '{}', 1, ?, ?, ?)`
			if backend == "postgres" {
				query = `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, bookkeeping, accumulator, revision, entered_state_at, created_at, updated_at) VALUES ($1::uuid, $2::uuid, $3, 'review_item', 'active', '{"approved":true}'::jsonb, '{"marker":"preserved"}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, $4, $4, $4)`
			}
			args := []any{testPipelineRunID, entityID, instancePath, occurredAt}
			if backend == "sqlite" {
				args = append(args, occurredAt, occurredAt)
			}
			if _, err := db.ExecContext(ctx, query, args...); err != nil {
				t.Fatalf("seed exact entity-only pre-state: %v", err)
			}

			evt := handlerTestRootIngress(
				uuid.NewString(), "work.ready", "", "", json.RawMessage(`{"item_id":"a"}`), 0,
				testPipelineRunID, "", handlerTestWorkflowEnvelope("review", instancePath, entityID), occurredAt.Add(time.Minute),
			)
			seedExactOnceEvent(t, store, ctx, evt)
			node := pipelineNode(t, "review", "node-a")
			target := events.RouteIdentity{FlowID: "review", FlowInstance: instancePath, EntityID: entityID}
			deliveryCtx := withWorkflowNodeDeliveryRoute(ctx, events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(node),
				Target:    events.MustExistingEntityTarget(target),
			})
			result, err := pc.executeNodeContractHandler(deliveryCtx, node, runtimecontracts.SystemNodeEventHandler{
				Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"},
			}, workflowTriggerContext{Event: evt}, false)
			if err != nil {
				t.Fatalf("execute exact entity-only pre-state: %v", err)
			}
			if !result.Handled {
				t.Fatal("entity-only pre-state handler was not handled")
			}
			instance, exists, err := store.Load(ctx, testWorkflowInstanceRoute(instancePath))
			if err != nil || !exists {
				t.Fatalf("load first-mutation workflow instance: found=%t err=%v", exists, err)
			}
			if instance.EntityID != entityID || instance.Fields["marker"] != "preserved" || !instance.Gates["approved"] || instance.Revision != 2 {
				t.Fatalf("first-mutation state = %#v, want exact preserved pre-state at revision 2", instance)
			}
		})
	}
}

func TestCompileDeliveryTargetCompatibilityPolicyKeepsDependencyAndAcquisitionIndependent(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		SelectEntity: &runtimecontracts.SelectEntitySpec{Bindings: []runtimecontracts.SelectEntityKeyBinding{{
			Field: "account_id", Ref: "payload.account_id", RefPath: paths.Parse("payload.account_id"),
		}}},
		Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"},
	}
	policy, err := CompileDeliveryTargetCompatibilityPolicy(nil, "review", "work.keyed", handler)
	if err != nil {
		t.Fatal(err)
	}
	if policy.Dependency != DeliveryTargetExistingEntityRequired || policy.Acquisition != DeliveryTargetAcquisitionSelect {
		t.Fatalf("policy = %#v, want existing-required plus select", policy)
	}
}
