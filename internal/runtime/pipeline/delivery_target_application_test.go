package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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

func TestDeliveryTargetApplicationRejectsStateOnlyChildRelabeledAsParentOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db, store := openHandlerEntityRequirementStore(t, backend)
			source := deliveryTargetNestedOwnershipSource()
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

			instancePath := "review/child/instance"
			entityID := eventtest.UUID("state-only-child-relabeled-as-parent-" + backend)
			now := time.Now().UTC().Truncate(time.Microsecond)
			query := `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, bookkeeping, accumulator, revision, entered_state_at, created_at, updated_at) VALUES (?, ?, ?, 'review_item', 'active', '{}', '{"marker":"unchanged"}', '{}', '{}', 1, ?, ?, ?)`
			args := []any{testPipelineRunID, entityID, instancePath, now, now, now}
			if backend == "postgres" {
				query = `INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, bookkeeping, accumulator, revision, entered_state_at, created_at, updated_at) VALUES ($1::uuid, $2::uuid, $3, 'review_item', 'active', '{}'::jsonb, '{"marker":"unchanged"}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, $4, $4, $4)`
				args = []any{testPipelineRunID, entityID, instancePath, now}
			}
			if _, err := db.ExecContext(ctx, query, args...); err != nil {
				t.Fatalf("seed state-only child target: %v", err)
			}

			node := pipelineNode(t, "review", "existing")
			handlerFact, err := AdmitDeliveryTargetHandler(source, node)
			if err != nil {
				t.Fatal(err)
			}
			handlerFact = handlerFact.ForEvent("work.ready")
			handler, ok := handlerFact.resolve(source, "work.ready")
			if !ok {
				t.Fatal("resolve parent delivery handler")
			}
			hostile := events.RouteIdentity{FlowID: "review", FlowInstance: instancePath, EntityID: entityID}
			evt := handlerTestRootIngress(
				uuid.NewString(), "work.ready", "", "", nil, 0, testPipelineRunID, "",
				handlerTestWorkflowEnvelope("review", instancePath, entityID), now,
			)
			if _, err := pc.prepareDeliveryTargetApplication(ctx, node.Key(), handlerFact, handler, evt, events.MustExistingEntityTarget(hostile)); err == nil || !strings.Contains(err.Error(), "not owned by flow review") {
				t.Fatalf("state-only child relabeling error = %v", err)
			}

			route := runtimeflowidentity.StoredRoute("review", runtimeflowidentity.LogicalInstanceID(instancePath), instancePath)
			persisted, found, err := store.LoadEntityState(ctx, route, runtimeidentity.NormalizeEntityID(entityID))
			var fields map[string]any
			fieldsErr := json.Unmarshal(persisted.Fields, &fields)
			if err != nil || !found || persisted.CurrentState != "active" || persisted.Revision != 1 || fieldsErr != nil || fields["marker"] != "unchanged" {
				t.Fatalf("rejected child state changed: found=%t err=%v state=%#v", found, err, persisted)
			}
		})
	}
}

func deliveryTargetNestedOwnershipSource() semanticview.Source {
	source := deliveryTargetOwnershipSource()
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil || bundle.FlowTree.Root == nil {
		panic("delivery target ownership source has no bundle")
	}
	parent := bundle.FlowTree.ByID["review"]
	if parent == nil {
		panic("delivery target ownership source has no review flow")
	}
	child := runtimecontracts.FlowContractView{
		Path:  "review/child",
		Paths: runtimecontracts.FlowContractPaths{ID: "child", Flow: "child", Mode: runtimecontracts.FlowModeTemplate},
		Schema: runtimecontracts.FlowSchemaDocument{
			Name: "child", Mode: runtimecontracts.FlowModeTemplate, InitialState: "active",
			States: []string{"active", "done"}, TerminalStates: []string{"done"},
		},
	}
	parent.Children = append(parent.Children, child)
	childView := &parent.Children[len(parent.Children)-1]
	bundle.FlowTree.ByID["child"] = childView
	if bundle.FlowSchemas == nil {
		bundle.FlowSchemas = make(map[string]runtimecontracts.FlowSchemaDocument)
	}
	bundle.FlowSchemas["child"] = childView.Schema
	return semanticview.Wrap(bundle)
}

func TestDeliveryTargetApplicationRejectsInvalidPersistencePresenceAndLifecycleWithoutMutation(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, testCase := range []struct {
			name      string
			wantError string
			seed      func(*testing.T, context.Context, string, string, *workflowInstanceStore, *sql.DB)
		}{
			{
				name: "lifecycle-only", wantError: "lifecycle companion without state",
				seed: func(t *testing.T, ctx context.Context, instancePath, _ string, _ *workflowInstanceStore, db *sql.DB) {
					t.Helper()
					query := `INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at) VALUES (?, 'review', 'template', '{}', 'active', ?)`
					args := []any{instancePath, time.Now().UTC()}
					if backend == "postgres" {
						query = `INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at) VALUES ($1, 'review', 'template', '{}'::jsonb, 'active', $2)`
					}
					if _, err := db.ExecContext(ctx, query, args...); err != nil {
						t.Fatalf("seed lifecycle-only target: %v", err)
					}
				},
			},
			{
				name: "wrong-descriptor", wantError: "descriptor or status conflicts",
				seed: func(t *testing.T, ctx context.Context, instancePath, entityID string, store *workflowInstanceStore, _ *sql.DB) {
					t.Helper()
					if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
						InstanceID: "wrong-descriptor", StorageRef: instancePath, EntityID: entityID,
						WorkflowName: "other-flow", WorkflowVersion: "1", Mode: "template", CurrentState: "active",
						Fields: map[string]any{"marker": "unchanged"},
					})); err != nil {
						t.Fatalf("seed wrong-descriptor target: %v", err)
					}
				},
			},
			{
				name: "terminated-status", wantError: "descriptor or status conflicts",
				seed: func(t *testing.T, ctx context.Context, instancePath, entityID string, store *workflowInstanceStore, db *sql.DB) {
					t.Helper()
					instance := materializedWorkflowInstanceForTest(WorkflowInstance{
						InstanceID: "terminated-status", StorageRef: instancePath, EntityID: entityID,
						WorkflowName: "review", WorkflowVersion: "1", Mode: "template", CurrentState: "active",
						Fields: map[string]any{"marker": "unchanged"},
					})
					if err := store.upsert(ctx, instance); err != nil {
						t.Fatalf("seed terminated target: %v", err)
					}
					query := `UPDATE flow_instances SET status = 'terminated', terminated_at = ? WHERE instance_id = ?`
					args := []any{time.Now().UTC(), instancePath}
					if backend == "postgres" {
						query = `UPDATE flow_instances SET status = 'terminated', terminated_at = $1 WHERE instance_id = $2`
					}
					if _, err := db.ExecContext(ctx, query, args...); err != nil {
						t.Fatalf("terminate target lifecycle: %v", err)
					}
				},
			},
		} {
			t.Run(backend+"/"+testCase.name, func(t *testing.T) {
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
				instancePath := "review/" + testCase.name
				entityID := eventtest.UUID("invalid-persistence-" + backend + "-" + testCase.name)
				testCase.seed(t, ctx, instancePath, entityID, store, db)

				node := pipelineNode(t, "review", "node-a")
				handlerFact := MustDeliveryTargetHandler(node).ForEvent("work.ready")
				handler := runtimecontracts.SystemNodeEventHandler{Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"}}
				target := events.RouteIdentity{FlowID: "review", FlowInstance: instancePath, EntityID: entityID}
				evt := handlerTestRootIngress(uuid.NewString(), "work.ready", "", "", nil, 0, testPipelineRunID, "", handlerTestWorkflowEnvelope("review", instancePath, entityID), time.Now().UTC())
				if _, err := pc.prepareDeliveryTargetApplication(ctx, node.Key(), handlerFact, handler, evt, events.MustExistingEntityTarget(target)); err == nil || !strings.Contains(err.Error(), testCase.wantError) {
					t.Fatalf("invalid %s persistence error = %v, want %q", testCase.name, err, testCase.wantError)
				}

				if testCase.name == "lifecycle-only" {
					var stateCount int
					query := `SELECT COUNT(*) FROM entity_state WHERE run_id = ? AND entity_id = ? AND flow_instance = ?`
					args := []any{testPipelineRunID, entityID, instancePath}
					if backend == "postgres" {
						query = `SELECT COUNT(*) FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid AND flow_instance = $3`
					}
					if err := db.QueryRowContext(ctx, query, args...).Scan(&stateCount); err != nil || stateCount != 0 {
						t.Fatalf("lifecycle-only rejection state rows = %d, err=%v", stateCount, err)
					}
					return
				}
				persisted, found, err := store.Load(ctx, testWorkflowInstanceRoute(instancePath))
				if err != nil || !found || persisted.Revision != 1 || persisted.Fields["marker"] != "unchanged" {
					t.Fatalf("invalid lifecycle rejection mutated target: found=%t err=%v instance=%#v", found, err, persisted)
				}
			})
		}
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

func TestDeliveryTargetApplicationProjectsScopedGatesWithoutMutatingPersistenceOnSQLiteAndPostgres(t *testing.T) {
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

			instancePath := "review/scoped-gates"
			entityID := eventtest.UUID("scoped-gates-existing-target")
			persisted := materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: "scoped-gates", StorageRef: instancePath, EntityID: entityID,
				WorkflowName: "review", WorkflowVersion: "1", Mode: "template", CurrentState: "active",
				Fields: map[string]any{"marker": "durable"}, Gates: map[string]bool{"review/approved": true},
			})
			if err := store.upsert(ctx, persisted); err != nil {
				t.Fatalf("seed exact scoped-gate target: %v", err)
			}

			node := pipelineNode(t, "review", "node-a")
			handlerFact := MustDeliveryTargetHandler(node).ForEvent("work.ready")
			handler := runtimecontracts.SystemNodeEventHandler{Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"}}
			evt := handlerTestRootIngress(
				uuid.NewString(), "work.ready", "", "", nil, 0, testPipelineRunID, "",
				handlerTestWorkflowEnvelope("review", instancePath, entityID), time.Now().UTC(),
			)
			target := events.RouteIdentity{FlowID: "review", FlowInstance: instancePath, EntityID: entityID}
			application, err := pc.prepareDeliveryTargetApplication(ctx, node.Key(), handlerFact, handler, evt, events.MustExistingEntityTarget(target))
			if err != nil {
				t.Fatalf("prepare scoped-gate application: %v", err)
			}

			snapshot, exists, err := application.persistedSnapshot()
			if err != nil || !exists {
				t.Fatalf("load execution snapshot: exists=%t err=%v", exists, err)
			}
			if !snapshot.StateCarrier.Gates["approved"] || !snapshot.StateCarrier.Gates["review/approved"] {
				t.Fatalf("execution gates = %#v, want local and qualified facts", snapshot.StateCarrier.Gates)
			}
			snapshot.StateCarrier.Gates["approved"] = false
			snapshot.StateCarrier.Fields["marker"] = "mutated"

			fresh, exists, err := application.persistedSnapshot()
			if err != nil || !exists || !fresh.StateCarrier.Gates["approved"] || fresh.StateCarrier.Fields["marker"] != "durable" {
				t.Fatalf("fresh immutable snapshot = %#v exists=%t err=%v", fresh, exists, err)
			}
			raw, presence := application.persistedInstance()
			if !presence.HasState() || raw.Gates["approved"] || !raw.Gates["review/approved"] {
				t.Fatalf("raw persisted application state = %#v", raw.Gates)
			}
			stored, exists, err := store.Load(ctx, testWorkflowInstanceRoute(instancePath))
			if err != nil || !exists || stored.Gates["approved"] || !stored.Gates["review/approved"] || stored.Fields["marker"] != "durable" {
				t.Fatalf("durable state changed by execution projection: exists=%t err=%v instance=%#v", exists, err, stored)
			}
		})
	}
}

func TestDeliveryTargetApplicationProjectsExactOwnerIntoEmptyPreviewAndRejectsConflict(t *testing.T) {
	source := handlerEntityRequirementExecutionSource()
	pc := &PipelineCoordinator{module: staticSemanticWorkflowModule{source: source}}
	node := pipelineNode(t, "review", "node-a")
	handlerFact := MustDeliveryTargetHandler(node).ForEvent("work.ready")
	handler := runtimecontracts.SystemNodeEventHandler{Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"}}
	entityID := eventtest.UUID("preview-exact-target")
	target := events.RouteIdentity{FlowID: "review", FlowInstance: "review/preview", EntityID: entityID}
	evt := handlerTestRootIngress(
		uuid.NewString(), "work.ready", "", "", nil, 0, testPipelineRunID, "",
		handlerTestWorkflowEnvelope("review", target.FlowInstance, entityID), time.Now().UTC(),
	)

	application, err := pc.prepareDeliveryTargetApplication(
		context.Background(), node.Key(), handlerFact, handler, evt, events.MustExistingEntityTarget(target),
		WorkflowState{Stage: "active", Metadata: map[string]any{"marker": "preview"}},
	)
	if err != nil {
		t.Fatalf("prepare empty-identity preview: %v", err)
	}
	if got := application.State(); got.EntityID != entityID || got.Control.FlowPath != target.FlowInstance || got.Metadata["marker"] != "preview" {
		t.Fatalf("projected preview state = %#v", got)
	}

	_, err = pc.prepareDeliveryTargetApplication(
		context.Background(), node.Key(), handlerFact, handler, evt, events.MustExistingEntityTarget(target),
		WorkflowState{EntityID: eventtest.UUID("conflicting-preview-target"), Stage: "active", Metadata: map[string]any{}},
	)
	if err == nil || !strings.Contains(err.Error(), "entity disagrees with admitted owner") {
		t.Fatalf("conflicting preview error = %v", err)
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
