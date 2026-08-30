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
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestExistingOwnerExecutionSemanticsPersistOnSQLiteAndPostgres(t *testing.T) {
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

			for _, engine := range []string{"bridge", "declarative"} {
				t.Run(engine+"/accumulator", func(t *testing.T) {
					nodeKey := pipelineNode(t, ".", "node-a").Key()
					instance, result := executeExistingOwnerBehavior(t, ctx, pc, engine, engine+"-accumulator", runtimecontracts.SystemNodeEventHandler{
						Accumulate: &runtimecontracts.AccumulateSpec{Into: "items", From: "payload"},
					}, json.RawMessage(`{"item_id":"a"}`), nil, nil)
					if !result.handled {
						t.Fatal("accumulator execution was not handled")
					}
					nodeBucket, ok := instance.StateBuckets[nodeKey].(map[string]any)
					if !ok {
						t.Fatalf("accumulator node bucket = %#v, want persisted node-a bucket", instance.StateBuckets)
					}
					if _, ok := nodeBucket["handler_accumulators"]; !ok {
						t.Fatalf("accumulator bucket = %#v, want persisted handler_accumulators", nodeBucket)
					}
				})

				t.Run(engine+"/clear", func(t *testing.T) {
					nodeKey := pipelineNode(t, ".", "node-a").Key()
					initialMetadata := map[string]any{
						"revision_count":    3,
						"dedup_key":         "pending-a",
						"accumulated_count": 1,
					}
					initialBuckets := map[string]any{nodeKey: map[string]any{
						"handler_accumulators": map[string]any{nodeKey + ":work.ready": map[string]any{"items": []any{"a"}}},
					}}
					instance, result := executeExistingOwnerBehavior(t, ctx, pc, engine, engine+"-clear", runtimecontracts.SystemNodeEventHandler{
						Clear: &runtimecontracts.ClearSpec{Targets: []string{"accumulator_state", "pending_dedup", "revision_count"}},
					}, nil, initialMetadata, initialBuckets)
					if !result.handled {
						t.Fatal("clear execution was not handled")
					}
					for _, field := range []string{"revision_count", "dedup_key", "accumulated_count"} {
						if _, ok := instance.Fields[field]; ok {
							t.Fatalf("clear retained field %q in %#v", field, instance.Fields)
						}
					}
					if nodeBucket, ok := instance.StateBuckets[nodeKey].(map[string]any); ok {
						if _, retained := nodeBucket["handler_accumulators"]; retained {
							t.Fatalf("clear retained handler accumulator state: %#v", nodeBucket)
						}
					}
				})

				t.Run(engine+"/guard_kill", func(t *testing.T) {
					instance, result := executeExistingOwnerBehavior(t, ctx, pc, engine, engine+"-guard-kill", runtimecontracts.SystemNodeEventHandler{
						Guard: &runtimecontracts.GuardSpec{Check: "false", OnFail: "kill"},
					}, nil, nil, nil)
					if !result.handled || (result.status != "" && result.status != HandlerOutcomeKilled) {
						t.Fatalf("guard kill outcome = handled:%t status:%q, want handled killed outcome", result.handled, result.status)
					}
					if got := strings.TrimSpace(instance.CurrentState); got != "killed" {
						t.Fatalf("guard kill current state = %q, want killed", got)
					}
				})
			}
		})
	}
}

func TestEntitylessDeclarativeEmissionDoesNotMaterializeWorkflowStateOnSQLiteAndPostgres(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			bus := &recordingPipelineBus{}
			pc := newDurablePipelineCoordinatorForTest(bus, store.testDB(), PipelineCoordinatorOptions{
				Module:              staticSemanticWorkflowModule{source: handlerEntityRequirementExecutionSource()},
				Persistence:         workflowPersistenceForTest(store),
				PipelineObligations: unavailablePipelineTestObligationOwner{},
			})
			runID := runtimecorrelation.RunIDFromContext(ctx)
			instancePath := runID
			evt := handlerTestRootIngress(
				uuid.NewString(), "work.ready", "", "", json.RawMessage(`{"item_id":"a"}`), 0, runID, "",
				handlerTestWorkflowEnvelope(".", instancePath, ""), time.Now().UTC(),
			)
			dialect := authoractivityfixture.DialectPostgres
			if store.isSQLite() {
				dialect = authoractivityfixture.DialectSQLite
			}
			seedPipelineEventRecordForDialect(t, ctx, store.testDB(), dialect, evt)
			node := pipelineNode(t, ".", "node-a")
			deliveryCtx := withWorkflowNodeDeliveryRoute(ctx, events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(node),
				Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{
					FlowID: ".", FlowInstance: instancePath,
				}),
			})

			outcome, err := newCoordinatorHandlerExecutionEngine(pc, node).ExecuteHandlerSteps(
				deliveryCtx,
				runtimecontracts.SystemNodeEventHandler{Emit: runtimecontracts.EmitSpec{Event: "work.emitted"}},
				evt,
				"work.ready",
			)
			if err != nil {
				t.Fatalf("execute entityless declarative handler: %v", err)
			}
			if outcome == nil || !outcome.Handled {
				t.Fatalf("entityless declarative outcome = %#v, want handled", outcome)
			}
			if bus.outboxCount() != 1 || bus.outboxIntent(0).Event.Type() != events.EventType("work.emitted") {
				t.Fatalf("entityless durable publications = %#v, want one work.emitted event", bus.outboxIntents)
			}

			assertCount := func(label, sqliteQuery, postgresQuery string, args ...any) {
				t.Helper()
				query := postgresQuery
				if store.isSQLite() {
					query = sqliteQuery
				}
				var count int
				if err := store.testDB().QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
					t.Fatalf("count %s: %v", label, err)
				}
				if count != 0 {
					t.Fatalf("%s rows = %d, want 0 after entityless declarative execution", label, count)
				}
			}
			assertCount("entity_state", "SELECT COUNT(*) FROM entity_state WHERE run_id = ?", "SELECT COUNT(*) FROM entity_state WHERE run_id = $1::uuid", runID)
			assertCount("flow_instances", "SELECT COUNT(*) FROM flow_instances WHERE instance_id = ?", "SELECT COUNT(*) FROM flow_instances WHERE instance_id = $1", instancePath)
		})
	}
}

func openHandlerEntityRequirementStore(t *testing.T, backend string) (*sql.DB, *workflowInstanceStore) {
	t.Helper()
	if backend == "sqlite" {
		db := newSQLiteWorkflowInstanceStoreTestDB(t)
		return db, newSQLiteWorkflowInstanceStoreForTest(t, db)
	}
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	return db, newPostgresWorkflowInstanceStoreForTest(db)
}

func executeExistingOwnerBehavior(
	t *testing.T,
	ctx context.Context,
	pc *PipelineCoordinator,
	engine string,
	name string,
	handler runtimecontracts.SystemNodeEventHandler,
	payload json.RawMessage,
	metadata map[string]any,
	stateBuckets map[string]any,
) (WorkflowInstance, existingOwnerExecutionResult) {
	t.Helper()
	runID := runtimecorrelation.RunIDFromContext(ctx)
	flowInstance := runID
	entityID := eventtest.UUID("entity-requirement-root")
	seedMetadata := cloneStringAnyMap(metadata)
	if seedMetadata == nil {
		seedMetadata = map[string]any{}
	}
	if err := pc.workflowStore.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID:      runID,
		StorageRef:      flowInstance,
		EntityID:        entityID,
		WorkflowName:    ".",
		WorkflowVersion: "1",
		CurrentState:    "active",
		Fields:          seedMetadata,
		StateBuckets:    stateBuckets,
		EntityType:      "test_entity",
	})); err != nil {
		t.Fatalf("seed %s workflow instance: %v", name, err)
	}

	sourceEvent := handlerTestRootIngress(
		uuid.NewString(), "work.ready", "", "", payload, 0, runID, "",
		handlerTestWorkflowEnvelope(".", flowInstance, entityID), time.Now().UTC(),
	)
	target := events.RouteIdentity{FlowID: ".", FlowInstance: flowInstance, EntityID: entityID}
	seedExactOnceEvent(t, pc.workflowStore, ctx, sourceEvent)
	evt := eventtest.TargetRouted(sourceEvent, target)
	node := pipelineNode(t, ".", "node-a")
	deliveryCtx := withWorkflowNodeDeliveryRoute(ctx, events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(node),
		Target:    events.MustExistingEntityTarget(target),
	})
	result := existingOwnerExecutionResult{}
	var err error
	switch engine {
	case "bridge":
		var executed contractHandlerExecutionResult
		executed, err = pc.executeNodeContractHandler(deliveryCtx, node, handler, workflowTriggerContext{
			Event: evt,
			State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(flowInstance), entityID),
		}, false)
		result.handled = executed.Handled
		if executed.Outcome != nil {
			result.status = executed.Outcome.Status
		}
	case "declarative":
		var executed *HandlerOutcome
		executed, err = newCoordinatorHandlerExecutionEngine(pc, node).ExecuteHandlerSteps(deliveryCtx, handler, evt, "work.ready")
		result.handled = executed != nil && executed.Handled
	default:
		t.Fatalf("unknown handler execution engine %q", engine)
	}
	if err != nil {
		t.Fatalf("execute %s %s handler: %v", engine, name, err)
	}
	instance, ok, err := pc.workflowStore.Load(ctx, testWorkflowInstanceRoute(flowInstance))
	if err != nil || !ok {
		t.Fatalf("load %s workflow instance: found=%t err=%v", name, ok, err)
	}
	return instance, result
}

type existingOwnerExecutionResult struct {
	handled bool
	status  HandlerOutcomeStatus
}

func handlerEntityRequirementExecutionSource() semanticview.Source {
	flow := runtimecontracts.FlowContractView{
		Path:   ".",
		Paths:  runtimecontracts.FlowContractPaths{FlowPath: "."},
		Events: map[string]runtimecontracts.EventCatalogEntry{"work.ready": {}, "work.emitted": {}},
		Schema: runtimecontracts.FlowSchemaDocument{
			Name: "review", Mode: runtimecontracts.FlowModeStatic, InitialState: "active",
			States: []string{"active", "killed"}, TerminalStates: []string{"killed"},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"node-a": {ID: "node-a", ExecutionType: runtimecontracts.SystemNodeExecutionType},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics:    runtimecontracts.WorkflowSemanticView{Name: "review", Version: "1"},
		RootSchema:   &flow.Schema,
		RootEntities: testEntityContractsForType("test_entity"),
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &flow, ByID: map[string]*runtimecontracts.FlowContractView{".": &flow},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{".": flow.Schema},
	}
	return handlerEntityRequirementSemanticSource{Source: semanticview.Wrap(bundle)}
}

type handlerEntityRequirementSemanticSource struct {
	semanticview.Source
}

func (s handlerEntityRequirementSemanticSource) ExecutableNodeSource(node runtimeidentity.ExecutableNode) (runtimecontracts.ContractItemSource, bool) {
	if node.Equal(mustPipelineNode(".", "node-a")) {
		return runtimecontracts.ContractItemSource{FlowPath: ".", Family: "nodes"}, true
	}
	return s.Source.ExecutableNodeSource(node)
}
