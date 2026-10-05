package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	entitystore "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	"github.com/google/uuid"
)

type flowConstructorFaultOwner struct {
	owner   manager.FlowInstanceActivationCommitter
	fault   error
	reject  bool
	writes  int
	results []pipeline.CommittedFlowInstanceActivation
}

type flowConstructorLifecycleRecorder struct {
	*pipeline.PipelineCoordinator
	finalizations int
}

func (r *flowConstructorLifecycleRecorder) FinalizeInitialEntryLifecycle(ctx context.Context, committed pipeline.CommittedWorkflowLifecycleMutation) error {
	r.finalizations++
	return r.PipelineCoordinator.FinalizeInitialEntryLifecycle(ctx, committed)
}

func (o *flowConstructorFaultOwner) CommitFlowInstanceActivation(ctx context.Context, plan pipeline.FlowInstanceActivationPlan) (pipeline.CommittedFlowInstanceActivation, error) {
	if o.reject {
		return pipeline.CommittedFlowInstanceActivation{Plan: plan, Created: true, ReadinessAttemptOrdinal: 1}, o.fault
	}
	result, err := o.owner.CommitFlowInstanceActivation(ctx, plan)
	o.results = append(o.results, result)
	if !result.Acknowledged {
		return result, err
	}
	if result.Created {
		o.writes++
	}
	return result, errors.Join(err, o.fault)
}

func TestFlowConstructorAcknowledgedFailureRetainsExactIdentityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, acknowledged := range []bool{false, true} {
			name := "unacknowledged"
			if acknowledged {
				name = "acknowledged"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				fault := errors.New("injected constructor response failure")
				owner := &flowConstructorFaultOwner{fault: fault, reject: !acknowledged}
				lifecycle := &flowConstructorLifecycleRecorder{}
				files := map[string]string{
					"schema.yaml":          "name: constructor-ack-proof\n",
					"review/schema.yaml":   "name: review\ninstance: request_id\nstages:\n  pending: {initial: true}\npins:\n  inputs:\n    - task.started\n",
					"review/entities.yaml": "review_item:\n  request_id: text\n",
					"review/events.yaml":   "task.started:\n",
				}
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, files, func(base manager.FlowInstanceActivationCommitter) manager.FlowInstanceActivationCommitter {
					owner.owner = base
					return owner
				}, func(options *manager.AgentManagerOptions) {
					lifecycle.PipelineCoordinator = options.WorkflowInstances.(*pipeline.PipelineCoordinator)
					options.WorkflowInstances = lifecycle
				})
				req := f.request("receipt", "receipt", "unchanged")
				if err := f.manager.ActivateFlowInstance(f.ctx, req); !errors.Is(err, fault) {
					t.Fatalf("constructor lost commit failure: %v", err)
				}
				identity, err := flowidentity.NewRunScopedFlowInstance(correlation.RunIDFromContext(f.ctx), req.Instance.Route())
				if err != nil {
					t.Fatal(err)
				}
				instance, found, err := f.workflows.Load(f.ctx, identity)
				if err != nil || found != acknowledged {
					t.Fatalf("constructor readback found=%t err=%v, want %t", found, err, acknowledged)
				}
				if !acknowledged {
					if owner.writes != 0 || len(owner.results) != 0 || lifecycle.finalizations != 0 || len(f.bus.routePaths()) != 0 {
						t.Fatalf("unacknowledged constructor retained evidence: owner=%+v finalizations=%d routes=%v", owner, lifecycle.finalizations, f.bus.routePaths())
					}
					assertConstructorRows(t, f, backend, 0)
					return
				}
				if owner.writes != 1 || len(owner.results) != 1 || !owner.results[0].Acknowledged || instance.EntityID != req.Instance.EntityID || lifecycle.finalizations != 1 {
					t.Fatalf("acknowledged constructor lost exact identity: instance=%+v results=%+v writes=%d finalizations=%d", instance, owner.results, owner.writes, lifecycle.finalizations)
				}
				assertConstructorRows(t, f, backend, 1)
				before := assertActualMutationLedger(t, f.ctx, exactFactStore{db: f.db, postgres: backend == "postgres"}, identity.RunID, instance.EntityID)
				owner.fault = nil
				if err := f.manager.ActivateFlowInstance(f.ctx, req); err != nil {
					t.Fatalf("exact constructor replay: %v", err)
				}
				if owner.writes != 1 || len(owner.results) != 2 || !owner.results[1].Acknowledged || owner.results[1].Created || owner.results[1].Plan.Identity != req.Instance || lifecycle.finalizations != 1 {
					t.Fatalf("replay repeated construction: results=%+v writes=%d finalizations=%d", owner.results, owner.writes, lifecycle.finalizations)
				}
				after := assertActualMutationLedger(t, f.ctx, exactFactStore{db: f.db, postgres: backend == "postgres"}, identity.RunID, instance.EntityID)
				if len(before) == 0 || len(after) != len(before) {
					t.Fatalf("replay changed construction mutation ledger: before=%d after=%d", len(before), len(after))
				}
				assertConstructorRows(t, f, backend, 1)
			})
		}
	}
}

func assertConstructorRows(t *testing.T, f receiverConfigActivationFixture, backend string, want int) {
	t.Helper()
	for _, table := range []string{"entity_state", "flow_instances", "workflow_instance_initial_materializations", "flow_instance_runtime_readiness"} {
		query := "SELECT COUNT(*) FROM " + table + " WHERE run_id = ?"
		if backend == "postgres" {
			query = "SELECT COUNT(*) FROM " + table + " WHERE run_id = $1::uuid"
		}
		var count int
		if err := f.db.QueryRowContext(f.ctx, query, correlation.RunIDFromContext(f.ctx)).Scan(&count); err != nil || count != want {
			t.Fatalf("constructor %s rows=%d err=%v, want %d", table, count, err, want)
		}
	}
}

func TestFlowConstructorActivationConsumesExactInputBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, test := range []struct {
			name, fields, eventFields, payload, input, refusal string
			key                                                any
		}{
			{name: "supplied", payload: `{"request_id":"r1","brief":"scope","message":"not state"}`, key: "r1"},
			{name: "optional absent", payload: `{"request_id":"r1","brief":"scope"}`, key: "r1"},
			{name: "optional supplied", payload: `{"request_id":"r1","brief":"scope","note":"present"}`, key: "r1"},
			{name: "contradictory key", payload: `{"request_id":"r2","brief":"scope"}`, key: "r1", refusal: "contradicts"},
			{name: "missing key", payload: `{"request_id":"r1","brief":"scope"}`, refusal: "requires admitted resolved key"},
			{name: "wrong key type", payload: `{"request_id":"r1","brief":"scope"}`, key: 7, refusal: "requires admitted resolved key"},
			{name: "missing required", payload: `{"request_id":"r1"}`, key: "r1", refusal: "payload"},
			{name: "wrong supplied type", payload: `{"request_id":"r1","brief":7}`, key: "r1", refusal: "payload"},
			{name: "undeclared input", payload: `{"request_id":"r1","brief":"scope"}`, key: "r1", input: "unknown", refusal: "not a declared input"},
			{name: "internal collision", payload: `{"request_id":"r1","brief":"scope","count":7}`, key: "r1", eventFields: "  count: integer\n", refusal: "collides with internal"},
			{name: "ineligible input", payload: `{"request_id":"r1","brief":"scope"}`, key: "r1", fields: "  missing: text\n", refusal: "not definitely assigned"},
		} {
			t.Run(backend+"/"+test.name, func(t *testing.T) {
				files := map[string]string{
					"schema.yaml":          "name: constructor-input\n",
					"review/schema.yaml":   "name: review\ninstance: request_id\nstages:\n  pending: {initial: true}\npins:\n  inputs:\n    - task.started\n",
					"review/entities.yaml": "review_item:\n  request_id: text\n  brief: text\n  note: text?\n  count: {type: integer, initial: 0}\n" + test.fields,
					"review/events.yaml":   "task.started:\n  request_id: text\n  brief: text\n  note: text?\n  message: text?\n" + test.eventFields,
				}
				if test.name == "ineligible input" {
					files["review/nodes.yaml"] = "inspect:\n  execution_type: system_node\n  event_handlers:\n    task.started:\n      guard: {check: entity.missing != ''}\n"
				}
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, files, nil)
				req := sqliteFlowActivationRequest(f.bundle, "review", "r1", "", "review/r1")
				req.ConstructorInput = "task.started"
				if test.input != "" {
					req.ConstructorInput = test.input
				}
				req.ResolvedKey = test.key
				req.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "task.started", "constructor-fixture", "", []byte(test.payload), 0, correlation.RunIDFromContext(f.ctx), events.EventEnvelope{}, req.OccurredAt)
				err := f.manager.ActivateFlowInstance(f.ctx, req)
				if test.refusal != "" {
					if err == nil || !strings.Contains(err.Error(), test.refusal) {
						t.Fatalf("want %s before construction, got %v", test.refusal, err)
					}
					assertConstructorRows(t, f, backend, 0)
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				owner, err := flowidentity.NewRunScopedFlowInstance(correlation.RunIDFromContext(f.ctx), req.Instance.Route())
				if err != nil {
					t.Fatal(err)
				}
				instance, found, err := f.workflows.Load(f.ctx, owner)
				if err != nil || !found || instance.Fields["request_id"] != "r1" || instance.Fields["brief"] != "scope" || instance.Fields["count"] == nil {
					t.Fatalf("constructor projection=%+v found=%t err=%v", instance, found, err)
				}
				if _, leaked := instance.Fields["message"]; leaked {
					t.Fatalf("constructor seeded message-only field: %v", instance.Fields)
				}
				note, supplied := instance.Fields["note"]
				if supplied != (test.name == "optional supplied") || supplied && note != "present" {
					t.Fatalf("constructor changed sparse optional supply: %v", instance.Fields)
				}
				assertConstructorRows(t, f, backend, 1)
			})
		}
	}
}

func TestFlowConstructorPersistsCreatingInputWithoutAutoEmitBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newEagerFlowConstructorFixture(t, backend)
			req := f.request("business-key", "r1", "first")
			plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Readiness.CreationEvent != nil {
				t.Fatal("fixture must not have auto-emission")
			}
			result, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
			if err != nil || !result.Acknowledged {
				t.Fatalf("constructor commit: %+v %v", result, err)
			}
			for _, construction := range plan.ConstructionPlans() {
				var raw []byte
				if err := f.db.QueryRowContext(f.ctx, `SELECT projection FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2`, correlation.RunIDFromContext(f.ctx), construction.Identity.InstancePath).Scan(&raw); err != nil {
					t.Fatal(err)
				}
				var receipt struct {
					CreatingInput struct {
						EventID string `json:"event_id"`
						Input   string `json:"input"`
					} `json:"creating_input"`
				}
				if err := json.Unmarshal(raw, &receipt); err != nil {
					t.Fatal(err)
				}
				wantInput := ""
				if construction.Identity == plan.Identity {
					wantInput = req.ConstructorInput
				}
				if receipt.CreatingInput.EventID != req.TriggerEvent.ID() || receipt.CreatingInput.Input != wantInput {
					t.Fatalf("lost exact constructor origin for %s: %+v want event=%s input=%s", construction.Identity.InstancePath, receipt, req.TriggerEvent.ID(), wantInput)
				}
			}
			req.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), req.TriggerEvent.Type(), "constructor-fixture", "", req.TriggerEvent.Payload(), 0, correlation.RunIDFromContext(f.ctx), events.EventEnvelope{}, req.TriggerEvent.CreatedAt())
			changed, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			result, err = (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, changed)
			if err == nil || result.Acknowledged || result.Created {
				t.Fatalf("different creating delivery reused construction: %+v %v", result, err)
			}
		})
	}
}

func TestFieldlessFlowConstructionKeepsLifecycleWithoutStateRowBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, staged := range []bool{false, true} {
			name := "inert"
			if staged {
				name = "staged"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				declaration := "name: review\n"
				if staged {
					declaration += "stages:\n  pending: {initial: true}\n"
				}
				receipts := &flowConstructorFaultOwner{}
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
					"schema.yaml":        "name: fieldless-construction\n",
					"review/schema.yaml": declaration,
				}, func(owner manager.FlowInstanceActivationCommitter) manager.FlowInstanceActivationCommitter {
					receipts.owner = owner
					return receipts
				})
				req := sqliteFlowActivationRequest(f.bundle, "review", "review", "", "review")
				rootIdentity := flowidentity.Stored(semanticview.Wrap(f.bundle), ".", correlation.RunIDFromContext(f.ctx), correlation.RunIDFromContext(f.ctx), correlation.RunIDFromContext(f.ctx), "")
				childIdentity, err := flowidentity.KeylessChild(semanticview.Wrap(f.bundle), rootIdentity, "review")
				if err != nil {
					t.Fatal(err)
				}
				req.Instance = childIdentity
				if err := f.manager.ActivateFlowInstance(f.ctx, req); err != nil {
					t.Fatalf("fieldless canonical constructor: %v", err)
				}
				owner, err := flowidentity.NewRunScopedFlowInstance(correlation.RunIDFromContext(f.ctx), req.Instance.Route())
				if err != nil {
					t.Fatal(err)
				}
				instance, found, err := f.workflows.Load(f.ctx, owner)
				if err != nil || !found || instance.EntityID != req.Instance.EntityID || len(instance.Fields) != 0 {
					t.Fatalf("fieldless header readback: found=%t instance=%+v err=%v", found, instance, err)
				}
				if instance.CurrentState != "pending" || instance.StageDefined != staged || instance.Revision != 1 {
					t.Fatalf("fieldless header lost exact lifecycle: %+v", instance)
				}
				target, err := f.store.(pipeline.WorkflowTargetPersistenceReader).LoadWorkflowTargetPersistence(f.ctx, owner, identity.NormalizeEntityID(instance.EntityID))
				if err != nil || target.Presence != pipeline.WorkflowTargetPersistenceCompleteFieldless {
					t.Fatalf("fieldless target presence=%d err=%v", target.Presence, err)
				}
				if _, err := target.DecodeComplete(owner.Route, identity.NormalizeEntityID(instance.EntityID)); err != nil {
					t.Fatalf("decode fieldless constructed target: %v", err)
				}
				root, _ := semanticview.WorkflowStageTopology(f.workflows.SemanticSource(), ".")
				flow, _ := semanticview.WorkflowStageTopology(f.workflows.SemanticSource(), "review")
				catalog, err := contracts.NewWorkflowStageClassifier(root, map[string]contracts.WorkflowStageTopology{"review": flow})
				if err != nil {
					t.Fatal(err)
				}
				dialect := entitystore.SummaryDialectSQLite
				if backend == "postgres" {
					dialect = entitystore.SummaryDialectPostgres
				}
				summary, err := entitystore.ReadRunSummary(f.ctx, f.db, dialect, owner.RunID, catalog)
				wantTotal := 0
				if staged {
					wantTotal = 1
				}
				if err != nil || summary.Total != wantTotal || summary.Nonterminal != wantTotal {
					t.Fatalf("fieldless staged lifecycle summary=%+v err=%v, want total %d", summary, err, wantTotal)
				}
				record, err := receipts.results[0].Plan.PersistenceRecord()
				if err != nil {
					t.Fatal(err)
				}
				mutation := record.State
				mutation.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
				mutation.ExpectedState, mutation.ExpectedRevision = instance.CurrentState, instance.Revision
				mutation.UpdatedAt = mutation.CreatedAt.Add(time.Second)
				mutation.Bookkeeping = json.RawMessage(`{"construction_progress_control":true}`)
				result, err := f.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(f.ctx, pipeline.WorkflowEngineMutationCommand{State: mutation})
				if err != nil || !result.Committed {
					t.Fatalf("fieldless header progress committed=%t err=%v", result.Committed, err)
				}
				if err := f.manager.ActivateFlowInstance(f.ctx, req); err != nil {
					t.Fatalf("constructor replay after lifecycle progress: %v", err)
				}
				instance, found, err = f.workflows.Load(f.ctx, owner)
				if err != nil || !found || instance.Revision != 2 || instance.Bookkeeping["construction_progress_control"] != true || receipts.writes != 1 {
					t.Fatalf("constructor replay replaced mutable progress: instance=%+v writes=%d err=%v", instance, receipts.writes, err)
				}
				for _, table := range []string{"flow_instances", "entity_state"} {
					query := "SELECT COUNT(*) FROM " + table + " WHERE run_id = ?"
					if backend == "postgres" {
						query = "SELECT COUNT(*) FROM " + table + " WHERE run_id = $1::uuid"
					}
					want := 1
					if table == "entity_state" {
						want = 0
					}
					var count int
					if err := f.db.QueryRowContext(f.ctx, query, owner.RunID).Scan(&count); err != nil || count != want {
						t.Fatalf("fieldless %s rows=%d err=%v, want %d", table, count, err, want)
					}
				}
			})
		}
	}
}

func TestOrdinaryWorkflowMutationCannotConstructOrRepairBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, ctx, runID := openStateOnlyAcquisitionStore(t, backend)
			for _, imported := range []bool{false, true} {
				name := "absent"
				if imported {
					name = "imported_state"
				}
				t.Run(name, func(t *testing.T) {
					flowID, entityID := "unconstructed-"+uuid.NewString(), uuid.NewString()
					path, at := flowID+"/receiver", time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
					record := stateOnlyWorkflowEngineMutationRecord(t, runID, flowID, path, entityID, "active", 1, at)
					if imported {
						seedWorkflowTargetStateForTransition(t, backend, db, runID, entityID, path, "active", 1, at)
					} else {
						record.ExpectedState, record.ExpectedRevision = "", 0
						record.Transition = pipeline.WorkflowEngineStateTransitionCreateStateAndCompanion
					}
					_, err := selected.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: record})
					if err == nil || !strings.Contains(err.Error(), "ordinary workflow mutation requires a constructed target") {
						t.Fatalf("unconstructed target execution admitted: %v", err)
					}
					query := "SELECT COUNT(*) FROM flow_instances WHERE run_id = ? AND instance_path = ?"
					if backend == "postgres" {
						query = "SELECT COUNT(*) FROM flow_instances WHERE run_id = $1::uuid AND instance_path = $2"
					}
					var headers int
					if err := db.QueryRowContext(ctx, query, runID, path).Scan(&headers); err != nil || headers != 0 {
						t.Fatalf("refused execution repaired header: headers=%d err=%v", headers, err)
					}
					assertNoWorkflowEngineHistory(t, backend, db, runID, entityID)
					if imported {
						assertWorkflowTargetTransitionRows(t, backend, db, runID, entityID, path, "", "active", 1, 0)
					}
				})
			}
		})
	}
}
