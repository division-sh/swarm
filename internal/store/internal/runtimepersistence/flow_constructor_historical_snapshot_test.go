package runtimepersistence

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestFlowConstructorHistoricalFieldlessSnapshotBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, staged := range []bool{false, true} {
			shape := "inert"
			if staged {
				shape = "staged"
			}
			t.Run(backend+"/"+shape, func(t *testing.T) {
				proveFlowConstructorHistoricalSnapshot(t, backend, staged, false, false, false)
			})
		}
	}
}

func TestFlowConstructorHistoricalFieldedAndTerminalSnapshotBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, shape := range []struct {
			name              string
			fielded, terminal bool
		}{{"fielded", true, false}, {"fielded_terminal", true, true}, {"fieldless_terminal", false, true}} {
			for _, staged := range []bool{false, true} {
				stage := "inert"
				if staged {
					stage = "staged"
				}
				t.Run(backend+"/"+shape.name+"/"+stage, func(t *testing.T) {
					proveFlowConstructorHistoricalSnapshot(t, backend, staged, shape.fielded, shape.terminal, false)
				})
			}
		}
	}
}

func TestR7HistoricalImmutableKeyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			proveFlowConstructorHistoricalSnapshot(t, backend, false, true, false, true)
		})
	}
}

func TestR7HistoricalDirectoryIgnoresPostCutConstructionAndLifecycleBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, request := newR7SelectionProof(t, backend)
			index := f.store.(pipeline.FlowInstanceIndexReader)
			selected, err := pipeline.PrepareFlowInstanceSelection(f.ctx, index, f.manager, request)
			if err != nil || selected.Activation == nil {
				t.Fatalf("prepare original receiver: %+v err=%v", selected, err)
			}
			committer := agentFixtureFlowActivationCommitter{store: f.store}
			if _, err := committer.CommitFlowInstanceActivation(f.ctx, *selected.Activation); err != nil {
				t.Fatal(err)
			}
			runID, source, fact := request.Lookup.RunID(), request.Lookup.Source(), request.Lookup.SourceFact()
			marker := eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType(source.ResolveFlowEventReference("worker", "item.finished")), "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, marker); err != nil {
				t.Fatal(err)
			}
			forks := f.store.(interface {
				PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
				MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
				RequireMaterializedRunFork(context.Context, string, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
			})
			cutRequest := runfork.RunForkPlanRequest{SourceRunID: runID, At: marker.ID()}
			cut, err := forks.PlanRunFork(f.ctx, cutRequest)
			if err != nil || len(cut.Entities) != 2 {
				t.Fatalf("capture root and original receiver: %+v err=%v", cut, err)
			}
			later := request.Constructor
			later.Instance, err = flowidentity.KeyedChild(source, request.Lookup.ParentIdentity(), "worker", "after-cut")
			if err != nil {
				t.Fatal(err)
			}
			later.ResolvedKey, later.OccurredAt = "later-key", time.Now().UTC()
			later.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "item.created", "constructor-fixture", "", []byte(`{"item_id":"later-key"}`), 0, runID, events.EventEnvelope{}, later.OccurredAt)
			laterPlan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, later)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := committer.CommitFlowInstanceActivation(f.ctx, laterPlan); err != nil {
				t.Fatal(err)
			}
			originalOwner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: selected.Identity().Route()}
			if err := f.workflows.MarkTerminated(f.ctx, originalOwner, identity.NormalizeEntityID(selected.Identity().EntityID), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			if current, found, err := index.LookupFlowInstance(f.ctx, request.Lookup); err != nil || !found {
				t.Fatalf("current terminal receiver disappeared: found=%t err=%v", found, err)
			} else if header, err := current.WorkflowInstance(); err != nil || header.Status != "terminated" || header.TerminatedAt.IsZero() {
				t.Fatalf("post-cut terminalization did not commit: %+v err=%v", header, err)
			}
			laterKeys, err := pipeline.AdmitFlowInstanceKeyMaterial(source, "worker", "later-key")
			if err != nil {
				t.Fatal(err)
			}
			laterLookup, err := pipeline.NewDeclaredFlowInstanceLookup(source, fact, runID, "worker", request.Lookup.ParentIdentity(), laterKeys)
			if err != nil {
				t.Fatal(err)
			}
			if current, found, err := index.LookupFlowInstance(f.ctx, laterLookup); err != nil || !found || current.Identity() != laterPlan.Identity {
				t.Fatalf("post-cut creation did not commit: %+v found=%t err=%v", current, found, err)
			}
			if again, err := forks.PlanRunFork(f.ctx, cutRequest); err != nil || again.ForkPoint != cut.ForkPoint || !reflect.DeepEqual(again.Entities, cut.Entities) {
				t.Fatalf("live construction/lifecycle changed the fixed cut: %+v err=%v", again, err)
			}
			forkRequest := runfork.RunForkMaterializeRequest{SourceRunID: runID, At: marker.ID()}
			fork, err := forks.MaterializeRunFork(f.ctx, forkRequest)
			if err != nil {
				t.Fatal(err)
			}
			rootLookup, err := pipeline.NewExactFlowInstanceLookup(source, fact, flowidentity.RunScopedFlowInstance{
				RunID: fork.ForkRunID, Route: flowidentity.StoredRoute(".", fork.ForkRunID, fork.ForkRunID),
			})
			if err != nil {
				t.Fatal(err)
			}
			root, found, err := index.LookupFlowInstance(f.ctx, rootLookup)
			if err != nil || !found {
				t.Fatalf("historical root missing: found=%t err=%v", found, err)
			}
			originalKeys, err := pipeline.AdmitFlowInstanceKeyMaterial(source, "worker", request.Constructor.ResolvedKey)
			if err != nil {
				t.Fatal(err)
			}
			originalLookup, err := pipeline.NewDeclaredFlowInstanceLookup(source, fact, fork.ForkRunID, "worker", root.Identity(), originalKeys)
			if err != nil {
				t.Fatal(err)
			}
			laterLookup, err = pipeline.NewDeclaredFlowInstanceLookup(source, fact, fork.ForkRunID, "worker", root.Identity(), laterKeys)
			if err != nil {
				t.Fatal(err)
			}
			readFixed := func() pipeline.FlowInstanceObservation {
				t.Helper()
				observed, found, err := index.LookupFlowInstance(f.ctx, originalLookup)
				if err != nil || !found || observed.Identity().EntityID != selected.Identity().EntityID || observed.InstanceKey() != "business-key" {
					t.Fatalf("historical selector lost original identity: %+v found=%t err=%v", observed, found, err)
				}
				header, err := observed.WorkflowInstance()
				if err != nil || header.Status != "active" || !header.TerminatedAt.IsZero() || header.CurrentState != "pending" || header.ParentFlowInstance != fork.ForkRunID {
					t.Fatalf("historical selector borrowed current lifecycle/parent: %+v err=%v", header, err)
				}
				fixed, historical := observed.HistoricalConstruction()
				if !historical || fixed.SourceOwner != originalOwner || fixed.SourceRevision != cut.ForkPoint.Revision {
					t.Fatalf("historical selector lost captured construction: %+v", fixed)
				}
				if _, native, err := observed.NativeConstruction(); err != nil || native {
					t.Fatalf("historical lookup fabricated original receipt: native=%t err=%v", native, err)
				}
				if _, ready := observed.Readiness(); ready {
					t.Fatal("materialize-only lookup fabricated fresh readiness")
				}
				if later, found, err := index.LookupFlowInstance(f.ctx, laterLookup); err != nil || found || later.Valid() {
					t.Fatalf("historical index borrowed a post-cut receiver: %+v found=%t err=%v", later, found, err)
				}
				return observed
			}
			original := readFixed()
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
			if err != nil {
				t.Fatal(err)
			}
			if replayed, err := forks.RequireMaterializedRunFork(f.ctx, fork.ForkRunID, forkRequest); err != nil || replayed.ForkRunID != fork.ForkRunID {
				t.Fatalf("exact fixed fork replay: %+v err=%v", replayed, err)
			}
			if after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store); err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("exact fixed fork replay changed durable evidence: %v", err)
			}
			artifact := sourceartifactfixture.New("agents.yaml", []byte("agents:\n  newer_source: {}\n"))
			sourceartifactfixture.RequireArtifact(t, f.ctx, f.store.(sourceartifactfixture.Writer), artifact)
			if _, err := f.store.(runlifecycle.OperationOwner).ReviseRunSource(f.ctx, runlifecycle.SourceRevisionRequest{RunID: runID, Source: sourceartifactfixture.FactFor(artifact)}); err != nil {
				t.Fatal(err)
			}
			before, err = ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
			if err != nil {
				t.Fatal(err)
			}
			if current := readFixed(); !reflect.DeepEqual(current, original) {
				t.Fatal("current source progress changed the captured observation")
			}
			if replayed, err := forks.RequireMaterializedRunFork(f.ctx, fork.ForkRunID, forkRequest); err == nil || replayed.ForkRunID != "" || !strings.Contains(err.Error(), "conflicts with persisted lifecycle state") {
				t.Fatalf("source progress rewrote frozen fork eligibility: %+v err=%v", replayed, err)
			}
			if after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store); err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("historical lookup/replay changed durable evidence: %v", err)
			}
		})
	}
}

func proveFlowConstructorHistoricalSnapshot(t *testing.T, backend string, staged, fielded, terminal, keyed bool) {
	stages := ""
	if staged {
		stages = "stages:\n  pending: {}\n"
	}
	files := map[string]string{
		"schema.yaml":        "name: historical-fieldless\n" + stages,
		"events.yaml":        "checkpoint:\n",
		"detail/schema.yaml": "name: detail\n",
	}
	if fielded {
		files["entities.yaml"] = "record:\n  marker: {type: text, initial: original}\n"
	}
	if keyed {
		files["schema.yaml"] += "instance: item_id\npins:\n  inputs:\n    - item.created\n"
		files["entities.yaml"] += "  item_id: text\n"
		files["events.yaml"] += "item.created:\n  item_id: text\n"
	}
	f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, files, nil)
	runID := correlation.RunIDFromContext(f.ctx)
	req := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
	req.Instance = flowidentity.Stored(req.ContractBundle, ".", runID, runID, runID, "")
	if keyed {
		req.ConstructorInput, req.ResolvedKey = "item.created", "original-key"
		req.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "item.created", "constructor-fixture", "", []byte(`{"item_id":"original-key"}`), 0, runID, events.EventEnvelope{}, req.OccurredAt)
	}
	plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil {
		t.Fatal(err)
	}
	if keyed {
		record, err := plan.PersistenceRecord()
		if err != nil {
			t.Fatal(err)
		}
		mutation := record.State
		mutation.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
		mutation.ExpectedState, mutation.ExpectedRevision = mutation.CurrentState, 1
		mutation.UpdatedAt = mutation.CreatedAt.Add(time.Second)
		mutation.Fields = []byte(`{"marker":"original","item_id":"changed-business-field"}`)
		if result, err := f.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(f.ctx, pipeline.WorkflowEngineMutationCommand{State: mutation}); err != nil || !result.Committed {
			t.Fatalf("ordinary key field update: committed=%t err=%v", result.Committed, err)
		}
	}
	originals := make(map[string]pipeline.WorkflowInstance)
	for _, construction := range plan.ConstructionPlans() {
		owner, err := flowidentity.NewRunScopedFlowInstance(runID, construction.Identity.Route())
		if err != nil {
			t.Fatal(err)
		}
		if terminal {
			if err := f.workflows.MarkTerminated(f.ctx, owner, identity.NormalizeEntityID(construction.Identity.EntityID), req.OccurredAt.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
		original, found, err := f.workflows.Load(f.ctx, owner)
		if err != nil || !found {
			t.Fatalf("read actual constructor commit: found=%t err=%v", found, err)
		}
		originals[construction.Identity.InstancePath] = original
	}
	marker := eventtest.ExistingRunRootIngress(uuid.NewString(), "checkpoint", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, marker); err != nil {
		t.Fatal(err)
	}
	owner := f.store.(interface {
		PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
		MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
		RequireMaterializedRunFork(context.Context, string, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
	})
	history, err := owner.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: marker.ID()})
	if err != nil || len(history.Entities) != 2 {
		t.Fatalf("constructed fieldless history: %+v %v", history.Entities, err)
	}
	request := runfork.RunForkMaterializeRequest{SourceRunID: runID, At: marker.ID()}
	fork, err := owner.MaterializeRunFork(f.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	for path, original := range originals {
		projection, err := runfork.ProjectEntityOwnership(runID, fork.ForkRunID, original.EntityID, path)
		if err != nil {
			t.Fatal(err)
		}
		route, err := runfork.ProjectExecutionRoute(runID, fork.ForkRunID, original.WorkflowName, flowidentity.StoredRoute(original.WorkflowName, original.InstanceID, path))
		if err != nil {
			t.Fatal(err)
		}
		parentEntity, parentPath := original.ParentEntityID, original.ParentFlowInstance
		if parentEntity != "" {
			parent, err := runfork.ProjectEntityOwnership(runID, fork.ForkRunID, parentEntity, parentPath)
			if err != nil {
				t.Fatal(err)
			}
			parentEntity, parentPath = parent.Fork.EntityID, parent.Fork.FlowInstance
		}
		identity, err := flowidentity.NewRunScopedFlowInstance(fork.ForkRunID, route)
		if err != nil {
			t.Fatal(err)
		}
		stored, found, err := f.workflows.Load(f.ctx, identity)
		if err != nil || !found || stored.EntityID != projection.Fork.EntityID || stored.StorageRef != projection.Fork.FlowInstance || stored.EntityType != original.EntityType || !reflect.DeepEqual(stored.Fields, original.Fields) ||
			stored.WorkflowVersion != original.WorkflowVersion || stored.StageDefined != original.StageDefined ||
			stored.CurrentState != original.CurrentState || !stored.EnteredStageAt.Equal(original.EnteredStageAt) ||
			stored.Status != original.Status || stored.Mode != original.Mode || stored.InstanceKind != original.InstanceKind ||
			!stored.CreatedAt.Equal(original.CreatedAt) || !stored.UpdatedAt.Equal(original.UpdatedAt) || !stored.TerminatedAt.Equal(original.TerminatedAt) ||
			stored.ParentFlowID != original.ParentFlowID || stored.ParentEntityID != parentEntity || stored.ParentFlowInstance != parentPath || stored.InstanceKey != original.InstanceKey {
			t.Fatalf("historical header lost construction: stored=%+v original=%+v found=%t err=%v", stored, original, found, err)
		}
		fact, present := correlation.SourceArtifactFactFromContext(f.ctx)
		if !present {
			t.Fatal("historical lookup requires the admitted artifact")
		}
		lookup, err := pipeline.NewExactFlowInstanceLookup(req.ContractBundle, fact, identity)
		if err != nil {
			t.Fatal(err)
		}
		observation, found, err := f.store.(pipeline.FlowInstanceIndexReader).LookupFlowInstance(f.ctx, lookup)
		if err != nil || !found || observation.Identity().EntityID != stored.EntityID || observation.InstanceKey() != original.InstanceKey {
			t.Fatalf("historical index discarded actual fixed construction: %+v found=%t err=%v", observation, found, err)
		}
		fixed, historical := observation.HistoricalConstruction()
		if !historical || fixed.SourceOwner.RunID != runID || fixed.SourceOwner.Route.InstancePath != path || fixed.SourceRevision != history.ForkPoint.Revision {
			t.Fatalf("index did not consume the exact admitted fork cut: %+v", fixed)
		}
		if _, ready := observation.Readiness(); ready {
			t.Fatal("materialize-only lookup invented desired attachment")
		}
		if _, native, err := observation.NativeConstruction(); err != nil || native {
			t.Fatalf("materialize-only lookup invented a fresh receipt: native=%t err=%v", native, err)
		}
	}
	listed, err := f.workflows.ListWorkflowInstances(f.ctx, fork.ForkRunID)
	if err != nil || len(listed) != len(originals) {
		t.Fatalf("historical instance list used field rows as instance inventory: got=%d want=%d err=%v", len(listed), len(originals), err)
	}
	var fields, readiness, construction int
	for table, count := range map[string]*int{"entity_state": &fields, "flow_instance_runtime_readiness": &readiness, "workflow_instance_initial_materializations": &construction} {
		if err := f.db.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM "+table+" WHERE run_id=$1", fork.ForkRunID).Scan(count); err != nil {
			t.Fatal(err)
		}
	}
	wantFields := 0
	if fielded {
		wantFields = 1
	}
	if fields != wantFields || readiness != 0 || construction != 0 {
		t.Fatalf("historical projection fabricated fresh state/execution: fields=%d readiness=%d construction=%d", fields, readiness, construction)
	}
	before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
	again, err := owner.RequireMaterializedRunFork(f.ctx, fork.ForkRunID, request)
	if err != nil || again.ForkRunID != fork.ForkRunID || !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
		t.Fatalf("exact materialize-only replay changed history: %+v %v", again, err)
	}
	if fielded && staged && !terminal {
		for _, fault := range []struct {
			name, column string
			value        any
		}{
			{"template", "flow_template", "foreign"}, {"mode", "mode", "template"},
			{"status", "status", "draining"}, {"stage", "current_state", "foreign"},
			{"stage_presence", "stage_defined", true}, {"config", "config", `{}`},
			{"entry_clock", "entered_state_at", time.Now().UTC()}, {"creation_clock", "created_at", req.OccurredAt.Add(-time.Hour)},
			{"update_clock", "updated_at", time.Now().UTC()}, {"revision", "revision", int64(2)},
			{"gates", "gates", `{"foreign":true}`}, {"bookkeeping", "bookkeeping", `{"foreign":true}`},
			{"accumulator", "accumulator", `{"foreign":true}`}, {"slug", "slug", "foreign"}, {"name", "name", "foreign"},
		} {
			t.Run("reuse_rejects/"+fault.name, func(t *testing.T) {
				coordinate := flowidentity.RunScopedFlowInstance{RunID: fork.ForkRunID, Route: flowidentity.StoredRoute("detail", "detail", "detail")}
				restore, err := FaultFlowConstructorHeaderForTest(f.ctx, f.store, coordinate, pipelinepersistence.FlowConstructorHeaderFaultField(fault.column), fault.value)
				if err != nil {
					t.Fatal(err)
				}
				before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
				if _, err := owner.RequireMaterializedRunFork(f.ctx, fork.ForkRunID, request); err == nil {
					t.Fatalf("exact replay accepted changed historical %s", fault.name)
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
					t.Fatal("historical evidence refusal repaired or changed state")
				}
				if err := restore(f.ctx); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
	if _, err := f.db.ExecContext(f.ctx, `DELETE FROM flow_instances WHERE run_id=$1 AND instance_path='detail'`, fork.ForkRunID); err != nil {
		t.Fatal(err)
	}
	before = snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
	if _, err := owner.RequireMaterializedRunFork(f.ctx, fork.ForkRunID, request); err == nil || !strings.Contains(err.Error(), "missing expected entities") {
		t.Fatalf("exact replay repaired missing historical header: %v", err)
	}
	if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
		t.Fatal("corrupt historical header refusal changed source or fork state")
	}
}
