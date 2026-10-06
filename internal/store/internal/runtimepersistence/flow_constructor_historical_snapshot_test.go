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
				proveFlowConstructorHistoricalSnapshot(t, backend, staged, false, false)
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
					proveFlowConstructorHistoricalSnapshot(t, backend, staged, shape.fielded, shape.terminal)
				})
			}
		}
	}
}

func proveFlowConstructorHistoricalSnapshot(t *testing.T, backend string, staged, fielded, terminal bool) {
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
	f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, files, nil)
	runID := correlation.RunIDFromContext(f.ctx)
	req := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
	req.Instance = flowidentity.Stored(req.ContractBundle, ".", runID, runID, runID, "")
	plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil {
		t.Fatal(err)
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
			stored.ParentFlowID != original.ParentFlowID || stored.ParentEntityID != parentEntity || stored.ParentFlowInstance != parentPath {
			t.Fatalf("historical header lost construction: stored=%+v original=%+v found=%t err=%v", stored, original, found, err)
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
	again, err := owner.MaterializeRunFork(f.ctx, request)
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
				var original any
				if err := f.db.QueryRowContext(f.ctx, "SELECT "+fault.column+" FROM flow_instances WHERE run_id=$1 AND instance_path='detail'", fork.ForkRunID).Scan(&original); err != nil {
					t.Fatal(err)
				}
				if _, err := f.db.ExecContext(f.ctx, "UPDATE flow_instances SET "+fault.column+"=$1 WHERE run_id=$2 AND instance_path='detail'", fault.value, fork.ForkRunID); err != nil {
					t.Fatal(err)
				}
				before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
				if _, err := owner.MaterializeRunFork(f.ctx, request); err == nil {
					t.Fatalf("exact replay accepted changed historical %s", fault.name)
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
					t.Fatal("historical evidence refusal repaired or changed state")
				}
				if _, err := f.db.ExecContext(f.ctx, "UPDATE flow_instances SET "+fault.column+"=$1 WHERE run_id=$2 AND instance_path='detail'", original, fork.ForkRunID); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
	if _, err := f.db.ExecContext(f.ctx, `DELETE FROM flow_instances WHERE run_id=$1 AND instance_path='detail'`, fork.ForkRunID); err != nil {
		t.Fatal(err)
	}
	before = snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
	if _, err := owner.MaterializeRunFork(f.ctx, request); err == nil || !strings.Contains(err.Error(), "missing expected entities") {
		t.Fatalf("exact replay repaired missing historical header: %v", err)
	}
	if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
		t.Fatal("corrupt historical header refusal changed source or fork state")
	}
}
