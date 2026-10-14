package runtimepersistence

import (
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestFlowConstructorRouteReadinessCorruptionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, target := range []struct {
			name  string
			index int
		}{
			{"keyed_fields", 0},
			{"static_fieldless", 1},
			{"static_fields", 2},
		} {
			cuts := []string{"missing_readiness", "missing_receipt"}
			if target.index < 2 {
				cuts = append(cuts, "parent_delete_refused")
			}
			for _, cut := range cuts {
				t.Run(backend+"/"+target.name+"/"+cut, func(t *testing.T) {
					f := newEagerFlowConstructorFixture(t, backend)
					plan := eagerFlowConstructorPlan(t, f)
					committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
					if err != nil || !committed.Acknowledged || !committed.Created {
						t.Fatalf("canonical construction: %+v %v", committed, err)
					}
					instance := plan.ConstructionPlans()[target.index]
					path, runID := instance.Identity.InstancePath, instance.Readiness.RunID
					fact, found := correlation.SourceArtifactFactFromContext(f.ctx)
					if !found {
						t.Fatal("constructor lost source authority")
					}
					reader := f.store.(pipeline.DynamicFlowRuntimeReadinessPersistence)
					if projection, err := reader.InspectDynamicFlowRuntimeReadinessForSource(f.ctx, fact); err != nil || len(projection.CurrentPending) != len(plan.ConstructionPlans())+1 {
						t.Fatalf("healthy constructed tree: %+v %v", projection, err)
					}
					for _, inspect := range []string{"source", "run"} {
						if err := inspectConstructedRouteReadiness(f, reader, fact, runID, inspect); err != nil {
							t.Fatalf("healthy %s inspection: %v", inspect, err)
						}
					}
					query := `DELETE FROM flow_instance_runtime_readiness WHERE run_id=$1 AND instance_path=$2`
					if cut == "missing_receipt" {
						query = `DELETE FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2`
					}
					if cut == "parent_delete_refused" {
						query = `DELETE FROM flow_instances WHERE run_id=$1 AND instance_path=$2`
						before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
						if _, err := f.db.ExecContext(f.ctx, query, runID, path); err == nil {
							t.Fatal("constructed descendant lost its exact foreign-key-bound parent")
						}
						if after := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres"); !reflect.DeepEqual(before, after) {
							t.Fatal("refused header deletion changed durable construction evidence")
						}
						return
					}
					if _, err := f.db.ExecContext(f.ctx, query, runID, path); err != nil {
						t.Fatal(err)
					}
					before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
					for _, inspect := range []string{"source", "run"} {
						var corruption *pipeline.FlowInstanceConstructionCorruption
						if err := inspectConstructedRouteReadiness(f, reader, fact, runID, inspect); !errors.As(err, &corruption) || corruption.RunID != runID || corruption.InstancePath != path {
							t.Errorf("%s admitted %s for %s: %v", inspect, cut, path, err)
						}
					}
					foreign := sourceartifactfixture.FactFor(sourceartifactfixture.New("schema.yaml", []byte("name: unrelated-readiness-source\n")))
					if projection, err := reader.InspectDynamicFlowRuntimeReadinessForSource(f.ctx, foreign); err != nil || len(projection.CurrentPending)+len(projection.CurrentCompleted)+len(projection.SourceTransitionRequired) != 0 {
						t.Fatalf("foreign source consumed corrupt construction: %+v %v", projection, err)
					}
					if rows, err := reader.InspectDynamicFlowRuntimeReadinessForRun(f.ctx, uuid.NewString(), fact); err != nil || len(rows) != 0 {
						t.Fatalf("foreign run consumed corrupt construction: %+v %v", rows, err)
					}
					if after := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres"); !reflect.DeepEqual(before, after) {
						t.Fatal("read-only refusal changed durable construction evidence")
					}
				})
			}
		}
	}
}

func inspectConstructedRouteReadiness(f receiverConfigActivationFixture, reader pipeline.DynamicFlowRuntimeReadinessPersistence, fact correlation.SourceArtifactFact, runID, surface string) error {
	if surface == "source" {
		_, err := reader.InspectDynamicFlowRuntimeReadinessForSource(f.ctx, fact)
		return err
	}
	_, err := reader.InspectDynamicFlowRuntimeReadinessForRun(f.ctx, runID, fact)
	return err
}
