package runtimepersistence

import (
	"reflect"
	"strings"
	"testing"
	"time"

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
			for _, cut := range []string{"missing_readiness", "header_delete_refused"} {
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
					if projection, err := reader.InspectDynamicFlowRuntimeReadinessForSource(f.ctx, fact); err != nil || len(projection.CurrentPending) != 3 {
						t.Fatalf("healthy constructed tree: %+v %v", projection, err)
					}
					// Native corruption fixtures are not executable route admission.
					query := `INSERT INTO routing_rules (run_id, event_pattern, subscriber_type, subscriber_id, flow_instance, source_flow, is_materialized, status, created_at) VALUES (?1, ?2, 'node', 'readiness-fixture', ?3, ?4, TRUE, 'active', ?5)`
					if backend == "postgres" {
						query = `INSERT INTO routing_rules (run_id, event_pattern, subscriber_type, subscriber_id, flow_instance, source_flow, is_materialized, status, created_at) VALUES ($1::uuid, $2, 'node', 'readiness-fixture', $3, $4, TRUE, 'active', $5)`
					}
					if _, err := f.db.ExecContext(f.ctx, query, runID, path+"/fixture", path, instance.Identity.TemplateID, time.Now().UTC()); err != nil {
						t.Fatal(err)
					}
					for _, inspect := range []string{"source", "run"} {
						if err := inspectConstructedRouteReadiness(f, reader, fact, runID, inspect); err != nil {
							t.Fatalf("healthy %s inspection: %v", inspect, err)
						}
					}
					query = `DELETE FROM flow_instance_runtime_readiness WHERE run_id=$1 AND instance_path=$2`
					if cut == "header_delete_refused" {
						query = `DELETE FROM flow_instances WHERE run_id=$1 AND instance_path=$2`
						before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
						if _, err := f.db.ExecContext(f.ctx, query, runID, path); err == nil {
							t.Fatal("active route lost its foreign-key-bound constructed header")
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
						if err := inspectConstructedRouteReadiness(f, reader, fact, runID, inspect); err == nil || !strings.Contains(err.Error(), "runtime readiness owner") {
							t.Errorf("%s admitted %s for %s: %v", inspect, cut, path, err)
						}
					}
					foreign := sourceartifactfixture.FactFor(sourceartifactfixture.New("schema.yaml", []byte("name: unrelated-readiness-source\n")))
					if projection, err := reader.InspectDynamicFlowRuntimeReadinessForSource(f.ctx, foreign); err != nil || len(projection.CurrentPending)+len(projection.CurrentCompleted)+len(projection.SourceTransitionRequired) != 0 {
						t.Fatalf("foreign source consumed corrupt route: %+v %v", projection, err)
					}
					if rows, err := reader.InspectDynamicFlowRuntimeReadinessForRun(f.ctx, uuid.NewString(), fact); err != nil || len(rows) != 0 {
						t.Fatalf("foreign run consumed corrupt route: %+v %v", rows, err)
					}
					if after := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres"); !reflect.DeepEqual(before, after) {
						t.Fatal("read-only refusal changed durable construction evidence")
					}
					if _, err := f.db.ExecContext(f.ctx, `UPDATE routing_rules SET status='inactive' WHERE run_id=$1 AND flow_instance=$2`, runID, path); err != nil {
						t.Fatal(err)
					}
					for _, inspect := range []string{"source", "run"} {
						if err := inspectConstructedRouteReadiness(f, reader, fact, runID, inspect); err != nil {
							t.Fatalf("inactive route acquired %s execution obligation: %v", inspect, err)
						}
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
