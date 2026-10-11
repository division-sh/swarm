package runtimepersistence

import (
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestR7ReadinessDiagnosticsPreserveOlderPlanClassesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			currentFiles := map[string]string{"schema.yaml": "name: current-readiness-source\n"}
			currentBundle := loadLifecyclePersistenceFixtureForTest(t, currentFiles)
			current := mustStoreTestSourceArtifactFact(currentBundle.SourceArtifact.BundleHash())
			for _, row := range []struct {
				name          string
				old, complete bool
			}{{"current_completed", false, true}, {"current_pending", false, false}, {"predecessor_completed", true, true}, {"predecessor_pending", true, false}} {
				t.Run(row.name, func(t *testing.T) {
					files := currentFiles
					if row.old {
						files = map[string]string{"schema.yaml": "name: predecessor-readiness-source\n"}
					}
					f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, files, nil)
					runID := correlation.RunIDFromContext(f.ctx)
					request := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
					request.Instance = flowidentity.Stored(request.ContractBundle, ".", runID, runID, runID, "")
					plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, request)
					if err != nil {
						t.Fatal(err)
					}
					committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
					if err != nil {
						t.Fatal(err)
					}
					if row.complete {
						if err := f.manager.FinalizeCommittedFlowInstanceActivation(f.ctx, committed); err != nil {
							t.Fatal(err)
						}
					}
					if row.old {
						sourceartifactfixture.RequireArtifact(t, f.ctx, f.store, currentBundle.SourceArtifact)
						if _, err := f.store.(runlifecycle.OperationOwner).ReviseRunSource(f.ctx, runlifecycle.SourceRevisionRequest{RunID: runID, Source: current}); err != nil {
							t.Fatal(err)
						}
						lookup, err := pipeline.NewExactFlowInstanceLookup(semanticview.Wrap(currentBundle), current, flowidentity.RunScopedFlowInstance{RunID: runID, Route: plan.Identity.Route()})
						if err != nil {
							t.Fatal(err)
						}
						if observed, found, err := f.store.(pipeline.FlowInstanceIndexReader).LookupFlowInstance(f.ctx, lookup); err == nil || found || observed.Valid() {
							t.Fatalf("diagnostic transition granted current-version execution: found=%t err=%v", found, err)
						}
					}
					before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
					if err != nil {
						t.Fatal(err)
					}
					reader := f.store.(pipeline.DynamicFlowRuntimeReadinessPersistence)
					projection, err := reader.InspectDynamicFlowRuntimeReadinessForSource(f.ctx, current)
					wantCompleted, wantPending, wantTransition := 0, 0, 0
					switch {
					case row.old:
						wantTransition = 1
					case row.complete:
						wantCompleted = 1
					default:
						wantPending = 1
					}
					if err != nil || len(projection.CurrentCompleted) != wantCompleted || len(projection.CurrentPending) != wantPending || len(projection.SourceTransitionRequired) != wantTransition {
						t.Fatalf("native diagnostic classes: %+v %v", projection, err)
					}
					items, err := reader.InspectDynamicFlowRuntimeReadinessForRun(f.ctx, runID, current)
					if err != nil || len(items) != 1 || items[0].Plan.RunID != runID {
						t.Fatalf("exact run diagnostic: %+v %v", items, err)
					}
					foreign := sourceartifactfixture.FactFor(sourceartifactfixture.New("schema.yaml", []byte("name: unrelated-readiness-source\n")))
					if items, err := reader.InspectDynamicFlowRuntimeReadinessForRun(f.ctx, runID, foreign); err != nil || len(items) != 0 {
						t.Fatalf("foreign source crossed run diagnostic: %+v %v", items, err)
					}
					if after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store); err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("diagnostics changed durable evidence: %v", err)
					}
				})
			}
		})
	}
}

func TestR7ReadinessDiagnosticsRejectModeKeyCorruptionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, target := range []struct {
			name, mode string
			index      int
		}{{"keyless_as_template", "template", 1}, {"keyed_as_static", "static", 0}} {
			t.Run(backend+"/"+target.name, func(t *testing.T) {
				f := newEagerFlowConstructorFixture(t, backend)
				plan := eagerFlowConstructorPlan(t, f)
				committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
				if err != nil || !committed.Acknowledged || !committed.Created {
					t.Fatalf("canonical construction: %+v %v", committed, err)
				}
				instance := plan.ConstructionPlans()[target.index]
				fact, found := correlation.SourceArtifactFactFromContext(f.ctx)
				if !found {
					t.Fatal("native mode proof lost its admitted source")
				}
				reader := f.store.(pipeline.DynamicFlowRuntimeReadinessPersistence)
				for _, surface := range []string{"source", "run"} {
					if err := inspectConstructedRouteReadiness(f, reader, fact, instance.Readiness.RunID, surface); err != nil {
						t.Fatalf("healthy %s diagnostic: %v", surface, err)
					}
				}
				owner := flowidentity.RunScopedFlowInstance{RunID: instance.Readiness.RunID, Route: instance.Identity.Route()}
				restore, err := FaultFlowConstructorHeaderForTest(f.ctx, f.store, owner, pipelinepersistence.FlowConstructorHeaderFaultField("mode"), target.mode)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := restore(f.ctx); err != nil {
						t.Error(err)
					}
				}()
				before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
				if err != nil {
					t.Fatal(err)
				}
				for _, surface := range []string{"source", "run"} {
					var corruption *pipeline.FlowInstanceConstructionCorruption
					if err := inspectConstructedRouteReadiness(f, reader, fact, owner.RunID, surface); !errors.As(err, &corruption) || corruption.RunID != owner.RunID || corruption.InstancePath != owner.Route.InstancePath {
						t.Errorf("%s accepted contradictory native mode/key: %v", surface, err)
					}
				}
				if after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store); err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("mode/key refusal repaired durable evidence: %v", err)
				}
			})
		}
	}
}
