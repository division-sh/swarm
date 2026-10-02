package runtimepersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func TestFlowConstructorReadinessKeepsExactRunOwnershipBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, plan := newWorkflowTargetConstructionFixture(t, backend)
			committer := agentFixtureFlowActivationCommitter{store: f.store}
			first, err := committer.CommitFlowInstanceActivation(f.ctx, plan)
			if err != nil || !first.Acknowledged || !first.Created {
				t.Fatalf("construct first run: %+v %v", first, err)
			}
			reader := f.store.(pipeline.DynamicFlowRuntimeReadinessPersistence)
			fact, ok := correlation.SourceArtifactFactFromContext(f.ctx)
			if !ok {
				t.Fatal("missing exact source fact")
			}
			route := flowidentity.RouteForInstancePath(plan.Identity.InstancePath)
			prior, found, err := reader.LoadDynamicFlowRuntimeReadiness(f.ctx, plan.Readiness.RunID, route)
			if err != nil || !found || prior.Phase != pipeline.FlowAttachmentPlanned || !prior.CreationEventEmittedAt.IsZero() {
				t.Fatalf("initial readiness: %+v found=%t err=%v", prior, found, err)
			}
			runs := f.store.(runlifecycle.OperationOwner)
			if _, _, err := runs.MarkTerminalRun(f.ctx, runlifecycle.TerminalRequest{RunID: prior.Plan.RunID, State: runlifecycle.StateCancelled, EndedAt: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			retired, found, err := reader.LoadDynamicFlowRuntimeReadiness(f.ctx, prior.Plan.RunID, route)
			if err != nil || !found || retired.Eligible() {
				t.Fatalf("retired readiness still eligible: %+v found=%t err=%v", retired, found, err)
			}
			nextID := uuid.NewString()
			nextCtx := correlation.WithRunID(f.ctx, nextID)
			requireRunFixtureForTest(t, nextCtx, f.store, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: nextID, Artifact: f.bundle.SourceArtifact, BundleHash: fact.BundleHash()})
			nextPlan, err := f.manager.PrepareFlowInstanceActivation(nextCtx, sqliteFlowActivationRequest(f.bundle, "review", "review", "", "review"))
			if err != nil {
				t.Fatal(err)
			}
			next, err := committer.CommitFlowInstanceActivation(nextCtx, nextPlan)
			if err != nil || !next.Acknowledged || !next.Created {
				t.Fatalf("construct successor run: %+v %v", next, err)
			}
			unchanged, found, err := reader.LoadDynamicFlowRuntimeReadiness(f.ctx, prior.Plan.RunID, route)
			if err != nil || !found || !reflect.DeepEqual(retired, unchanged) {
				t.Fatalf("successor changed prior readiness: %+v found=%t err=%v", unchanged, found, err)
			}
			for _, status := range []runlifecycle.State{runlifecycle.StateRunning, runlifecycle.StatePaused, runlifecycle.StateCancelled} {
				if status == runlifecycle.StatePaused {
					if _, err := runs.TransitionActiveRun(nextCtx, runlifecycle.ActiveTransitionRequest{RunID: nextID, State: status}); err != nil {
						t.Fatal(err)
					}
				} else if status == runlifecycle.StateCancelled {
					if _, _, err := runs.MarkTerminalRun(nextCtx, runlifecycle.TerminalRequest{RunID: nextID, State: status, EndedAt: time.Now().UTC()}); err != nil {
						t.Fatal(err)
					}
				}
				projection, err := reader.InspectDynamicFlowRuntimeReadinessForSource(nextCtx, fact)
				current := append(projection.CurrentPending, projection.CurrentCompleted...)
				if err != nil {
					t.Fatal(err)
				}
				if status == runlifecycle.StateCancelled {
					if len(current) != 0 {
						t.Fatalf("terminal run exposed pending readiness: %+v", current)
					}
				} else if len(current) != 1 || current[0].Plan.RunID != nextID || current[0].Phase != pipeline.FlowAttachmentPlanned {
					t.Fatalf("%s projection lost exact successor: %+v", status, current)
				}
			}
			before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
			refused, err := committer.CommitFlowInstanceActivation(nextCtx, nextPlan)
			if err == nil || refused.Created || !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
				t.Fatalf("terminal replay changed construction: %+v err=%v", refused, err)
			}
		})
	}
}
