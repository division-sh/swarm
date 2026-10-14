package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
			source := semanticview.Wrap(f.bundle)
			rootRequest := sqliteFlowActivationRequest(f.bundle, ".", nextID, "", nextID)
			rootRequest.Instance = flowidentity.Stored(source, ".", nextID, nextID, nextID, "")
			rootPlan, err := f.manager.PrepareFlowInstanceActivation(nextCtx, rootRequest)
			if err != nil {
				t.Fatal(err)
			}
			root, err := committer.CommitFlowInstanceActivation(nextCtx, rootPlan)
			if err != nil || !root.Acknowledged || !root.Created {
				t.Fatalf("construct successor parent: %+v %v", root, err)
			}
			child, err := flowidentity.KeyedChild(source, root.Plan.Identity, "review", plan.Identity.InstanceID)
			if err != nil {
				t.Fatal(err)
			}
			nextRequest := sqliteFlowActivationRequest(f.bundle, "review", "review", "", "review")
			nextRequest.Instance = child
			nextRequest.ConstructorInput, nextRequest.ResolvedKey = plan.CreatingInput.Input, plan.Instance.Fields["account_id"]
			payload, err := json.Marshal(map[string]any{"account_id": nextRequest.ResolvedKey})
			if err != nil {
				t.Fatal(err)
			}
			nextRequest.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType(nextRequest.ConstructorInput), "constructor-fixture", "", payload, 0, nextID, events.EventEnvelope{}, nextRequest.OccurredAt)
			nextPlan, err := f.manager.PrepareFlowInstanceActivation(nextCtx, nextRequest)
			if err != nil {
				t.Fatal(err)
			}
			next, err := committer.CommitFlowInstanceActivation(nextCtx, nextPlan)
			if err != nil || !next.Acknowledged || !next.Created {
				t.Fatalf("construct successor run: %+v %v", next, err)
			}
			parentRoute := flowidentity.ParentRoute{FlowID: rootPlan.Identity.TemplateID, FlowInstance: rootPlan.Identity.InstancePath, EntityID: rootPlan.Identity.EntityID}
			if nextPlan.Identity.ParentRoute != parentRoute || nextPlan.Identity.ParentEntityID != rootPlan.Identity.EntityID {
				t.Fatalf("successor child lost its exact constructed parent: %+v", nextPlan.Identity)
			}
			expected := make(map[string]pipeline.DynamicFlowRuntimeReadinessPlan, 2)
			committed := []pipeline.CommittedFlowInstanceActivation{root, next}
			for index, prepared := range []pipeline.FlowInstanceActivationPlan{rootPlan, nextPlan} {
				want, err := prepared.Readiness.Normalized()
				if err != nil {
					t.Fatal(err)
				}
				if want.Identity != prepared.Identity || want.RunID != nextID || want.BundleHash != fact.BundleHash() || want.WorkflowVersion != source.WorkflowVersion() {
					t.Fatalf("prepared successor lost exact run/source/version: %+v", want)
				}
				acknowledged, err := committed[index].Plan.Readiness.Normalized()
				if err != nil || committed[index].Plan.Identity != prepared.Identity || committed[index].ReadinessAttemptOrdinal != 1 || !reflect.DeepEqual(want, acknowledged) {
					t.Fatalf("acknowledged successor differs from prepared plan: %+v err=%v", committed[index], err)
				}
				expected[prepared.Identity.InstancePath] = want
			}
			if len(expected) != 2 {
				t.Fatal("prepared successor root and child must have distinct paths")
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
				if err != nil {
					t.Fatal(err)
				}
				if status == runlifecycle.StateCancelled {
					if len(projection.CurrentPending) != 0 || len(projection.CurrentCompleted) != 0 || len(projection.SourceTransitionRequired) != 0 {
						t.Fatalf("terminal run exposed readiness: %+v", projection)
					}
					continue
				}
				if len(projection.CurrentPending) != 2 || len(projection.CurrentCompleted) != 0 || len(projection.SourceTransitionRequired) != 0 {
					t.Fatalf("%s projection lost exact pending root and child: %+v", status, projection)
				}
				seen := make(map[string]bool, 2)
				for _, current := range projection.CurrentPending {
					want, present := expected[current.InstancePath]
					if !present || seen[current.InstancePath] {
						t.Fatalf("%s projection contains unexpected or duplicate readiness: %+v", status, current)
					}
					seen[current.InstancePath] = true
					if !reflect.DeepEqual(current.Plan, want) || !current.OwningRunSource.Matches(fact) {
						t.Fatalf("%s readiness differs from exact prepared identity/plan/source: %+v want=%+v", status, current, want)
					}
					hash, err := want.Hash()
					if err != nil || current.PlanHash != hash {
						t.Fatalf("%s readiness plan fingerprint changed: %+v err=%v", status, current, err)
					}
					if current.RunStatus != string(status) || current.InstanceStatus != "active" || !current.InstanceTerminatedAt.IsZero() || current.Phase != pipeline.FlowAttachmentPlanned || current.AttemptState != "planned" || current.AttemptOrdinal != 1 || !current.CreationEventEmittedAt.IsZero() {
						t.Fatalf("%s readiness changed planned attempt/lifecycle evidence: %+v", status, current)
					}
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
