package pipelinepersistence

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestGroupInstancePlanningInventoryKeepsImmutableFactsAndExactReadStrength(t *testing.T) {
	for _, change := range []string{"stage", "termination", "attachment", "attempt", "emitted", "agent", "creation", "native_input", "native_fields", "run_state", "membership"} {
		t.Run(change, func(t *testing.T) {
			before, after := groupReadStrengthObservations(t, change)
			scope, err := pipeline.NewFlowInstanceLookupScope(before.Selection().Source(), before.SourceFact(), before.Owner().RunID, []string{"."}, nil)
			if err != nil {
				t.Fatal(err)
			}
			previous, current := []pipeline.FlowInstanceObservation{before}, []pipeline.FlowInstanceObservation{after}
			if change == "membership" {
				current = nil
			}
			reads := flowInstancePlanningReadSet{reads: []flowInstancePlanningRead{{scope: &scope, observations: previous}}}
			snapshot := flowInstanceIndexSnapshot{source: scope.Source(), run: runlifecycle.Snapshot{RunID: scope.RunID()}, declarations: map[string][]pipeline.FlowInstanceObservation{".": current}}
			changed, err := reads.reobserve(context.Background(), &snapshot)
			mutable := change == "stage" || change == "termination" || change == "attachment" || change == "attempt" || change == "emitted"
			if err != nil || changed == mutable {
				t.Fatalf("inventory change=%s stale=%v mutable=%v err=%v", change, changed, mutable, err)
			}
			// The same observation, when actually looked up, retains its full
			// availability/readiness dependency instead of borrowing list strength.
			if same, err := sameFlowInstancePlanningObservations(previous, current, true); err != nil || same {
				t.Fatalf("exact read lost %s: same=%v err=%v", change, same, err)
			}
		})
	}
}

func TestGroupInstancePlanningOverlappingRequestsKeepBothReadStrengths(t *testing.T) {
	before, _ := groupReadStrengthObservations(t, "stage")
	request := before.Selection()
	scope, err := pipeline.NewFlowInstanceLookupScope(request.Source(), request.SourceFact(), request.RunID(), []string{"."}, []flowidentity.RunScopedFlowInstance{before.Owner()})
	if err != nil {
		t.Fatal(err)
	}
	for _, lookupFirst := range []bool{false, true} {
		reads := flowInstancePlanningReadSet{}
		inventory := flowInstancePlanningRead{scope: &scope, observations: []pipeline.FlowInstanceObservation{before}}
		exact := flowInstancePlanningRead{lookup: &request, observations: []pipeline.FlowInstanceObservation{before}}
		for range 4 {
			if lookupFirst {
				reads.record(exact)
				reads.record(inventory)
			} else {
				reads.record(inventory)
				reads.record(exact)
			}
		}
		if len(reads.reads) != 2 || (reads.reads[0].lookup != nil) != lookupFirst || (reads.reads[1].lookup != nil) == lookupFirst {
			t.Fatalf("lookupFirst=%v lost independent exact/list capture: %+v", lookupFirst, reads.reads)
		}
	}
}

func groupReadStrengthObservations(t *testing.T, change string) (pipeline.FlowInstanceObservation, pipeline.FlowInstanceObservation) {
	t.Helper()
	artifact := sourceartifactfixture.Artifact()
	root := &contracts.FlowContractView{Paths: contracts.FlowContractPaths{FlowPath: "."}}
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{
		SourceArtifact: artifact, Semantics: contracts.WorkflowSemanticView{Version: "original"},
		RootSchema: &contracts.FlowSchemaDocument{},
		FlowTree:   contracts.FlowTree{Root: root, ByID: map[string]*contracts.FlowContractView{".": root}},
	})
	fact := sourceartifactfixture.FactFor(artifact)
	const runID = "11111111-1111-1111-1111-111111111111"
	identity, err := flowidentity.StandingForGeneration(source, ".", runID)
	if err != nil {
		t.Fatal(err)
	}
	request, err := pipeline.NewExactFlowInstanceLookup(source, fact, flowidentity.RunScopedFlowInstance{RunID: runID, Route: identity.Route()})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	instance := pipeline.WorkflowInstance{WorkflowName: ".", WorkflowVersion: "original", Mode: "static", Status: "active", InstanceID: runID, StorageRef: runID, EntityID: runID, CurrentState: "pending", StageDefined: true, Revision: 1, CreatedAt: at, UpdatedAt: at}
	run := runlifecycle.Snapshot{RunID: runID, State: runlifecycle.StateRunning, Origin: runlifecycle.DeploymentRunOrigin(), BundleHash: fact.BundleHash(), StartedAt: at}
	receipt := pipeline.FlowConstructionPublicationEvidence{Identity: identity, WorkflowVersion: "original", BundleHash: fact.BundleHash(), Fields: map[string]any{"seed": "original"}}
	readiness := pipeline.DynamicFlowRuntimeReadiness{Plan: pipeline.DynamicFlowRuntimeReadinessPlan{Identity: identity, RunID: runID, BundleHash: fact.BundleHash(), WorkflowVersion: "original", ExecutionMode: executionmode.Live}, OwningRunSource: fact, RunStatus: "running", InstanceStatus: "active", Phase: pipeline.FlowAttachmentReady, AttemptState: "accepted", AttemptOrdinal: 1}
	admit := func() pipeline.FlowInstanceObservation {
		t.Helper()
		plan, err := readiness.Plan.Normalized()
		if err != nil {
			t.Fatal(err)
		}
		readiness.Plan = plan
		readiness.PlanHash, err = plan.Hash()
		if err != nil {
			t.Fatal(err)
		}
		observation, err := pipeline.AdmitNativeFlowInstanceObservation(request, instance, run, 1, receipt, readiness)
		if err != nil {
			t.Fatal(err)
		}
		return observation
	}
	before := admit()
	switch change {
	case "stage":
		instance.CurrentState = "completed"
	case "termination":
		instance.Status, readiness.InstanceStatus = "terminated", "terminated"
		instance.TerminatedAt, readiness.InstanceTerminatedAt = at.Add(time.Second), at.Add(time.Second)
	case "attachment":
		readiness.Phase = pipeline.FlowAttachmentPlanned
	case "attempt":
		readiness.AttemptOrdinal++
		readiness.AttemptState = "retired"
	case "emitted":
		readiness.CreationEventEmittedAt = at.Add(time.Second)
	case "agent":
		name, err := agentidentity.DeclaredName("worker", ".")
		if err != nil {
			t.Fatal(err)
		}
		route, err := identity.Route().AgentIdentityRoute()
		if err != nil {
			t.Fatal(err)
		}
		agent, err := agentidentity.New(runID, name, route)
		if err != nil {
			t.Fatal(err)
		}
		readiness.Plan.Agents = []pipeline.DynamicFlowRuntimeAgentExpectation{{Identity: agent, ConfigRevision: strings.Repeat("a", 64), EntityID: runID}}
	case "creation":
		readiness.Plan.CreationEvent = &pipeline.DynamicFlowRuntimeCreationEventPlan{EventID: "22222222-2222-2222-2222-222222222222", EventType: "created", RunID: runID, ParentEventID: "parent", ExecutionMode: executionmode.Live, CreatedAt: at, Payload: []byte(`{}`)}
	case "native_input":
		receipt.CreatingInput = pipeline.FlowConstructionInput{EventID: "22222222-2222-2222-2222-222222222222", Input: "changed"}
	case "native_fields":
		receipt.Fields["seed"] = "changed"
	case "run_state":
		run.State = runlifecycle.StatePaused
		readiness.RunStatus = "paused"
	}
	return before, admit()
}

func TestGroupInstancePlanningInventoryRetainsFixedHistoricalConstruction(t *testing.T) {
	for _, change := range []string{"revision", "source_owner", "version"} {
		t.Run(change, func(t *testing.T) {
			native, _ := groupReadStrengthObservations(t, "stage")
			instance, err := native.WorkflowInstance()
			if err != nil {
				t.Fatal(err)
			}
			const sourceRun = "22222222-2222-2222-2222-222222222222"
			history := pipeline.HistoricalFlowInstanceConstruction{Identity: native.Identity(), SourceOwner: flowidentity.RunScopedFlowInstance{RunID: sourceRun, Route: flowidentity.StoredRoute(".", sourceRun, sourceRun)}, SourceRevision: 2, WorkflowVersion: instance.WorkflowVersion}
			admit := func() pipeline.FlowInstanceObservation {
				t.Helper()
				origin, err := runlifecycle.ForkMaterializationRunOrigin(history.SourceOwner.RunID, runlifecycle.ForkOriginPointDeploymentRevision, history.SourceRevision, "")
				if err != nil {
					t.Fatal(err)
				}
				run := runlifecycle.Snapshot{RunID: native.Owner().RunID, State: native.RunState(), Origin: origin, BundleHash: native.SourceFact().BundleHash(), StartedAt: instance.CreatedAt}
				observation, err := pipeline.AdmitHistoricalFlowInstanceObservation(native.Selection(), instance, run, 3, history, nil)
				if err != nil {
					t.Fatal(err)
				}
				return observation
			}
			before := admit()
			switch change {
			case "revision":
				history.SourceRevision++
			case "source_owner":
				const otherRun = "33333333-3333-3333-3333-333333333333"
				history.SourceOwner = flowidentity.RunScopedFlowInstance{RunID: otherRun, Route: flowidentity.StoredRoute(".", otherRun, otherRun)}
			case "version":
				history.WorkflowVersion, instance.WorkflowVersion = "captured-version", "captured-version"
			}
			after := admit()
			for _, exact := range []bool{false, true} {
				if same, err := sameFlowInstancePlanningObservations([]pipeline.FlowInstanceObservation{before}, []pipeline.FlowInstanceObservation{after}, exact); err != nil || same {
					t.Fatalf("exact=%v lost fixed %s: same=%v err=%v", exact, change, same, err)
				}
			}
		})
	}
}
