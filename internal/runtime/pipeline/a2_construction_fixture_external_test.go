package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
)

// These explicit component inputs exercise lifecycle persistence, not public
// constructor eligibility or parent-driven eager construction.
func commitA2FixtureConstruction(t *testing.T, coordinator *pipeline.PipelineCoordinator, selected any, ctx context.Context, owner flowidentity.RunScopedFlowInstance, initial pipeline.WorkflowInstance, at time.Time) pipeline.FlowInstanceActivationPlan {
	t.Helper()
	source := coordinator.SemanticSource()
	schema, found := source.FlowSchemaByID(initial.WorkflowName)
	if !found {
		t.Fatal("component construction requires its exact declared flow")
	}
	if initial.Mode == "" {
		initial.Mode = "static"
		if !schema.Instance.Empty() {
			initial.Mode = "template"
		}
	}
	if !schema.Instance.Empty() {
		key, err := pipeline.AdmitFlowInstanceKey(source, initial.WorkflowName, initial.Fields[schema.Instance.Path()])
		if err != nil {
			t.Fatalf("admit component construction key: %v", err)
		}
		initial.InstanceKey = key
	}
	if initial.WorkflowName != semanticview.RootExecutionFlowID(source) && initial.ParentFlowID == "" && initial.ParentFlowInstance == "" && initial.ParentEntityID == "" {
		bundle, found := semanticview.Bundle(source)
		if !found {
			t.Fatal("component construction requires its admitted flow tree")
		}
		view, found := bundle.FlowViewByID(initial.WorkflowName)
		if !found || view.Parent == nil {
			t.Fatal("component construction requires its explicit child declaration")
		}
		parent, err := flowidentity.StandingForGeneration(source, view.Parent.Paths.FlowPath, owner.RunID)
		if err != nil {
			t.Fatal(err)
		}
		var constructed flowidentity.Instance
		if view.Schema.Instance.Empty() {
			constructed, err = flowidentity.KeylessChild(source, parent, initial.WorkflowName)
		} else {
			constructed, err = flowidentity.KeyedChild(source, parent, initial.WorkflowName, owner.Route.InstanceID)
		}
		if err != nil || constructed.InstancePath != initial.StorageRef {
			t.Fatalf("component constructor path: constructed=%+v err=%v", constructed, err)
		}
		initial.ParentFlowID, initial.ParentFlowInstance, initial.ParentEntityID = constructed.ParentRoute.FlowID, constructed.ParentRoute.FlowInstance, constructed.ParentEntityID
	}
	initialized, lifecycle, err := coordinator.PrepareInitialEntryLifecycle(ctx, owner, initial, at)
	if err != nil {
		t.Fatalf("prepare component lifecycle: %v", err)
	}
	command, err := flowactivationfixture.Command(ctx, initialized, lifecycle, at)
	if err != nil {
		t.Fatalf("assemble component construction: %v", err)
	}
	committed, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, command)
	if err != nil || !committed.Acknowledged {
		t.Fatalf("commit component construction: acknowledged=%v err=%v", committed.Acknowledged, err)
	}
	if committed.Created {
		if err := coordinator.FinalizeInitialEntryLifecycle(ctx, committed.Lifecycle); err != nil {
			t.Fatalf("finalize acknowledged component lifecycle: %v", err)
		}
	}
	return command.Plan
}

func nativeConstructionContextFixture(t *testing.T, selected storetest.RunFixtureStore, source semanticview.Source, runID string) (context.Context, correlation.SourceArtifactFact) {
	t.Helper()
	bundle, found := semanticview.Bundle(source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("native construction requires its exact source artifact")
	}
	fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContextForSource(t, context.Background(), fact), runID))
	if err := storetest.MaterializeRun(ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: bundle.SourceArtifact, BundleHash: fact.BundleHash()}); err != nil {
		t.Fatal(err)
	}
	return ctx, fact
}

func requireIndexedConstructionFixture(t testing.TB, ctx context.Context, persistence pipeline.WorkflowPersistence, source semanticview.Source, runID string, expected flowidentity.Instance) {
	t.Helper()
	fact, found := correlation.SourceArtifactFactFromContext(ctx)
	if !found {
		t.Fatal("construction observation requires its admitted source")
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, expected.Route())
	if err != nil {
		t.Fatal(err)
	}
	request, err := pipeline.NewExactFlowInstanceLookup(source, fact, owner)
	if err != nil {
		t.Fatal(err)
	}
	observed, found, err := persistence.LookupFlowInstance(ctx, request)
	if err != nil || !found {
		t.Fatalf("native construction observation: found=%v err=%v", found, err)
	}
	if err := observed.ValidateSelection(request); err != nil || observed.Identity() != expected {
		t.Fatalf("native construction disagrees with committed identity: actual=%+v expected=%+v err=%v", observed.Identity(), expected, err)
	}
}
