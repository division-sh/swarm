package runtime_test

import (
	"context"
	"testing"
	"time"

	swarmruntime "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
)

func init() {
	swarmruntime.InboundTransportNativeIndexFixture = nativeInboundTransportIndex
}

func nativeInboundTransportIndex(t testing.TB, source semanticview.Source, targets []swarmruntime.InboundTarget) runtimepipeline.FlowInstanceIndexReader {
	t.Helper()
	bundle, present := semanticview.Bundle(source)
	if !present || bundle.SourceArtifact == nil {
		t.Fatal("transport index requires its admitted source")
	}
	fact := runtimeTestSourceArtifactFact(t, source)
	selected := storetest.StartSQLiteRuntimeStore(t)
	seen := make(map[string]bool)
	for _, target := range targets {
		ctx := runtimecorrelation.WithRunID(runtimecorrelation.WithSourceArtifactFact(context.Background(), fact), target.RunID)
		ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fact.BundleHash()))
		ctx = effects.WithExecutionMode(ctx, executionmode.Live)
		if !seen[target.RunID] {
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(),
				RunID: target.RunID, BundleHash: fact.BundleHash(), Artifact: bundle.SourceArtifact})
		}
		root := runtimeflowidentity.Stored(source, ".", target.RunID, target.RunID, target.RunID, "")
		child, err := runtimeflowidentity.KeylessChild(source, root, target.FlowPath)
		if err != nil {
			t.Fatal(err)
		}
		for _, identity := range []runtimeflowidentity.Instance{root, child} {
			if seen[target.RunID+"/"+identity.InstancePath] {
				continue
			}
			schema, _ := source.FlowSchemaByID(identity.TemplateID)
			stages, ok := semanticview.WorkflowStageTopology(source, identity.TemplateID)
			if !ok {
				t.Fatal("transport fixture has no compiled stage catalog")
			}
			initial, err := stages.InitialStoredStage()
			if err != nil {
				t.Fatal(err)
			}
			at := time.Now().UTC()
			instance := runtimepipeline.WorkflowInstance{InstanceID: identity.InstanceID, StorageRef: identity.InstancePath,
				EntityID: identity.EntityID, WorkflowName: identity.TemplateID, WorkflowVersion: source.WorkflowVersion(), Mode: schema.EffectiveMode(),
				ParentFlowID: identity.ParentRoute.FlowID, ParentFlowInstance: identity.ParentRoute.FlowInstance, ParentEntityID: identity.ParentEntityID,
				CurrentState: initial.ID(), StageDefined: stages.StageCount() != 0, Fields: map[string]any{}, CreatedAt: at, EnteredStageAt: at}
			command, err := flowactivationfixture.Command(ctx, instance, runtimepipeline.WorkflowLifecycleMutationPlan{}, at)
			if err != nil {
				t.Fatal(err)
			}
			result, err := selected.CommitFlowInstanceActivation(ctx, command)
			if err != nil || !result.Acknowledged || !result.Created {
				t.Fatalf("construct transport fixture %s: acknowledged=%t created=%t: %v", identity.InstancePath, result.Acknowledged, result.Created, err)
			}
			seen[target.RunID+"/"+identity.InstancePath] = true
		}
		seen[target.RunID] = true
	}
	return selected
}
