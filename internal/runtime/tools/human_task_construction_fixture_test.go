package tools_test

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
)

type humanTaskConstructionLoader struct{ persistence pipeline.WorkflowPersistence }

func (l humanTaskConstructionLoader) Load(ctx context.Context, owner flowidentity.RunScopedFlowInstance) (pipeline.WorkflowInstance, bool, error) {
	return l.persistence.LoadWorkflowInstance(ctx, owner)
}

// This supplies canonical component construction to the real tool/store test;
// it does not assert installed topology, live agent admission or provider proof.
func humanTaskConstructedRequester(t *testing.T, ctx context.Context, selected humanTaskToolStore, source semanticview.Source, actor actors.AgentConfig) (actors.AgentConfig, humanTaskConstructionLoader) {
	t.Helper()
	runID := correlation.RunIDFromContext(ctx)
	instance, err := flowidentity.StandingForGeneration(source, actor.FlowID, runID)
	if err != nil {
		t.Fatal(err)
	}
	actor.EntityID = instance.EntityID
	actor.Identity = agentidentitytest.DeclaredForRun(t, runID, actor.Identity.AgentID(), actor.Identity.Name.Owner, instance.ScopeKey, instance.InstanceID, instance.InstancePath)
	at := time.Now().UTC().Truncate(time.Microsecond)
	header := pipeline.WorkflowInstance{WorkflowName: instance.TemplateID, WorkflowVersion: source.WorkflowVersion(),
		StorageRef: instance.InstancePath, InstanceID: instance.InstanceID, EntityID: instance.EntityID,
		ParentFlowID: instance.ParentRoute.FlowID, ParentFlowInstance: instance.ParentRoute.FlowInstance, ParentEntityID: instance.ParentEntityID,
		CurrentState: "queued", Status: "active", CreatedAt: at, UpdatedAt: at, EnteredStageAt: at,
		Fields: map[string]any{}, Config: map[string]any{},
	}
	command, err := flowactivationfixture.Command(effects.WithExecutionMode(ctx, executionmode.Live), header, pipeline.WorkflowLifecycleMutationPlan{}, at)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, command)
	if err != nil || !committed.Acknowledged {
		t.Fatalf("requester component construction: acknowledged=%v err=%v", committed.Acknowledged, err)
	}
	return actor, humanTaskConstructionLoader{pipeline.NewWorkflowPersistence(selected.(pipeline.WorkflowPersistenceOwner))}
}

func humanTaskImportedSource(t *testing.T, actor actors.AgentConfig) semanticview.Source {
	t.Helper()
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"gateway": {SchemaYAML: "name: gateway\n"},
		"gateway/provider": {
			SchemaYAML:   "name: provider\nstages:\n  queued: {initial: true}\n",
			EntitiesYAML: "provider_record:\n  status: text\n", AgentsYAML: entityToolAgentYAML(actor),
		},
	})
	return semanticview.Wrap(bundle)
}
