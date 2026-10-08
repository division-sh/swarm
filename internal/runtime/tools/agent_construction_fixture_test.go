package tools

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Component fixtures use the real constructor, but this loader is not live
// agent admission or selected-store execution proof.
func toolTestConstructedActor(t testing.TB, exec *Executor, actor actors.AgentConfig) actors.AgentConfig {
	t.Helper()
	schema, found := exec.workflowSource.FlowSchemaByID(actor.FlowID)
	if !found {
		t.Fatalf("producer fixture requires declared flow %s", actor.FlowID)
	}
	instance := flowidentity.Derive(exec.workflowSource, actor.FlowID, actor.Identity.Route.InstanceID)
	if schema.Instance.Empty() {
		var err error
		instance, err = flowidentity.StandingForGeneration(exec.workflowSource, actor.FlowID, actor.Identity.RunID)
		if err != nil {
			t.Fatal(err)
		}
		if actor.EntityID != "" {
			actor.EntityID = instance.EntityID
		}
	} else {
		bundle, found := semanticview.Bundle(exec.workflowSource)
		if !found {
			t.Fatal("producer fixture requires its admitted flow tree")
		}
		view, found := bundle.FlowViewByID(actor.FlowID)
		if !found || view.Parent == nil {
			t.Fatal("producer fixture requires its exact parent declaration")
		}
		parent, err := flowidentity.StandingForGeneration(exec.workflowSource, view.Parent.Paths.FlowPath, actor.Identity.RunID)
		if err != nil {
			t.Fatal(err)
		}
		instance, err = flowidentity.KeyedChild(exec.workflowSource, parent, actor.FlowID, actor.Identity.Route.InstanceID)
		if err != nil {
			t.Fatal(err)
		}
		if actor.EntityID != "" {
			instance.EntityID = actor.EntityID
		}
	}
	if err := instance.ValidateConstruction(exec.workflowSource, actor.Identity.RunID); err != nil {
		t.Fatal(err)
	}
	if instance.InstancePath != actor.FlowPath {
		t.Fatalf("producer fixture route %s conflicts with constructed %s", actor.FlowPath, instance.InstancePath)
	}
	exec.workflowInstances = emitWorkflowInstanceLoader{rows: map[string]pipeline.WorkflowInstance{instance.InstancePath: {
		WorkflowName: instance.TemplateID, InstanceID: instance.InstanceID, StorageRef: instance.InstancePath, EntityID: instance.EntityID,
		ParentFlowID: instance.ParentRoute.FlowID, ParentFlowInstance: instance.ParentRoute.FlowInstance, ParentEntityID: instance.ParentEntityID,
	}}}
	return actor
}
