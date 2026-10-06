package pipeline

import (
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestWorkflowJoinAdmissionUsesConstructedDescendantCoordinate(t *testing.T) {
	files := workflowJoinLifecycleFixtureFiles(false, "")
	files["orders/child/schema.yaml"] = "name: child\nstages:\n  awaiting: {}\n  ready: {final: true}\n  attention: {final: true}\n"
	for _, name := range []string{"entities.yaml", "events.yaml", "types.yaml", "nodes.yaml"} {
		files["orders/child/"+name] = files["orders/"+name]
	}
	source := semanticview.Wrap(loadWorkflowTempBundle(t, files))
	runID := uuid.NewString()
	parent := flowidentity.Stored(source, "orders", "orders/one", "one", FlowInstanceEntityID("orders/one"), "")
	child, err := flowidentity.KeylessChild(source, parent, "orders/child")
	if err != nil {
		t.Fatal(err)
	}
	target := events.RouteIdentity{FlowID: child.TemplateID, FlowInstance: child.InstancePath, EntityID: child.EntityID}
	owner, err := WorkflowJoinAdmissionOwner(source, runID, target)
	if err != nil || owner.Route != child.Route() || owner.RunID != runID {
		t.Fatalf("constructed descendant owner = %#v err=%v", owner, err)
	}
	instance := WorkflowInstance{WorkflowName: child.TemplateID, InstanceID: child.InstanceID,
		StorageRef: child.InstancePath, EntityID: child.EntityID, ParentFlowID: child.ParentRoute.FlowID,
		ParentFlowInstance: child.ParentRoute.FlowInstance, ParentEntityID: child.ParentEntityID}
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPipelineNode("orders/child", "join-node")),
		Target: events.MustExistingEntityTarget(target)}
	// The exact header exists, but missing entry evidence must not become early
	// admission. A declaration-prefix lookup must not reject its concrete path.
	if _, _, err := PrepareWorkflowJoinAdmission(source, runID, "item.completed", route, &instance); err == nil || err.Error() != "existing join receiver is missing lifecycle entry evidence" {
		t.Fatalf("exact constructed receiver did not reach entry admission: %v", err)
	}
	for _, mutation := range []struct {
		name  string
		apply func(*WorkflowInstance)
	}{
		{"wrong parent", func(i *WorkflowInstance) { i.ParentFlowInstance = "orders/two" }},
		{"missing parent", func(i *WorkflowInstance) { i.ParentFlowID, i.ParentFlowInstance, i.ParentEntityID = "", "", "" }},
		{"foreign owner", func(i *WorkflowInstance) { i.WorkflowName = "orders" }},
		{"foreign entity", func(i *WorkflowInstance) { i.EntityID = parent.EntityID }},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			corrupt := instance
			mutation.apply(&corrupt)
			if _, _, err := PrepareWorkflowJoinAdmission(source, runID, "item.completed", route, &corrupt); err == nil || err.Error() == "existing join receiver is missing lifecycle entry evidence" {
				t.Fatalf("contradictory constructed receiver reached entry admission: %v", err)
			}
		})
	}
}

func TestWorkflowJoinAdmissionOwnerRejectsForeignRootAndUnnormalizedCoordinate(t *testing.T) {
	source := workflowJoinLifecycleRootAndFlowSource(workflowJoinLifecycleBundle(t))
	runID := uuid.NewString()
	for _, target := range []events.RouteIdentity{
		{FlowID: source.WorkflowName(), FlowInstance: uuid.NewString(), EntityID: runID},
		{FlowID: "orders", FlowInstance: " orders/one", EntityID: "entity"},
		{FlowID: "missing", FlowInstance: "orders/one", EntityID: "entity"},
		{FlowID: "orders", FlowInstance: "orders/one"},
	} {
		if owner, err := WorkflowJoinAdmissionOwner(source, runID, target); err == nil {
			t.Fatalf("invalid exact lookup coordinate admitted: target=%#v owner=%#v", target, owner)
		}
	}
}
