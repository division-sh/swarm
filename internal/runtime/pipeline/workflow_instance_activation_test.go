package pipeline

import (
	"context"
	"path/filepath"
	"testing"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/paths"
	"swarm/internal/runtime/semanticview"
)

func TestCreateFlowInstanceResolvesInstanceIDFromPayloadPath(t *testing.T) {
	var captured FlowInstanceActivationRequest
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			captured = req
			return nil
		},
	}
	trigger := (events.Event{
		Type:    events.EventType("custom.triggered"),
		Payload: []byte(`{"entity_id":"ent-1","desired_instance_id":"inst-42"}`),
	}).WithEntityID("ent-1")

	ok := pc.createFlowInstance(context.Background(), workflowTriggerContext{Event: trigger}, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.desired_instance_id",
		InstanceIDPath: paths.Parse("payload.desired_instance_id"),
	})
	if ok != nil {
		t.Fatalf("expected createFlowInstance to succeed: %v", ok)
	}
	if captured.InstanceID != "inst-42" {
		t.Fatalf("instance id = %q, want inst-42", captured.InstanceID)
	}
}

func TestCreateFlowInstanceResolvesConfigFromBindings(t *testing.T) {
	var captured FlowInstanceActivationRequest
	pc := &PipelineCoordinator{
		instanceActivator: func(_ context.Context, req FlowInstanceActivationRequest) error {
			captured = req
			return nil
		},
	}
	trigger := (events.Event{
		Type:    events.EventType("spawn.requested"),
		Payload: []byte(`{"entity_id":"ent-1","instance_id":"inst-42","name":"alpha","priority":1}`),
	}).WithEntityID("ent-1")

	err := pc.createFlowInstance(context.Background(), workflowTriggerContext{Event: trigger}, handlerExecutionPlan{
		Template:       "review",
		InstanceIDFrom: "payload.instance_id",
		InstanceIDPath: paths.Parse("payload.instance_id"),
		ConfigFrom: &runtimecontracts.ConfigFromSpec{
			Bindings: map[string]string{
				"name":     "payload.name",
				"priority": "payload.priority",
			},
		},
	})
	if err != nil {
		t.Fatalf("expected createFlowInstance to succeed: %v", err)
	}
	if captured.Config["name"] != "alpha" {
		t.Fatalf("config name = %#v, want alpha", captured.Config["name"])
	}
	if captured.Config["priority"] != float64(1) && captured.Config["priority"] != 1 {
		t.Fatalf("config priority = %#v, want 1", captured.Config["priority"])
	}
}

func TestWorkflowEmitTargetsParentEntity_OutputPin(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-child-flow-local-events")
	if !workflowEmitTargetsParentEntity(source, "child", "child/child.done") {
		t.Fatal("expected child/child.done to target the parent entity")
	}
	if workflowEmitTargetsParentEntity(source, "child", "child/child.internal") {
		t.Fatal("did not expect child/child.internal to target the parent entity")
	}

	pinSource := loadWorkflowFixtureSource(t, "test-child-flow-pin-wiring")
	if !workflowEmitTargetsParentEntity(pinSource, "child", "child/work.completed") {
		t.Fatal("expected child/work.completed to target the parent entity")
	}
}

func TestWorkflowEmitTargetsParentEntity_DoesNotScanOtherFlows(t *testing.T) {
	source := loadWorkflowFixtureSource(t, "test-child-flow-sibling-isolation")
	if workflowEmitTargetsParentEntity(source, "flow-a", "flow-b/work.done") {
		t.Fatal("did not expect flow-b output event to retarget when executing inside flow-a")
	}
	if workflowEmitTargetsParentEntity(source, "", "flow-a/work.done") {
		t.Fatal("did not expect root/no-flow context to retarget from a child flow output declaration")
	}
}

func TestHandlerEmitPayload_UsesParentEntityForOutputPinsAndLocalEntityForInternalEvents(t *testing.T) {
	bundle := loadWorkflowFixtureBundle(t, "test-child-flow-local-events")
	module, err := newPipelineFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule: %v", err)
	}
	pc := &PipelineCoordinator{module: module}
	trigger := workflowTriggerContext{
		Event: events.Event{
			Type:    events.EventType("child/child.start"),
			Payload: []byte(`{"entity_id":"ent-child"}`),
		}.WithEntityID("ent-child"),
		State: WorkflowState{
			EntityID: "ent-child",
			Metadata: map[string]any{
				"subject_id":       "ent-parent",
				"parent_entity_id": "ent-parent",
			},
		},
	}

	internalPayload := pc.handlerEmitPayload(withPipelineFlowScope(context.Background(), "child"), trigger, "child/child.internal")
	if got := asString(internalPayload["entity_id"]); got != "ent-child" {
		t.Fatalf("internal payload entity_id = %q, want ent-child", got)
	}

	outputPayload := pc.handlerEmitPayload(withPipelineFlowScope(context.Background(), "child"), trigger, "child/child.done")
	if got := asString(outputPayload["entity_id"]); got != "ent-parent" {
		t.Fatalf("output payload entity_id = %q, want ent-parent", got)
	}

	pinBundle := loadWorkflowFixtureBundle(t, "test-child-flow-pin-wiring")
	pinModule, err := newPipelineFixtureWorkflowModule(pinBundle)
	if err != nil {
		t.Fatalf("newPipelineFixtureWorkflowModule(pin wiring): %v", err)
	}
	pinPC := &PipelineCoordinator{module: pinModule}
	pinPayload := pinPC.handlerEmitPayload(withPipelineFlowScope(context.Background(), "child"), trigger, "child/work.completed")
	if got := asString(pinPayload["entity_id"]); got != "ent-parent" {
		t.Fatalf("pin output payload entity_id = %q, want ent-parent", got)
	}
}

func loadWorkflowFixtureSource(t *testing.T, fixture string) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(loadWorkflowFixtureBundle(t, fixture))
}

func loadWorkflowFixtureBundle(t *testing.T, fixture string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", fixture)
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("load fixture bundle: %v", err)
	}
	return bundle
}
