package pipeline

import (
	"context"
	"testing"

	"empireai/internal/events"
	"empireai/internal/runtime/core/paths"
)

func TestCreateFlowInstanceResolvesInstanceIDFromPayloadPath(t *testing.T) {
	var captured FlowInstanceActivationRequest
	pc := &FactoryPipelineCoordinator{
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
	if !ok {
		t.Fatal("expected createFlowInstance to succeed")
	}
	if captured.InstanceID != "inst-42" {
		t.Fatalf("instance id = %q, want inst-42", captured.InstanceID)
	}
}
