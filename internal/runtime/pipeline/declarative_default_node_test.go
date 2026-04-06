package pipeline

import (
	"context"
	"testing"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
)

func TestCoordinatorHandlerExecutionEngineUsesRuntimeEnginePath(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := NewPipelineCoordinatorWithOptions(bus, nil, PipelineCoordinatorOptions{
		Module: NewGenericTestWorkflowModule(),
	})

	engine := newCoordinatorHandlerExecutionEngine(pc, "node-a")
	if engine == nil {
		t.Fatal("expected engine")
	}
	outcome, err := engine.ExecuteHandlerSteps(context.Background(), runtimecontracts.SystemNodeEventHandler{
		Emits: runtimecontracts.EventEmission{Single: "custom.emitted"},
	}, events.Event{Type: events.EventType("custom.trigger")}.WithEntityID("ent-1"))
	if err != nil {
		t.Fatalf("ExecuteHandlerSteps: %v", err)
	}
	if outcome == nil || !outcome.Handled {
		t.Fatalf("handled outcome = %#v", outcome)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	if got := string(bus.publishedEvent(0).Type); got != "custom.emitted" {
		t.Fatalf("published event type = %q, want custom.emitted", got)
	}
}

func TestEnsureHandlerEntityIDMintsForEntityMaterializingHandler(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			EntitySchema: runtimecontracts.EntitySchema{
				Groups: []runtimecontracts.EntitySchemaGroup{
					{
						Name: "identity",
						Fields: []runtimecontracts.EntitySchemaField{
							{Name: "name", Type: "text"},
						},
					},
				},
			},
		},
	})
	handler := runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "name", Value: runtimecontracts.LiteralExpression("Minted Entity")},
			},
		},
	}

	entityID, evt := ensureHandlerEntityID(source, handler, "", events.Event{Type: events.EventType("custom.trigger")})

	if entityID == "" {
		t.Fatal("expected minted entity_id")
	}
	if got := evt.EntityID(); got == "" || got != entityID {
		t.Fatalf("event entity_id = %q, want %q", got, entityID)
	}
}

func TestEnsureHandlerEntityIDCreateEntityKeepsInboundEventReference(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{CreateEntity: true}
	inbound := events.Event{Type: events.EventType("custom.trigger")}.WithEntityID("ent-parent")

	entityID, evt := ensureHandlerEntityID(nil, handler, "ent-parent", inbound)

	if entityID == "" || entityID == "ent-parent" {
		t.Fatalf("entityID = %q, want fresh id", entityID)
	}
	if got := evt.EntityID(); got != "ent-parent" {
		t.Fatalf("event entity_id = %q, want preserved inbound reference", got)
	}
}
