package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
	outcome, err := engine.ExecuteHandlerSteps(testAuthorActivityContext(t, context.Background()), runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{Event: "custom.emitted"},
	}, eventtest.RunCreatingRootIngress("00000000-0000-0000-0000-000000000001", events.EventType("custom.trigger"), "", "", nil, 0, testPipelineRunID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Unix(1, 0).UTC()), "custom.trigger")
	if err != nil {
		t.Fatalf("ExecuteHandlerSteps: %v", err)
	}
	if outcome == nil || !outcome.Handled {
		t.Fatalf("handled outcome = %#v", outcome)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	if got := string(bus.publishedEvent(0).Type()); got != "custom.emitted" {
		t.Fatalf("published event type = %q, want custom.emitted", got)
	}
}

func TestHandlerExecutionStateSnapshotRejectsMalformedPersistedGateShape(t *testing.T) {
	_, err := handlerExecutionStateSnapshot(runtimecontracts.SystemNodeEventHandler{}, "ent-1", WorkflowState{
		EntityID: "ent-1",
		Stage:    WorkflowStateID("queued"),
		Metadata: map[string]any{"gates": "invalid"},
	}, "default", "v-test")
	if err == nil {
		t.Fatal("expected malformed persisted gates to fail closed")
	}
	if !strings.Contains(err.Error(), "metadata.gates") {
		t.Fatalf("error = %v, want metadata.gates context", err)
	}
}

func TestEnsureHandlerEntityIDUsesCanonicalPrimaryForEntityMaterializingHandler(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootEntities: runtimecontracts.EntityContractsDocument{
			"subject": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"name": {Type: "text"},
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

	entityID, evt, resolveErr := ensureHandlerEntityID(source, "", handler, "", eventtest.RunCreatingRootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if resolveErr != nil {
		t.Fatalf("ensureHandlerEntityID: %v", resolveErr)
	}

	if entityID != FlowInstanceEntityID("root") {
		t.Fatalf("entityID = %q, want canonical root primary", entityID)
	}
	if got := evt.EntityID(); got == "" || got != entityID {
		t.Fatalf("event entity_id = %q, want %q", got, entityID)
	}
}

func TestEnsureHandlerEntityIDCreateEntityUsesInboundPrimaryReference(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{CreateEntity: true}
	inbound := eventtest.RunCreatingRootIngress(
		"",
		events.EventType("custom.trigger"),
		"",
		"",
		nil,
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-parent"),
		time.Time{},
	)

	entityID, evt, resolveErr := ensureHandlerEntityID(nil, "", handler, "ent-parent", inbound)
	if resolveErr != nil {
		t.Fatalf("ensureHandlerEntityID: %v", resolveErr)
	}

	if entityID != "ent-parent" {
		t.Fatalf("entityID = %q, want inbound primary reference", entityID)
	}
	if got := evt.EntityID(); got != "ent-parent" {
		t.Fatalf("event entity_id = %q, want preserved inbound reference", got)
	}
}
