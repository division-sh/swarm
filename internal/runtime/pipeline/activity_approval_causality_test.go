package pipeline

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestApprovedActivityKeepsIdentityAndOwnPhysicalDecisionParent(t *testing.T) {
	intent := testActivityIntent("https://example.com/retained-input")
	ordinary, err := activityRequestEmitIntentFromAdmittedSource(intent)
	if err != nil {
		t.Fatal(err)
	}
	decisionID := uuid.NewString()
	approved, err := activityRequestEmitIntentWithParent(intent, decisionID)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := activityIntentFromRequestEvent(approved.Event)
	if err != nil || approved.Event.ID() != ordinary.Event.ID() || approved.Event.ParentEventID() != decisionID ||
		!reflect.DeepEqual(approved.Event.Payload(), ordinary.Event.Payload()) || !reflect.DeepEqual(decoded, intent) {
		t.Fatalf("approval changed immutable activity identity/input: decoded=%+v error=%v", decoded, err)
	}
	bus := &recordingPipelineBus{}
	coordinator := newPreviewPipelineCoordinatorForTest(bus, PipelineCoordinatorOptions{Module: staticSemanticWorkflowModule{source: semanticview.Wrap(&contracts.WorkflowContractBundle{})}})
	dispatcher := pipelineActivityDispatcher{coordinator: coordinator, publicationParentEventID: approved.Event.ParentEventID()}
	payload := activitySuccessPayload(intent, map[string]any{"title": "approved"})
	if err := dispatcher.publishActivityResult(context.Background(), intent, intent.SuccessEvent, payload); err != nil {
		t.Fatal(err)
	}
	if len(bus.publishes) != 1 || bus.publishes[0].ParentEventID() != decisionID {
		t.Fatalf("approved result fabricated a projected input parent: %+v", bus.publishes)
	}
	lineage := ActivityRequestLineage{request: approved.Event, intent: intent}
	if err := lineage.ValidateResult(bus.publishes[0]); err != nil {
		t.Fatalf("physical decision lineage refused its own result: %v", err)
	}
	ordinaryLineage := ActivityRequestLineage{request: ordinary.Event, intent: intent}
	if err := ordinaryLineage.ValidateResult(bus.publishes[0]); err == nil {
		t.Fatal("a different physical cause admitted the result")
	}
}
