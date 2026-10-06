package bus

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

func TestPreparedPublicationStageRejectsMissingOrForeignReceipt(t *testing.T) {
	runID, entityID := uuid.NewString(), uuid.NewString()
	instance := flowidentity.RunScopedFlowInstance{RunID: runID, Route: flowidentity.RouteForInstancePath("orders/one")}
	event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "orders.done", "stage-fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, eventtest.ConcreteTemplateRoutingSource("orders", "orders/one", entityID), time.Now().UTC())
	stage := engine.CommittedStage{Instance: instance, EntityID: entityID, Stage: "ready", StageDefined: true, Revision: 7, UpdatedAt: time.Now().UTC()}
	receipt, err := pipelineobligation.StageReceiptEvidence(event.ID(), stage)
	if err != nil {
		t.Fatal(err)
	}
	prepared := PreparedPublish{Event: event, stageFeedback: &pipeline.WorkflowPublicationStageRequest{Instance: instance, EntityID: entityID}}
	if _, err := prepared.withAcceptedPublicationStage(nil); err == nil {
		t.Fatal("missing acceptance filled from current state")
	}
	foreign, err := pipelineobligation.StageReceiptEvidence(uuid.NewString(), stage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepared.withAcceptedPublicationStage(&foreign); err == nil {
		t.Fatal("another publication supplied acceptance feedback")
	}
	bound, err := prepared.withAcceptedPublicationStage(&receipt)
	if err != nil {
		t.Fatal(err)
	}
	receipt = foreign
	retained, found := bound.AcceptedPublicationStage()
	if !found || retained.EventID() != event.ID() || retained.Stage() != stage {
		t.Fatalf("handoff did not freeze acceptance evidence: %+v found=%t", retained, found)
	}
	if _, found := prepared.AcceptedPublicationStage(); found {
		t.Fatal("binding mutated an uncommitted preparation")
	}
}
