package operatorsurface

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

func TestAttributedEmissionReceiptPreservesExactProducerBinding(t *testing.T) {
	runID, eventID := uuid.NewString(), uuid.NewString()
	instance, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(".", runID, runID))
	if err != nil {
		t.Fatal(err)
	}
	stage := engine.CommittedStage{Instance: instance, EntityID: "constructed-header-entity", Stage: "waiting", StageDefined: true, Revision: 1, UpdatedAt: time.Now().UTC()}
	acceptance, err := pipelineobligation.StageReceiptEvidence(eventID, stage)
	if err != nil {
		t.Fatal(err)
	}
	receipt := pipeline.WorkflowPublicationStageEvidence{Acceptance: acceptance}
	other := instance
	other.RunID = uuid.NewString()
	for _, tc := range []struct {
		name    string
		event   string
		request pipeline.WorkflowPublicationStageRequest
		valid   bool
	}{
		{"entityless_producer", eventID, pipeline.WorkflowPublicationStageRequest{Instance: instance}, true},
		{"authored_entity", eventID, pipeline.WorkflowPublicationStageRequest{Instance: instance, EntityID: stage.EntityID}, true},
		{"foreign_entity", eventID, pipeline.WorkflowPublicationStageRequest{Instance: instance, EntityID: "another-entity"}, false},
		{"foreign_run", eventID, pipeline.WorkflowPublicationStageRequest{Instance: other}, false},
		{"foreign_occurrence", uuid.NewString(), pipeline.WorkflowPublicationStageRequest{Instance: instance}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAttributedEmissionReceipt(receipt, tc.event, tc.request)
			if tc.valid && err != nil {
				t.Fatal(err)
			}
			if !tc.valid && (err == nil || err.Error() != "accepted emission contradicts its immutable publication receipt") {
				t.Fatalf("mismatched receipt lost exact refusal: %v", err)
			}
		})
	}
	if err := validateAttributedEmissionReceipt(pipeline.WorkflowPublicationStageEvidence{}, eventID, pipeline.WorkflowPublicationStageRequest{Instance: instance}); err == nil {
		t.Fatal("missing acceptance became positive evidence")
	}
}
