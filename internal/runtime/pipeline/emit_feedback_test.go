package pipeline

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

func TestWorkflowEmitFeedbackClosedCodec(t *testing.T) {
	runID := uuid.NewString()
	receipt, err := pipelineobligation.StageReceiptEvidence(uuid.NewString(), engine.CommittedStage{
		Instance: flowidentity.RunScopedFlowInstance{RunID: runID, Route: flowidentity.RouteForInstancePath("orders/one")},
		EntityID: uuid.NewString(), Stage: "accepted", StageDefined: true, Revision: 7, UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	feedback := WorkflowEmitFeedback{Receipt: receipt, StageOrigin: EmitStageAcceptance, Dispatch: EmitDispatchQueued}
	raw, err := json.Marshal(feedback)
	if err != nil {
		t.Fatal(err)
	}
	var restored WorkflowEmitFeedback
	if err := json.Unmarshal(raw, &restored); err != nil || restored != feedback {
		t.Fatalf("feedback changed on round trip: %+v err=%v", restored, err)
	}
	for _, invalid := range []string{
		`{}`, string(raw) + `{}`, strings.Replace(string(raw), `"version":1`, `"version":2`, 1),
		strings.Replace(string(raw), `"stage_origin":"acceptance"`, `"stage_origin":"handler_commit"`, 1),
		strings.Replace(string(raw), `"dispatch":"accepted_deferred"`, `"dispatch":""`, 1),
		strings.Replace(string(raw), `"version":1`, `"extra":true,"version":1`, 1),
	} {
		if err := json.Unmarshal([]byte(invalid), &restored); err == nil {
			t.Fatalf("invalid feedback accepted: %s", invalid)
		}
		if restored != feedback {
			t.Fatal("failed decoding changed the existing result")
		}
	}
}
