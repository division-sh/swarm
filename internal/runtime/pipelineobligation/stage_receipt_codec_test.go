package pipelineobligation

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/google/uuid"
)

func TestCommittedStageReceiptDurableCodecRejectsIncompleteEvidence(t *testing.T) {
	stage := engine.CommittedStage{Instance: flowidentity.RunScopedFlowInstance{RunID: uuid.NewString(), Route: flowidentity.RouteForInstancePath("orders/one")}, EntityID: uuid.NewString(), Stage: "done", StageDefined: true, Revision: 3, UpdatedAt: time.Now().UTC()}
	outcome, err := (ExecutionOutcome{Committed: true}).WithCommittedStage(uuid.NewString(), stage)
	if err != nil {
		t.Fatal(err)
	}
	receipt := outcome.StageReceipts()[0]
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	var restored CommittedStageReceipt
	if err := json.Unmarshal(raw, &restored); err != nil || restored != receipt {
		t.Fatalf("durable stage evidence did not round trip: %s err=%v", raw, err)
	}
	for _, invalid := range []string{"{}", "null", string(raw) + `{}`, strings.Replace(string(raw), `"revision":3`, `"revision":0`, 1), strings.Replace(string(raw), `"version":1`, `"version":2`, 1), strings.Replace(string(raw), `"stage_defined":true,`, "", 1), strings.Replace(string(raw), `"stage_defined":true`, `"stage_defined":null`, 1)} {
		if err := json.Unmarshal([]byte(invalid), &restored); err == nil {
			t.Fatalf("invalid stage evidence admitted: %s", invalid)
		}
	}
	if _, err := json.Marshal(CommittedStageReceipt{}); err == nil {
		t.Fatal("missing committed evidence serialized as a receipt")
	}
}
