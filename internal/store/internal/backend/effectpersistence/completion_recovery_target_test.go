package effectpersistence

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func TestCompletionRecoveryRejectsForeignOrMalformedTargetEvidence(t *testing.T) {
	for _, scenario := range []string{"foreign_uuid", "malformed_evidence_uuid", "malformed_stored_uuid", "different_kind", "different_ordinal"} {
		t.Run(scenario, func(t *testing.T) {
			evidence := completionRecoveryAuthorityEvidence{ExecutionMode: "mock"}
			evidence.UsageTarget.Kind = string(runtimeeffects.UsageTargetAgentTurn)
			evidence.UsageTarget.ID = "ef3e7bbf-cb27-47bb-b089-715d34a8388c"
			recovered := completionRecoveryAttempt{
				AttemptID: "50b925a7-1672-4e7d-ab4a-770e92b95d98", OperationMode: "mock", AttemptMode: "mock",
				TargetKind: evidence.UsageTarget.Kind, TargetID: evidence.UsageTarget.ID,
			}
			switch scenario {
			case "foreign_uuid":
				recovered.TargetID = "24f17f7e-6887-4ad5-9aac-67e5d9224112"
			case "malformed_evidence_uuid":
				evidence.UsageTarget.ID = "invalid"
			case "malformed_stored_uuid":
				recovered.TargetID = "invalid"
			case "different_kind":
				recovered.TargetKind = string(runtimeeffects.UsageTargetConversationForkCompletion)
			case "different_ordinal":
				recovered.TargetOrdinal = 1
			}
			encoded, err := json.Marshal(evidence)
			if err != nil {
				t.Fatal(err)
			}
			recovered.AuthorityEvidence = string(encoded)
			attempt, settlement, err := completionRecoverySettlement(recovered, runtimeeffects.StateAuthorized, nil, time.Now().UTC())
			if err == nil || !strings.Contains(err.Error(), "completion recovery target evidence conflicts") || attempt.AttemptID != "" || settlement.AgentTurn != nil {
				t.Fatalf("target mismatch gained completion authority: attempt=%+v settlement=%+v err=%v", attempt, settlement, err)
			}
		})
	}
}
