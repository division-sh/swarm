package pipelineobligation

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/google/uuid"
)

func TestExecutionOutcomeRetainsStageWithoutChangingSettlementRights(t *testing.T) {
	stage := engine.CommittedStage{Instance: flowidentity.RunScopedFlowInstance{RunID: uuid.NewString(), Route: flowidentity.RouteForInstancePath("orders")}, EntityID: uuid.NewString(), Stage: "ready", StageDefined: true, Revision: 4, UpdatedAt: time.Now().UTC()}
	ack, err := (ExecutionOutcome{Committed: true}).WithCommittedStage(uuid.NewString(), stage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Continue().WithCommittedStage(uuid.NewString(), stage); err == nil {
		t.Fatal("unacknowledged result manufactured a stage receipt")
	}
	for _, parent := range []ExecutionOutcome{Continue(), DeadLetterExecution("terminal", nil), ReleaseForRetry("not_ready", nil)} {
		beforeDisposition, beforeHasDisposition := parent.Disposition()
		beforeRetry, beforeHasRetry := parent.RetryRelease()
		combined := parent.RetainStageReceipts(ack).RetainStageReceipts(ack)
		afterDisposition, afterHasDisposition := combined.Disposition()
		afterRetry, afterHasRetry := combined.RetryRelease()
		if combined.Committed || beforeHasDisposition != afterHasDisposition || beforeDisposition.Kind() != afterDisposition.Kind() || beforeHasRetry != afterHasRetry || beforeRetry.ReasonCode() != afterRetry.ReasonCode() {
			t.Fatal("receipt retention changed the parent's settlement rights")
		}
		receipts := combined.StageReceipts()
		if len(receipts) != 1 || receipts[0].Stage() != stage {
			t.Fatalf("exact stage receipt lost or duplicated: %+v", receipts)
		}
		receipts[0] = CommittedStageReceipt{}
		if combined.StageReceipts()[0].Stage() != stage {
			t.Fatal("receipt reader mutated the retained evidence")
		}
	}
}
