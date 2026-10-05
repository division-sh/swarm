package bus

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

func TestBusInterceptionRetainsCommittedStageThroughLaterDispositions(t *testing.T) {
	for _, stop := range []string{"continue", "terminal", "retry", "uncommitted_error"} {
		t.Run(stop, func(t *testing.T) {
			event := receiverProjectionEvent("stage-receipt")
			stage := engine.CommittedStage{Instance: flowidentity.RunScopedFlowInstance{RunID: uuid.NewString(), Route: flowidentity.RouteForInstancePath("orders")}, EntityID: uuid.NewString(), Stage: "done", StageDefined: true, Revision: 8}
			ack, err := (pipelineobligation.ExecutionOutcome{Committed: true}).WithCommittedStage(event.ID(), stage)
			if err != nil {
				t.Fatal(err)
			}
			later := &outcomeTestInterceptor{}
			switch stop {
			case "terminal":
				later.outcome = pipelineobligation.DeadLetterExecution("later_terminal", nil)
			case "retry":
				later.outcome = pipelineobligation.ReleaseForRetry("later_retry", nil)
			case "uncommitted_error":
				later.err = errors.New("later uncommitted failure")
			}
			bus, _ := interceptorOutcomeBus(t, newTargetRouteMemoryStore(), &outcomeTestInterceptor{outcome: ack}, later)
			_, _, outcome, err := bus.runInterceptorSet(context.Background(), event, bus.interceptorsSnapshot())
			if stop == "uncommitted_error" && !errors.Is(err, later.err) {
				t.Fatalf("later failure lost: %v", err)
			}
			receipts := outcome.StageReceipts()
			if len(receipts) != 1 || receipts[0].EventID() != event.ID() || receipts[0].Stage() != stage {
				t.Fatalf("acknowledged stage lost: %+v", receipts)
			}
			if stop != "continue" && outcome.Committed {
				t.Fatal("retained stage acknowledged an uncommitted parent's disposition")
			}
		})
	}
}
