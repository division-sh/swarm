package runtimepersistence

import (
	"testing"
	"time"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"

	"github.com/google/uuid"
)

func TestDecisionMarkRetainsAcknowledgementAfterHandoffFailure(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) authorActivityReceiptFixture
	}{
		{name: "sqlite", open: openSQLiteAuthorActivityReceiptFixture},
		{name: "postgres", open: openPostgresAuthorActivityReceiptFixture},
	} {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected, ok := fixture.store.(pipelineObligationParityStore)
			if !ok {
				t.Fatalf("%T does not expose pipeline obligations", fixture.store)
			}
			ctx := storeTestOriginalProcessContext(t, testAuthorActivityContext(), selected)
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			at := time.Now().UTC().Add(-time.Minute)
			eventID := commitPipelineParityEvent(t, ctx, selected, runID, at)
			insertProducerIdentityDecisionObligation(t, fixture, ctx, eventID, runID, at)
			owner := selected.PipelineObligations()
			work, found, err := claimNextPipelineWorkForTest(t, ctx, owner, runtimepipelineobligation.DecisionRouteQuery())
			if err != nil || !found || work.Event.ID() != eventID {
				t.Fatalf("claim decision route: found=%t work=%s err=%v", found, work.Event.ID(), err)
			}
			outcome, err := owner.MarkDecisionProcessed(ctx, work.Claim)
			if !outcome.Committed() || !outcome.DeliveryHandoffCommitted() || err != nil {
				t.Fatalf("mark committed=%t handoff=%t err=%v", outcome.Committed(), outcome.DeliveryHandoffCommitted(), err)
			}
			settlement, err := owner.Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("decision_route_converged"))
			if err != nil || !settlement.Committed() {
				t.Fatalf("settle acknowledged decision: committed=%t err=%v", settlement.Committed(), err)
			}
			if status := readDecisionRouteStatus(t, ctx, fixture, eventID); status != "completed" {
				t.Fatalf("decision route status = %q, want completed", status)
			}
		})
	}
}
