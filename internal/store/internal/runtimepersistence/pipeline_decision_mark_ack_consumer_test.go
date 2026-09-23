package runtimepersistence

import (
	"errors"
	"testing"
	"time"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
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
			ctx := testAuthorActivityContext()
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
			query := `SELECT bundle_hash FROM runs WHERE run_id=?`
			if backend.name == "postgres" {
				query = `SELECT bundle_hash FROM runs WHERE run_id=$1::uuid`
			}
			var bundleHash string
			if err := fixture.db.QueryRowContext(ctx, query, runID).Scan(&bundleHash); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected decision mark postcommit handoff failure")
			submits := 0
			sink := &completionHandoffEvidenceProbeSink{submit: func(candidate runtimerunlifecycle.Candidate) error {
				submits++
				if candidate.RunID != runID {
					t.Errorf("foreign candidate: %+v", candidate)
				}
				return injected
			}}
			registrar := selected.(runtimerunlifecycle.CandidateRegistrar)
			registration, err := registrar.RegisterCompletionCandidateSink(ctx, runtimerunlifecycle.CandidateScope{BundleHash: bundleHash}, sink)
			if err != nil {
				t.Fatal(err)
			}
			defer registration.Release()
			outcome, err := owner.MarkDecisionProcessed(ctx, work.Claim)
			if !outcome.Committed() || !outcome.DeliveryHandoffCommitted() || !errors.Is(err, injected) || submits != 1 {
				t.Fatalf("mark committed=%t handoff=%t err=%v submissions=%d", outcome.Committed(), outcome.DeliveryHandoffCommitted(), err, submits)
			}
			registration.Release()
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
