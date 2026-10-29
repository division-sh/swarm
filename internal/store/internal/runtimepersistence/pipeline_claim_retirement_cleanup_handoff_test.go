package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/google/uuid"
)

func TestPipelineSettlementRetirementCleanupStillHandsOffCandidateBothStores(t *testing.T) {
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
			ctx := originalStoreFixtureContext(t, testAuthorActivityContext(), fixture.store)
			runID := uuid.NewString()
			ctx = seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			eventID := commitPipelineParityEvent(t, ctx, selected, runID, time.Now().UTC().Add(-time.Minute))
			owner := selected.PipelineObligations()
			work, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
			if err != nil {
				t.Fatalf("claim pipeline event: %v", err)
			}
			fault := errors.New("injected claim retirement cleanup failure")
			switch store := selected.(type) {
			case *PostgresStore:
				state, err := store.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(work.Claim)
				if err != nil {
					t.Fatalf("load postgres claim state: %v", err)
				}
				state.LeaseForTest().SetUnlockForTest(func(context.Context, *postgresbackend.SessionAuthority, string) (bool, error) {
					return false, fault
				})
			case *SQLiteRuntimeStore:
				store.pipelineSQLiteOwner.SetPipelineReleaseErrorForTest(func() error {
					return fault
				})
				defer store.pipelineSQLiteOwner.SetPipelineReleaseErrorForTest(nil)
			default:
				t.Fatalf("unsupported pipeline store %T", selected)
			}

			outcome, err := owner.Settle(ctx, work.Claim, runtimepipelineobligation.Acknowledged("processed"))
			if !outcome.Committed() || !outcome.DeliveryHandoffCommitted() || !errors.Is(err, fault) {
				t.Fatalf("settlement committed=%t handoff=%t err=%v", outcome.Committed(), outcome.DeliveryHandoffCommitted(), err)
			}
			if err := owner.Release(ctx, work.Claim); !errors.Is(err, runtimepipelineobligation.ErrStaleClaim) {
				t.Fatalf("retirement cleanup left the original claim live: %v", err)
			}
			if _, err := owner.ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery); !errors.Is(err, runtimepipelineobligation.ErrIneligible) {
				t.Fatalf("committed settlement remained replayable: %v", err)
			}
		})
	}
}
