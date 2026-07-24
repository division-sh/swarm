package bus_test

import (
	"context"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func TestPipelineScanReturnsCorruptScopeToStartupConsumerOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newCompleteEventDispatchFixture(t, backend, false)
			logger := &recordingLoggerHook{}
			bus, err := newScopedTestEventBus(fixture.store, runtimebus.EventBusOptions{Logger: logger})
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			fixture.bus = bus
			deleteCommittedReplayScope(t, fixture, fixture.event.ID())

			ctx := runtimepipelineobligation.WithStartupRecoveryDiagnostics(fixture.ctx)
			result, err := fixture.bus.SweepPipelineObligations(ctx, 2)
			if err != nil {
				t.Fatalf("SweepPipelineObligations: %v", err)
			}
			if result.Settled != 1 || result.Examined != 1 || !result.Exhausted || result.Blocked {
				t.Fatalf("corrupt-scope sweep result = %#v", result)
			}
			outcome, reason := pipelineReceiptOutcome(t, fixture, fixture.event.ID())
			if outcome != "dead_letter" || reason != "committed_pipeline_scope_missing" {
				t.Fatalf("corrupt-scope receipt = %q/%q", outcome, reason)
			}
			foundAftermath := false
			for _, entry := range logger.entries {
				if entry.Action == "startup_recovery_pipeline_replay_aftermath" {
					foundAftermath = true
					break
				}
			}
			if !foundAftermath {
				t.Fatal("startup consumer did not record corrupt-scope replay aftermath")
			}
		})
	}
}

func TestAcknowledgedDecisionRouteWinsBeforeCorruptScopeQuarantineOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newCompleteEventDispatchFixture(t, backend, true)
			owner := fixture.store.PipelineObligations()
			work, err := owner.ClaimEvent(fixture.ctx, fixture.event.ID(), runtimepipelineobligation.PurposeDecisionRoute)
			if err != nil {
				t.Fatalf("ClaimEvent: %v", err)
			}
			if err := owner.MarkDecisionProcessed(fixture.ctx, work.Claim); err != nil {
				t.Fatalf("MarkDecisionProcessed: %v", err)
			}
			if err := owner.Release(fixture.ctx, work.Claim); err != nil {
				t.Fatalf("Release: %v", err)
			}
			deleteCommittedReplayScope(t, fixture, fixture.event.ID())

			result, err := fixture.bus.SweepPipelineObligations(fixture.ctx, 2)
			if err != nil {
				t.Fatalf("SweepPipelineObligations: %v", err)
			}
			if result.Settled != 1 || !result.Exhausted {
				t.Fatalf("acknowledged corrupt-scope result = %#v", result)
			}
			if status := fixture.decisionObligationStatus(t, fixture.event.ID()); status != "completed" {
				t.Fatalf("decision route status = %q, want completed", status)
			}
			outcome, reason := pipelineReceiptOutcome(t, fixture, fixture.event.ID())
			if outcome != "success" || reason != "decision_route_processed" {
				t.Fatalf("acknowledged decision receipt = %q/%q", outcome, reason)
			}
		})
	}
}

func deleteCommittedReplayScope(t testing.TB, fixture completeEventDispatchFixture, eventID string) {
	t.Helper()
	query := `DELETE FROM committed_replay_scopes WHERE event_id = ?`
	if fixture.dialect == "postgres" {
		query = `DELETE FROM committed_replay_scopes WHERE event_id = $1::uuid`
	}
	if _, err := fixture.db.ExecContext(fixture.ctx, query, eventID); err != nil {
		t.Fatalf("delete committed replay scope: %v", err)
	}
}

func pipelineReceiptOutcome(t testing.TB, fixture completeEventDispatchFixture, eventID string) (string, string) {
	t.Helper()
	query := `
		SELECT outcome, COALESCE(reason_code, '')
		FROM event_receipts
		WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	if fixture.dialect == "postgres" {
		query = `
			SELECT outcome, COALESCE(reason_code, '')
			FROM event_receipts
			WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	}
	var outcome, reason string
	if err := fixture.db.QueryRowContext(context.Background(), query, eventID).Scan(&outcome, &reason); err != nil {
		t.Fatalf("load pipeline receipt: %v", err)
	}
	return outcome, reason
}
