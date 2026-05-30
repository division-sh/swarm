package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/runtime/preservationcleanup"
	storerunlifecycle "swarm/internal/store/runlifecycle"
	"swarm/internal/testutil"
)

func TestPostgresStore_ApplyUnavailableBundleStartupPreservationCleanup_OrphansRunScopedState(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	now := time.Date(2026, 5, 27, 9, 30, 0, 0, time.UTC)
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO agents (agent_id, role, model_tier, conversation_mode)
		VALUES ('agent-a', 'operator', 'default', 'session')
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	targets := []preservationcleanup.RunTarget{}
	byRun := map[string]preservationcleanup.RunTarget{}
	type seededRun struct {
		runID       string
		eventID     string
		untouchedID string
		sessionID   string
		timerID     string
	}
	seeded := map[string]seededRun{}
	for _, source := range []string{
		storerunlifecycle.BundleSourceEphemeral,
		storerunlifecycle.BundleSourceDeleted,
		storerunlifecycle.BundleSourceLegacy,
	} {
		runID := uuid.NewString()
		sessionID := uuid.NewString()
		timerID := uuid.NewString()
		if _, err := pg.DB.ExecContext(ctx, `
			INSERT INTO runs (run_id, status, bundle_source, bundle_fingerprint, started_at)
			VALUES ($1::uuid, 'running', $2, $3, now())
			ON CONFLICT (run_id) DO UPDATE SET
				status = 'running',
				bundle_source = EXCLUDED.bundle_source,
				bundle_fingerprint = EXCLUDED.bundle_fingerprint
		`, runID, source, testBootBundleFingerprint); err != nil {
			t.Fatalf("seed run %s: %v", source, err)
		}
		eventID := seedDestructiveResetEvent(t, ctx, pg, runID, "startup."+source+".pending")
		untouchedID := seedDestructiveResetEvent(t, ctx, pg, runID, "startup."+source+".failed")
		if _, err := pg.DB.ExecContext(ctx, `
			INSERT INTO event_deliveries (
				run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, reason_code, created_at
			) VALUES (
				$1::uuid, $2::uuid, 'agent', 'agent-a', 'in_progress', $3::uuid, 'agent_processing', now()
			)
		`, runID, eventID, sessionID); err != nil {
			t.Fatalf("seed active delivery %s: %v", source, err)
		}
		if _, err := pg.DB.ExecContext(ctx, `
			INSERT INTO event_deliveries (
				run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, created_at, delivered_at
			) VALUES (
				$1::uuid, $2::uuid, 'agent', 'agent-a', 'failed', 1, 'agent_retryable_error', now(), now()
			)
		`, runID, untouchedID); err != nil {
			t.Fatalf("seed retryable failed delivery %s: %v", source, err)
		}
		if _, err := pg.DB.ExecContext(ctx, `
			INSERT INTO agent_sessions (session_id, run_id, agent_id, scope_key, scope, runtime_mode, status)
			VALUES ($1::uuid, $2::uuid, 'agent-a', $2::text, 'flow', 'session', 'active')
		`, sessionID, runID); err != nil {
			t.Fatalf("seed session %s: %v", source, err)
		}
		if _, err := pg.DB.ExecContext(ctx, `
			INSERT INTO timers (timer_id, timer_name, run_id, fire_event, fire_at, status)
			VALUES ($1::uuid, $2, $3::uuid, 'timer.fired', now() + interval '1 hour', 'active')
		`, timerID, "timer-"+source, runID); err != nil {
			t.Fatalf("seed timer %s: %v", source, err)
		}
		cause, ok := preservationcleanup.CauseForBundleSource(source)
		if !ok {
			t.Fatalf("missing cause for source %s", source)
		}
		target := preservationcleanup.RunTarget{RunID: runID, BundleSource: source, BundleFingerprint: testBootBundleFingerprint, ReasonCode: cause}
		targets = append(targets, target)
		byRun[runID] = target
		seeded[source] = seededRun{runID: runID, eventID: eventID, untouchedID: untouchedID, sessionID: sessionID, timerID: timerID}
	}

	result, err := pg.ApplyUnavailableBundleStartupPreservationCleanup(ctx, preservationcleanup.Request{
		OperationName: preservationcleanup.UnavailableBundleStartupOperationName,
		RequestedAt:   now,
		ControlledBy:  preservationcleanup.UnavailableBundleStartupControlledBy,
		Targets:       targets,
	})
	if err != nil {
		t.Fatalf("ApplyUnavailableBundleStartupPreservationCleanup: %v", err)
	}
	if len(result.Runs) != 3 || len(result.Deliveries) != 3 || len(result.Sessions) != 3 || len(result.Timers) != 3 || result.PipelineReceiptCount != 3 {
		t.Fatalf("cleanup result = runs:%d deliveries:%d sessions:%d timers:%d pipeline:%d, want 3 each", len(result.Runs), len(result.Deliveries), len(result.Sessions), len(result.Timers), result.PipelineReceiptCount)
	}

	for source, item := range seeded {
		target := byRun[item.runID]
		assertUnavailableBundlePreservationRun(t, ctx, pg, item.runID, source, target.ReasonCode)
		assertUnavailableBundlePreservationDelivery(t, ctx, pg, item.eventID, target.ReasonCode)
		assertUnavailableBundlePreservationReceipt(t, ctx, pg, item.eventID, "agent-a", target.ReasonCode)
		assertUnavailableBundlePreservationReceipt(t, ctx, pg, item.eventID, activeRunQuiescencePipelineSubscriberID, target.ReasonCode)
		assertUnavailableBundlePreservationSession(t, ctx, pg, item.sessionID, target.ReasonCode)
		assertUnavailableBundlePreservationTimer(t, ctx, pg, item.timerID)
		assertUnavailableBundlePreservationFailedDeliveryUntouched(t, ctx, pg, item.untouchedID)
	}
	var eventCount int
	if err := pg.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 6 {
		t.Fatalf("events count = %d, want 6 preserved rows", eventCount)
	}
}

func assertUnavailableBundlePreservationRun(t *testing.T, ctx context.Context, pg *PostgresStore, runID, wantSource, wantReason string) {
	t.Helper()
	var status, source, errorSummary, controlStatus, controlReason, controlledBy string
	var endedAt sql.NullTime
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT
			r.status,
			r.bundle_source,
			COALESCE(r.error_summary, ''),
			r.ended_at,
			rc.control_status,
			COALESCE(rc.reason, ''),
			COALESCE(rc.controlled_by, '')
		FROM runs r
		JOIN run_control_state rc ON rc.run_id = r.run_id
		WHERE r.run_id = $1::uuid
	`, runID).Scan(&status, &source, &errorSummary, &endedAt, &controlStatus, &controlReason, &controlledBy); err != nil {
		t.Fatalf("load run/control %s: %v", runID, err)
	}
	if status != "cancelled" || source != wantSource || errorSummary != wantReason || !endedAt.Valid {
		t.Fatalf("run %s = status:%s source:%s error:%s ended:%v, want cancelled/%s/%s/ended", runID, status, source, errorSummary, endedAt.Valid, wantSource, wantReason)
	}
	if controlStatus != "stopped" || controlReason != wantReason || controlledBy != preservationcleanup.UnavailableBundleStartupControlledBy {
		t.Fatalf("run control %s = %s/%s/%s, want stopped/%s/%s", runID, controlStatus, controlReason, controlledBy, wantReason, preservationcleanup.UnavailableBundleStartupControlledBy)
	}
}

func assertUnavailableBundlePreservationDelivery(t *testing.T, ctx context.Context, pg *PostgresStore, eventID, wantReason string) {
	t.Helper()
	var status, reason string
	var activeSession sql.NullString
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, ''), active_session_id::text
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'agent-a'
	`, eventID).Scan(&status, &reason, &activeSession); err != nil {
		t.Fatalf("load delivery %s: %v", eventID, err)
	}
	if status != "dead_letter" || reason != wantReason || activeSession.Valid {
		t.Fatalf("delivery %s = %s/%s active=%v, want dead_letter/%s/no active session", eventID, status, reason, activeSession.Valid, wantReason)
	}
}

func assertUnavailableBundlePreservationReceipt(t *testing.T, ctx context.Context, pg *PostgresStore, eventID, subscriberID, wantReason string) {
	t.Helper()
	var outcome, reason string
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT outcome, COALESCE(reason_code, '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_id = $2
	`, eventID, subscriberID).Scan(&outcome, &reason); err != nil {
		t.Fatalf("load receipt %s/%s: %v", eventID, subscriberID, err)
	}
	if outcome != "dead_letter" || reason != wantReason {
		t.Fatalf("receipt %s/%s = %s/%s, want dead_letter/%s", eventID, subscriberID, outcome, reason, wantReason)
	}
}

func assertUnavailableBundlePreservationSession(t *testing.T, ctx context.Context, pg *PostgresStore, sessionID, wantDetail string) {
	t.Helper()
	var status, reason, detail string
	var terminatedAt sql.NullTime
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(termination_reason, ''), COALESCE(termination_detail, ''), terminated_at
		FROM agent_sessions
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&status, &reason, &detail, &terminatedAt); err != nil {
		t.Fatalf("load session %s: %v", sessionID, err)
	}
	if status != "terminated" || reason != preservationcleanup.SessionTerminationReasonOrphaned || detail != wantDetail || !terminatedAt.Valid {
		t.Fatalf("session %s = %s/%s/%s ended:%v, want terminated/orphaned/%s/ended", sessionID, status, reason, detail, terminatedAt.Valid, wantDetail)
	}
}

func assertUnavailableBundlePreservationTimer(t *testing.T, ctx context.Context, pg *PostgresStore, timerID string) {
	t.Helper()
	var status string
	if err := pg.DB.QueryRowContext(ctx, `SELECT status FROM timers WHERE timer_id = $1::uuid`, timerID).Scan(&status); err != nil {
		t.Fatalf("load timer %s: %v", timerID, err)
	}
	if status != "cancelled" {
		t.Fatalf("timer %s status = %s, want cancelled", timerID, status)
	}
}

func assertUnavailableBundlePreservationFailedDeliveryUntouched(t *testing.T, ctx context.Context, pg *PostgresStore, eventID string) {
	t.Helper()
	var status, reason string
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
	`, eventID).Scan(&status, &reason); err != nil {
		t.Fatalf("load untouched delivery %s: %v", eventID, err)
	}
	if status != "failed" || reason != "agent_retryable_error" {
		t.Fatalf("untouched delivery %s = %s/%s, want failed/agent_retryable_error", eventID, status, reason)
	}
}
