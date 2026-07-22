package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/preservationcleanup"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_ApplyUnavailableBundleStartupPreservationCleanup_OrphansRunScopedState(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := testAuthorActivityContext()
	now := time.Date(2026, 5, 27, 9, 30, 0, 0, time.UTC)
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO agents (agent_id, flow_instance, role, model, memory_enabled, memory_source)
		VALUES ('agent-a', 'preservation', 'operator', 'regular', TRUE, 'authored')
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
		if _, err := pg.DB.ExecContext(ctx, `
			INSERT INTO agent_sessions (session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status)
			VALUES ($1::uuid, $2::uuid, 'agent-a', 'preservation', TRUE, 'authored', 'active')
		`, sessionID, runID); err != nil {
			t.Fatalf("seed session %s: %v", source, err)
		}
		eventID, activeClaim := seedPreservationClaimedDelivery(t, ctx, pg, runID, "startup."+source+".active")
		if _, err := pg.BindAgentSession(ctx, activeClaim, sessionID); err != nil {
			t.Fatalf("bind active delivery %s: %v", source, err)
		}
		untouchedID, retryClaim := seedPreservationClaimedDelivery(t, ctx, pg, runID, "startup."+source+".retry")
		if snapshot, err := pg.SettleFailure(ctx, retryClaim, runtimedelivery.Settlement{
			Disposition: runtimedelivery.FailureRetry,
			Failure:     testRetryableFailure(),
			RetryBase:   time.Hour,
		}); err != nil || snapshot.Status != runtimedelivery.StatusFailed {
			t.Fatalf("seed retryable delivery %s: snapshot=%#v err=%v", source, snapshot, err)
		}
		if _, err := pg.DB.ExecContext(ctx, `
			INSERT INTO timers (timer_id, timer_name, run_id, fire_event, fire_at, status)
			VALUES ($1::uuid, $2, $3::uuid, 'timer.fired', now() + interval '1 hour', 'active')
		`, timerID, aggregateWorkflowTimerTaskID(timerID), runID); err != nil {
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
	if len(result.Runs) != 3 || len(result.Deliveries) != 6 || len(result.Sessions) != 3 || len(result.Timers) != 3 || result.PipelineReceiptCount != 6 {
		t.Fatalf("cleanup result = runs:%d deliveries:%d sessions:%d timers:%d pipeline:%d, want 3/6/3/3/6", len(result.Runs), len(result.Deliveries), len(result.Sessions), len(result.Timers), result.PipelineReceiptCount)
	}

	for source, item := range seeded {
		target := byRun[item.runID]
		assertUnavailableBundlePreservationRun(t, ctx, pg, item.runID, source, target.ReasonCode)
		assertUnavailableBundlePreservationDelivery(t, ctx, pg, item.eventID, target.ReasonCode)
		assertUnavailableBundlePreservationReceipt(t, ctx, pg, item.eventID, "agent-a", target.ReasonCode)
		assertUnavailableBundlePreservationReceipt(t, ctx, pg, item.eventID, activeRunQuiescencePipelineSubscriberID, target.ReasonCode)
		assertUnavailableBundlePreservationDelivery(t, ctx, pg, item.untouchedID, target.ReasonCode)
		assertUnavailableBundlePreservationReceipt(t, ctx, pg, item.untouchedID, "agent-a", target.ReasonCode)
		assertUnavailableBundlePreservationReceipt(t, ctx, pg, item.untouchedID, activeRunQuiescencePipelineSubscriberID, target.ReasonCode)
		assertUnavailableBundlePreservationSession(t, ctx, pg, item.sessionID, target.ReasonCode)
		assertUnavailableBundlePreservationTimer(t, ctx, pg, item.timerID)
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
	var status, source, controlStatus, controlReason, controlledBy string
	var failure []byte
	var endedAt sql.NullTime
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT
			r.status,
			r.bundle_source,
			r.failure,
			r.ended_at,
			rc.control_status,
			COALESCE(rc.reason, ''),
			COALESCE(rc.controlled_by, '')
		FROM runs r
		JOIN run_control_state rc ON rc.run_id = r.run_id
		WHERE r.run_id = $1::uuid
	`, runID).Scan(&status, &source, &failure, &endedAt, &controlStatus, &controlReason, &controlledBy); err != nil {
		t.Fatalf("load run/control %s: %v", runID, err)
	}
	if status != "cancelled" || source != wantSource || len(failure) != 0 || !endedAt.Valid {
		t.Fatalf("run %s = status:%s source:%s failure:%s ended:%v, want cancelled/%s/no-failure/ended", runID, status, source, failure, endedAt.Valid, wantSource)
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
	query := `
		SELECT o.outcome, COALESCE(o.reason_code, '')
		FROM event_delivery_outcomes o
		JOIN event_deliveries d ON d.delivery_id = o.delivery_id
		WHERE d.event_id = $1::uuid AND d.subscriber_id = $2
		ORDER BY o.claim_version DESC
		LIMIT 1
	`
	wantOutcome := "terminalized"
	if subscriberID == activeRunQuiescencePipelineSubscriberID {
		query = `SELECT outcome, COALESCE(reason_code, '') FROM event_receipts WHERE event_id = $1::uuid AND subscriber_id = $2`
		wantOutcome = "dead_letter"
	}
	if err := pg.DB.QueryRowContext(ctx, query, eventID, subscriberID).Scan(&outcome, &reason); err != nil {
		t.Fatalf("load receipt %s/%s: %v", eventID, subscriberID, err)
	}
	if outcome != wantOutcome || reason != wantReason {
		t.Fatalf("receipt %s/%s = %s/%s, want %s/%s", eventID, subscriberID, outcome, reason, wantOutcome, wantReason)
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

func seedPreservationClaimedDelivery(t *testing.T, ctx context.Context, pg *PostgresStore, runID, eventName string) (string, runtimedelivery.Claim) {
	t.Helper()
	event := eventtest.RunCreatingRootIngress(
		uuid.NewString(), events.EventType(eventName), "test", "", []byte(`{}`), 0,
		runID, "", events.EventEnvelope{}, time.Now().UTC(),
	)
	if err := commitSemanticEventFixtureWithAgents(ctx, pg, event, []string{"agent-a"}); err != nil {
		t.Fatalf("commit preservation delivery %s: %v", eventName, err)
	}
	route := events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: "agent-a"}
	claimed, err := pg.ClaimAgentDelivery(ctx, event, route)
	if err != nil {
		t.Fatalf("claim preservation delivery %s: %v", eventName, err)
	}
	return event.ID(), claimed.Claim
}
