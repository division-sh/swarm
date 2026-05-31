package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/runtime/bundledelete"
	"swarm/internal/runtime/preservationcleanup"
	storerunlifecycle "swarm/internal/store/runlifecycle"
	"swarm/internal/testutil"
)

const bundleDeleteTestHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const bundleDeleteOtherHash = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func TestPostgresStore_BundleDeleteForceCleanupAndFinalMutation(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	if _, err := pg.DB.ExecContext(ctx, `INSERT INTO agents (agent_id, role, model_tier, conversation_mode) VALUES ('agent-a', 'operator', 'default', 'session')`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	seedBundleDeleteBundle(t, ctx, pg, bundleDeleteTestHash)
	seedBundleDeleteBundle(t, ctx, pg, bundleDeleteOtherHash)

	activeRunID := uuid.NewString()
	completedRunID := uuid.NewString()
	otherRunID := uuid.NewString()
	sessionID := uuid.NewString()
	timerID := uuid.NewString()
	seedBundleDeleteRun(t, ctx, pg, activeRunID, "running", bundleDeleteTestHash)
	seedBundleDeleteRun(t, ctx, pg, completedRunID, "completed", bundleDeleteTestHash)
	seedBundleDeleteRun(t, ctx, pg, otherRunID, "running", bundleDeleteOtherHash)
	eventID := seedDestructiveResetEvent(t, ctx, pg, activeRunID, "bundle.delete.pending")
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, reason_code, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'agent', 'agent-a', 'pending', $3::uuid, 'ready', now()
		)
	`, activeRunID, eventID, sessionID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO agent_sessions (session_id, run_id, agent_id, scope_key, scope, runtime_mode, status)
		VALUES ($1::uuid, $2::uuid, 'agent-a', $2::text, 'flow', 'session', 'active')
	`, sessionID, activeRunID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO timers (timer_id, timer_name, run_id, fire_event, fire_at, status)
		VALUES ($1::uuid, 'bundle-delete-timer', $2::uuid, 'timer.fired', now() + interval '1 hour', 'active')
	`, timerID, activeRunID); err != nil {
		t.Fatalf("seed timer: %v", err)
	}

	plan, err := pg.PlanBundleDelete(ctx, bundledelete.Request{
		ActorTokenID: "token",
		BundleHash:   bundleDeleteTestHash,
		Force:        true,
		RequestedAt:  now,
	})
	if err != nil {
		t.Fatalf("PlanBundleDelete: %v", err)
	}
	if len(plan.ActiveRuns) != 1 || len(plan.NonActiveRuns) != 1 || len(plan.AffectedRuns) != 2 || len(plan.ActiveDeliveries) != 1 || len(plan.ActiveSessions) != 1 || len(plan.ActiveTimers) != 1 {
		t.Fatalf("plan counts active=%d non_active=%d affected=%d deliveries=%d sessions=%d timers=%d",
			len(plan.ActiveRuns), len(plan.NonActiveRuns), len(plan.AffectedRuns), len(plan.ActiveDeliveries), len(plan.ActiveSessions), len(plan.ActiveTimers))
	}

	cleanupResult, err := pg.ApplyBundleForceDeletePreservationCleanup(ctx, preservationcleanup.Request{
		OperationName: preservationcleanup.BundleForceDeleteOperationName,
		RequestedAt:   now,
		ControlledBy:  preservationcleanup.BundleForceDeleteControlledBy,
		Targets:       bundledelete.ActiveRunTargets(plan),
	})
	if err != nil {
		t.Fatalf("ApplyBundleForceDeletePreservationCleanup: %v", err)
	}
	if len(cleanupResult.Runs) != 1 || len(cleanupResult.Deliveries) != 1 || len(cleanupResult.Sessions) != 1 || len(cleanupResult.Timers) != 1 {
		t.Fatalf("cleanup counts runs=%d deliveries=%d sessions=%d timers=%d", len(cleanupResult.Runs), len(cleanupResult.Deliveries), len(cleanupResult.Sessions), len(cleanupResult.Timers))
	}

	final, err := pg.ApplyBundleDeleteFinalMutation(ctx, bundledelete.FinalMutationRequest{
		OperationName: bundledelete.DefaultOperationName,
		BundleHash:    bundleDeleteTestHash,
		RequestedAt:   now,
	})
	if err != nil {
		t.Fatalf("ApplyBundleDeleteFinalMutation: %v", err)
	}
	if !final.Deleted || final.BundleRowsDeleted != 1 || final.RunsMarkedDeleted != 2 {
		t.Fatalf("final mutation = %#v, want deleted row and two run source updates", final)
	}

	assertBundleDeleteRun(t, ctx, pg, activeRunID, "cancelled", storerunlifecycle.BundleSourceDeleted, preservationcleanup.BundleForceDeletedReason)
	assertBundleDeleteRun(t, ctx, pg, completedRunID, "completed", storerunlifecycle.BundleSourceDeleted, "")
	assertBundleDeleteRun(t, ctx, pg, otherRunID, "running", storerunlifecycle.BundleSourcePersisted, "")
	assertUnavailableBundlePreservationDelivery(t, ctx, pg, eventID, preservationcleanup.BundleForceDeletedReason)
	assertUnavailableBundlePreservationReceipt(t, ctx, pg, eventID, "agent-a", preservationcleanup.BundleForceDeletedReason)
	assertUnavailableBundlePreservationReceipt(t, ctx, pg, eventID, activeRunQuiescencePipelineSubscriberID, preservationcleanup.BundleForceDeletedReason)
	assertUnavailableBundlePreservationSession(t, ctx, pg, sessionID, preservationcleanup.BundleForceDeletedReason)
	assertUnavailableBundlePreservationTimer(t, ctx, pg, timerID)
	assertBundleRowAbsent(t, ctx, pg, bundleDeleteTestHash)
	assertBundleRowPresent(t, ctx, pg, bundleDeleteOtherHash)
}

func TestPostgresStore_BundleDeleteFinalMutationFailsBeforeDeletingWithActiveRuns(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	runID := uuid.NewString()
	seedBundleDeleteBundle(t, ctx, pg, bundleDeleteTestHash)
	seedBundleDeleteRun(t, ctx, pg, runID, "running", bundleDeleteTestHash)

	_, err = pg.ApplyBundleDeleteFinalMutation(ctx, bundledelete.FinalMutationRequest{
		OperationName: bundledelete.DefaultOperationName,
		BundleHash:    bundleDeleteTestHash,
		RequestedAt:   time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC),
	})
	if !errors.Is(err, bundledelete.ErrActiveRunsRemain) {
		t.Fatalf("ApplyBundleDeleteFinalMutation active error = %v, want ErrActiveRunsRemain", err)
	}
	assertBundleDeleteRun(t, ctx, pg, runID, "running", storerunlifecycle.BundleSourcePersisted, "")
	assertBundleRowPresent(t, ctx, pg, bundleDeleteTestHash)
}

func seedBundleDeleteBundle(t *testing.T, ctx context.Context, pg *PostgresStore, bundleHash string) {
	t.Helper()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json, metadata, ingested_at)
		VALUES ($1, 'version: 1', '{}'::jsonb, '{}'::jsonb, now())
		ON CONFLICT (bundle_hash) DO NOTHING
	`, bundleHash); err != nil {
		t.Fatalf("seed bundle %s: %v", bundleHash, err)
	}
}

func seedBundleDeleteRun(t *testing.T, ctx context.Context, pg *PostgresStore, runID, status, bundleHash string) {
	t.Helper()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, bundle_fingerprint, started_at)
		VALUES ($1::uuid, $2, $3, $4, $5, now())
	`, runID, status, bundleHash, storerunlifecycle.BundleSourcePersisted, testBootBundleFingerprint); err != nil {
		t.Fatalf("seed bundle delete run %s: %v", runID, err)
	}
}

func assertBundleDeleteRun(t *testing.T, ctx context.Context, pg *PostgresStore, runID, wantStatus, wantSource, wantError string) {
	t.Helper()
	var status, source, errorSummary string
	var endedAt sql.NullTime
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT status, bundle_source, COALESCE(error_summary, ''), ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&status, &source, &errorSummary, &endedAt); err != nil {
		t.Fatalf("load run %s: %v", runID, err)
	}
	if status != wantStatus || source != wantSource || errorSummary != wantError {
		t.Fatalf("run %s = status:%s source:%s error:%s ended:%v, want %s/%s/%s", runID, status, source, errorSummary, endedAt.Valid, wantStatus, wantSource, wantError)
	}
	if wantStatus == "cancelled" && !endedAt.Valid {
		t.Fatalf("run %s cancelled without ended_at", runID)
	}
	if wantStatus == "cancelled" {
		var controlStatus, reason, controlledBy string
		if err := pg.DB.QueryRowContext(ctx, `
			SELECT control_status, COALESCE(reason, ''), COALESCE(controlled_by, '')
			FROM run_control_state
			WHERE run_id = $1::uuid
		`, runID).Scan(&controlStatus, &reason, &controlledBy); err != nil {
			t.Fatalf("load run control %s: %v", runID, err)
		}
		if controlStatus != preservationcleanup.RunControlStatusStopped || reason != preservationcleanup.BundleForceDeletedReason || controlledBy != preservationcleanup.BundleForceDeleteControlledBy {
			t.Fatalf("run control %s = %s/%s/%s, want stopped/%s/%s", runID, controlStatus, reason, controlledBy, preservationcleanup.BundleForceDeletedReason, preservationcleanup.BundleForceDeleteControlledBy)
		}
	}
}

func assertBundleRowPresent(t *testing.T, ctx context.Context, pg *PostgresStore, bundleHash string) {
	t.Helper()
	var exists bool
	if err := pg.DB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM bundles WHERE bundle_hash = $1)`, bundleHash).Scan(&exists); err != nil {
		t.Fatalf("check bundle row %s: %v", bundleHash, err)
	}
	if !exists {
		t.Fatalf("bundle row %s missing, want present", bundleHash)
	}
}

func assertBundleRowAbsent(t *testing.T, ctx context.Context, pg *PostgresStore, bundleHash string) {
	t.Helper()
	var exists bool
	if err := pg.DB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM bundles WHERE bundle_hash = $1)`, bundleHash).Scan(&exists); err != nil {
		t.Fatalf("check bundle row %s: %v", bundleHash, err)
	}
	if exists {
		t.Fatalf("bundle row %s present, want absent", bundleHash)
	}
}
