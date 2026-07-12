package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bundledelete"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/preservationcleanup"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const bundleDeleteTestHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const bundleDeleteOtherHash = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func TestPostgresStore_BundleDeleteForceCleanupAndFinalMutation(t *testing.T) {
	dsn, _, cleanup := testutil.AcquirePostgres(t, testutil.PostgresFreshPhysical())
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	if _, err := pg.DB.ExecContext(ctx, `INSERT INTO agents (agent_id, role, model, conversation_mode) VALUES ('agent-a', 'operator', 'default', 'session')`); err != nil {
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
	dsn, _, cleanup := testutil.AcquirePostgres(t, testutil.PostgresRowState())
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

func TestPostgresStore_BundleDeleteFinalMutationMarksOnlyNonActivePersistedRunsDeleted(t *testing.T) {
	dsn, _, cleanup := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	seedBundleDeleteBundle(t, ctx, pg, bundleDeleteTestHash)

	persisted := map[string]string{
		uuid.NewString(): "completed",
		uuid.NewString(): "failed",
		uuid.NewString(): "cancelled",
		uuid.NewString(): "forked",
	}
	for runID, status := range persisted {
		seedBundleDeleteRunWithSource(t, ctx, pg, runID, status, bundleDeleteTestHash, storerunlifecycle.BundleSourcePersisted)
	}
	ephemeralRunID := uuid.NewString()
	deletedRunID := uuid.NewString()
	legacyRunID := uuid.NewString()
	seedBundleDeleteRunWithSource(t, ctx, pg, ephemeralRunID, "completed", bundleDeleteTestHash, storerunlifecycle.BundleSourceEphemeral)
	seedBundleDeleteRunWithSource(t, ctx, pg, deletedRunID, "completed", bundleDeleteTestHash, storerunlifecycle.BundleSourceDeleted)
	seedBundleDeleteRunWithSource(t, ctx, pg, legacyRunID, "completed", bundleDeleteTestHash, storerunlifecycle.BundleSourceLegacy)

	final, err := pg.ApplyBundleDeleteFinalMutation(ctx, bundledelete.FinalMutationRequest{
		OperationName: bundledelete.DefaultOperationName,
		BundleHash:    bundleDeleteTestHash,
		RequestedAt:   time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("ApplyBundleDeleteFinalMutation: %v", err)
	}
	if !final.Deleted || final.BundleRowsDeleted != 1 || final.RunsMarkedDeleted != len(persisted) {
		t.Fatalf("final mutation = %#v, want deleted row and %d persisted run updates", final, len(persisted))
	}
	for runID, status := range persisted {
		assertBundleDeleteRunBundle(t, ctx, pg, runID, status, storerunlifecycle.BundleSourceDeleted, bundleDeleteTestHash)
	}
	assertBundleDeleteRunBundle(t, ctx, pg, ephemeralRunID, "completed", storerunlifecycle.BundleSourceEphemeral, bundleDeleteTestHash)
	assertBundleDeleteRunBundle(t, ctx, pg, deletedRunID, "completed", storerunlifecycle.BundleSourceDeleted, bundleDeleteTestHash)
	assertBundleDeleteRunBundle(t, ctx, pg, legacyRunID, "completed", storerunlifecycle.BundleSourceLegacy, bundleDeleteTestHash)
	assertBundleRowAbsent(t, ctx, pg, bundleDeleteTestHash)
}

func TestPostgresStore_BundleDeleteFinalMutationSerializesConcurrentRunCreation(t *testing.T) {
	dsn, _, cleanup := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	seedBundleDeleteBundle(t, ctx, pg, bundleDeleteTestHash)

	runCreationTx, err := pg.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin run creation tx: %v", err)
	}
	defer func() { _ = runCreationTx.Rollback() }()
	if _, err := runCreationTx.ExecContext(ctx, `LOCK TABLE runs IN ROW EXCLUSIVE MODE`); err != nil {
		t.Fatalf("hold run creation lock: %v", err)
	}

	finalCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := pg.ApplyBundleDeleteFinalMutation(finalCtx, bundledelete.FinalMutationRequest{
			OperationName: bundledelete.DefaultOperationName,
			BundleHash:    bundleDeleteTestHash,
			RequestedAt:   time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC),
		})
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("final mutation completed before concurrent run creation released: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	runID := uuid.NewString()
	if _, err := runCreationTx.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, bundle_fingerprint, started_at)
		VALUES ($1::uuid, 'running', $2, $3, $4, now())
	`, runID, bundleDeleteTestHash, storerunlifecycle.BundleSourcePersisted, testBootBundleFingerprint); err != nil {
		t.Fatalf("insert concurrent run: %v", err)
	}
	if err := runCreationTx.Commit(); err != nil {
		t.Fatalf("commit concurrent run: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, bundledelete.ErrActiveRunsRemain) {
			t.Fatalf("final mutation concurrent error = %v, want ErrActiveRunsRemain", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("final mutation did not finish after concurrent run creation committed")
	}
	assertBundleDeleteRun(t, ctx, pg, runID, "running", storerunlifecycle.BundleSourcePersisted, "")
	assertBundleRowPresent(t, ctx, pg, bundleDeleteTestHash)
}

func TestPostgresStore_BundleDeleteFinalMutationBlocksPostDeletePersistedSourceRunCreation(t *testing.T) {
	dsn, _, cleanup := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	seedBundleDeleteBundle(t, ctx, pg, bundleDeleteTestHash)
	final, err := pg.ApplyBundleDeleteFinalMutation(ctx, bundledelete.FinalMutationRequest{
		OperationName: bundledelete.DefaultOperationName,
		BundleHash:    bundleDeleteTestHash,
		RequestedAt:   time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("ApplyBundleDeleteFinalMutation: %v", err)
	}
	if !final.Deleted {
		t.Fatalf("final mutation = %#v, want deleted", final)
	}

	runID := uuid.NewString()
	eventID := uuid.NewString()
	publishCtx := runtimecorrelation.WithBundleSourceFact(ctx, runtimecorrelation.BundleSourceFact{
		BundleHash:        bundleDeleteTestHash,
		BundleSource:      storerunlifecycle.BundleSourcePersisted,
		BundleFingerprint: testBootBundleFingerprint,
	})
	err = pg.AppendEvent(publishCtx, eventtest.PersistedProjection(eventID,

		"scan.requested",
		"api.v1", "", []byte(`{"topic":"medicine"}`), 0, runID, "", events.EventEnvelope{}, time.Date(2026, 5, 31, 12, 1, 0, 0, time.UTC)))
	if !errors.Is(err, storerunlifecycle.ErrPersistedBundleUnavailable) {
		t.Fatalf("AppendEvent after delete error = %v, want ErrPersistedBundleUnavailable", err)
	}
	assertBundleDeleteRunAbsent(t, ctx, pg, runID)
	assertBundleDeleteEventAbsent(t, ctx, pg, eventID)
	assertBundleRowAbsent(t, ctx, pg, bundleDeleteTestHash)
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
	seedBundleDeleteRunWithSource(t, ctx, pg, runID, status, bundleHash, storerunlifecycle.BundleSourcePersisted)
}

func seedBundleDeleteRunWithSource(t *testing.T, ctx context.Context, pg *PostgresStore, runID, status, bundleHash, bundleSource string) {
	t.Helper()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, bundle_fingerprint, started_at)
		VALUES ($1::uuid, $2, $3, $4, $5, now())
	`, runID, status, bundleHash, bundleSource, testBootBundleFingerprint); err != nil {
		t.Fatalf("seed bundle delete run %s: %v", runID, err)
	}
}

func assertBundleDeleteRunBundle(t *testing.T, ctx context.Context, pg *PostgresStore, runID, wantStatus, wantSource, wantHash string) {
	t.Helper()
	var status, source, bundleHash string
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT status, bundle_source, COALESCE(bundle_hash, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&status, &source, &bundleHash); err != nil {
		t.Fatalf("load run bundle %s: %v", runID, err)
	}
	if status != wantStatus || source != wantSource || bundleHash != wantHash {
		t.Fatalf("run bundle %s = status:%s source:%s hash:%s, want %s/%s/%s", runID, status, source, bundleHash, wantStatus, wantSource, wantHash)
	}
}

func assertBundleDeleteRun(t *testing.T, ctx context.Context, pg *PostgresStore, runID, wantStatus, wantSource, wantControlReason string) {
	t.Helper()
	var status, source string
	var failure []byte
	var endedAt sql.NullTime
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT status, bundle_source, failure, ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&status, &source, &failure, &endedAt); err != nil {
		t.Fatalf("load run %s: %v", runID, err)
	}
	if status != wantStatus || source != wantSource || len(failure) != 0 {
		t.Fatalf("run %s = status:%s source:%s failure:%s ended:%v, want %s/%s/no-failure", runID, status, source, failure, endedAt.Valid, wantStatus, wantSource)
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
		if controlStatus != preservationcleanup.RunControlStatusStopped || reason != wantControlReason || controlledBy != preservationcleanup.BundleForceDeleteControlledBy {
			t.Fatalf("run control %s = %s/%s/%s, want stopped/%s/%s", runID, controlStatus, reason, controlledBy, preservationcleanup.BundleForceDeletedReason, preservationcleanup.BundleForceDeleteControlledBy)
		}
	}
}

func assertBundleDeleteRunAbsent(t *testing.T, ctx context.Context, pg *PostgresStore, runID string) {
	t.Helper()
	var count int
	if err := pg.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE run_id = $1::uuid`, runID).Scan(&count); err != nil {
		t.Fatalf("count run %s: %v", runID, err)
	}
	if count != 0 {
		t.Fatalf("run %s count = %d, want absent", runID, count)
	}
}

func assertBundleDeleteEventAbsent(t *testing.T, ctx context.Context, pg *PostgresStore, eventID string) {
	t.Helper()
	var count int
	if err := pg.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`, eventID).Scan(&count); err != nil {
		t.Fatalf("count event %s: %v", eventID, err)
	}
	if count != 0 {
		t.Fatalf("event %s count = %d, want absent", eventID, count)
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
