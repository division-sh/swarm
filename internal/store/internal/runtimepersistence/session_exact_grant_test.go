package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestSelectedSameOwnerReplacementFencesEverySessionWriterBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		fixture := newExactFactFixture(t, selected)
		identity := agentmemory.Identity(mustTestAgentIdentityForRun(fixture.runID, "grant-agent", "support/instance-1"))
		seedTestAgentRow(t, testAuthorActivityContext(), selected.db, selected.postgres, identity, "active")
		ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
		store := selected.selected.(interface {
			sessions.Registry
			runtimellm.ConversationPersistence
		})
		first, err := store.Acquire(ctx, identity, "worker")
		if err != nil {
			t.Fatal(err)
		}
		second, err := store.Acquire(ctx, identity, "worker")
		if err != nil {
			t.Fatal(err)
		}
		if first.SessionID != second.SessionID || first.GrantID == second.GrantID {
			t.Fatalf("same-owner acquire did not supersede grant: first=%+v second=%+v", first, second)
		}
		first.ExpiresAt = second.ExpiresAt
		beforeConflict := snapshotRotationEffects(t, ctx, selected.db, fixture.runID)
		if _, err := store.Acquire(ctx, identity, "other-worker"); !errors.Is(err, sessions.ErrSessionLeased) {
			t.Fatalf("other owner acquired live grant: %v", err)
		}
		requireRotationEffectsUnchanged(t, beforeConflict, snapshotRotationEffects(t, ctx, selected.db, fixture.runID))
		conversation := runtimellm.ConversationRecord{
			SessionID: first.SessionID, AgentID: identity.AgentID(), Identity: identity, Memory: agentmemory.Authored(true),
			Messages: []runtimellm.Message{{Role: "assistant", Content: "stale"}}, TurnCount: 99, Status: "active",
		}
		watchdog := runtimellm.ConversationWatchdogUpdate{
			SessionID: first.SessionID, AgentID: identity.AgentID(), Identity: identity,
			Watchdog: &runtimellm.ConversationWatchdog{State: "healthy_long_running", BlockingLayer: "session_execution", Action: "turn_long_running", Outcome: "observed", LastOutputAt: "2026-07-15T12:00:00Z", RecordedAt: "2026-07-15T12:00:30Z"},
		}
		before := snapshotRotationEffects(t, ctx, selected.db, fixture.runID)
		if _, err := store.Renew(ctx, first); !errors.Is(err, sessions.ErrSessionLeased) {
			t.Fatalf("stale renew=%v", err)
		}
		if result, err := store.ReleaseOutcome(ctx, first); result.Acknowledged || err == nil {
			t.Fatalf("stale release=%+v err=%v", result, err)
		}
		if result, err := store.IncrementTurnOutcome(ctx, first); result.Acknowledged || err == nil {
			t.Fatalf("stale turn=%+v err=%v", result, err)
		}
		if _, err := store.Rotate(ctx, first, sessions.RotationMetadata{RetryReason: "stale"}); err == nil {
			t.Fatal("stale rotate succeeded")
		}
		if err := store.UpsertConversation(ctx, first, conversation); err == nil {
			t.Fatal("stale conversation persisted")
		}
		if err := store.UpdateLiveSessionWatchdog(ctx, first, watchdog); err == nil {
			t.Fatal("stale watchdog persisted")
		}
		requireRotationEffectsUnchanged(t, before, snapshotRotationEffects(t, ctx, selected.db, fixture.runID))

		renewed, err := store.Renew(ctx, second)
		if err != nil || renewed == nil || renewed.GrantID != second.GrantID || renewed.ExpiresAt.Before(second.ExpiresAt) {
			t.Fatalf("current renewal changed grant or regressed expiry: lease=%+v err=%v", renewed, err)
		}
		conversation.Messages = []runtimellm.Message{{Role: "assistant", Content: "current"}}
		conversation.TurnCount = 0
		if err := store.UpsertConversation(ctx, second, conversation); err != nil {
			t.Fatalf("current conversation: %v", err)
		}
		if err := store.UpdateLiveSessionWatchdog(ctx, second, watchdog); err != nil {
			t.Fatalf("current watchdog: %v", err)
		}
		if result, err := store.IncrementTurnOutcome(ctx, second); err != nil || !result.Acknowledged {
			t.Fatalf("current turn=%+v err=%v", result, err)
		}
		third, err := store.Rotate(ctx, second, sessions.RotationMetadata{RetryReason: "current"})
		if err != nil || third == nil || third.SessionID == second.SessionID || third.GrantID == second.GrantID {
			t.Fatalf("current rotation=%+v err=%v", third, err)
		}
		before = snapshotRotationEffects(t, ctx, selected.db, fixture.runID)
		if result, err := store.ReleaseOutcome(ctx, second); result.Acknowledged || err == nil {
			t.Fatalf("retired predecessor release=%+v err=%v", result, err)
		}
		requireRotationEffectsUnchanged(t, before, snapshotRotationEffects(t, ctx, selected.db, fixture.runID))
		if result, err := store.ReleaseOutcome(ctx, third); err != nil || !result.Acknowledged {
			t.Fatalf("current release=%+v err=%v", result, err)
		}
		expired, err := store.Acquire(ctx, identity, "worker")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := selected.db.ExecContext(ctx, `UPDATE agent_sessions SET lease_expires_at=$2 WHERE session_id=$1`, expired.SessionID, time.Now().UTC().Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Renew(ctx, expired); !errors.Is(err, sessions.ErrSessionLeased) {
			t.Fatalf("expired grant renewed: %v", err)
		}
		reclaimed, err := store.Acquire(ctx, identity, "other-worker")
		if err != nil || reclaimed == nil || reclaimed.GrantID == expired.GrantID || reclaimed.LockOwner != "other-worker" {
			t.Fatalf("expired grant reclaim=%+v err=%v", reclaimed, err)
		}
		beforeReclaimed := snapshotRotationEffects(t, ctx, selected.db, fixture.runID)
		if result, err := store.ReleaseOutcome(ctx, expired); result.Acknowledged || err == nil {
			t.Fatalf("expired predecessor cleared reclaimed grant: result=%+v err=%v", result, err)
		}
		requireRotationEffectsUnchanged(t, beforeReclaimed, snapshotRotationEffects(t, ctx, selected.db, fixture.runID))
		if _, err := store.Renew(ctx, reclaimed); err != nil {
			t.Fatal(err)
		}
		if result, err := store.ReleaseOutcome(ctx, reclaimed); err != nil || !result.Acknowledged {
			t.Fatalf("renewed exact grant refused old-expiry cleanup: result=%+v err=%v", result, err)
		}
		expiredExact, err := store.Acquire(ctx, identity, "worker")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := selected.db.ExecContext(ctx, `UPDATE agent_sessions SET lease_expires_at=$2 WHERE session_id=$1`, expiredExact.SessionID, time.Now().UTC().Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
		if result, err := store.ReleaseOutcome(ctx, expiredExact); err != nil || !result.Acknowledged {
			t.Fatalf("expired exact grant cleanup: result=%+v err=%v", result, err)
		}
		if result, err := store.ReleaseOutcome(ctx, expiredExact); result.Acknowledged || err == nil {
			t.Fatalf("duplicate expired cleanup: result=%+v err=%v", result, err)
		}
		var holder, grant string
		var expiryNull bool
		if err := selected.db.QueryRowContext(ctx, `SELECT COALESCE(lease_holder,''),COALESCE(lease_grant_id,''),lease_expires_at IS NULL FROM agent_sessions WHERE session_id=$1`, expiredExact.SessionID).Scan(&holder, &grant, &expiryNull); err != nil || holder != "" || grant != "" || !expiryNull {
			t.Fatalf("expired cleanup left authority: holder=%q grant=%q expiry_null=%v err=%v", holder, grant, expiryNull, err)
		}
	})
}

func TestPostgresOrdinarySessionMutationChecksExpiryAfterRowLock(t *testing.T) {
	for _, operation := range []string{"renew", "increment", "upsert", "watchdog"} {
		t.Run(operation, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			store := admitTestPostgresStore(t, db)
			fixture := newCompletionSettlementFixture(t, store, db, false)
			ctx := runtimeeffects.WithLifecycleToken(fixture.context, fixture.authority.Normal)
			lease, record, err := store.AcquireLiveSession(ctx, fixture.authority.Target.AgentIdentity, fixture.leaseHolder)
			if err != nil {
				t.Fatal(err)
			}
			var expiry time.Time
			if err := db.QueryRowContext(ctx, `UPDATE agent_sessions SET lease_expires_at=clock_timestamp()+INTERVAL '1500 milliseconds' WHERE session_id=$1::uuid RETURNING lease_expires_at`, lease.SessionID).Scan(&expiry); err != nil {
				t.Fatal(err)
			}
			before := snapshotRotationEffects(t, ctx, db, fixture.authority.Target.RunID)
			blocker, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = blocker.Rollback() }()
			if _, err := blocker.ExecContext(ctx, `SELECT 1 FROM agent_sessions WHERE session_id=$1::uuid FOR UPDATE`, lease.SessionID); err != nil {
				t.Fatal(err)
			}
			watchdog := runtimellm.ConversationWatchdogUpdate{
				SessionID: lease.SessionID, AgentID: record.AgentID, Identity: record.Identity,
				Watchdog: &runtimellm.ConversationWatchdog{State: "healthy_long_running", BlockingLayer: "session_execution", Action: "turn_long_running", Outcome: "observed", LastOutputAt: "2026-07-15T12:00:00Z", RecordedAt: "2026-07-15T12:00:30Z"},
			}
			record.Messages = []runtimellm.Message{{Role: "user", Content: "post-lock grant proof"}}
			mutate := func(lease *sessions.Lease) error {
				switch operation {
				case "renew":
					_, err := store.Renew(ctx, lease)
					return err
				case "increment":
					_, err := store.IncrementTurnOutcome(ctx, lease)
					return err
				case "upsert":
					return store.UpsertConversation(ctx, lease, record)
				default:
					return store.UpdateLiveSessionWatchdog(ctx, lease, watchdog)
				}
			}
			done := make(chan error, 1)
			go func() { done <- mutate(lease) }()
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			for {
				var waiting int
				if err := db.QueryRowContext(waitCtx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%lease_grant_id%' AND query LIKE '%agent_sessions%'`).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting > 0 {
					var observedAt time.Time
					if err := db.QueryRowContext(waitCtx, `SELECT clock_timestamp()`).Scan(&observedAt); err != nil {
						t.Fatal(err)
					}
					if !observedAt.Before(expiry) {
						t.Fatalf("%s did not enter row-lock wait before grant expiry: observed=%s expiry=%s", operation, observedAt, expiry)
					}
					break
				}
				select {
				case err := <-done:
					t.Fatalf("%s returned before row lock released: %v", operation, err)
				case <-waitCtx.Done():
					t.Fatalf("%s never waited on exact session row", operation)
				case <-time.After(10 * time.Millisecond):
				}
			}
			for {
				var now time.Time
				if err := db.QueryRowContext(waitCtx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
					t.Fatal(err)
				}
				if !now.Before(expiry) {
					break
				}
				select {
				case <-waitCtx.Done():
					t.Fatal("grant did not expire while writer waited")
				case <-time.After(10 * time.Millisecond):
				}
			}
			if err := blocker.Commit(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if !errors.Is(err, sessions.ErrSessionLeased) {
					t.Fatalf("expired %s result = %v, want exact-grant refusal", operation, err)
				}
			case <-waitCtx.Done():
				t.Fatalf("%s did not settle after row unlock", operation)
			}
			requireRotationEffectsUnchanged(t, before, snapshotRotationEffects(t, ctx, db, fixture.authority.Target.RunID))
			current, err := store.Acquire(ctx, fixture.authority.Target.AgentIdentity, fixture.leaseHolder)
			if err != nil {
				t.Fatal(err)
			}
			if err := mutate(current); err != nil {
				t.Fatalf("current %s refused: %v", operation, err)
			}
		})
	}
}

func TestPostgresSessionAcquireChecksExpiryAfterRowLock(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := admitTestPostgresStore(t, db)
	fixture := newCompletionSettlementFixture(t, store, db, false)
	registry, ok := fixture.store.(sessions.Registry)
	if !ok {
		t.Fatal("selected store has no session registry")
	}
	ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
	identity := agentmemory.Identity(fixture.authority.Normal.Identity)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	var sessionID string
	if err := tx.QueryRowContext(ctx, `SELECT session_id::text FROM agent_sessions WHERE session_id=$1::uuid FOR UPDATE`, fixture.sessionID).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	type acquireResult struct {
		lease *sessions.Lease
		err   error
	}
	acquired := make(chan acquireResult, 1)
	go func() {
		lease, err := registry.Acquire(ctx, identity, "worker-b")
		acquired <- acquireResult{lease: lease, err: err}
	}()
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		var waiting int
		if err := db.QueryRowContext(waitCtx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%FROM agent_sessions%'`).Scan(&waiting); err != nil {
			t.Fatalf("observe blocked acquire: %v", err)
		}
		if waiting > 0 {
			break
		}
		select {
		case <-waitCtx.Done():
			t.Fatal("acquire did not block on the session row")
		case <-time.After(10 * time.Millisecond):
		}
	}
	var expiresAt time.Time
	if err := tx.QueryRowContext(ctx, `UPDATE agent_sessions SET lease_expires_at=clock_timestamp()+INTERVAL '250 milliseconds' WHERE session_id=$1::uuid RETURNING lease_expires_at`, fixture.sessionID).Scan(&expiresAt); err != nil {
		t.Fatal(err)
	}
	for {
		var now time.Time
		if err := db.QueryRowContext(waitCtx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			t.Fatalf("observe expiry: %v", err)
		}
		if !now.Before(expiresAt) {
			break
		}
		select {
		case <-waitCtx.Done():
			t.Fatal("session grant did not expire before releasing row lock")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-acquired:
		if result.err != nil || result.lease == nil || result.lease.SessionID != fixture.sessionID || result.lease.GrantID == fixture.grantID {
			t.Fatalf("post-lock acquire=%+v err=%v", result.lease, result.err)
		}
	case <-waitCtx.Done():
		t.Fatal("post-lock acquire did not settle")
	}
}

func TestSelectedLeaseHeartbeatCannotRenewReplacementBothStores(t *testing.T) {
	for _, replacement := range []string{"same_owner_acquire", "rotation"} {
		t.Run(replacement, func(t *testing.T) {
			eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
				fixture := newExactFactFixture(t, selected)
				identity := agentmemory.Identity(mustTestAgentIdentityForRun(fixture.runID, "grant-agent", "support/instance-1"))
				seedTestAgentRow(t, testAuthorActivityContext(), selected.db, selected.postgres, identity, "active")
				ctx := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
				registry, ok := selected.selected.(sessions.Registry)
				if !ok {
					t.Fatal("selected store has no session registry")
				}
				first, err := registry.Acquire(ctx, identity, "worker")
				if err != nil {
					t.Fatal(err)
				}
				process := worklifetime.NewProcess()
				runtime, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: "selected-heartbeat-test", BundleHash: "selected-heartbeat-test"})
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if _, err := runtime.RetireAndWait(context.Background()); err != nil {
						t.Errorf("retire runtime: %v", err)
					}
					process.Retire()
					if _, err := process.Join(context.Background()); err != nil {
						t.Errorf("join process: %v", err)
					}
				}()
				work, err := runtime.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = work.Done() }()
				workCtx := worklifetime.WithOccurrence(work.Context(), runtime)
				scheduled := *first
				scheduled.ExpiresAt = time.Now().Add(10 * time.Second)
				failed := make(chan error, 1)
				stop := sessions.StartLeaseHeartbeatWithErrorHandler(workCtx, registry, &scheduled, func(err error) { failed <- err })
				defer stop()
				var successor *sessions.Lease
				if replacement == "rotation" {
					successor, err = registry.Rotate(workCtx, first, sessions.RotationMetadata{RetryReason: "heartbeat rotation"})
				} else {
					successor, err = registry.Acquire(workCtx, identity, "worker")
				}
				if err != nil || successor == nil || successor.GrantID == first.GrantID {
					t.Fatalf("replace grant: successor=%+v err=%v", successor, err)
				}
				before := snapshotRotationEffects(t, ctx, selected.db, fixture.runID)
				select {
				case err := <-failed:
					if !errors.Is(err, sessions.ErrSessionLeased) {
						t.Fatalf("stale heartbeat error=%v", err)
					}
				case <-time.After(12 * time.Second):
					t.Fatal("stale heartbeat did not report lost grant")
				}
				stop()
				requireRotationEffectsUnchanged(t, before, snapshotRotationEffects(t, ctx, selected.db, fixture.runID))
			})
		})
	}
}
