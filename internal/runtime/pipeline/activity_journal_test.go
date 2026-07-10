package pipeline

import (
	"context"
	"database/sql"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestActivityAttemptJournalSQLiteAndPostgres(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name  string
		store func(t *testing.T, ctx context.Context) (*sql.DB, *WorkflowInstanceStore, bool)
	}{
		{
			name: "sqlite",
			store: func(t *testing.T, ctx context.Context) (*sql.DB, *WorkflowInstanceStore, bool) {
				db, journal := newSQLiteActivityJournalStore(t, ctx)
				return db, journal, true
			},
		},
		{
			name: "postgres",
			store: func(t *testing.T, ctx context.Context) (*sql.DB, *WorkflowInstanceStore, bool) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return db, NewWorkflowInstanceStore(db), false
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, journal, sqlite := tc.store(t, ctx)
			runID := uuid.NewString()
			seedActivityRun(t, db, sqlite, runID)
			intent := testNonIdempotentActivityIntent(runID, uuid.NewString(), uuid.NewString())
			start := activityAttemptStartRecord(intent, activityInputHash(intent.Input))

			started, inserted, err := journal.StartActivityAttempt(ctx, start)
			if err != nil {
				t.Fatalf("StartActivityAttempt: %v", err)
			}
			if !inserted || started.Status != ActivityAttemptStatusStarted {
				t.Fatalf("started = (%v, %q), want inserted started", inserted, started.Status)
			}

			again, inserted, err := journal.StartActivityAttempt(ctx, start)
			if err != nil {
				t.Fatalf("duplicate StartActivityAttempt: %v", err)
			}
			if inserted || again.RequestEventID != started.RequestEventID || again.Status != ActivityAttemptStatusStarted {
				t.Fatalf("duplicate start = (%v, %#v), want existing started", inserted, again)
			}

			payload := activitySuccessPayload(intent, map[string]any{"ok": true})
			terminal := started.withTerminal(ActivityAttemptStatusSucceeded, activityResultEventID(intent, intent.SuccessEvent), intent.SuccessEvent, payload, nil)
			completed, err := journal.CompleteActivityAttempt(ctx, terminal)
			if err != nil {
				t.Fatalf("CompleteActivityAttempt: %v", err)
			}
			if completed.Status != ActivityAttemptStatusSucceeded || completed.ResultEventID == "" {
				t.Fatalf("completed journal = %#v, want succeeded with result event", completed)
			}

			terminalAgain, inserted, err := journal.StartActivityAttempt(ctx, start)
			if err != nil {
				t.Fatalf("terminal duplicate StartActivityAttempt: %v", err)
			}
			if inserted || terminalAgain.Status != ActivityAttemptStatusSucceeded {
				t.Fatalf("terminal duplicate = (%v, %q), want existing succeeded", inserted, terminalAgain.Status)
			}
		})
	}
}

func TestActivityAttemptJournalPreservesReplyContextAcrossRestart(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name  string
		store func(t *testing.T, ctx context.Context) (*sql.DB, *WorkflowInstanceStore, bool)
	}{
		{
			name: "sqlite",
			store: func(t *testing.T, ctx context.Context) (*sql.DB, *WorkflowInstanceStore, bool) {
				db, journal := newSQLiteActivityJournalStore(t, ctx)
				return db, journal, true
			},
		},
		{
			name: "postgres",
			store: func(t *testing.T, ctx context.Context) (*sql.DB, *WorkflowInstanceStore, bool) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return db, NewWorkflowInstanceStore(db), false
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, journal, sqlite := tc.store(t, ctx)
			runID := uuid.NewString()
			seedActivityRun(t, db, sqlite, runID)
			requestEventID := uuid.NewString()
			replyContextID := "reply-v1:activity-" + uuid.NewString()
			seedActivityReplyContext(t, db, sqlite, runID, requestEventID, replyContextID)
			intent := testNonIdempotentActivityIntent(runID, uuid.NewString(), uuid.NewString())
			start := activityAttemptStartRecord(intent, activityInputHash(intent.Input))
			start.ReplyContextID = replyContextID
			if _, inserted, err := journal.StartActivityAttempt(ctx, start); err != nil || !inserted {
				t.Fatalf("StartActivityAttempt inserted=%v err=%v", inserted, err)
			}
			restarted := NewWorkflowInstanceStore(db)
			if sqlite {
				restarted = NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, &recordingRuntimeMutationRunner{db: db})
			}
			loaded, ok, err := restarted.LoadActivityAttempt(ctx, start.RequestEventID)
			if err != nil || !ok || loaded.ReplyContextID != replyContextID {
				t.Fatalf("restarted activity attempt = %#v ok=%v err=%v", loaded, ok, err)
			}
		})
	}
}

func seedActivityReplyContext(t *testing.T, db *sql.DB, sqlite bool, runID, requestEventID, replyContextID string) {
	t.Helper()
	if sqlite {
		return
	}
	if _, err := db.Exec(`
		INSERT INTO events (run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at)
		VALUES ($1::uuid, $2::uuid, 'provider.requested', 'global', '{}'::jsonb, 'test', 'platform', now())
	`, runID, requestEventID); err != nil {
		t.Fatalf("seed postgres reply request event: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO reply_contexts (
			reply_context_id, run_id, request_event_id, requester_flow_id, request_output_pin,
			reply_input_pin, provider_flow_id, provider_input_pin, provider_output_pin,
			origin_route, request_correlation_id, state, created_at, updated_at
		)
		VALUES ($1, $2::uuid, $3::uuid, 'requester', 'provider_requested', 'provider_replied', 'provider',
			'provider_requested', 'provider_replied', '{"flow_id":"requester","flow_instance":"requester/a","entity_id":"entity-a"}'::jsonb, $3, 'open', now(), now())
	`, replyContextID, runID, requestEventID); err != nil {
		t.Fatalf("seed postgres reply context: %v", err)
	}
}
