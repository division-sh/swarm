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
			terminal := started.withTerminal(ActivityAttemptStatusSucceeded, activityResultEventID(intent, intent.SuccessEvent), intent.SuccessEvent, payload, "")
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
