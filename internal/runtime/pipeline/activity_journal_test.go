package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestActivityAttemptJournalSQLiteAndPostgres(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
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
			conflicting := start
			conflicting.InputHash = "sha256:conflicting"
			if _, _, err := journal.StartActivityAttempt(ctx, conflicting); err == nil {
				t.Fatal("conflicting request identity was accepted")
			}
			conflictingMode := start
			conflictingMode.ExecutionMode = executionmode.Mock
			if _, _, err := journal.StartActivityAttempt(ctx, conflictingMode); err == nil {
				t.Fatal("cross-mode request identity was accepted")
			}

			payload := activitySuccessPayload(intent, map[string]any{"ok": true})
			terminal := started.withTerminal(ActivityAttemptStatusSucceeded, activityResultEventID(intent, intent.SuccessEvent), intent.SuccessEvent, payload, nil)
			conflictingTerminal := terminal
			conflictingTerminal.ExecutionMode = executionmode.Mock
			if _, err := journal.CompleteActivityAttempt(ctx, conflictingTerminal); err == nil {
				t.Fatal("cross-mode terminal transition was accepted")
			}
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
			if !sqlite {
				var revisions int
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id = $1::uuid`, runID).Scan(&revisions); err != nil {
					t.Fatalf("count activity-only run fork revisions: %v", err)
				}
				if revisions != 0 {
					t.Fatalf("activity-only run fork revisions = %d, want 0 for post-frontier selected-execution evidence", revisions)
				}
			}
		})
	}
}

func TestActivityAttemptJournalPreservesReplyContextAcrossRestart(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())
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

func TestLoopActivityClaimOrdersAgainstRepeatAndCloseOnBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)

			t.Run("claim_wins", func(t *testing.T) {
				activation, entityID := seedLoopActivityInstance(t, store, ctx, "review")
				record := loopActivityStartRecord(ctx, activation, entityID, uuid.NewString())
				started, inserted, err := store.ClaimActivityAttemptForLoopGeneration(ctx, record)
				if err != nil || !inserted || started.Status != ActivityAttemptStatusStarted {
					t.Fatalf("claim = %#v inserted=%v err=%v", started, inserted, err)
				}
				advanceLoopActivityInstance(t, store, ctx, entityID, "repeat")
				loaded, ok, err := store.LoadActivityAttempt(ctx, record.RequestEventID)
				if err != nil || !ok || loaded.Status != ActivityAttemptStatusStarted || !loaded.Generation.Equal(activation.Generation()) {
					t.Fatalf("started claim after repeat = %#v found=%v err=%v", loaded, ok, err)
				}
			})

			for _, operation := range []string{"repeat", "close"} {
				t.Run(operation+"_wins", func(t *testing.T) {
					activation, entityID := seedLoopActivityInstance(t, store, ctx, "review")
					record := loopActivityStartRecord(ctx, activation, entityID, uuid.NewString())
					advanceLoopActivityInstance(t, store, ctx, entityID, operation)
					if _, _, err := store.ClaimActivityAttemptForLoopGeneration(ctx, record); !isFailureClass(err, runtimefailures.ClassStaleArrival) {
						t.Fatalf("claim after %s error = %v, want stale_arrival", operation, err)
					}
					if _, ok, err := store.LoadActivityAttempt(ctx, record.RequestEventID); err != nil || ok {
						t.Fatalf("attempt after %s = found %v err %v, want no started row", operation, ok, err)
					}
				})
			}

			t.Run("duplicate_claim", func(t *testing.T) {
				activation, entityID := seedLoopActivityInstance(t, store, ctx, "review")
				record := loopActivityStartRecord(ctx, activation, entityID, uuid.NewString())
				if _, inserted, err := store.ClaimActivityAttemptForLoopGeneration(ctx, record); err != nil || !inserted {
					t.Fatalf("first claim inserted=%v err=%v", inserted, err)
				}
				if existing, inserted, err := store.ClaimActivityAttemptForLoopGeneration(ctx, record); err != nil || inserted || existing.Status != ActivityAttemptStatusStarted {
					t.Fatalf("duplicate claim = %#v inserted=%v err=%v", existing, inserted, err)
				}
			})
		})
	}
}

func seedLoopActivityInstance(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, stage string) (loopruntime.Activation, string) {
	t.Helper()
	runID := runtimecorrelation.RunIDFromContext(ctx)
	path := "validation/" + uuid.NewString()
	entityID := FlowInstanceEntityID(path)
	activation, err := loopruntime.New(runID, entityID, "validation", "revision", "revision_id", uuid.NewString(), stage, 3, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	buckets := map[string]map[string]any{}
	if err := loopruntime.Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	carrier := runtimeengine.NewStateCarrier(map[string]any{}, nil, buckets)
	if err := store.Upsert(ctx, WorkflowInstance{
		InstanceID: uuid.NewString(), StorageRef: path, WorkflowName: "validation", WorkflowVersion: "1.0.0",
		CurrentState: stage, EnteredStageAt: time.Now().UTC(), Metadata: map[string]any{"entity_id": entityID},
		StateBuckets: carrier.PersistedStateBuckets(),
	}); err != nil {
		t.Fatal(err)
	}
	return activation, entityID
}

func loopActivityStartRecord(ctx context.Context, activation loopruntime.Activation, entityID, sourceEventID string) ActivityAttemptRecord {
	intent := testNonIdempotentActivityIntent(runtimecorrelation.RunIDFromContext(ctx), sourceEventID, entityID)
	intent.Generation = activation.Generation()
	intent.LoopStage = activation.CurrentStage
	return activityAttemptStartRecord(intent, activityInputHash(intent.Input))
}

func advanceLoopActivityInstance(t *testing.T, store *WorkflowInstanceStore, ctx context.Context, entityID, operation string) {
	t.Helper()
	if err := store.MutateE(ctx, entityID, func(instance *WorkflowInstance) error {
		carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
		if err != nil {
			return err
		}
		activation, ok, err := loopruntime.Load(carrier.StateBuckets, "validation", "revision")
		if err != nil || !ok {
			return err
		}
		now := time.Now().UTC()
		switch operation {
		case "repeat":
			_, err = activation.Repeat("drafting", uuid.NewString(), now)
		case "close":
			err = activation.Close("approved", uuid.NewString(), now)
		}
		if err != nil {
			return err
		}
		if err := loopruntime.Store(carrier.StateBuckets, activation); err != nil {
			return err
		}
		instance.CurrentState = activation.CurrentStage
		instance.StateBuckets = carrier.PersistedStateBuckets()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func isFailureClass(err error, class runtimefailures.Class) bool {
	envelope, ok := runtimefailures.EnvelopeFromError(err)
	return ok && envelope.Class == class
}

func seedActivityReplyContext(t *testing.T, db *sql.DB, sqlite bool, runID, requestEventID, replyContextID string) {
	t.Helper()
	if sqlite {
		return
	}
	if _, err := db.Exec(`
		INSERT INTO events (execution_mode, run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at)
		VALUES ('live', $1::uuid, $2::uuid, 'provider.requested', 'global', '{}'::jsonb, 'test', 'platform', now())
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
