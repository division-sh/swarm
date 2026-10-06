package pipeline

import (
	"context"
	"reflect"
	"testing"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/google/uuid"
)

func VerifyActivityJournalFixtureTerminalNoopBothStoresForTest(t *testing.T, open func(*testing.T, string) WorkflowActivityNativeFixtureForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := open(t, backend)
			store, ctx := fixture.Persistence.store, fixture.Context
			runID := uuid.NewString()
			if err := fixture.RequireRun(ctx, runID); err != nil {
				t.Fatal(err)
			}
			ctx = runtimecorrelation.WithRunID(ctx, runID)
			for _, status := range []string{ActivityAttemptStatusSucceeded, ActivityAttemptStatusFailed, ActivityAttemptStatusUncertain} {
				t.Run(status, func(t *testing.T) {
					intent := testNonIdempotentActivityIntent(runtimecorrelation.RunIDFromContext(ctx), uuid.NewString(), uuid.NewString())
					record := activityAttemptStartRecord(intent, activityInputHash(intent.Input))
					started, inserted, err := store.StartActivityAttempt(ctx, record)
					if err != nil || !inserted {
						t.Fatalf("start: %v inserted=%v", err, inserted)
					}
					terminal := started.withTerminal(status, uuid.NewString(), intent.SuccessEvent, map[string]any{"ok": true}, nil)
					failure, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "fixture_story_test", "activity-runtime", "execute", nil))
					if !ok {
						t.Fatal("missing failure")
					}
					if status != ActivityAttemptStatusSucceeded {
						terminal.Failure, terminal.ResultEventType = &failure, intent.FailureEvent
					}
					var committed bool
					terminal, committed, err = store.CompleteActivityAttempt(ctx, terminal)
					if err != nil || !committed {
						t.Fatal(err)
					}
					before, err := fixture.ReadJournal(ctx, runID)
					if err != nil {
						t.Fatal(err)
					}
					again, inserted, err := store.StartActivityAttempt(ctx, terminal)
					if err != nil || inserted || !reflect.DeepEqual(again, terminal) {
						t.Fatalf("duplicate start: %v inserted=%v", err, inserted)
					}
					again, committed, err = store.CompleteActivityAttempt(ctx, terminal)
					if err != nil || !committed || !reflect.DeepEqual(again, terminal) {
						t.Fatalf("duplicate completion: %v", err)
					}
					uncertain := terminal
					uncertain.Failure, uncertain.ResultEventType = &failure, intent.FailureEvent
					again, committed, err = store.MarkActivityAttemptUncertain(ctx, uncertain)
					if err != nil || !committed || !reflect.DeepEqual(again, terminal) {
						t.Fatalf("terminal uncertainty: %v", err)
					}
					after, err := fixture.ReadJournal(ctx, runID)
					if err != nil {
						t.Fatal(err)
					}
					if before.StoryCount != after.StoryCount || before.StoryHead != after.StoryHead {
						t.Fatal("fixture invented a no-op story")
					}
				})
			}
		})
	}
}

func VerifyActivityAttemptJournalSQLiteAndPostgresForTest(t *testing.T, open func(*testing.T, string) WorkflowActivityNativeFixtureForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := open(t, backend)
			journal, ctx := fixture.Persistence.store, fixture.Context
			runID := uuid.NewString()
			if err := fixture.RequireRun(ctx, runID); err != nil {
				t.Fatal(err)
			}
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
			if _, _, err := journal.CompleteActivityAttempt(ctx, conflictingTerminal); err == nil {
				t.Fatal("cross-mode terminal transition was accepted")
			}
			completed, committed, err := journal.CompleteActivityAttempt(ctx, terminal)
			if err != nil || !committed {
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
			if backend == "postgres" {
				observed, err := fixture.ReadJournal(ctx, runID)
				if err != nil {
					t.Fatalf("count activity-only run fork revisions: %v", err)
				}
				if observed.RunForkRevisions != 0 {
					t.Fatalf("activity-only run fork revisions = %d, want 0 for post-frontier selected-execution evidence", observed.RunForkRevisions)
				}
			}
		})
	}
}

func VerifyActivityAttemptJournalPreservesReplyContextAcrossRestartForTest(t *testing.T, open func(*testing.T, string) WorkflowActivityNativeFixtureForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := open(t, backend)
			journal, ctx := fixture.Persistence.store, fixture.Context
			runID := uuid.NewString()
			if err := fixture.RequireRun(ctx, runID); err != nil {
				t.Fatal(err)
			}
			requestEventID := uuid.NewString()
			replyContextID := "reply-v1:activity-" + uuid.NewString()
			if err := fixture.CreateReply(ctx, runID, requestEventID, replyContextID); err != nil {
				t.Fatal(err)
			}
			intent := testNonIdempotentActivityIntent(runID, uuid.NewString(), uuid.NewString())
			start := activityAttemptStartRecord(intent, activityInputHash(intent.Input))
			start.ReplyContextID = replyContextID
			if _, inserted, err := journal.StartActivityAttempt(ctx, start); err != nil || !inserted {
				t.Fatalf("StartActivityAttempt inserted=%v err=%v", inserted, err)
			}
			restarted := fixture.Reopen().Persistence.store
			loaded, ok, err := restarted.LoadActivityAttempt(ctx, start.RequestEventID)
			if err != nil || !ok || loaded.ReplyContextID != replyContextID {
				t.Fatalf("restarted activity attempt = %#v ok=%v err=%v", loaded, ok, err)
			}
		})
	}
}

func VerifyLoopActivityClaimOrdersAgainstRepeatAndCloseOnBothStoresForTest(t *testing.T, open func(*testing.T, string) WorkflowActivityNativeFixtureForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := open(t, backend)
			store, ctx := fixture.Persistence.store, fixture.Context
			runID := uuid.NewString()
			if err := fixture.RequireRun(ctx, runID); err != nil {
				t.Fatal(err)
			}
			ctx = runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(ctx, runID), executionmode.Live)

			t.Run("claim_wins", func(t *testing.T) {
				activation, flowInstance, entityID := seedNativeLoopActivityInstance(t, fixture, ctx, "review")
				record := loopActivityStartRecord(ctx, activation, flowInstance, entityID, uuid.NewString())
				started, inserted, err := store.ClaimActivityAttemptForLoopGeneration(ctx, record)
				if err != nil || !inserted || started.Status != ActivityAttemptStatusStarted {
					t.Fatalf("claim = %#v inserted=%v err=%v", started, inserted, err)
				}
				advanceLoopActivityInstance(t, store, ctx, flowInstance, "repeat")
				loaded, ok, err := store.LoadActivityAttempt(ctx, record.RequestEventID)
				if err != nil || !ok || loaded.Status != ActivityAttemptStatusStarted || !loaded.Generation.Equal(activation.Generation()) {
					t.Fatalf("started claim after repeat = %#v found=%v err=%v", loaded, ok, err)
				}
			})

			for _, operation := range []string{"repeat", "close"} {
				t.Run(operation+"_wins", func(t *testing.T) {
					activation, flowInstance, entityID := seedNativeLoopActivityInstance(t, fixture, ctx, "review")
					record := loopActivityStartRecord(ctx, activation, flowInstance, entityID, uuid.NewString())
					advanceLoopActivityInstance(t, store, ctx, flowInstance, operation)
					if _, _, err := store.ClaimActivityAttemptForLoopGeneration(ctx, record); !isFailureClass(err, runtimefailures.ClassStaleArrival) {
						t.Fatalf("claim after %s error = %v, want stale_arrival", operation, err)
					}
					if _, ok, err := store.LoadActivityAttempt(ctx, record.RequestEventID); err != nil || ok {
						t.Fatalf("attempt after %s = found %v err %v, want no started row", operation, ok, err)
					}
				})
			}

			t.Run("duplicate_claim", func(t *testing.T) {
				activation, flowInstance, entityID := seedNativeLoopActivityInstance(t, fixture, ctx, "review")
				record := loopActivityStartRecord(ctx, activation, flowInstance, entityID, uuid.NewString())
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

func seedNativeLoopActivityInstance(t *testing.T, fixture WorkflowActivityNativeFixtureForTest, ctx context.Context, stage string) (loopruntime.Activation, string, string) {
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
	if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: runtimeflowidentity.LogicalInstanceID(path), StorageRef: path, EntityID: entityID,
		WorkflowName: "validation", WorkflowVersion: "1.0.0",
		CurrentState: stage, EnteredStageAt: time.Now().UTC(), Fields: map[string]any{},
		StateBuckets: carrier.PersistedStateBuckets(),
		EntityType:   "test_entity",
	})); err != nil {
		t.Fatal(err)
	}
	return activation, path, entityID
}

func loopActivityStartRecord(ctx context.Context, activation loopruntime.Activation, flowInstance, entityID, sourceEventID string) ActivityAttemptRecord {
	intent := testNonIdempotentActivityIntent(runtimecorrelation.RunIDFromContext(ctx), sourceEventID, entityID)
	intent.FlowInstance = flowInstance
	intent.Generation = activation.Generation()
	intent.LoopStage = activation.CurrentStage
	return activityAttemptStartRecord(intent, activityInputHash(intent.Input))
}

func advanceLoopActivityInstance(t *testing.T, store *workflowInstanceStore, ctx context.Context, flowInstance, operation string) {
	t.Helper()
	if err := store.mutateE(ctx, testRunScopedWorkflowInstanceForRun(runtimecorrelation.RunIDFromContext(ctx), flowInstance), func(instance *WorkflowInstance) error {
		carrier, err := workflowInstanceStateCarrier(*instance)
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
