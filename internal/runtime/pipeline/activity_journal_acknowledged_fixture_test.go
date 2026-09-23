package pipeline

import (
	"errors"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func TestActivityJournalFixtureTerminalAcknowledgementSurvivesPostCommitError(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		for _, uncertain := range []bool{false, true} {
			name := "complete"
			if uncertain {
				name = "uncertain"
			}
			t.Run(tc.name+"/"+name, func(t *testing.T) {
				store, ctx := tc.open(t)
				runner, ok := store.testRuntimeMutation().(*recordingRuntimeMutationRunner)
				if !ok {
					t.Fatalf("activity journal fixture runner = %T", store.testRuntimeMutation())
				}
				intent := testNonIdempotentActivityIntent(runtimecorrelation.RunIDFromContext(ctx), uuid.NewString(), uuid.NewString())
				started, inserted, err := store.StartActivityAttempt(ctx, activityAttemptStartRecord(intent, activityInputHash(intent.Input)))
				if err != nil || !inserted {
					t.Fatalf("start: inserted=%t err=%v", inserted, err)
				}
				terminal := started.withTerminal(ActivityAttemptStatusSucceeded, uuid.NewString(), intent.SuccessEvent, map[string]any{"ok": true}, nil)
				if uncertain {
					failure, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "fixture_uncertain", "activity-runtime", "execute", nil))
					if !ok {
						t.Fatal("missing uncertain failure envelope")
					}
					terminal.Failure = &failure
					terminal.ResultEventType = intent.FailureEvent
				}
				fault := errors.New("selected mutation cleanup failed after acknowledgement")
				runner.postCommitErr = fault
				var committed bool
				if uncertain {
					terminal, committed, err = store.MarkActivityAttemptUncertain(ctx, terminal)
				} else {
					terminal, committed, err = store.CompleteActivityAttempt(ctx, terminal)
				}
				if !committed || !errors.Is(err, fault) {
					t.Fatalf("terminal acknowledgement=%t err=%v, want acknowledged cleanup fault", committed, err)
				}
				stored, found, err := store.LoadActivityAttempt(ctx, started.RequestEventID)
				if err != nil || !found || stored.Status != terminal.Status || stored.ResultEventID != terminal.ResultEventID {
					t.Fatalf("terminal row missing after cleanup fault: found=%t stored=%+v result=%+v err=%v", found, stored, terminal, err)
				}
			})
		}
	}
}
