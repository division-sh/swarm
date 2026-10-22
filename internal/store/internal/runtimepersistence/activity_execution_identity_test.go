package runtimepersistence

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestActivitySettlementKeepsOriginalNativeIdentityBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			journal := fixture.store.(activityStoryJournal)
			for _, operation := range []string{"complete", "uncertain"} {
				t.Run(operation, func(t *testing.T) {
					// A retained legacy row is evidence, not permission to assign a new role on a callback.
					original := seedActivityStoryAttempt(t, fixture, executionmode.Live, "started", false)
					status := "succeeded"
					if operation == "uncertain" {
						status = "uncertain"
					}
					terminal := activityStoryTerminal(t, original, status)
					before := activityStoryOccurrences(t, fixture, backend.name == "postgres", original.RunID)
					head := activityJournalSequenceHead(t, fixture.db)
					for _, change := range []struct {
						name string
						edit func(*runtimepipeline.ActivityAttemptRecord)
					}{
						{"source", func(r *runtimepipeline.ActivityAttemptRecord) { r.SourceEventID = uuid.NewString() }},
						{"parent", func(r *runtimepipeline.ActivityAttemptRecord) { r.ParentEventID = uuid.NewString() }},
						{"reply", func(r *runtimepipeline.ActivityAttemptRecord) { r.ReplyContextID = "another-reply" }},
						{"input", func(r *runtimepipeline.ActivityAttemptRecord) { r.InputHash = "different-input" }},
						{"attempt", func(r *runtimepipeline.ActivityAttemptRecord) { r.Attempt = 2 }},
						{"missing_attempt", func(r *runtimepipeline.ActivityAttemptRecord) { r.Attempt = 0 }},
						{"late_binding", func(r *runtimepipeline.ActivityAttemptRecord) {
							r.ExecutionBinding = processbinding.ActivityExecutionBinding{Version: processbinding.ActivityExecutionBindingVersion, GrantID: uuid.NewString(), StateVersion: 3}
						}},
					} {
						t.Run(change.name, func(t *testing.T) {
							candidate := terminal
							change.edit(&candidate)
							var got runtimepipeline.ActivityAttemptRecord
							var committed bool
							var err error
							if operation == "complete" {
								got, committed, err = journal.CompleteActivityAttempt(testAuthorActivityContext(), candidate)
							} else {
								got, committed, err = journal.MarkActivityAttemptUncertain(testAuthorActivityContext(), candidate)
							}
							if err == nil || committed || got.RequestEventID != "" {
								t.Fatalf("changed callback committed: record=%+v committed=%t err=%v", got, committed, err)
							}
							if change.name == "attempt" || change.name == "missing_attempt" {
								if !strings.Contains(err.Error(), "requires the original exact attempt") {
									t.Fatalf("wrong attempt refusal: %v", err)
								}
							} else if failure, ok := runtimefailures.EnvelopeFromError(err); !ok || failure.Class != runtimefailures.ClassConflictingDuplicate {
								t.Fatalf("wrong identity refusal: %v", err)
							}
							loaded, found, err := journal.LoadActivityAttempt(testAuthorActivityContext(), original.RequestEventID)
							if err != nil || !found || !reflect.DeepEqual(loaded, original) {
								t.Fatalf("refused callback changed original receipt: %+v found=%t err=%v", loaded, found, err)
							}
							if after := activityStoryOccurrences(t, fixture, backend.name == "postgres", original.RunID); !reflect.DeepEqual(after, before) || activityJournalSequenceHead(t, fixture.db) != head {
								t.Fatal("refused callback emitted story or advanced its sequence")
							}
						})
					}
				})
			}
		})
	}
}
