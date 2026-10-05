package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type faultingTerminalActivityJournal struct {
	ActivityAttemptJournal
	fault      error
	uncertain  bool
	faultCount int
}

func (j *faultingTerminalActivityJournal) CompleteActivityAttempt(ctx context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, bool, error) {
	stored, committed, err := j.ActivityAttemptJournal.CompleteActivityAttempt(ctx, record)
	if committed && err == nil && !j.uncertain {
		j.faultCount++
		return stored, true, j.fault
	}
	return stored, committed, err
}

func (j *faultingTerminalActivityJournal) MarkActivityAttemptUncertain(ctx context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, bool, error) {
	stored, committed, err := j.ActivityAttemptJournal.MarkActivityAttemptUncertain(ctx, record)
	if committed && err == nil && j.uncertain {
		j.faultCount++
		return stored, true, j.fault
	}
	return stored, committed, err
}

func TestActivityTerminalPostCommitErrorPublishesJournaledResultBothStores(t *testing.T) {
	for _, tc := range activityBoringStoreCases() {
		for _, uncertain := range []bool{false, true} {
			name := "complete"
			if uncertain {
				name = "uncertain"
			}
			t.Run(tc.name+"/"+name, func(t *testing.T) {
				ctx := testAuthorActivityContext(t, context.Background())
				runID := uuid.NewString()
				db, store, sqlite := newActivityJournalStoreForCase(t, ctx, tc.kind)
				seedActivityRun(t, db, sqlite, runID)
				fault := errors.New("injected activity post-commit cleanup fault")
				journal := &faultingTerminalActivityJournal{ActivityAttemptJournal: store.activityJournal, fault: fault, uncertain: uncertain}
				store.activityJournal = journal

				calls := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					calls++
					_, _ = io.WriteString(w, `{"ok":true,"result":{"message_id":42}}`)
				}))
				defer server.Close()
				tool := testCompiledChannelActivityTool(server.URL)
				source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{"channel.ops.deliver": tool}})
				bus := &recordingPipelineBus{}
				pc := newDurablePipelineCoordinatorForTest(bus, db, PipelineCoordinatorOptions{
					Module: staticSemanticWorkflowModule{source: source}, Persistence: workflowPersistenceForTest(store), PipelineObligations: unavailablePipelineTestObligationOwner{},
				})
				dispatcher := pipelineActivityDispatcher{coordinator: pc}
				if uncertain {
					dispatcher.client = &http.Client{Transport: activityRoundTripFunc(func(*http.Request) (*http.Response, error) {
						calls++
						return nil, fmt.Errorf("connection reset after dispatch")
					})}
				}
				intent := testNonIdempotentActivityIntent(runID, uuid.NewString(), uuid.NewString())
				intent.Tool = "channel.ops.deliver"
				intent.ActivityID = "channel_deliver"
				intent.SuccessEvent = "channel.deliver.succeeded"
				intent.FailureEvent = "channel.deliver.failed"
				intent.Input = mustActivityInput(map[string]any{"message_id": 42})
				intent = intent.Normalized()
				if err := dispatcher.executeNonIdempotentActivityIntent(ctx, intent, tool, nil); !errors.Is(err, fault) {
					t.Fatalf("terminal execution error = %v, want post-commit fault", err)
				}
				if journal.faultCount != 1 || calls != 1 || len(bus.publishes) != 1 {
					t.Fatalf("committed activity follow-up: faults=%d provider_calls=%d publications=%d", journal.faultCount, calls, len(bus.publishes))
				}
				stored, found, err := store.LoadActivityAttempt(ctx, activityRequestEventID(intent))
				if err != nil || !found {
					t.Fatalf("load terminal attempt: found=%t err=%v", found, err)
				}
				wantStatus := ActivityAttemptStatusSucceeded
				if uncertain {
					wantStatus = ActivityAttemptStatusUncertain
				}
				if stored.Status != wantStatus || bus.publishes[0].ID() != stored.ResultEventID {
					t.Fatalf("journaled publication = status %q event %q, persisted = status %q event %q", wantStatus, bus.publishes[0].ID(), stored.Status, stored.ResultEventID)
				}
				if err := dispatcher.executeNonIdempotentActivityIntent(ctx, intent, tool, nil); err != nil {
					t.Fatalf("replay terminal attempt: %v", err)
				}
				if calls != 1 || len(bus.publishes) != 2 || bus.publishes[1].ID() != stored.ResultEventID {
					t.Fatalf("terminal replay: provider_calls=%d publications=%d", calls, len(bus.publishes))
				}
			})
		}
	}
}

func TestActivityTerminalNonzeroRecordWithoutAcknowledgmentDoesNotPublish(t *testing.T) {
	bus := &recordingPipelineBus{}
	dispatcher := pipelineActivityDispatcher{coordinator: &PipelineCoordinator{bus: bus}}
	fault := errors.New("commit was not acknowledged")
	record := ActivityAttemptRecord{RequestEventID: uuid.NewString(), ResultEventID: uuid.NewString(), ResultEventType: "activity.succeeded", ResultPayload: map[string]any{"ok": true}}
	err := dispatcher.publishCommittedActivityAttempt(context.Background(), testActivityIntent("https://example.com"), record, false, fault, "complete_activity_attempt")
	if !errors.Is(err, fault) || len(bus.publishes) != 0 {
		t.Fatalf("unacknowledged terminal record: error=%v publications=%d", err, len(bus.publishes))
	}
}
