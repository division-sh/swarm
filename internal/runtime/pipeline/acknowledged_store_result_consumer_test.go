package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
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

type faultingInitialMaterializationOwner struct {
	WorkflowInitialMaterializationCommitOwner
	fault error
	used  bool
}

func (o *faultingInitialMaterializationOwner) CommitWorkflowInitialMaterialization(ctx context.Context, command WorkflowInitialMaterializationCommand) (CommittedWorkflowInitialMaterialization, error) {
	result, err := o.WorkflowInitialMaterializationCommitOwner.CommitWorkflowInitialMaterialization(ctx, command)
	if result.Committed && err == nil && !o.used {
		o.used = true
		return result, o.fault
	}
	return result, err
}

type unacknowledgedInitialMaterializationOwner struct{}

func (unacknowledgedInitialMaterializationOwner) CommitWorkflowInitialMaterialization(context.Context, WorkflowInitialMaterializationCommand) (CommittedWorkflowInitialMaterialization, error) {
	return CommittedWorkflowInitialMaterialization{Result: WorkflowInitialMaterializationCreated}, nil
}

func TestInitialMaterializationPostCommitErrorFinalizesLifecycleBothStores(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (*workflowInstanceStore, context.Context)
	}{
		{"sqlite", func(t *testing.T) (*workflowInstanceStore, context.Context) {
			db := newSQLiteWorkflowInstanceStoreTestDB(t)
			return newSQLiteWorkflowInstanceStoreForTest(t, db), withLiveWorkflowInitialEntry(sqliteExactOnceRunContext(t, db))
		}},
		{"postgres", func(t *testing.T) (*workflowInstanceStore, context.Context) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			return newPostgresWorkflowInstanceStoreForTest(db), withLiveWorkflowInitialEntry(testPipelineRunContext(t, db))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.setup(t)
			lifecycle := &workflowInitialMaterializationTestOwner{}
			store.lifecycleOwner = lifecycle
			fault := errors.New("injected initial materialization post-commit cleanup fault")
			commit := &faultingInitialMaterializationOwner{WorkflowInitialMaterializationCommitOwner: store.initialCommits, fault: fault}
			store.initialCommits = commit
			instance := WorkflowInstance{InstanceID: "inst-1", StorageRef: "review/inst-1", WorkflowName: "review", WorkflowVersion: "1.0.0", CurrentState: "pending", Fields: map[string]any{}, EntityType: "test_entity"}
			identity := testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef)
			at := time.Date(2026, time.July, 29, 12, 0, 0, 0, time.UTC)
			result, err := store.MaterializeInitialEntry(ctx, identity, instance, at)
			if result != WorkflowInitialMaterializationCreated || !errors.Is(err, fault) || !commit.used || lifecycle.effects != 1 {
				t.Fatalf("committed initial entry = result %v err %v injected %t lifecycle finalizations %d", result, err, commit.used, lifecycle.effects)
			}
			if _, found, err := store.Load(ctx, identity); err != nil || !found {
				t.Fatalf("load committed initial entry: found=%t err=%v", found, err)
			}
			replayed, err := store.MaterializeInitialEntry(ctx, identity, instance, at)
			if err != nil || replayed != WorkflowInitialMaterializationAlreadyExists || lifecycle.effects != 1 {
				t.Fatalf("initial entry replay = result %v err %v lifecycle finalizations %d", replayed, err, lifecycle.effects)
			}
			store.initialCommits = unacknowledgedInitialMaterializationOwner{}
			unacknowledged := instance
			unacknowledged.InstanceID = "inst-2"
			unacknowledged.StorageRef = "review/inst-2"
			unacknowledgedIdentity := testRunScopedWorkflowInstanceFromContext(ctx, unacknowledged.StorageRef)
			result, err = store.MaterializeInitialEntry(ctx, unacknowledgedIdentity, unacknowledged, at)
			if result != WorkflowInitialMaterializationUnknown || err == nil || lifecycle.effects != 1 {
				t.Fatalf("unacknowledged nonzero materialization = result %v err %v lifecycle finalizations %d", result, err, lifecycle.effects)
			}
		})
	}
}
