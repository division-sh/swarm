package pipeline

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

// Component proofs consume the original native semantic owner, never a pool
// or a test transaction runner reconstructed around the selected database.
type WorkflowActivityNativeFixtureForTest struct {
	Persistence               WorkflowPersistence
	Context                   context.Context
	Runs                      runlifecycle.OperationOwner
	RequireRun                func(context.Context, string) error
	NewCoordinator            func(Bus, PipelineCoordinatorOptions) *PipelineCoordinator
	Reopen                    func() WorkflowActivityNativeFixtureForTest
	ReadJournal               func(context.Context, string) (WorkflowJournalStorageForTest, error)
	CreateReply               func(context.Context, string, string, string) error
	CleanupFault              func(error) (WorkflowPersistence, func() int32)
	CleanupFaultCoordinator   func(Bus, PipelineCoordinatorOptions, error) (*PipelineCoordinator, func() int32)
	Construct                 func(context.Context, WorkflowInstance) error
	ClaimReplyLossCoordinator func(context.Context, Bus, PipelineCoordinatorOptions, string, string, error) (*PipelineCoordinator, func() int32)
	ReadAttemptStatuses       func(context.Context) ([]string, error)
	EventIDCount              func(context.Context, string) int
	Publish                   func(context.Context, events.Event)
}

type WorkflowJournalStorageForTest struct {
	StoryCount, StoryHead, RunForkRevisions int64
	MockAttemptStories, Attempts            int64
}

func VerifyWorkflowActivityNativeJournalRequiresExplicitSetupBothStoresForTest(t *testing.T, open func(*testing.T, string) WorkflowActivityNativeFixtureForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := open(t, backend)
			journal, ctx := fixture.Persistence.store, fixture.Context
			run := uuid.NewString()
			start := activityAttemptStartRecord(testNonIdempotentActivityIntent(run, uuid.NewString(), uuid.NewString()), "exact-input")
			if got, inserted, err := journal.StartActivityAttempt(ctx, start); err == nil || inserted || got.RequestEventID != "" {
				t.Fatalf("unprepared fixture acquired write permission: %+v inserted=%t err=%v", got, inserted, err)
			}
			if err := fixture.RequireRun(ctx, run); err != nil {
				t.Fatal(err)
			}
			stored, inserted, err := journal.StartActivityAttempt(ctx, start)
			if err != nil || !inserted || stored.ExecutionBinding.Validate() != nil {
				t.Fatalf("explicit native setup did not capture its role: %+v inserted=%t err=%v", stored, inserted, err)
			}
			sibling := uuid.NewString()
			if err := fixture.RequireRun(ctx, sibling); err != nil {
				t.Fatal(err)
			}
			other := activityAttemptStartRecord(testNonIdempotentActivityIntent(sibling, uuid.NewString(), uuid.NewString()), "other-input")
			if got, inserted, err := journal.StartActivityAttempt(ctx, other); err != nil || !inserted || got.ExecutionBinding != stored.ExecutionBinding {
				t.Fatalf("subsequent setup replaced the original role: %+v inserted=%t err=%v", got, inserted, err)
			}
			reopened := fixture.Reopen()
			if got, found, err := reopened.Persistence.store.LoadActivityAttempt(reopened.Context, start.RequestEventID); err != nil || !found || !reflect.DeepEqual(got, stored) {
				t.Fatalf("reopen lost immutable original evidence: %+v found=%t err=%v", got, found, err)
			}
			if got, inserted, err := reopened.Persistence.store.StartActivityAttempt(reopened.Context, start); err == nil || inserted || got.RequestEventID != "" {
				t.Fatalf("reopen inferred write permission from history: %+v inserted=%t err=%v", got, inserted, err)
			}
			if got, inserted, err := journal.StartActivityAttempt(ctx, other); err == nil || inserted || got.RequestEventID != "" {
				t.Fatalf("retired original fixture retained write permission: %+v inserted=%t err=%v", got, inserted, err)
			}
		})
	}
}
