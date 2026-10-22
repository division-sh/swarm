package pipeline

import (
	"context"
	"encoding/json"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
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
	ReadMutationProjection    func(context.Context, string, string) (WorkflowMutationProjectionForTest, error)
	RemoveMutationLog         func(context.Context) error
	TryInvalidMutationPath    func(context.Context, string, string) error
	PersistEvent              func(context.Context, events.Event)
	AdmitDelivery             func(context.Context, events.Event, events.DeliveryRoute) (deliverylifecycle.Snapshot, error)
}

type WorkflowMutationProjectionForTest struct {
	Current   mutationlog.EntityStateProjection
	Mutations []WorkflowMutationRowForTest
}

type WorkflowMutationRowForTest struct {
	Domain                                  mutationlog.Domain
	Path, WriterType, WriterID, HandlerStep string
	OldValue, NewValue                      json.RawMessage
}

type WorkflowJournalStorageForTest struct {
	StoryCount, StoryHead, RunForkRevisions int64
	MockAttemptStories, Attempts            int64
}
