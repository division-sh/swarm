package pipeline

import "context"

// Component proofs consume the original native semantic owner, never a pool
// or a test transaction runner reconstructed around the selected database.
type WorkflowActivityNativeFixtureForTest struct {
	Persistence    WorkflowPersistence
	Context        context.Context
	RequireRun     func(context.Context, string) error
	NewCoordinator func(Bus, PipelineCoordinatorOptions) *PipelineCoordinator
	Reopen         func() WorkflowActivityNativeFixtureForTest
	ReadJournal    func(context.Context, string) (WorkflowJournalStorageForTest, error)
	CreateReply    func(context.Context, string, string, string) error
}

type WorkflowJournalStorageForTest struct {
	StoryCount, StoryHead, RunForkRevisions int64
	MockAttemptStories, Attempts            int64
}
