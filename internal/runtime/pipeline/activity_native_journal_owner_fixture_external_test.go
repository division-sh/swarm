package pipeline_test

import (
	"context"
	"errors"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// Run setup binds once before execution. Calls never issue a grant or consult a
// replacement owner; an unbound reopened fixture can only read native history.
type nativeActivityJournalFixtureOwner struct {
	pipeline.WorkflowPersistenceOwner
	mu      sync.RWMutex
	journal pipeline.ActivityAttemptJournal
}

func (f *nativeActivityJournalFixtureOwner) bind(journal pipeline.ActivityAttemptJournal) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if journal == nil || f.journal != nil {
		return errors.New("native activity fixture journal must be captured exactly once")
	}
	f.journal = journal
	return nil
}

func (f *nativeActivityJournalFixtureOwner) original(ctx context.Context) (pipeline.ActivityAttemptJournal, error) {
	if ctx == nil {
		return nil, errors.New("native activity fixture requires its original caller context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.journal == nil {
		return nil, errors.New("native activity fixture requires explicit original journal setup")
	}
	return f.journal, nil
}

func (f *nativeActivityJournalFixtureOwner) StartActivityAttempt(ctx context.Context, record pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	journal, err := f.original(ctx)
	if err != nil {
		return pipeline.ActivityAttemptRecord{}, false, err
	}
	return journal.StartActivityAttempt(ctx, record)
}

func (f *nativeActivityJournalFixtureOwner) ClaimActivityAttemptForLoopGeneration(ctx context.Context, record pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	journal, err := f.original(ctx)
	if err != nil {
		return pipeline.ActivityAttemptRecord{}, false, err
	}
	return journal.ClaimActivityAttemptForLoopGeneration(ctx, record)
}

func (f *nativeActivityJournalFixtureOwner) CompleteActivityAttempt(ctx context.Context, record pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	journal, err := f.original(ctx)
	if err != nil {
		return pipeline.ActivityAttemptRecord{}, false, err
	}
	return journal.CompleteActivityAttempt(ctx, record)
}

func (f *nativeActivityJournalFixtureOwner) MarkActivityAttemptUncertain(ctx context.Context, record pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	journal, err := f.original(ctx)
	if err != nil {
		return pipeline.ActivityAttemptRecord{}, false, err
	}
	return journal.MarkActivityAttemptUncertain(ctx, record)
}

type activityFixtureJoin func(context.Context) error

func (join activityFixtureJoin) WaitForQuiescence(ctx context.Context) error { return join(ctx) }
