package runtimepersistence

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// Preserve the selected journal's actual commit verdict and add the independent,
// post-acknowledgment cleanup fault without inventing transaction authority.
func ActivityJournalCleanupPersistenceFaultForTest(selected any, fault error) (pipeline.WorkflowPersistence, func() int32, error) {
	journal, err := activityJournalCleanupFaultForTest(selected, fault)
	if err != nil {
		return pipeline.WorkflowPersistence{}, nil, err
	}
	return pipeline.NewWorkflowPersistence(journal), journal.acknowledgments.Load, nil
}

func ActivityJournalCleanupBoundFaultForTest(selected any, original pipeline.ActivityAttemptJournal, fault error) (pipeline.ActivityAttemptJournal, func() int32, error) {
	if original == nil {
		return nil, nil, fmt.Errorf("activity journal cleanup proof requires its captured original journal")
	}
	journal, err := activityJournalCleanupFaultForTest(selected, fault)
	if err != nil {
		return nil, nil, err
	}
	journal.original = original
	return journal, journal.acknowledgments.Load, nil
}

func activityJournalCleanupFaultForTest(selected any, fault error) (*activityJournalCleanupFault, error) {
	if fault == nil {
		return nil, fmt.Errorf("activity journal cleanup proof requires a fault")
	}
	var journal pipeline.WorkflowPersistenceOwner
	switch selected := selected.(type) {
	case *SQLiteRuntimeStore:
		if selected == nil || selected.backend == nil {
			return nil, fmt.Errorf("sqlite activity journal owner is required")
		}
		journal = selected
	case *PostgresStore:
		if selected == nil || selected.backend == nil {
			return nil, fmt.Errorf("postgres activity journal owner is required")
		}
		journal = selected
	default:
		return nil, fmt.Errorf("unsupported activity journal cleanup fault owner %T", selected)
	}
	return &activityJournalCleanupFault{WorkflowPersistenceOwner: journal, original: journal, fault: fault}, nil
}

type activityJournalCleanupFault struct {
	pipeline.WorkflowPersistenceOwner
	original        pipeline.ActivityAttemptJournal
	fault           error
	acknowledgments atomic.Int32
}

func (j *activityJournalCleanupFault) CompleteActivityAttempt(ctx context.Context, request pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	result, acknowledged, err := j.original.CompleteActivityAttempt(ctx, request)
	if acknowledged {
		j.acknowledgments.Add(1)
		err = errors.Join(err, j.fault)
	}
	return result, acknowledged, err
}

func (j *activityJournalCleanupFault) MarkActivityAttemptUncertain(ctx context.Context, request pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	result, acknowledged, err := j.original.MarkActivityAttemptUncertain(ctx, request)
	if acknowledged {
		j.acknowledgments.Add(1)
		err = errors.Join(err, j.fault)
	}
	return result, acknowledged, err
}

func (j *activityJournalCleanupFault) LoadActivityAttempt(ctx context.Context, id string) (pipeline.ActivityAttemptRecord, bool, error) {
	return j.original.LoadActivityAttempt(ctx, id)
}

func (j *activityJournalCleanupFault) StartActivityAttempt(ctx context.Context, record pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	return j.original.StartActivityAttempt(ctx, record)
}

func (j *activityJournalCleanupFault) ClaimActivityAttemptForLoopGeneration(ctx context.Context, record pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	return j.original.ClaimActivityAttemptForLoopGeneration(ctx, record)
}
