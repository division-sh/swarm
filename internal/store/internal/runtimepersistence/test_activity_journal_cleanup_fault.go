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
	return &activityJournalCleanupFault{WorkflowPersistenceOwner: journal, fault: fault}, nil
}

type activityJournalCleanupFault struct {
	pipeline.WorkflowPersistenceOwner
	fault           error
	acknowledgments atomic.Int32
}

func (j *activityJournalCleanupFault) CompleteActivityAttempt(ctx context.Context, request pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	result, acknowledged, err := j.WorkflowPersistenceOwner.CompleteActivityAttempt(ctx, request)
	if acknowledged {
		j.acknowledgments.Add(1)
		err = errors.Join(err, j.fault)
	}
	return result, acknowledged, err
}

func (j *activityJournalCleanupFault) MarkActivityAttemptUncertain(ctx context.Context, request pipeline.ActivityAttemptRecord) (pipeline.ActivityAttemptRecord, bool, error) {
	result, acknowledged, err := j.WorkflowPersistenceOwner.MarkActivityAttemptUncertain(ctx, request)
	if acknowledged {
		j.acknowledgments.Add(1)
		err = errors.Join(err, j.fault)
	}
	return result, acknowledged, err
}
