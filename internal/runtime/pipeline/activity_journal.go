package pipeline

import (
	"context"
	"fmt"

	runtimeactivityresult "github.com/division-sh/swarm/internal/runtime/activityresult"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

type ActivityAttemptJournal interface {
	StartActivityAttempt(context.Context, ActivityAttemptRecord) (ActivityAttemptRecord, bool, error)
	ClaimActivityAttemptForLoopGeneration(context.Context, ActivityAttemptRecord) (ActivityAttemptRecord, bool, error)
	CompleteActivityAttempt(context.Context, ActivityAttemptRecord) (ActivityAttemptRecord, error)
	MarkActivityAttemptUncertain(context.Context, ActivityAttemptRecord) (ActivityAttemptRecord, error)
	LoadActivityAttempt(context.Context, string) (ActivityAttemptRecord, bool, error)
}

func (s *workflowInstanceStore) recordedActivityResult(ctx context.Context, intent runtimeengine.ActivityIntent) (activityRecordedResult, bool, error) {
	if s == nil || s.activityResults == nil {
		return activityRecordedResult{}, false, fmt.Errorf("activity result reader is required")
	}
	successID := activityResultEventID(intent, intent.SuccessEvent)
	failureID := activityResultEventID(intent, intent.FailureEvent)
	return s.activityResults.LoadRecordedActivityResult(ctx, runtimeactivityresult.Query{
		ActivityID:     intent.ActivityID,
		RequestEventID: activityRequestEventID(intent),
		SuccessEventID: successID,
		FailureEventID: failureID,
	})
}

func (s *workflowInstanceStore) StartActivityAttempt(ctx context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, bool, error) {
	if s == nil || s.activityJournal == nil {
		return ActivityAttemptRecord{}, false, fmt.Errorf("activity attempt journal is required")
	}
	return s.activityJournal.StartActivityAttempt(ctx, record)
}

func (s *workflowInstanceStore) ClaimActivityAttemptForLoopGeneration(ctx context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, bool, error) {
	if s == nil || s.activityJournal == nil {
		return ActivityAttemptRecord{}, false, fmt.Errorf("activity attempt journal is required")
	}
	return s.activityJournal.ClaimActivityAttemptForLoopGeneration(ctx, record)
}

func (s *workflowInstanceStore) CompleteActivityAttempt(ctx context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, error) {
	if s == nil || s.activityJournal == nil {
		return ActivityAttemptRecord{}, fmt.Errorf("activity attempt journal is required")
	}
	return s.activityJournal.CompleteActivityAttempt(ctx, record)
}

func (s *workflowInstanceStore) MarkActivityAttemptUncertain(ctx context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, error) {
	if s == nil || s.activityJournal == nil {
		return ActivityAttemptRecord{}, fmt.Errorf("activity attempt journal is required")
	}
	return s.activityJournal.MarkActivityAttemptUncertain(ctx, record)
}

func (s *workflowInstanceStore) LoadActivityAttempt(ctx context.Context, requestEventID string) (ActivityAttemptRecord, bool, error) {
	if s == nil || s.activityJournal == nil {
		return ActivityAttemptRecord{}, false, fmt.Errorf("activity attempt journal is required")
	}
	return s.activityJournal.LoadActivityAttempt(ctx, requestEventID)
}
