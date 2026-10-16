package pipeline

import (
	"context"
	"fmt"
)

type ActivityAttemptJournal interface {
	StartActivityAttempt(context.Context, ActivityAttemptRecord) (ActivityAttemptRecord, bool, error)
	ClaimActivityAttemptForLoopGeneration(context.Context, ActivityAttemptRecord) (ActivityAttemptRecord, bool, error)
	CompleteActivityAttempt(context.Context, ActivityAttemptRecord) (ActivityAttemptRecord, bool, error)
	MarkActivityAttemptUncertain(context.Context, ActivityAttemptRecord) (ActivityAttemptRecord, bool, error)
	LoadActivityAttempt(context.Context, string) (ActivityAttemptRecord, bool, error)
}

// ActivityRecoverySummary carries results out of their owning recovery mutation,
// not an ACK, work queue or execution grant. Publication consumes the durable
// journal/readback obligation only after native transaction acknowledgement.
type ActivityRecoverySummary struct {
	Results []RecoveredHTTPActivity
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

func (s *workflowInstanceStore) CompleteActivityAttempt(ctx context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, bool, error) {
	if s == nil || s.activityJournal == nil {
		return ActivityAttemptRecord{}, false, fmt.Errorf("activity attempt journal is required")
	}
	return s.activityJournal.CompleteActivityAttempt(ctx, record)
}

func (s *workflowInstanceStore) MarkActivityAttemptUncertain(ctx context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, bool, error) {
	if s == nil || s.activityJournal == nil {
		return ActivityAttemptRecord{}, false, fmt.Errorf("activity attempt journal is required")
	}
	return s.activityJournal.MarkActivityAttemptUncertain(ctx, record)
}

func (s *workflowInstanceStore) LoadActivityAttempt(ctx context.Context, requestEventID string) (ActivityAttemptRecord, bool, error) {
	if s == nil || s.activityJournal == nil {
		return ActivityAttemptRecord{}, false, fmt.Errorf("activity attempt journal is required")
	}
	return s.activityJournal.LoadActivityAttempt(ctx, requestEventID)
}
