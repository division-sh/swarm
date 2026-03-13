package pipeline

import "context"

type SchedulePersistence interface {
	UpsertSchedule(ctx context.Context, sc Schedule) error
	CancelSchedule(ctx context.Context, agentID, eventType string) error
	LoadActiveSchedules(ctx context.Context) ([]Schedule, error)
	MarkScheduleFired(ctx context.Context, sc Schedule) error
}

type ExactSchedulePersistence interface {
	CancelScheduleExact(ctx context.Context, sc Schedule) error
	MarkScheduleFiredExact(ctx context.Context, sc Schedule) error
}
