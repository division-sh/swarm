package runtimepersistence

import (
	"context"

	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
)

func (s *PostgresStore) LoadGenericScheduleActivation(ctx context.Context, activationID string) (runtimegenericschedule.Activation, bool, error) {
	return s.genericSchedulePostgresOwner.LoadGenericScheduleActivation(ctx, activationID)
}

func (s *SQLiteRuntimeStore) LoadGenericScheduleActivation(ctx context.Context, activationID string) (runtimegenericschedule.Activation, bool, error) {
	return s.genericScheduleSQLiteOwner.LoadGenericScheduleActivation(ctx, activationID)
}

func (s *PostgresStore) ListActiveGenericScheduleActivations(ctx context.Context) ([]runtimegenericschedule.Activation, error) {
	return s.genericSchedulePostgresOwner.ListActiveGenericScheduleActivations(ctx)
}

func (s *SQLiteRuntimeStore) ListActiveGenericScheduleActivations(ctx context.Context) ([]runtimegenericschedule.Activation, error) {
	return s.genericScheduleSQLiteOwner.ListActiveGenericScheduleActivations(ctx)
}

func (s *PostgresStore) PrepareGenericScheduleOccurrence(ctx context.Context, wakeup runtimegenericschedule.Wakeup) (runtimegenericschedule.PreparationCommit, error) {
	return s.genericSchedulePostgresOwner.PrepareGenericScheduleOccurrence(ctx, wakeup)
}

func (s *SQLiteRuntimeStore) PrepareGenericScheduleOccurrence(ctx context.Context, wakeup runtimegenericschedule.Wakeup) (runtimegenericschedule.PreparationCommit, error) {
	return s.genericScheduleSQLiteOwner.PrepareGenericScheduleOccurrence(ctx, wakeup)
}

func (s *PostgresStore) CommitGenericScheduleOccurrence(ctx context.Context, command runtimegenericschedule.CommitCommand) (runtimegenericschedule.CommitResult, error) {
	return s.pipelinePostgresOwner.CommitGenericScheduleOccurrence(ctx, command)
}

func (s *SQLiteRuntimeStore) CommitGenericScheduleOccurrence(ctx context.Context, command runtimegenericschedule.CommitCommand) (runtimegenericschedule.CommitResult, error) {
	return s.pipelineSQLiteOwner.CommitGenericScheduleOccurrence(ctx, command)
}

func (s *PostgresStore) ClaimGenericScheduleWakeup(ctx context.Context, wakeup runtimegenericschedule.Wakeup) (bool, error) {
	return s.genericSchedulePostgresOwner.ClaimGenericScheduleWakeup(ctx, wakeup)
}

func (s *SQLiteRuntimeStore) ClaimGenericScheduleWakeup(ctx context.Context, wakeup runtimegenericschedule.Wakeup) (bool, error) {
	return s.genericScheduleSQLiteOwner.ClaimGenericScheduleWakeup(ctx, wakeup)
}

func (s *PostgresStore) ReleaseGenericScheduleWakeup(ctx context.Context, wakeup runtimegenericschedule.Wakeup) error {
	return s.genericSchedulePostgresOwner.ReleaseGenericScheduleWakeup(ctx, wakeup)
}

func (s *SQLiteRuntimeStore) ReleaseGenericScheduleWakeup(ctx context.Context, wakeup runtimegenericschedule.Wakeup) error {
	return s.genericScheduleSQLiteOwner.ReleaseGenericScheduleWakeup(ctx, wakeup)
}

func (s *PostgresStore) ReleaseGenericScheduleClaims(ctx context.Context) error {
	return s.genericSchedulePostgresOwner.ReleaseGenericScheduleClaims(ctx)
}

func (s *SQLiteRuntimeStore) ReleaseGenericScheduleClaims(ctx context.Context) error {
	return s.genericScheduleSQLiteOwner.ReleaseGenericScheduleClaims(ctx)
}

var _ runtimegenericschedule.Store = (*PostgresStore)(nil)
var _ runtimegenericschedule.Store = (*SQLiteRuntimeStore)(nil)
