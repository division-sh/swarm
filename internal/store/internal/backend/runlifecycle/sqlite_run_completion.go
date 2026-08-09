package runlifecycle

import (
	"context"
	"fmt"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func (s *RunLifecycleSQLiteOwner) LoadRunLifecycleSnapshot(ctx context.Context, runID string) (runtimebus.RunLifecycleSnapshot, error) {
	if s == nil || s.backend == nil {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf("sqlite runtime store is required")
	}
	snap, err := loadSQLiteRunLifecycleSnapshot(ctx, s.backend, runID)
	if err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	return projectBusRunLifecycleSnapshot(snap), nil
}

func (s *RunLifecyclePostgresOwner) LoadRunLifecycleSnapshot(ctx context.Context, runID string) (runtimebus.RunLifecycleSnapshot, error) {
	if s == nil || s.backend == nil {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf("postgres runtime store is required")
	}
	snapshot, err := loadPostgresRunLifecycleSnapshot(ctx, s.backend, runID, false)
	if err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	return projectBusRunLifecycleSnapshot(snapshot), nil
}

func projectBusRunLifecycleSnapshot(snapshot runtimerunlifecycle.Snapshot) runtimebus.RunLifecycleSnapshot {
	return runtimebus.RunLifecycleSnapshot{
		RunID: snapshot.RunID, Status: string(snapshot.State),
		EventCount: snapshot.EventCount, EntityCount: snapshot.EntityCount,
		Failure:   runtimefailures.CloneEnvelope(snapshot.Failure),
		StartedAt: snapshot.StartedAt, EndedAt: snapshot.EndedAt,
	}
}

func ProjectBusRunLifecycleSnapshot(snapshot runtimerunlifecycle.Snapshot) runtimebus.RunLifecycleSnapshot {
	return projectBusRunLifecycleSnapshot(snapshot)
}
