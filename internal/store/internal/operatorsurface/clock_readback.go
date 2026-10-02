package operatorsurface

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
)

func (s *RunPostgres) LoadRunClockSchedules(ctx context.Context, runID string) ([]genericschedule.ClockReadback, error) {
	header, err := s.LoadRunHeader(ctx, runID)
	if err != nil {
		return nil, err
	}
	state, err := runtimerunlifecycle.ParseState(header.Status)
	if err != nil {
		return nil, err
	}
	return storegenericschedule.ReadRunClocks(ctx, s.backend, true, header.RunID, state.Active())
}

func (s *RunSQLite) LoadRunClockSchedules(ctx context.Context, runID string) ([]genericschedule.ClockReadback, error) {
	header, err := s.LoadRunHeader(ctx, runID)
	if err != nil {
		return nil, err
	}
	state, err := runtimerunlifecycle.ParseState(header.Status)
	if err != nil {
		return nil, err
	}
	return storegenericschedule.ReadRunClocks(ctx, s.backend, false, header.RunID, state.Active())
}
