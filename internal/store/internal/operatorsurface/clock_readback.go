package operatorsurface

import (
	"context"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
)

func (s *RunPostgres) withClockReadback(ctx context.Context, header operatorread.RunHeader) (operatorread.RunHeader, error) {
	state, err := runtimerunlifecycle.ParseState(header.Status)
	if err != nil {
		return operatorread.RunHeader{}, err
	}
	header.ClockSchedules, err = storegenericschedule.ReadRunClocks(ctx, s.backend, true, header.RunID, state.Active())
	return header, err
}

func (s *RunSQLite) withClockReadback(ctx context.Context, header operatorread.RunHeader) (operatorread.RunHeader, error) {
	state, err := runtimerunlifecycle.ParseState(header.Status)
	if err != nil {
		return operatorread.RunHeader{}, err
	}
	header.ClockSchedules, err = storegenericschedule.ReadRunClocks(ctx, s.backend, false, header.RunID, state.Active())
	return header, err
}
