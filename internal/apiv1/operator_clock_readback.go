package apiv1

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/operatorread"
)

func loadRunHeaderWithClocks(ctx context.Context, selected RunReadStore, runID string) (operatorread.RunHeader, error) {
	runs, err := requireRunReadStore(selected)
	if err != nil {
		return operatorread.RunHeader{}, err
	}
	header, err := runs.LoadRunHeader(ctx, runID)
	if errors.Is(err, operatorread.ErrRunNotFound) {
		return operatorread.RunHeader{}, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": runID})
	}
	if err != nil {
		return operatorread.RunHeader{}, err
	}
	header.ClockSchedules, err = runs.LoadRunClockSchedules(ctx, runID)
	return header, err
}
