package genericschedule

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/correlation"
)

type runWakeupReader interface {
	ListActiveGenericScheduleActivationsForRun(context.Context, string) ([]Activation, error)
}

// ReconcileRunWakeups never scans unrelated runs or treats readback as a claim.
// Each exact activation is reloaded and freshly fenced by ReconcileWakeup.
func (l *Lifecycle) ReconcileRunWakeups(ctx context.Context, runID string) error {
	if l == nil || runID == "" || correlation.RunIDFromContext(ctx) != runID {
		return fmt.Errorf("schedule reconciliation requires its exact run context")
	}
	reader, ok := l.store.(runWakeupReader)
	if !ok {
		return fmt.Errorf("schedule reconciliation requires the canonical run-scoped census")
	}
	rows, err := reader.ListActiveGenericScheduleActivationsForRun(ctx, runID)
	if err != nil {
		return err
	}
	ids := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if err := row.Validate(); err != nil {
			return err
		}
		if row.Command.RunID != runID || row.Status != StatusActive {
			return fmt.Errorf("schedule reconciliation received foreign or nonactive work")
		}
		if _, duplicate := ids[row.ID]; duplicate {
			return fmt.Errorf("schedule reconciliation received duplicate work")
		}
		ids[row.ID] = struct{}{}
	}
	for _, row := range rows {
		if err := l.ReconcileWakeup(ctx, row.ID); err != nil {
			return err
		}
	}
	return nil
}
