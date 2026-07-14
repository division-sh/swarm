package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
)

type pipelineRunForkRevisionChangesKey struct{}

type pipelineRunForkRevisionChanges struct {
	byRun map[string]map[runforkrevision.Family]struct{}
}

func withPipelineRunForkRevisionChanges(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Value(pipelineRunForkRevisionChangesKey{}).(*pipelineRunForkRevisionChanges); ok {
		return ctx
	}
	return context.WithValue(ctx, pipelineRunForkRevisionChangesKey{}, &pipelineRunForkRevisionChanges{
		byRun: map[string]map[runforkrevision.Family]struct{}{},
	})
}

func declarePipelineRunForkRevisionChange(ctx context.Context, runID string, families ...runforkrevision.Family) error {
	changes, ok := ctx.Value(pipelineRunForkRevisionChangesKey{}).(*pipelineRunForkRevisionChanges)
	if !ok || changes == nil {
		return fmt.Errorf("pipeline transaction is missing its run fork revision owner")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("pipeline run fork revision change requires run_id")
	}
	if changes.byRun[runID] == nil {
		changes.byRun[runID] = map[runforkrevision.Family]struct{}{}
	}
	for _, family := range families {
		if !runforkrevision.ValidFamily(family) {
			return fmt.Errorf("pipeline run fork revision change has unsupported family %q", family)
		}
		changes.byRun[runID][family] = struct{}{}
	}
	return nil
}

// CapturePipelineRunForkRevisionChanges publishes every bounded fact changed
// by the current transaction, including explicitly declared deletion-sensitive
// projections that PostgreSQL row writer stamps cannot discover.
func CapturePipelineRunForkRevisionChanges(ctx context.Context, tx *sql.Tx) error {
	if _, err := runforkrevision.CaptureCurrentTransaction(ctx, tx); err != nil {
		return err
	}
	changes, ok := ctx.Value(pipelineRunForkRevisionChangesKey{}).(*pipelineRunForkRevisionChanges)
	if !ok || changes == nil || len(changes.byRun) == 0 {
		return nil
	}
	runIDs := make([]string, 0, len(changes.byRun))
	for runID := range changes.byRun {
		runIDs = append(runIDs, runID)
	}
	sort.Strings(runIDs)
	declared := make([]runforkrevision.Change, 0, len(runIDs))
	for _, runID := range runIDs {
		families := make([]runforkrevision.Family, 0, len(changes.byRun[runID]))
		for family := range changes.byRun[runID] {
			families = append(families, family)
		}
		declared = append(declared, runforkrevision.Change{RunID: runID, Families: families})
	}
	_, err := runforkrevision.CaptureChanges(ctx, tx, declared...)
	return err
}
