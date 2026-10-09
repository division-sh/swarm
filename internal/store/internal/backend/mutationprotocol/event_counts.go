package mutationprotocol

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/counterprojection"
)

type sqlAttemptKey struct{}

// AddEventCountDelta records actual physical event-row changes, not publication
// attempts. The existing finalizer applies them before historical reconstruction.
func (a *Attempt) AddEventCountDelta(runID string, delta int64) error {
	if err := a.requireActive(); err != nil {
		return err
	}
	runID = strings.TrimSpace(runID)
	if runID == "" || delta == 0 {
		return nil
	}
	if a.kind == WholeParentDeletion {
		return errors.New("whole-parent deletion has no surviving event counter")
	}
	key, _ := physicalRunKey(a.dialect, runID)
	if a.eventCounts == nil {
		a.eventCounts = make(map[string]int64)
	}
	if _, seen := a.eventCounts[key]; !seen {
		// Keep the original contribution order; do not introduce a new run-lock order.
		a.eventCountOrder = append(a.eventCountOrder, runID)
	}
	a.eventCounts[key] += delta
	return nil
}

func (a *Attempt) flushEventCount(ctx context.Context, runID string) error {
	if err := a.requireActive(); err != nil {
		return err
	}
	key, _ := physicalRunKey(a.dialect, runID)
	delta := a.eventCounts[key]
	if delta == 0 {
		return nil
	}
	if err := counterprojection.Apply(ctx, a.tx, a.dialect, runID, delta); err != nil {
		return err
	}
	a.eventCounts[key] = 0
	return nil
}

func (a *Attempt) pendingEventCounts() []counterprojection.Delta {
	deltas := make([]counterprojection.Delta, 0, len(a.eventCountOrder))
	for _, runID := range a.eventCountOrder {
		key, _ := physicalRunKey(a.dialect, runID)
		if delta := a.eventCounts[key]; delta != 0 {
			deltas = append(deltas, counterprojection.Delta{RunID: runID, Amount: delta})
		}
	}
	return deltas
}

// FlushEventCounts preserves visibility at explicit result/history boundaries.
// A later flush applies only physical changes made after the preceding one.
func (a *Attempt) FlushEventCounts(ctx context.Context) error {
	if err := a.requireActive(); err != nil {
		return err
	}
	for _, runID := range a.eventCountOrder {
		if err := a.flushEventCount(ctx, runID); err != nil {
			return err
		}
	}
	return nil
}

// FlushEventCountBeforeRead does not mint authority from context. A reader can
// consume pending deltas only from the matching, still-active native attempt.
func FlushEventCountBeforeRead(ctx context.Context, tx *sql.Tx, runID string) error {
	if ctx == nil {
		return errors.New("event-count reader context is required")
	}
	attempt, ok := ctx.Value(sqlAttemptKey{}).(*Attempt)
	if !ok {
		return nil
	}
	if attempt == nil || attempt.tx != tx {
		return errors.New("event-count reader belongs to another transaction")
	}
	return attempt.flushEventCount(ctx, strings.TrimSpace(runID))
}
