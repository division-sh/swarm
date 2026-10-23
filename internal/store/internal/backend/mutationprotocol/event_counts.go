package mutationprotocol

import (
	"context"
	"database/sql"
	"errors"
	"math"
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
		// Retain contribution spelling; finalization orders only a copied lock list.
		a.eventCountOrder = append(a.eventCountOrder, runID)
	}
	if (delta > 0 && a.eventCounts[key] > math.MaxInt64-delta) || (delta < 0 && a.eventCounts[key] < math.MinInt64-delta) {
		return errors.New("run event-count delta overflow")
	}
	a.eventCounts[key] += delta
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

// FlushEventCounts is a finalization boundary, never a read-visibility step.
func (a *Attempt) FlushEventCounts(ctx context.Context) error {
	if err := a.requireActive(); err != nil {
		return err
	}
	if err := counterprojection.ApplyAll(ctx, a.tx, a.dialect, a.pendingEventCounts()); err != nil {
		return err
	}
	for key := range a.eventCounts {
		a.eventCounts[key] = 0
	}
	return nil
}

// EventCountForRead overlays only the matching live attempt's pending delta,
// leaving the database counter lock for the commit tail.
func EventCountForRead(ctx context.Context, tx *sql.Tx, runID string, stored int) (int, error) {
	if ctx == nil {
		return 0, errors.New("event-count reader context is required")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	current := ctx.Value(sqlAttemptKey{})
	if current == nil {
		return stored, nil
	}
	attempt, ok := current.(*Attempt)
	if !ok || attempt == nil || attempt.tx != tx {
		return 0, errors.New("event-count reader belongs to another transaction")
	}
	if err := attempt.requireActive(); err != nil {
		return 0, err
	}
	if attempt.callerCtx != nil && attempt.callerCtx.Err() != nil {
		return 0, attempt.callerCtx.Err()
	}
	key, _ := physicalRunKey(attempt.dialect, runID)
	delta := attempt.eventCounts[key]
	if (delta > 0 && int64(stored) > math.MaxInt64-delta) || (delta < 0 && int64(stored) < math.MinInt64-delta) {
		return 0, errors.New("run event-count read overflow")
	}
	total := int64(stored) + delta
	if total < 0 || int64(int(total)) != total {
		return 0, errors.New("run event-count read is out of range")
	}
	return int(total), nil
}
