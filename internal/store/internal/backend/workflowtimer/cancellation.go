package workflowtimer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func CancelRunsTx(ctx context.Context, tx *sql.Tx, postgres bool, effects *privaterunforkrevision.Effects, runIDs []string) ([]runtimetimercancellation.Ref, error) {
	if tx == nil || effects == nil {
		return nil, errors.New("workflow timer run cancellation requires transaction and revision effects")
	}
	ids := normalizedIDs(runIDs)
	if len(ids) == 0 {
		return nil, nil
	}
	query, args := activeRunQuery(postgres, ids)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("lock active workflow timers: %w", err)
	}
	defer rows.Close()
	refs := make([]runtimetimercancellation.Ref, 0)
	for rows.Next() {
		var activationID string
		var taskID string
		var dueRaw any
		var runID string
		if err := rows.Scan(&activationID, &taskID, &dueRaw, &runID); err != nil {
			return nil, fmt.Errorf("scan active workflow timer: %w", err)
		}
		due, err := selectedTime(dueRaw)
		if err != nil {
			return nil, fmt.Errorf("scan active workflow timer due coordinate: %w", err)
		}
		ref := runtimetimercancellation.Ref{
			Family: runtimetimercancellation.FamilyWorkflowTimer, ActivationID: activationID,
			RunID: runID, TaskID: taskID, DueAt: due.UTC(),
		}
		if err := ref.Validate(); err != nil {
			return nil, fmt.Errorf("invalid active workflow timer cancellation fact: %w", err)
		}
		refs = append(refs, ref.Canonical())
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read active workflow timers: %w", err)
	}
	for _, ref := range refs {
		query := `UPDATE timers SET status = 'cancelled' WHERE timer_id = ? AND task_type = 'workflow_timer' AND status = 'active'`
		args := []any{ref.ActivationID}
		if postgres {
			query = `UPDATE timers SET status = 'cancelled' WHERE timer_id = $1::uuid AND task_type = 'workflow_timer' AND status = 'active'`
		}
		result, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("cancel workflow timer %s: %w", ref.ActivationID, err)
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return nil, fmt.Errorf("workflow timer %s cancellation changed %d rows: %w", ref.ActivationID, changed, err)
		}
		if err := effects.Add(ref.RunID, privaterunforkrevision.FamilyTimers); err != nil {
			return nil, err
		}
	}
	return refs, nil
}

func selectedTime(raw any) (time.Time, error) {
	switch value := raw.(type) {
	case time.Time:
		return value.UTC(), nil
	case string:
		for _, layout := range []string{
			time.RFC3339Nano,
			"2006-01-02 15:04:05.999999999 -0700 MST",
			"2006-01-02 15:04:05.999999 -0700 MST",
			"2006-01-02 15:04:05 -0700 MST",
			"2006-01-02 15:04:05.999999999-07:00",
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05-07:00",
			"2006-01-02 15:04:05",
		} {
			parsed, err := time.Parse(layout, value)
			if err == nil {
				return parsed.UTC(), nil
			}
		}
		return time.Time{}, fmt.Errorf("invalid selected-store timestamp %q", value)
	case []byte:
		return selectedTime(string(value))
	default:
		return time.Time{}, fmt.Errorf("unsupported selected-store timestamp %T", raw)
	}
}

func normalizedIDs(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func activeRunQuery(postgres bool, runIDs []string) (string, []any) {
	marks := make([]string, len(runIDs))
	args := make([]any, len(runIDs))
	for index, runID := range runIDs {
		args[index] = runID
		if postgres {
			marks[index] = fmt.Sprintf("$%d::uuid", index+1)
		} else {
			marks[index] = "?"
		}
	}
	query := `SELECT CAST(timer_id AS TEXT), timer_name, fire_at, CAST(run_id AS TEXT) FROM timers
		WHERE run_id IN (` + strings.Join(marks, ",") + `)
		AND task_type = 'workflow_timer' AND status = 'active'
		ORDER BY timer_id`
	if postgres {
		query += " FOR UPDATE"
	}
	return query, args
}
