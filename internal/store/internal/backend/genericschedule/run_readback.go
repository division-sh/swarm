package genericschedule

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
)

// ReadPublishedOccurrencesTx is immutable evidence, not execution admission.
// The caller holds settlement authority; the shared decoder validates every
// accepted one-shot row before its exact event can become a lineage root.
func ReadPublishedOccurrencesTx(ctx context.Context, tx *sql.Tx, postgres bool, runID string) ([]runtimegenericschedule.Activation, error) {
	if tx == nil || runID == "" {
		return nil, fmt.Errorf("published schedule readback requires its transaction and exact run")
	}
	return readRunActivations(ctx, tx, postgres, runID, "fired")
}

func (o *PostgresOwner) ListActiveGenericScheduleActivationsForRun(ctx context.Context, runID string) ([]runtimegenericschedule.Activation, error) {
	if err := o.requireSchema(); err != nil {
		return nil, err
	}
	if err := requireRunWakeupCensusScope(ctx, runID); err != nil {
		return nil, err
	}
	rows, err := readRunActivations(ctx, o.backend, true, runID, "active")
	if err != nil {
		return nil, err
	}
	return observeRunActivations(ctx, rows, func(ctx context.Context, id string) (bool, error) {
		return observeActivationExecution(ctx, o.backend, o.execution, id)
	})
}

func (o *SQLiteOwner) ListActiveGenericScheduleActivationsForRun(ctx context.Context, runID string) ([]runtimegenericschedule.Activation, error) {
	if err := o.requireSchema(); err != nil {
		return nil, err
	}
	if err := requireRunWakeupCensusScope(ctx, runID); err != nil {
		return nil, err
	}
	rows, err := readRunActivations(ctx, o.backend, false, runID, "active")
	if err != nil {
		return nil, err
	}
	return observeRunActivations(ctx, rows, func(ctx context.Context, id string) (bool, error) {
		return observeActivationExecution(ctx, o.backend, o.execution, id)
	})
}

func requireRunWakeupCensusScope(ctx context.Context, runID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if runID == "" || correlation.RunIDFromContext(ctx) != runID {
		return fmt.Errorf("schedule wakeup census requires its exact run context")
	}
	return nil
}

func readRunActivations(ctx context.Context, query queryContext, postgres bool, runID, status string) ([]runtimegenericschedule.Activation, error) {
	d := dialectFor(postgres)
	statement := activationSelectColumns + ` FROM timers WHERE run_id = ? AND status = ?
		AND task_type IN ('timer','scheduled_task','global_recurring') ORDER BY timer_id`
	if postgres {
		statement = activationSelectColumns + ` FROM timers WHERE run_id = $1::uuid AND status = $2
			AND task_type IN ('timer','scheduled_task','global_recurring') ORDER BY timer_id`
	}
	rows, err := query.QueryContext(ctx, statement, runID, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var inventory []runtimegenericschedule.Activation
	for rows.Next() {
		row, err := scanActivationRow(rows, d)
		if err != nil {
			return nil, err
		}
		if row.Command.RunID != runID || string(row.Status) != status {
			return nil, fmt.Errorf("schedule census contradicts its exact run and lifecycle state")
		}
		inventory = append(inventory, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return inventory, rows.Close()
}

func observeRunActivations(ctx context.Context, rows []runtimegenericschedule.Activation, observe func(context.Context, string) (bool, error)) ([]runtimegenericschedule.Activation, error) {
	var eligible []runtimegenericschedule.Activation
	for _, row := range rows {
		allowed, err := observe(ctx, row.ID)
		if err != nil {
			return nil, err
		}
		if allowed {
			eligible = append(eligible, row)
		}
	}
	return eligible, nil
}
