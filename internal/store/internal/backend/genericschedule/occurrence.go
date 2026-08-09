package genericschedule

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
)

// AdvanceOccurrenceTx is called only by the named outer event-publication
// transaction after durable event acceptance has succeeded.
func AdvanceOccurrenceTx(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	command runtimegenericschedule.CommitCommand,
) (runtimegenericschedule.Activation, runtimegenericschedule.CommitOutcome, error) {
	if err := command.Validate(); err != nil {
		return runtimegenericschedule.Activation{}, "", err
	}
	persisted, found, err := loadByIDTx(ctx, tx, dialectFor(postgres), command.Activation.ID, postgres)
	if err != nil {
		return runtimegenericschedule.Activation{}, "", err
	}
	if !found {
		return runtimegenericschedule.Activation{}, runtimegenericschedule.CommitTerminal, nil
	}
	if persisted.Status == runtimegenericschedule.StatusCancelled {
		return persisted, runtimegenericschedule.CommitStaleCancelled, nil
	}
	if persisted.Status != runtimegenericschedule.StatusActive {
		return persisted, runtimegenericschedule.CommitTerminal, nil
	}
	if persisted.ImmutableHash != command.Activation.ImmutableHash ||
		!persisted.CurrentDueAt.Equal(command.Occurrence.DueAt) ||
		persisted.CurrentEventID != command.Occurrence.EventID ||
		!persisted.CurrentEventAdmittedAt.Equal(command.Occurrence.AdmittedAt) {
		return persisted, runtimegenericschedule.CommitTerminal, nil
	}

	next := persisted.Canonical()
	next.AcceptedAt = canonicalTime(command.AcceptedAt)
	next.FiredAt = next.AcceptedAt
	query := `UPDATE timers SET status = 'fired', fired_at = ?, accepted_at = ?
		WHERE timer_id = ? AND task_type IN ('timer','scheduled_task','global_recurring')
		AND status = 'active' AND fire_at = ? AND occurrence_event_id = ? AND occurrence_admitted_at = ?`
	args := []any{next.FiredAt, next.AcceptedAt, next.ID, persisted.CurrentDueAt, persisted.CurrentEventID, persisted.CurrentEventAdmittedAt}
	if persisted.Command.Due.Recurring() {
		nextDue, err := persisted.Command.Due.Next(persisted.CurrentDueAt)
		if err != nil {
			return runtimegenericschedule.Activation{}, "", err
		}
		next.CurrentDueAt = nextDue
		next.CurrentEventID = ""
		next.CurrentEventAdmittedAt = time.Time{}
		next.Status = runtimegenericschedule.StatusActive
		query = `UPDATE timers SET fire_at = ?, fired_at = ?, accepted_at = ?, occurrence_event_id = NULL, occurrence_admitted_at = NULL
			WHERE timer_id = ? AND task_type IN ('scheduled_task','global_recurring')
			AND status = 'active' AND fire_at = ? AND occurrence_event_id = ? AND occurrence_admitted_at = ?`
		args = []any{next.CurrentDueAt, next.FiredAt, next.AcceptedAt, next.ID, persisted.CurrentDueAt, persisted.CurrentEventID, persisted.CurrentEventAdmittedAt}
	} else {
		next.Status = runtimegenericschedule.StatusFired
	}
	if postgres {
		if persisted.Command.Due.Recurring() {
			query = `UPDATE timers SET fire_at = $1, fired_at = $2, accepted_at = $3, occurrence_event_id = NULL, occurrence_admitted_at = NULL
				WHERE timer_id = $4::uuid AND task_type IN ('scheduled_task','global_recurring')
				AND status = 'active' AND fire_at = $5 AND occurrence_event_id = $6::uuid AND occurrence_admitted_at = $7`
		} else {
			query = `UPDATE timers SET status = 'fired', fired_at = $1, accepted_at = $2
				WHERE timer_id = $3::uuid AND task_type = 'timer'
				AND status = 'active' AND fire_at = $4 AND occurrence_event_id = $5::uuid AND occurrence_admitted_at = $6`
		}
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return runtimegenericschedule.Activation{}, "", fmt.Errorf("advance generic schedule occurrence: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return runtimegenericschedule.Activation{}, "", err
	}
	if rows != 1 {
		return runtimegenericschedule.Activation{}, "", fmt.Errorf("generic schedule occurrence advanced %d rows", rows)
	}
	if err := next.Validate(); err != nil {
		return runtimegenericschedule.Activation{}, "", err
	}
	return next, runtimegenericschedule.CommitCommitted, nil
}

func LoadOccurrenceCommitStateTx(ctx context.Context, tx *sql.Tx, postgres bool, activationID string) (runtimegenericschedule.Activation, bool, error) {
	if tx == nil {
		return runtimegenericschedule.Activation{}, false, errors.New("generic schedule occurrence read requires transaction")
	}
	return loadByIDTx(ctx, tx, dialectFor(postgres), activationID, postgres)
}
