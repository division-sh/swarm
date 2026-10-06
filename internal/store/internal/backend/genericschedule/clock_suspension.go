package genericschedule

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

// Parking shares the standing transaction; other schedule families retain their
// destructive quiescence. Returned coordinates retire the original wakeups.
func ParkClockRunsTx(ctx context.Context, attempt *mutationprotocol.Attempt, postgres bool, runIDs []string, cause string, at time.Time) ([]runtimetimercancellation.Ref, error) {
	if attempt == nil {
		return nil, errors.New("clock parking requires its standing mutation")
	}
	var refs []runtimetimercancellation.Ref
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		refs, err = runActivationRefsTx(ctx, tx, postgres, runIDs)
		if err != nil {
			return err
		}
		for _, ref := range refs {
			activation, found, err := loadByIDTx(ctx, tx, dialectFor(postgres), ref.ActivationID, postgres)
			if err != nil || !found {
				return errors.Join(err, errors.New("standing schedule disappeared during parking"))
			}
			if activation.Command.OwnerKind != runtimegenericschedule.OwnerInstance {
				if _, err := CancelTx(ctx, attempt, postgres, runtimegenericschedule.CancelCommand{ActivationID: ref.ActivationID, Cause: cause, CancelledAt: at}); err != nil {
					return err
				}
				continue
			}
			if activation.Status == runtimegenericschedule.StatusParked {
				continue
			}
			parked, err := runtimegenericschedule.ParkClock(activation, at)
			if err != nil {
				return err
			}
			if err := writeClockTransitionTx(ctx, tx, attempt, postgres, activation, parked); err != nil {
				return err
			}
		}
		return nil
	})
	return refs, err
}

func ResumeClockRunTx(ctx context.Context, attempt *mutationprotocol.Attempt, postgres bool, runID string, at time.Time) error {
	if attempt == nil {
		return errors.New("clock resumption requires its standing mutation")
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		refs, err := runActivationRefsTx(ctx, tx, postgres, []string{runID})
		if err != nil {
			return err
		}
		for _, ref := range refs {
			activation, found, err := loadByIDTx(ctx, tx, dialectFor(postgres), ref.ActivationID, postgres)
			if err != nil || !found {
				return errors.Join(err, errors.New("standing schedule disappeared during resumption"))
			}
			if activation.Status != runtimegenericschedule.StatusParked {
				continue
			}
			resumed, err := runtimegenericschedule.ResumeClock(activation, at)
			if err != nil {
				return err
			}
			if err := writeClockTransitionTx(ctx, tx, attempt, postgres, activation, resumed); err != nil {
				return err
			}
		}
		return nil
	})
}

func writeClockTransitionTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, postgres bool, previous, next runtimegenericschedule.Activation) error {
	encoded, err := json.Marshal(next.ClockSuspension)
	if err != nil {
		return err
	}
	query := `UPDATE timers SET status = ?, fire_at = ?, occurrence_event_id = NULLIF(?, ''), occurrence_admitted_at = ?,
		clock_suspension = ? WHERE timer_id = ? AND owner_kind = 'instance' AND status = ? AND fire_at = ?`
	args := []any{next.Status, next.CurrentDueAt, next.CurrentEventID, optionalClockTime(next.CurrentEventAdmittedAt), string(encoded), next.ID, previous.Status, previous.CurrentDueAt}
	if postgres {
		query = `UPDATE timers SET status = $1, fire_at = $2, occurrence_event_id = NULLIF($3, '')::uuid, occurrence_admitted_at = $4,
			clock_suspension = $5::jsonb WHERE timer_id = $6::uuid AND owner_kind = 'instance' AND status = $7 AND fire_at = $8`
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return fmt.Errorf("clock transition changed %d rows: %w", rows, err)
	}
	return addTimerEffect(attempt, next.Command.RunID, next.ID)
}

func optionalClockTime(at time.Time) any {
	if at.IsZero() {
		return nil
	}
	return at
}

func decodeClockSuspension(raw any) (*runtimegenericschedule.ClockSuspension, error) {
	if raw == nil {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(jsonBytes(raw)))
	decoder.DisallowUnknownFields()
	var suspension runtimegenericschedule.ClockSuspension
	if err := decoder.Decode(&suspension); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("clock suspension contains trailing data")
	}
	return suspension.Canonical(), nil
}
