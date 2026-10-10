package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Read the physical identity at the caller's existing run/name cut. An
// ambiguous fixture is a refusal, never permission to choose an arbitrary row.
func ReadRunNamedEventIdentityStorageForTest(ctx context.Context, selected any, runID, eventName string) (string, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return "", err
	}
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID || eventName == "" || strings.TrimSpace(eventName) != eventName {
		return "", fmt.Errorf("run event identity read requires a canonical run and exact event name")
	}
	var eventID string
	err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT CAST(event_id AS TEXT) FROM events WHERE run_id=$1 AND event_name=$2 LIMIT 2`, runID, eventName)
		if err != nil {
			return err
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
			if err := rows.Scan(&eventID); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		switch count {
		case 0:
			return sql.ErrNoRows
		case 1:
			return nil
		default:
			return fmt.Errorf("ambiguous physical event identity for run %s and event %s", runID, eventName)
		}
	})
	if err != nil {
		return "", err
	}
	return eventID, nil
}
