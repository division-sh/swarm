package genericschedule

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// SelectedStoreTimeTx returns the time coordinate owned by the selected
// store transaction. PostgreSQL must not mix a process clock with database
// acceptance timestamps; SQLite uses its selected-store injectable clock.
func SelectedStoreTimeTx(ctx context.Context, tx *sql.Tx, postgres bool, sqliteNow func() time.Time) (time.Time, error) {
	if tx == nil {
		return time.Time{}, errors.New("generic schedule selected-store clock requires transaction")
	}
	if postgres {
		var selected time.Time
		if err := tx.QueryRowContext(ctx, `SELECT CURRENT_TIMESTAMP`).Scan(&selected); err != nil {
			return time.Time{}, err
		}
		return canonicalTime(selected), nil
	}
	if sqliteNow == nil {
		return time.Time{}, errors.New("generic schedule sqlite selected-store clock is required")
	}
	selected := canonicalTime(sqliteNow())
	if selected.IsZero() {
		return time.Time{}, errors.New("generic schedule selected-store clock returned zero")
	}
	return selected, nil
}
