package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// PrepareOriginalSessionInterruptionForTest binds the fixed native-loss
// operation to this live session and its backend start, never a current holder.
// Interruption deliberately performs no local fencing or lease disposal.
func (b *Backend) PrepareOriginalSessionInterruptionForTest(ctx context.Context, session *SessionAuthority) (func(context.Context) error, error) {
	if !b.Valid() || session == nil {
		return nil, errors.New("original PostgreSQL session is required")
	}
	finish, err := session.beginOperation()
	if err != nil {
		return nil, err
	}
	defer finish()
	if !session.testInterruptionIssued.CompareAndSwap(false, true) {
		return nil, errors.New("original PostgreSQL session already issued its interruption handle")
	}
	conn, err := session.connection()
	if err != nil {
		return nil, err
	}
	var pid int
	var started time.Time
	if err := conn.QueryRowContext(ctx, `SELECT pg_backend_pid(), backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()`).Scan(&pid, &started); err != nil {
		return nil, err
	}
	var consumed atomic.Bool
	return func(ctx context.Context) error {
		if !consumed.CompareAndSwap(false, true) {
			return errors.New("original PostgreSQL session interruption was already consumed")
		}
		finish, err := session.beginOperation()
		if err != nil {
			return err
		}
		defer finish()
		if _, err := session.connection(); err != nil {
			return err
		}
		var interrupted bool
		if err := b.db.QueryRowContext(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid = $1 AND backend_start = $2 AND datname = current_database()`, pid, started).Scan(&interrupted); err != nil {
			return err
		}
		if !interrupted {
			return errors.New("original PostgreSQL session interruption was not acknowledged")
		}
		return nil
	}, nil
}
