package channelonboarding

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

// SessionPublicationLock owns one exact adverse-cut transaction. Neither its
// connection nor general transaction work is available to the fixture caller.
type SessionPublicationLock struct {
	tx   *sql.Tx
	db   *sql.DB
	pid  int
	once sync.Once
	err  error
}

func (s *PostgresOwner) HoldSessionPublicationOnboardingLockForTest(ctx context.Context, operationID string) (*SessionPublicationLock, error) {
	return holdSessionPublicationOnboardingLockForTest(ctx, s.backend.ConstructionHandle(), true, operationID)
}

func (s *SQLiteOwner) HoldSessionPublicationOnboardingLockForTest(ctx context.Context, operationID string) (*SessionPublicationLock, error) {
	return holdSessionPublicationOnboardingLockForTest(ctx, s.backend.ConstructionHandle(), false, operationID)
}

func holdSessionPublicationOnboardingLockForTest(ctx context.Context, db *sql.DB, postgres bool, operationID string) (*SessionPublicationLock, error) {
	if uuid.Validate(operationID) != nil {
		return nil, fmt.Errorf("onboarding lock requires an exact operation")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	lock := &SessionPublicationLock{tx: tx, db: db}
	if postgres {
		var revision int64
		err = tx.QueryRowContext(ctx, `SELECT operation_revision FROM channel_onboarding_operations WHERE operation_id=$1 FOR UPDATE`, operationID).Scan(&revision)
		if err == nil {
			err = tx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&lock.pid)
		}
	} else {
		var result sql.Result
		result, err = tx.ExecContext(ctx, `UPDATE channel_onboarding_operations SET updated_at=updated_at WHERE operation_id=?`, operationID)
		if err == nil {
			var count int64
			count, err = result.RowsAffected()
			if err == nil && count != 1 {
				err = fmt.Errorf("onboarding lock operation is absent")
			}
		}
	}
	if err != nil {
		return nil, errors.Join(err, lock.Release())
	}
	return lock, nil
}

func (l *SessionPublicationLock) Waiting(ctx context.Context) (bool, error) {
	if l.pid == 0 {
		return false, fmt.Errorf("SQL lock waiter observation requires postgres")
	}
	var blocked bool
	err := l.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)))`, l.pid).Scan(&blocked)
	return blocked, err
}

func (l *SessionPublicationLock) Release() error {
	l.once.Do(func() {
		l.err = l.tx.Rollback()
		if errors.Is(l.err, sql.ErrTxDone) {
			l.err = nil
		}
	})
	return l.err
}
