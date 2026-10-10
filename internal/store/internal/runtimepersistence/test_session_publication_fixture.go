package runtimepersistence

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

func HoldSessionPublicationOnboardingLockForTest(ctx context.Context, selected any, operationID string) (*SessionPublicationLock, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	if uuid.Validate(operationID) != nil {
		return nil, fmt.Errorf("onboarding lock requires an exact operation")
	}
	var db *sql.DB
	var postgres bool
	switch owner := selected.(type) {
	case *PostgresStore:
		db, postgres = owner.backend.ConstructionHandle(), true
	case *SQLiteRuntimeStore:
		db = owner.backend.ConstructionHandle()
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

func SetSessionPublicationInsertFaultForTest(ctx context.Context, selected any, enabled bool) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	apply := func(ctx context.Context, tx *sql.Tx, postgres bool) error {
		statements := []string{`DROP TRIGGER reject_whatsapp_publication`}
		if enabled {
			statements = []string{`CREATE TRIGGER reject_whatsapp_publication BEFORE INSERT ON inbound_publication_events BEGIN SELECT RAISE(ABORT,'publication rollback proof'); END`}
		}
		if postgres {
			statements = []string{`DROP TRIGGER reject_whatsapp_publication ON inbound_publication_events`, `DROP FUNCTION reject_whatsapp_publication()`}
			if enabled {
				statements = []string{
					`CREATE FUNCTION reject_whatsapp_publication() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'publication rollback proof'; END $$`,
					`CREATE TRIGGER reject_whatsapp_publication BEFORE INSERT ON inbound_publication_events FOR EACH ROW EXECUTE FUNCTION reject_whatsapp_publication()`,
				}
			}
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return apply(ctx, tx, true) })
	case *SQLiteRuntimeStore:
		return owner.backend.RunTransaction(ctx, "session publication insert fault fixture", func(ctx context.Context, tx *sql.Tx) error { return apply(ctx, tx, false) })
	default:
		return fmt.Errorf("unsupported session publication fault owner %T", selected)
	}
}
