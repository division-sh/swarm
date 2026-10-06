package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/lib/pq"
)

// Only native settlement owners may mint this evidence, after explicit
// pre-COMMIT rollback and clean connection/session disposition.
type rolledBackSerializationConflict struct{ cause error }

func (e *rolledBackSerializationConflict) Error() string { return e.cause.Error() }
func (e *rolledBackSerializationConflict) Unwrap() error { return e.cause }

// IsRolledBackSerializationConflict accepts only sealed native evidence, not a
// SQLSTATE found somewhere in an error tree or a join with independent failure.
func IsRolledBackSerializationConflict(err error) bool {
	_, ok := err.(*rolledBackSerializationConflict)
	return ok
}

func exactSerializationConflict(err error) bool {
	for err != nil {
		if pgErr, ok := err.(*pq.Error); ok {
			return pgErr.Code == "40001"
		}
		wrapped, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = wrapped.Unwrap()
	}
	return false
}

func (b *Backend) RunTransaction(ctx context.Context, operation func(context.Context, *sql.Tx) error) (err error) {
	_, err = b.RunTransactionOutcome(ctx, operation)
	return err
}

// RunTransactionOutcome reports acknowledged COMMIT independently of cleanup
// errors. False means no acknowledged commit, not proof of rollback.
func (b *Backend) RunTransactionOutcome(ctx context.Context, operation func(context.Context, *sql.Tx) error) (committed bool, err error) {
	return b.RunTransactionWithOptionsOutcome(ctx, nil, operation)
}

func (b *Backend) RunTransactionWithOptions(ctx context.Context, opts *sql.TxOptions, operation func(context.Context, *sql.Tx) error) error {
	_, err := b.RunTransactionWithOptionsOutcome(ctx, opts, operation)
	return err
}

func (b *Backend) RunTransactionWithOptionsOutcome(ctx context.Context, opts *sql.TxOptions, operation func(context.Context, *sql.Tx) error) (bool, error) {
	return b.runTransactionOutcome(ctx, opts, true, operation)
}

// RunReadTransaction owns one caller-scoped, transactionally consistent read.
func (b *Backend) RunReadTransaction(ctx context.Context, operation func(context.Context, *sql.Tx) error) error {
	if tx, err := b.inspectionTransaction(ctx); err != nil {
		return err
	} else if tx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		if operation == nil {
			return nil
		}
		return errors.Join(operation(b.inspectionSQLContext(ctx), tx), ctx.Err())
	}
	_, err := b.runTransactionOutcome(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}, false, operation)
	return err
}

func (b *Backend) runTransactionOutcome(ctx context.Context, opts *sql.TxOptions, drain bool, operation func(context.Context, *sql.Tx) error) (committed bool, err error) {
	if err := b.refuseInspectionMutation(ctx); err != nil {
		return false, err
	}
	if !b.Valid() {
		return false, fmt.Errorf("postgres backend is required")
	}
	if operation == nil {
		return false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	poolStarted := time.Now()
	conn, err := b.db.Conn(ctx)
	if err != nil {
		return false, err
	}
	poolWait := time.Since(poolStarted)
	discard := false
	serializationConflict := false
	var tx *sql.Tx
	probe := b.testTransactions.Begin(opts != nil && opts.ReadOnly, false)
	defer func() { probe.Finish(err) }()
	defer func() {
		cleanupStarted := time.Now()
		var cleanupErr error
		rolledBack := false
		if tx != nil {
			probe.RollbackAttempted()
			rollbackErr := tx.Rollback()
			rolledBack = rollbackErr == nil
			if rollbackErr != nil {
				discard = true
				if rollbackErr != sql.ErrTxDone {
					cleanupErr = rollbackErr
				}
			}
		}
		if discard {
			rawErr := conn.Raw(func(any) error { return driver.ErrBadConn })
			if rawErr == driver.ErrBadConn || rawErr == sql.ErrConnDone {
				rawErr = nil
			}
			cleanupErr = errors.Join(cleanupErr, rawErr)
		}
		closeErr := conn.Close()
		if closeErr == sql.ErrConnDone {
			closeErr = nil
		}
		cleanupErr = errors.Join(cleanupErr, closeErr)
		if cleanupErr != nil {
			slog.Error("postgres transaction cleanup failed", "error", cleanupErr)
			err = errors.Join(err, cleanupErr)
		}
		if serializationConflict && rolledBack && cleanupErr == nil && ctx.Err() == nil {
			err = &rolledBackSerializationConflict{cause: err}
		}
		probe.RecordCleanup(time.Since(cleanupStarted))
	}()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	// The admitted unit is closed SQL and result cleanup, not arbitrary external
	// work. Logical operation cancellation is checked before COMMIT admission.
	sqlCtx := ctx
	if drain {
		sqlCtx = context.WithoutCancel(ctx)
	}
	beginStarted := time.Now()
	tx, err = conn.BeginTx(sqlCtx, opts)
	probe.RecordAdmission(0, poolWait, time.Since(beginStarted))
	if err != nil {
		if callerErr := ctx.Err(); callerErr != nil {
			return false, errors.Join(callerErr, err)
		}
		return false, err
	}
	probe.Begun()
	sqlCtx = transactiontest.WithAttempt(sqlCtx, probe)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if operationErr := operation(sqlCtx, tx); operationErr != nil {
		if callerErr := ctx.Err(); callerErr != nil {
			return false, errors.Join(callerErr, operationErr)
		}
		serializationConflict = drain && (opts == nil || !opts.ReadOnly) && exactSerializationConflict(operationErr)
		return false, operationErr
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	probe.BeforeCommit()
	if commitErr := tx.Commit(); commitErr != nil {
		probe.CommitFailed()
		discard = true
		return false, commitErr
	}
	probe.Committed()
	tx = nil
	return true, nil
}
