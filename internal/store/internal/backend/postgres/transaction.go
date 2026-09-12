package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
)

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
	_, err := b.runTransactionOutcome(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}, false, operation)
	return err
}

func (b *Backend) runTransactionOutcome(ctx context.Context, opts *sql.TxOptions, drain bool, operation func(context.Context, *sql.Tx) error) (committed bool, err error) {
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
	conn, err := b.db.Conn(ctx)
	if err != nil {
		return false, err
	}
	discard := false
	var tx *sql.Tx
	defer func() {
		var cleanupErr error
		if tx != nil {
			rollbackErr := tx.Rollback()
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
	tx, err = conn.BeginTx(sqlCtx, opts)
	if err != nil {
		if callerErr := ctx.Err(); callerErr != nil {
			return false, errors.Join(callerErr, err)
		}
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if operationErr := operation(sqlCtx, tx); operationErr != nil {
		if callerErr := ctx.Err(); callerErr != nil {
			return false, errors.Join(callerErr, operationErr)
		}
		return false, operationErr
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if commitErr := tx.Commit(); commitErr != nil {
		discard = true
		return false, commitErr
	}
	tx = nil
	return true, nil
}
