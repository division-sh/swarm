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
	return b.runTransaction(ctx, nil, operation)
}

// RunReadTransaction owns one caller-scoped, transactionally consistent read.
func (b *Backend) RunReadTransaction(ctx context.Context, operation func(context.Context, *sql.Tx) error) error {
	return b.runTransaction(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}, operation)
}

func (b *Backend) runTransaction(ctx context.Context, opts *sql.TxOptions, operation func(context.Context, *sql.Tx) error) (err error) {
	if !b.Valid() {
		return fmt.Errorf("postgres backend is required")
	}
	if operation == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := b.db.Conn(ctx)
	if err != nil {
		return err
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
	tx, err = conn.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	if operationErr := operation(ctx, tx); operationErr != nil {
		return errors.Join(ctx.Err(), operationErr)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if commitErr := tx.Commit(); commitErr != nil {
		discard = true
		if commitErr == sql.ErrTxDone && ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.Join(ctx.Err(), commitErr)
	}
	tx = nil
	return nil
}
