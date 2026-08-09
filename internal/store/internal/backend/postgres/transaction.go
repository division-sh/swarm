package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
)

func (b *Backend) RunTransaction(ctx context.Context, operation func(context.Context, *sql.Tx) error) (err error) {
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
	defer func() {
		if discard {
			rawErr := conn.Raw(func(any) error { return driver.ErrBadConn })
			if errors.Is(rawErr, driver.ErrBadConn) {
				rawErr = nil
			}
			err = errors.Join(err, rawErr)
		}
		err = errors.Join(err, conn.Close())
	}()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if operationErr := operation(ctx, tx); operationErr != nil {
		rollbackErr := tx.Rollback()
		if errors.Is(rollbackErr, sql.ErrTxDone) {
			rollbackErr = nil
		}
		if rollbackErr != nil {
			discard = true
		}
		return errors.Join(operationErr, rollbackErr)
	}
	if commitErr := tx.Commit(); commitErr != nil {
		discard = true
		rollbackErr := tx.Rollback()
		if errors.Is(rollbackErr, sql.ErrTxDone) {
			rollbackErr = nil
		}
		return errors.Join(commitErr, rollbackErr)
	}
	return nil
}
