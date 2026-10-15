package postgres

import (
	"context"
	"database/sql"
	"fmt"
)

// Fix the original one-connection cut before any administrative session is
// retained. Active capacity reservations cannot be resized by a fixture.
func (b *Backend) LimitAdministrativeFixturePool(ctx context.Context) error {
	if !b.Valid() {
		return fmt.Errorf("administrative pool fixture requires an initialized postgres owner")
	}
	b.capacityMu.Lock()
	defer b.capacityMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.capacityReservations != 0 {
		return fmt.Errorf("administrative pool fixture requires no retained sessions")
	}
	if b.db.Stats().InUse != 0 {
		return fmt.Errorf("administrative pool fixture requires no active connection work")
	}
	if err := b.Ping(ctx); err != nil {
		return err
	}
	b.db.SetMaxOpenConns(1)
	return nil
}

func (b *Backend) ReadAdministrativeUnrelatedProgress(ctx context.Context) (int, error) {
	var value int
	err := b.QueryRowContext(ctx, `SELECT 1`).Scan(&value)
	return value, err
}

func ReadAdministrativeSurroundingTransactionTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var value int
	err := tx.QueryRowContext(ctx, `SELECT 1`).Scan(&value)
	return value, err
}
