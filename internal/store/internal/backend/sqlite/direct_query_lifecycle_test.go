package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

// Identical SQL must not share bindings or cursor state while an earlier result
// remains open. This exercises the direct-query path used by persistence owners,
// not reuse of a caller-prepared statement with an outstanding result.
func TestDirectQueryOverlappingCursorsThroughTransactionOwner(t *testing.T) {
	b := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "direct-cursors.db"))
	for _, readOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("read_only=%t", readOnly), func(t *testing.T) {
			callback := func(ctx context.Context, tx *sql.Tx) error {
				const query = `SELECT ? UNION ALL SELECT ?`
				first, err := tx.QueryContext(ctx, query, 11, 12)
				if err != nil {
					return err
				}
				defer first.Close()
				if err := expectDirectQueryRow(first, 11); err != nil {
					return err
				}
				second, err := tx.QueryContext(ctx, query, 21, 22)
				if err != nil {
					return err
				}
				defer second.Close()
				if err := expectDirectQueryRow(second, 21); err != nil {
					return err
				}
				if err := expectDirectQueryRow(first, 12); err != nil {
					return err
				}
				if first.Next() || first.Err() != nil {
					return fmt.Errorf("first cursor end: %v", first.Err())
				}
				if err := expectDirectQueryRow(second, 22); err != nil {
					return err
				}
				if second.Next() || second.Err() != nil {
					return fmt.Errorf("second cursor end: %v", second.Err())
				}
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				if rows, err := tx.QueryContext(cancelled, query, 31, 32); !errors.Is(err, context.Canceled) {
					if rows != nil {
						_ = rows.Close()
					}
					return fmt.Errorf("cancelled query: %v", err)
				}
				var got int
				if err := tx.QueryRowContext(ctx, `SELECT ?`, 41).Scan(&got); err != nil {
					return err
				}
				if got != 41 {
					return fmt.Errorf("query after cancellation = %d", got)
				}
				return nil
			}
			var err error
			if readOnly {
				err = b.RunReadTransaction(context.Background(), callback)
			} else {
				err = b.RunTransaction(context.Background(), "direct query lifecycle", callback)
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
	if err := b.RunReadTransaction(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		var got int
		if err := tx.QueryRowContext(ctx, `SELECT ?`, 51).Scan(&got); err != nil {
			return err
		}
		if got != 51 {
			return fmt.Errorf("next transaction = %d", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func expectDirectQueryRow(rows *sql.Rows, want int) error {
	if !rows.Next() {
		return fmt.Errorf("missing row %d: %v", want, rows.Err())
	}
	var got int
	if err := rows.Scan(&got); err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("row = %d, want %d", got, want)
	}
	return nil
}
