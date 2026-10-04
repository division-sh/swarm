package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

func TestInspectionSnapshotBindsFixedReadersAndRejectsEscapes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inspection.db")
	writer := newTransactionTestBackend(t, path)
	if _, err := writer.db.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	b, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	other, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var slot FixedReadStatement
	const query = `SELECT value FROM mutation_probe WHERE id = 1`
	stmt, err := slot.PrepareContext(ctx, b, query)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stmt.Close() })
	var retained context.Context
	err = b.InspectSnapshot(ctx, func(snapshot context.Context) error {
		retained = snapshot
		var first int
		if err := b.QueryRowContext(snapshot, query).Scan(&first); err != nil {
			return err
		}
		if _, err := writer.db.ExecContext(ctx, `UPDATE mutation_probe SET value = 2 WHERE id = 1`); err != nil {
			return err
		}
		rows, err := slot.QueryContext(snapshot, b, query)
		if err != nil {
			return err
		}
		if !rows.Next() {
			return fmt.Errorf("fixed reader returned no row: %w", rows.Close())
		}
		var second int
		if err := rows.Scan(&second); err != nil {
			rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if first != 0 || second != first {
			return fmt.Errorf("fixed reader escaped snapshot: %d -> %d", first, second)
		}
		if _, err := slot.QueryContext(snapshot, b, `SELECT 42`); err == nil {
			t.Fatal("inspection changed fixed statement SQL")
		}
		if _, err := slot.PrepareContext(snapshot, b, query); err == nil {
			t.Fatal("inspection acquired a pool-backed statement")
		}
		if _, err := b.ExecContext(snapshot, `UPDATE mutation_probe SET value = 3 WHERE id = 1`); err == nil {
			t.Fatal("inspection permitted write")
		}
		if _, err := b.Conn(snapshot); err == nil {
			t.Fatal("inspection acquired a connection")
		}
		if err := b.RunTransaction(snapshot, "invalid inspection write", func(context.Context, *sql.Tx) error {
			t.Fatal("inspection entered mutable callback")
			return nil
		}); err == nil {
			t.Fatal("inspection permitted mutable transaction")
		}
		if _, err := other.QueryContext(snapshot, query); err == nil {
			t.Fatal("inspection crossed store identity")
		}
		return b.RunReadTransaction(snapshot, func(nested context.Context, tx *sql.Tx) error {
			var value int
			if err := tx.QueryRowContext(nested, query).Scan(&value); err != nil {
				return err
			}
			if value != first {
				return fmt.Errorf("nested reader escaped snapshot: %d", value)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := slot.QueryContext(retained, b, query); err == nil {
		t.Fatal("closed inspection fell back to cached statement")
	}
	if err := b.InspectSnapshot(retained, func(context.Context) error {
		t.Fatal("closed snapshot reentered callback")
		return nil
	}); err == nil {
		t.Fatal("closed inspection reopened")
	}
	var current int
	if err := b.QueryRowContext(ctx, query).Scan(&current); err != nil || current != 2 {
		t.Fatalf("ordinary reader changed: value=%d err=%v", current, err)
	}
}
