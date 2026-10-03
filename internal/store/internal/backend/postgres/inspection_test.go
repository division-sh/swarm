package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
)

func TestInspectionSnapshotBindsReadersAndRejectsEscapes(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`CREATE TABLE inspection_probe (value integer NOT NULL); INSERT INTO inspection_probe VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	b, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	other, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var retained context.Context
	err = b.InspectSnapshot(ctx, func(snapshot context.Context) error {
		retained = snapshot
		var first int
		if err := b.QueryRowContext(snapshot, `SELECT value FROM inspection_probe`).Scan(&first); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `UPDATE inspection_probe SET value = 2`); err != nil {
			return err
		}
		if err := b.RunReadTransaction(snapshot, func(nested context.Context, tx *sql.Tx) error {
			var second int
			if err := tx.QueryRowContext(nested, `SELECT value FROM inspection_probe`).Scan(&second); err != nil {
				return err
			}
			if first != 1 || second != first {
				return fmt.Errorf("snapshot changed: %d -> %d", first, second)
			}
			return nil
		}); err != nil {
			return err
		}
		if _, err := b.ExecContext(snapshot, `UPDATE inspection_probe SET value = 3`); err == nil {
			t.Fatal("inspection permitted a write")
		}
		if _, err := b.Conn(snapshot); err == nil {
			t.Fatal("inspection acquired a connection")
		}
		if _, err := b.BeginTx(snapshot, nil); err == nil {
			t.Fatal("inspection acquired a new transaction")
		}
		if err := b.RunTransaction(snapshot, func(context.Context, *sql.Tx) error {
			t.Fatal("inspection entered a mutable callback")
			return nil
		}); err == nil {
			t.Fatal("inspection permitted mutable transaction")
		}
		if _, err := other.QueryContext(snapshot, `SELECT value FROM inspection_probe`); err == nil {
			t.Fatal("inspection crossed store identity")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.QueryContext(retained, `SELECT value FROM inspection_probe`); err == nil {
		t.Fatal("closed inspection fell back to live pool")
	}
	if err := b.InspectSnapshot(retained, func(context.Context) error {
		t.Fatal("closed snapshot reentered callback")
		return nil
	}); err == nil {
		t.Fatal("closed inspection reopened")
	}
	var current int
	if err := b.QueryRowContext(ctx, `SELECT value FROM inspection_probe`).Scan(&current); err != nil || current != 2 {
		t.Fatalf("ordinary reader changed: value=%d err=%v", current, err)
	}
}
