package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
)

func TestRunReadTransactionRetainsRepeatableSnapshot(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`
		CREATE TABLE postgres_read_snapshot_probe (
			id integer PRIMARY KEY,
			value integer NOT NULL
		);
		INSERT INTO postgres_read_snapshot_probe (id, value) VALUES (1, 1)
	`); err != nil {
		t.Fatalf("prepare snapshot probe: %v", err)
	}
	backend, err := New(db)
	if err != nil {
		t.Fatalf("construct PostgreSQL backend: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var isolation string
		if err := tx.QueryRowContext(txctx, `SHOW transaction_isolation`).Scan(&isolation); err != nil {
			return err
		}
		if isolation != "repeatable read" {
			t.Fatalf("read transaction isolation = %q, want repeatable read", isolation)
		}

		var before int
		if err := tx.QueryRowContext(txctx, `SELECT value FROM postgres_read_snapshot_probe WHERE id = 1`).Scan(&before); err != nil {
			return err
		}

		updateReady := make(chan struct{})
		releaseUpdate := make(chan struct{})
		updateDone := make(chan error, 1)
		go func() {
			close(updateReady)
			<-releaseUpdate
			_, err := db.ExecContext(ctx, `UPDATE postgres_read_snapshot_probe SET value = 2 WHERE id = 1`)
			updateDone <- err
		}()
		<-updateReady
		close(releaseUpdate)
		if err := <-updateDone; err != nil {
			return err
		}

		var after int
		if err := tx.QueryRowContext(txctx, `SELECT value FROM postgres_read_snapshot_probe WHERE id = 1`).Scan(&after); err != nil {
			return err
		}
		if before != 1 || after != before {
			t.Fatalf("read snapshot changed across committed update: before=%d after=%d", before, after)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("run repeatable read transaction: %v", err)
	}

	var committed int
	if err := db.QueryRowContext(ctx, `SELECT value FROM postgres_read_snapshot_probe WHERE id = 1`).Scan(&committed); err != nil {
		t.Fatalf("read committed update: %v", err)
	}
	if committed != 2 {
		t.Fatalf("committed value = %d, want 2", committed)
	}
}
