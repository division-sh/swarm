package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func TestCommitFailureReturnsCleanConnectionProbe(t *testing.T) {
	for _, failAtCommit := range []bool{false, true} {
		name := "callback_rollback_control"
		if failAtCommit {
			name = "deferred_commit_failure"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "commit.db")
			b := newTransactionTestBackend(t, path)
			for _, query := range []string{
				"PRAGMA foreign_keys=ON",
				"CREATE TABLE parent (id INTEGER PRIMARY KEY)",
				"CREATE TABLE child (parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
			} {
				if _, err := b.db.Exec(query); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			err := b.RunTransaction(ctx, "commit failure probe", func(ctx context.Context, tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, "UPDATE mutation_probe SET value=1 WHERE id=1"); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, "INSERT INTO child(parent_id) VALUES (99)"); err != nil {
					return err
				}
				if !failAtCommit {
					return errors.New("callback failure control")
				}
				return nil
			})
			if err == nil {
				t.Fatal("injection unexpectedly succeeded")
			}
			var pooled, independent int
			if err := b.db.QueryRow("SELECT value FROM mutation_probe WHERE id=1").Scan(&pooled); err != nil {
				t.Fatal(err)
			}
			other, openErr := sql.Open("sqlite", "file:"+path)
			if openErr != nil {
				t.Fatal(openErr)
			}
			defer other.Close()
			if err := other.QueryRow("SELECT value FROM mutation_probe WHERE id=1").Scan(&independent); err != nil {
				t.Fatal(err)
			}
			entered := false
			nextErr := b.RunTransaction(ctx, "next ordinary transaction", func(context.Context, *sql.Tx) error { entered = true; return nil })
			t.Logf("failure=%v pooled=%d independent=%d next_entered=%t next_error=%v", err, pooled, independent, entered, nextErr)
			if pooled != 0 || independent != 0 || !entered || nextErr != nil {
				t.Error("failed commit did not return a rolled-back reusable connection")
			}
		})
	}
}
