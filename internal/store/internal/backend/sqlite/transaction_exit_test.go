package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestReviewerTransactionPanicCleanup(t *testing.T) {
	for _, read := range []bool{false, true} {
		name := "write"
		if read {
			name = "read"
		}
		t.Run(name, func(t *testing.T) {
			b := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "panic.db"))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			marker := errors.New("callback panic")
			func() {
				defer func() {
					if recover() != marker {
						t.Error("panic not preserved")
					}
				}()
				callback := func(ctx context.Context, tx *sql.Tx) error {
					var value int
					if err := tx.QueryRowContext(ctx, "SELECT value FROM mutation_probe WHERE id=1").Scan(&value); err != nil {
						t.Fatal(err)
					}
					panic(marker)
				}
				if read {
					_ = b.RunReadTransaction(ctx, callback)
				} else {
					_ = b.RunTransaction(ctx, "panic", callback)
				}
			}()
			next, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer stop()
			var value int
			err := b.db.QueryRowContext(next, "SELECT value FROM mutation_probe WHERE id=1").Scan(&value)
			t.Logf("read=%v in_use=%d next_read_error=%v", read, b.db.Stats().InUse, err)
			if err != nil {
				t.Error("callback panic leaked transaction connection")
			}
		})
	}
}

func TestReviewerCommitFailureCapturesLaterAutocommitWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "later.db")
	b := newTransactionTestBackend(t, path)
	for _, q := range []string{"PRAGMA foreign_keys=ON", "CREATE TABLE parent(id INTEGER PRIMARY KEY)", "CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)"} {
		if _, err := b.db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	err := b.RunTransaction(context.Background(), "commit cut", func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "INSERT INTO child VALUES(99)")
		return err
	})
	if err == nil {
		t.Fatal("expected commit failure")
	}
	if _, err := b.Exec("UPDATE mutation_probe SET value=2 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	other, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	var pooled, durable int
	if err := b.db.QueryRow("SELECT value FROM mutation_probe WHERE id=1").Scan(&pooled); err != nil {
		t.Fatal(err)
	}
	if err := other.QueryRow("SELECT value FROM mutation_probe WHERE id=1").Scan(&durable); err != nil {
		t.Fatal(err)
	}
	t.Logf("successful later Exec: pooled=%d durable=%d", pooled, durable)
	if durable != 2 {
		t.Error("later successful autocommit write was captured by failed transaction")
	}
}

func TestReviewerBusyAtCommitRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "busy-commit.db")
	b := newTransactionTestBackend(t, path)
	other, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	reader, err := other.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()
	var value int
	if err := reader.QueryRow("SELECT value FROM mutation_probe WHERE id=1").Scan(&value); err != nil {
		t.Fatal(err)
	}
	released := make(chan struct{})
	go func() { time.Sleep(75 * time.Millisecond); _ = reader.Rollback(); close(released) }()
	defer func() { <-released }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	attempts := 0
	err = b.RunTransaction(ctx, "busy at commit", func(ctx context.Context, tx *sql.Tx) error {
		attempts++
		_, err := tx.ExecContext(ctx, "UPDATE mutation_probe SET value=value+1 WHERE id=1")
		return err
	})
	t.Logf("attempts=%d result=%v", attempts, err)
	if err != nil {
		t.Fatalf("busy COMMIT recovery failed: %v", err)
	}
	if err := other.QueryRow("SELECT value FROM mutation_probe WHERE id=1").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != 1 {
		t.Fatalf("durable increments=%d, want 1", value)
	}
}
