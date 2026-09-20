package authoractivity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
)

func postgresOrderFixture(t *testing.T) *sql.DB {
	t.Helper()
	_, db, _ := testutil.StartEmptyPostgres(t)
	if _, err := db.Exec(`CREATE TABLE author_activity_order (
		singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
		last_sequence BIGINT NOT NULL CHECK (last_sequence >= 0))`); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestPostgresOrderExistingRowNeedsNoInitializationWrite(t *testing.T) {
	db := postgresOrderFixture(t)
	for _, query := range []string{
		`INSERT INTO author_activity_order VALUES (1, 17)`,
		`CREATE FUNCTION reject_order_write() RETURNS trigger LANGUAGE plpgsql AS $$
		 BEGIN RAISE EXCEPTION 'unexpected ordering write'; END $$`,
		`CREATE TRIGGER reject_order_write BEFORE INSERT OR UPDATE ON author_activity_order
		 FOR EACH ROW EXECUTE FUNCTION reject_order_write()`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	m, err := Begin(ctx, tx, DialectPostgres)
	if err != nil || m.last != 17 {
		t.Fatalf("existing order: mutation=%+v error=%v", m, err)
	}
	if err := FenceMutationOrder(ctx, tx, DialectPostgres); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := Begin(ctx, tx, DialectPostgres); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("closed transaction: %v", err)
	}
}

func TestPostgresOrderConcurrentInitializationAndFreshLock(t *testing.T) {
	for _, existing := range []bool{false, true} {
		for _, commit := range []bool{false, true} {
			t.Run(fmt.Sprintf("existing=%t/commit=%t", existing, commit), func(t *testing.T) {
				db := postgresOrderFixture(t)
				if existing {
					if _, err := db.Exec(`INSERT INTO author_activity_order VALUES (1, 11)`); err != nil {
						t.Fatal(err)
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				first, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer first.Rollback()
				m, err := Begin(ctx, first, DialectPostgres)
				if err != nil {
					t.Fatal(err)
				}
				if err := m.updateLast(ctx, 19); err != nil {
					t.Fatal(err)
				}
				second, err := db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer second.Rollback()
				var pid int
				if err := second.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
					t.Fatal(err)
				}
				type result struct {
					mutation *Mutation
					err      error
				}
				done := make(chan result, 1)
				go func() {
					m, err := Begin(ctx, second, DialectPostgres)
					done <- result{m, err}
				}()
				// Observe the actual database lock wait, not a scheduling sleep.
				for {
					var waiting bool
					if err := db.QueryRowContext(ctx, `SELECT COALESCE(wait_event_type = 'Lock', FALSE)
						FROM pg_stat_activity WHERE pid = $1`, pid).Scan(&waiting); err != nil {
						t.Fatal(err)
					}
					if waiting {
						break
					}
					select {
					case got := <-done:
						t.Fatalf("second mutation escaped the ordering fence: %+v", got)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(time.Millisecond):
					}
				}
				want := int64(0)
				if commit {
					want = 19
					if err := first.Commit(); err != nil {
						t.Fatal(err)
					}
				} else {
					if existing {
						want = 11
					}
					if err := first.Rollback(); err != nil {
						t.Fatal(err)
					}
				}
				got := <-done
				if got.err != nil || got.mutation.last != want {
					t.Fatalf("second mutation=%+v error=%v want sequence=%d", got.mutation, got.err, want)
				}
				if err := second.Commit(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestPostgresOrderReadFailureDoesNotInitialize(t *testing.T) {
	db := postgresOrderFixture(t)
	if _, err := db.Exec(`ALTER TABLE author_activity_order RENAME COLUMN last_sequence TO broken_sequence`); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := Begin(ctx, tx, DialectPostgres); err == nil {
		t.Fatal("invalid schema was accepted")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM author_activity_order`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed initialization mutated order: count=%d error=%v", count, err)
	}
}
