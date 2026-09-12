package postgres

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
)

func TestImplicitTransactionScopeEndsAtSettlement(t *testing.T) {
	for _, settlement := range []string{"commit", "rollback"} {
		t.Run(settlement, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			var pid int
			if err := conn.QueryRowContext(context.Background(), "SELECT pg_backend_pid()").Scan(&pid); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tx, err := conn.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if settlement == "commit" {
				err = tx.Commit()
			} else {
				err = tx.Rollback()
			}
			if err != nil {
				t.Fatal(err)
			}
			cancel()
			var nextPID int
			if err := conn.QueryRowContext(context.Background(), "SELECT pg_backend_pid()").Scan(&nextPID); err != nil {
				t.Fatalf("settled transaction governed independent query: %v", err)
			}
			if nextPID != pid {
				t.Fatal("healthy connection was replaced")
			}
			stmt, err := conn.PrepareContext(context.Background(), "SELECT $1::int")
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()
			var got int
			if err := stmt.QueryRowContext(context.Background(), 7).Scan(&got); err != nil || got != 7 {
				t.Fatalf("independent prepared query = %d, %v", got, err)
			}
			if _, err := stmt.ExecContext(context.Background(), 8); err != nil {
				t.Fatal(err)
			}
			next, err := conn.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer next.Rollback()
			if _, err := next.ExecContext(context.Background(), "SELECT 1"); err != nil {
				t.Fatal(err)
			}
			if err := next.Commit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestImplicitSuccessorScopeDoesNotInheritPredecessor(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}
	second, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Rollback()
	cancel()
	if _, err := second.ExecContext(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("predecessor cancellation governed successor transaction: %v", err)
	}
	if err := second.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestImplicitTransactionScopeDoesNotBlockAdvisoryCleanup(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Mirror conversation-fork's retained connection, transaction and independent
	// cleanup contexts. The fallback cleanup only prevents a failed probe leaking a lock.
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock_all()")
	}()
	if _, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_lock(244105)"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	cancel()
	var unlocked bool
	if err := conn.QueryRowContext(context.Background(), "SELECT pg_advisory_unlock(244105)").Scan(&unlocked); err != nil || !unlocked {
		t.Fatalf("independent advisory cleanup = %v, %v", unlocked, err)
	}
}
