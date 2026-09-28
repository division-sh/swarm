package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestTransactionProbeIncludesConnectionPoolWait(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	db.SetMaxOpenConns(1)
	backend, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	collector, restore, err := backend.InstallTransactionProbeForTest(transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	held, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
			var one int
			return tx.QueryRowContext(ctx, `SELECT 1`).Scan(&one)
		})
	}()
	<-started
	time.Sleep(25 * time.Millisecond)
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	counts := collector.Snapshot().Total
	if counts.WriteCommits != 1 || counts.PermitWait != 0 || counts.PoolWait < 20*time.Millisecond ||
		counts.BeginDuration <= 0 || counts.CleanupDuration <= 0 {
		t.Fatalf("transaction phase receipt = %+v", counts)
	}
}
