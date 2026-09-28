package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func TestTransactionProbeIncludesWriterPermitAndConnectionPhases(t *testing.T) {
	backend := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "timing.db"))
	collector, restore, err := backend.InstallTransactionProbeForTest(transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	holderStarted := make(chan struct{})
	holderRelease := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- backend.RunTransaction(ctx, "permit holder", func(context.Context, *sql.Tx) error {
			close(holderStarted)
			select {
			case <-holderRelease:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	<-holderStarted
	queuedDone := make(chan error, 1)
	go func() {
		queuedDone <- backend.RunTransaction(ctx, "queued writer", func(context.Context, *sql.Tx) error { return nil })
	}()
	waitForQueuedMutations(t, backend, 1)
	time.Sleep(25 * time.Millisecond)
	close(holderRelease)
	if err := <-holderDone; err != nil {
		t.Fatal(err)
	}
	if err := <-queuedDone; err != nil {
		t.Fatal(err)
	}
	counts := collector.Snapshot().Total
	if counts.WriteCommits != 2 || counts.PermitWait < 20*time.Millisecond ||
		counts.PoolWait <= 0 || counts.BeginDuration <= 0 || counts.CleanupDuration <= 0 {
		t.Fatalf("transaction phase receipt = %+v", counts)
	}
}
