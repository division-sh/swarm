package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func TestReporterDelayedReadJoinPrecedesFixtureCancellation(t *testing.T) {
	for _, joinBeforeCancel := range []bool{false, true} {
		name := "stop_then_cancel"
		if joinBeforeCancel {
			name = "stop_join_then_cancel"
		}
		t.Run(name, func(t *testing.T) {
			backend, driverProbe := newExitProbe(t)
			collector, restore, err := backend.InstallTransactionProbeForTest(transactiontest.Options{
				Delay: 300 * time.Millisecond, DelayScope: transactiontest.DelayAllCommits,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- backend.RunReadTransaction(ctx, func(ctx context.Context, _ *sql.Tx) error {
					transactiontest.Mark(ctx, transactiontest.FanOutObservation)
					return nil
				})
			}()
			class := transactiontest.ActiveClass{Operation: transactiontest.FanOutObservation, Phase: transactiontest.PhaseCommitDelay, ReadOnly: true}
			timer := time.NewTimer(3 * time.Second)
			defer timer.Stop()
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for collector.Snapshot().ActiveByClass[class] != 1 {
				select {
				case err := <-done:
					t.Fatalf("read returned before its observed delay cut: %v", err)
				case <-ticker.C:
				case <-timer.C:
					t.Fatal("read never reached native pre-COMMIT delay")
				}
			}
			collector.Stop()
			if joinBeforeCancel {
				joinCtx, stopJoin := context.WithTimeout(context.Background(), 3*time.Second)
				if err := collector.StopAndJoin(joinCtx); err != nil {
					stopJoin()
					t.Fatal(err)
				}
				stopJoin()
				cancel()
				err = <-done
			} else {
				cancel()
				select {
				case <-driverProbe.rollbackDone:
				case <-timer.C:
					t.Fatal("read cancellation did not join real driver rollback")
				}
				err = <-done
			}
			snapshot := collector.Snapshot()
			t.Logf("cut=%s error=%T %v context=%v active=%d receipt=%+v", name, err, err, ctx.Err(), snapshot.Active, snapshot.Total)
			if snapshot.Active != 0 || snapshot.Total.CleanupFailures != 0 {
				t.Fatalf("native cleanup was not joined: %+v", snapshot)
			}
			if joinBeforeCancel {
				if err != nil || snapshot.Total.ReadCommits != 1 || snapshot.Total.CommitFailures != 0 {
					t.Fatalf("joined-before-cancel control failed: err=%v snapshot=%+v", err, snapshot)
				}
			} else if (!errors.Is(err, sql.ErrTxDone) && !errors.Is(err, context.Canceled)) || snapshot.Total.ReadCommits != 0 || snapshot.Total.CommitFailures != 1 || snapshot.Total.RollbackAttempts != 1 {
				t.Fatalf("cancellation cut did not reproduce native unacknowledged COMMIT: err=%v snapshot=%+v", err, snapshot)
			}
		})
	}
}
