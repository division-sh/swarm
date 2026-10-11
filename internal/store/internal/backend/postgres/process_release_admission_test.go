package postgres

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
)

func TestPostgresProcessReleaseAdmissionAndDisposition(t *testing.T) {
	for _, phase := range []string{"healthy", "cancel_before", "cancel_queued", "cancel_admitted", "unlock_error", "not_owned", "close_error"} {
		t.Run(phase, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			lease, acquired, err := AcquireAdvisoryLockLease(ctx, db, "process-release-admission")
			if err != nil || !acquired {
				t.Fatalf("acquire=%t err=%v", acquired, err)
			}
			t.Cleanup(func() {
				if err := lease.Release(context.Background()); err != nil {
					t.Errorf("cleanup: %v", err)
				}
			})
			var terminal atomic.Int32
			var clean atomic.Bool
			lease.InstallTerminalOwner(nil, func() {
				terminal.Add(1)
				clean.Store(lease.ReleasedCleanly())
			}, nil)
			failure := errors.New("independent native release failure")
			switch phase {
			case "cancel_before":
				cancel()
			case "cancel_admitted":
				lease.SetUnlockForTest(func(queryCtx context.Context, session *SessionAuthority, key string) (bool, error) {
					cancel()
					if queryCtx.Err() != nil {
						t.Error("logical cancellation interrupted admitted native release")
					}
					var unlocked bool
					err := session.QueryRowContext(queryCtx, unlockAdvisoryLockSQL, key).Scan(&unlocked)
					return unlocked, err
				})
			case "unlock_error":
				lease.SetUnlockForTest(func(context.Context, *SessionAuthority, string) (bool, error) { return false, failure })
			case "not_owned":
				lease.SetUnlockForTest(func(context.Context, *SessionAuthority, string) (bool, error) { return false, nil })
			case "close_error":
				lease.SetReleaseSessionForTest(func() error { return failure })
			}
			if phase == "cancel_queued" {
				entered, resume := make(chan struct{}), make(chan struct{})
				transaction := make(chan error, 1)
				go func() {
					transaction <- lease.RunTransaction(context.Background(), func(context.Context, *sql.Tx) error {
						close(entered)
						<-resume
						return nil
					})
				}()
				<-entered
				queued := make(chan struct{})
				lease.SetBeforeBeginOperationForTest(func() { close(queued) })
				released := make(chan error, 1)
				go func() { released <- lease.ReleaseProcess(ctx) }()
				<-queued
				cancel()
				select {
				case err := <-released:
					close(resume)
					<-transaction
					t.Fatalf("release bypassed queued native ownership: %v", err)
				case <-time.After(20 * time.Millisecond):
				}
				close(resume)
				if err := <-transaction; err != nil {
					t.Fatalf("healthy queued transaction: %v", err)
				}
				err = <-released
				lease.SetBeforeBeginOperationForTest(nil)
			} else {
				err = lease.ReleaseProcess(ctx)
			}
			switch phase {
			case "cancel_before", "cancel_queued":
				if !errors.Is(err, context.Canceled) || !lease.Current() || lease.ReleasedCleanly() || terminal.Load() != 0 {
					t.Fatalf("nonadmitted cancellation lost possession: err=%v current=%t clean=%t terminal=%d", err, lease.Current(), lease.ReleasedCleanly(), terminal.Load())
				}
				if err := lease.ProveCurrent(context.Background()); err != nil {
					t.Fatalf("cancelled release disturbed native possession: %v", err)
				}
			case "unlock_error", "close_error", "not_owned":
				if err == nil || lease.Current() || lease.ReleasedCleanly() || clean.Load() || terminal.Load() != 1 {
					t.Fatalf("failed release fabricated graceful disposition: err=%v current=%t clean=%t terminal=%d", err, lease.Current(), clean.Load(), terminal.Load())
				}
				if phase != "not_owned" && !errors.Is(err, failure) {
					t.Fatalf("independent native cause lost: %v", err)
				}
			default:
				if err != nil || lease.Current() || !lease.ReleasedCleanly() || !clean.Load() || terminal.Load() != 1 {
					t.Fatalf("acknowledged release not distinguished: err=%v current=%t clean=%t terminal=%d", err, lease.Current(), clean.Load(), terminal.Load())
				}
			}
		})
	}
}
