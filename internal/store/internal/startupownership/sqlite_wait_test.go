//go:build darwin || linux

package startupownership

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteServingWaitJoinsBeforeReturningOrPublishing(t *testing.T) {
	for _, cancelWait := range []bool{false, true} {
		t.Run(map[bool]string{false: "handoff", true: "cancelled_wait"}[cancelWait], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "runtime.db")
			holder, err := AcquireSQLiteConstructionGuard(path)
			if err != nil {
				t.Fatal(err)
			}
			defer holder.Release()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type result struct {
				guard *SQLiteConstructionGuard
				err   error
			}
			returned := make(chan result, 1)
			started := make(chan struct{})
			go func() {
				close(started)
				guard, err := WaitSQLiteConstructionGuard(ctx, path)
				returned <- result{guard, err}
			}()
			<-started
			select {
			case got := <-returned:
				if got.guard != nil {
					_ = got.guard.Release()
				}
				t.Fatalf("native contention returned rather than waiting: %v", got.err)
			case <-time.After(20 * time.Millisecond):
			}
			if cancelWait {
				cancel()
				select {
				case got := <-returned:
					t.Fatalf("cancellation acknowledged an unjoined flock: %+v", got)
				case <-time.After(20 * time.Millisecond):
				}
			}
			if err := holder.Release(); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-returned:
				if cancelWait {
					if got.guard != nil || !errors.Is(got.err, context.Canceled) {
						t.Fatalf("cancelled wait published possession: %+v", got)
					}
				} else {
					if got.err != nil || got.guard == nil {
						t.Fatalf("handoff: %+v", got)
					}
					if err := got.guard.Release(); err != nil {
						t.Fatal(err)
					}
				}
			case <-time.After(time.Second):
				t.Fatal("released holder did not join the native waiter")
			}
			successor, err := AcquireSQLiteConstructionGuard(path)
			if err != nil {
				t.Fatalf("waiter leaked native possession: %v", err)
			}
			if err := successor.Release(); err != nil {
				t.Fatal(err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := filepath.Join(t.TempDir(), "missing", "runtime.db")
	if guard, err := WaitSQLiteConstructionGuard(ctx, path); guard != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel-before: guard=%v err=%v", guard, err)
	}
}
