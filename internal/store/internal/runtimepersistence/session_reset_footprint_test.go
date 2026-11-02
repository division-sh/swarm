package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/lib/pq"
)

// Invariant/reach: multi-run reset admits its full captured footprint before
// the first session lock, and reports exactly the sessions it actually resets.
func TestSessionResetAdmitsFullFootprintBeforeSessionLocksBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		store := selected.selected.(completionSettlementTestStore)
		ctx := originalStoreFixtureContext(t, testAuthorActivityContext(), store)
		fixtures := []completionSettlementFixture{
			newCompletionSettlementFixture(t, store, selected.db, !selected.postgres, ctx),
			newCompletionSettlementFixture(t, store, selected.db, !selected.postgres, ctx),
		}
		sort.Slice(fixtures, func(i, j int) bool {
			return fixtures[i].authority.Target.AgentIdentity.RunID < fixtures[j].authority.Target.AgentIdentity.RunID
		})
		ctx = fixtures[0].original
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		resetter := selected.selected.(interface {
			ResetAll(context.Context, runtimesessions.ResetMetadata) (runtimesessions.ResetSummary, error)
		})
		var summary runtimesessions.ResetSummary
		var resetErr error
		if selected.postgres {
			entered, release := make(chan struct{}), make(chan struct{})
			lockerDone, resetDone := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			var lockerErr error
			var pid int
			resetStarted := false
			defer func() {
				unblock()
				cancel()
				<-lockerDone
				if resetStarted {
					<-resetDone
				}
			}()
			go func() {
				defer close(lockerDone)
				lockerErr = runSelectedFixtureMutation(ctx, selected.selected, "held reset session", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
					return attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
						if _, err := selectedAdmissionLifecycleWriter(selected.selected).RequireActiveSourceTx(txctx, tx, fixtures[0].authority.Target.AgentIdentity.RunID); err != nil {
							return err
						}
						var sessionID string
						if err := tx.QueryRowContext(txctx, `SELECT session_id::text FROM agent_sessions WHERE session_id=$1::uuid FOR UPDATE`, fixtures[0].sessionID).Scan(&sessionID); err != nil {
							return err
						}
						if err := tx.QueryRowContext(txctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
							return err
						}
						close(entered)
						select {
						case <-release:
							return nil
						case <-ctx.Done():
							return ctx.Err()
						}
					})
				})
			}()
			select {
			case <-entered:
			case <-lockerDone:
				t.Fatal(lockerErr)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			resetStarted = true
			go func() {
				defer close(resetDone)
				summary, resetErr = resetter.ResetAll(ctx, runtimesessions.ResetMetadata{Source: "full-footprint"})
			}()
			for {
				var blocked bool
				if err := selected.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, pid).Scan(&blocked); err != nil {
					t.Fatal(err)
				}
				if blocked {
					break
				}
				select {
				case <-resetDone:
					t.Fatalf("reset bypassed held session: %v", resetErr)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(time.Millisecond):
				}
			}
			probeErr := runSelectedFixtureMutation(ctx, selected.selected, "probe captured second run", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				return attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
					runID := fixtures[1].authority.Target.AgentIdentity.RunID
					if _, err := mutationprotocol.RunRowLock(txctx, tx, runID, true); err != nil {
						return err
					}
					var id string
					return tx.QueryRowContext(txctx, `SELECT run_id::text FROM runs WHERE run_id=$1::uuid FOR UPDATE NOWAIT`, runID).Scan(&id)
				})
			})
			var locked *pq.Error
			if !errors.As(probeErr, &locked) || locked.Code != "55P03" {
				t.Fatalf("second run was not admitted before first session lock: %v", probeErr)
			}
			unblock()
			<-resetDone
			<-lockerDone
			if lockerErr != nil {
				t.Fatal(lockerErr)
			}
		} else {
			summary, resetErr = resetter.ResetAll(ctx, runtimesessions.ResetMetadata{Source: "full-footprint"})
		}
		if resetErr != nil || len(summary.OrphanedSessions) != 2 {
			t.Fatalf("exact reset summary: %+v %v", summary, resetErr)
		}
		seen := map[string]bool{}
		for _, d := range summary.OrphanedSessions {
			seen[d.SessionID] = true
		}
		for _, fixture := range fixtures {
			if !seen[fixture.sessionID] {
				t.Fatalf("captured session missing from reset: %s", fixture.sessionID)
			}
			var status, reason string
			if err := selected.db.QueryRowContext(ctx, `SELECT status, termination_reason FROM agent_sessions WHERE session_id=$1`, fixture.sessionID).Scan(&status, &reason); err != nil || status != "terminated" || reason != "orphaned" {
				t.Fatalf("reset physical session: %s/%s %v", status, reason, err)
			}
		}
	})
}
