package runtimepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
)

type forkContentionWait struct {
	pid         int
	transaction string
	query       string
	blockers    string
	fenced      bool
}

// Identify the one real native waiter in this otherwise idle fixture database,
// not a pool wait or an arbitrary connection blocked somewhere by the winner.
func forkContentionPostgresWait(t *testing.T, ctx context.Context, db *sql.DB, winnerPID, observerPID int) forkContentionWait {
	t.Helper()
	var wait forkContentionWait
	var count int
	err := db.QueryRowContext(ctx, `SELECT pid,
		COALESCE(backend_xid::text, '') || '/' || xact_start::text,
		query, pg_blocking_pids(pid)::text, $2=ANY(pg_blocking_pids(pid)),
		count(*) OVER ()
		FROM pg_stat_activity
		WHERE datname=current_database() AND pid<>$2
		  AND $1=ANY(pg_blocking_pids(pid))`, winnerPID, observerPID).
		Scan(&wait.pid, &wait.transaction, &wait.query, &wait.blockers, &wait.fenced, &count)
	if err != nil || count != 1 || wait.transaction == "" {
		t.Fatalf("identify exact native contender: count=%d wait=%+v err=%v", count, wait, err)
	}
	return wait
}

func TestForkContentionObserverFencesNativeWriter(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		if backend.name != "postgres" {
			continue
		}
		t.Run(backend.name, func(t *testing.T) {
			f := newForkActivationFrontierFixture(t, backend, true)
			staged, request := stageForkContentionFixture(t, f, true)
			ctx, cancel := context.WithTimeout(f.ctx, 25*time.Second)
			defer cancel()
			barrier := newForkContentionBarrier(t, backend.name, true)
			barrier.install(t, f.db, f.runID, staged.ForkRunID, "activation")
			before := snapshotForkHistoricalExecutionTables(t, f.db, true)
			winner, loser := make(chan forkContentionResult, 1), make(chan forkContentionResult, 1)
			winnerJoined, loserJoined := make(chan struct{}), make(chan struct{})
			loserStarted := false
			t.Cleanup(func() {
				cancel()
				barrier.release()
				barrier.resume()
				for _, joined := range []<-chan struct{}{winnerJoined, loserJoined} {
					if joined == loserJoined && !loserStarted {
						continue
					}
					select {
					case <-joined:
					case <-time.After(5 * time.Second):
						t.Error("native observer-control worker did not join")
					}
				}
			})
			go func() {
				defer close(winnerJoined)
				result, err := f.store.(runforkexecution.SelectedContractForkLifecycle).
					ActivateRunForkForSelectedContractExecution(ctx, request)
				winner <- forkContentionResult{activation: result, err: err}
			}()
			winnerPID := barrier.awaitWinner(t, ctx, f.db, winner)
			gate := barrier.queueGate(t, ctx, f.db, f.runID, "activation", winnerPID)
			loserStarted = true
			go func() {
				defer close(loserJoined)
				loser <- forkContentionResult{err: f.write(ctx, f.store)}
			}()
			barrier.awaitContender(t, ctx, f.db, winnerPID, gate, loser)
			wait := forkContentionPostgresWait(t, ctx, f.db, winnerPID, gate.pid)
			t.Logf("before settlement winner_pid=%d observer_pid=%d native_loser=%+v", winnerPID, gate.pid, wait)
			if !strings.Contains(wait.query, "author_activity_order") || !wait.fenced {
				t.Errorf("observer does not fence identified native contender before winner release: %+v", wait)
			}
			barrier.release()
			won := awaitForkContentionResult(t, ctx, winner)
			if won.err == nil || !strings.Contains(won.err.Error(), "h18_requested_rollback") || won.activation.Activated || won.activation.SourceFrozen {
				t.Fatalf("native winner did not preserve rollback discriminator: %+v", won)
			}
			barrier.awaitGate(t, ctx, gate)
			after := snapshotForkHistoricalExecutionTables(t, f.db, true)
			t.Logf("winner result=%+v loser_result_available_before_open=%t", won, len(loser) != 0)
			for _, diagnostic := range forkContentionFailureDiagnostics(before, after, won, loser) {
				t.Log(diagnostic)
			}
			if !reflect.DeepEqual(before, after) {
				t.Error("observer admitted application changes before its gate opened")
			}
			barrier.openGate(t, gate)
			barrier.resume()
			lost := awaitForkContentionResult(t, ctx, loser)
			<-loserJoined
			t.Logf("identified native_loser_pid=%d transaction=%s returned=%+v", wait.pid, wait.transaction, lost)
			if lost.err != nil {
				t.Fatalf("lawful losing writer refused after rollback: %v", lost.err)
			}
			requireForkContentionState(t, f.db, f, staged.ForkRunID, true, false)
		})
	}
}
