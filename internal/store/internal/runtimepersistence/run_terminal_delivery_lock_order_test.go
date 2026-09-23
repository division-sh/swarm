package runtimepersistence

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestPostgresMarkRunTerminalLocksRunBeforeDeliverySettlement(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
	defer cancel()

	fixture := seedNormalRunCompletionFixture(t, db, "active", "lock-order/instance", "lock-order")
	route := testAgentDeliveryRoute(t, fixture.RunID, "lock-order-agent", "fixture/lock-order-agent")
	event := commitPostgresDeliveryFixture(t, ctx, db, fixture.EventID, route)
	claimed := claimPostgresDeliveryFixture(t, ctx, db, event, route)
	settlementStore := postgresDeliveryFixtureStore(db)

	terminalDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open terminal store: %v", err)
	}
	defer terminalDB.Close()
	terminalStore := postgresDeliveryFixtureStore(terminalDB)

	runLocked := make(chan struct{})
	renew := make(chan struct{})
	renewalDone := make(chan error, 1)
	var signalRunLocked sync.Once
	go func() {
		result := mutationprotocol.RunPostgres(ctx, settlementStore.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, settlementStore.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			if err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
				return requirePostgresRunActive(txctx, tx, fixture.RunID)
			}); err != nil {
				return struct{}{}, err
			}
			signalRunLocked.Do(func() { close(runLocked) })
			select {
			case <-renew:
			case <-txctx.Done():
				return struct{}{}, context.Cause(txctx)
			}
			_, err := postgresDeliveryAdapter.RenewClaim(txctx, attempt, claimed.Claim, runtimedelivery.DefaultLeaseTTL)
			return struct{}{}, err
		})
		renewalDone <- result.Err()
	}()

	select {
	case <-runLocked:
	case <-ctx.Done():
		t.Fatalf("renewal did not lock run: %v", context.Cause(ctx))
	}

	type terminalResult struct {
		status string
		err    error
	}
	terminalDone := make(chan terminalResult, 1)
	terminalPIDReady := make(chan int, 1)
	go func() {
		var snapshot runtimerunlifecycle.Snapshot
		result := mutationprotocol.RunPostgres(ctx, terminalStore.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, terminalStore.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			var terminalPID int
			if err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
				return tx.QueryRowContext(txctx, `SELECT pg_backend_pid()`).Scan(&terminalPID)
			}); err != nil {
				return struct{}{}, err
			}
			terminalPIDReady <- terminalPID
			var err error
			snapshot, _, err = terminalStore.runLifecyclePostgresOwner.MarkTerminalTx(txctx, attempt, runtimerunlifecycle.TerminalRequest{
				RunID: fixture.RunID, State: runtimerunlifecycle.StateCancelled, EndedAt: time.Now().UTC(),
			})
			return struct{}{}, err
		})
		terminalDone <- terminalResult{status: string(snapshot.State), err: result.Err()}
	}()

	terminalPID := <-terminalPIDReady
	query := observePostgresTerminalRunLock(t, ctx, db, terminalPID)
	upperQuery := strings.ToUpper(query)
	if !strings.Contains(upperQuery, "FROM RUNS") || !strings.Contains(upperQuery, "FOR UPDATE") {
		t.Fatalf("terminalization blocked query = %q, want canonical run lifecycle lock", query)
	}
	close(renew)

	select {
	case err := <-renewalDone:
		if err != nil {
			t.Fatalf("renew while terminalization waits on run lock: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("renewal did not complete: %v", context.Cause(ctx))
	}
	select {
	case result := <-terminalDone:
		if result.err != nil {
			t.Fatalf("terminalize after settlement: %v", result.err)
		}
		if result.status != "cancelled" {
			t.Fatalf("terminal status = %q, want cancelled", result.status)
		}
	case <-ctx.Done():
		t.Fatalf("terminalization did not complete: %v", context.Cause(ctx))
	}

	snapshot := loadDeliverySnapshotFixture(t, ctx, settlementStore, event.ID(), route)
	if snapshot.State() != runtimedelivery.StateExhausted || snapshot.ReasonCode != "run_cancelled" {
		t.Fatalf("delivery = state:%q reason:%q, want exhausted/run_cancelled", snapshot.State(), snapshot.ReasonCode)
	}
}

func observePostgresTerminalRunLock(t testing.TB, ctx context.Context, db *sql.DB, pid int) string {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waitType, query string
		if err := db.QueryRowContext(ctx, `
			SELECT COALESCE(wait_event_type, ''), COALESCE(query, '')
			FROM pg_stat_activity
			WHERE pid = $1
		`, pid).Scan(&waitType, &query); err != nil {
			t.Fatalf("observe terminal connection: %v", err)
		}
		if waitType == "Lock" {
			return query
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("terminal connection did not wait on run lock: %v", context.Cause(ctx))
		}
	}
}
