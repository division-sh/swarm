package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestPostgresMarkRunTerminalLocksRunBeforeDeliverySettlement(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
	defer cancel()

	fixture := seedNormalRunCompletionFixture(t, db, "active", "lock-order/instance", "lock-order")
	route := events.DeliveryRoute{
		SubscriberType: string(runtimedelivery.SubscriberAgent),
		SubscriberID:   "lock-order-agent",
	}
	event := commitPostgresDeliveryFixture(t, ctx, db, fixture.EventID, route)
	claimed := claimPostgresDeliveryFixture(t, ctx, db, event, route)
	settlementStore := postgresDeliveryFixtureStore(db)

	terminalDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open terminal store: %v", err)
	}
	defer terminalDB.Close()
	terminalStore := postgresDeliveryFixtureStore(terminalDB)
	terminalConn, err := terminalDB.Conn(ctx)
	if err != nil {
		t.Fatalf("borrow terminal connection: %v", err)
	}
	defer terminalConn.Close()
	var terminalPID int
	if err := terminalConn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&terminalPID); err != nil {
		t.Fatalf("load terminal connection pid: %v", err)
	}
	terminalCtx := runtimepipeline.WithPipelineSQLConnContext(ctx, terminalConn)
	terminalTx, err := terminalConn.BeginTx(terminalCtx, nil)
	if err != nil {
		t.Fatalf("begin terminal transaction: %v", err)
	}
	defer terminalTx.Rollback()
	terminalCtx = runtimepipeline.WithPipelineSQLTxContext(terminalCtx, terminalTx)
	terminalCtx, attached := eventCommitterForPipelineContext(terminalCtx, terminalStore)
	if !attached {
		t.Fatal("attach terminal event commit owner")
	}
	storyCtx, err := runtimeauthoractivity.Begin(terminalCtx, terminalTx, runtimeauthoractivity.DialectPostgres)
	if err != nil {
		t.Fatalf("begin terminal author activity mutation: %v", err)
	}

	runLocked := make(chan struct{})
	renew := make(chan struct{})
	renewalDone := make(chan error, 1)
	go func() {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			renewalDone <- err
			return
		}
		defer tx.Rollback()
		if err := storerunlifecycle.RequireActive(ctx, tx, fixture.RunID, storerunlifecycle.DialectPostgres); err != nil {
			renewalDone <- err
			return
		}
		close(runLocked)
		select {
		case <-renew:
		case <-ctx.Done():
			renewalDone <- context.Cause(ctx)
			return
		}
		if _, err := postgresDeliveryAdapter.RenewClaim(ctx, tx, claimed.Claim, runtimedelivery.DefaultLeaseTTL); err != nil {
			renewalDone <- err
			return
		}
		renewalDone <- tx.Commit()
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
	go func() {
		snapshot, err := terminalStore.markRunTerminalTx(storyCtx, terminalTx, fixture.RunID, "cancelled", nil, time.Now().UTC())
		if err == nil {
			err = runtimepipeline.CapturePipelineRunForkRevisionChanges(storyCtx, terminalTx)
		}
		if err == nil {
			err = runtimeauthoractivity.Finalize(storyCtx)
		}
		if err == nil {
			err = terminalTx.Commit()
		}
		terminalDone <- terminalResult{status: snapshot.Status, err: err}
	}()

	query := observePostgresTerminalRunLock(t, ctx, db, terminalPID)
	if !strings.Contains(strings.ToUpper(query), "UPDATE RUNS") {
		t.Fatalf("terminalization blocked query = %q, want run-row mutation", query)
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
