package genericschedule

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
)

func idleClaimOwner(t *testing.T) (*PostgresOwner, sqlmock.Sqlmock, *sql.DB) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewPostgres(backend, func() error { return nil }, standaloneExecutionFixture{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = owner.ReleaseGenericScheduleClaims(context.Background())
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		_ = db.Close()
	})
	return owner, mock, db
}

func idleClaimWakeup(t *testing.T, id string) runtimegenericschedule.Wakeup {
	t.Helper()
	wakeup, err := runtimegenericschedule.NewWakeup(id, time.Unix(200, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	return wakeup
}

func expectIdleClaimAcquired(mock sqlmock.Sqlmock, wakeup runtimegenericschedule.Wakeup) {
	mock.ExpectQuery(`SELECT pg_try_advisory_lock`).WithArgs(claimKey(wakeup)).WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(true))
	mock.ExpectQuery(`SELECT EXISTS`).WithArgs(wakeup.ActivationID(), wakeup.DueAt()).WillReturnRows(sqlmock.NewRows([]string{"active"}).AddRow(true))
	mock.ExpectQuery(`SELECT CAST\(run_id AS TEXT\) FROM timers`).WithArgs(wakeup.ActivationID()).WillReturnRows(sqlmock.NewRows([]string{"run_id"}).AddRow(nil))
}

func TestGenericClaimUnusedConnectionClosesOnEveryUnheldExit(t *testing.T) {
	for _, scenario := range []string{"unacquired", "inactive", "claim_error", "active_read_error", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			owner, mock, db := idleClaimOwner(t)
			conn, err := owner.ensureClaimConn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			wakeup := idleClaimWakeup(t, "00000000-0000-4000-8000-000000000642")
			ctx := context.Background()
			var failure error
			switch scenario {
			case "cancelled":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx, failure = cancelled, context.Canceled
			case "claim_error":
				failure = errors.New("advisory claim failed")
				mock.ExpectQuery(`SELECT pg_try_advisory_lock`).WithArgs(claimKey(wakeup)).WillReturnError(failure)
			case "unacquired":
				mock.ExpectQuery(`SELECT pg_try_advisory_lock`).WithArgs(claimKey(wakeup)).WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(false))
			default:
				mock.ExpectQuery(`SELECT pg_try_advisory_lock`).WithArgs(claimKey(wakeup)).WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(true))
				query := mock.ExpectQuery(`SELECT EXISTS`).WithArgs(wakeup.ActivationID(), wakeup.DueAt())
				if scenario == "active_read_error" {
					failure = errors.New("active wakeup read failed")
					query.WillReturnError(failure)
				} else {
					query.WillReturnRows(sqlmock.NewRows([]string{"active"}).AddRow(false))
				}
				mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs(claimKey(wakeup)).WillReturnResult(sqlmock.NewResult(0, 1))
			}
			claimed, err := owner.ClaimGenericScheduleWakeup(ctx, wakeup)
			if claimed || (failure == nil && err != nil) || (failure != nil && !errors.Is(err, failure)) {
				t.Fatalf("unheld claim exit: claimed=%t err=%v want=%v", claimed, err, failure)
			}
			if owner.claims.conn != nil || len(owner.claims.keys) != 0 || db.Stats().InUse != 0 {
				t.Fatal("unheld claim retained a shared cleanup connection")
			}
			if err := conn.PingContext(context.Background()); !errors.Is(err, sql.ErrConnDone) {
				t.Fatalf("unused claim connection remains leased: %v", err)
			}
		})
	}
}

func TestGenericClaimExactReleasePreservesSiblingConnection(t *testing.T) {
	owner, mock, db := idleClaimOwner(t)
	a := idleClaimWakeup(t, "00000000-0000-4000-8000-000000000642")
	b := idleClaimWakeup(t, "00000000-0000-4000-8000-000000000643")
	for _, wakeup := range []runtimegenericschedule.Wakeup{a, b} {
		expectIdleClaimAcquired(mock, wakeup)
		if claimed, err := owner.ClaimGenericScheduleWakeup(context.Background(), wakeup); err != nil || !claimed {
			t.Fatalf("initial native claim failed: %t %v", claimed, err)
		}
	}
	conn := owner.claims.conn
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs(claimKey(a)).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := owner.ReleaseGenericScheduleWakeup(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if owner.claims.conn != conn || len(owner.claims.keys) != 1 || db.Stats().InUse != 1 {
		t.Fatal("exact release closed the sibling's claim session")
	}
	if _, held := owner.claims.keys[claimKey(b)]; !held {
		t.Fatal("sibling claim was released")
	}
	mock.ExpectQuery(`SELECT EXISTS`).WithArgs(b.ActivationID(), b.DueAt()).WillReturnRows(sqlmock.NewRows([]string{"active"}).AddRow(true))
	mock.ExpectQuery(`SELECT CAST\(run_id AS TEXT\) FROM timers`).WithArgs(b.ActivationID()).WillReturnRows(sqlmock.NewRows([]string{"run_id"}).AddRow(nil))
	if claimed, err := owner.ClaimGenericScheduleWakeup(context.Background(), b); err != nil || !claimed {
		t.Fatalf("sibling reentrance changed: %t %v", claimed, err)
	}
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs(claimKey(b)).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := owner.ReleaseGenericScheduleWakeup(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if owner.claims.conn != nil || len(owner.claims.keys) != 0 || db.Stats().InUse != 0 {
		t.Fatal("last exact release did not return the idle connection")
	}
}

func TestGenericClaimUnacquiredAndCancelledPreserveHeldSibling(t *testing.T) {
	owner, mock, db := idleClaimOwner(t)
	a := idleClaimWakeup(t, "00000000-0000-4000-8000-000000000642")
	b := idleClaimWakeup(t, "00000000-0000-4000-8000-000000000643")
	expectIdleClaimAcquired(mock, a)
	if claimed, err := owner.ClaimGenericScheduleWakeup(context.Background(), a); err != nil || !claimed {
		t.Fatalf("initial native claim failed: %t %v", claimed, err)
	}
	conn := owner.claims.conn
	mock.ExpectQuery(`SELECT pg_try_advisory_lock`).WithArgs(claimKey(b)).WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(false))
	if claimed, err := owner.ClaimGenericScheduleWakeup(context.Background(), b); err != nil || claimed {
		t.Fatalf("unacquired sibling claim = %t %v", claimed, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if claimed, err := owner.ClaimGenericScheduleWakeup(ctx, b); claimed || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled admission = %t %v", claimed, err)
	}
	if owner.claims.conn != conn || len(owner.claims.keys) != 1 || db.Stats().InUse != 1 {
		t.Fatal("unsuccessful admission released the held sibling")
	}
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs(claimKey(a)).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := owner.ReleaseGenericScheduleWakeup(context.Background(), a); err != nil {
		t.Fatal(err)
	}
}
