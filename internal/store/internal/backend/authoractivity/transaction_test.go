package authoractivity

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func orderingScopeFixture(t *testing.T, dialect Dialect, head int64, domainFirst bool) (context.Context, *sql.Tx, func(), func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if domainFirst {
		mock.ExpectQuery("SELECT 'domain-before-ordering'").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("domain"))
	}
	if dialect == DialectSQLite {
		mock.ExpectExec("INSERT OR IGNORE INTO author_activity_order").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("UPDATE author_activity_order SET last_sequence = last_sequence").WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectQuery("SELECT last_sequence FROM author_activity_order").WillReturnRows(sqlmock.NewRows([]string{"last_sequence"}).AddRow(head))
	mock.ExpectRollback()
	caller, cancel := context.WithCancel(context.Background())
	ctx, release := BindTransaction(caller, context.WithoutCancel(caller), tx, dialect)
	t.Cleanup(func() {
		_ = tx.Rollback()
		release()
		cancel()
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		_ = db.Close()
	})
	return ctx, tx, cancel, release
}

func TestOrderingScopeReusesOnlyExactTransactionPossession(t *testing.T) {
	for _, dialect := range []Dialect{DialectPostgres, DialectSQLite} {
		t.Run(string(dialect), func(t *testing.T) {
			ctx, tx, _, _ := orderingScopeFixture(t, dialect, 17, true)
			var slot transactiontest.Slot
			collector, restore, err := slot.Install(transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			probe := slot.Begin(false, false)
			ctx = transactiontest.WithAttempt(ctx, probe)
			story, err := Begin(ctx, tx, dialect)
			if err != nil {
				t.Fatal(err)
			}
			var domain string
			if err := tx.QueryRowContext(ctx, "SELECT 'domain-before-ordering'").Scan(&domain); err != nil || domain != "domain" {
				t.Fatalf("domain work could not precede ordering: %q %v", domain, err)
			}
			if err := FenceMutationOrder(ctx, tx, dialect); err != nil {
				t.Fatal(err)
			}
			for range 20 {
				if err := FenceMutationOrder(ctx, tx, dialect); err != nil {
					t.Fatalf("held fence required another SQL acquisition: %v", err)
				}
			}
			if err := story.Finalize(ctx); err != nil || story.last != 17 {
				t.Fatalf("story lost locked sequence: %+v %v", story, err)
			}
			if _, err := Begin(ctx, tx, dialect); err == nil {
				t.Fatal("second story batch admitted")
			}
			other := DialectSQLite
			if dialect == other {
				other = DialectPostgres
			}
			if err := FenceMutationOrder(ctx, tx, other); err == nil {
				t.Fatal("different dialect borrowed possession")
			}
			if err := FenceMutationOrder(ctx, new(sql.Tx), dialect); err == nil {
				t.Fatal("foreign transaction borrowed possession")
			}
			probe.Finish(nil)
			counts := collector.Snapshot().Total.Ordering
			if counts.AcquisitionAttempts != 1 || counts.Acquired != 1 || counts.Reuses != 21 ||
				counts.AcquisitionErrors != 0 || counts.UnresolvedHolders != 1 {
				t.Fatalf("ordering possession receipt = %+v", counts)
			}
		})
	}
}

func TestOrderingScopeCancellationRetirementAndFreshAttempt(t *testing.T) {
	for _, dialect := range []Dialect{DialectPostgres, DialectSQLite} {
		t.Run(string(dialect), func(t *testing.T) {
			ctx, tx, cancel, release := orderingScopeFixture(t, dialect, 17, false)
			var slot transactiontest.Slot
			collector, restore, err := slot.Install(transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			first := slot.Begin(false, false)
			ctx = transactiontest.WithAttempt(ctx, first)
			if err := FenceMutationOrder(ctx, tx, dialect); err != nil {
				t.Fatal(err)
			}
			cancel()
			if err := FenceMutationOrder(ctx, tx, dialect); !errors.Is(err, context.Canceled) {
				t.Fatalf("drained SQL context hid cancellation on reuse: %v", err)
			}
			release()
			if err := FenceMutationOrder(ctx, tx, dialect); !errors.Is(err, sql.ErrTxDone) {
				t.Fatalf("ended native transaction retained possession: %v", err)
			}
			nextCtx, nextTx, _, _ := orderingScopeFixture(t, dialect, 23, false)
			next := slot.Begin(false, false)
			nextCtx = transactiontest.WithAttempt(nextCtx, next)
			if err := FenceMutationOrder(ctx, nextTx, dialect); !errors.Is(err, sql.ErrTxDone) {
				t.Fatalf("retired scope borrowed a successor transaction: %v", err)
			}
			if err := FenceMutationOrder(nextCtx, nextTx, dialect); err != nil {
				t.Fatal(err)
			}
			story, err := Begin(nextCtx, nextTx, dialect)
			if err != nil || story.last != 23 {
				t.Fatalf("retry reused predecessor sequence: %+v %v", story, err)
			}
			if err := story.Finalize(nextCtx); err != nil {
				t.Fatal(err)
			}
			first.Finish(context.Canceled)
			next.Finish(nil)
			counts := collector.Snapshot().Total.Ordering
			if counts.AcquisitionAttempts != 2 || counts.Acquired != 2 || counts.Reuses != 1 || counts.UnresolvedHolders != 2 {
				t.Fatalf("refused or ended scope changed receipt: %+v", counts)
			}
		})
	}
}

func TestOrderingScopeRefusesMissingAndBooleanAuthority(t *testing.T) {
	tx := new(sql.Tx)
	for _, ctx := range []context.Context{context.Background(), context.WithValue(context.Background(), orderingTransactionKey{}, true)} {
		if _, err := Begin(ctx, tx, DialectPostgres); err == nil {
			t.Fatal("non-native context reserved a story batch")
		}
		if err := FenceMutationOrder(ctx, tx, DialectPostgres); err == nil {
			t.Fatal("non-native context authorized ordering reuse")
		}
	}
}
