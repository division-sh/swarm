package transactiontest_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"

	"github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

type orderingNativeFixture struct {
	db          *sql.DB
	mock        sqlmock.Sqlmock
	dialect     authoractivity.Dialect
	collector   *transactiontest.Collector
	postgres    *postgres.Backend
	session     *postgres.SessionAuthority
	sqlite      *sqlite.Backend
	run         func(context.Context, func(context.Context, *sql.Tx) error) (bool, error)
	expectClose bool
}

func newOrderingNativeFixture(t *testing.T, surface string, enabled bool) *orderingNativeFixture {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	f := &orderingNativeFixture{db: db, mock: mock, dialect: authoractivity.DialectPostgres}
	var install func(transactiontest.Options) (*transactiontest.Collector, func(), error)
	if surface == "sqlite" {
		f.dialect = authoractivity.DialectSQLite
		f.sqlite, err = sqlite.New(db)
		if err != nil {
			t.Fatal(err)
		}
		f.run = func(ctx context.Context, fn func(context.Context, *sql.Tx) error) (bool, error) {
			return f.sqlite.RunTransactionOutcome(ctx, "ordering receipt", fn)
		}
		install = f.sqlite.InstallTransactionProbeForTest
	} else {
		f.postgres, err = postgres.New(db)
		if err != nil {
			t.Fatal(err)
		}
		install = f.postgres.InstallTransactionProbeForTest
		f.run = f.postgres.RunTransactionOutcome
		if surface == "retained_postgres" {
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			f.session, err = f.postgres.NewSessionAuthority(conn)
			if err != nil {
				t.Fatal(err)
			}
			f.run = func(ctx context.Context, fn func(context.Context, *sql.Tx) error) (bool, error) {
				return postgres.RunAuthorityTransactionOutcome(ctx, f.session, fn)
			}
		}
	}
	if enabled {
		var restore func()
		f.collector, restore, err = install(transactiontest.Options{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(restore)
	}
	t.Cleanup(func() {
		if !f.expectClose {
			mock.ExpectClose()
		}
		if f.session != nil {
			if err := f.session.Release(); err != nil {
				t.Error(err)
			}
		}
		if err := db.Close(); err != nil {
			t.Error(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	return f
}

func (f *orderingNativeFixture) expectAcquisition() *sqlmock.ExpectedQuery {
	f.mock.ExpectBegin()
	if f.dialect == authoractivity.DialectSQLite {
		f.mock.ExpectExec("INSERT OR IGNORE INTO author_activity_order").WillReturnResult(sqlmock.NewResult(0, 0))
		f.mock.ExpectExec("UPDATE author_activity_order SET last_sequence = last_sequence").WillReturnResult(sqlmock.NewResult(0, 1))
	}
	return f.mock.ExpectQuery("SELECT last_sequence FROM author_activity_order")
}

func (f *orderingNativeFixture) acquired() {
	f.expectAcquisition().WillReturnRows(sqlmock.NewRows([]string{"last_sequence"}).AddRow(17))
}

func TestOrderingNativeRollbackAndPanicOutcomes(t *testing.T) {
	for _, surface := range []string{"pooled_postgres", "retained_postgres", "sqlite"} {
		for _, panics := range []bool{false, true} {
			for _, fails := range []bool{false, true} {
				t.Run(surface+"/"+map[bool]string{false: "return", true: "panic"}[panics]+"/"+map[bool]string{false: "rollback", true: "rollback_error"}[fails], func(t *testing.T) {
					f := newOrderingNativeFixture(t, surface, true)
					f.acquired()
					primary := errors.New("operation evidence")
					cleanup := errors.New("rollback evidence")
					rollback := f.mock.ExpectRollback()
					if fails {
						rollback.WillReturnError(cleanup)
						f.mock.ExpectClose()
						f.expectClose = true
					}
					var recovered any
					var operationErr error
					func() {
						defer func() { recovered = recover() }()
						_, operationErr = f.run(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
							if err := authoractivity.FenceMutationOrder(ctx, tx, f.dialect); err != nil {
								return err
							}
							if panics {
								panic(primary)
							}
							return primary
						})
					}()
					if panics && recovered != primary {
						t.Fatalf("panic evidence=%v", recovered)
					}
					if !panics && (!errors.Is(operationErr, primary) || (fails && !errors.Is(operationErr, cleanup))) {
						t.Fatalf("operation/cleanup evidence=%v", operationErr)
					}
					o := f.collector.Snapshot().Total.Ordering
					if o.Acquired != 1 || o.CommittedHolders != 0 || (fails && o.UnresolvedHolders != 1) ||
						(!fails && (o.RolledBackHolders != 1 || o.RollbackHoldDuration <= 0 || o.UnresolvedHolders != 0)) {
						t.Fatalf("rollback receipt=%+v", o)
					}
				})
			}
		}
	}
}

func TestOrderingRetainedCommitExcludesLateFailedCleanup(t *testing.T) {
	f := newOrderingNativeFixture(t, "retained_postgres", true)
	f.acquired()
	f.mock.ExpectCommit()
	f.mock.ExpectClose()
	f.expectClose = true
	cleanup := errors.New("late session cleanup failure")
	var beforeAcquire, cleanupEntered time.Time
	f.session.SetEndTxErrorForTest(func() error {
		cleanupEntered = time.Now()
		// Make the late cleanup interval larger than the entire preceding work,
		// so an incorrect cleanup cut is distinguishable without a speed ceiling.
		time.Sleep(time.Since(beforeAcquire) + time.Millisecond)
		return cleanup
	})
	committed, err := f.run(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		beforeAcquire = time.Now()
		return authoractivity.FenceMutationOrder(ctx, tx, f.dialect)
	})
	if !committed || !errors.Is(err, cleanup) {
		t.Fatalf("acknowledged commit=%t cleanup=%v", committed, err)
	}
	counts := f.collector.Snapshot().Retained
	if counts.Ordering.CommittedHolders != 1 || counts.Ordering.CommitHoldDuration > cleanupEntered.Sub(beforeAcquire) ||
		counts.CleanupFailures != 1 || counts.WriteCommits != 1 || counts.CleanupDuration < time.Millisecond {
		t.Fatalf("late cleanup changed native commit receipt=%+v", counts)
	}
}

func TestOrderingNativeUnacknowledgedCommitStaysUnresolved(t *testing.T) {
	for _, surface := range []string{"pooled_postgres", "retained_postgres", "sqlite"} {
		t.Run(surface, func(t *testing.T) {
			f := newOrderingNativeFixture(t, surface, true)
			f.acquired()
			uncertain := errors.New("unacknowledged COMMIT")
			f.mock.ExpectCommit().WillReturnError(uncertain)
			f.mock.ExpectClose()
			f.expectClose = true
			committed, err := f.run(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
				return authoractivity.FenceMutationOrder(ctx, tx, f.dialect)
			})
			if committed || !errors.Is(err, uncertain) {
				t.Fatalf("uncertain native commit=%t err=%v", committed, err)
			}
			counts := f.collector.Snapshot().Total
			o := counts.Ordering
			if o.Acquired != 1 || o.UnresolvedHolders != 1 || o.CommittedHolders != 0 || o.RolledBackHolders != 0 ||
				o.CommitHoldDuration != 0 || o.RollbackHoldDuration != 0 || counts.WriteCommits != 0 || counts.CommitFailures != 1 {
				t.Fatalf("unacknowledged settlement fabricated a cut: %+v", counts)
			}
		})
	}
}

func TestOrderingNativeReadsAndPreBeginRefusalHaveNoHolder(t *testing.T) {
	for _, surface := range []string{"pooled_postgres", "retained_postgres", "sqlite"} {
		t.Run(surface+"/read_only", func(t *testing.T) {
			f := newOrderingNativeFixture(t, surface, true)
			f.mock.ExpectBegin()
			f.mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"one"}).AddRow(1))
			f.mock.ExpectCommit()
			read := func(ctx context.Context, tx *sql.Tx) error {
				var one int
				return tx.QueryRowContext(ctx, "SELECT 1").Scan(&one)
			}
			var err error
			if f.sqlite != nil {
				err = f.sqlite.RunReadTransaction(context.Background(), read)
			} else if f.session != nil {
				err = postgres.RunAuthorityReadTransaction(context.Background(), f.session, read)
			} else {
				err = f.postgres.RunReadTransaction(context.Background(), read)
			}
			if err != nil {
				t.Fatal(err)
			}
			counts := f.collector.Snapshot().Total
			if counts.ReadCommits != 1 || counts.Ordering != (transactiontest.OrderingCounts{}) {
				t.Fatalf("read-only work invented ordering possession: %+v", counts)
			}
		})
		t.Run(surface+"/pre_begin", func(t *testing.T) {
			f := newOrderingNativeFixture(t, surface, true)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			called := false
			committed, err := f.run(ctx, func(context.Context, *sql.Tx) error {
				called = true
				return nil
			})
			if committed || called || !errors.Is(err, context.Canceled) {
				t.Fatalf("pre-Begin refusal commit=%t callback=%t error=%v", committed, called, err)
			}
			counts := f.collector.Snapshot().Total
			if counts.Begun != 0 || counts.CommitAttempts != 0 || counts.Ordering != (transactiontest.OrderingCounts{}) {
				t.Fatalf("pre-Begin refusal invented a native holder: %+v", counts)
			}
		})
	}
}

func TestOrderingFailedPoolAcquisitionHasNoHolder(t *testing.T) {
	for _, surface := range []string{"pooled_postgres", "sqlite"} {
		t.Run(surface, func(t *testing.T) {
			f := newOrderingNativeFixture(t, surface, true)
			f.mock.ExpectClose()
			f.expectClose = true
			if err := f.db.Close(); err != nil {
				t.Fatal(err)
			}
			called := false
			committed, err := f.run(context.Background(), func(context.Context, *sql.Tx) error {
				called = true
				return nil
			})
			if committed || called || err == nil {
				t.Fatalf("closed-pool refusal commit=%t callback=%t error=%v", committed, called, err)
			}
			counts := f.collector.Snapshot().Total
			if counts.Begun != 0 || counts.CommitAttempts != 0 || counts.Ordering != (transactiontest.OrderingCounts{}) {
				t.Fatalf("failed pool acquisition invented a native holder: %+v", counts)
			}
		})
	}
}
