package transactiontest_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/lib/pq"
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

func newOrderingNativeFixture(t *testing.T, surface string, enabled bool, queryEntered chan struct{}) *orderingNativeFixture {
	t.Helper()
	var once sync.Once
	matcher := sqlmock.QueryMatcherFunc(func(expected, actual string) error {
		err := sqlmock.QueryMatcherRegexp.Match(expected, actual)
		if err == nil && queryEntered != nil && strings.HasPrefix(actual, "SELECT last_sequence FROM author_activity_order") {
			once.Do(func() { close(queryEntered) })
		}
		return err
	})
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
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

func TestOrderingNativeCommitAndDisabledSQLParity(t *testing.T) {
	for _, surface := range []string{"pooled_postgres", "retained_postgres", "sqlite"} {
		for _, enabled := range []bool{false, true} {
			t.Run(surface+"/"+map[bool]string{false: "disabled", true: "zero_delay"}[enabled], func(t *testing.T) {
				f := newOrderingNativeFixture(t, surface, enabled, nil)
				f.acquired()
				f.mock.ExpectCommit()
				var ended context.Context
				var native *sql.Tx
				committed, err := f.run(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
					transactiontest.Mark(ctx, transactiontest.WorkflowMutation)
					ended, native = ctx, tx
					for range 3 {
						if err := authoractivity.FenceMutationOrder(ctx, tx, f.dialect); err != nil {
							return err
						}
					}
					_, err := authoractivity.Begin(ctx, tx, f.dialect)
					return err
				})
				if err != nil || !committed {
					t.Fatalf("native commit=%t err=%v", committed, err)
				}
				if err := authoractivity.FenceMutationOrder(ended, native, f.dialect); !errors.Is(err, sql.ErrTxDone) {
					t.Fatalf("ended scope error=%v", err)
				}
				if enabled {
					snapshot := f.collector.Snapshot()
					counts := snapshot.Total
					o := counts.Ordering
					if o.AcquisitionAttempts != 1 || o.Acquired != 1 || o.Reuses != 3 ||
						o.CommittedHolders != 1 || o.RolledBackHolders != 0 || o.UnresolvedHolders != 0 ||
						o.AcquisitionDuration <= 0 || o.CommitHoldDuration <= 0 || counts.WriteCommits != 1 || counts.DelayedCommits != 0 {
						t.Fatalf("native commit receipt=%+v", counts)
					}
					if snapshot.ByOperation[transactiontest.WorkflowMutation].Ordering != o {
						t.Fatalf("operation lost native ordering receipt: %+v", snapshot)
					}
					if f.session != nil && snapshot.Retained.Ordering != o {
						t.Fatalf("retained aggregate lost native ordering receipt: %+v", snapshot)
					}
					if f.session == nil && snapshot.Retained.Ordering != (transactiontest.OrderingCounts{}) {
						t.Fatalf("pooled ordering receipt leaked into retained aggregate: %+v", snapshot)
					}
				}
			})
		}
	}
}

func TestOrderingNativeCancelledAcquisition(t *testing.T) {
	for _, surface := range []string{"pooled_postgres", "retained_postgres", "sqlite"} {
		t.Run(surface, func(t *testing.T) {
			entered := make(chan struct{})
			f := newOrderingNativeFixture(t, surface, true, entered)
			f.expectAcquisition().WillDelayFor(time.Minute).WillReturnRows(sqlmock.NewRows([]string{"last_sequence"}).AddRow(17))
			f.mock.ExpectRollback()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := f.run(context.Background(), func(txctx context.Context, tx *sql.Tx) error {
					wait, stop := context.WithCancel(txctx)
					defer stop()
					join := context.AfterFunc(ctx, stop)
					defer join()
					return authoractivity.FenceMutationOrder(wait, tx, f.dialect)
				})
				done <- err
			}()
			<-entered
			cancel()
			if err := <-done; !errors.Is(err, sqlmock.ErrCancelled) {
				t.Fatalf("canceled native acquisition=%v", err)
			}
			o := f.collector.Snapshot().Total.Ordering
			if o.AcquisitionAttempts != 1 || o.AcquisitionErrors != 1 || o.Acquired != 0 || o.Reuses != 0 ||
				o.AcquisitionDuration <= 0 || o.CommittedHolders != 0 || o.RolledBackHolders != 0 || o.UnresolvedHolders != 0 {
				t.Fatalf("canceled acquisition receipt=%+v", o)
			}
		})
	}
}

type orderingBusyError struct{}

func (orderingBusyError) Error() string { return "database is busy (SQLITE_BUSY)" }
func (orderingBusyError) Code() int     { return 5 }

func TestOrderingNativeRetryStartsFreshAcquisition(t *testing.T) {
	for _, surface := range []string{"pooled_postgres", "retained_postgres", "sqlite"} {
		t.Run(surface, func(t *testing.T) {
			f := newOrderingNativeFixture(t, surface, true, nil)
			var conflict error = &pq.Error{Code: "40001"}
			if f.sqlite != nil {
				conflict = orderingBusyError{}
			}
			f.expectAcquisition().WillReturnError(conflict)
			f.mock.ExpectRollback()
			f.acquired()
			f.mock.ExpectCommit()
			calls := 0
			write := func(context.Context, *mutationprotocol.Attempt) (int, error) {
				calls++
				return 23, nil
			}
			var result mutationprotocol.Result[int]
			if f.sqlite != nil {
				result = mutationprotocol.RunSQLite(context.Background(), f.sqlite, "ordering retry", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
			} else if f.session != nil {
				result = mutationprotocol.RunRetainedPostgres(context.Background(), f.session, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
			} else {
				result = mutationprotocol.RunPostgres(context.Background(), f.postgres, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
			}
			value, committed := result.Value()
			if result.Err() != nil || !committed || value != 23 || calls != 1 {
				t.Fatalf("retry value=%d commit=%t calls=%d err=%v", value, committed, calls, result.Err())
			}
			counts := f.collector.Snapshot().Total
			o := counts.Ordering
			if counts.BeginAttempts != 2 || o.AcquisitionAttempts != 2 || o.AcquisitionErrors != 1 ||
				o.Acquired != 1 || o.CommittedHolders != 1 || o.Reuses != 0 || o.UnresolvedHolders != 0 {
				t.Fatalf("retry receipt=%+v", counts)
			}
		})
	}
}

func TestOrderingNativeRollbackAndPanicOutcomes(t *testing.T) {
	for _, surface := range []string{"pooled_postgres", "retained_postgres", "sqlite"} {
		for _, panics := range []bool{false, true} {
			for _, fails := range []bool{false, true} {
				t.Run(surface+"/"+map[bool]string{false: "return", true: "panic"}[panics]+"/"+map[bool]string{false: "rollback", true: "rollback_error"}[fails], func(t *testing.T) {
					f := newOrderingNativeFixture(t, surface, true, nil)
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
	f := newOrderingNativeFixture(t, "retained_postgres", true, nil)
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
			f := newOrderingNativeFixture(t, surface, true, nil)
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
			f := newOrderingNativeFixture(t, surface, true, nil)
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
			f := newOrderingNativeFixture(t, surface, true, nil)
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
			f := newOrderingNativeFixture(t, surface, true, nil)
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
