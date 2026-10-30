package transactiontest_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

type orderingNativeFixture struct {
	db          *sql.DB
	mock        sqlmock.Sqlmock
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
	f := &orderingNativeFixture{db: db, mock: mock}
	var install func(transactiontest.Options) (*transactiontest.Collector, func(), error)
	if surface == "sqlite" {
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
