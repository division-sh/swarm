package testpostgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestServerCapacityAdmission(t *testing.T) {
	for _, test := range []struct {
		name  string
		value any
		have  int
		bad   bool
	}{
		{name: "field100", value: "100", have: 100, bad: true},
		{name: "below299", value: "299", have: 299, bad: true},
		{name: "exact300", value: "300"},
		{name: "above301", value: "301"},
		{name: "large", value: "1000"},
		{name: "zero", value: "0", bad: true},
		{name: "negative", value: "-1", have: -1, bad: true},
		{name: "null", value: nil, bad: true},
		{name: "malformed", value: "not-a-number", bad: true},
		{name: "overflow", value: "99999999999999999999999999999", bad: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			rows := sqlmock.NewRows([]string{"max_connections"})
			mock.ExpectQuery("^SHOW max_connections$").WillReturnRows(rows.AddRow(test.value))
			err = ValidateServerCapacity(context.Background(), db)
			if (err != nil) != test.bad {
				t.Fatalf("capacity %v: %v", test.value, err)
			}
			if test.value != nil && test.name != "malformed" && test.name != "overflow" && test.bad {
				var capacity *CapacityError
				if !errors.As(err, &capacity) || capacity.Have != test.have || capacity.Need != RequiredMaxConnections {
					t.Fatalf("typed capacity error = %#v, %v", capacity, err)
				}
				if !strings.Contains(err.Error(), "ALTER SYSTEM SET max_connections = 300") || !strings.Contains(err.Error(), "restart") {
					t.Fatalf("missing manual remediation: %v", err)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("query_failure_preserves_cause_not_credentials", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		cause := errors.New("driver failure with capacity-secret-sentinel")
		mock.ExpectQuery("^SHOW max_connections$").WillReturnError(cause)
		err = ValidateServerCapacity(context.Background(), db)
		if !errors.Is(err, cause) || strings.Contains(err.Error(), "capacity-secret-sentinel") {
			t.Fatalf("unsafe or dropped observation cause: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("empty_result", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("^SHOW max_connections$").WillReturnRows(sqlmock.NewRows([]string{"max_connections"}))
		if err := ValidateServerCapacity(context.Background(), db); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("missing capacity observation accepted: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

type capacityDeadlineQuery struct {
	db       *sql.DB
	deadline time.Time
}

func (q *capacityDeadlineQuery) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	q.deadline, _ = ctx.Deadline()
	return q.db.QueryRowContext(ctx, query, args...)
}

func TestServerCapacityAdmissionContext(t *testing.T) {
	for _, test := range []struct {
		name   string
		budget time.Duration
	}{
		{name: "bounded_observation", budget: 10 * time.Second},
		{name: "earlier_parent_deadline", budget: 300 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx, cancel := context.WithTimeout(context.Background(), test.budget)
			defer cancel()
			parentDeadline, _ := ctx.Deadline()
			mock.ExpectQuery("^SHOW max_connections$").WillReturnRows(sqlmock.NewRows([]string{"max_connections"}).AddRow("300"))
			q := &capacityDeadlineQuery{db: db}
			before := time.Now()
			if err := ValidateServerCapacity(ctx, q); err != nil {
				t.Fatal(err)
			}
			if q.deadline.After(parentDeadline) || q.deadline.After(time.Now().Add(capacityObservationTimeout)) || q.deadline.Before(before) {
				t.Fatalf("observation deadline %s exceeds parent/budget", q.deadline)
			}
			if test.budget > capacityObservationTimeout && q.deadline.Before(before.Add(capacityObservationTimeout)) {
				t.Fatalf("unexpectedly shortened observation deadline: %s", q.deadline)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("cancelled", func(t *testing.T) {
		db, _, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := ValidateServerCapacity(ctx, db); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled admission: %v", err)
		}
	})
	t.Run("deadline_during_query", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		mock.ExpectQuery("^SHOW max_connections$").WillDelayFor(time.Second).WillReturnRows(sqlmock.NewRows([]string{"max_connections"}).AddRow("300"))
		if err := ValidateServerCapacity(ctx, db); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline admission: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}
