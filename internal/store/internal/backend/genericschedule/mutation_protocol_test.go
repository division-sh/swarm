package genericschedule

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

func TestCancelGenericScheduleMutationProtocolNoopAndRollback(t *testing.T) {
	for _, dialect := range []string{"postgres", "sqlite"} {
		for _, testCase := range []struct {
			name string
			err  error
		}{
			{name: "missing activation"},
			{name: "read failure", err: errors.New("schedule read failed")},
		} {
			t.Run(dialect+"/"+testCase.name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				mock.ExpectBegin()
				query := mock.ExpectQuery("FROM timers WHERE timer_id")
				if testCase.err != nil {
					query.WillReturnError(testCase.err)
					mock.ExpectRollback()
				} else {
					query.WillReturnRows(sqlmock.NewRows([]string{"timer_id"}))
					mock.ExpectCommit()
				}
				command := runtimegenericschedule.CancelCommand{
					ActivationID: "00000000-0000-4000-8000-000000002446",
					Cause:        "operator_cancelled",
					CancelledAt:  time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC),
				}
				var result runtimegenericschedule.CancelCommit
				var callErr error
				switch dialect {
				case "postgres":
					backend, err := postgresbackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					owner, err := NewPostgres(backend, func() error { return nil })
					if err != nil {
						t.Fatal(err)
					}
					result, callErr = owner.CancelGenericScheduleOutcome(context.Background(), command)
				case "sqlite":
					backend, err := sqlitebackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					owner, err := NewSQLite(backend, func() error { return nil })
					if err != nil {
						t.Fatal(err)
					}
					result, callErr = owner.CancelGenericScheduleOutcome(context.Background(), command)
				}
				if testCase.err != nil {
					if !errors.Is(callErr, testCase.err) || result.Acknowledged || result.Result.Outcome != "" || result.Result.Activation.ID != "" {
						t.Fatalf("rolled-back result = %+v, err = %v", result, callErr)
					}
				} else if callErr != nil || !result.Acknowledged || result.Result.Outcome != runtimegenericschedule.CancelMissing {
					t.Fatalf("no-op result = %+v, err = %v", result, callErr)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
