package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

const unacceptedAdmissionEventID = "11111111-1111-4111-8111-111111111111"

func TestUnacceptedSelectedStoreRuntimeCallsFailBeforeSQL(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			var call func() error
			if backend == "postgres" {
				store := &PostgresStore{DB: db}
				call = func() error {
					_, err := store.ListActiveAgentDescriptors(context.Background())
					return err
				}
			} else {
				store := &SQLiteRuntimeStore{SQLiteSchemaStore: &SQLiteSchemaStore{DB: db}}
				call = func() error {
					_, err := store.ListActiveAgentDescriptors(context.Background())
					return err
				}
			}

			err = call()
			if err == nil || !strings.Contains(err.Error(), "schema is unaccepted") {
				t.Fatalf("runtime call error = %v, want unaccepted admission failure", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unaccepted runtime call issued SQL: %v", err)
			}
		})
	}
}

func TestUnacceptedSelectedStoreEventMutationBoundariesFailBeforeSQL(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			callbackCalled := false
			var calls []struct {
				name string
				call func() error
			}
			if backend == "postgres" {
				store := &PostgresStore{DB: db}
				calls = []struct {
					name string
					call func() error
				}{
					{name: "run_event_transaction", call: func() error {
						return store.runEventTransaction(context.Background(), func(context.Context, *sql.Tx) error {
							callbackCalled = true
							return nil
						})
					}},
					{name: "delivery_snapshot", call: func() error {
						_, err := store.Snapshot(context.Background(), unacceptedAdmissionEventID)
						return err
					}},
					{name: "delivery_summary", call: func() error {
						_, err := store.SummarizeRun(context.Background(), unacceptedAdmissionEventID)
						return err
					}},
				}
			} else {
				store := &SQLiteRuntimeStore{SQLiteSchemaStore: &SQLiteSchemaStore{DB: db}}
				calls = []struct {
					name string
					call func() error
				}{
					{name: "run_event_transaction", call: func() error {
						return store.runEventTransaction(context.Background(), func(context.Context, *sql.Tx) error {
							callbackCalled = true
							return nil
						})
					}},
					{name: "run_runtime_mutation", call: func() error {
						return store.RunRuntimeMutation(context.Background(), func(context.Context, *sql.Tx) error {
							callbackCalled = true
							return nil
						})
					}},
					{name: "delivery_snapshot", call: func() error {
						_, err := store.Snapshot(context.Background(), unacceptedAdmissionEventID)
						return err
					}},
					{name: "delivery_summary", call: func() error {
						_, err := store.SummarizeRun(context.Background(), unacceptedAdmissionEventID)
						return err
					}},
				}
			}

			for _, tc := range calls {
				t.Run(tc.name, func(t *testing.T) {
					callbackCalled = false
					err := tc.call()
					requireUnacceptedAdmissionFailure(t, err)
					if callbackCalled {
						t.Fatal("unaccepted mutation callback executed")
					}
				})
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unaccepted event mutation issued SQL: %v", err)
			}
		})
	}
}

func requireUnacceptedAdmissionFailure(t *testing.T, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "schema is unaccepted") {
		t.Fatalf("runtime call error = %v, want unaccepted admission failure", err)
	}
}
