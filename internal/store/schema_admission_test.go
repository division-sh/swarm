package store

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

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
