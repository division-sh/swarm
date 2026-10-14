package pipelinepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func TestWorkflowHeaderSubmittedJSONBothDialects(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, create := range []bool{false, true} {
			for _, fail := range []bool{false, true} {
				t.Run(fmt.Sprintf("postgres_%v/create_%v/fail_%v", postgres, create, fail), func(t *testing.T) {
					db, mock, err := sqlmock.New()
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					mock.ExpectBegin()
					tx, err := db.Begin()
					if err != nil {
						t.Fatal(err)
					}
					var slot transactiontest.Slot
					collector, restore, err := slot.Install(transactiontest.Options{})
					if err != nil {
						t.Fatal(err)
					}
					defer restore()
					a := slot.Begin(false, false)
					a.Begun()
					ctx := transactiontest.WithAttempt(context.Background(), a)
					// Whitespace is intentional: observe these submitted bytes,
					// not another JSON serialization or PostgreSQL's JSONB output.
					config := []byte(`{ "name" : "exact argument" }`)
					record := pipeline.WorkflowEngineStateRecord{Config: config}
					args := make([]driver.Value, 21)
					for index := range args {
						args[index] = sqlmock.AnyArg()
					}
					query, position := `(?s)^UPDATE flow_instances`, 2
					if create {
						query, position = `(?s)^INSERT INTO flow_instances`, 10
					}
					args[position] = string(config)
					expect := mock.ExpectQuery(query).WithArgs(args...)
					refusal := errors.New("native header write refused")
					if fail {
						expect.WillReturnError(refusal)
					} else {
						expect.WillReturnRows(sqlmock.NewRows([]string{"run", "entity"}).AddRow("run", "entity"))
					}
					_, writeErr := commitWorkflowInstanceHeader(ctx, tx, postgres, record, create)
					want := transactiontest.CopyCounts{Calls: 1, Copies: 1, SubmittedBytes: uint64(len(config))}
					if fail {
						if !errors.Is(writeErr, refusal) {
							t.Fatalf("write lost original refusal: %v", writeErr)
						}
						mock.ExpectRollback()
						a.RollbackAttempted()
						if err := tx.Rollback(); err != nil {
							t.Fatal(err)
						}
						want.FailedCalls, want.FailedBytes = 1, uint64(len(config))
					} else {
						if writeErr != nil {
							t.Fatal(writeErr)
						}
						mock.ExpectCommit()
						a.BeforeCommit()
						if err := tx.Commit(); err != nil {
							t.Fatal(err)
						}
						a.Committed()
						want.SucceededCalls, want.SucceededBytes, want.CommittedBytes = 1, uint64(len(config)), uint64(len(config))
					}
					a.Finish(writeErr)
					if got := collector.Snapshot().Total.JSONCopies.WorkflowHeader; got != want {
						t.Fatalf("actual header argument observation=%+v want %+v", got, want)
					}
					if err := mock.ExpectationsWereMet(); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}
