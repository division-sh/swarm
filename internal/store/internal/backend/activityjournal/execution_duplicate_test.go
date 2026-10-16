package activityjournal

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestActivityDuplicateStartKeepsOriginalExecutionReference(t *testing.T) {
	for _, dialect := range []Dialect{DialectSQLite, DialectPostgres} {
		for _, cell := range []string{"exact", "grant", "version", "missing"} {
			t.Run(dialectName(dialect)+"/"+cell, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				mock.ExpectBegin()
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				original := executionIdentityRecord(t)
				candidate := original
				switch cell {
				case "grant":
					candidate.ExecutionBinding.GrantID = uuid.NewString()
				case "version":
					candidate.ExecutionBinding.StateVersion++
				case "missing":
					candidate.ExecutionBinding = processbinding.ActivityExecutionBinding{}
				}
				mock.ExpectExec("INSERT INTO activity_attempts .* ON CONFLICT ").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery("SELECT .* FROM activity_attempts WHERE request_event_id").WithArgs(original.RequestEventID).
					WillReturnRows(executionIdentityRows(t, original))
				story := &routeSettlementDraftRecorder{}
				got, inserted, err := Start(context.Background(), tx, dialect, func(context.Context, string) error { return nil }, story, candidate)
				if cell == "exact" {
					if err != nil || inserted || got.ExecutionBinding != original.ExecutionBinding {
						t.Fatalf("exact duplicate changed reference: %+v inserted=%t err=%v", got, inserted, err)
					}
				} else {
					failure, ok := runtimefailures.EnvelopeFromError(err)
					if !ok || failure.Class != runtimefailures.ClassConflictingDuplicate || inserted || got.RequestEventID != "" {
						t.Fatalf("duplicate replaced original attribution: %+v inserted=%t err=%v", got, inserted, err)
					}
				}
				if len(story.drafts) != 0 {
					t.Fatal("duplicate emitted story work")
				}
				mock.ExpectRollback()
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestActivityTerminalRequiresExactAttemptBeforeSQL(t *testing.T) {
	for _, dialect := range []Dialect{DialectSQLite, DialectPostgres} {
		for _, number := range []int{-1, 0, 2} {
			for _, operation := range []string{"complete", "uncertain"} {
				record := executionIdentityRecord(t)
				record.Attempt = number
				var got runtimepipeline.ActivityAttemptRecord
				var changed bool
				var err error
				if operation == "complete" {
					got, changed, err = Complete(context.Background(), nil, dialect, nil, nil, record)
				} else {
					got, changed, err = MarkUncertain(context.Background(), nil, dialect, nil, nil, record)
				}
				if err == nil || err.Error() != "activity settlement requires the original exact attempt" || changed || got.RequestEventID != "" {
					t.Fatalf("%s/%s attempt=%d reached SQL or normalization: record=%+v changed=%t err=%v", dialectName(dialect), operation, number, got, changed, err)
				}
			}
		}
	}
}
