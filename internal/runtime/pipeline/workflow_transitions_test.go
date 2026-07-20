package pipeline_test

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestRecordPipelineTransitionPersistsCurrentReceipt(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	eventID := uuid.NewString()
	pipelineID := uuid.NewString()
	mock.ExpectQuery(`SELECT EXISTS\(SELECT 1 FROM events WHERE event_id = \$1::uuid\)`).
		WithArgs(eventID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO event_receipts").
		WithArgs(eventID, "pipeline:"+pipelineID, "success", "pipeline_transition_applied", "", "", sqlmock.AnyArg(), nil, 0).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT DISTINCT run_id, family`).
		WillReturnRows(sqlmock.NewRows([]string{"run_id", "family"}))
	mock.ExpectCommit()

	err = runtimepipeline.RecordPipelineTransition(testAuthorActivityContext(t, context.Background()), db, runtimepipeline.PipelineTransitionInput{
		EventID:    eventID,
		EventType:  "review.requested",
		Handler:    "node-a",
		PipelineID: pipelineID,
	})
	if err != nil {
		t.Fatalf("RecordPipelineTransition: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}
