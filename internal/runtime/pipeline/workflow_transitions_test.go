package pipeline_test

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
)

func TestRecordPipelineTransition_PersistsViaCanonicalCapabilityOwner(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("FROM information_schema.columns").WillReturnRows(canonicalTransitionSchemaRows())
	mock.ExpectQuery("FROM pg_index").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	eventID := uuid.NewString()
	pipelineID := uuid.NewString()
	mock.ExpectQuery(`SELECT EXISTS\(SELECT 1 FROM events WHERE event_id = \$1::uuid\)`).
		WithArgs(eventID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO event_receipts").
		WithArgs(eventID, "pipeline:"+pipelineID, "success", "pipeline_transition_applied", "", "", sqlmock.AnyArg(), nil, 0).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("FROM information_schema.columns").
		WillReturnRows(sqlmock.NewRows([]string{"table_name", "column_name"}))
	mock.ExpectCommit()

	pg := &store.PostgresStore{DB: db}
	err = runtimepipeline.RecordPipelineTransition(context.Background(), db, pg.CanonicalEventReceiptsCapability, runtimepipeline.PipelineTransitionInput{
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

func TestRecordPipelineTransition_FailsClosedOnMixedSchemaCapability(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("FROM information_schema.columns").WillReturnRows(mixedTransitionSchemaRows())
	mock.ExpectQuery("FROM pg_index").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	pg := &store.PostgresStore{DB: db}
	err = runtimepipeline.RecordPipelineTransition(context.Background(), db, pg.CanonicalEventReceiptsCapability, runtimepipeline.PipelineTransitionInput{
		EventID:    uuid.NewString(),
		EventType:  "review.requested",
		Handler:    "node-a",
		PipelineID: uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("RecordPipelineTransition: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func canonicalTransitionSchemaRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"table_name", "column_name"}).
		AddRow("events", "event_id").
		AddRow("events", "event_name").
		AddRow("events", "entity_id").
		AddRow("events", "flow_instance").
		AddRow("events", "scope").
		AddRow("events", "payload").
		AddRow("events", "chain_depth").
		AddRow("events", "produced_by").
		AddRow("events", "produced_by_type").
		AddRow("events", "source_event_id").
		AddRow("events", "created_at").
		AddRow("event_receipts", "receipt_id").
		AddRow("event_receipts", "event_id").
		AddRow("event_receipts", "subscriber_type").
		AddRow("event_receipts", "subscriber_id").
		AddRow("event_receipts", "entity_id").
		AddRow("event_receipts", "flow_instance").
		AddRow("event_receipts", "outcome").
		AddRow("event_receipts", "reason_code").
		AddRow("event_receipts", "state_before").
		AddRow("event_receipts", "state_after").
		AddRow("event_receipts", "side_effects").
		AddRow("event_receipts", "failure").
		AddRow("event_receipts", "duration_ms").
		AddRow("event_receipts", "idempotency_key").
		AddRow("event_receipts", "processed_at")
}

func mixedTransitionSchemaRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"table_name", "column_name"}).
		AddRow("events", "id").
		AddRow("events", "type").
		AddRow("events", "source_agent").
		AddRow("events", "task_id").
		AddRow("events", "entity_id").
		AddRow("events", "payload").
		AddRow("events", "created_at").
		AddRow("event_receipts", "receipt_id").
		AddRow("event_receipts", "event_id").
		AddRow("event_receipts", "subscriber_type").
		AddRow("event_receipts", "subscriber_id").
		AddRow("event_receipts", "entity_id").
		AddRow("event_receipts", "flow_instance").
		AddRow("event_receipts", "outcome").
		AddRow("event_receipts", "reason_code").
		AddRow("event_receipts", "state_before").
		AddRow("event_receipts", "state_after").
		AddRow("event_receipts", "side_effects").
		AddRow("event_receipts", "failure").
		AddRow("event_receipts", "duration_ms").
		AddRow("event_receipts", "idempotency_key").
		AddRow("event_receipts", "processed_at")
}
