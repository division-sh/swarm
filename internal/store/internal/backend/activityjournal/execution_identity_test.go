package activityjournal

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func executionIdentityRecord(t *testing.T) runtimepipeline.ActivityAttemptRecord {
	t.Helper()
	failure, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "activity_provider_outcome_uncertain", "activity-runtime", "execute", nil))
	if !ok {
		t.Fatal("missing canonical failure")
	}
	return runtimepipeline.ActivityAttemptRecord{
		RequestEventID: uuid.NewString(), RunID: uuid.NewString(), ExecutionMode: "live",
		SourceEventID: uuid.NewString(), ParentEventID: uuid.NewString(), EntityID: uuid.NewString(),
		FlowInstance: "review", NodeID: "review/check", HandlerEventKey: "work.received",
		ActivityID: "check:1", Tool: "provider.check", EffectClass: "non_idempotent_write", Attempt: 1,
		Status: runtimepipeline.ActivityAttemptStatusUncertain, SuccessEvent: "check.done", FailureEvent: "check.failed",
		ResultEventID: uuid.NewString(), ResultEventType: "check.failed", ResultPayload: map[string]any{},
		Failure: &failure, InputHash: "exact-input", ReplyContextID: "reply-original",
		ExecutionBinding: processbinding.ActivityExecutionBinding{Version: processbinding.ActivityExecutionBindingVersion, GrantID: uuid.NewString(), StateVersion: 3},
	}
}

func executionIdentityRows(t *testing.T, record runtimepipeline.ActivityAttemptRecord) *sqlmock.Rows {
	t.Helper()
	binding, err := activityExecutionBindingValue(record.ExecutionBinding)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0).UTC()
	columns := strings.Fields(`request_event_id run_id source_event_id parent_event_id entity_id flow_instance
		node_id handler_event_key activity_id tool effect_class attempt status success_event failure_event
		result_event_id result_event_type result_payload failure input_hash loop_generation loop_stage reply_context_id execution_mode
		started_at completed_at updated_at execution_binding`)
	return sqlmock.NewRows(columns).AddRow(
		record.RequestEventID, record.RunID, record.SourceEventID, record.ParentEventID, record.EntityID, record.FlowInstance,
		record.NodeID, record.HandlerEventKey, record.ActivityID, record.Tool, record.EffectClass, record.Attempt, "started",
		record.SuccessEvent, record.FailureEvent, nil, nil, nil, nil, record.InputHash, "{}", record.LoopStage, record.ReplyContextID, string(record.ExecutionMode),
		now, nil, now, binding,
	)
}

func TestActivityTerminalRefusesChangedExecutionBeforeWrite(t *testing.T) {
	for _, dialect := range []Dialect{DialectSQLite, DialectPostgres} {
		for _, operation := range []string{"complete", "uncertain"} {
			for _, change := range []struct {
				name string
				edit func(*runtimepipeline.ActivityAttemptRecord)
			}{
				{"run", func(r *runtimepipeline.ActivityAttemptRecord) { r.RunID = uuid.NewString() }},
				{"source", func(r *runtimepipeline.ActivityAttemptRecord) { r.SourceEventID = uuid.NewString() }},
				{"parent", func(r *runtimepipeline.ActivityAttemptRecord) { r.ParentEventID = uuid.NewString() }},
				{"reply", func(r *runtimepipeline.ActivityAttemptRecord) { r.ReplyContextID = "reply-other" }},
				{"input", func(r *runtimepipeline.ActivityAttemptRecord) { r.InputHash = "changed-input" }},
				{"mode", func(r *runtimepipeline.ActivityAttemptRecord) { r.ExecutionMode = "mock" }},
				{"grant", func(r *runtimepipeline.ActivityAttemptRecord) { r.ExecutionBinding.GrantID = uuid.NewString() }},
				{"version", func(r *runtimepipeline.ActivityAttemptRecord) { r.ExecutionBinding.StateVersion++ }},
				{"missing", func(r *runtimepipeline.ActivityAttemptRecord) {
					r.ExecutionBinding = processbinding.ActivityExecutionBinding{}
				}},
			} {
				t.Run(dialectName(dialect)+"/"+operation+"/"+change.name, func(t *testing.T) {
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
					callback := original
					change.edit(&callback)
					mock.ExpectQuery("SELECT .* FROM activity_attempts WHERE request_event_id").
						WithArgs(original.RequestEventID).WillReturnRows(executionIdentityRows(t, original))
					// No mutation is expected: identity must be checked before SQL writes or story work.
					active := func(context.Context, string) error { return nil }
					var got runtimepipeline.ActivityAttemptRecord
					var changed bool
					if operation == "complete" {
						got, changed, err = Complete(context.Background(), tx, dialect, active, nil, callback)
					} else {
						got, changed, err = MarkUncertain(context.Background(), tx, dialect, active, nil, callback)
					}
					if err == nil || changed || got.RequestEventID != "" {
						t.Fatalf("changed callback accepted: record=%+v changed=%t err=%v", got, changed, err)
					}
					failure, ok := runtimefailures.EnvelopeFromError(err)
					if !ok || failure.Class != runtimefailures.ClassConflictingDuplicate {
						t.Fatalf("expected exact identity refusal, not a read/write error: %v", err)
					}
					mock.ExpectRollback()
					if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
						t.Fatal(err)
					}
					if err := mock.ExpectationsWereMet(); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func dialectName(dialect Dialect) string {
	if dialect == DialectPostgres {
		return "postgres"
	}
	return "sqlite"
}
