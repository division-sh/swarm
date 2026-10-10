package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// These are owner-level statement/admission controls, not native-store journey
// receipts. Unexpected data UPDATEs fail the original sqlmock expectation.
func TestQ10RequiredCompanionOnUnchangedData(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, caseName := range []string{"unchanged", "missing", "foreign_type", "foreign_identity", "foreign_path", "duplicate", "bad_fields", "zero_creation_clock", "zero_update_clock", "reversed_data_clocks", "changed", "integer_to_double", "absent_to_null"} {
			t.Run(q10Dialect(postgres)+"/"+caseName, func(t *testing.T) {
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
				run := "3c111111-1111-4111-8111-111111111111"
				entity := "3c222222-2222-4222-8222-222222222222"
				at := time.Date(2026, 10, 9, 1, 0, 0, 0, time.UTC)
				record := pipeline.WorkflowEngineStateRecord{
					Identity: flowidentity.RunScopedFlowInstance{RunID: run, Route: flowidentity.StoredRoute("work", "work", "work")},
					EntityID: entity, EntityType: "task", WorkflowName: "work", Mode: "static", Status: "active",
					CurrentState: "done", ExpectedState: "ready", ExpectedRevision: 1, StageDefined: true,
					Fields: []byte(`{"count":1}`), Gates: []byte(`{}`), Bookkeeping: []byte(`{}`), Accumulator: []byte(`{}`), Config: []byte(`{}`),
					CreatedAt: at, EnteredStageAt: at.Add(time.Second), UpdatedAt: at.Add(time.Second),
				}
				mock.ExpectQuery(`(?s)^UPDATE flow_instances SET`).WillReturnRows(sqlmock.NewRows([]string{"run_id", "entity_id"}).AddRow(run, entity))
				columns := []string{"entity_id", "instance_path", "entity_type", "flow_template", "current_state", "revision", "gates", "bookkeeping", "accumulator", "field_id", "field_path", "field_type", "fields"}
				fieldType := "task"
				fields := `{"count":1}`
				var fieldID, fieldPath, fieldTypeValue, fieldsValue any = entity, "work", fieldType, fields
				switch caseName {
				case "missing":
					fieldID, fieldPath, fieldTypeValue, fieldsValue = nil, nil, nil, nil
				case "foreign_type":
					fieldTypeValue = "other"
				case "foreign_identity":
					fieldID = "3c333333-3333-4333-8333-333333333333"
				case "foreign_path":
					fieldPath = "other/path"
				case "bad_fields":
					fieldsValue = `null`
				case "changed":
					record.Fields = []byte(`{"count":2}`)
				case "integer_to_double":
					record.Fields = []byte(`{"count":1.0}`)
				case "absent_to_null":
					fieldsValue = `{}`
					record.Fields = []byte(`{"count":null}`)
				}
				rows := sqlmock.NewRows(columns).AddRow(entity, "work", "task", "work", "done", int64(2), `{}`, `{}`, `{}`, fieldID, fieldPath, fieldTypeValue, fieldsValue)
				if caseName == "duplicate" {
					rows.AddRow(entity, "work", "task", "work", "done", int64(2), `{}`, `{}`, `{}`, "3c333333-3333-4333-8333-333333333333", "work", "task", fields)
				}
				mock.ExpectQuery(`(?s)^SELECT fi.entity_id, fi.instance_path`).WithArgs(run, entity, "work").WillReturnRows(rows)
				valid := caseName == "unchanged" || caseName == "changed" || caseName == "integer_to_double" || caseName == "absent_to_null"
				badClock := caseName == "zero_creation_clock" || caseName == "zero_update_clock" || caseName == "reversed_data_clocks"
				// Bad JSON is diagnosed after the exact PostgreSQL field lock.
				if postgres && (valid || badClock || caseName == "bad_fields") {
					mock.ExpectQuery(`(?s)^SELECT entity_id FROM entity_state .* FOR UPDATE$`).WithArgs(run, entity, "work", "task").WillReturnRows(sqlmock.NewRows([]string{"entity_id"}).AddRow(entity))
				}
				if valid || badClock {
					createdAt, updatedAt := at.Format(time.RFC3339Nano), at.Format(time.RFC3339Nano)
					switch caseName {
					case "zero_creation_clock":
						createdAt = time.Time{}.Format(time.RFC3339Nano)
					case "zero_update_clock":
						updatedAt = time.Time{}.Format(time.RFC3339Nano)
					case "reversed_data_clocks":
						updatedAt = at.Add(-time.Second).Format(time.RFC3339Nano)
					}
					mock.ExpectQuery(`(?s)^SELECT COALESCE\(slug,''\), COALESCE\(name,''\), created_at, updated_at`).WithArgs(run, entity, "work", "task").WillReturnRows(sqlmock.NewRows([]string{"slug", "name", "created_at", "updated_at"}).AddRow("", "", createdAt, updatedAt))
				}
				if valid {
					if caseName != "unchanged" {
						mock.ExpectExec(`(?s)^UPDATE entity_state SET slug = .* fields = .* updated_at = .*WHERE`).WithArgs("", "", string(record.Fields), record.UpdatedAt, run, entity, "work", "task").WillReturnResult(sqlmock.NewResult(0, 1))
					}
					mock.ExpectCommit()
				} else {
					mock.ExpectRollback()
				}
				_, mutationErr := commitWorkflowHeaderAndFields(context.Background(), tx, postgres, record, workflowEngineStateDecision{})
				if valid {
					if mutationErr != nil {
						t.Fatal(mutationErr)
					}
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
				} else {
					if mutationErr == nil {
						t.Fatal("unchanged data bypassed required-companion admission")
					}
					if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
						t.Fatal(err)
					}
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func q10Dialect(postgres bool) string {
	if postgres {
		return "postgres"
	}
	return "sqlite"
}
