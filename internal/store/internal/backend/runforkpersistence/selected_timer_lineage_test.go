package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestSelectedTimerLineageRejectsUnprovenPublicationBothDialects(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, fault := range []string{"invalid_task", "wrong_event", "missing_activation", "foreign_activation", "failed_activation_read"} {
			t.Run(map[bool]string{false: "sqlite", true: "postgres"}[postgres]+"/"+fault, func(t *testing.T) {
				activation := workflowTimerProjectionSource(t, false)
				occurrence := activation.Occurrence()
				eventID, taskID := timeridentity.WorkflowTimerOccurrenceEventID(occurrence), occurrence.TaskID()
				if fault == "invalid_task" {
					taskID = "untyped-timer"
				} else if fault == "wrong_event" {
					eventID = uuid.NewString()
				}
				tx, mock := startSnapshotTransaction(t)
				mock.ExpectQuery(`SELECT event_id, task_id FROM events`).WithArgs(activation.RunID).
					WillReturnRows(sqlmock.NewRows([]string{"event", "task"}).AddRow(eventID, taskID))
				loaded := false
				ids, err := selectedContractWorkflowTimerLineage(context.Background(), tx, activation.RunID, postgres,
					func(_ context.Context, actual *sql.Tx, id string) (pipeline.WorkflowTimerActivation, bool, error) {
						loaded = true
						if actual != tx || id != activation.Ref.ActivationID {
							t.Fatal("lineage read changed native transaction or activation identity")
						}
						if fault == "failed_activation_read" {
							return pipeline.WorkflowTimerActivation{}, false, errors.New("native timer read failed")
						}
						if fault == "foreign_activation" {
							activation.RunID = uuid.NewString()
						}
						return activation, fault != "missing_activation", nil
					})
				if err == nil || len(ids) != 0 || loaded != (fault != "invalid_task" && fault != "wrong_event") {
					t.Fatalf("unproven timer became a settlement root: ids=%v loaded=%t err=%v", ids, loaded, err)
				}
			})
		}
	}
}
