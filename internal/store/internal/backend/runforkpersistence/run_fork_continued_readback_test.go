package runforkpersistence

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestRunForkContinuedReadbackDistinguishesUnpublishedFromMissingEvidence(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, fault := range []string{"unpublished", "orphan_outcome", "orphan_event", "selected_without_outcome", "generic_without_outcome", "competing_owners", "foreign_source", "read_failure"} {
			t.Run(map[bool]string{false: "sqlite", true: "postgres"}[postgres]+"/"+fault, func(t *testing.T) {
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
				child, source := uuid.NewString(), uuid.NewString()
				replay := runfork.RunForkFanOutPendingReplay{Ordinal: 0, SourceEventID: uuid.NewString(), SourceDeliveryIDs: []string{uuid.NewString()}}
				eventID := activityidentity.ForkLineageEventID(child, replay.SourceEventID)
				owners := sqlmock.NewRows([]string{"event", "source", "owner"})
				switch fault {
				case "selected_without_outcome":
					owners.AddRow(eventID, source, "selected")
				case "generic_without_outcome":
					owners.AddRow(eventID, source, "generic")
				case "competing_owners":
					owners.AddRow(eventID, source, "selected").AddRow(eventID, source, "generic")
				case "foreign_source":
					owners.AddRow(eventID, uuid.NewString(), "selected")
				}
				query := mock.ExpectQuery(`FROM run_fork_selected_contract_executions`).WithArgs(child, replay.SourceEventID)
				if fault == "read_failure" {
					query.WillReturnError(context.DeadlineExceeded)
				} else {
					query.WillReturnRows(owners)
				}
				if fault == "unpublished" || fault == "orphan_outcome" || fault == "orphan_event" {
					mock.ExpectQuery(`SELECT EXISTS\(SELECT 1 FROM events`).WithArgs(child, eventID, deterministicRunForkReplayEventID(child, replay.SourceEventID)).
						WillReturnRows(sqlmock.NewRows([]string{"orphan"}).AddRow(fault == "orphan_event"))
				}
				err = requireRunForkContinuedPublicationReadback(context.Background(), tx, postgres, child,
					runfork.RunForkPlan{SourceRunID: source}, replay, runForkContinuedOutcomeRow{}, fault == "orphan_outcome")
				if (err == nil) != (fault == "unpublished") {
					t.Fatalf("readback success=%t fault=%s err=%v", err == nil, fault, err)
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
