package runforkpersistence

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

func TestSelectedForkDuplicatePublicationRequiresOriginalEvidenceBothDialects(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, fault := range []string{"exact", "fresh_retry_time", "missing_lineage", "duplicate_lineage", "different_source", "different_authority", "missing_event", "changed_time", "changed_event", "missing_joined_lineage", "fan_out_read_failure"} {
			t.Run(map[bool]string{false: "sqlite", true: "postgres"}[postgres]+"/"+fault, func(t *testing.T) {
				f := newSelectedInputReadFixture(t)
				want := f.lineage
				lineages := 1
				readEvent := true
				eventFound := true
				switch fault {
				case "fresh_retry_time":
					want.CreatedAt = want.CreatedAt.Add(time.Hour)
				case "missing_lineage":
					lineages, readEvent = 0, false
				case "duplicate_lineage":
					lineages, readEvent = 2, false
				case "different_source":
					want.SourceRunID, readEvent = uuid.NewString(), false
				case "different_authority":
					want.SelectionAuthority, readEvent = "different-executor", false
				case "missing_event":
					eventFound = false
				case "changed_time":
					f.record.CreatedAt = f.record.CreatedAt.Add(time.Second)
				case "changed_event":
					f.record.EventName = "different.event"
				case "missing_joined_lineage":
					f.record.SelectedForkLineageOwners = 0
				}
				tx, mock := startSnapshotTransaction(t)
				expectSelectedInputLineage(mock, f, lineages)
				if readEvent {
					mock.ExpectQuery(`FROM events e`).WithArgs(f.lineage.ForkEventID).
						WillReturnRows(selectedInputRecordRows(f.record, !postgres, eventFound))
				}
				if fault == "exact" || fault == "fresh_retry_time" || fault == "fan_out_read_failure" {
					expectSelectedAttachmentBinding(mock, f.attachment.evidence.binding)
					query := mock.ExpectQuery(`SELECT EXISTS\(SELECT 1 FROM fan_out_intents WHERE run_id=`).WithArgs(want.ForkRunID)
					if fault == "fan_out_read_failure" {
						query.WillReturnError(context.DeadlineExceeded)
					} else {
						query.WillReturnRows(sqlmock.NewRows([]string{"has_fan_out"}).AddRow(false))
					}
				}
				err := requireSelectedForkExecutionEvidenceTx(context.Background(), tx, want, postgres)
				if (err == nil) != (fault == "exact" || fault == "fresh_retry_time") {
					t.Fatalf("duplicate evidence fault=%s err=%v", fault, err)
				}
			})
		}
	}
}
