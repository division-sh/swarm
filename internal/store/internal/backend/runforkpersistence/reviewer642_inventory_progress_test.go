package runforkpersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/genericschedule"
)

func TestReviewer642InventoryRejectsTerminalBeforeOccurrence(t *testing.T) {
	for _, root := range []bool{false, true} {
		for _, status := range []genericschedule.Status{genericschedule.StatusCancelled, genericschedule.StatusFailed, genericschedule.StatusFired} {
			t.Run(map[bool]string{false: "flow/", true: "root/"}[root]+string(status), func(t *testing.T) {
				_, _, requests, rows := arrivalJoinInventoryFixture(t, root, false)
				row := &rows[0]
				base := row.AdmittedAt
				if row.CurrentDueAt.After(base) {
					base = row.CurrentDueAt
				}
				row.CurrentEventID = genericschedule.OccurrenceEventID(row.ID, row.CurrentDueAt)
				row.CurrentEventAdmittedAt = base.Add(2 * time.Minute)
				row.Status = status
				switch status {
				case genericschedule.StatusCancelled:
					row.CancelCause, row.CancelledAt = "join_stage_exit", base.Add(time.Minute)
				case genericschedule.StatusFailed:
					row.Failure, row.FailedAt = genericschedule.Failure{Code: "publication_failed"}, base.Add(time.Minute)
				case genericschedule.StatusFired:
					row.FiredAt, row.AcceptedAt = base.Add(time.Minute), base.Add(3*time.Minute)
				}
				if err := requireExactRunForkArrivalJoinInventory(requests, rows, runForkArrivalScheduleContinuing); err == nil {
					t.Fatal("complete continuing inventory accepted terminalization before occurrence admission")
				}
			})
		}
	}
}
