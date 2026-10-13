package runforkpersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/genericschedule"
)

func TestForkArrivalInventoryTerminalOccurrenceChronology(t *testing.T) {
	for _, root := range []bool{false, true} {
		for _, status := range []genericschedule.Status{genericschedule.StatusCancelled, genericschedule.StatusFailed, genericschedule.StatusFired} {
			for _, offset := range []time.Duration{-time.Microsecond, 0, time.Microsecond} {
				t.Run(map[bool]string{false: "flow/", true: "root/"}[root]+string(status)+"/"+offset.String(), func(t *testing.T) {
					_, _, requests, rows := arrivalJoinInventoryFixture(t, root, false)
					row := &rows[0]
					base := row.AdmittedAt
					if row.CurrentDueAt.After(base) {
						base = row.CurrentDueAt
					}
					admission := base.Add(2 * time.Minute)
					row.CurrentEventID, row.CurrentEventAdmittedAt = genericschedule.OccurrenceEventID(row.ID, row.CurrentDueAt), admission
					row.Status = status
					switch status {
					case genericschedule.StatusCancelled:
						row.CancelCause, row.CancelledAt = "join_stage_exit", admission.Add(offset)
					case genericschedule.StatusFailed:
						row.Failure, row.FailedAt = genericschedule.Failure{Code: "publication_failed"}, admission.Add(offset)
					case genericschedule.StatusFired:
						row.FiredAt, row.AcceptedAt = admission.Add(offset), admission.Add(time.Minute)
					}
					if err := requireExactRunForkArrivalJoinInventory(requests, rows, runForkArrivalScheduleContinuing); (err == nil) != (offset >= 0) {
						t.Fatalf("continuing phase offset=%s err=%v", offset, err)
					}
					if err := requireExactRunForkArrivalJoinInventory(requests, rows, runForkArrivalScheduleAtCut); err == nil {
						t.Fatal("staged phase accepted progressed occurrence")
					}
				})
			}
		}
	}
}
