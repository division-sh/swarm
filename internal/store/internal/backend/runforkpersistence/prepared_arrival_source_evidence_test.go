package runforkpersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/google/uuid"
)

func TestPreparedArrivalRequiresCompleteFixedCutPublicationAbsence(t *testing.T) {
	for _, root := range []bool{false, true} {
		for _, completion := range []bool{false, true} {
			for _, fault := range []string{"exact", "unknown_history", "wrong_revision", "published", "unrelated_event", "foreign_run", "before_due", "future_preparation"} {
				t.Run(map[bool]string{false: "flow/", true: "root/"}[root]+map[bool]string{false: "timeout/", true: "completion/"}[completion]+fault, func(t *testing.T) {
					plan, bornAt, _, _ := arrivalJoinInventoryFixture(t, root, completion)
					var row *genericschedule.Activation
					for i := range plan.JoinSchedules {
						if plan.JoinSchedules[i].Status == genericschedule.StatusActive {
							if row != nil {
								t.Fatal("fixture requires exactly one active arrival")
							}
							row = &plan.JoinSchedules[i]
						}
					}
					if row == nil {
						t.Fatal("fixture lacks an active arrival")
					}
					row.CurrentEventID = genericschedule.OccurrenceEventID(row.ID, row.CurrentDueAt)
					row.CurrentEventAdmittedAt = row.CurrentDueAt.Add(time.Microsecond)
					plan = plan.WithHistoricalEvents(plan.ForkPoint.Revision, nil)
					switch fault {
					case "unknown_history":
						plan = plan.WithHistoricalEvents(0, nil)
					case "wrong_revision":
						plan = plan.WithHistoricalEvents(plan.ForkPoint.Revision+1, nil)
					case "published":
						plan = plan.WithHistoricalEvents(plan.ForkPoint.Revision, []string{row.CurrentEventID})
					case "unrelated_event":
						plan = plan.WithHistoricalEvents(plan.ForkPoint.Revision, []string{uuid.NewString()})
					case "foreign_run":
						plan.SourceRunID = uuid.NewString()
					case "before_due":
						row.CurrentEventAdmittedAt = row.CurrentDueAt.Add(-time.Microsecond)
					case "future_preparation":
						row.CurrentEventAdmittedAt = bornAt.Add(time.Microsecond)
					}
					before := row.Canonical()
					requests, err := prepareRunForkArrivalJoinRequests(plan, workflowTimerProjectionChildRun, bornAt)
					allowed := fault == "exact" || fault == "unrelated_event"
					if (err == nil) != allowed {
						t.Fatalf("prepared %s restoration: allowed=%t err=%v", fault, allowed, err)
					}
					if allowed {
						for _, request := range requests {
							child, err := request.Expected(uuid.NewString())
							if err != nil || child.CurrentEventID != "" || !child.CurrentEventAdmittedAt.IsZero() ||
								!child.InitialDueAt.Equal(request.Source.InitialDueAt) || !child.CurrentDueAt.Equal(request.Source.CurrentDueAt) ||
								child.ForkJoinOrigin == nil || child.ForkJoinOrigin.SourceActivationID != request.Source.ID {
								t.Fatalf("child copied a source candidate or rearmed its due: %+v err=%v", child, err)
							}
						}
					}
					if !reflect.DeepEqual(before, row.Canonical()) {
						t.Fatal("restoration validation changed source evidence")
					}
					if !allowed && requests != nil {
						t.Fatal("refusal returned partial restoration work")
					}
				})
			}
		}
	}
}
