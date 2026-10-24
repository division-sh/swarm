package runforkpersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"

	"github.com/google/uuid"
)

func arrivalJoinInventoryFixture(t *testing.T, root, completion bool) (runfork.RunForkPlan, time.Time, []storegenericschedule.ForkJoinRequest, []genericschedule.Activation) {
	t.Helper()
	snapshot, entities, _ := arrivalJoinScheduleProjectionFixture(t, root, completion)
	schedules, err := loadRunForkArrivalJoinSchedules(snapshot, entities)
	if err != nil {
		t.Fatal(err)
	}
	plan := runfork.RunForkPlan{SourceRunID: snapshot.RunID, Entities: entities, JoinSchedules: schedules,
		ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 7}}
	bornAt := schedules[0].AdmittedAt.Add(2 * time.Hour)
	requests, err := prepareRunForkArrivalJoinRequests(plan, workflowTimerProjectionChildRun, bornAt)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]genericschedule.Activation, 0, len(requests))
	for _, request := range requests {
		row, err := request.Expected(uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	return plan, bornAt, requests, rows
}

func TestForkArrivalInventorySeparatesCutAndContinuingProgress(t *testing.T) {
	for _, root := range []bool{false, true} {
		_, _, requests, original := arrivalJoinInventoryFixture(t, root, true)
		for _, state := range []string{"prepared", "fired", "cancelled", "failed", "reopened_companion", "changed_companion"} {
			t.Run(map[bool]string{false: "flow/", true: "root/"}[root]+state, func(t *testing.T) {
				rows := make([]genericschedule.Activation, len(original))
				for i, row := range original {
					rows[i] = row.Canonical()
				}
				for i := range rows {
					row := &rows[i]
					if row.Status == genericschedule.StatusCancelled {
						if state == "reopened_companion" {
							row.Status, row.CancelCause, row.CancelledAt = genericschedule.StatusActive, "", time.Time{}
						}
						if state == "changed_companion" {
							row.CancelCause = "different_cause"
						}
						continue
					}
					at := row.AdmittedAt.Add(time.Minute)
					switch state {
					case "prepared", "fired":
						row.CurrentEventID = genericschedule.OccurrenceEventID(row.ID, row.CurrentDueAt)
						row.CurrentEventAdmittedAt = at
						if state == "fired" {
							row.Status, row.FiredAt, row.AcceptedAt = genericschedule.StatusFired, at, at
						}
					case "cancelled":
						row.Status, row.CancelCause, row.CancelledAt = genericschedule.StatusCancelled, "join_stage_exit", at
					case "failed":
						row.Status, row.FailedAt, row.Failure = genericschedule.StatusFailed, at, genericschedule.Failure{Code: "publication_failed", Message: "original failure"}
					}
				}
				if err := requireExactRunForkArrivalJoinInventory(requests, rows, runForkArrivalScheduleAtCut); err == nil {
					t.Fatal("staged child silently accepted lifecycle progress")
				}
				err := requireExactRunForkArrivalJoinInventory(requests, rows, runForkArrivalScheduleContinuing)
				allowed := state != "reopened_companion" && state != "changed_companion"
				if (err == nil) != allowed {
					t.Fatalf("continuing %s: err=%v", state, err)
				}
				if reflect.DeepEqual(rows, original) {
					t.Fatal("fixture did not progress")
				}
			})
		}
	}
}
