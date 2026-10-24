package runforkpersistence

import (
	"encoding/json"

	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/genericschedule"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestFixedRevisionArrivalHistoryHasCompletePendingInventory(t *testing.T) {
	for _, root := range []bool{false, true} {
		for _, completion := range []bool{false, true} {
			t.Run(map[bool]string{false: "flow/", true: "root/"}[root]+map[bool]string{false: "timeout", true: "completion"}[completion], func(t *testing.T) {
				snapshot, entities, _ := arrivalJoinScheduleProjectionFixture(t, root, completion)
				before, err := json.Marshal(snapshot)
				if err != nil {
					t.Fatal(err)
				}
				evidence, err := loadRunForkAdmissionEvidenceFromRevision(snapshot, entities, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				evidence.TimerHistory.Point = runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 7}
				if _, err := evidence.TimerHistory.pendingCertificate(); err != nil {
					t.Fatalf("exact arrival history lacks complete pending inventory: %+v err=%v", evidence.TimerHistory, err)
				}
				admission := runForkReplayResumeAdmission(evidence)
				blocked := false
				for _, blocker := range admission.UnsupportedBlockers {
					blocked = blocked || blocker.Code == runfork.RunForkBlockerTimerHistoryUnproven
				}
				if !blocked || admission.StateOnlyExecutionReady || admission.DeliveryEventReplayReady {
					t.Fatal("source correspondence granted execution before complete native readback")
				}
				after, err := json.Marshal(snapshot)
				if err != nil || string(before) != string(after) {
					t.Fatalf("pending inventory changed its source snapshot: %v", err)
				}
			})
		}
	}
}

func TestRunForkArrivalSourceInventoryRequiresExactPhysicalRows(t *testing.T) {
	for _, name := range []string{"exact", "missing_physical", "duplicate", "changed_valid_lifecycle", "foreign_owner", "unresolved_extra"} {
		t.Run(name, func(t *testing.T) {
			snapshot, entities, _ := arrivalJoinScheduleProjectionFixture(t, true, false)
			arrivals, err := loadRunForkArrivalJoinSchedules(snapshot, entities)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "missing_physical":
				snapshot.Timers = nil
			case "duplicate":
				arrivals = append(arrivals, arrivals[0])
			case "changed_valid_lifecycle":
				arrivals[0].Status = genericschedule.StatusCancelled
				arrivals[0].CancelCause, arrivals[0].CancelledAt = "join_closed", arrivals[0].AdmittedAt.Add(time.Second)
				if err := arrivals[0].Validate(); err != nil {
					t.Fatal(err)
				}
			case "foreign_owner":
				arrivals[0].Command.RunID = workflowTimerProjectionChildRun
			case "unresolved_extra":
				extra := snapshot.Timers[0]
				extra.TimerID = "99999999-9999-4999-8999-999999999999"
				snapshot.Timers = append(snapshot.Timers, extra)
			}
			inventory, err := loadRunForkTimerHistoryInventory(snapshot, loadRunForkSourceFactsFromRevision(snapshot, entities), nil, arrivals)
			if name != "exact" && name != "unresolved_extra" {
				if err == nil {
					t.Fatal("substituted arrival inventory gained source admission")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			inventory.Point = runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 7}
			_, err = inventory.pendingCertificate()
			if (err == nil) != (name == "exact") {
				t.Fatalf("unresolved physical family was lost: %+v err=%v", inventory, err)
			}
		})
	}
}
