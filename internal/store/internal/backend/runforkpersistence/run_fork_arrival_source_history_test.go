package runforkpersistence

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
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

func mixedTimerHistoryFixture(t *testing.T) (runfork.RunForkPlan, time.Time, []runForkWorkflowTimerProjection, []genericschedule.Activation) {
	t.Helper()
	plan, bornAt, _, arrivals := arrivalJoinInventoryFixture(t, true, true)
	ordinary := workflowTimerProjectionSource(t, true)
	ordinary.RunID, ordinary.EntityID = plan.SourceRunID, plan.Entities[0].EntityID
	ordinary.Route = flowidentity.StoredRoute(".", plan.SourceRunID, plan.SourceRunID)
	var err error
	ordinary.RoutingSource, err = events.NewRootRoutingSource(ordinary.EntityID)
	if err != nil {
		t.Fatal(err)
	}
	plan.WorkflowTimers = []pipeline.WorkflowTimerActivationPersistenceRecord{ordinary.PersistenceRecord()}
	inventory, err := runForkTimerRecordInventory(plan.SourceRunID, plan.WorkflowTimers, plan.JoinSchedules)
	if err != nil {
		t.Fatal(err)
	}
	inventory.Complete, inventory.Point = true, plan.ForkPoint
	plan.ReplayResumeAdmission = runForkReplayResumeAdmission(runForkAdmissionEvidence{RelevantTimer: true, TimerHistory: inventory})
	projected, err := prepareRunForkWorkflowTimers(plan, workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, bornAt)
	if err != nil || len(projected) != 1 {
		t.Fatalf("mixed ordinary projection: %+v err=%v", projected, err)
	}
	return plan, bornAt, projected, arrivals
}

// These are complete-owner/frame proofs. The family readers below are mocked,
// not native persistence or selected-child join execution.
func TestRunForkTimerHistoryDischargeRequiresBothCompleteInventories(t *testing.T) {
	for _, name := range []string{"exact", "missing_workflow", "extra_workflow", "missing_arrival", "extra_arrival", "arrival_failure", "foreign_arrival", "empty_arrival_owner"} {
		t.Run(name, func(t *testing.T) {
			plan, bornAt, projected, arrivals := mixedTimerHistoryFixture(t)
			ordinary := projected[0].activation
			workflow := &workflowTimerMaterializerOwner{rows: map[string]pipeline.WorkflowTimerActivation{ordinary.Ref.ActivationID: ordinary}}
			arrival := &arrivalJoinInventoryOwner{rows: arrivals}
			var arrivalOwner runForkArrivalJoinMaterializationOwner = arrival
			switch name {
			case "missing_workflow":
				workflow.rows = nil
			case "extra_workflow":
				extra := ordinary.Canonical()
				extra.Ref.ActivationID = "99999999-9999-4999-8999-999999999999"
				workflow.rows[extra.Ref.ActivationID] = extra
			case "missing_arrival":
				arrival.rows = arrival.rows[1:]
			case "extra_arrival":
				arrival.rows = append(arrival.rows, arrival.rows[0])
			case "arrival_failure":
				arrival.err = errors.New("arrival read failed after ordinary readback")
			case "foreign_arrival":
				arrival.rows[0].Command.RunID = plan.SourceRunID
			case "empty_arrival_owner":
				arrivalOwner = nil
			}
			before, _ := json.Marshal(plan.ReplayResumeAdmission)
			withWorkflowTimerReadbackAttempt(t, func(ctx context.Context, attempt *mutationprotocol.Attempt) {
				for _, phase := range []runForkWorkflowTimerReadbackPhase{runForkWorkflowTimerAtCut, runForkWorkflowTimerContinuing} {
					got, err := requireRunForkTimerHistory(ctx, attempt, plan, workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, workflow, arrivalOwner, bornAt, plan.ReplayResumeAdmission, phase)
					if name != "exact" {
						if err == nil || !reflect.DeepEqual(got, plan.ReplayResumeAdmission) {
							t.Fatalf("partial family discharged source history: phase=%d admission=%+v err=%v", phase, got, err)
						}
						continue
					}
					if err != nil || len(got.UnsupportedBlockers) != 0 || got.StateOnlyExecutionReady || !got.ReplayResumeFactsPresent {
						t.Fatalf("mixed complete inventory: phase=%d admission=%+v err=%v", phase, got, err)
					}
					assertWorkflowTimerAppliedDisposition(t, got, runfork.RunForkReplayResumeDispositionReconstruct)
				}
			})
			after, _ := json.Marshal(plan.ReplayResumeAdmission)
			if string(before) != string(after) || workflow.writes != 0 || workflow.inserts != 0 || workflow.cancels != 0 {
				t.Fatal("require-only discharge changed source admission or wrote business state")
			}
		})
	}
}

func TestRunForkTimerHistoryContinuationUsesStableImmutableEvidence(t *testing.T) {
	plan, bornAt, projected, arrivals := mixedTimerHistoryFixture(t)
	ordinary := projected[0].activation
	workflow := &workflowTimerMaterializerOwner{rows: map[string]pipeline.WorkflowTimerActivation{ordinary.Ref.ActivationID: ordinary}}
	arrival := &arrivalJoinInventoryOwner{rows: arrivals}
	withWorkflowTimerReadbackAttempt(t, func(ctx context.Context, attempt *mutationprotocol.Attempt) {
		cut, err := requireMaterializedRunForkTimerHistory(ctx, attempt, plan, workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, workflow, arrival, bornAt, plan.ReplayResumeAdmission)
		if err != nil {
			t.Fatal(err)
		}
		ordinary.Status, ordinary.FiredAt = "fired", bornAt.Add(time.Minute)
		workflow.rows[ordinary.Ref.ActivationID] = ordinary
		for i := range arrival.rows {
			row := &arrival.rows[i]
			if row.Status != genericschedule.StatusActive {
				continue
			}
			row.CurrentEventID = genericschedule.OccurrenceEventID(row.ID, row.CurrentDueAt)
			row.CurrentEventAdmittedAt = bornAt.Add(time.Minute)
			row.Status, row.FiredAt, row.AcceptedAt = genericschedule.StatusFired, row.CurrentEventAdmittedAt, row.CurrentEventAdmittedAt
		}
		if got, err := requireMaterializedRunForkTimerHistory(ctx, attempt, plan, workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, workflow, arrival, bornAt, plan.ReplayResumeAdmission); err == nil || !reflect.DeepEqual(got, plan.ReplayResumeAdmission) {
			t.Fatal("unactivated phase accepted progressed child evidence")
		}
		continued, err := requireContinuingRunForkTimerHistory(ctx, attempt, plan, workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, workflow, arrival, bornAt, plan.ReplayResumeAdmission)
		if err != nil || !reflect.DeepEqual(cut, continued) {
			t.Fatalf("lawful progress changed fixed-cut discharge: cut=%+v continued=%+v err=%v", cut, continued, err)
		}
	})
}

func TestRunForkArrivalHistoryRetainsPublishedAndTerminalRefusals(t *testing.T) {
	for _, status := range []string{"prepared", "fired", "failed"} {
		t.Run(status, func(t *testing.T) {
			plan, bornAt, _, _ := arrivalJoinInventoryFixture(t, true, false)
			row := &plan.JoinSchedules[0]
			at := row.CurrentDueAt.Add(time.Second)
			row.CurrentEventID = genericschedule.OccurrenceEventID(row.ID, row.CurrentDueAt)
			row.CurrentEventAdmittedAt = at
			switch status {
			case "fired":
				row.Status, row.FiredAt, row.AcceptedAt = genericschedule.StatusFired, at, at
			case "failed":
				row.Status, row.FailedAt = genericschedule.StatusFailed, at
				row.Failure = genericschedule.Failure{Code: "publication_failed", Message: "original failure"}
			}
			inventory, err := runForkTimerRecordInventory(plan.SourceRunID, nil, plan.JoinSchedules)
			if err != nil {
				t.Fatal(err)
			}
			inventory.Complete, inventory.Point = true, plan.ForkPoint
			plan.ReplayResumeAdmission = runForkReplayResumeAdmission(runForkAdmissionEvidence{RelevantTimer: true, TimerHistory: inventory})
			if allowed, err := runForkTimerHistoryMaterializable(plan); err != nil || allowed {
				t.Fatalf("source evidence permitted rearming %s: allowed=%t err=%v", status, allowed, err)
			}
			if _, err := prepareRunForkArrivalJoinRequests(plan, workflowTimerProjectionChildRun, bornAt); err == nil {
				t.Fatal("published occurrence became an unpublished ForkJoinRequest")
			}
			workflow, arrival := &workflowTimerMaterializerOwner{}, &arrivalJoinInventoryOwner{}
			withWorkflowTimerReadbackAttempt(t, func(ctx context.Context, attempt *mutationprotocol.Attempt) {
				for _, phase := range []runForkWorkflowTimerReadbackPhase{runForkWorkflowTimerAtCut, runForkWorkflowTimerContinuing} {
					got, err := requireRunForkTimerHistory(ctx, attempt, plan, workflowTimerProjectionChildRun, nil, workflow, arrival, bornAt, plan.ReplayResumeAdmission, phase)
					if err == nil || !reflect.DeepEqual(got, plan.ReplayResumeAdmission) {
						t.Fatal("missing historical occurrence continuation discharged timer history")
					}
				}
			})
			if workflow.reads != 0 || workflow.writes != 0 || arrival.reads != 0 {
				t.Fatal("unsupported occurrence reached child schedule reconstruction or readback")
			}
		})
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
