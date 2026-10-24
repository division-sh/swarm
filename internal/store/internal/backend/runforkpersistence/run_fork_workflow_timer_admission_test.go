package runforkpersistence

import (
	"encoding/json"

	"reflect"

	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func workflowTimerHistorySnapshot(t *testing.T, source pipeline.WorkflowTimerActivation) runForkRevisionTimer {
	t.Helper()
	var snapshot runforkrevision.TimerSnapshot
	if err := json.Unmarshal(workflowTimerProjectionRaw(t, source), &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.CancelCause = string(source.CancelCause)
	if !source.CancelledAt.IsZero() {
		snapshot.CancelledAt = &source.CancelledAt
	}
	return runForkRevisionTimer{TimerSnapshot: snapshot}
}

func workflowTimerHistoryPlan(t *testing.T, timers ...pipeline.WorkflowTimerActivation) runfork.RunForkPlan {
	t.Helper()
	plan := workflowTimerMaterializerPlan(timers[0], runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 7})
	plan.WorkflowTimers = nil
	for _, timer := range timers {
		plan.WorkflowTimers = append(plan.WorkflowTimers, timer.PersistenceRecord())
	}
	inventory, err := runForkTimerRecordInventory(plan.SourceRunID, plan.WorkflowTimers, plan.JoinSchedules)
	if err != nil {
		t.Fatal(err)
	}
	inventory.Complete, inventory.Point = true, plan.ForkPoint
	plan.ReplayResumeAdmission = runForkReplayResumeAdmission(runForkAdmissionEvidence{RelevantTimer: true, TimerHistory: inventory})
	return plan
}

func workflowTimerHistoryTerminal(t *testing.T, status string, id string) pipeline.WorkflowTimerActivation {
	t.Helper()
	timer := workflowTimerProjectionSource(t, true)
	timer.Ref.ActivationID, timer.Status = id, status
	if status == "fired" {
		timer.FiredAt = timer.FireAt.Add(time.Second)
	}
	if err := timer.Validate(); err != nil {
		t.Fatal(err)
	}
	return timer
}

func TestRunForkWorkflowTimerHistoryInventoryCoversAllRelevantRows(t *testing.T) {
	active := workflowTimerProjectionSource(t, true)
	fired := workflowTimerHistoryTerminal(t, "fired", "66666666-6666-4666-8666-666666666666")
	cancelled := workflowTimerHistoryTerminal(t, "cancelled", "77777777-7777-4777-8777-777777777777")
	snapshot := &runForkRevisionSnapshot{RunID: active.RunID, Timers: []runForkRevisionTimer{
		workflowTimerHistorySnapshot(t, cancelled), workflowTimerHistorySnapshot(t, active), workflowTimerHistorySnapshot(t, fired),
	}}
	inventory, err := loadRunForkTimerHistoryInventory(snapshot, runForkSourceFacts{}, nil, nil)
	if err != nil || !inventory.Complete || len(inventory.WorkflowTimerIDs) != 3 || len(inventory.ActiveTimerIDs) != 1 || len(inventory.UnresolvedTimerIDs) != 0 {
		t.Fatalf("incomplete ordinary inventory: %+v err=%v", inventory, err)
	}
	inventory.Point = runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 7}
	if _, err := inventory.pendingCertificate(); err != nil {
		t.Fatal(err)
	}
	for _, taskType := range []string{"timer", "join_timeout", "barrier_timeout", "unknown"} {
		t.Run(taskType, func(t *testing.T) {
			mixed := *snapshot
			mixed.Timers = append([]runForkRevisionTimer(nil), snapshot.Timers...)
			mixed.Timers = append(mixed.Timers, runForkRevisionTimer{TimerSnapshot: runforkrevision.TimerSnapshot{
				TimerID: "generic-deadline", RunID: active.RunID, TaskType: taskType,
			}})
			got, err := loadRunForkTimerHistoryInventory(&mixed, runForkSourceFacts{}, nil, nil)
			if err != nil || len(got.WorkflowTimerIDs) != 3 || !reflect.DeepEqual(got.UnresolvedTimerIDs, []string{"generic-deadline"}) {
				t.Fatalf("mixed history lost rows: %+v err=%v", got, err)
			}
			got.Point = inventory.Point
			admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{RelevantTimer: true, TimerHistory: got})
			plan := workflowTimerHistoryPlan(t, active, fired, cancelled)
			plan.ReplayResumeAdmission = admission
			if allowed, err := runForkTimerHistoryMaterializable(plan); err != nil || allowed {
				t.Fatalf("ordinary subset discharged unresolved timer family: allowed=%t err=%v", allowed, err)
			}
		})
	}
}

func TestRunForkWorkflowTimerHistoryInventoryPreservesForeignAndBarrierBoundaries(t *testing.T) {
	source := workflowTimerProjectionSource(t, true)
	foreign := workflowTimerHistorySnapshot(t, source)
	foreign.TimerID, foreign.RunID = "foreign-timer", workflowTimerProjectionChildRun
	global := runForkRevisionTimer{TimerSnapshot: runforkrevision.TimerSnapshot{TimerID: "global-timer", FlowInstance: source.Route.InstancePath, TaskType: "timer"}}
	unrelated := runForkRevisionTimer{TimerSnapshot: runforkrevision.TimerSnapshot{TimerID: "unrelated", RunID: workflowTimerProjectionChildRun, TaskType: "timer"}}
	owned := runForkRevisionTimer{TimerSnapshot: runforkrevision.TimerSnapshot{TimerID: "owned-barrier", RunID: source.RunID, TaskType: "timer"}}
	snapshot := &runForkRevisionSnapshot{RunID: source.RunID, Timers: []runForkRevisionTimer{
		workflowTimerHistorySnapshot(t, source), foreign, global, unrelated, owned,
	}}
	facts := runForkSourceFacts{EntityIDs: []string{source.EntityID}, FlowInstances: []string{source.Route.InstancePath}}
	inventory, err := loadRunForkTimerHistoryInventory(snapshot, facts, map[string]struct{}{owned.TimerID: {}}, nil)
	if err != nil || !reflect.DeepEqual(inventory.UnresolvedTimerIDs, []string{"foreign-timer", "global-timer"}) || len(inventory.WorkflowTimerIDs) != 1 {
		t.Fatalf("relevance or validated barrier exclusion changed: %+v err=%v", inventory, err)
	}
	for _, corruption := range []string{"duplicate", "empty", "padded", "bad_task_ref"} {
		t.Run(corruption, func(t *testing.T) {
			bad := workflowTimerHistorySnapshot(t, source)
			rows := []runForkRevisionTimer{bad}
			switch corruption {
			case "duplicate":
				rows = append(rows, bad)
			case "empty":
				rows[0].TimerID = ""
			case "padded":
				rows[0].TimerID = " " + bad.TimerID
			case "bad_task_ref":
				rows[0].TimerName = "not-a-workflow-timer"
			}
			if _, err := loadRunForkTimerHistoryInventory(&runForkRevisionSnapshot{RunID: source.RunID, Timers: rows}, facts, nil, nil); err == nil {
				t.Fatal("corrupt inventory acquired prospective admission")
			}
		})
	}
}

func TestRunForkWorkflowTimerHistoryProspectiveCertificateIsExactAndStillBlocked(t *testing.T) {
	source := workflowTimerProjectionSource(t, true)
	fired := workflowTimerHistoryTerminal(t, "fired", "66666666-6666-4666-8666-666666666666")
	plan := workflowTimerHistoryPlan(t, source, fired)
	before, _ := json.Marshal(plan)
	if allowed, err := runForkTimerHistoryMaterializable(plan); err != nil || !allowed {
		t.Fatalf("exact ordinary inventory cannot be materialized: allowed=%t err=%v", allowed, err)
	}
	after, _ := json.Marshal(plan)
	if string(before) != string(after) || plan.ReplayResumeAdmission.StateOnlyExecutionReady ||
		plan.ReplayResumeAdmission.DeliveryEventReplayReady || len(plan.ReplayResumeAdmission.UnsupportedBlockers) != 1 {
		t.Fatal("prospective certificate mutated admission or granted execution")
	}
	for _, name := range []string{"omitted", "extra", "duplicate", "payload", "due", "status", "padded_run", "point", "old_evidence", "stale_reconstruct", "missing_blocker", "duplicate_blocker", "missing_fact"} {
		t.Run(name, func(t *testing.T) {
			candidate := workflowTimerHistoryPlan(t, source, fired)
			switch name {
			case "omitted":
				candidate.WorkflowTimers = candidate.WorkflowTimers[:1]
			case "extra", "duplicate":
				row := source.PersistenceRecord()
				if name == "extra" {
					other := source.Canonical()
					other.Ref.ActivationID = "88888888-8888-4888-8888-888888888888"
					row = other.PersistenceRecord()
				}
				candidate.WorkflowTimers = append(candidate.WorkflowTimers, row)
			case "payload":
				candidate.WorkflowTimers[0].Payload = []byte(`{"changed":true}`)
			case "due":
				candidate.WorkflowTimers[0].FireAt = source.FireAt.Add(time.Second)
			case "status":
				candidate.WorkflowTimers[0].Status = "cancelled"
			case "padded_run":
				candidate.WorkflowTimers[0].RunID = " " + source.RunID
			case "point":
				candidate.ForkPoint.Revision++
			case "old_evidence":
				candidate.ReplayResumeAdmission = runForkReplayResumeAdmission(runForkAdmissionEvidence{RelevantTimer: true})
			case "stale_reconstruct":
				for i := range candidate.ReplayResumeAdmission.Dispositions {
					if candidate.ReplayResumeAdmission.Dispositions[i].Fact == runfork.RunForkReplayResumeFactTimerHistory {
						candidate.ReplayResumeAdmission.Dispositions[i].Disposition = runfork.RunForkReplayResumeDispositionReconstruct
					}
				}
			case "missing_blocker":
				candidate.ReplayResumeAdmission.UnsupportedBlockers = nil
			case "duplicate_blocker":
				candidate.ReplayResumeAdmission.UnsupportedBlockers = append(candidate.ReplayResumeAdmission.UnsupportedBlockers, candidate.ReplayResumeAdmission.UnsupportedBlockers[0])
			case "missing_fact":
				candidate.ReplayResumeAdmission.Dispositions = candidate.ReplayResumeAdmission.Dispositions[:3]
			}
			if allowed, _ := runForkTimerHistoryMaterializable(candidate); allowed {
				t.Fatal("changed or incomplete inventory acquired materialization admission")
			}
		})
	}
	plan.WorkflowTimers[0], plan.WorkflowTimers[1] = plan.WorkflowTimers[1], plan.WorkflowTimers[0]
	if allowed, err := runForkTimerHistoryMaterializable(plan); err != nil || !allowed {
		t.Fatalf("row ordering changed canonical inventory: allowed=%t err=%v", allowed, err)
	}
}
