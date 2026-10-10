package runforkpersistence

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
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
	inventory, err := runForkWorkflowTimerRecordInventory(plan.SourceRunID, plan.WorkflowTimers)
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
	inventory, err := loadRunForkTimerHistoryInventory(snapshot, runForkSourceFacts{}, nil)
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
			got, err := loadRunForkTimerHistoryInventory(&mixed, runForkSourceFacts{}, nil)
			if err != nil || len(got.WorkflowTimerIDs) != 3 || !reflect.DeepEqual(got.UnresolvedTimerIDs, []string{"generic-deadline"}) {
				t.Fatalf("mixed history lost rows: %+v err=%v", got, err)
			}
			got.Point = inventory.Point
			admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{RelevantTimer: true, TimerHistory: got})
			plan := workflowTimerHistoryPlan(t, active, fired, cancelled)
			plan.ReplayResumeAdmission = admission
			if allowed, err := runForkWorkflowTimerHistoryMaterializable(plan); err != nil || allowed {
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
	inventory, err := loadRunForkTimerHistoryInventory(snapshot, facts, map[string]struct{}{owned.TimerID: {}})
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
			if _, err := loadRunForkTimerHistoryInventory(&runForkRevisionSnapshot{RunID: source.RunID, Timers: rows}, facts, nil); err == nil {
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
	if allowed, err := runForkWorkflowTimerHistoryMaterializable(plan); err != nil || !allowed {
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
			if allowed, _ := runForkWorkflowTimerHistoryMaterializable(candidate); allowed {
				t.Fatal("changed or incomplete inventory acquired materialization admission")
			}
		})
	}
	plan.WorkflowTimers[0], plan.WorkflowTimers[1] = plan.WorkflowTimers[1], plan.WorkflowTimers[0]
	if allowed, err := runForkWorkflowTimerHistoryMaterializable(plan); err != nil || !allowed {
		t.Fatalf("row ordering changed canonical inventory: allowed=%t err=%v", allowed, err)
	}
}

// This is a SQLmock frame proof, not native PostgreSQL/SQLite timer persistence.
func withWorkflowTimerReadbackAttempt(t *testing.T, check func(context.Context, *mutationprotocol.Attempt)) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	mock.ExpectRollback()
	rollback := errors.New("workflow timer readback unit rollback")
	ctx := correlation.WithRunID(context.Background(), workflowTimerProjectionChildRun)
	result := mutationprotocol.RunPostgres(ctx, backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil,
		func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			check(ctx, attempt)
			return struct{}{}, rollback
		})
	if result.Acknowledged() || !errors.Is(result.Err(), rollback) {
		t.Fatalf("unit frame outcome: acknowledged=%t err=%v", result.Acknowledged(), result.Err())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRunForkWorkflowTimerAdmissionReadbackIsRequireOnlyAndComplete(t *testing.T) {
	source := workflowTimerProjectionSource(t, true)
	fired := workflowTimerHistoryTerminal(t, "fired", "66666666-6666-4666-8666-666666666666")
	cancelled := workflowTimerHistoryTerminal(t, "cancelled", "77777777-7777-4777-8777-777777777777")
	for _, removed := range []bool{false, true} {
		t.Run(map[bool]string{false: "selected", true: "removed"}[removed], func(t *testing.T) {
			plan := workflowTimerHistoryPlan(t, source, fired, cancelled)
			bornAt := source.CreatedAt.Add(2 * time.Hour)
			selection := workflowTimerMaterializerSelection{removed: map[string]bool{source.Ref.DeclarationKey: removed}}
			owner := &workflowTimerMaterializerOwner{rows: make(map[string]pipeline.WorkflowTimerActivation)}
			if _, err := materializeRunForkWorkflowTimers(context.Background(), new(mutationprotocol.Attempt), plan,
				workflowTimerProjectionChildRun, selection, owner, bornAt, false); err != nil {
				t.Fatal(err)
			}
			if len(owner.rows) != 1 || owner.inserts != 1 {
				t.Fatal("terminal source facts were rearmed")
			}
			writes := owner.writes
			before, _ := json.Marshal(plan.ReplayResumeAdmission)
			withWorkflowTimerReadbackAttempt(t, func(ctx context.Context, attempt *mutationprotocol.Attempt) {
				for i := 0; i < 2; i++ {
					admitted, err := requireMaterializedRunForkWorkflowTimers(ctx, attempt, plan, workflowTimerProjectionChildRun,
						selection, owner, bornAt, plan.ReplayResumeAdmission)
					if err != nil || len(admitted.UnsupportedBlockers) != 0 || !admitted.ReplayResumeFactsPresent || admitted.StateOnlyExecutionReady {
						t.Fatalf("complete readback admission=%+v err=%v", admitted, err)
					}
					assertWorkflowTimerAppliedDisposition(t, admitted, runfork.RunForkReplayResumeDispositionReconstruct)
					admitted.Dispositions[0].Message = "caller-local"
				}
			})
			after, _ := json.Marshal(plan.ReplayResumeAdmission)
			if owner.writes != writes || owner.requires != 2 || string(before) != string(after) {
				t.Fatal("require-only admission wrote, skipped a readback, or aliased source admission")
			}
		})
	}
}

func assertWorkflowTimerAppliedDisposition(t *testing.T, admission runfork.RunForkReplayResumeAdmission, want string) {
	t.Helper()
	for _, disposition := range admission.Dispositions {
		if disposition.Fact != runfork.RunForkReplayResumeFactTimerHistory {
			continue
		}
		if disposition.Owner != runForkWorkflowTimerReadbackOwner || !strings.HasPrefix(disposition.Classification, runForkWorkflowTimerAppliedPrefix) ||
			disposition.Disposition != want || disposition.BlockerCode != "" {
			t.Fatalf("incorrect applied disposition: %+v", disposition)
		}
		return
	}
	t.Fatal("applied timer disposition missing")
}

func TestRunForkWorkflowTimerAdmissionTerminalOnlyNeverRearms(t *testing.T) {
	fired := workflowTimerHistoryTerminal(t, "fired", "66666666-6666-4666-8666-666666666666")
	cancelled := workflowTimerHistoryTerminal(t, "cancelled", "77777777-7777-4777-8777-777777777777")
	plan := workflowTimerHistoryPlan(t, fired, cancelled)
	owner := &workflowTimerMaterializerOwner{rows: make(map[string]pipeline.WorkflowTimerActivation)}
	withWorkflowTimerReadbackAttempt(t, func(ctx context.Context, attempt *mutationprotocol.Attempt) {
		admitted, err := requireMaterializedRunForkWorkflowTimers(ctx, attempt, plan, workflowTimerProjectionChildRun, nil, owner,
			fired.CreatedAt.Add(2*time.Hour), plan.ReplayResumeAdmission)
		if err != nil || !admitted.StateOnlyExecutionReady || admitted.ReplayResumeFactsPresent || len(admitted.UnsupportedBlockers) != 0 {
			t.Fatalf("terminal-only admission=%+v err=%v", admitted, err)
		}
		assertWorkflowTimerAppliedDisposition(t, admitted, runfork.RunForkReplayResumeDispositionNoHistoricalAction)
	})
	if owner.requires != 0 || owner.writes != 0 || len(owner.rows) != 0 {
		t.Fatal("terminal-only history created or required a child activation")
	}
}

func TestRunForkWorkflowTimerAdmissionReadbackRequiresEveryActiveSourceRow(t *testing.T) {
	first := workflowTimerProjectionSource(t, true)
	second := first.Canonical()
	second.Ref.ActivationID, second.Ref.DeclarationKey = "66666666-6666-4666-8666-666666666666", "stage:.:second"
	plan := workflowTimerHistoryPlan(t, first, second)
	bornAt := first.CreatedAt.Add(2 * time.Hour)
	selection := workflowTimerMaterializerSelection{}
	projected, err := prepareRunForkWorkflowTimers(plan, workflowTimerProjectionChildRun, selection, bornAt)
	if err != nil || len(projected) != 2 {
		t.Fatalf("complete projection=%+v err=%v", projected, err)
	}
	owner := &workflowTimerMaterializerOwner{rows: map[string]pipeline.WorkflowTimerActivation{
		projected[0].activation.Ref.ActivationID: projected[0].activation,
	}}
	withWorkflowTimerReadbackAttempt(t, func(ctx context.Context, attempt *mutationprotocol.Attempt) {
		got, err := requireMaterializedRunForkWorkflowTimers(ctx, attempt, plan, workflowTimerProjectionChildRun, selection, owner, bornAt, plan.ReplayResumeAdmission)
		if err == nil || !reflect.DeepEqual(got, plan.ReplayResumeAdmission) || owner.requires != 2 || owner.writes != 0 {
			t.Fatalf("partial readback discharged complete inventory: admission=%+v owner=%+v err=%v", got, owner, err)
		}
		owner.rows[projected[1].activation.Ref.ActivationID] = projected[1].activation
		got, err = requireMaterializedRunForkWorkflowTimers(ctx, attempt, plan, workflowTimerProjectionChildRun, selection, owner, bornAt, plan.ReplayResumeAdmission)
		if err != nil || len(got.UnsupportedBlockers) != 0 || owner.requires != 4 || owner.writes != 0 {
			t.Fatalf("complete require-only inventory failed: admission=%+v owner=%+v err=%v", got, owner, err)
		}
	})
}

func TestRunForkWorkflowTimerAdmissionReadbackRefusesCorruptionAndPreservesOtherBlockers(t *testing.T) {
	source := workflowTimerProjectionSource(t, true)
	plan := workflowTimerHistoryPlan(t, source)
	bornAt := source.CreatedAt.Add(2 * time.Hour)
	selection := workflowTimerMaterializerSelection{}
	projected, err := prepareRunForkWorkflowTimers(plan, workflowTimerProjectionChildRun, selection, bornAt)
	if err != nil {
		t.Fatal(err)
	}
	expected := projected[0].activation
	for _, name := range []string{"missing", "due", "payload", "source", "selected_effect", "removed_not_cancelled", "missing_cause"} {
		t.Run(name, func(t *testing.T) {
			actual := expected.Canonical()
			selected := selection
			switch name {
			case "due":
				actual.FireAt = actual.FireAt.Add(time.Second)
			case "payload":
				actual.Payload = []byte(`{"changed":true}`)
			case "source":
				actual.SourceTimerID = "88888888-8888-4888-8888-888888888888"
			case "selected_effect":
				actual.EventType = "timer.other"
			case "removed_not_cancelled", "missing_cause":
				selected.removed = map[string]bool{source.Ref.DeclarationKey: true}
				rows, err := prepareRunForkWorkflowTimers(plan, workflowTimerProjectionChildRun, selected, bornAt)
				if err != nil {
					t.Fatal(err)
				}
				actual = rows[0].activation
				if name == "missing_cause" {
					actual.Status = "cancelled"
				}
			}
			owner := &workflowTimerMaterializerOwner{rows: map[string]pipeline.WorkflowTimerActivation{actual.Ref.ActivationID: actual}}
			if name == "missing" {
				owner.rows = nil
			}
			withWorkflowTimerReadbackAttempt(t, func(ctx context.Context, attempt *mutationprotocol.Attempt) {
				admitted, err := requireMaterializedRunForkWorkflowTimers(ctx, attempt, plan, workflowTimerProjectionChildRun,
					selected, owner, bornAt, plan.ReplayResumeAdmission)
				if err == nil || !reflect.DeepEqual(admitted, plan.ReplayResumeAdmission) || owner.writes != 0 {
					t.Fatalf("corrupt readback discharged or repaired history: admitted=%+v writes=%d err=%v", admitted, owner.writes, err)
				}
			})
		})
	}
	owner := &workflowTimerMaterializerOwner{rows: map[string]pipeline.WorkflowTimerActivation{expected.Ref.ActivationID: expected}}
	admission := runForkReplayResumeAdmissionWithBlocker(plan.ReplayResumeAdmission, runfork.RunForkReplayResumeFactOpenReplyContext,
		runForkReplayResumeBlocker(runfork.RunForkBlockerOpenReplyContextUnsupported))
	withWorkflowTimerReadbackAttempt(t, func(ctx context.Context, attempt *mutationprotocol.Attempt) {
		got, err := requireMaterializedRunForkWorkflowTimers(ctx, attempt, plan, workflowTimerProjectionChildRun, selection, owner, bornAt, admission)
		if err != nil || len(got.UnsupportedBlockers) != 1 || got.UnsupportedBlockers[0].Code != runfork.RunForkBlockerOpenReplyContextUnsupported || got.StateOnlyExecutionReady {
			t.Fatalf("timer discharge changed unrelated reply refusal: %+v err=%v", got, err)
		}
	})
}

func TestRunForkWorkflowTimerAdmissionRequiresExactLiveFrameIncludingTerminalOnly(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "terminal"}[terminal], func(t *testing.T) {
			source := workflowTimerProjectionSource(t, true)
			if terminal {
				source = workflowTimerHistoryTerminal(t, "fired", "66666666-6666-4666-8666-666666666666")
			}
			plan := workflowTimerHistoryPlan(t, source)
			bornAt := source.CreatedAt.Add(2 * time.Hour)
			owner := &workflowTimerMaterializerOwner{rows: make(map[string]pipeline.WorkflowTimerActivation)}
			selection := workflowTimerMaterializerSelection{}
			var staleCtx context.Context
			var staleAttempt *mutationprotocol.Attempt
			withWorkflowTimerReadbackAttempt(t, func(ctx context.Context, attempt *mutationprotocol.Attempt) {
				staleCtx, staleAttempt = ctx, attempt
				detached := correlation.WithRunID(context.Background(), workflowTimerProjectionChildRun)
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				for _, candidate := range []context.Context{detached, cancelled, correlation.WithRunID(ctx, plan.SourceRunID)} {
					if got, err := requireMaterializedRunForkWorkflowTimers(candidate, attempt, plan, workflowTimerProjectionChildRun,
						selection, owner, bornAt, plan.ReplayResumeAdmission); err == nil || !reflect.DeepEqual(got, plan.ReplayResumeAdmission) {
						t.Fatal("detached, cancelled, or foreign child context acquired timer discharge")
					}
				}
				withWorkflowTimerReadbackAttempt(t, func(_ context.Context, foreign *mutationprotocol.Attempt) {
					if _, err := requireMaterializedRunForkWorkflowTimers(ctx, foreign, plan, workflowTimerProjectionChildRun,
						selection, owner, bornAt, plan.ReplayResumeAdmission); err == nil {
						t.Fatal("foreign attempt borrowed another native frame")
					}
				})
			})
			if _, err := requireMaterializedRunForkWorkflowTimers(staleCtx, staleAttempt, plan, workflowTimerProjectionChildRun,
				selection, owner, bornAt, plan.ReplayResumeAdmission); err == nil || owner.requires != 0 || owner.writes != 0 {
				t.Fatal("stale frame acquired discharge or refused frame reached the owner")
			}
		})
	}
}
