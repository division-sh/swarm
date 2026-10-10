package runforkpersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

type arrivalJoinInventoryOwner struct {
	rows  []genericschedule.Activation
	reads int
	err   error
}

func (o *arrivalJoinInventoryOwner) MaterializeRunForkArrivalJoinScheduleTx(context.Context, *mutationprotocol.Attempt, storegenericschedule.ForkJoinRequest) (genericschedule.Activation, error) {
	return genericschedule.Activation{}, errors.New("readback must not materialize")
}

func (o *arrivalJoinInventoryOwner) ReadRunForkArrivalJoinScheduleInventoryTx(ctx context.Context, _ *mutationprotocol.Attempt, _ string) ([]genericschedule.Activation, error) {
	o.reads++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return o.rows, o.err
}

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

func TestForkArrivalInventoryRequiresEveryInheritedRow(t *testing.T) {
	for _, root := range []bool{false, true} {
		for _, completion := range []bool{false, true} {
			plan, bornAt, requests, rows := arrivalJoinInventoryFixture(t, root, completion)
			for _, name := range []string{"exact", "missing", "extra", "duplicate", "foreign", "partial_origin", "ordinary", "changed_cut", "duplicate_request"} {
				t.Run(map[bool]string{false: "flow/", true: "root/"}[root]+map[bool]string{false: "timeout/", true: "completion/"}[completion]+name, func(t *testing.T) {
					actual := make([]genericschedule.Activation, len(rows))
					for i, row := range rows {
						actual[i] = row.Canonical()
					}
					wanted := append([]storegenericschedule.ForkJoinRequest(nil), requests...)
					switch name {
					case "missing":
						actual = actual[1:]
					case "extra":
						actual = append(actual, actual[0].Canonical())
						actual[len(actual)-1].ID = uuid.NewString()
					case "duplicate":
						actual = append(actual, actual[0])
					case "foreign":
						actual[0].Command.RunID = uuid.NewString()
					case "partial_origin":
						actual[0].ForkJoinOrigin.SourceActivationID = ""
					case "ordinary":
						actual[0].ForkJoinOrigin = nil
					case "changed_cut":
						actual[0].ForkJoinOrigin.PointRevision++
					case "duplicate_request":
						wanted = append(wanted, wanted[0])
						actual = append(actual, actual[0])
					}
					for _, phase := range []runForkArrivalScheduleReadbackPhase{runForkArrivalScheduleAtCut, runForkArrivalScheduleContinuing} {
						err := requireExactRunForkArrivalJoinInventory(wanted, actual, phase)
						if (err == nil) != (name == "exact") {
							t.Fatalf("phase=%d %s: err=%v", phase, name, err)
						}
					}
					if name == "exact" {
						owner := &arrivalJoinInventoryOwner{rows: actual}
						withWorkflowTimerReadbackAttempt(t, func(ctx context.Context, attempt *mutationprotocol.Attempt) {
							if err := requireMaterializedRunForkArrivalJoinSchedules(ctx, attempt, plan, workflowTimerProjectionChildRun, bornAt, owner); err != nil {
								t.Fatal(err)
							}
							if err := requireContinuingRunForkArrivalJoinSchedules(ctx, attempt, plan, workflowTimerProjectionChildRun, bornAt, owner); err != nil {
								t.Fatal(err)
							}
						})
						if owner.reads != 2 {
							t.Fatal("readback skipped complete native inventory")
						}
					}
				})
			}
		}
	}
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

func TestForkArrivalInventoryReadbackRequiresNativeFrameAndEmptyCensus(t *testing.T) {
	plan, bornAt, _, rows := arrivalJoinInventoryFixture(t, true, false)
	plan.Entities, plan.JoinSchedules = nil, nil
	owner := &arrivalJoinInventoryOwner{rows: rows}
	withWorkflowTimerReadbackAttempt(t, func(ctx context.Context, attempt *mutationprotocol.Attempt) {
		if err := requireMaterializedRunForkArrivalJoinSchedules(ctx, attempt, plan, workflowTimerProjectionChildRun, bornAt, owner); err == nil {
			t.Fatal("empty expected set hid extra inherited rows")
		}
		owner.rows = nil
		if err := requireMaterializedRunForkArrivalJoinSchedules(ctx, attempt, plan, workflowTimerProjectionChildRun, bornAt, owner); err != nil {
			t.Fatal(err)
		}
		for _, bad := range []context.Context{context.Background(), correlation.WithRunID(ctx, uuid.NewString())} {
			before := owner.reads
			if err := requireMaterializedRunForkArrivalJoinSchedules(bad, attempt, plan, workflowTimerProjectionChildRun, bornAt, owner); err == nil || owner.reads != before {
				t.Fatal("foreign frame reached readback")
			}
		}
		if err := requireMaterializedRunForkArrivalJoinSchedules(ctx, attempt, plan, workflowTimerProjectionChildRun, bornAt, nil); err == nil {
			t.Fatal("empty inventory skipped canonical owner")
		}
		if err := requireRunForkArrivalJoinInventory(ctx, attempt, plan, workflowTimerProjectionChildRun, bornAt, owner, 0); err == nil {
			t.Fatal("unclassified lifecycle phase accepted")
		}
	})
	if err := requireMaterializedRunForkArrivalJoinSchedules(context.Background(), nil, plan, workflowTimerProjectionChildRun, bornAt, owner); err == nil {
		t.Fatal("missing native attempt accepted")
	}
}
