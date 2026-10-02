package runtimepersistence

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/google/uuid"
)

func TestClockScheduleRunReadbackOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, _, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			reader := store.(interface {
				LoadRunClockSchedules(context.Context, string) ([]genericschedule.ClockReadback, error)
			})
			clocks, err := reader.LoadRunClockSchedules(ctx, runID)
			if err != nil || len(clocks) != 0 {
				t.Fatalf("read invented a clock binding: %#v, %v", clocks, err)
			}
			var activations []genericschedule.Activation
			for _, flow := range []string{".", "alpha/poller", "beta/poller"} {
				instance, emit := flow, flow+"/poll.tick"
				if flow == "." {
					instance, emit = runID, "poll.tick"
				}
				source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: flow, FlowInstance: instance})
				if err != nil {
					t.Fatal(err)
				}
				command := genericschedule.AdmissionCommand{ScheduleKey: "poll", RunID: runID, FlowInstance: instance,
					OwnerKind: genericschedule.OwnerInstance, OwnerID: flow, EventType: emit, Payload: semanticvalue.EmptyObject(),
					RoutingSource: source, ExecutionMode: executionmode.Live, Due: genericschedule.EveryDue(time.Minute)}
				activations = append(activations, admitGenericScheduleFixture(t, ctx, store, command))
			}
			admitGenericScheduleFixture(t, ctx, store, testAgentGenericScheduleCommand(t, runID, "agent", "agent/instance", uuid.NewString(), "poll", genericschedule.DelayDue(time.Hour)))
			for repeatedRead := 0; repeatedRead < 2; repeatedRead++ {
				clocks, err = reader.LoadRunClockSchedules(ctx, runID)
				if err != nil || len(clocks) != 3 {
					t.Fatalf("read omitted clocks: %#v, %v", clocks, err)
				}
				for i, view := range clocks {
					want, err := genericschedule.ProjectClockReadback(activations[i], true)
					if err != nil || !reflect.DeepEqual(view, want) {
						t.Fatalf("read fabricated clock facts: got=%#v want=%#v err=%v", view, want, err)
					}
					loaded, found, err := store.LoadGenericScheduleActivation(ctx, view.ActivationID)
					if err != nil || !found || !reflect.DeepEqual(loaded, activations[i]) {
						t.Fatalf("read changed persisted clock: %#v, %v", loaded, err)
					}
				}
			}
			cancelGenericScheduleFixture(t, ctx, store, activations[1], "clock_removed", activations[1].AdmittedAt.Add(time.Second))
			clocks, err = reader.LoadRunClockSchedules(ctx, runID)
			if err != nil || len(clocks) != 3 {
				t.Fatalf("cancelled clock disappeared from readback: %#v, %v", clocks, err)
			}
			cancelled := clocks[1]
			if cancelled.Status != genericschedule.StatusCancelled || cancelled.NextDueAt != nil || cancelled.RetainsRun || cancelled.CancelCause != "clock_removed" || !clocks[2].RetainsRun {
				t.Fatalf("cancellation corrupted sibling retention: %#v", clocks)
			}
			scope, err := timerobligation.Run(runID)
			if err != nil {
				t.Fatal(err)
			}
			timers, err := store.(timerobligation.Reader).ReadTimerObligations(ctx, scope, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			runTimers, found := timers.Run(runID)
			if !found || runTimers.Totals().ActiveCount != 3 || !runTimers.Summary(timers.ObservedAt).BlocksCompletion() {
				t.Fatalf("clock readback disagrees with canonical retention owner: %#v", timers)
			}
		})
	}
}

func TestClockScheduleReadbackRefusesCorruptEvidenceWithoutMutationOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, db, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: runID})
			if err != nil {
				t.Fatal(err)
			}
			activation := admitGenericScheduleFixture(t, ctx, store, genericschedule.AdmissionCommand{
				ScheduleKey: "poll", RunID: runID, FlowInstance: runID, OwnerKind: genericschedule.OwnerInstance, OwnerID: ".",
				EventType: "poll.tick", Payload: semanticvalue.EmptyObject(), RoutingSource: source,
				ExecutionMode: executionmode.Live, Due: genericschedule.EveryDue(time.Minute),
			})
			query := "UPDATE timers SET immutable_hash = 'corrupt' WHERE timer_id = ?"
			if tc.name == "postgres" {
				query = "UPDATE timers SET immutable_hash = 'corrupt' WHERE timer_id = $1::uuid"
			}
			if _, err := db.ExecContext(ctx, query, activation.ID); err != nil {
				t.Fatal(err)
			}
			reader := store.(interface {
				LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
				LoadRunOrigin(context.Context, string) (runlifecycle.RunOrigin, error)
				LoadRunClockSchedules(context.Context, string) ([]genericschedule.ClockReadback, error)
			})
			if _, err := reader.LoadRunHeader(ctx, runID); err != nil {
				t.Fatalf("basic header depends on clock history: %v", err)
			}
			if _, err := reader.LoadRunOrigin(ctx, runID); err != nil {
				t.Fatalf("run origin depends on clock history: %v", err)
			}
			_, err = reader.LoadRunClockSchedules(ctx, runID)
			if err == nil || !strings.Contains(err.Error(), "immutable hash") {
				t.Fatalf("corrupt clock rendered as admitted: %v", err)
			}
			var status, hash string
			if err := db.QueryRowContext(ctx, "SELECT status, immutable_hash FROM timers").Scan(&status, &hash); err != nil || status != "active" || hash != "corrupt" {
				t.Fatalf("read repaired or terminalized corrupt evidence: status=%q hash=%q err=%v", status, hash, err)
			}
		})
	}
}
