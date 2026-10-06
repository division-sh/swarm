package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

func TestClockStorageObservationIsClosedAndExactBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			selected, _, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: runID})
			if err != nil {
				t.Fatal(err)
			}
			command := genericschedule.AdmissionCommand{ScheduleKey: "poll", RunID: runID, FlowInstance: runID,
				OwnerKind: genericschedule.OwnerInstance, OwnerID: ".", EventType: "poll.tick", Payload: semanticvalue.EmptyObject(),
				RoutingSource: source, ExecutionMode: executionmode.Live, Due: genericschedule.EveryDue(time.Minute)}
			activation := admitGenericScheduleFixture(t, ctx, selected, command)
			before, err := ReadClockStorageForTest(ctx, selected, runID, activation.ID)
			if err != nil || before.Rows != 1 || before.Status != "active" || before.ImmutableHash != activation.ImmutableHash {
				t.Fatalf("exact physical clock=%+v err=%v", before, err)
			}
			for _, foreign := range []struct{ run, id string }{{uuid.NewString(), activation.ID}, {runID, uuid.NewString()}, {"invalid", activation.ID}} {
				if err := CorruptClockImmutableHashForTest(ctx, selected, foreign.run, foreign.id); err == nil {
					t.Fatal("fault mutated a missing or foreign clock")
				}
				out, err := ReadClockStorageForTest(ctx, selected, foreign.run, foreign.id)
				if err == nil || !reflect.DeepEqual(out, ClockStorageObservation{}) {
					t.Fatalf("foreign read leaked partial evidence=%+v err=%v", out, err)
				}
			}
			after, err := ReadClockStorageForTest(ctx, selected, runID, activation.ID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("refused faults changed original clock=%+v err=%v", after, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			for _, owner := range []any{struct{}{}, nil} {
				if err := CorruptClockImmutableHashForTest(ctx, owner, runID, activation.ID); err == nil {
					t.Fatal("fault accepted a non-native owner")
				}
			}
			out, err := ReadClockStorageForTest(cancelled, selected, runID, activation.ID)
			if err == nil || !reflect.DeepEqual(out, ClockStorageObservation{}) {
				t.Fatalf("cancelled read leaked evidence=%+v err=%v", out, err)
			}
			if err := CorruptClockImmutableHashForTest(ctx, selected, runID, activation.ID); err != nil {
				t.Fatal(err)
			}
			out, err = ReadClockStorageForTest(ctx, selected, runID, activation.ID)
			if err != nil || out.Rows != 1 || out.Status != "active" || out.ImmutableHash != "corrupt" {
				t.Fatalf("physical corruption was normalized or repaired=%+v err=%v", out, err)
			}
		})
	}
}
