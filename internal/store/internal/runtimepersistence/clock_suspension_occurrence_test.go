package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	privategenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func TestClockParkingResumeCannotInventOccurrenceAcceptanceBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		for _, accepted := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: "/unaccepted", true: "/accepted"}[accepted], func(t *testing.T) {
				selected, _, seedCtx := tc.open(t)
				runID := runtimecorrelation.RunIDFromContext(seedCtx)
				ctx := authorGenericScheduleConsumerContext(runID)
				registerTestAuthorActivityCatalogForContext(t, selected.(testAuthorActivityCatalogRegistrar), testAuthorActivityContext())
				publisher, err := newStoreTestEventBus(t, selected.(storeTestDurableEventBusStore))
				if err != nil {
					t.Fatal(err)
				}
				source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: runID})
				if err != nil {
					t.Fatal(err)
				}
				setGenericScheduleClock(t, selected, func() time.Time { return time.Now().UTC().Add(-time.Hour) })
				command := genericschedule.AdmissionCommand{ScheduleKey: "poll", RunID: runID, FlowInstance: runID,
					OwnerKind: genericschedule.OwnerInstance, OwnerID: ".", EventType: "test.node_emitted",
					Payload: semanticvalue.EmptyObject(), RoutingSource: source, ExecutionMode: executionmode.Live, Due: genericschedule.EveryDue(time.Minute)}
				activation := admitGenericScheduleFixture(t, ctx, selected, command)
				setGenericScheduleClock(t, selected, time.Now)
				wake, err := activation.Wakeup()
				if err != nil {
					t.Fatal(err)
				}
				prepared, err := selected.PrepareGenericScheduleOccurrence(ctx, wake)
				if err != nil || !prepared.Acknowledged || prepared.Result.Outcome != genericschedule.PrepareReady {
					t.Fatalf("prepare clock occurrence=%+v err=%v", prepared, err)
				}
				occurrence := prepared.Result.Occurrence
				event, err := events.NewInstancePublicationEvent(events.InstancePublicationEventInput{RunID: runID, Facts: events.EventFacts{
					ID: occurrence.EventID, Type: events.EventType(command.EventType), Producer: events.ProducerClaim{Type: events.EventProducerInstance, ID: "."},
					Payload: []byte(`{}`), Envelope: events.EventEnvelope{FlowInstance: runID}, RoutingSource: source, CreatedAt: occurrence.DueAt, ExecutionMode: executionmode.Live,
				}})
				if err != nil {
					t.Fatal(err)
				}
				plans, err := publisher.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: event}})
				if err != nil || len(plans) != 1 {
					t.Fatalf("prepare ordinary clock publication: %v", err)
				}
				t.Cleanup(func() {
					if err := publisher.ReleaseEnginePublications(ctx, plans); err != nil {
						t.Error(err)
					}
				})
				commit := genericschedule.CommitCommand{Activation: prepared.Result.Activation, Occurrence: occurrence, Publication: plans[0]}
				if accepted {
					result, err := selected.CommitGenericScheduleOccurrence(ctx, commit)
					if err != nil || result.Outcome != genericschedule.CommitCommitted || result.PublicationAlreadyCommitted {
						t.Fatalf("accept original occurrence=%+v err=%v", result, err)
					}
				}
				at := time.Now().UTC()
				for _, resume := range []bool{false, true} {
					transitionClockLifetimeForTest(t, ctx, selected, runID, at, resume)
					before, found, err := selected.LoadGenericScheduleActivation(ctx, activation.ID)
					if err != nil || !found {
						t.Fatalf("read clock before stale commit: %v", err)
					}
					result, err := selected.CommitGenericScheduleOccurrence(ctx, commit)
					if err != nil {
						t.Fatal(err)
					}
					if accepted {
						if result.Outcome != genericschedule.CommitCommitted || !result.PublicationAlreadyCommitted || result.Next.Status != before.Status {
							t.Fatalf("known acceptance lost during parking/resume=%+v", result)
						}
					} else if result.Outcome != genericschedule.CommitTerminal || result.Publication != nil {
						t.Fatalf("rearming became evidence of unaccepted publication=%+v", result)
					}
					after, found, err := selected.LoadGenericScheduleActivation(ctx, activation.ID)
					if err != nil || !found || !reflect.DeepEqual(before, after) {
						t.Fatalf("old occurrence mutated the clock: before=%+v after=%+v err=%v", before, after, err)
					}
					_, found, err = selected.(bus.PreparedPublishEventReader).LoadPreparedPublishEvent(ctx, occurrence.EventID)
					if err != nil || found != accepted {
						t.Fatalf("stale occurrence acceptance changed: found=%t wanted=%t err=%v", found, accepted, err)
					}
					at = at.Add(time.Hour)
				}
			})
		}
	}
}

func transitionClockLifetimeForTest(t *testing.T, ctx context.Context, selected selectedScheduleStore, runID string, at time.Time, resume bool) {
	t.Helper()
	change := func(ctx context.Context, attempt *mutationprotocol.Attempt, postgres bool) (struct{}, error) {
		if resume {
			return struct{}{}, privategenericschedule.ResumeClockRunTx(ctx, attempt, postgres, runID, at)
		}
		_, err := privategenericschedule.ParkClockRunsTx(ctx, attempt, postgres, []string{runID}, "operator_suspend", at)
		return struct{}{}, err
	}
	var outcome mutationprotocol.Result[struct{}]
	if native, ok := selected.(*PostgresStore); ok {
		outcome = mutationprotocol.RunPostgres(ctx, native.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			return change(ctx, attempt, true)
		})
	} else {
		native := selected.(*SQLiteRuntimeStore)
		outcome = mutationprotocol.RunSQLite(ctx, native.backend, "test clock lifetime transition", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			return change(ctx, attempt, false)
		})
	}
	if err := outcome.Err(); err != nil || !outcome.Acknowledged() {
		t.Fatalf("clock lifetime transition: acknowledged=%t err=%v", outcome.Acknowledged(), err)
	}
}
