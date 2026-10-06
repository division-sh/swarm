package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

func TestInstanceClockHandoffRequiresAcknowledgmentAndExactBusBothStores(t *testing.T) {
	for _, backend := range selectedScheduleStoreCases() {
		t.Run(backend.name, func(t *testing.T) {
			selected, _, ctx := backend.open(t)
			runID := correlation.RunIDFromContext(ctx)
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
			activation := admitGenericScheduleFixture(t, ctx, selected, genericschedule.AdmissionCommand{
				ScheduleKey: "poll", RunID: runID, FlowInstance: runID, OwnerKind: genericschedule.OwnerInstance, OwnerID: ".",
				EventType: "test.node_emitted", Payload: semanticvalue.EmptyObject(), RoutingSource: source,
				ExecutionMode: executionmode.Live, Due: genericschedule.EveryDue(time.Minute),
			})
			setGenericScheduleClock(t, selected, time.Now)
			wake, err := activation.Wakeup()
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := selected.PrepareGenericScheduleOccurrence(ctx, wake)
			if err != nil || !prepared.Acknowledged || prepared.Result.Outcome != genericschedule.PrepareReady {
				t.Fatalf("prepare instance occurrence=%+v err=%v", prepared, err)
			}
			occurrence := prepared.Result.Occurrence
			event, err := events.NewInstancePublicationEvent(events.InstancePublicationEventInput{RunID: runID, Facts: events.EventFacts{
				ID: occurrence.EventID, Type: "test.node_emitted", Producer: events.ProducerClaim{Type: events.EventProducerInstance, ID: "."},
				Payload: []byte(`{}`), Envelope: events.EventEnvelope{FlowInstance: runID}, RoutingSource: source,
				CreatedAt: occurrence.DueAt, ExecutionMode: executionmode.Live,
			}})
			if err != nil {
				t.Fatal(err)
			}
			plans, err := publisher.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: event}})
			if err != nil || len(plans) != 1 {
				t.Fatalf("prepare publication=%+v err=%v", plans, err)
			}
			dispatch := publisher.EngineDispatcher().(engine.CommittedPublicationDispatcher)
			unacknowledged, err := bus.NewCommittedEnginePublication(plans[0].(bus.EnginePublicationPlan), bus.CommittedPublication{AppendOutcome: bus.EventAppendInserted})
			if err != nil {
				t.Fatal(err)
			}
			if err := dispatch.DispatchCommittedPublication(ctx, unacknowledged); err == nil {
				t.Fatal("transaction-local publication evidence acquired handoff authority")
			}
			if _, found, err := selected.(bus.PreparedPublishEventReader).LoadPreparedPublishEvent(ctx, event.ID()); err != nil || found {
				t.Fatalf("unacknowledged evidence published: found=%t err=%v", found, err)
			}
			result, err := selected.CommitGenericScheduleOccurrence(ctx, genericschedule.CommitCommand{Activation: prepared.Result.Activation, Occurrence: occurrence, Publication: plans[0]})
			if err != nil || result.Outcome != genericschedule.CommitCommitted {
				t.Fatalf("accept exact publication=%+v err=%v", result, err)
			}
			if err := dispatch.DispatchCommittedPublication(ctx, result.Publication); err == nil {
				t.Fatal("receipt bypassed required outbox finalization")
			}
			foreign, err := newStoreTestEventBus(t, selected.(storeTestDurableEventBusStore))
			if err != nil {
				t.Fatal(err)
			}
			if err := foreign.EngineDispatcher().(engine.CommittedPublicationDispatcher).DispatchCommittedPublication(ctx, result.Publication); err == nil {
				t.Fatal("a different bus borrowed the original publication claim")
			}
			if err := publisher.FinalizeEnginePublications(ctx, []engine.CommittedDurablePublication{result.Publication}); err != nil {
				t.Fatal(err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := dispatch.DispatchCommittedPublication(cancelled, result.Publication); err != nil {
				t.Fatalf("accepted receipt could not finish handoff after caller cancellation: %v", err)
			}
			if err := dispatch.DispatchCommittedPublication(ctx, result.Publication); err == nil {
				t.Fatal("spent outbox operation was reminted for a second handoff")
			}
			after, found, err := selected.LoadGenericScheduleActivation(ctx, activation.ID)
			if err != nil || !found || !reflect.DeepEqual(after, result.Next) {
				t.Fatalf("handoff replay changed clock evidence: after=%+v want=%+v err=%v", after, result.Next, err)
			}
		})
	}
}
