package runtimepersistence

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

func TestClockScheduleCarrierPersistsAndReplaysOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, _, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			var ids []string
			for _, flow := range []string{".", "account/poller", "other/poller"} {
				instance, eventType := flow, flow+"/poll.tick"
				if flow == "." {
					instance, eventType = runID, "poll.tick"
				}
				source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: flow, FlowInstance: instance})
				if err != nil {
					t.Fatal(err)
				}
				command := genericschedule.AdmissionCommand{ScheduleKey: "poll", RunID: runID, FlowInstance: instance,
					OwnerKind: genericschedule.OwnerInstance, OwnerID: flow, EventType: eventType,
					Payload: semanticvalue.EmptyObject(), RoutingSource: source, ExecutionMode: executionmode.Live, Due: genericschedule.EveryDue(time.Minute)}
				activation := admitGenericScheduleFixture(t, ctx, store, command)
				ids = append(ids, activation.ID)
				loaded, found, err := store.LoadGenericScheduleActivation(ctx, activation.ID)
				if err != nil || !found || loaded.Command.RoutingSource != source || loaded.Command.OwnerKind != genericschedule.OwnerInstance || loaded.ImmutableHash != activation.ImmutableHash {
					t.Fatalf("readback lost instance authority: %#v, %v", loaded, err)
				}
				replay, err := store.AdmitGenericScheduleOutcome(ctx, command)
				if err != nil || !replay.Acknowledged || replay.Result.Outcome != genericschedule.AdmissionExactReplay || replay.Result.Activation.ID != activation.ID || !replay.Result.Activation.CurrentDueAt.Equal(activation.CurrentDueAt) {
					t.Fatalf("replay rearmed instance schedule: %#v, %v", replay, err)
				}
				changed := command
				changed.Due = genericschedule.EveryDue(2 * time.Minute)
				if _, err := store.AdmitGenericScheduleOutcome(ctx, changed); !genericschedule.IsConflict(err) {
					t.Fatalf("changed content did not conflict: %v", err)
				}
				cancelGenericScheduleFixture(t, ctx, store, activation, "clock_removed", activation.AdmittedAt.Add(time.Second))
			}
			if ids[0] == ids[1] || ids[1] == ids[2] || ids[0] == ids[2] {
				t.Fatalf("sibling clock names collapsed: %v", ids)
			}
		})
	}
}

func TestClockScheduleOccurrenceAtomicReplayOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, _, seedCtx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(seedCtx)
			ctx := authorGenericScheduleConsumerContext(runID)
			registerTestAuthorActivityCatalogForContext(t, store.(testAuthorActivityCatalogRegistrar), testAuthorActivityContext())
			selected := store.(storeTestDurableEventBusStore)
			publisher, err := newStoreTestEventBus(t, selected)
			if err != nil {
				t.Fatal(err)
			}
			source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: runID})
			if err != nil {
				t.Fatal(err)
			}
			setGenericScheduleClock(t, store, func() time.Time { return time.Now().UTC().Add(-time.Hour) })
			command := genericschedule.AdmissionCommand{ScheduleKey: "poll", RunID: runID, FlowInstance: runID,
				OwnerKind: genericschedule.OwnerInstance, OwnerID: ".", EventType: "test.node_emitted",
				Payload: semanticvalue.EmptyObject(), RoutingSource: source, ExecutionMode: executionmode.Live, Due: genericschedule.EveryDue(time.Minute)}
			activation := admitGenericScheduleFixture(t, ctx, store, command)
			setGenericScheduleClock(t, store, time.Now)
			wakeup, err := activation.Wakeup()
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := store.PrepareGenericScheduleOccurrence(ctx, wakeup)
			if err != nil || !prepared.Acknowledged || prepared.Result.Outcome != genericschedule.PrepareReady {
				t.Fatalf("prepare occurrence: %#v, %v", prepared, err)
			}
			occurrence := prepared.Result.Occurrence
			event, err := events.NewInstancePublicationEvent(events.InstancePublicationEventInput{RunID: runID, Facts: events.EventFacts{
				ID: occurrence.EventID, Type: events.EventType(command.EventType), Producer: events.ProducerClaim{Type: events.EventProducerInstance, ID: "."},
				Payload: []byte(`{}`), Envelope: events.EventEnvelope{FlowInstance: runID}, RoutingSource: source,
				CreatedAt: occurrence.DueAt, ExecutionMode: executionmode.Live,
			}})
			if err != nil {
				t.Fatal(err)
			}
			for replay := 0; replay < 2; replay++ {
				plans, err := publisher.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: event}})
				if err != nil || len(plans) != 1 {
					t.Fatalf("prepare business publication: plans=%d err=%v", len(plans), err)
				}
				result, err := store.CommitGenericScheduleOccurrence(ctx, genericschedule.CommitCommand{Activation: prepared.Result.Activation, Occurrence: occurrence, Publication: plans[0]})
				if err != nil || result.Outcome != genericschedule.CommitCommitted || result.PublicationAlreadyCommitted != (replay == 1) {
					t.Fatalf("commit/replay %d: %#v, %v", replay, result, err)
				}
				if !result.Next.CurrentDueAt.Equal(occurrence.DueAt.Add(time.Minute)) {
					t.Fatal("occurrence advancement was repeated")
				}
				if err := publisher.ReleaseEnginePublications(ctx, plans); err != nil {
					t.Fatal(err)
				}
			}
			read, found, err := selected.(bus.PreparedPublishEventReader).LoadPreparedPublishEvent(ctx, event.ID())
			if err != nil || !found || read.Event.Event().RoutingSource() != source || read.Event.Class() != events.EventAdmissionInstancePublication || read.Event.Event().ParentEventID() != "" || len(read.DeliveryRoutes) != 0 {
				t.Fatalf("durable instance readback: %#v, %v", read, err)
			}
			public, err := store.(interface {
				LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
			}).LoadOperatorEvent(ctx, event.ID())
			if err != nil || public.EventID != event.ID() {
				t.Fatalf("operator readback: %#v, %v", public, err)
			}
			recipients, err := selected.ListEventDeliveryRecipients(ctx, event.ID())
			if err != nil || len(recipients) != 0 {
				t.Fatalf("zero-consumer carrier invented deliveries: %v, %v", recipients, err)
			}
		})
	}
}

func TestClockScheduleInvalidCadenceCannotArmOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, db, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: runID})
			if err != nil {
				t.Fatal(err)
			}
			command := genericschedule.AdmissionCommand{ScheduleKey: "poll", RunID: runID, FlowInstance: runID,
				OwnerKind: genericschedule.OwnerInstance, OwnerID: ".", EventType: "poll.tick",
				Payload: semanticvalue.EmptyObject(), RoutingSource: source, ExecutionMode: executionmode.Live}
			for _, due := range []genericschedule.DueBasis{
				genericschedule.EveryDue(time.Nanosecond), genericschedule.EveryDue(1500 * time.Nanosecond), genericschedule.CronDue("0 0 31 2 *"),
			} {
				command.Due = due
				if _, err := store.AdmitGenericScheduleOutcome(ctx, command); err == nil {
					t.Fatalf("invalid cadence armed: %#v", due)
				}
			}
			setGenericScheduleClock(t, store, func() time.Time { return time.Date(2097, 3, 1, 0, 0, 0, 0, time.UTC) })
			command.Due = genericschedule.CronDue("0 0 29 2 *")
			if _, err := store.AdmitGenericScheduleOutcome(ctx, command); err == nil {
				t.Fatal("missing next occurrence was durably armed")
			}
			var count int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM timers").Scan(&count); err != nil || count != 0 {
				t.Fatalf("refusal mutated timer rows: count=%d err=%v", count, err)
			}
		})
	}
}

func TestClockScheduleRestoreTerminalizesMalformedCadenceOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		for _, hostile := range []struct{ name, duration, cron, reason string }{
			{name: "stationary", duration: "1ns", reason: "persisted microseconds"},
			{name: "rounded", duration: "1.5us", reason: "persisted microseconds"},
			{name: "impossible cron", cron: "0 0 31 2 *", reason: "executable future occurrence"},
		} {
			t.Run(tc.name+"/"+hostile.name, func(t *testing.T) {
				store, db, ctx := tc.open(t)
				runID := runtimecorrelation.RunIDFromContext(ctx)
				source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: runID})
				if err != nil {
					t.Fatal(err)
				}
				command := genericschedule.AdmissionCommand{ScheduleKey: "poll", RunID: runID, FlowInstance: runID,
					OwnerKind: genericschedule.OwnerInstance, OwnerID: ".", EventType: "poll.tick",
					Payload: semanticvalue.EmptyObject(), RoutingSource: source, ExecutionMode: executionmode.Live, Due: genericschedule.EveryDue(time.Minute)}
				if hostile.cron != "" {
					command.Due = genericschedule.CronDue("0 9 * * *")
				}
				activation := admitGenericScheduleFixture(t, ctx, store, command)
				column, value := "due_basis_duration", hostile.duration
				if hostile.cron != "" {
					column, value = "due_basis_cron", hostile.cron
				}
				query := "UPDATE timers SET " + column + " = ? WHERE timer_id = ?"
				if _, ok := store.(*PostgresStore); ok {
					query = "UPDATE timers SET " + column + " = $1 WHERE timer_id = $2::uuid"
				}
				if _, err := db.ExecContext(ctx, query, value, activation.ID); err != nil {
					t.Fatal(err)
				}
				scheduler := &selectedStoreLifecycleScheduler{}
				lifecycle, err := genericschedule.NewLifecycle(store, scheduler, &terminalSchedulePlannerProbe{}, &terminalScheduleDispatcherProbe{}, nil, executionposture.Live)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = lifecycle.Stop(context.Background()) })
				if restored, err := lifecycle.Restore(ctx); err != nil || restored != 0 || len(scheduler.registered) != 0 {
					t.Fatalf("invalid persisted cadence registered wakeups: restored=%d registered=%v err=%v", restored, scheduler.registered, err)
				}
				query = "SELECT " + column + ", status, immutable_hash, failure_code, failure_message FROM timers WHERE timer_id = ?"
				if _, ok := store.(*PostgresStore); ok {
					query = "SELECT " + column + ", status, immutable_hash, failure_code, failure_message FROM timers WHERE timer_id = $1::uuid"
				}
				var persisted, status, hash, code, message string
				if err := db.QueryRowContext(ctx, query, activation.ID).Scan(&persisted, &status, &hash, &code, &message); err != nil {
					t.Fatal(err)
				}
				if persisted != value || status != string(genericschedule.StatusFailed) || hash != activation.ImmutableHash || code != "malformed_persisted_activation" || !strings.Contains(message, hostile.reason) {
					t.Fatalf("malformed clock was not terminalized with exact evidence: value=%q status=%q hash=%q code=%q message=%q", persisted, status, hash, code, message)
				}
				requireClockTimerRetention(t, ctx, store, runID, 0)
			})
		}
	}
}

func TestClockScheduleTerminalRunCannotAdmitOrFireOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, _, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: runID})
			if err != nil {
				t.Fatal(err)
			}
			command := genericschedule.AdmissionCommand{ScheduleKey: "poll", RunID: runID, FlowInstance: runID,
				OwnerKind: genericschedule.OwnerInstance, OwnerID: ".", EventType: "poll.tick",
				Payload: semanticvalue.EmptyObject(), RoutingSource: source, ExecutionMode: executionmode.Live, Due: genericschedule.EveryDue(time.Minute)}
			activation := admitGenericScheduleFixture(t, ctx, store, command)
			wakeup, err := activation.Wakeup()
			if err != nil {
				t.Fatal(err)
			}
			transitionGenericScheduleRun(t, store, runID, true)
			result, err := store.PrepareGenericScheduleOccurrence(ctx, wakeup)
			if err != nil || !result.Acknowledged || result.Result.Outcome != genericschedule.PrepareStaleCancelled || result.Result.Activation.Status != genericschedule.StatusCancelled {
				t.Fatalf("stopped clock can fire: %#v, %v", result, err)
			}
			command.ScheduleKey = "new_after_stop"
			if _, err := store.AdmitGenericScheduleOutcome(ctx, command); err == nil {
				t.Fatal("stopped run admitted a new instance clock")
			}
		})
	}
}
