package runtimepersistence

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

func TestClockScheduleRootIdentityRefusalDoesNotMutateOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, db, ctx := tc.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			for _, instance := range []string{uuid.NewString(), uuid.NewSHA1(uuid.NameSpaceURL, []byte("clock-service/generation/1")).String(), "clock-service"} {
				source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: instance})
				if err != nil {
					t.Fatal(err)
				}
				command := genericschedule.AdmissionCommand{ScheduleKey: "poll", RunID: runID, FlowInstance: instance,
					OwnerKind: genericschedule.OwnerInstance, OwnerID: ".", EventType: "poll.tick",
					Payload: semanticvalue.EmptyObject(), RoutingSource: source, ExecutionMode: executionmode.Live, Due: genericschedule.EveryDue(time.Minute)}
				if _, err := store.AdmitGenericScheduleOutcome(ctx, command); err == nil || !strings.Contains(err.Error(), "root instance publication source must match its run") {
					t.Errorf("foreign root schedule did not fail exact-run admission: instance=%q err=%v", instance, err)
				}
				var count int
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM timers").Scan(&count); err != nil || count != 0 {
					t.Fatalf("root identity refusal mutated timers: count=%d err=%v", count, err)
				}
			}
			source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: runID})
			if err != nil {
				t.Fatal(err)
			}
			command := genericschedule.AdmissionCommand{ScheduleKey: "poll", RunID: runID, FlowInstance: runID,
				OwnerKind: genericschedule.OwnerInstance, OwnerID: ".", EventType: "poll.tick",
				Payload: semanticvalue.EmptyObject(), RoutingSource: source, ExecutionMode: executionmode.Live, Due: genericschedule.EveryDue(time.Minute)}
			missing := command
			missing.FlowInstance = ""
			if _, err := store.AdmitGenericScheduleOutcome(ctx, missing); err == nil {
				t.Fatal("root schedule without an instance was admitted")
			}
			var count int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM timers").Scan(&count); err != nil || count != 0 {
				t.Fatalf("missing-root refusal mutated timers: count=%d err=%v", count, err)
			}
			activation := admitGenericScheduleFixture(t, ctx, store, command)
			loaded, found, err := store.LoadGenericScheduleActivation(ctx, activation.ID)
			if err != nil || !found || loaded.Command.RunID != runID || loaded.Command.FlowInstance != runID || loaded.Command.RoutingSource != source || loaded.ImmutableHash != activation.ImmutableHash {
				t.Fatalf("exact current root did not roundtrip: %#v, %v", loaded, err)
			}
		})
	}
}

func TestClockScheduleRestoreTerminalizesForeignRootOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		for _, hostile := range []struct{ name, instance string }{
			{"foreign run", uuid.NewString()},
			{"previous generation", uuid.NewSHA1(uuid.NameSpaceURL, []byte("clock-service/generation/1")).String()},
			{"service identity", "clock-service"},
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
				activation := admitGenericScheduleFixture(t, ctx, store, command)
				foreign, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: hostile.instance})
				if err != nil {
					t.Fatal(err)
				}
				rawSource, err := json.Marshal(foreign)
				if err != nil {
					t.Fatal(err)
				}
				query := "UPDATE timers SET flow_instance = ?, routing_source = ? WHERE timer_id = ?"
				if _, ok := store.(*PostgresStore); ok {
					query = "UPDATE timers SET flow_instance = $1, routing_source = $2::jsonb WHERE timer_id = $3::uuid"
				}
				if _, err := db.ExecContext(ctx, query, hostile.instance, string(rawSource), activation.ID); err != nil {
					t.Fatal(err)
				}
				scheduler := &selectedStoreLifecycleScheduler{}
				lifecycle, err := genericschedule.NewLifecycle(store, scheduler, &terminalSchedulePlannerProbe{}, &terminalScheduleDispatcherProbe{}, nil, executionposture.Live)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = lifecycle.Stop(context.Background()) })
				if restored, err := lifecycle.Restore(ctx); err != nil || restored != 0 || len(scheduler.registered) != 0 {
					t.Fatalf("foreign root registered wakeups: restored=%d registered=%v err=%v", restored, scheduler.registered, err)
				}
				query = "SELECT flow_instance, status, immutable_hash, failure_code, failure_message FROM timers WHERE timer_id = ?"
				if _, ok := store.(*PostgresStore); ok {
					query = "SELECT flow_instance, status, immutable_hash, failure_code, failure_message FROM timers WHERE timer_id = $1::uuid"
				}
				var instance, status, hash, code, message string
				if err := db.QueryRowContext(ctx, query, activation.ID).Scan(&instance, &status, &hash, &code, &message); err != nil {
					t.Fatal(err)
				}
				if instance != hostile.instance || status != string(genericschedule.StatusFailed) || hash != activation.ImmutableHash || code != "malformed_persisted_activation" || !strings.Contains(message, "root instance publication source must match its run") {
					t.Fatalf("foreign root lost fail-loud evidence: instance=%q status=%q hash=%q code=%q message=%q", instance, status, hash, code, message)
				}
			})
		}
	}
}
