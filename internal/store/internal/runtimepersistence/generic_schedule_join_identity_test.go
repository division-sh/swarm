package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/google/uuid"
)

func TestSystemJoinScheduleAdmissionAndHydrationRejectDeclarationDriftOnBothStores(t *testing.T) {
	for _, storeCase := range selectedScheduleStoreCases() {
		t.Run(storeCase.name, func(t *testing.T) {
			store, db, ctx := storeCase.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			generation := attemptgeneration.Generation{LoopID: "revision", ActivationID: "activation", RevisionField: "revision_id", RevisionID: "rev-2", Attempt: 2}
			bases := map[string]runtimegenericschedule.AdmissionCommand{
				"root": selectedStoreJoinScheduleCommand(t, runID, "", "", generation),
				"flow": selectedStoreJoinScheduleCommand(t, runID, "orders", "orders/order-1", generation),
			}
			hostiles := selectedStoreJoinScheduleHostileCases(generation)

			for _, hostile := range hostiles {
				t.Run("admission_"+hostile.scope+"_"+hostile.name, func(t *testing.T) {
					before := selectedStoreTimerCount(t, ctx, db)
					command := bases[hostile.scope]
					hostile.mutate(t, &command)
					if _, err := store.AdmitGenericSchedule(ctx, command); err == nil {
						t.Fatal("hostile join schedule reached persistence")
					}
					if after := selectedStoreTimerCount(t, ctx, db); after != before {
						t.Fatalf("hostile admission mutated timers: before=%d after=%d", before, after)
					}
				})
			}

			admitted := make(map[string]runtimegenericschedule.Activation, len(bases))
			for scope, command := range bases {
				result, err := store.AdmitGenericSchedule(ctx, command)
				if err != nil {
					t.Fatalf("admit valid %s join schedule: %v", scope, err)
				}
				admitted[scope] = result.Activation
			}

			for _, hostile := range hostiles {
				t.Run("hydration_"+hostile.scope+"_"+hostile.name, func(t *testing.T) {
					command := bases[hostile.scope]
					hostile.mutate(t, &command)
					activationID := admitted[hostile.scope].ID
					validSnapshot := selectedStoreTimerSnapshotForID(t, ctx, db, store, activationID)
					err := writeSelectedStoreScheduleProjection(ctx, db, store, activationID, command)
					if hostile.storageRejects {
						if err == nil {
							t.Fatal("selected-store schema accepted an impossible hostile projection")
						}
						if after := selectedStoreTimerSnapshotForID(t, ctx, db, store, activationID); validSnapshot != after {
							t.Fatalf("rejected hostile storage write mutated timer row\nbefore=%#v\nafter=%#v", validSnapshot, after)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					before := selectedStoreTimerSnapshotForID(t, ctx, db, store, activationID)
					if activation, found, err := store.LoadGenericScheduleActivation(ctx, activationID); err == nil || found {
						t.Fatalf("hostile activation loaded: found=%v activation=%#v err=%v", found, activation, err)
					}
					after := selectedStoreTimerSnapshotForID(t, ctx, db, store, activationID)
					if before != after {
						t.Fatalf("failed hydration mutated timer row\nbefore=%#v\nafter=%#v", before, after)
					}
					if err := writeSelectedStoreScheduleProjection(ctx, db, store, activationID, bases[hostile.scope]); err != nil {
						t.Fatal(err)
					}
					if restored, found, err := store.LoadGenericScheduleActivation(ctx, activationID); err != nil || !found || restored.ID != activationID {
						t.Fatalf("restored valid activation = found:%v activation:%#v err:%v", found, restored, err)
					}
				})
			}

			poisoned := bases["root"]
			selectedStoreMutateJoinPayload(t, &poisoned, func(payload, _, _ map[string]any) {
				payload[generation.RevisionField] = "rev-hostile"
			})
			if err := writeSelectedStoreScheduleProjection(ctx, db, store, admitted["root"].ID, poisoned); err != nil {
				t.Fatal(err)
			}
			beforeRestore := selectedStoreTimerSnapshotForID(t, ctx, db, store, admitted["root"].ID)

			scheduler := &selectedStoreLifecycleScheduler{}
			lifecycle, err := runtimegenericschedule.NewLifecycle(store, scheduler, &terminalSchedulePlannerProbe{}, &terminalScheduleDispatcherProbe{}, nil, executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lifecycle.Stop(context.Background()) })
			if restored, err := lifecycle.Restore(ctx); err == nil || restored != 0 {
				t.Fatalf("poisoned restore = restored:%d err:%v", restored, err)
			}
			if len(scheduler.registered) != 0 {
				t.Fatalf("poisoned restore partially registered wakeups: %#v", scheduler.registered)
			}
			if afterRestore := selectedStoreTimerSnapshotForID(t, ctx, db, store, admitted["root"].ID); beforeRestore != afterRestore {
				t.Fatalf("failed restore mutated poisoned row\nbefore=%#v\nafter=%#v", beforeRestore, afterRestore)
			}
		})
	}
}

type selectedStoreJoinScheduleHostileCase struct {
	name           string
	scope          string
	storageRejects bool
	mutate         func(testing.TB, *runtimegenericschedule.AdmissionCommand)
}

func selectedStoreJoinScheduleHostileCases(generation attemptgeneration.Generation) []selectedStoreJoinScheduleHostileCase {
	payload := func(mutate func(map[string]any, map[string]any, map[string]any)) func(testing.TB, *runtimegenericschedule.AdmissionCommand) {
		return func(t testing.TB, command *runtimegenericschedule.AdmissionCommand) {
			selectedStoreMutateJoinPayload(t, command, mutate)
		}
	}
	return []selectedStoreJoinScheduleHostileCase{
		{name: "schedule_key", scope: "root", mutate: func(_ testing.TB, c *runtimegenericschedule.AdmissionCommand) { c.ScheduleKey += "-hostile" }},
		{name: "task_id", scope: "root", mutate: func(_ testing.TB, c *runtimegenericschedule.AdmissionCommand) { c.TaskID += "-hostile" }},
		{name: "event_kind", scope: "root", mutate: func(_ testing.TB, c *runtimegenericschedule.AdmissionCommand) { c.EventType = "platform.join_complete" }},
		{name: "scalar_payload", scope: "root", mutate: func(t testing.TB, c *runtimegenericschedule.AdmissionCommand) {
			var err error
			c.Payload, err = canonicaljson.FromGo("hostile")
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unrelated_event", scope: "root", mutate: func(_ testing.TB, c *runtimegenericschedule.AdmissionCommand) {
			c.EventType = "platform.test_timer_fired"
		}},
		{name: "owner_id", scope: "root", mutate: func(_ testing.TB, c *runtimegenericschedule.AdmissionCommand) { c.OwnerID = "runtime" }},
		{name: "owner_kind", scope: "root", storageRejects: true, mutate: func(_ testing.TB, c *runtimegenericschedule.AdmissionCommand) {
			c.OwnerKind = runtimegenericschedule.OwnerAgent
		}},
		{name: "run_missing", scope: "root", mutate: func(_ testing.TB, c *runtimegenericschedule.AdmissionCommand) { c.RunID = "" }},
		{name: "entity", scope: "root", mutate: func(_ testing.TB, c *runtimegenericschedule.AdmissionCommand) { c.EntityID = uuid.NewString() }},
		{name: "routing_kind", scope: "root", mutate: func(t testing.TB, c *runtimegenericschedule.AdmissionCommand) {
			var err error
			c.RoutingSource, err = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: "orders", FlowInstance: "orders/order-1", EntityID: c.EntityID})
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "flow_id_runtime_alias", scope: "root", mutate: payload(func(_, _, join map[string]any) { join["flow_id"] = "workflow-runtime-name" })},
		{name: "flow_id_missing", scope: "root", mutate: payload(func(_, _, join map[string]any) { delete(join, "flow_id") })},
		{name: "node_id", scope: "root", mutate: payload(func(_, _, join map[string]any) { join["node_id"] = "other-node" })},
		{name: "handler_event", scope: "root", mutate: payload(func(_, _, join map[string]any) { join["handler_event"] = "other.completed" })},
		{name: "stage", scope: "root", mutate: payload(func(_, _, join map[string]any) { join["stage"] = "other-stage" })},
		{name: "join_id", scope: "root", mutate: payload(func(_, _, join map[string]any) { join["join_id"] = "other-join" })},
		{name: "window", scope: "root", mutate: payload(func(_, _, join map[string]any) { join["window"] = "other-window" })},
		{name: "generation", scope: "root", mutate: payload(func(_, _, join map[string]any) {
			join[attemptgeneration.PayloadKey].(map[string]any)["revision_id"] = "rev-hostile"
		})},
		{name: "duplicate_generation", scope: "root", mutate: payload(func(_, handle, join map[string]any) {
			handle[attemptgeneration.PayloadKey] = join[attemptgeneration.PayloadKey]
		})},
		{name: "handle_kind", scope: "root", mutate: payload(func(_, handle, _ map[string]any) { handle["kind"] = "join_complete" })},
		{name: "revision_pin", scope: "root", mutate: payload(func(payload, _, _ map[string]any) { payload[generation.RevisionField] = "rev-hostile" })},
		{name: "revision_pin_missing", scope: "root", mutate: payload(func(payload, _, _ map[string]any) { delete(payload, generation.RevisionField) })},
		{name: "flow_id_root_alias", scope: "flow", mutate: payload(func(_, _, join map[string]any) { join["flow_id"] = "" })},
		{name: "flow_id_other", scope: "flow", mutate: payload(func(_, _, join map[string]any) { join["flow_id"] = "returns" })},
		{name: "flow_instance", scope: "flow", mutate: func(_ testing.TB, c *runtimegenericschedule.AdmissionCommand) { c.FlowInstance = "orders/order-2" }},
		{name: "routing_flow", scope: "flow", mutate: func(t testing.TB, c *runtimegenericschedule.AdmissionCommand) {
			var err error
			c.RoutingSource, err = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: "returns", FlowInstance: c.FlowInstance, EntityID: c.EntityID})
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "routing_instance", scope: "flow", mutate: func(t testing.TB, c *runtimegenericschedule.AdmissionCommand) {
			var err error
			c.RoutingSource, err = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: "orders", FlowInstance: "orders/order-2", EntityID: c.EntityID})
			if err != nil {
				t.Fatal(err)
			}
		}},
		{name: "routing_entity", scope: "flow", mutate: func(t testing.TB, c *runtimegenericschedule.AdmissionCommand) {
			var err error
			c.RoutingSource, err = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: "orders", FlowInstance: c.FlowInstance, EntityID: uuid.NewString()})
			if err != nil {
				t.Fatal(err)
			}
		}},
	}
}

func selectedStoreMutateJoinPayload(t testing.TB, command *runtimegenericschedule.AdmissionCommand, mutate func(map[string]any, map[string]any, map[string]any)) {
	t.Helper()
	payload := selectedStoreSchedulePayload(t, command)
	handle, ok := payload["timer_handle"].(map[string]any)
	if !ok {
		t.Fatalf("join payload lacks timer_handle: %#v", payload)
	}
	join, ok := handle["join"].(map[string]any)
	if !ok {
		t.Fatalf("join payload lacks declaration: %#v", payload)
	}
	mutate(payload, handle, join)
	value, err := canonicaljson.FromGo(payload)
	if err != nil {
		t.Fatal(err)
	}
	command.Payload = value
}

type selectedStoreTimerSnapshot [14]string

func selectedStoreTimerSnapshotForID(t testing.TB, ctx context.Context, db *sql.DB, store runtimegenericschedule.Store, activationID string) selectedStoreTimerSnapshot {
	t.Helper()
	query := `SELECT schedule_key, COALESCE(CAST(run_id AS TEXT), ''), COALESCE(CAST(entity_id AS TEXT), ''),
		COALESCE(flow_instance, ''), owner_kind, COALESCE(owner_agent, ''), fire_event,
		CAST(fire_payload AS TEXT), CAST(routing_source AS TEXT), COALESCE(task_id, ''), immutable_hash,
		status, COALESCE(cancel_cause, ''), COALESCE(CAST(occurrence_event_id AS TEXT), '')
		FROM timers WHERE timer_id = ?`
	if _, ok := store.(*PostgresStore); ok {
		query = `SELECT schedule_key, COALESCE(CAST(run_id AS TEXT), ''), COALESCE(CAST(entity_id AS TEXT), ''),
			COALESCE(flow_instance, ''), owner_kind, COALESCE(owner_agent, ''), fire_event,
			CAST(fire_payload AS TEXT), CAST(routing_source AS TEXT), COALESCE(task_id, ''), immutable_hash,
			status, COALESCE(cancel_cause, ''), COALESCE(CAST(occurrence_event_id AS TEXT), '')
			FROM timers WHERE timer_id = $1::uuid`
	}
	var snapshot selectedStoreTimerSnapshot
	dest := make([]any, len(snapshot))
	for i := range snapshot {
		dest[i] = &snapshot[i]
	}
	if err := db.QueryRowContext(ctx, query, activationID).Scan(dest...); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func writeSelectedStoreScheduleProjection(ctx context.Context, db *sql.DB, store runtimegenericschedule.Store, activationID string, command runtimegenericschedule.AdmissionCommand) error {
	payload, err := canonicaljson.Encode(command.Payload)
	if err != nil {
		return err
	}
	routing, err := json.Marshal(command.RoutingSource)
	if err != nil {
		return err
	}
	args := []any{
		command.ScheduleKey, command.RunID, command.EntityID, command.FlowInstance, command.OwnerKind,
		command.OwnerID, command.EventType, string(payload), string(routing), command.TaskID, activationID,
	}
	query := `UPDATE timers SET schedule_key = ?, run_id = NULLIF(?, ''), entity_id = NULLIF(?, ''),
		flow_instance = NULLIF(?, ''), owner_kind = ?, owner_agent = ?, fire_event = ?, fire_payload = ?,
		routing_source = ?, task_id = NULLIF(?, '') WHERE timer_id = ?`
	if _, ok := store.(*PostgresStore); ok {
		query = `UPDATE timers SET schedule_key = $1, run_id = NULLIF($2, '')::uuid, entity_id = NULLIF($3, '')::uuid,
			flow_instance = NULLIF($4, ''), owner_kind = $5, owner_agent = $6, fire_event = $7, fire_payload = $8::jsonb,
			routing_source = $9::jsonb, task_id = NULLIF($10, '') WHERE timer_id = $11::uuid`
	}
	_, err = db.ExecContext(ctx, query, args...)
	return err
}

func TestJoinScheduleRestoreRejectsPoisonedSameLeafIdentityWithoutRegisteringPartialState(t *testing.T) {
	for _, storeCase := range selectedScheduleStoreCases() {
		t.Run(storeCase.name, func(t *testing.T) {
			store, db, ctx := storeCase.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			generation := attemptgeneration.Generation{LoopID: "revision", ActivationID: "activation", RevisionField: "revision_id", RevisionID: "rev-2", Attempt: 2}
			root := selectedStoreJoinScheduleCommand(t, runID, "", "", generation)
			flow := selectedStoreJoinScheduleCommand(t, runID, "orders", "orders/order-1", generation)
			poison := selectedStoreJoinScheduleCommand(t, runID, "returns", "returns/order-1", generation)
			rootResult, err := store.AdmitGenericSchedule(ctx, root)
			if err != nil {
				t.Fatal(err)
			}
			flowResult, err := store.AdmitGenericSchedule(ctx, flow)
			if err != nil {
				t.Fatal(err)
			}
			poisonResult, err := store.AdmitGenericSchedule(ctx, poison)
			if err != nil {
				t.Fatal(err)
			}
			poisonedPayload := selectedStoreSchedulePayload(t, &poison)
			poisonedPayload[generation.RevisionField] = "rev-hostile"
			raw, err := json.Marshal(poisonedPayload)
			if err != nil {
				t.Fatal(err)
			}
			update := `UPDATE timers SET fire_payload = ? WHERE timer_id = ?`
			args := []any{string(raw), poisonResult.Activation.ID}
			if _, ok := store.(*PostgresStore); ok {
				update = `UPDATE timers SET fire_payload = $1::jsonb WHERE timer_id = $2::uuid`
			}
			if _, err := db.ExecContext(ctx, update, args...); err != nil {
				t.Fatal(err)
			}

			scheduler := &selectedStoreLifecycleScheduler{}
			lifecycle, err := runtimegenericschedule.NewLifecycle(store, scheduler, &terminalSchedulePlannerProbe{}, &terminalScheduleDispatcherProbe{}, nil, executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lifecycle.Stop(context.Background()) })
			if restored, err := lifecycle.Restore(ctx); err == nil || restored != 0 || len(scheduler.registered) != 0 {
				t.Fatalf("mixed poisoned restore = restored:%d registered:%#v err:%v", restored, scheduler.registered, err)
			}

			deleteSQL := `DELETE FROM timers WHERE timer_id = ?`
			if _, ok := store.(*PostgresStore); ok {
				deleteSQL = `DELETE FROM timers WHERE timer_id = $1::uuid`
			}
			if _, err := db.ExecContext(ctx, deleteSQL, poisonResult.Activation.ID); err != nil {
				t.Fatal(err)
			}
			if restored, err := lifecycle.Restore(ctx); err != nil || restored != 2 || len(scheduler.registered) != 2 {
				t.Fatalf("clean restore = restored:%d registered:%#v err:%v", restored, scheduler.registered, err)
			}
			registered := map[string]bool{}
			for _, wakeup := range scheduler.registered {
				registered[wakeup.ActivationID()] = true
			}
			if !registered[rootResult.Activation.ID] || !registered[flowResult.Activation.ID] {
				t.Fatalf("clean restore registered wrong identities: %#v", registered)
			}
		})
	}
}

func TestJoinScheduleRestoreRejectsDriftedEventWithoutFailingTypedJoinRow(t *testing.T) {
	for _, storeCase := range selectedScheduleStoreCases() {
		t.Run(storeCase.name, func(t *testing.T) {
			store, db, ctx := storeCase.open(t)
			runID := runtimecorrelation.RunIDFromContext(ctx)
			generation := attemptgeneration.Generation{LoopID: "revision", ActivationID: "activation", RevisionField: "revision_id", RevisionID: "rev-2", Attempt: 2}
			root := selectedStoreJoinScheduleCommand(t, runID, "", "", generation)
			flow := selectedStoreJoinScheduleCommand(t, runID, "orders", "orders/order-1", generation)
			if _, err := store.AdmitGenericSchedule(ctx, root); err != nil {
				t.Fatal(err)
			}
			flowResult, err := store.AdmitGenericSchedule(ctx, flow)
			if err != nil {
				t.Fatal(err)
			}

			update := `UPDATE timers SET fire_event = ? WHERE timer_id = ?`
			args := []any{"platform.test_timer_fired", flowResult.Activation.ID}
			if _, ok := store.(*PostgresStore); ok {
				update = `UPDATE timers SET fire_event = $1 WHERE timer_id = $2::uuid`
			}
			if _, err := db.ExecContext(ctx, update, args...); err != nil {
				t.Fatal(err)
			}
			before := selectedStoreTimerSnapshotForID(t, ctx, db, store, flowResult.Activation.ID)

			scheduler := &selectedStoreLifecycleScheduler{}
			lifecycle, err := runtimegenericschedule.NewLifecycle(store, scheduler, &terminalSchedulePlannerProbe{}, &terminalScheduleDispatcherProbe{}, nil, executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lifecycle.Stop(context.Background()) })
			if restored, err := lifecycle.Restore(ctx); err == nil || restored != 0 || len(scheduler.registered) != 0 {
				t.Fatalf("event-drift restore = restored:%d registered:%#v err:%v", restored, scheduler.registered, err)
			}
			if after := selectedStoreTimerSnapshotForID(t, ctx, db, store, flowResult.Activation.ID); before != after {
				t.Fatalf("event-drift restore mutated typed join row\nbefore=%#v\nafter=%#v", before, after)
			}
		})
	}
}

func selectedStoreJoinScheduleCommand(t *testing.T, runID, flowID, flowInstance string, generation attemptgeneration.Generation) runtimegenericschedule.AdmissionCommand {
	t.Helper()
	entityID := uuid.NewString()
	ref, err := timeridentity.NewJoinRefForGeneration(flowID, "join-node", "item.completed", "awaiting", "shared", "window-1", generation)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := timeridentity.JoinTimeoutHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := canonicaljson.FromGo(handle.PayloadMetadata())
	if err != nil {
		t.Fatal(err)
	}
	var routing events.RoutingSource
	if flowID == "" {
		routing, err = events.NewRootRoutingSource(entityID)
	} else {
		routing, err = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: flowID, FlowInstance: flowInstance, EntityID: entityID})
	}
	if err != nil {
		t.Fatal(err)
	}
	return runtimegenericschedule.AdmissionCommand{
		ScheduleKey: handle.TaskID(), RunID: runID, EntityID: entityID, FlowInstance: flowInstance,
		OwnerKind: runtimegenericschedule.OwnerSystem, OwnerID: "workflow-runtime", EventType: handle.EventType(), Payload: payload,
		RoutingSource: routing, ExecutionMode: executionmode.Live, Due: runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(time.Hour)), TaskID: handle.TaskID(),
	}
}

func selectedStoreSchedulePayload(t testing.TB, command *runtimegenericschedule.AdmissionCommand) map[string]any {
	t.Helper()
	raw, err := canonicaljson.Encode(command.Payload)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}

func selectedStoreTimerCount(t *testing.T, ctx context.Context, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM timers`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
