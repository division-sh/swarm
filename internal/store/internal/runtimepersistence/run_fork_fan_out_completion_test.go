package runtimepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
)

type forkCompletionScheduleLogger struct{ t *testing.T }

func (l forkCompletionScheduleLogger) GenericScheduleFailure(_ context.Context, operation, activation string, err error) {
	l.t.Logf("completion schedule %s activation=%s: %v", operation, activation, err)
}

func (l forkCompletionScheduleLogger) GenericScheduleCatchupWarning(_ context.Context, activation string, count int) {
	l.t.Logf("completion schedule catchup activation=%s count=%d", activation, count)
}

func consumeForkFanOutBarrierCompletion(t *testing.T, fixture authorActivityReceiptFixture, backend string, eventBus *bus.EventBus, ctx context.Context, intent fanoutobligation.Intent) {
	t.Helper()
	runID := intent.Request.Key.RunID
	selected := fixture.store.(storeTestDurableEventBusStore)
	store := fixture.store.(genericschedule.Store)
	loopBefore := readForkBarrierLoop(t, ctx, fixture.db, runID)
	advanceFanOutBarriersForTest(t, ctx, selected, fixture.db, runID, time.Now().UTC())
	var activationID string
	var raw []byte
	if err := fixture.db.QueryRowContext(ctx, `SELECT schedule_activation_id,timer_handle FROM fan_out_obligation_barriers WHERE run_id=$1`, runID).Scan(&activationID, &raw); err != nil {
		t.Fatal(err)
	}
	var handle timeridentity.TimerHandle
	if err := json.Unmarshal(raw, &handle); err != nil {
		t.Fatal(err)
	}
	ref, ok := handle.JoinRef()
	if !ok || ref.Generation() != loopBefore.Generation() {
		t.Fatal("completion is not bound to the current child generation")
	}
	activation, found, err := store.LoadGenericScheduleActivation(ctx, activationID)
	if err != nil || !found || activation.Status != genericschedule.StatusActive || activation.Command.TaskID != handle.TaskID() {
		t.Fatalf("child completion activation: %+v found=%v err=%v", activation, found, err)
	}
	scheduler := &selectedStoreLifecycleScheduler{}
	lifecycle, err := genericschedule.NewLifecycle(store, scheduler, eventBus, eventBus.EngineDispatcher(), forkCompletionScheduleLogger{t}, executionposture.Live)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopSelectedStoreScheduleLifecycle(t, lifecycle) })
	if err := lifecycle.ReconcileWakeup(ctx, activationID); err != nil || len(scheduler.registered) != 1 || scheduler.callback == nil {
		t.Fatalf("restore exact completion wakeup: registered=%+v err=%v", scheduler.registered, err)
	}
	wakeup := scheduler.registered[0]
	scheduler.callback(ctx, wakeup)
	fired, found, err := store.LoadGenericScheduleActivation(ctx, activationID)
	if err != nil || !found || fired.Status != genericschedule.StatusFired || fired.CurrentEventID == "" {
		t.Fatalf("canonical completion did not fire: status=%s event=%s found=%v err=%v", fired.Status, fired.CurrentEventID, found, err)
	}
	prepared, found, err := selected.LoadPreparedPublishEvent(ctx, fired.CurrentEventID)
	if err != nil || !found || len(prepared.DeliveryRoutes) != 1 || prepared.Event.Event().TaskID() != handle.TaskID() {
		t.Fatalf("completion occurrence readback: %+v found=%v err=%v", prepared, found, err)
	}
	occurrence := prepared.Event.Event()
	wantRoute := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(ref.Node()), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: runID})}
	var payload map[string]any
	if err := json.Unmarshal(occurrence.Payload(), &payload); err != nil {
		t.Fatal(err)
	}
	actualHandle, actualRef, valid := timeridentity.ParseJoinHandle(payload)
	if !valid || actualHandle != handle || actualRef != ref || occurrence.RunID() != runID || occurrence.RoutingSource() != activation.Command.RoutingSource || !reflect.DeepEqual(prepared.DeliveryRoutes[0], wantRoute) {
		t.Fatalf("completion occurrence lost exact child handle/source/receiver: event=%+v route=%+v", occurrence, prepared.DeliveryRoutes)
	}
	var status, failure string
	if err := fixture.db.QueryRowContext(ctx, `SELECT status,COALESCE(CAST(failure AS TEXT),'') FROM event_deliveries WHERE event_id=$1`, fired.CurrentEventID).Scan(&status, &failure); err != nil || status != "delivered" {
		t.Fatalf("completion recipient settlement: status=%s failure=%s err=%v", status, failure, err)
	}
	summary := fanoutbarrier.Summary{Total: intent.Request.Cardinality, Succeeded: intent.Request.Cardinality}
	assertFanOutBarrierState(t, ctx, fixture.db, runID, intent.Request.Key.TriggeringDeliveryID, intent.Request.PlanRef.ElementRef.SemanticPath, fanoutbarrier.StatusFired, &summary, handle.TaskID())
	rows, err := fixture.db.QueryContext(ctx, `SELECT payload FROM events WHERE run_id=$1 AND event_name='batch.completed'`, runID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var got struct {
			Total        int    `json:"total"`
			RevisionID   string `json:"revision_id"`
			LoopID       string `json:"loop_id"`
			ActivationID string `json:"activation_id"`
			Attempt      int    `json:"attempt"`
			MaxAttempts  int    `json:"max_attempts"`
		}
		if err := json.Unmarshal(raw, &got); err != nil || got.Total != intent.Request.Cardinality || got.RevisionID != ref.Generation().RevisionID || got.LoopID != loopBefore.LoopID || got.ActivationID != loopBefore.ActivationID || got.Attempt != ref.Generation().Attempt || got.MaxAttempts != loopBefore.MaxAttempts || got.RevisionID == intent.Request.Capsule.Entity["business_revision"] {
			t.Fatalf("barrier output substituted source/current context: payload=%s captured=%+v owner=%+v err=%v", raw, ref.Generation(), loopBefore, err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil || count != 1 {
		t.Fatalf("completion output count=%d err=%v", count, err)
	}
	if !reflect.DeepEqual(loopBefore, readForkBarrierLoop(t, ctx, fixture.db, runID)) {
		t.Fatal("barrier completion changed owning child loop")
	}
	beforeDuplicate := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
	scheduler.callback(ctx, wakeup)
	if err := eventBus.EngineDispatcher().DispatchPostCommit(ctx, []engine.EmitIntent{{Event: prepared.Event.Event()}}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beforeDuplicate, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
		t.Fatal("duplicate completion wakeup/delivery changed history")
	}
	stopSelectedStoreScheduleLifecycle(t, lifecycle)
	restoredScheduler := &selectedStoreLifecycleScheduler{}
	restored, err := genericschedule.NewLifecycle(store, restoredScheduler, eventBus, eventBus.EngineDispatcher(), forkCompletionScheduleLogger{t}, executionposture.Live)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopSelectedStoreScheduleLifecycle(t, restored) })
	if count, err := restored.Restore(ctx); err != nil || count != 0 || len(restoredScheduler.registered) != 0 {
		t.Fatalf("reconstructed lifecycle revived completed barrier: count=%d registered=%v err=%v", count, restoredScheduler.registered, err)
	}
	if !reflect.DeepEqual(beforeDuplicate, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
		t.Fatal("reconstructed completion lifecycle changed history")
	}
}
