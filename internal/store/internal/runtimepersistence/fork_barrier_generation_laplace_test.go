package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
)

// The ordinal writer/evaluator tests already cover generation-bearing armed
// barriers, including fork-of-fork. This extends the existing six-state fixture
// to completion/terminal persistence with current and owned historical attempts.
func TestForkBarrierGenerationCorrespondenceBothStores(t *testing.T) {
	for _, mode := range []string{"current", "historical"} {
		t.Run(mode, func(t *testing.T) { runForkBarrierFixedRevisionMatrix(t, mode) })
	}
}

func readG28Loop(t *testing.T, ctx context.Context, db *sql.DB, runID string) loopruntime.Activation {
	t.Helper()
	var raw []byte
	if err := db.QueryRowContext(ctx, `SELECT accumulator FROM entity_state WHERE run_id=$1 AND entity_id=$1`, runID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var buckets map[string]any
	if err := json.Unmarshal(raw, &buckets); err != nil {
		t.Fatal(err)
	}
	carrier, err := engine.StateCarrierFromPersisted(nil, nil, nil, buckets)
	if err != nil {
		t.Fatal(err)
	}
	activations, err := loopruntime.List(carrier.StateBuckets)
	if err != nil || len(activations) != 1 {
		t.Fatalf("loop readback: %+v %v", activations, err)
	}
	return activations[0]
}

func assertG28ForkBarrier(t *testing.T, ctx context.Context, db *sql.DB, owner genericschedule.Store, fixture fanOutOwnerFixture, childRun string, sourceBarrier *fanoutbarrier.Barrier, mode string) timeridentity.TimerHandle {
	t.Helper()
	sourceHandle := sourceBarrier.Registration.Handle
	source := readG28Loop(t, ctx, db, fixture.runID)
	child := readG28Loop(t, ctx, db, childRun)
	sourceRef, _ := sourceHandle.JoinRef()
	if !source.OwnsGeneration(sourceRef.Generation()) ||
		(mode == "historical" && source.Attempt != sourceRef.Generation().Attempt+1) ||
		(mode == "current" && source.Generation() != sourceRef.Generation()) {
		t.Fatal("fixture did not retain the intended current or historical reference")
	}
	wantActivation, err := loopruntime.Fork(source, childRun, childRun)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(child, wantActivation) {
		t.Fatalf("child loop differs: got=%+v want=%+v", child, wantActivation)
	}
	wantGeneration, err := loopruntime.ForkGeneration(sourceRef.Generation(), childRun, childRun)
	if err != nil {
		t.Fatal(err)
	}
	wantRef, err := sourceRef.WithGeneration(wantGeneration)
	if err != nil {
		t.Fatal(err)
	}
	wantHandle, err := timeridentity.JoinCompleteHandle(wantRef)
	if err != nil {
		t.Fatal(err)
	}
	var raw, routingRaw []byte
	var entity, scope, instance, path string
	var schedule sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT timer_handle,routing_source,entity_id,route_scope_key,route_instance_id,route_instance_path,schedule_activation_id FROM fan_out_obligation_barriers WHERE run_id=$1 AND triggering_delivery_id=$2`, childRun, fixture.deliveryID).Scan(&raw, &routingRaw, &entity, &scope, &instance, &path, &schedule); err != nil {
		t.Fatal(err)
	}
	var got timeridentity.TimerHandle
	var routing events.RoutingSource
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(routingRaw, &routing); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, wantHandle) || got.TaskID() == sourceHandle.TaskID() || entity != childRun || scope != "." || instance != childRun || path != childRun || routing != eventtest.RootRoutingSource(childRun) {
		t.Fatalf("child barrier registration lost exact ownership: handle=%+v entity=%s route=%s/%s/%s source=%+v", got, entity, scope, instance, path, routing)
	}
	if !child.OwnsGeneration(wantGeneration) {
		t.Fatal("child barrier generation is not historically owned")
	}
	if schedule.Valid {
		activation, found, err := owner.LoadGenericScheduleActivation(ctx, schedule.String)
		if err != nil || !found {
			t.Fatalf("child completion schedule readback: found=%v err=%v", found, err)
		}
		barrier := *sourceBarrier
		barrier.Registration.IntentKey.RunID = childRun
		barrier.Registration.Handle = wantHandle
		barrier.Registration.EntityID = childRun
		barrier.Registration.Route = flowidentity.StoredRoute(".", childRun, childRun)
		barrier.Registration.RoutingSource = eventtest.RootRoutingSource(childRun)
		barrier.ScheduleKey, barrier.ScheduleActivationID = wantHandle.TaskID(), schedule.String
		if err := genericschedule.ValidateFanOutBarrierScheduleRelation(barrier, activation); err != nil {
			t.Fatalf("full child completion generation/payload/task/ownership: %v", err)
		}
	}
	return wantHandle
}
