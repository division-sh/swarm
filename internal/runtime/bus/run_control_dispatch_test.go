package bus_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestEventBusRunControlPauseQueuesOnlyTargetRunAndContinueReleases(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	pg := &store.PostgresStore{DB: db}
	eb, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeruncontrol.NewController(pg, eb, runtimeruncontrol.Options{})
	eb.SetRunDispatchGate(controller)

	agentID := "agent-run-control"
	eventType := events.EventType("custom.run_control")
	seedActiveRuntimeBusAgent(t, ctx, pg, agentID)
	ch := eb.Subscribe(agentID, eventType)
	defer eb.Unsubscribe(agentID)

	pausedRunID := uuid.NewString()
	otherRunID := uuid.NewString()
	for _, runID := range []string{pausedRunID, otherRunID} {
		if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
			t.Fatalf("seed run %s: %v", runID, err)
		}
	}
	if _, err := controller.Pause(ctx, runtimeruncontrol.TransitionRequest{RunID: pausedRunID, Reason: "test", ControlledBy: "test"}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	pausedEventID := uuid.NewString()
	if err := eb.Publish(ctx, eventtest.WithEntityID(eventtest.Projection(pausedEventID,

		eventType,
		"api.v1", "", []byte(`{"entity_id":"21000000-0000-0000-0000-000000000002"}`), 0, pausedRunID, "", events.EventEnvelope{}, time.Now().UTC()),
		"21000000-0000-0000-0000-000000000002")); err != nil {
		t.Fatalf("Publish paused run event: %v", err)
	}
	requireNoBusEvent(t, ch, "paused run before continue")
	if got := countPipelineReceiptsForEvent(t, ctx, db, pausedEventID); got != 0 {
		t.Fatalf("paused run pipeline receipts = %d, want 0", got)
	}

	otherEventID := uuid.NewString()
	if err := eb.Publish(ctx, eventtest.WithEntityID(eventtest.Projection(otherEventID,

		eventType,
		"api.v1", "", []byte(`{"entity_id":"21000000-0000-0000-0000-000000000003"}`), 0, otherRunID, "", events.EventEnvelope{}, time.Now().UTC()),
		"21000000-0000-0000-0000-000000000003")); err != nil {
		t.Fatalf("Publish other run event: %v", err)
	}
	got := requireBusEvent(t, ch, "other run dispatch")
	if got.ID() != otherEventID {
		t.Fatalf("delivered event = %s, want other run %s", got.ID(), otherEventID)
	}

	result, err := controller.Continue(ctx, runtimeruncontrol.TransitionRequest{RunID: pausedRunID, Reason: "test", ControlledBy: "test"})
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if result.ReleasedDeliveries != 1 {
		t.Fatalf("released deliveries = %d, want 1", result.ReleasedDeliveries)
	}
	got = requireBusEvent(t, ch, "paused run release")
	if got.ID() != pausedEventID {
		t.Fatalf("released event = %s, want paused run %s", got.ID(), pausedEventID)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, pausedEventID); got != 1 {
		t.Fatalf("paused run pipeline receipts after continue = %d, want 1", got)
	}
}

func TestEventBusRunControlPauseQueuesBeforeInterceptorsAndContinueReplaysThem(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	pg := &store.PostgresStore{DB: db}
	eventType := events.EventType("custom.run_control.intercepted")
	deferredType := events.EventType("custom.run_control.deferred")
	recorder := &runControlRecordingInterceptor{
		triggerType:  eventType,
		deferredType: deferredType,
	}
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{recorder},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	controller := runtimeruncontrol.NewController(pg, eb, runtimeruncontrol.Options{})
	eb.SetRunDispatchGate(controller)

	agentID := "agent-run-control-interceptor"
	seedActiveRuntimeBusAgent(t, ctx, pg, agentID)
	ch := eb.Subscribe(agentID, eventType, deferredType)
	defer eb.Unsubscribe(agentID)

	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := controller.Pause(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "test", ControlledBy: "test"}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	queuedEventID := uuid.NewString()
	if err := eb.Publish(ctx, eventtest.WithEntityID(eventtest.Projection(queuedEventID,

		eventType,
		"api.v1", "", []byte(`{"entity_id":"22000000-0000-0000-0000-000000000001"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC()),
		"22000000-0000-0000-0000-000000000001")); err != nil {
		t.Fatalf("Publish paused run event: %v", err)
	}
	requireNoBusEvent(t, ch, "paused intercepted run before continue")
	if got := recorder.count(); got != 0 {
		t.Fatalf("interceptor executions before continue = %d, want 0", got)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, queuedEventID); got != 0 {
		t.Fatalf("queued event receipts before continue = %d, want 0", got)
	}

	result, err := controller.Continue(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "test", ControlledBy: "test"})
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if result.ReleasedDeliveries != 1 {
		t.Fatalf("released deliveries = %d, want 1", result.ReleasedDeliveries)
	}
	if got := recorder.count(); got != 1 {
		t.Fatalf("interceptor executions after continue = %d, want 1", got)
	}

	requireBusEventTypes(t, ch, "released original and deferred events", eventType, deferredType)
	if got := countPipelineReceiptsForEvent(t, ctx, db, queuedEventID); got != 1 {
		t.Fatalf("queued event receipts after continue = %d, want 1", got)
	}
}

func TestEventBusRunControlPauseQueuesPostCommitEmitBeforeInterceptors(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	pg := &store.PostgresStore{DB: db}
	eventType := events.EventType("custom.run_control.postcommit")
	deferredType := events.EventType("custom.run_control.postcommit.deferred")
	recorder := &runControlRecordingInterceptor{
		triggerType:  eventType,
		deferredType: deferredType,
	}
	eb, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{recorder},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	controller := runtimeruncontrol.NewController(pg, eb, runtimeruncontrol.Options{})
	eb.SetRunDispatchGate(controller)

	agentID := "agent-run-control-postcommit"
	seedActiveRuntimeBusAgent(t, ctx, pg, agentID)
	ch := eb.Subscribe(agentID, eventType, deferredType)
	defer eb.Unsubscribe(agentID)

	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	intent := runtimeengine.EmitIntent{
		Event: eventtest.WithEntityID(eventtest.Projection(uuid.NewString(),

			eventType,
			"runtime", "", []byte(`{"entity_id":"23000000-0000-0000-0000-000000000001"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC()),
			"23000000-0000-0000-0000-000000000001"),
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	if err := eb.EngineOutbox().WriteOutbox(txctx, []runtimeengine.EmitIntent{intent}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("WriteOutbox: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit outbox tx: %v", err)
	}

	if _, err := controller.Pause(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "test", ControlledBy: "test"}); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if err := eb.EngineDispatcher().DispatchPostCommit(ctx, []runtimeengine.EmitIntent{intent}); err != nil {
		t.Fatalf("DispatchPostCommit while paused: %v", err)
	}
	requireNoBusEvent(t, ch, "paused post-commit dispatch before continue")
	if got := recorder.count(); got != 0 {
		t.Fatalf("post-commit interceptor executions while paused = %d, want 0", got)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, intent.Event.ID()); got != 0 {
		t.Fatalf("post-commit event receipts while paused = %d, want 0", got)
	}

	result, err := controller.Continue(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "test", ControlledBy: "test"})
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if result.ReleasedDeliveries != 1 {
		t.Fatalf("released deliveries = %d, want 1", result.ReleasedDeliveries)
	}
	if got := recorder.count(); got != 1 {
		t.Fatalf("post-commit interceptor executions after continue = %d, want 1", got)
	}
	requireBusEventTypes(t, ch, "released post-commit original and deferred events", eventType, deferredType)
	if got := countPipelineReceiptsForEvent(t, ctx, db, intent.Event.ID()); got != 1 {
		t.Fatalf("post-commit event receipts after continue = %d, want 1", got)
	}
}

type runControlRecordingInterceptor struct {
	mu           sync.Mutex
	triggerType  events.EventType
	deferredType events.EventType
	seen         []string
}

func (i *runControlRecordingInterceptor) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, error) {
	if evt.Type() != i.triggerType {
		return true, nil, nil
	}
	i.mu.Lock()
	i.seen = append(i.seen, evt.ID())
	i.mu.Unlock()
	return true, []events.Event{eventtest.WithEntityID((eventtest.Projection(uuid.NewString(),

		i.deferredType,
		"runtime", "", []byte(`{"entity_id":"22000000-0000-0000-0000-000000000002"}`), 0, evt.RunID(), "", events.EventEnvelope{}, time.Now().UTC())), "22000000-0000-0000-0000-000000000002")}, nil
}

func (i *runControlRecordingInterceptor) count() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.seen)
}
