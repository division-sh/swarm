package bus_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestEventBusRunControlPauseQueuesOnlyTargetRunAndContinueReleases(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := testAuthorActivityContext(context.Background())
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	eb, err := newScopedTestEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeruncontrol.NewController(pg, eb, runtimeruncontrol.Options{})
	eb.SetRunDispatchGate(controller)

	agentID := "agent-run-control"
	eventType := events.EventType("custom.run_control")
	pausedRunID := uuid.NewString()
	otherRunID := uuid.NewString()
	for _, runID := range []string{pausedRunID, otherRunID} {
		seedRunControlTestRun(t, ctx, db, runID)
	}
	pausedIdentity := seedActiveRuntimeBusAgent(t, ctx, pg, pausedRunID, agentID)
	otherIdentity := seedActiveRuntimeBusAgent(t, ctx, pg, otherRunID, agentID)
	pausedCh := runtimebustest.SubscribeForRun(t, eb, pausedRunID, agentID, eventType)
	otherCh := runtimebustest.SubscribeForRun(t, eb, otherRunID, agentID, eventType)
	defer runtimebustest.UnsubscribeIdentity(eb, pausedIdentity)
	defer runtimebustest.UnsubscribeIdentity(eb, otherIdentity)

	if _, err := controller.Pause(ctx, runtimeruncontrol.TransitionRequest{RunID: pausedRunID, Reason: "test", ControlledBy: "test"}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	pausedEventID := uuid.NewString()
	if err := eb.Publish(ctx, eventtest.ExistingRunRootIngress(
		pausedEventID,
		eventType,
		"api.v1",
		"",
		[]byte(`{"entity_id":"21000000-0000-0000-0000-000000000002"}`),
		0,
		pausedRunID,
		events.EnvelopeForEntityID(events.EventEnvelope{}, "21000000-0000-0000-0000-000000000002"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish paused run event: %v", err)
	}
	requireNoBusEvent(t, pausedCh, "paused run before continue")
	if got := countPipelineReceiptsForEvent(t, ctx, db, pausedEventID); got != 0 {
		t.Fatalf("paused run pipeline receipts = %d, want 0", got)
	}

	otherEventID := uuid.NewString()
	if err := eb.Publish(ctx, eventtest.ExistingRunRootIngress(
		otherEventID,
		eventType,
		"api.v1",
		"",
		[]byte(`{"entity_id":"21000000-0000-0000-0000-000000000003"}`),
		0,
		otherRunID,
		events.EnvelopeForEntityID(events.EventEnvelope{}, "21000000-0000-0000-0000-000000000003"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish other run event: %v", err)
	}
	got := requireBusEvent(t, otherCh, "other run dispatch")
	if got.ID() != otherEventID {
		t.Fatalf("delivered event = %s, want other run %s", got.ID(), otherEventID)
	}

	result, err := controller.Continue(ctx, runtimeruncontrol.TransitionRequest{RunID: pausedRunID, Reason: "test", ControlledBy: "test"})
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if result.Recovery.Sweep.Settled != 1 {
		t.Fatalf("released deliveries = %d, want 1", result.Recovery.Sweep.Settled)
	}
	got = requireBusEvent(t, pausedCh, "paused run release")
	if got.ID() != pausedEventID {
		t.Fatalf("released event = %s, want paused run %s", got.ID(), pausedEventID)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, pausedEventID); got != 1 {
		t.Fatalf("paused run pipeline receipts after continue = %d, want 1", got)
	}
}

func TestEventBusRunControlContinueReleasesPendingDeliveryWithPipelineReceipt(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := testAuthorActivityContext(context.Background())
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	eb, err := newScopedTestEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeruncontrol.NewController(pg, eb, runtimeruncontrol.Options{})
	eb.SetRunDispatchGate(controller)

	agentID := "agent-run-control-acked"
	eventType := events.EventType("custom.run_control.acked")
	runID := uuid.NewString()
	seedRunControlTestRun(t, ctx, db, runID)
	agentIdentity := seedActiveRuntimeBusAgent(t, ctx, pg, runID, agentID)
	ch := runtimebustest.SubscribeForRun(t, eb, runID, agentID, eventType)
	defer runtimebustest.UnsubscribeIdentity(eb, agentIdentity)

	if _, err := controller.Pause(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "test", ControlledBy: "test"}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	eventID := uuid.NewString()
	if err := eb.Publish(ctx, eventtest.ExistingRunRootIngress(
		eventID,
		eventType,
		"api.v1",
		"",
		[]byte(`{"entity_id":"21000000-0000-0000-0000-000000000004"}`),
		0,
		runID,
		events.EnvelopeForEntityID(events.EventEnvelope{}, "21000000-0000-0000-0000-000000000004"),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("Publish paused run event: %v", err)
	}
	requireNoBusEvent(t, ch, "paused run with pipeline receipt before continue")
	acknowledgePipelineTestEvent(t, ctx, pg, eventID)
	if got := countPipelineReceiptsForEvent(t, ctx, db, eventID); got != 1 {
		t.Fatalf("queued event pipeline receipts = %d, want 1", got)
	}

	result, err := controller.Continue(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "test", ControlledBy: "test"})
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if result.Recovery.Sweep.Settled != 0 {
		t.Fatalf("released pipeline obligations = %d, want 0 for already acknowledged event", result.Recovery.Sweep.Settled)
	}
	requireNoBusEvent(t, ch, "acknowledged event is not re-dispatched by the pipeline owner")
}

func TestEventBusRunControlPauseQueuesBeforeInterceptorsAndContinueReplaysThem(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := testAuthorActivityContext(context.Background())
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	eventType := events.EventType("custom.run_control.intercepted")
	deferredType := events.EventType("custom.run_control.deferred")
	recorder := &runControlRecordingInterceptor{
		triggerType:  eventType,
		deferredType: deferredType,
	}
	eb, err := newScopedTestEventBus(pg, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{recorder},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	controller := runtimeruncontrol.NewController(pg, eb, runtimeruncontrol.Options{})
	eb.SetRunDispatchGate(controller)

	agentID := "agent-run-control-interceptor"
	runID := uuid.NewString()
	seedRunControlTestRun(t, ctx, db, runID)
	agentIdentity := seedActiveRuntimeBusAgent(t, ctx, pg, runID, agentID)
	ch := runtimebustest.SubscribeForRun(t, eb, runID, agentID, eventType, deferredType)
	defer runtimebustest.UnsubscribeIdentity(eb, agentIdentity)

	if _, err := controller.Pause(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "test", ControlledBy: "test"}); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	queuedEventID := uuid.NewString()
	if err := eb.Publish(ctx, eventtest.ExistingRunRootIngress(
		queuedEventID,
		eventType,
		"api.v1",
		"",
		[]byte(`{"entity_id":"22000000-0000-0000-0000-000000000001"}`),
		0,
		runID,
		events.EnvelopeForEntityID(events.EventEnvelope{}, "22000000-0000-0000-0000-000000000001"),
		time.Now().UTC(),
	)); err != nil {
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
	if result.Recovery.Sweep.Settled != 1 {
		t.Fatalf("released deliveries = %d, want 1", result.Recovery.Sweep.Settled)
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

	ctx := testAuthorActivityContext(context.Background())
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	eventType := events.EventType("custom.run_control.postcommit")
	deferredType := events.EventType("custom.run_control.postcommit.deferred")
	recorder := &runControlRecordingInterceptor{
		triggerType:  eventType,
		deferredType: deferredType,
	}
	eb, err := newScopedTestEventBus(pg, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{recorder},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	controller := runtimeruncontrol.NewController(pg, eb, runtimeruncontrol.Options{})
	eb.SetRunDispatchGate(controller)

	agentID := "agent-run-control-postcommit"
	runID := uuid.NewString()
	seedRunControlTestRun(t, ctx, db, runID)
	agentIdentity := seedActiveRuntimeBusAgent(t, ctx, pg, runID, agentID)
	ch := runtimebustest.SubscribeForRun(t, eb, runID, agentID, eventType, deferredType)
	defer runtimebustest.UnsubscribeIdentity(eb, agentIdentity)

	intent := runtimeengine.EmitIntent{
		Event: eventtest.ExistingRunRootIngress(
			uuid.NewString(),
			eventType,
			"runtime",
			"",
			[]byte(`{"entity_id":"23000000-0000-0000-0000-000000000001"}`),
			0,
			runID,
			events.EnvelopeForEntityID(events.EventEnvelope{}, "23000000-0000-0000-0000-000000000001"),
			time.Now().UTC(),
		),
	}
	if _, err := controller.Pause(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "test", ControlledBy: "test"}); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if err := eb.PublishAcknowledged(ctx, intent.Event); err != nil {
		t.Fatalf("PublishAcknowledged while paused: %v", err)
	}
	requireNoBusEvent(t, ch, "paused post-commit dispatch before continue")
	if got := recorder.count(); got != 0 {
		t.Fatalf("post-commit interceptor executions while paused = %d, want 0", got)
	}
	if got := countPipelineReceiptsForEvent(t, ctx, db, intent.Event.ID()); got != 0 {
		t.Fatalf("post-commit event receipts while paused = %d, want 0", got)
	}
	blockedWaitCtx, blockedWaitCancel := context.WithTimeout(ctx, 5*time.Second)
	if err := eb.WaitForQuiescence(blockedWaitCtx); err != nil {
		blockedWaitCancel()
		t.Fatalf("wait for paused post-commit owner to release its claim: %v", err)
	}
	blockedWaitCancel()

	result, err := controller.Continue(ctx, runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "test", ControlledBy: "test"})
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if result.Recovery.Sweep.Settled != 1 {
		t.Fatalf("released deliveries = %d, want 1", result.Recovery.Sweep.Settled)
	}
	if got := recorder.count(); got != 1 {
		t.Fatalf("post-commit interceptor executions after continue = %d, want 1", got)
	}
	requireBusEventTypes(t, ch, "released post-commit original and deferred events", eventType, deferredType)
	if got := countPipelineReceiptsForEvent(t, ctx, db, intent.Event.ID()); got != 1 {
		t.Fatalf("post-commit event receipts after continue = %d, want 1", got)
	}
}

func seedRunControlTestRun(t *testing.T, ctx context.Context, db *sql.DB, runID string) {
	t.Helper()
	runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, BundleHash: authorActivityTestBundleHash, BundleSource: authorActivityTestBundleSource})
}

type runControlRecordingInterceptor struct {
	mu           sync.Mutex
	triggerType  events.EventType
	deferredType events.EventType
	seen         []string
}

func (i *runControlRecordingInterceptor) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if evt.Type() != i.triggerType {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	i.mu.Lock()
	i.seen = append(i.seen, evt.ID())
	i.mu.Unlock()
	return true, []events.Event{eventtest.PersistedChildForProducer(
		uuid.NewString(),
		i.deferredType,
		eventtest.Producer(events.EventProducerPlatform, "runtime"),
		"",
		[]byte(`{"entity_id":"22000000-0000-0000-0000-000000000002"}`),
		evt.ChainDepth()+1,
		evt.RunID(),
		evt.ID(),
		events.EnvelopeForEntityID(events.EventEnvelope{}, "22000000-0000-0000-0000-000000000002"),
		time.Now().UTC(),
	)}, runtimepipelineobligation.Continue(), nil
}

func (i *runControlRecordingInterceptor) count() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.seen)
}
