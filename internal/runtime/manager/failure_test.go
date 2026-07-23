package manager

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
)

func testFailure(detailCode string) *runtimefailures.Envelope {
	failure := runtimefailures.Normalize(
		runtimefailures.New(runtimefailures.ClassConnectorFailure, detailCode, "manager-test", "delivery", nil),
		"manager-test",
		"delivery",
	)
	return &failure
}

func testAuthFailure() runtimefailures.Envelope {
	return runtimefailures.Normalize(
		runtimefailures.New(runtimefailures.ClassAuthenticationNeeded, "credential_required", "manager-test", "authenticate", map[string]any{"auth_kind": "provider"}),
		"manager-test",
		"authenticate",
	)
}

type failureReturningAgent struct {
	id  string
	err error
}

type countingFailureAgent struct {
	failureReturningAgent
	calls int
}

func (a *countingFailureAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	a.calls++
	return nil, a.err
}

func (a failureReturningAgent) ID() string                      { return a.id }
func (failureReturningAgent) Type() string                      { return "test" }
func (failureReturningAgent) Subscriptions() []events.EventType { return nil }
func (a failureReturningAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, a.err
}

func TestProcessEventPreservesAgentFailureEnvelopeAcrossReceiptAndReplayRecord(t *testing.T) {
	tests := []struct {
		name       string
		newFailure func() error
	}{
		{name: "authentication", newFailure: func() error {
			return runtimefailures.New(runtimefailures.ClassAuthenticationNeeded, "provider_credential_missing", "test-agent", "call_provider", map[string]any{"auth_kind": "provider_credential"})
		}},
		{name: "credit", newFailure: func() error {
			return runtimefailures.New(runtimefailures.ClassConnectorFailure, "provider_credit_exhausted", "test-agent", "call_provider", map[string]any{"status": 402})
		}},
		{name: "timeout", newFailure: func() error {
			return runtimefailures.New(runtimefailures.ClassTimeout, "provider_request_timeout", "test-agent", "call_provider", nil)
		}},
		{name: "budget", newFailure: func() error {
			return runtimefailures.New(runtimefailures.ClassBudgetExhausted, "agent_turn_limit_reached", "test-agent", "run_turn", map[string]any{"budget_kind": "agent_turns", "limit": 12, "actual": 13})
		}},
		{name: "internal", newFailure: func() error {
			return runtimefailures.New(runtimefailures.ClassInternalFailure, "agent_runtime_defect", "test-agent", "run_turn", nil)
		}},
		{name: "direct dead letter", newFailure: func() error {
			return runtimeengine.ErrChainDepthExceeded
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deliveryStore := newManagerDeliveryTestStore(t)
			bus := &recordingReceiptBus{}
			am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{DeliveryStore: deliveryStore})
			err := tt.newFailure()
			expected := runtimeengine.NormalizeFailure(err, "agent-manager", "process_event.on_event").Failure
			evt := eventtest.RunCreatingRootIngress(eventtest.UUID("evt-"+tt.name), events.EventType("work.requested"), "", "", nil, 0, eventtest.UUID("failure-run-"+tt.name), "", events.EventEnvelope{}, time.Time{})
			agent := failureReturningAgent{id: "agent-a", err: err}
			result := am.processEventDetailed(managerAgentDeliveryContext(testAuthorActivityContext(context.Background()), agent.ID()), agent, evt)

			if result.err == nil {
				t.Fatal("processEventDetailed error = nil")
			}
			if result.record.Failure == nil {
				t.Fatal("startup replay record failure = nil")
			}
			wantStatus := receiptStatusForAgentFailure(err)
			obligation, obligationErr := runtimedelivery.NewObligation(evt.ID(), evt.RunID(), managerAgentDeliveryRoute(agent.ID()))
			if obligationErr != nil {
				t.Fatalf("derive failure delivery obligation: %v", obligationErr)
			}
			snapshot, snapshotErr := deliveryStore.Snapshot(context.Background(), obligation.DeliveryID())
			if snapshotErr != nil {
				t.Fatalf("load failure delivery snapshot: %v", snapshotErr)
			}
			wantDeliveryStatus := runtimedelivery.StatusDeadLetter
			if wantStatus == ReceiptStatusError {
				wantDeliveryStatus = runtimedelivery.StatusFailed
			}
			if snapshot.Status != wantDeliveryStatus || snapshot.Failure == nil {
				t.Fatalf("delivery = status:%q failure:%#v, want status %q with typed failure", snapshot.Status, snapshot.Failure, wantDeliveryStatus)
			}
			assertManagerFailureEqual(t, *result.record.Failure, expected)
			assertManagerFailureEqual(t, *snapshot.Failure, expected)
			returned, ok := runtimefailures.EnvelopeFromError(result.err)
			if !ok {
				t.Fatalf("returned error = %v, want canonical failure", result.err)
			}
			assertManagerFailureEqual(t, returned, expected)
		})
	}
}

func TestRunningManagerInterventionFailureSettlesClaimBeforeShutdownAndRecovery(t *testing.T) {
	tests := []struct {
		name       string
		newFailure func() error
	}{
		{name: "authentication_needed", newFailure: func() error {
			return runtimefailures.New(runtimefailures.ClassAuthenticationNeeded, "provider_credential_missing", "test-agent", "call_provider", map[string]any{"auth_kind": "provider_credential"})
		}},
		{name: "provider_credit_exhausted", newFailure: func() error {
			return runtimefailures.New(runtimefailures.ClassConnectorFailure, "provider_credit_exhausted", "test-agent", "call_provider", map[string]any{"status": 402})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runtimebus.ResumeRuntimeIngress()
			t.Cleanup(runtimebus.ResumeRuntimeIngress)

			deliveryStore := newManagerDeliveryTestStore(t)
			persistence := &startupReplayTestStore{
				recoveryTestStore:        recoveryTestStore{},
				managerDeliveryTestStore: deliveryStore,
			}
			probe := lifecycletest.New(t)
			eventBus, err := runtimebus.NewEventBusWithOptions(nil, runtimebus.EventBusOptions{WorkOwner: newTestManagerWorkOwner(t)})
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			var calls atomic.Int32
			agent := shutdownTestAgent{
				id:            "intervention-agent",
				subscriptions: []events.EventType{"test.intervention"},
				onEvent: func(context.Context, events.Event) ([]events.Event, error) {
					calls.Add(1)
					return nil, tt.newFailure()
				},
			}
			newFactory := func(runtimeactors.AgentConfig) (Agent, error) { return agent, nil }
			manager := newTestAgentManagerWithOptions(t, eventBus, newFactory, AgentManagerOptions{
				DeliveryStore:      deliveryStore,
				TestLifecycleProbe: probe.Raw(),
			}, persistence)
			if err := manager.spawnAgentInternal(testAuthorActivityContext(context.Background()), PersistedAgent{Config: runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            agent.ID(),
				Subscriptions: []string{"test.intervention"},
			}}, false); err != nil {
				t.Fatalf("spawn intervention agent: %v", err)
			}
			if err := manager.Run(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
				t.Fatalf("run intervention manager: %v", err)
			}
			managerRunCtx, _, running := manager.lifecycle.runSnapshot()
			if !running || managerRunCtx == nil {
				t.Fatal("intervention manager is not running")
			}

			inbound := eventtest.RunCreatingRootIngress(
				eventtest.UUID("intervention-"+tt.name),
				events.EventType("test.intervention"),
				"test", "", nil, 0,
				eventtest.UUID("intervention-run-"+tt.name),
				"", events.EventEnvelope{}, time.Now().UTC(),
			)
			if err := eventBus.Publish(testAuthorActivityContext(context.Background()), inbound); err != nil {
				t.Fatalf("publish intervention event: %v", err)
			}
			probe.RequireAgentInProgress(inbound.ID(), agent.ID())
			probe.RequireAgentDeadLetter(inbound.ID(), agent.ID())
			select {
			case <-managerRunCtx.Done():
			case <-time.After(time.Second):
				t.Fatal("intervention failure did not request shared shutdown after settlement")
			}
			if err := manager.Shutdown(); err != nil {
				t.Fatalf("join intervention shutdown: %v", err)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("agent calls = %d, want 1", got)
			}

			obligation, err := runtimedelivery.NewObligation(inbound.ID(), inbound.RunID(), managerAgentDeliveryRoute(agent.ID()))
			if err != nil {
				t.Fatalf("derive intervention obligation: %v", err)
			}
			snapshot, err := deliveryStore.Snapshot(context.Background(), obligation.DeliveryID())
			if err != nil {
				t.Fatalf("load settled intervention delivery: %v", err)
			}
			if snapshot.Status != runtimedelivery.StatusDeadLetter || !snapshot.Terminal() || snapshot.Failure == nil {
				t.Fatalf("settled intervention delivery = status:%q terminal:%t failure:%#v, want terminal dead letter with failure", snapshot.Status, snapshot.Terminal(), snapshot.Failure)
			}
			outcomes, err := deliveryStore.Outcomes(context.Background(), obligation.DeliveryID())
			if err != nil {
				t.Fatalf("load intervention outcomes: %v", err)
			}
			if len(outcomes) != 1 || outcomes[0].Outcome != "dead_letter" || outcomes[0].ClaimVersion != snapshot.ClaimVersion || outcomes[0].Failure == nil {
				t.Fatalf("intervention outcomes = %#v, want one exact dead-letter outcome at claim version %d", outcomes, snapshot.ClaimVersion)
			}
			summary, err := deliveryStore.SummarizeRun(context.Background(), inbound.RunID())
			if err != nil {
				t.Fatalf("summarize intervention run: %v", err)
			}
			if !summary.Settled() || summary.InProgress != 0 || summary.DeadLetter != 1 {
				t.Fatalf("intervention run summary = %#v, want settled with one dead letter", summary)
			}

			recoveryBus, err := runtimebus.NewEventBusWithOptions(nil, runtimebus.EventBusOptions{WorkOwner: newTestManagerWorkOwner(t)})
			if err != nil {
				t.Fatalf("NewEventBus(recovery): %v", err)
			}
			recoveryManager := newTestAgentManagerWithOptions(t, recoveryBus, newFactory, AgentManagerOptions{
				DeliveryStore: deliveryStore,
			}, persistence)
			if err := recoveryManager.spawnAgentInternal(testAuthorActivityContext(context.Background()), PersistedAgent{Config: runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            agent.ID(),
				Subscriptions: []string{"test.intervention"},
			}}, false); err != nil {
				t.Fatalf("spawn recovery agent: %v", err)
			}
			if err := recoveryManager.Run(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
				t.Fatalf("run recovery manager: %v", err)
			}
			if err := recoveryManager.ReplayAgentBacklog(testAuthorActivityContext(context.Background()), agent.ID()); err != nil {
				t.Fatalf("replay intervention backlog: %v", err)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("agent calls after recovery = %d, want settled delivery not to execute again", got)
			}
		})
	}
}

func TestProcessEventOutcomeUncertainTerminalDeliverySuppressesReplay(t *testing.T) {
	deliveryStore := newManagerDeliveryTestStore(t)
	store := &startupReplayTestStore{recoveryTestStore: recoveryTestStore{}, managerDeliveryTestStore: deliveryStore}
	am := newTestAgentManager(t, &recordingReceiptBus{}, nil, store)
	err := runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "claude_cli_attempt_outcome_unconfirmed", "claude-cli-adapter", "wait", nil)
	agent := &countingFailureAgent{failureReturningAgent: failureReturningAgent{id: "agent-a", err: err}}
	evt := eventtest.RunCreatingRootIngress(eventtest.UUID("evt-uncertain"), events.EventType("work.requested"), "", "", nil, 0, eventtest.UUID("uncertain-run"), "", events.EventEnvelope{}, time.Time{})
	deliveryStore.seedAgentDeliveries(t, agent.ID(), []events.Event{evt})
	route := events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: agent.ID()}
	ctx := runtimedelivery.WithRoute(testAuthorActivityContext(context.Background()), route)
	first := am.processEventDetailed(ctx, agent, evt)
	if first.err == nil || agent.calls != 1 {
		t.Fatalf("first result err=%v calls=%d, want one terminal failure", first.err, agent.calls)
	}
	backlog, err := deliveryStore.ClaimAgentBacklog(testAuthorActivityContext(context.Background()), agent.ID(), 10)
	if err != nil {
		t.Fatalf("claim terminal delivery backlog: %v", err)
	}
	if len(backlog) != 0 || agent.calls != 1 {
		t.Fatalf("terminal delivery backlog=%#v calls=%d, want no replay", backlog, agent.calls)
	}
}

func TestProcessEventSelectedForkTerminalizesRetryableFailureBeforeRuntimeRetirement(t *testing.T) {
	deliveryStore := newManagerDeliveryTestStore(t)
	am := newTestAgentManagerWithOptions(t, &recordingReceiptBus{}, nil, AgentManagerOptions{DeliveryStore: deliveryStore})
	agent := failureReturningAgent{
		id:  "selected-agent",
		err: runtimefailures.New(runtimefailures.ClassTimeout, "provider_request_timeout", "selected-agent", "call_provider", nil),
	}
	forkRunID := eventtest.UUID("selected-retry-run")
	evt := eventtest.RunCreatingRootIngress(
		eventtest.UUID("selected-retry-event"), events.EventType("work.requested"), "", "", nil, 0,
		forkRunID, "", events.EventEnvelope{}, time.Time{},
	)
	admission, err := managedexecution.New(
		managedexecution.KindSelectedContractFork,
		eventtest.UUID("selected-retry-execution"),
		1,
		forkRunID,
		"selected-retry-actors",
		"selected-retry-bundle",
		nil,
	)
	if err != nil {
		t.Fatalf("managedexecution.New: %v", err)
	}
	ctx := managerAgentDeliveryContext(testAuthorActivityContext(context.Background()), agent.ID())
	ctx = managedexecution.WithAdmission(ctx, admission)
	result := am.processEventDetailed(ctx, agent, evt)
	if result.err == nil {
		t.Fatal("selected-fork retryable handler failure returned nil")
	}

	obligation, err := runtimedelivery.NewObligation(evt.ID(), evt.RunID(), managerAgentDeliveryRoute(agent.ID()))
	if err != nil {
		t.Fatalf("derive selected-fork delivery obligation: %v", err)
	}
	snapshot, err := deliveryStore.Snapshot(context.Background(), obligation.DeliveryID())
	if err != nil {
		t.Fatalf("load selected-fork delivery snapshot: %v", err)
	}
	if snapshot.Status != runtimedelivery.StatusDeadLetter || snapshot.ReasonCode != "terminal_failure" || snapshot.RetryCount != 0 {
		t.Fatalf("selected-fork delivery = status:%s reason:%s retries:%d, want terminal dead letter without retry", snapshot.Status, snapshot.ReasonCode, snapshot.RetryCount)
	}
	backlog, err := deliveryStore.ClaimAgentBacklog(testAuthorActivityContext(context.Background()), agent.ID(), 1)
	if err != nil {
		t.Fatalf("claim normal-manager backlog after selected failure: %v", err)
	}
	if len(backlog) != 0 {
		t.Fatalf("normal-manager backlog claimed selected-fork delivery: %#v", backlog)
	}
}

func TestQuarantineCarriesTriggeringPanicFailureWithoutReclassification(t *testing.T) {
	bus := &recordingReceiptBus{}
	am := newTestAgentManager(t, bus, nil)
	failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassInternalFailure, "agent_event_panic", "agent-manager", "process_event", map[string]any{"agent_id": "agent-a"}), "agent-manager", "process_event")
	for i := 0; i < poisonEventEntityThreshold; i++ {
		evt := eventtest.RunCreatingRootIngress("evt-quarantine", events.EventType("work.requested"), "", "", nil, 0, "run-1", "", events.EventEnvelope{EntityID: "entity-" + string(rune('a'+i))}, time.Time{})
		am.quarantinePoisonEvent(testAuthorActivityContext(context.Background()), "agent-a", evt, poisonPanicQuarantineAt, failure)
	}
	if len(bus.published) != 1 || bus.published[0].Type() != events.EventType("platform.event_quarantined") {
		t.Fatalf("published = %#v, want one quarantine event", bus.published)
	}
	var payload struct {
		LastFailure runtimefailures.Envelope `json:"last_failure"`
	}
	if err := json.Unmarshal(bus.published[0].Payload(), &payload); err != nil {
		t.Fatalf("unmarshal quarantine payload: %v", err)
	}
	assertManagerFailureEqual(t, payload.LastFailure, failure)
}

func assertManagerFailureEqual(t testing.TB, got, want runtimefailures.Envelope) {
	t.Helper()
	gotRaw, err := runtimefailures.MarshalEnvelope(got)
	if err != nil {
		t.Fatalf("marshal got failure: %v", err)
	}
	wantRaw, err := runtimefailures.MarshalEnvelope(want)
	if err != nil {
		t.Fatalf("marshal want failure: %v", err)
	}
	if string(gotRaw) != string(wantRaw) {
		t.Fatalf("failure = %s, want %s", gotRaw, wantRaw)
	}
}
