package manager

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
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
			store := &receiptReaderStub{}
			bus := &recordingReceiptBus{}
			am := newTestAgentManager(t, bus, nil, store)
			err := tt.newFailure()
			expected := runtimeengine.NormalizeFailure(err, "agent-manager", "process_event.on_event").Failure
			evt := eventtest.RunCreatingRootIngress("evt-"+tt.name, events.EventType("work.requested"), "", "", nil, 0, "run-1", "", events.EventEnvelope{}, time.Time{})
			result := am.processEventDetailed(testAuthorActivityContext(context.Background()), failureReturningAgent{id: "agent-a", err: err}, evt)

			if result.err == nil {
				t.Fatal("processEventDetailed error = nil")
			}
			if result.record.Failure == nil {
				t.Fatal("startup replay record failure = nil")
			}
			wantStatus := receiptStatusForAgentFailure(err)
			if store.lastStatus != wantStatus || store.lastFailure == nil {
				t.Fatalf("receipt = status:%q failure:%#v, want status %q with typed failure", store.lastStatus, store.lastFailure, wantStatus)
			}
			assertManagerFailureEqual(t, *result.record.Failure, expected)
			assertManagerFailureEqual(t, *store.lastFailure, expected)
			returned, ok := runtimefailures.EnvelopeFromError(result.err)
			if !ok {
				t.Fatalf("returned error = %v, want canonical failure", result.err)
			}
			assertManagerFailureEqual(t, returned, expected)
		})
	}
}

func TestProcessEventOutcomeUncertainTerminalReceiptSuppressesReplay(t *testing.T) {
	store := &receiptReaderStub{}
	am := newTestAgentManager(t, &recordingReceiptBus{}, nil, store)
	err := runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "claude_cli_attempt_outcome_unconfirmed", "claude-cli-adapter", "wait", nil)
	agent := &countingFailureAgent{failureReturningAgent: failureReturningAgent{id: "agent-a", err: err}}
	evt := eventtest.RunCreatingRootIngress("evt-uncertain", events.EventType("work.requested"), "", "", nil, 0, "run-1", "", events.EventEnvelope{}, time.Time{})
	first := am.processEventDetailed(testAuthorActivityContext(context.Background()), agent, evt)
	if first.err == nil || agent.calls != 1 || store.lastStatus != ReceiptStatusTerminal || store.lastFailure == nil {
		t.Fatalf("first result err=%v calls=%d status=%s failure=%#v", first.err, agent.calls, store.lastStatus, store.lastFailure)
	}
	store.receipt = EventReceipt{EventID: evt.ID(), AgentID: agent.ID(), Status: ReceiptStatusDeadLetter, RetryCount: 0, Failure: runtimefailures.CloneEnvelope(store.lastFailure)}
	store.found = true
	second := am.processEventDetailed(testAuthorActivityContext(context.Background()), agent, evt)
	if second.err != nil || agent.calls != 1 || second.record.ReasonCode != startupManagerReplayReasonReceiptDeadLettered {
		t.Fatalf("replay result err=%v calls=%d record=%#v", second.err, agent.calls, second.record)
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
