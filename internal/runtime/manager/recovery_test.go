package manager

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	runtimepipeline "swarm/internal/runtime/pipeline"
)

type recoveryTestBus struct {
	storedRoutes []runtimebus.FlowInstanceRouteRecord
	restored     []string
}

func (*recoveryTestBus) Publish(context.Context, events.Event) error                 { return nil }
func (*recoveryTestBus) PublishDirect(context.Context, events.Event, []string) error { return nil }
func (*recoveryTestBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (*recoveryTestBus) Unsubscribe(string)                                              {}
func (*recoveryTestBus) ResetInMemoryState() error                                       { return nil }
func (*recoveryTestBus) LogRuntime(context.Context, runtimepipeline.RuntimeLogEntry)     {}
func (b *recoveryTestBus) Store() runtimebus.EventStore                                  { return b }
func (b *recoveryTestBus) AppendEvent(context.Context, events.Event) error               { return nil }
func (b *recoveryTestBus) InsertEventDeliveries(context.Context, string, []string) error { return nil }
func (b *recoveryTestBus) ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (b *recoveryTestBus) UpsertFlowInstanceRoute(context.Context, runtimebus.FlowInstanceRouteRecord) error {
	return nil
}
func (b *recoveryTestBus) DeleteFlowInstanceRoute(context.Context, string, string) error { return nil }
func (b *recoveryTestBus) ListFlowInstanceRoutes(context.Context) ([]runtimebus.FlowInstanceRouteRecord, error) {
	return append([]runtimebus.FlowInstanceRouteRecord(nil), b.storedRoutes...), nil
}
func (b *recoveryTestBus) AddFlowInstance(_ runtimecontracts.SystemNodeContract, instancePath string) error {
	b.restored = append(b.restored, instancePath)
	return nil
}

type recoveryTestStore struct {
	agents []PersistedAgent
}

func (s *recoveryTestStore) UpsertAgent(context.Context, PersistedAgent) error { return nil }
func (s *recoveryTestStore) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return append([]PersistedAgent(nil), s.agents...), nil
}
func (s *recoveryTestStore) MarkAgentTerminated(context.Context, string) error { return nil }
func (s *recoveryTestStore) EnsureEntitySchema(context.Context, string) error  { return nil }
func (s *recoveryTestStore) UpsertEventReceipt(context.Context, string, string, ReceiptStatus, string) error {
	return nil
}
func (s *recoveryTestStore) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (s *recoveryTestStore) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

func TestRecoverRestoresPersistedFlowInstanceRoutes(t *testing.T) {
	bus := &recoveryTestBus{
		storedRoutes: []runtimebus.FlowInstanceRouteRecord{{
			TemplateID:   "review",
			InstanceID:   "inst-1",
			InstancePath: "review/inst-1",
		}},
	}
	store := &recoveryTestStore{
		agents: []PersistedAgent{{
			Config: models.AgentConfig{
				ID:       "reviewer-inst-1",
				Role:     "reviewer",
				EntityID: "ent-1",
				Config:   mustRecoveryJSON(t, map[string]any{"tools": []string{"agent_message"}}),
			},
			StartedAt: time.Now().UTC(),
		}},
	}
	am := NewAgentManager(bus, func(cfg models.AgentConfig) (Agent, error) {
		return recoveryTestAgent{id: cfg.ID}, nil
	}, store)

	if err := am.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(bus.restored) != 1 || bus.restored[0] != "review/inst-1" {
		t.Fatalf("restored routes = %#v, want [review/inst-1]", bus.restored)
	}
}

type recoveryTestAgent struct{ id string }

func (a recoveryTestAgent) ID() string                      { return a.id }
func (recoveryTestAgent) Type() string                      { return "generic" }
func (recoveryTestAgent) Subscriptions() []events.EventType { return nil }
func (recoveryTestAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func mustRecoveryJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return raw
}
