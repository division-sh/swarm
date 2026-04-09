package manager

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	runtimeownership "swarm/internal/runtime/core/ownership"
	runtimepipeline "swarm/internal/runtime/pipeline"
)

type recoveryTestBus struct {
	storedRoutes []runtimebus.FlowInstanceRouteRecord
	restored     []string
	replayable   []events.PersistedReplayEvent
	deliveries   map[string][]string
	runtimeLogs  []runtimepipeline.RuntimeLogEntry
	direct       []events.Event
}

func (*recoveryTestBus) Publish(context.Context, events.Event) error                 { return nil }
func (*recoveryTestBus) PublishDirect(context.Context, events.Event, []string) error { return nil }
func (b *recoveryTestBus) PublishPersistedRecipients(_ context.Context, evt events.Event, _ []string) error {
	b.direct = append(b.direct, evt)
	return nil
}
func (*recoveryTestBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (*recoveryTestBus) Unsubscribe(string)        {}
func (*recoveryTestBus) ResetInMemoryState() error { return nil }
func (b *recoveryTestBus) LogRuntime(_ context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	b.runtimeLogs = append(b.runtimeLogs, entry)
	return nil
}
func (b *recoveryTestBus) Store() runtimebus.EventStore                                  { return b }
func (b *recoveryTestBus) AppendEvent(context.Context, events.Event) error               { return nil }
func (b *recoveryTestBus) InsertEventDeliveries(context.Context, string, []string) error { return nil }
func (b *recoveryTestBus) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	return append([]string(nil), b.deliveries[eventID]...), nil
}
func (*recoveryTestBus) ClaimPipelineReplay(context.Context, string) (runtimeownership.Lease, bool, error) {
	return recoveryTestReplayLease{}, true, nil
}
func (b *recoveryTestBus) ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error) {
	return append([]events.PersistedReplayEvent(nil), b.replayable...), nil
}
func (b *recoveryTestBus) UpsertFlowInstanceRoute(context.Context, runtimebus.FlowInstanceRouteRecord) error {
	return nil
}
func (b *recoveryTestBus) DeleteFlowInstanceRoute(context.Context, runtimeflowidentity.Route) error {
	return nil
}
func (b *recoveryTestBus) ListFlowInstanceRoutes(context.Context) ([]runtimeflowidentity.Route, error) {
	out := make([]runtimeflowidentity.Route, 0, len(b.storedRoutes))
	for _, route := range b.storedRoutes {
		out = append(out, route.Identity)
	}
	return out, nil
}
func (b *recoveryTestBus) AddFlowInstanceRoute(_ runtimecontracts.SystemNodeContract, identity runtimeflowidentity.Route) error {
	b.restored = append(b.restored, identity.InstancePath)
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
			Identity: runtimeflowidentity.DeriveRoute("review", "inst-1"),
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

func TestRecover_UsesCanonicalLoadedAgentMetadata(t *testing.T) {
	bus := &recoveryTestBus{}
	store := &recoveryTestStore{
		agents: []PersistedAgent{{
			Config: models.AgentConfig{
				ID:               "reviewer-inst-1",
				Type:             "review-worker",
				Role:             "reviewer",
				Mode:             "review",
				ModelTier:        "sonnet",
				LLMBackend:       "api",
				ConversationMode: "session_per_entity",
				Subscriptions:    []string{"review.ready"},
				EmitEvents:       []string{"review.completed"},
				WorkspaceClass:   "shared_flow",
				ManagerFallback:  "control-plane",
				FlowPath:         "review/inst-1",
				EntityID:         "ent-1",
				ParentAgent:      "control-plane",
				Config: mustRecoveryJSON(t, map[string]any{
					"system_prompt":      "x",
					"subscriptions":      []string{"wrong.subscription"},
					"manager_fallback":   "wrong-manager",
					"conversation_mode":  "task",
					"workspace_class":    "wrong-workspace",
					"max_turns_per_task": 99,
				}),
			},
			StartedAt: time.Now().UTC(),
		}},
	}
	var hydrated models.AgentConfig
	am := NewAgentManager(bus, func(cfg models.AgentConfig) (Agent, error) {
		hydrated = cfg
		return recoveryTestAgent{id: cfg.ID}, nil
	}, store)

	if err := am.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if hydrated.ID != "reviewer-inst-1" {
		t.Fatalf("hydrated id = %q, want reviewer-inst-1", hydrated.ID)
	}
	if hydrated.ConversationMode != "session_per_entity" {
		t.Fatalf("conversation_mode = %q, want session_per_entity", hydrated.ConversationMode)
	}
	if len(hydrated.Subscriptions) != 1 || hydrated.Subscriptions[0] != "review.ready" {
		t.Fatalf("subscriptions = %#v, want [review.ready]", hydrated.Subscriptions)
	}
	if hydrated.ManagerFallback != "control-plane" {
		t.Fatalf("manager_fallback = %q, want control-plane", hydrated.ManagerFallback)
	}
	if hydrated.WorkspaceClass != "shared_flow" {
		t.Fatalf("workspace_class = %q, want shared_flow", hydrated.WorkspaceClass)
	}
	if strings.TrimSpace(hydrated.FlowPath) != "review/inst-1" {
		t.Fatalf("flow_path = %q, want review/inst-1", hydrated.FlowPath)
	}
}

func TestRecover_UsesCanonicalPipelineReplayAftermathDiagnostics(t *testing.T) {
	childID := "evt-replay"
	parentID := "evt-parent"
	bus := &recoveryTestBus{
		replayable: []events.PersistedReplayEvent{{
			Event: events.Event{
				ID:            childID,
				Type:          events.EventType("system.recover"),
				RunID:         "run-1",
				ParentEventID: parentID,
				CreatedAt:     time.Now().UTC(),
			},
		}},
		deliveries: map[string][]string{
			childID: {"agent-a"},
		},
	}
	am := NewAgentManager(bus, func(cfg models.AgentConfig) (Agent, error) {
		return recoveryTestAgent{id: cfg.ID}, nil
	}, &recoveryTestStore{})

	if err := am.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(bus.direct) != 1 || bus.direct[0].ID != childID {
		t.Fatalf("direct replayed events = %#v, want [%s]", bus.direct, childID)
	}
	entry := findManagerRecoveryAftermathLog(t, bus.runtimeLogs, childID, "replayed", "persisted_recipients_replayed")
	if strings.TrimSpace(entry.Component) != "pipeline-recovery" {
		t.Fatalf("runtime log component = %q, want pipeline-recovery", entry.Component)
	}
}

type recoveryTestAgent struct{ id string }

type recoveryTestReplayLease struct{}

func (recoveryTestReplayLease) Release(context.Context) error { return nil }

func findManagerRecoveryAftermathLog(t *testing.T, logs []runtimepipeline.RuntimeLogEntry, eventID, outcome, reason string) runtimepipeline.RuntimeLogEntry {
	t.Helper()
	for _, entry := range logs {
		if strings.TrimSpace(entry.Action) != "startup_recovery_pipeline_replay_aftermath" {
			continue
		}
		if strings.TrimSpace(entry.EventID) != strings.TrimSpace(eventID) {
			continue
		}
		detail, _ := entry.Detail.(map[string]any)
		outcomeText, _ := detail["decision_outcome"].(string)
		reasonText, _ := detail["decision_reason_code"].(string)
		if strings.TrimSpace(outcomeText) != strings.TrimSpace(outcome) {
			continue
		}
		if strings.TrimSpace(reasonText) != strings.TrimSpace(reason) {
			continue
		}
		return entry
	}
	t.Fatalf("missing manager recovery aftermath log for event=%q outcome=%q reason=%q in %#v", eventID, outcome, reason, logs)
	return runtimepipeline.RuntimeLogEntry{}
}

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
