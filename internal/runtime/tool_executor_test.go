package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
)

type mailboxStoreStub struct {
	last MailboxItem
}

func (m *mailboxStoreStub) InsertMailboxItem(_ context.Context, item MailboxItem) (string, error) {
	m.last = item
	if item.ID == "" {
		return "m-1", nil
	}
	return item.ID, nil
}
func (m *mailboxStoreStub) ListMailboxItems(context.Context, string, int) ([]MailboxItem, error) {
	return nil, nil
}
func (m *mailboxStoreStub) CountMailboxItems(context.Context, string) (int, error) { return 0, nil }
func (m *mailboxStoreStub) GetMailboxItem(context.Context, string) (MailboxItem, error) {
	return MailboxItem{}, nil
}
func (m *mailboxStoreStub) ExpireMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}
func (m *mailboxStoreStub) ListUnnotifiedCriticalMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}
func (m *mailboxStoreStub) MarkMailboxItemNotified(context.Context, string) error { return nil }
func (m *mailboxStoreStub) DecideMailboxItem(context.Context, string, string, string, string) error {
	return nil
}

func TestRuntimeToolExecutor_AgentMessage(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	ch := bus.Subscribe("t", events.EventType("agent.message"))
	manager := NewAgentManager(bus, nil)
	if err := manager.SpawnAgent(models.AgentConfig{ID: "t", Role: "vp-product", Mode: "operating", VerticalID: "v1"}); err != nil {
		t.Fatalf("spawn target agent: %v", err)
	}
	exec := NewRuntimeToolExecutor(bus, nil, manager)
	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:         "a1",
		Role:       "opco-ceo",
		Mode:       "operating",
		VerticalID: "v1",
	})

	_, err := exec.Execute(ctx, "agent_message", map[string]any{
		"target_agent_id": "t",
		"event_type":      "agent.message",
		"payload":         map[string]any{"ok": true},
	})
	if err != nil {
		t.Fatalf("execute agent_message: %v", err)
	}

	select {
	case evt := <-ch:
		if evt.SourceAgent != "a1" {
			t.Fatalf("unexpected source agent: %s", evt.SourceAgent)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected published event")
	}
}

func TestRuntimeToolExecutor_Schedule(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	ch := bus.Subscribe("t", events.EventType("timer.tick"))
	s := NewScheduler(func(sc Schedule) {
		_ = bus.Publish(context.Background(), events.Event{
			ID:          "id-1",
			Type:        events.EventType(sc.EventType),
			SourceAgent: sc.AgentID,
			Payload:     sc.Payload,
			CreatedAt:   time.Now(),
		})
	})
	defer s.Stop()

	exec := NewRuntimeToolExecutor(bus, s, nil)
	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:         "a1",
		Role:       "opco-ceo",
		Mode:       "operating",
		VerticalID: "v1",
	})
	at := time.Now().Add(40 * time.Millisecond).UTC().Format(time.RFC3339)
	_, err := exec.Execute(ctx, "schedule", map[string]any{
		"agent_id":   "a1",
		"event_type": "timer.tick",
		"mode":       "once",
		"at":         at,
		"payload":    map[string]any{"n": 1},
	})
	if err != nil {
		t.Fatalf("execute schedule: %v", err)
	}

	select {
	case <-ch:
	case <-time.After(800 * time.Millisecond):
		t.Fatal("expected scheduled event to fire")
	}
}

func TestRuntimeToolExecutor_AgentHireFire(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	manager := NewAgentManager(bus, nil)
	exec := NewRuntimeToolExecutor(bus, nil, manager)
	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:         "ceo-v1",
		Role:       "opco-ceo",
		Mode:       "operating",
		VerticalID: "v1",
	})

	_, err := exec.Execute(ctx, "agent_hire", map[string]any{
		"vertical_id": "v1",
		"config": map[string]any{
			"id":            "a-hire",
			"type":          "worker",
			"role":          "r1",
			"mode":          "factory",
			"subscriptions": []string{"x.y"},
		},
	})
	if err != nil {
		t.Fatalf("agent_hire failed: %v", err)
	}
	if manager.Count() != 1 {
		t.Fatalf("expected manager count 1, got %d", manager.Count())
	}

	_, err = exec.Execute(ctx, "agent_fire", map[string]any{
		"agent_id": "a-hire",
	})
	if err != nil {
		t.Fatalf("agent_fire failed: %v", err)
	}
	if manager.Count() != 0 {
		t.Fatalf("expected manager count 0, got %d", manager.Count())
	}

	_, err = exec.Execute(ctx, "agent_reconfigure", map[string]any{
		"agent_id": "a-hire",
		"config":   models.AgentConfig{ID: "a-hire"},
	})
	if err == nil {
		t.Fatalf("expected reconfigure on removed agent to fail")
	}
}

func TestRuntimeToolExecutor_ConfigureRouting(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	ch := bus.Subscribe("watcher", events.EventType("opco.routing_updated"))
	manager := NewAgentManager(bus, nil)
	_ = manager.SpawnAgent(models.AgentConfig{
		ID:            "a1",
		Type:          "worker",
		Role:          "marketing-agent",
		Mode:          "operating",
		VerticalID:    "v1",
		Subscriptions: []string{"foo.*"},
	})
	exec := NewRuntimeToolExecutor(bus, nil, manager)
	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:         "ceo-v1",
		Role:       "opco-ceo",
		Mode:       "operating",
		VerticalID: "v1",
	})

	_, err := exec.Execute(ctx, "configure_routing", map[string]any{
		"vertical_id":   "v1",
		"event_pattern": "foo.*",
		"subscriber_id": "a1",
		"installed_by":  "ceo-v1",
	})
	if err != nil {
		t.Fatalf("configure_routing failed: %v", err)
	}
	rt := bus.GetRoutingTable("v1")
	if rt == nil || len(rt.Routes) != 1 {
		t.Fatalf("expected one route")
	}
	if rt.Routes[0].EventPattern != "foo.*" {
		t.Fatalf("unexpected pattern: %s", rt.Routes[0].EventPattern)
	}

	b, _ := json.Marshal(rt.Routes[0])
	if len(b) == 0 {
		t.Fatal("expected marshalable route")
	}
	select {
	case evt := <-ch:
		if string(evt.Type) != "opco.routing_updated" {
			t.Fatalf("unexpected event type: %s", evt.Type)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected opco.routing_updated event")
	}
}

func TestRuntimeToolExecutor_ConfigureRoutingCoSRequiresProposed(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	manager := NewAgentManager(bus, nil)
	_ = manager.SpawnAgent(models.AgentConfig{
		ID:            "a2",
		Type:          "worker",
		Role:          "vp-growth",
		Mode:          "operating",
		VerticalID:    "v1",
		Subscriptions: []string{"foo.*"},
	})
	exec := NewRuntimeToolExecutor(bus, nil, manager)
	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:         "cos-v1",
		Role:       "chief-of-staff",
		Mode:       "operating",
		VerticalID: "v1",
	})

	_, err := exec.Execute(ctx, "configure_routing", map[string]any{
		"vertical_id":   "v1",
		"event_pattern": "foo.*",
		"subscriber_id": "a2",
		"status":        "active",
	})
	if err == nil {
		t.Fatal("expected CoS active routing request to be rejected")
	}
}

func TestRuntimeToolExecutor_ConfigureRoutingRejectsBootstrapMutation(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	manager := NewAgentManager(bus, nil)
	if err := manager.SpawnOpCo("v1", models.MandateDocument{}); err != nil {
		t.Fatalf("spawn opco: %v", err)
	}
	exec := NewRuntimeToolExecutor(bus, nil, manager)
	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:         "opco-ceo-v1",
		Role:       "opco-ceo",
		Mode:       "operating",
		VerticalID: "v1",
	})

	_, err := exec.Execute(ctx, "configure_routing", map[string]any{
		"vertical_id":   "v1",
		"event_pattern": "product_spec_ready",
		"subscriber_id": "cto-agent-v1",
		"status":        "deactivated",
	})
	if err == nil {
		t.Fatal("expected bootstrap route mutation to be rejected")
	}
}

func TestRuntimeToolExecutor_MailboxSend(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	manager := NewAgentManager(bus, nil)
	exec := NewRuntimeToolExecutor(bus, nil, manager)
	ms := &mailboxStoreStub{}
	exec.SetMailboxStore(ms)

	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:         "ceo-v1",
		Role:       "opco-ceo",
		Mode:       "operating",
		VerticalID: "v1",
	})
	out, err := exec.Execute(ctx, "mailbox_send", map[string]any{
		"type":       "spend_request",
		"priority":   "normal",
		"summary":    "Need budget",
		"context":    map[string]any{"amount": 12},
		"timeout_at": time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("mailbox_send failed: %v", err)
	}
	m, ok := out.(map[string]any)
	if !ok || m["status"] != "queued" {
		t.Fatalf("unexpected output: %#v", out)
	}
	if ms.last.Type != "spend_request" || ms.last.FromAgent != "ceo-v1" {
		t.Fatalf("unexpected mailbox item: %+v", ms.last)
	}
}

func TestRuntimeToolExecutor_SQLExecuteRequiresDB(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:         "ceo-v1",
		Role:       "opco-ceo",
		Mode:       "operating",
		VerticalID: "v1",
	})
	_, err := exec.Execute(ctx, "sql_execute", map[string]any{
		"query": "SELECT 1",
	})
	if err == nil {
		t.Fatal("expected sql_execute to fail without db")
	}
}

func TestRuntimeToolExecutor_RespectsAllowedToolsFromConfig(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:         "ceo-v1",
		Role:       "opco-ceo",
		Mode:       "operating",
		VerticalID: "v1",
		Config:     []byte(`{"tools":["agent_message"]}`),
	})
	_, err := exec.Execute(ctx, "schedule", map[string]any{
		"agent_id":   "ceo-v1",
		"event_type": "timer.tick",
		"mode":       "once",
		"at":         time.Now().Add(1 * time.Minute).UTC().Format(time.RFC3339),
	})
	if err == nil {
		t.Fatal("expected disallowed tool usage to fail")
	}
}
