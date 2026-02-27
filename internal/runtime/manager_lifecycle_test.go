package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
)

type workspaceLifecycleStub struct {
	ensureCount int
	stopCount   int
}

func (s *workspaceLifecycleStub) EnsureSystemWorkspaces(context.Context) error { return nil }
func (s *workspaceLifecycleStub) ResolveWorkspace(context.Context, models.AgentConfig) (*WorkspaceTarget, error) {
	return nil, nil
}
func (s *workspaceLifecycleStub) EnsureVerticalWorkspace(context.Context, string) error {
	s.ensureCount++
	return nil
}
func (s *workspaceLifecycleStub) StopVerticalWorkspace(context.Context, string) error {
	s.stopCount++
	return nil
}

func TestManagerReconfigureAgent(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	am := NewAgentManager(bus, nil)

	err := am.SpawnAgent(models.AgentConfig{
		ID:            "a1",
		Type:          "worker",
		Role:          "pm-agent",
		Mode:          "operating",
		VerticalID:    "v1",
		Subscriptions: []string{"a.b"},
	})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}

	err = am.ReconfigureAgent("a1", models.AgentConfig{
		Role:          "support-agent",
		Subscriptions: []string{"x.y"},
	})
	if err != nil {
		t.Fatalf("reconfigure: %v", err)
	}

	cfg, ok := am.GetAgentConfig("a1")
	if !ok {
		t.Fatal("agent config missing after reconfigure")
	}
	if cfg.Role != "support-agent" {
		t.Fatalf("expected role support-agent, got %s", cfg.Role)
	}
	if len(cfg.Subscriptions) != 1 || cfg.Subscriptions[0] != "x.y" {
		t.Fatalf("unexpected subscriptions after reconfigure: %+v", cfg.Subscriptions)
	}
}

func TestManagerTeardownOpCo(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	am := NewAgentManager(bus, nil)

	_ = am.SpawnAgent(models.AgentConfig{
		ID:            "opco-ceo-v1",
		Type:          "operating",
		Role:          "opco-ceo",
		Mode:          "operating",
		VerticalID:    "v1",
		Subscriptions: []string{"x"},
	})
	_ = am.SpawnAgent(models.AgentConfig{
		ID:            "vp-product-v1",
		Type:          "operating",
		Role:          "vp-product",
		Mode:          "operating",
		VerticalID:    "v1",
		Subscriptions: []string{"y"},
	})
	_ = am.SpawnAgent(models.AgentConfig{
		ID:            "factory-1",
		Type:          "factory",
		Role:          "factory_cto",
		Mode:          "factory",
		Subscriptions: []string{"z"},
	})

	err := am.ConfigureRouting(PersistedRoutingRule{
		VerticalID:   "v1",
		EventPattern: "foo.*",
		SubscriberID: "opco-ceo-v1",
		InstalledBy:  "opco-ceo-v1",
		Status:       "active",
	})
	if err != nil {
		t.Fatalf("configure route: %v", err)
	}

	if err := am.TeardownOpCo("v1"); err != nil {
		t.Fatalf("teardown opco: %v", err)
	}
	if am.Count() != 1 {
		t.Fatalf("expected only factory agent to remain, count=%d", am.Count())
	}
	if _, ok := am.GetAgentConfig("factory-1"); !ok {
		t.Fatal("factory agent should remain")
	}
	if _, ok := am.GetAgentConfig("opco-ceo-v1"); ok {
		t.Fatal("opco ceo should be removed")
	}
	rt := bus.GetRoutingTable("v1")
	if rt == nil {
		t.Fatal("routing table should exist")
	}
	if len(rt.Routes) != 0 {
		t.Fatalf("expected routing table cleared, got %d routes", len(rt.Routes))
	}
}

func TestManagerTeardownOpCo_EmitsTypedPayloadWithPriority(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	am := NewAgentManager(bus, nil)

	_ = am.SpawnAgent(models.AgentConfig{
		ID:            "opco-ceo-v1",
		Type:          "operating",
		Role:          "opco-ceo",
		Mode:          "operating",
		VerticalID:    "v1",
		Subscriptions: []string{"x"},
	})

	if err := am.TeardownOpCo("v1"); err != nil {
		t.Fatalf("teardown opco: %v", err)
	}
	var teardown events.Event
	for i := len(store.events) - 1; i >= 0; i-- {
		if store.events[i].Type == events.EventType("opco.teardown_complete") {
			teardown = store.events[i]
			break
		}
	}
	if teardown.ID == "" {
		t.Fatal("expected opco.teardown_complete event")
	}
	var payload OpCOTeardownCompletePayload
	if err := json.Unmarshal(teardown.Payload, &payload); err != nil {
		t.Fatalf("unmarshal teardown payload: %v", err)
	}
	if payload.VerticalID != "v1" {
		t.Fatalf("expected vertical_id=v1, got %q", payload.VerticalID)
	}
	if payload.Priority != "normal" {
		t.Fatalf("expected priority=normal, got %q", payload.Priority)
	}
	if !payload.RoutingCleared {
		t.Fatalf("expected routing_cleared=true, got false")
	}
}

func TestDefaultOpCoRoutesBootstrapSeededCounts(t *testing.T) {
	routes := defaultOpCoRoutes("v1")
	if len(routes) != 19 {
		t.Fatalf("expected 19 default routes (18 bootstrap + 1 seeded), got %d", len(routes))
	}
	bootstrap := 0
	seeded := 0
	deployToCTO := false
	deployToDevOps := false
	for _, r := range routes {
		switch r.Source {
		case "bootstrap":
			bootstrap++
		case "seeded":
			seeded++
		}
		if r.EventPattern == "deploy_requested" && strings.HasPrefix(r.SubscriberID, "cto-agent-") {
			deployToCTO = true
		}
		if r.EventPattern == "deploy_requested" && strings.HasPrefix(r.SubscriberID, "devops-agent-") {
			deployToDevOps = true
		}
	}
	if bootstrap != 18 {
		t.Fatalf("expected 18 bootstrap routes, got %d", bootstrap)
	}
	if seeded != 1 {
		t.Fatalf("expected 1 seeded route, got %d", seeded)
	}
	if !deployToCTO || !deployToDevOps {
		t.Fatalf("expected deploy_requested bootstrap routes to CTO and DevOps, got cto=%v devops=%v", deployToCTO, deployToDevOps)
	}
}

func TestDefaultOpCoRosterCEOIncludesCrossDomainReport(t *testing.T) {
	roster := defaultOpCoRoster("v1")
	var ceo PersistedAgent
	found := false
	for _, agent := range roster {
		if agent.Config.Role == "opco-ceo" {
			ceo = agent
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected opco-ceo in default roster")
	}
	has := false
	for _, sub := range ceo.Config.Subscriptions {
		if strings.TrimSpace(sub) == "cross_domain_report" {
			has = true
			break
		}
	}
	if !has {
		t.Fatalf("expected opco-ceo subscriptions to include cross_domain_report, got %v", ceo.Config.Subscriptions)
	}
}

func TestManagerOpCoWorkspaceLifecycleHooks(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	am := NewAgentManager(bus, nil)
	ws := &workspaceLifecycleStub{}
	am.SetWorkspaceLifecycle(ws)

	if err := am.SpawnOpCo("v1", models.MandateDocument{}); err != nil {
		t.Fatalf("spawn opco: %v", err)
	}
	if ws.ensureCount != 1 {
		t.Fatalf("expected ensure workspace call, got %d", ws.ensureCount)
	}

	if err := am.TeardownOpCo("v1"); err != nil {
		t.Fatalf("teardown opco: %v", err)
	}
	if ws.stopCount != 1 {
		t.Fatalf("expected stop workspace call, got %d", ws.stopCount)
	}
}

func TestManagerHandlesOpCoSpinupControlEvent(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	am := NewAgentManager(bus, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	am.Run(ctx)
	defer func() { _ = am.Shutdown() }()

	evt := events.Event{
		ID:          "33333333-3333-3333-3333-333333333333",
		Type:        events.EventType("opco.spinup_requested"),
		SourceAgent: "empire-coordinator",
		VerticalID:  "v-control",
		Payload:     []byte(`{"vertical_id":"v-control","mandate":{"vertical_id":"v-control"}}`),
		CreatedAt:   time.Now(),
	}
	if err := bus.Publish(context.Background(), evt); err != nil {
		t.Fatalf("publish spinup control event: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if am.Count() >= 13 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected opco roster to be spawned from control event, got count=%d", am.Count())
}
