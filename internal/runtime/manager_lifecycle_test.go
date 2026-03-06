package runtime

import (
	"context"
	"empireai/internal/events"
	"empireai/internal/models"
	"encoding/json"
	"strings"
	"testing"
	"time"

	workspace "empireai/internal/runtime/workspace"
)

type workspaceLifecycleStub struct {
	ensureCount int
	stopCount   int
	killCount   int
	killErr     error
}

func (s *workspaceLifecycleStub) EnsureSystemWorkspaces(context.Context) error { return nil }
func (s *workspaceLifecycleStub) ResolveWorkspace(context.Context, models.AgentConfig) (*workspace.Target, error) {
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
func (s *workspaceLifecycleStub) KillOrphanProcesses(context.Context) error {
	s.killCount++
	return s.killErr
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
	if len(routes) != 20 {
		t.Fatalf("expected 20 default routes (all bootstrap), got %d", len(routes))
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
	if bootstrap != 20 {
		t.Fatalf("expected 20 bootstrap routes, got %d", bootstrap)
	}
	if seeded != 0 {
		t.Fatalf("expected 0 seeded routes, got %d", seeded)
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

type budgetPolicyAgent struct {
	id    string
	calls int
}

func (a *budgetPolicyAgent) ID() string { return a.id }

func (a *budgetPolicyAgent) Type() string { return "llm" }

func (a *budgetPolicyAgent) Subscriptions() []events.EventType { return nil }

func (a *budgetPolicyAgent) OnEvent(_ context.Context, _ events.Event) ([]events.Event, error) {
	a.calls++
	return nil, nil
}

func TestAgentManager_BudgetSuppressionPolicy(t *testing.T) {
	makeMgr := func(stateKey, stateValue string, role string) (*AgentManager, *budgetPolicyAgent) {
		am := NewAgentManager(NewEventBus(InMemoryEventStore{}), nil, nil)
		agent := &budgetPolicyAgent{id: "a1"}
		am.agents[agent.id] = agent
		am.agentCfg[agent.id] = models.AgentConfig{
			ID:         agent.id,
			Role:       role,
			Mode:       "operating",
			VerticalID: "v1",
		}
		am.SetBudgetTracker(&BudgetTracker{
			lastState: map[string]string{
				stateKey: stateValue,
			},
		})
		return am, agent
	}

	t.Run("emergency suppresses non-critical flows", func(t *testing.T) {
		am, agent := makeMgr("vertical|v1", "emergency", "marketing-agent")
		err := am.processEvent(context.Background(), agent, events.Event{
			ID:   "e1",
			Type: events.EventType("market_signals"),
		})
		if err != nil {
			t.Fatalf("processEvent: %v", err)
		}
		if agent.calls != 0 {
			t.Fatalf("expected suppression during emergency, calls=%d", agent.calls)
		}
	})

	t.Run("emergency allows support", func(t *testing.T) {
		am, agent := makeMgr("vertical|v1", "emergency", "support-agent")
		err := am.processEvent(context.Background(), agent, events.Event{
			ID:   "e2",
			Type: events.EventType("inbound.whatsapp_message"),
		})
		if err != nil {
			t.Fatalf("processEvent: %v", err)
		}
		if agent.calls != 1 {
			t.Fatalf("expected support flow allowed, calls=%d", agent.calls)
		}
	})

	t.Run("throttle pauses growth", func(t *testing.T) {
		am, agent := makeMgr("vertical|v1", "throttle", "vp-growth")
		err := am.processEvent(context.Background(), agent, events.Event{
			ID:   "e3",
			Type: events.EventType("outreach_digest"),
		})
		if err != nil {
			t.Fatalf("processEvent: %v", err)
		}
		if agent.calls != 0 {
			t.Fatalf("expected growth pause on throttle, calls=%d", agent.calls)
		}
	})

	t.Run("throttle suppresses management heartbeats", func(t *testing.T) {
		am, agent := makeMgr("vertical|v1", "throttle", "opco-ceo")
		err := am.processEvent(context.Background(), agent, events.Event{
			ID:   "e4",
			Type: events.EventType("heartbeat.opco_ceo"),
		})
		if err != nil {
			t.Fatalf("processEvent: %v", err)
		}
		if agent.calls != 0 {
			t.Fatalf("expected heartbeat suppression on throttle, calls=%d", agent.calls)
		}
	})
}
func TestDirectiveRequiresCoordinator(t *testing.T) {
	complex := events.Event{
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload: mustJSON(map[string]any{
			"directive_text": "Focus on compliance-driven opportunities in LATAM countries with over 80 percent internet penetration",
		}),
	}
	if !directiveRequiresCoordinator(complex) {
		t.Fatal("expected complex directive to require coordinator")
	}

	simple := events.Event{
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload: mustJSON(map[string]any{
			"directive_text": "SaaS in Uruguay",
		}),
	}
	if directiveRequiresCoordinator(simple) {
		t.Fatal("expected simple directive to be runtime-handled")
	}

	forwarded := events.Event{
		Type:        events.EventType("system.directive"),
		SourceAgent: "scan-campaign-manager",
		Payload:     mustJSON(map[string]any{"directive_text": "anything"}),
	}
	if !directiveRequiresCoordinator(forwarded) {
		t.Fatal("expected scan-campaign-manager forwarded directive to require coordinator")
	}
}

type directiveProbeAgent struct {
	id    string
	calls int
}

func (a *directiveProbeAgent) ID() string { return a.id }

func (a *directiveProbeAgent) Type() string { return "worker" }

func (a *directiveProbeAgent) Subscriptions() []events.EventType {
	return []events.EventType{events.EventType("system.directive")}
}

func (a *directiveProbeAgent) OnEvent(_ context.Context, _ events.Event) ([]events.Event, error) {
	a.calls++
	return nil, nil
}

func TestAgentManager_ProcessEvent_InterceptsSimpleDirective(t *testing.T) {
	am := NewAgentManager(NewEventBus(InMemoryEventStore{}), nil, nil)
	agent := &directiveProbeAgent{id: "empire-coordinator"}
	am.agents[agent.id] = agent
	am.agentCfg[agent.id] = models.AgentConfig{
		ID:   agent.id,
		Role: "empire-coordinator",
		Mode: "holding",
	}

	err := am.processEvent(context.Background(), agent, events.Event{
		ID:          "evt-simple-directive",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload: mustJSON(map[string]any{
			"directive_text": "SaaS in Uruguay",
		}),
	})
	if err != nil {
		t.Fatalf("processEvent returned error: %v", err)
	}
	if agent.calls != 0 {
		t.Fatalf("expected simple directive to be intercepted by runtime, calls=%d", agent.calls)
	}
}

func TestAgentManager_ProcessEvent_ForwardsComplexDirective(t *testing.T) {
	am := NewAgentManager(NewEventBus(InMemoryEventStore{}), nil, nil)
	agent := &directiveProbeAgent{id: "empire-coordinator"}
	am.agents[agent.id] = agent
	am.agentCfg[agent.id] = models.AgentConfig{
		ID:   agent.id,
		Role: "empire-coordinator",
		Mode: "holding",
	}

	err := am.processEvent(context.Background(), agent, events.Event{
		ID:          "evt-complex-directive",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload: mustJSON(map[string]any{
			"directive_text": "Focus on compliance-driven opportunities in LATAM countries with over 80 percent internet penetration",
		}),
	})
	if err != nil {
		t.Fatalf("processEvent returned error: %v", err)
	}
	if agent.calls != 1 {
		t.Fatalf("expected complex directive to reach coordinator agent, calls=%d", agent.calls)
	}
}
