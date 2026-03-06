package tools_test

import (
	"context"
	"empireai/internal/events"
	"empireai/internal/models"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/testutil"
	"encoding/json"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
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

func TestRuntimeToolExecutor_ToolDefinitionsHaveSchema(t *testing.T) {
	exec := NewRuntimeToolExecutor(nil, nil, nil)
	defs := exec.ToolDefinitions()
	if len(defs) == 0 {
		t.Fatal("expected non-empty tool definitions")
	}
	for _, def := range defs {
		if def.Schema == nil {
			t.Fatalf("tool %s missing schema", def.Name)
		}
	}
}

func TestRuntimeToolExecutor_ToolSchemasAvoidTopLevelCombinators(t *testing.T) {
	exec := NewRuntimeToolExecutor(nil, nil, nil)
	for _, def := range exec.ToolDefinitions() {
		schema, ok := def.Schema.(map[string]any)
		if !ok || schema == nil {
			t.Fatalf("tool %s schema should be object map", def.Name)
		}
		for _, key := range []string{"oneOf", "anyOf", "allOf"} {
			if _, exists := schema[key]; exists {
				t.Fatalf("tool %s schema uses unsupported top-level %s", def.Name, key)
			}
		}
	}
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
		"to":      "t",
		"message": "hello",
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
	_, err := exec.Execute(ctx, "schedule", map[string]any{
		"action":        "timer.tick",
		"delay_seconds": 0,
		"context":       map[string]any{"n": 1},
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
		"agent_id": "a-hire",
		"role":     "r1",
	})
	if err != nil {
		t.Fatalf("agent_hire failed: %v", err)
	}
	if manager.Count() != 1 {
		t.Fatalf("expected manager count 1, got %d", manager.Count())
	}

	_, err = exec.Execute(ctx, "agent_fire", map[string]any{
		"agent_id": "a-hire",
		"reason":   "test",
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
		"operation":     "add",
		"event_type":    "foo.*",
		"subscriber_id": "a1",
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
		"operation":     "add",
		"event_type":    "foo.*",
		"subscriber_id": "a2",
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
		"operation":     "remove",
		"event_type":    "product_spec_ready",
		"subscriber_id": "cto-agent-v1",
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
		"type":     "spend_request",
		"priority": "normal",
		"subject":  "Need budget",
		"payload":  map[string]any{"amount": 12},
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

func TestRuntimeToolExecutor_MailboxSend_NormalizesApprovalAliases(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	manager := NewAgentManager(bus, nil)
	exec := NewRuntimeToolExecutor(bus, nil, manager)
	ms := &mailboxStoreStub{}
	exec.SetMailboxStore(ms)

	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:         "empire-coordinator",
		Role:       "empire-coordinator",
		Mode:       "holding",
		VerticalID: "v1",
	})

	cases := []string{"approval", "vertical.promotion_review"}
	for _, mt := range cases {
		_, err := exec.Execute(ctx, "mailbox_send", map[string]any{
			"type":     mt,
			"priority": "normal",
			"subject":  "Needs approval",
			"payload":  map[string]any{"source": "test"},
		})
		if err != nil {
			t.Fatalf("mailbox_send(%q) failed: %v", mt, err)
		}
		if ms.last.Type != "vertical_approval" {
			t.Fatalf("expected type vertical_approval for %q, got %q", mt, ms.last.Type)
		}
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
func TestRuntimeToolExecutor_HandleEmitToolPublishesEvent(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding"}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "dir-1",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"directive_text": "SaaS in Paraguay"}),
	})

	out, err := exec.Execute(ctx, "emit_scan_requested", map[string]any{
		"mode":      "saas_gap",
		"geography": "paraguay",
	})
	if err != nil {
		t.Fatalf("execute emit tool: %v", err)
	}
	if out == nil {
		t.Fatal("expected publish ack output")
	}
	if len(store.events) == 0 {
		t.Fatal("expected published event")
	}
	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if string(store.events[i].Type) == "scan.requested" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected scan.requested event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if strings.TrimSpace(asString(payload["mode"])) != "saas_gap" {
		t.Fatalf("expected mode preserved, got %+v", payload["mode"])
	}
	if _, ok := payload["priority"]; ok {
		t.Fatalf("expected legacy priority field to be trimmed by contract schema, got payload=%+v", payload)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolTransitionGuardrail(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding"}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "scan-1",
		Type:        events.EventType("scan.completed"),
		SourceAgent: "discovery-coordinator",
		Payload:     mustJSON(map[string]any{"discoveries_count": 3}),
	})
	_, err := exec.Execute(ctx, "emit_opco_spinup_requested", map[string]any{
		"vertical_id": "v1",
		"mandate":     map[string]any{"vertical_id": "v1"},
	})
	if err == nil || !strings.Contains(err.Error(), "guardrail_violation") {
		t.Fatalf("expected guardrail violation, got %v", err)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolSchemaValidation(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding"}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "dir-1",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"directive_text": "SaaS in Paraguay"}),
	})
	_, err := exec.Execute(ctx, "emit_scan_requested", map[string]any{
		"priority": "normal",
	})
	if err == nil {
		t.Fatal("expected schema validation error for missing required mode")
	}
	if !strings.Contains(err.Error(), "is required") {
		t.Fatalf("expected required-field schema error, got %v", err)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolVerticalDerivedCoercesLegacyRationaleString(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{ID: "analysis-agent", Role: "analysis-agent", Mode: "factory"}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "score-req-legacy-rationale",
		Type:        events.EventType("scoring.requested"),
		SourceAgent: "scoring-node",
		VerticalID:  "v-parent-1",
		Payload:     mustJSON(map[string]any{"vertical_id": "v-parent-1"}),
	})
	_, err := exec.Execute(ctx, "emit_vertical_derived", map[string]any{
		"parent_id":            "v-parent-1",
		"generation_depth":     1,
		"generator_agent_id":   "analysis-agent",
		"derivation_rationale": "narrow ICP to owner-operated firms",
		"opportunity_name":     "Derived Opportunity",
		"signal_strength":      72,
		"discovery_context":    map[string]any{"mode": "derived"},
	})
	if err != nil {
		t.Fatalf("expected legacy derivation_rationale string to be normalized, got %v", err)
	}

	if len(store.events) == 0 {
		t.Fatal("expected published vertical.derived event")
	}
	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if strings.TrimSpace(string(store.events[i].Type)) == "vertical.derived" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected vertical.derived event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	rationale, ok := payload["derivation_rationale"].(map[string]any)
	if !ok {
		t.Fatalf("expected derivation_rationale object after normalization, got %T", payload["derivation_rationale"])
	}
	if strings.TrimSpace(asString(rationale["summary"])) == "" {
		t.Fatalf("expected derivation_rationale.summary to be populated, got %#v", rationale)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolCoordinatorLegacyNestedPayload(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding"}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "dir-legacy-1",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"directive_text": "SaaS in Paraguay"}),
	})
	_, err := exec.Execute(ctx, "emit_scan_requested", map[string]any{
		"payload": map[string]any{
			"mode":      "discovery",
			"priority":  "medium",
			"geography": "paraguay",
		},
	})
	if err != nil {
		t.Fatalf("expected legacy nested payload to be normalized, got %v", err)
	}

	if len(store.events) == 0 {
		t.Fatal("expected emitted event")
	}
	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if string(store.events[i].Type) == "scan.requested" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected scan.requested event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := strings.TrimSpace(asString(payload["mode"])); got != "saas_gap" {
		t.Fatalf("expected mode alias discovery->saas_gap, got %q", got)
	}
	if _, ok := payload["priority"]; ok {
		t.Fatalf("expected legacy priority field removed after normalization, got %+v", payload)
	}
	if _, hasNested := payload["payload"]; hasNested {
		t.Fatalf("expected nested payload key removed after normalization, got %+v", payload)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolCoordinatorInvalidModeCoerced(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding"}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "dir-invalid-mode-1",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"directive_text": "SaaS in Argentina"}),
	})
	_, err := exec.Execute(ctx, "emit_scan_requested", map[string]any{
		"mode":     "simple",
		"priority": "normal",
	})
	if err != nil {
		t.Fatalf("expected invalid mode to be coerced for coordinator scan.requested, got %v", err)
	}

	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if string(store.events[i].Type) == "scan.requested" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected scan.requested event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := strings.TrimSpace(asString(payload["mode"])); got != "saas_gap" {
		t.Fatalf("expected invalid mode coerced to saas_gap, got %q", got)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolContextEnrichment(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{
		ID:         "business-research-agent",
		Role:       "business-research-agent",
		Mode:       "factory",
		VerticalID: "v1",
	}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "vs-1",
		Type:        events.EventType("validation.started"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  "v1",
		Payload:     mustJSON(map[string]any{"vertical_id": "v1"}),
	})

	if _, err := exec.Execute(ctx, "emit_spec_requested", map[string]any{}); err != nil {
		t.Fatalf("expected context-enriched emit to pass, got %v", err)
	}
	if len(store.events) == 0 {
		t.Fatal("expected emitted event")
	}
	last := store.events[len(store.events)-1]
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if strings.TrimSpace(asString(payload["vertical_id"])) != "v1" {
		t.Fatalf("expected enriched vertical_id=v1, got %+v", payload["vertical_id"])
	}
}

func TestRuntimeToolExecutor_HandleEmitToolOneShotSpecApproved(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{
		ID:         "business-research-agent",
		Role:       "business-research-agent",
		Mode:       "factory",
		VerticalID: "v1",
	}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "spr-1",
		Type:        events.EventType("spec_review.passed"),
		SourceAgent: "spec-reviewer",
		VerticalID:  "v1",
		Payload:     mustJSON(map[string]any{"vertical_id": "v1"}),
	})
	if _, err := exec.Execute(ctx, "emit_spec_approved", map[string]any{"vertical_id": "v1"}); err != nil {
		t.Fatalf("first spec.approved should pass: %v", err)
	}
	if _, err := exec.Execute(ctx, "emit_spec_approved", map[string]any{"vertical_id": "v1"}); err == nil {
		t.Fatal("expected duplicate spec.approved to be blocked")
	}
}

func TestRuntimeToolExecutor_HandleEmitToolFlattensNestedCategoryAssessedPayload(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{
		ID:         "market-research-agent-shard-0",
		Role:       "market-research-agent",
		Mode:       "factory",
		VerticalID: "",
	}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "scan-assigned-1",
		Type:        events.EventType("market_research.scan_assigned"),
		SourceAgent: "pipeline-coordinator",
		Payload: mustJSON(map[string]any{
			"scan_id":     "scan-123",
			"campaign_id": "camp-1",
			"mode":        "saas_gap",
			"geography":   "argentina",
		}),
	})
	if _, err := exec.Execute(ctx, "emit_category_assessed", map[string]any{
		"payload": map[string]any{
			"scan_id":          "scan-123",
			"campaign_id":      "camp-1",
			"mode":             "saas_gap",
			"geography":        "argentina",
			"category":         "operations",
			"subcategory":      "clinic_scheduling",
			"signal_strength":  76,
			"opportunity_name": "Clinic Scheduling Automation",
			"preliminary_icp":  "Clinic operations manager",
			"build_sketch": map[string]any{
				"core_features":    []any{"calendar sync"},
				"key_integrations": []any{"whatsapp"},
				"red_flags":        []any{},
			},
			"opportunity_hypothesis": "Automate patient bookings and reminders.",
			"geographic_scope":       "local",
			"evidence": map[string]any{
				"competitors": []any{
					map[string]any{"name": "ClinicFlow", "pricing": "$49/mo", "source_url": "https://example.com/competitor"},
				},
				"pain_signals": []any{
					map[string]any{"signal": "Manual follow-up workflows are common", "source_url": "https://example.com/pain"},
				},
				"regulatory": []any{
					map[string]any{"detail": "Consent requirements apply", "source_url": "https://example.com/reg"},
				},
				"buyer_communities": []any{
					map[string]any{"name": "Clinic Ops LATAM", "source_url": "https://example.com/community"},
				},
			},
		},
	}); err != nil {
		t.Fatalf("expected nested payload normalization for category.assessed, got %v", err)
	}

	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if string(store.events[i].Type) == "category.assessed" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected category.assessed event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if strings.TrimSpace(asString(payload["scan_id"])) != "scan-123" {
		t.Fatalf("expected scan_id flattened into root, got payload=%v", payload)
	}
	if _, hasNested := payload["payload"]; hasNested {
		t.Fatalf("expected nested payload key removed, got payload=%v", payload)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolFlattensNestedScanCompletePayload(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{
		ID:   "market-research-agent-shard-1",
		Role: "market-research-agent",
		Mode: "factory",
	}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "scan-assigned-2",
		Type:        events.EventType("market_research.scan_assigned"),
		SourceAgent: "pipeline-coordinator",
		Payload: mustJSON(map[string]any{
			"scan_id": "scan-456",
		}),
	})
	if _, err := exec.Execute(ctx, "emit_market_research_scan_complete", map[string]any{
		"payload": map[string]any{
			"scan_id": "scan-456",
		},
	}); err != nil {
		t.Fatalf("expected nested payload normalization for market_research.scan_complete, got %v", err)
	}

	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if string(store.events[i].Type) == "market_research.scan_complete" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected market_research.scan_complete event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if strings.TrimSpace(asString(payload["scan_id"])) != "scan-456" {
		t.Fatalf("expected scan_id flattened into root, got payload=%v", payload)
	}
	if _, hasNested := payload["payload"]; hasNested {
		t.Fatalf("expected nested payload key removed, got payload=%v", payload)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolSourceScrapedBackfillsGeographyFromAssignment(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{
		ID:   "scanner-agent-shard-0",
		Role: "scanner-agent",
		Mode: "factory",
	}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "scanner-assigned-1",
		Type:        events.EventType("scanner.directories.scan_assigned"),
		SourceAgent: "pipeline-coordinator",
		Payload: mustJSON(map[string]any{
			"scan_id":     "scan-geo-1",
			"campaign_id": "camp-geo-1",
			"mode":        "local_services",
			"geography":   "United States",
		}),
	})
	if _, err := exec.Execute(ctx, "emit_source_scraped", map[string]any{
		"payload": map[string]any{
			"scan_id":         "scan-geo-1",
			"source":          "directories",
			"evidence":        "Signal from directory crawl",
			"signal_strength": 72,
		},
	}); err != nil {
		t.Fatalf("expected source.scraped payload to backfill geography from assignment, got %v", err)
	}

	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if string(store.events[i].Type) == "source.scraped" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected source.scraped event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := strings.TrimSpace(asString(payload["geography"])); got != "United States" {
		t.Fatalf("expected geography backfilled from assignment, got %q payload=%v", got, payload)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolSourceScrapedPlaceholderGeographyReplaced(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{
		ID:   "scanner-agent-shard-1",
		Role: "scanner-agent",
		Mode: "factory",
	}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "scanner-assigned-2",
		Type:        events.EventType("scanner.google_maps.scan_assigned"),
		SourceAgent: "pipeline-coordinator",
		Payload: mustJSON(map[string]any{
			"scan_id":     "scan-geo-2",
			"campaign_id": "camp-geo-2",
			"mode":        "local_services",
			"geography":   "US",
		}),
	})
	if _, err := exec.Execute(ctx, "emit_source_scraped", map[string]any{
		"scan_id":         "scan-geo-2",
		"source":          "google_maps",
		"evidence":        "Signal from maps crawl",
		"signal_strength": 68,
		"geography":       "unspecified, unspecified",
	}); err != nil {
		t.Fatalf("expected placeholder geography to be normalized from assignment, got %v", err)
	}

	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if string(store.events[i].Type) == "source.scraped" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected source.scraped event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := strings.TrimSpace(asString(payload["geography"])); got != "US" {
		t.Fatalf("expected geography=US after placeholder replacement, got %q payload=%v", got, payload)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolSourceScrapedRejectsMissingGeographyWithoutAssignment(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{
		ID:   "scanner-agent-shard-2",
		Role: "scanner-agent",
		Mode: "factory",
	}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "scanner-assigned-3",
		Type:        events.EventType("scanner.reviews.scan_assigned"),
		SourceAgent: "pipeline-coordinator",
		Payload: mustJSON(map[string]any{
			"scan_id": "scan-geo-3",
		}),
	})
	if _, err := exec.Execute(ctx, "emit_source_scraped", map[string]any{
		"scan_id":         "scan-geo-3",
		"source":          "reviews",
		"evidence":        "Signal from review crawl",
		"signal_strength": 65,
	}); err == nil || !strings.Contains(err.Error(), "geography is required") {
		t.Fatalf("expected source.scraped to reject missing geography without assignment fallback, got %v", err)
	}
}

func TestRuntimeToolExecutor_HandleEmitToolScoreDimensionDoesNotInjectTaskID(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	actor := models.AgentConfig{
		ID:   "analysis-agent",
		Role: "analysis-agent",
		Mode: "factory",
	}

	ctx := WithActor(context.Background(), actor)
	ctx = WithInboundEvent(ctx, events.Event{
		ID:          "score-req-1",
		Type:        events.EventType("scoring.requested"),
		SourceAgent: "pipeline-coordinator",
		TaskID:      "task-score-1",
		VerticalID:  "vertical-1",
		Payload: mustJSON(map[string]any{
			"vertical_id": "vertical-1",
		}),
	})

	if _, err := exec.Execute(ctx, "emit_score_dimension_complete", map[string]any{
		"dimension": "market_size",
		"score":     73,
		"evidence":  "validated demand signal from sources",
	}); err != nil {
		t.Fatalf("expected emit_score_dimension_complete to pass without task_id injection, got %v", err)
	}
	if len(store.events) == 0 {
		t.Fatal("expected emitted event")
	}
	var last events.Event
	found := false
	for i := len(store.events) - 1; i >= 0; i-- {
		if string(store.events[i].Type) == "score.dimension_complete" {
			last = store.events[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected score.dimension_complete event, got %+v", store.events)
	}
	var payload map[string]any
	if err := json.Unmarshal(last.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if _, ok := payload["task_id"]; ok {
		t.Fatalf("expected strict payload to omit task_id, got payload=%v", payload)
	}
}
func TestToolExecutor_SystemTools_ValidationAndAuth(t *testing.T) {
	exec := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)

	// nginx_reload only holding-devops
	_, err := exec.Execute(WithActor(context.Background(), models.AgentConfig{ID: "a1", Role: "opco-ceo"}), "nginx_reload", map[string]any{})
	if err == nil {
		t.Fatal("expected nginx_reload to reject non holding-devops")
	}

	// systemd_control validation
	_, err = exec.Execute(WithActor(context.Background(), models.AgentConfig{ID: "a1", Role: "holding-devops"}), "systemd_control", map[string]any{
		"action": "nope",
		"unit":   "empireai-x",
	})
	if err == nil {
		t.Fatal("expected invalid action error")
	}
	_, err = exec.Execute(WithActor(context.Background(), models.AgentConfig{ID: "a1", Role: "holding-devops"}), "systemd_control", map[string]any{
		"action":  "restart",
		"service": "nginx",
	})
	if err == nil {
		t.Fatal("expected unit prefix error")
	}

	// certbot_execute domain required
	_, err = exec.Execute(WithActor(context.Background(), models.AgentConfig{ID: "a1", Role: "holding-devops"}), "certbot_execute", map[string]any{
		"domain": "",
	})
	if err == nil {
		t.Fatal("expected domain required error")
	}
}

func TestRuntimeToolExecutor_SQLExecute_ReadOnlySelect(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	bus := NewEventBus(InMemoryEventStore{})
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	exec.SetSQLDB(db)

	verticalID := "11111111-1111-1111-1111-111111111111"
	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:         "agent-1",
		Role:       "opco-ceo",
		Mode:       "operating",
		VerticalID: verticalID,
	})

	// SELECT path uses derived schema from slug when available.
	mock.ExpectQuery("SELECT COALESCE\\(NULLIF\\(slug, ''\\), ''\\)\\s+FROM verticals").
		WithArgs(verticalID).
		WillReturnRows(sqlmock.NewRows([]string{"slug"}).AddRow("Acme"))
	mock.ExpectBegin()
	mock.ExpectExec("SET LOCAL search_path = \"acme_schema\"").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("SET TRANSACTION READ ONLY").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("SET LOCAL statement_timeout = '15s'").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("select 1 as x LIMIT 200").
		WillReturnRows(sqlmock.NewRows([]string{"x"}).AddRow([]byte("1")))
	mock.ExpectCommit()

	out, err := exec.Execute(ctx, "sql_execute", map[string]any{
		"query": "select 1 as x",
	})
	if err != nil {
		t.Fatalf("sql_execute select: %v", err)
	}
	m, _ := out.(map[string]any)
	rows, _ := m["rows"].([]map[string]any)
	if len(rows) != 1 || rows[0]["x"] != "1" {
		t.Fatalf("unexpected select rows: %#v", out)
	}
	if m["schema"] != "acme_schema" {
		t.Fatalf("expected schema acme_schema, got %#v", m["schema"])
	}
	if m["read_only"] != true {
		t.Fatalf("expected read_only=true, got %#v", m["read_only"])
	}

	// Non-select statements must be rejected.
	out, err = exec.Execute(ctx, "sql_execute", map[string]any{
		"query": "update t set a=1",
	})
	if err == nil {
		t.Fatalf("expected non-select query rejection, got out=%#v", out)
	}

	// Small direct helper coverage.
	_ = exec.ToolDefinitions()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestSanitizeSQLReadQuery_Guards(t *testing.T) {
	t.Run("appends default limit", func(t *testing.T) {
		q, err := runtimetools.SanitizeSQLReadQueryForTest("select id from t")
		if err != nil {
			t.Fatalf("sanitize query: %v", err)
		}
		if q != "select id from t LIMIT 200" {
			t.Fatalf("unexpected normalized query: %q", q)
		}
	})

	t.Run("rejects non-select", func(t *testing.T) {
		if _, err := runtimetools.SanitizeSQLReadQueryForTest("delete from t"); err == nil {
			t.Fatal("expected non-select rejection")
		}
	})

	t.Run("rejects schema qualified from clause", func(t *testing.T) {
		if _, err := runtimetools.SanitizeSQLReadQueryForTest("select id from public.orders limit 10"); err == nil {
			t.Fatal("expected restricted schema rejection")
		}
	})

	t.Run("rejects quoted restricted schema", func(t *testing.T) {
		if _, err := runtimetools.SanitizeSQLReadQueryForTest(`select id from "public".orders limit 10`); err == nil {
			t.Fatal("expected quoted restricted schema rejection")
		}
	})

	t.Run("rejects schema qualification with quoted identifier", func(t *testing.T) {
		if _, err := runtimetools.SanitizeSQLReadQueryForTest(`select id from "tenant".orders`); err == nil {
			t.Fatal("expected schema-qualified quoted identifier rejection")
		}
	})

	t.Run("rejects schema qualification with spaced dot", func(t *testing.T) {
		if _, err := runtimetools.SanitizeSQLReadQueryForTest(`select id from "tenant"   . orders`); err == nil {
			t.Fatal("expected schema-qualified reference rejection")
		}
	})

	t.Run("rejects oversized limit", func(t *testing.T) {
		if _, err := runtimetools.SanitizeSQLReadQueryForTest("select id from t limit 9999"); err == nil {
			t.Fatal("expected limit rejection")
		}
	})
}
func TestRuntimeToolExecutor_ExternalProxy_LoadsCredsDecryptsAndCallsEndpoint(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Endpoint that asserts headers + method and returns JSON.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Fatalf("expected Authorization Bearer tok, got %q", got)
		}
		if got := r.Header.Get("X-From"); got != "cred" {
			t.Fatalf("expected X-From=cred, got %q", got)
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	oldKey := os.Getenv("EMPIREAI_CREDENTIALS_KEY")
	t.Cleanup(func() { _ = os.Setenv("EMPIREAI_CREDENTIALS_KEY", oldKey) })
	_ = os.Setenv("EMPIREAI_CREDENTIALS_KEY", "k")

	verticalID := "11111111-1111-1111-1111-111111111111"

	// loadVerticalCredentials query + decrypt query.
	credsJSON, _ := json.Marshal(map[string]any{
		"whatsapp": map[string]any{
			"endpoint": srv.URL,
			"api_key":  "enc::dG9r", // base64("tok")
			"headers":  map[string]any{"X-From": "cred"},
		},
	})
	mock.ExpectQuery("SELECT COALESCE\\(credentials, '\\{\\}'::jsonb\\)\\s+FROM verticals").
		WithArgs(verticalID).
		WillReturnRows(sqlmock.NewRows([]string{"credentials"}).AddRow(credsJSON))
	mock.ExpectQuery("SELECT pgp_sym_decrypt").
		WithArgs("dG9r", "k").
		WillReturnRows(sqlmock.NewRows([]string{"plain"}).AddRow("tok"))

	exec := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	exec.SetSQLDB(db)

	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:         "a1",
		Role:       "opco-ceo",
		Mode:       "operating",
		VerticalID: verticalID,
	})

	out, err := exec.Execute(ctx, "whatsapp_business_api", map[string]any{
		"to":      "+15551234567",
		"message": "hello world",
	})
	if err != nil {
		t.Fatalf("external proxy: %v", err)
	}
	m, _ := out.(map[string]any)
	if m["status"] != "ok" {
		t.Fatalf("unexpected out: %#v", out)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestRuntimeToolExecutor_ExternalProxy_DefaultMethodAndParseBody(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Endpoint that returns plain text.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("expected GET, got %s", r.Method)
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	verticalID := "22222222-2222-2222-2222-222222222222"
	credsJSON, _ := json.Marshal(map[string]any{
		"registrar": map[string]any{
			"endpoint": srv.URL,
			"token":    "t1",
		},
	})
	mock.ExpectQuery("SELECT COALESCE\\(credentials, '\\{\\}'::jsonb\\)\\s+FROM verticals").
		WithArgs(verticalID).
		WillReturnRows(sqlmock.NewRows([]string{"credentials"}).AddRow(credsJSON))

	exec := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	exec.SetSQLDB(db)

	ctx := WithActor(context.Background(), models.AgentConfig{ID: "a1", Role: "opco-ceo", Mode: "operating", VerticalID: verticalID})

	out, err := exec.Execute(ctx, "domain_availability_check", map[string]any{
		"domain": "example.com",
	})
	if err != nil {
		t.Fatalf("domain_availability_check: %v", err)
	}
	m, _ := out.(map[string]any)
	body := m["body"]
	if s, ok := body.(string); !ok || strings.TrimSpace(s) != "ok" {
		t.Fatalf("expected plain body ok, got %#v", body)
	}

	// Small helpers.
	if DefaultExternalMethod("whatsapp_name_check") != http.MethodGet {
		t.Fatal("expected GET for whatsapp_name_check")
	}
	if ParseExternalResponseBody([]byte(`{"x":1}`)) == nil {
		t.Fatal("expected parsed json")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}
func TestRuntimeToolExecutor_ExternalProxy_Succeeds_WithVerticalCredentials(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"available":true}`))
	}))
	defer okSrv.Close()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', $2::jsonb, now(), now())
	`, verticalID, `{"registrar":{"endpoint":"`+okSrv.URL+`","api_key":"k1"}}`); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	scheduler := NewScheduler(func(Schedule) {})
	defer scheduler.Stop()
	exec := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), scheduler, nil)
	exec.SetSQLDB(db)

	actor := models.AgentConfig{
		ID:         "opco-ceo-" + verticalID,
		Role:       "opco-ceo",
		Mode:       "operating",
		Type:       "stub",
		VerticalID: verticalID,
		Config:     json.RawMessage(`{"system_prompt":"x","tools":["domain_availability_check"]}`),
	}

	out, err := exec.Execute(WithActor(ctx, actor), "domain_availability_check", map[string]any{
		"domain": "example.com",
	})
	if err != nil {
		t.Fatalf("external proxy: %v", err)
	}
	m, _ := out.(map[string]any)
	if m["status"] != "ok" {
		t.Fatalf("unexpected output: %v", out)
	}

	// Error status path.
	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r
		http.Error(w, "no", http.StatusInternalServerError)
	}))
	defer badSrv.Close()
	if _, err := db.ExecContext(ctx, `UPDATE verticals SET credentials=$2::jsonb WHERE id=$1::uuid`, verticalID, `{"registrar":{"endpoint":"`+badSrv.URL+`","api_key":"k1"}}`); err != nil {
		t.Fatalf("update creds: %v", err)
	}
	if _, err := exec.Execute(WithActor(ctx, actor), "domain_availability_check", map[string]any{}); err == nil {
		t.Fatal("expected error on 500 response")
	}
}
