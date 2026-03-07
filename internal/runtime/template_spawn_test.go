package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimemanager "empireai/internal/runtime/manager"
)

type templateStoreStub struct {
	ensureSchemaCalls int
	upsertAgentCalls  int
	upsertRouteCalls  int
	setTplCalls       int
	lastTplVersion    string
	bootstrapVersion  int
	resolverCalls     int
	routedVersions    []int
	info              VerticalInfo
}

func (s *templateStoreStub) UpsertAgent(context.Context, PersistedAgent) error {
	s.upsertAgentCalls++
	return nil
}
func (s *templateStoreStub) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return nil, nil
}
func (s *templateStoreStub) MarkAgentTerminated(context.Context, string) error { return nil }
func (s *templateStoreStub) EnsureVerticalSchema(context.Context, string) error {
	s.ensureSchemaCalls++
	return nil
}
func (s *templateStoreStub) LoadLatestOrgTemplate(context.Context) (OrgTemplateRecord, error) {
	agents := []map[string]any{
		{
			"role":          "opco-ceo",
			"type":          "worker",
			"system_prompt": "CEO for {vertical_id} {vertical_name} in {geography}\nRoster:\n{org_roster}\nMandate:\n{mandate_document}",
			"tools":         []string{"agent_message", "configure_routing"},
			"subscriptions": []string{"opco.*"},
		},
		{
			"role":          "vp-product",
			"parent_role":   "opco-ceo",
			"type":          "worker",
			"system_prompt": "VP for {vertical_slug}",
			"tools":         []string{"agent_message"},
			"subscriptions": []string{"product.*"},
		},
	}
	bootstrap := []map[string]any{
		{"event_pattern": "opco.*", "subscriber_role": "opco-ceo", "reason": "bootstrap"},
	}
	seeded := []map[string]any{
		{"event_pattern": "product.*", "subscriber_role": "vp-product", "reason": "seeded"},
	}
	aj, _ := json.Marshal(agents)
	bj, _ := json.Marshal(bootstrap)
	sj, _ := json.Marshal(seeded)
	return OrgTemplateRecord{
		Version:         "tpl-1",
		Agents:          aj,
		BootstrapRoutes: bj,
		SeededRoutes:    sj,
		CreatedBy:       "test",
		Description:     "test",
		CreatedAt:       time.Now(),
	}, nil
}
func (s *templateStoreStub) LoadOrgTemplate(context.Context, string) (OrgTemplateRecord, error) {
	return OrgTemplateRecord{}, nil
}
func (s *templateStoreStub) SetVerticalTemplateVersion(_ context.Context, _ string, version string) error {
	s.setTplCalls++
	s.lastTplVersion = version
	return nil
}
func (s *templateStoreStub) UpsertRoutingRule(_ context.Context, r PersistedRoutingRule) error {
	s.upsertRouteCalls++
	s.routedVersions = append(s.routedVersions, r.BootstrapVersion)
	return nil
}
func (s *templateStoreStub) LoadRoutingRules(context.Context) ([]PersistedRoutingRule, error) {
	return nil, nil
}
func (s *templateStoreStub) DeactivateRoutingRulesByVertical(context.Context, string) error {
	return nil
}
func (s *templateStoreStub) UpsertEventReceipt(context.Context, string, string, string, string) error {
	return nil
}
func (s *templateStoreStub) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (s *templateStoreStub) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (s *templateStoreStub) ResolveBootstrapVersion(_ context.Context, _ string) (int, error) {
	s.resolverCalls++
	if s.bootstrapVersion <= 0 {
		return 1, nil
	}
	return s.bootstrapVersion, nil
}
func (s *templateStoreStub) GetVerticalInfo(_ context.Context, _ string) (VerticalInfo, bool, error) {
	return s.info, true, nil
}

func TestManager_SpawnOpCo_UsesTemplateAndExpandsPrompts(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	store := &templateStoreStub{
		bootstrapVersion: 7,
		info: VerticalInfo{
			ID:        "v1",
			Name:      "Acme Vertical",
			Slug:      "acme",
			Geography: "US",
		},
	}
	am := runtimemanager.NewAgentManager(bus, nil, store)

	readyCh := bus.Subscribe("watch", events.EventType("opco.ceo_ready"))
	if err := am.SpawnOpCo("v1", models.MandateDocument{VerticalID: "v1", FounderNotes: "do it"}); err != nil {
		t.Fatalf("SpawnOpCo: %v", err)
	}
	if am.Count() != 2 {
		t.Fatalf("expected 2 agents, got %d", am.Count())
	}
	if store.ensureSchemaCalls != 1 {
		t.Fatalf("expected EnsureVerticalSchema called once, got %d", store.ensureSchemaCalls)
	}
	if store.upsertAgentCalls != 2 {
		t.Fatalf("expected UpsertAgent called twice, got %d", store.upsertAgentCalls)
	}
	if store.upsertRouteCalls != 2 {
		t.Fatalf("expected UpsertRoutingRule called twice, got %d", store.upsertRouteCalls)
	}
	if store.resolverCalls != 1 {
		t.Fatalf("expected ResolveBootstrapVersion called once, got %d", store.resolverCalls)
	}
	for _, v := range store.routedVersions {
		if v != 7 {
			t.Fatalf("expected routing bootstrap_version=7, got %d", v)
		}
	}
	if store.setTplCalls != 1 || store.lastTplVersion != "tpl-1" {
		t.Fatalf("expected SetVerticalTemplateVersion tpl-1, got calls=%d version=%q", store.setTplCalls, store.lastTplVersion)
	}

	cfg, ok := am.GetAgentConfig("opco-ceo-v1")
	if !ok {
		t.Fatal("missing opco-ceo-v1")
	}
	var runtimeCfg map[string]any
	if err := json.Unmarshal(cfg.Config, &runtimeCfg); err != nil {
		t.Fatalf("agent cfg.config json: %v", err)
	}
	sp, _ := runtimeCfg["system_prompt"].(string)
	if sp == "" {
		t.Fatal("expected non-empty expanded system_prompt")
	}
	if want := "Acme Vertical"; !strings.Contains(sp, want) {
		t.Fatalf("expected system_prompt to contain %q, got: %s", want, sp)
	}

	rt := bus.GetRoutingTable("v1")
	if rt == nil || len(rt.Routes) != 2 {
		t.Fatalf("expected 2 active routes, got %+v", rt)
	}

	select {
	case evt := <-readyCh:
		if string(evt.Type) != "opco.ceo_ready" {
			t.Fatalf("unexpected ready event: %s", evt.Type)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected opco.ceo_ready")
	}
}
