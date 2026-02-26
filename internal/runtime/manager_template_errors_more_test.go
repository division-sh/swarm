package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
)

type templateErrStore struct {
	rec OrgTemplateRecord
	err error
}

func (s *templateErrStore) UpsertAgent(context.Context, PersistedAgent) error                     { return nil }
func (s *templateErrStore) LoadAgents(context.Context) ([]PersistedAgent, error)                 { return nil, nil }
func (s *templateErrStore) MarkAgentTerminated(context.Context, string) error                    { return nil }
func (s *templateErrStore) EnsureVerticalSchema(context.Context, string) error                   { return nil }
func (s *templateErrStore) LoadLatestOrgTemplate(context.Context) (OrgTemplateRecord, error)     { return s.rec, s.err }
func (s *templateErrStore) LoadOrgTemplate(context.Context, string) (OrgTemplateRecord, error)   { return s.rec, s.err }
func (s *templateErrStore) SetVerticalTemplateVersion(context.Context, string, string) error     { return nil }
func (s *templateErrStore) UpsertRoutingRule(context.Context, PersistedRoutingRule) error        { return nil }
func (s *templateErrStore) LoadRoutingRules(context.Context) ([]PersistedRoutingRule, error)     { return nil, nil }
func (s *templateErrStore) DeactivateRoutingRulesByVertical(context.Context, string) error       { return nil }
func (s *templateErrStore) UpsertEventReceipt(context.Context, string, string, string, string) error {
	return nil
}
func (s *templateErrStore) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (s *templateErrStore) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

func TestManager_LoadLatestTemplate_ErrorBranches(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	am := NewAgentManager(bus, nil)

	// No store.
	if _, err := am.loadLatestTemplate(context.Background()); err == nil {
		t.Fatalf("expected error without store")
	}

	// Store error.
	am.store = &templateErrStore{err: errors.New("boom")}
	if _, err := am.loadLatestTemplate(context.Background()); err == nil {
		t.Fatalf("expected store error")
	}

	// Empty version.
	am.store = &templateErrStore{rec: OrgTemplateRecord{Version: "", Agents: []byte("[]"), BootstrapRoutes: []byte("[]"), SeededRoutes: []byte("[]")}}
	if _, err := am.loadLatestTemplate(context.Background()); err == nil {
		t.Fatalf("expected empty version error")
	}

	// No bootstrap routes.
	am.store = &templateErrStore{rec: OrgTemplateRecord{Version: "v1", Agents: []byte("[]"), BootstrapRoutes: []byte("[]"), SeededRoutes: []byte("[]")}}
	if _, err := am.loadLatestTemplate(context.Background()); err == nil {
		t.Fatalf("expected missing bootstrap routes error")
	}

	// Minimal valid template.
	bootstrap, _ := json.Marshal([]map[string]any{{"event_pattern": "opco.*", "subscriber_role": "opco-ceo", "reason": "b"}})
	agents, _ := json.Marshal([]map[string]any{{"role": "opco-ceo", "type": "worker", "system_prompt": "x"}})
	am.store = &templateErrStore{rec: OrgTemplateRecord{Version: "v2", Agents: agents, BootstrapRoutes: bootstrap, SeededRoutes: []byte("[]")}}
	snap, err := am.loadLatestTemplate(context.Background())
	if err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
	if snap.Version != "v2" || len(snap.BootstrapRoutes) != 1 || len(snap.Agents) != 1 {
		t.Fatalf("unexpected snap: %#v", snap)
	}
}

func TestManager_TemplateHelpers_RenderAndExpand(t *testing.T) {
	if got := expandTemplateText("", map[string]string{"x": "y"}); got != "" {
		t.Fatalf("expected empty passthrough")
	}
	if got := expandTemplateText("hello {k}", map[string]string{"{k}": "v"}); got != "hello v" {
		t.Fatalf("unexpected expand: %q", got)
	}

	roster := renderOpCoRoster([]orgTemplateAgent{
		{Role: "opco-ceo"},
		{Role: "vp-product"},
		{Role: ""},
	}, "v1")
	if roster == "" {
		t.Fatalf("expected roster text")
	}

	mandate := renderMandateText(models.MandateDocument{
		VerticalID:   "v1",
		FounderNotes: "notes",
		BusinessBrief: []byte(`{"problem":"x"}`),
		MVPSpec:       []byte(`{"features":["a"]}`),
		Brand:         []byte(`{"name":"b"}`),
		Budget:        []byte(`{"cap":1}`),
	})
	if mandate == "" || mandate[0] != '{' {
		t.Fatalf("unexpected mandate: %q", mandate)
	}

	if got := resolveTemplateSubscriber("v1", orgTemplateRoute{SubscriberID: "explicit"}); got != "explicit" {
		t.Fatalf("expected explicit id, got %q", got)
	}
	if got := resolveTemplateSubscriber("v1", orgTemplateRoute{SubscriberRole: "vp-product"}); got == "" || got == "vp-product" {
		t.Fatalf("expected role-derived opco id, got %q", got)
	}
}

