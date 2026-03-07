package manager

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimebus "empireai/internal/runtime/bus"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestAgentManager_ControlEvent_SpinupRequested_SpawnsOpCo(t *testing.T) {
	bus := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	am := NewAgentManager(bus, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	am.Run(ctx)
	defer func() { _ = am.Shutdown() }()

	if am.Count() != 0 {
		t.Fatalf("expected empty manager initially")
	}

	payload := []byte(`{"vertical_id":"v1","mandate":{"vertical_id":"v1","founder_notes":"go"}}`)
	if err := bus.Publish(context.Background(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("opco.spinup_requested"),
		SourceAgent: "tester",
		VerticalID:  "v1",
		Payload:     payload,
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		if am.Count() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected opco agents spawned")
}

type panicProbeAgent struct{ id string }

func (a *panicProbeAgent) ID() string { return a.id }

func (a *panicProbeAgent) Type() string { return "stub" }

func (a *panicProbeAgent) Subscriptions() []events.EventType { return nil }

func (a *panicProbeAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func TestPanicBackoff(t *testing.T) {
	cases := []struct {
		panics int
		want   time.Duration
	}{
		{panics: 0, want: 1 * time.Second},
		{panics: 1, want: 1 * time.Second},
		{panics: 2, want: 5 * time.Second},
		{panics: 3, want: 30 * time.Second},
		{panics: 4, want: 2 * time.Minute},
		{panics: 5, want: 10 * time.Minute},
		{panics: 9, want: 10 * time.Minute},
	}
	for _, tc := range cases {
		if got := panicBackoff(tc.panics); got != tc.want {
			t.Fatalf("panicBackoff(%d)=%s want=%s", tc.panics, got, tc.want)
		}
	}
}

func TestAgentManager_HandleAgentLoopPanic_EmitsAndEscalates(t *testing.T) {
	bus := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	am := NewAgentManager(bus, nil)

	am.mu.Lock()
	am.agentCfg["worker-1"] = models.AgentConfig{
		ID:          "worker-1",
		Type:        "stub",
		Role:        "backend-agent",
		Mode:        "holding",
		ParentAgent: "empire-coordinator",
		Config:      mustJSON(map[string]any{"system_prompt": "x"}),
	}
	am.mu.Unlock()

	panicCh := bus.Subscribe("watch-panic", events.EventType("ops.agent_panic"))
	failedCh := bus.Subscribe("empire-coordinator", events.EventType("ops.agent_failed"))
	agent := &panicProbeAgent{id: "worker-1"}

	am.handleAgentLoopPanic(context.Background(), agent, 1, "boom once")

	select {
	case evt := <-panicCh:
		if evt.Type != events.EventType("ops.agent_panic") {
			t.Fatalf("unexpected panic event type: %s", evt.Type)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected ops.agent_panic event for first panic")
	}

	select {
	case evt := <-failedCh:
		t.Fatalf("did not expect ops.agent_failed on first panic, got %s", evt.ID)
	case <-time.After(200 * time.Millisecond):

	}

	am.handleAgentLoopPanic(context.Background(), agent, 5, "boom terminal")

	select {
	case <-panicCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected ops.agent_panic on terminal panic")
	}
	select {
	case evt := <-failedCh:
		if evt.Type != events.EventType("ops.agent_failed") {
			t.Fatalf("unexpected failure escalation type: %s", evt.Type)
		}
	case <-time.After(800 * time.Millisecond):
		t.Fatal("expected ops.agent_failed escalation event after terminal panic")
	}
}

type templateErrStore struct {
	rec OrgTemplateRecord
	err error
}

func (s *templateErrStore) UpsertAgent(context.Context, PersistedAgent) error { return nil }

func (s *templateErrStore) LoadAgents(context.Context) ([]PersistedAgent, error) { return nil, nil }

func (s *templateErrStore) MarkAgentTerminated(context.Context, string) error { return nil }

func (s *templateErrStore) EnsureVerticalSchema(context.Context, string) error { return nil }

func (s *templateErrStore) LoadLatestOrgTemplate(context.Context) (OrgTemplateRecord, error) {
	return s.rec, s.err
}

func (s *templateErrStore) LoadOrgTemplate(context.Context, string) (OrgTemplateRecord, error) {
	return s.rec, s.err
}

func (s *templateErrStore) SetVerticalTemplateVersion(context.Context, string, string) error {
	return nil
}

func (s *templateErrStore) UpsertRoutingRule(context.Context, PersistedRoutingRule) error { return nil }

func (s *templateErrStore) LoadRoutingRules(context.Context) ([]PersistedRoutingRule, error) {
	return nil, nil
}

func (s *templateErrStore) DeactivateRoutingRulesByVertical(context.Context, string) error {
	return nil
}

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
	bus := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	am := NewAgentManager(bus, nil)

	if _, err := am.loadLatestTemplate(context.Background()); err == nil {
		t.Fatalf("expected error without store")
	}

	am.store = &templateErrStore{err: errors.New("boom")}
	if _, err := am.loadLatestTemplate(context.Background()); err == nil {
		t.Fatalf("expected store error")
	}

	am.store = &templateErrStore{rec: OrgTemplateRecord{Version: "", Agents: []byte("[]"), BootstrapRoutes: []byte("[]"), SeededRoutes: []byte("[]")}}
	if _, err := am.loadLatestTemplate(context.Background()); err == nil {
		t.Fatalf("expected empty version error")
	}

	am.store = &templateErrStore{rec: OrgTemplateRecord{Version: "v1", Agents: []byte("[]"), BootstrapRoutes: []byte("[]"), SeededRoutes: []byte("[]")}}
	if _, err := am.loadLatestTemplate(context.Background()); err == nil {
		t.Fatalf("expected missing bootstrap routes error")
	}

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
		VerticalID:    "v1",
		FounderNotes:  "notes",
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

type heartbeatStoreStub struct {
	schedules []runtimepipeline.Schedule
}

func (s *heartbeatStoreStub) UpsertSchedule(_ context.Context, sc runtimepipeline.Schedule) error {
	s.schedules = append(s.schedules, sc)
	return nil
}

func (s *heartbeatStoreStub) CancelSchedule(context.Context, string, string) error { return nil }

func (s *heartbeatStoreStub) LoadActiveSchedules(context.Context) ([]runtimepipeline.Schedule, error) {
	return nil, nil
}

func (s *heartbeatStoreStub) MarkScheduleFired(context.Context, runtimepipeline.Schedule) error {
	return nil
}

func (s *heartbeatStoreStub) UpsertAgent(context.Context, PersistedAgent) error { return nil }

func (s *heartbeatStoreStub) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return nil, nil
}

func (s *heartbeatStoreStub) MarkAgentTerminated(context.Context, string) error { return nil }

func (s *heartbeatStoreStub) EnsureVerticalSchema(context.Context, string) error { return nil }

func (s *heartbeatStoreStub) LoadLatestOrgTemplate(context.Context) (OrgTemplateRecord, error) {
	return OrgTemplateRecord{}, nil
}

func (s *heartbeatStoreStub) LoadOrgTemplate(context.Context, string) (OrgTemplateRecord, error) {
	return OrgTemplateRecord{}, nil
}

func (s *heartbeatStoreStub) SetVerticalTemplateVersion(context.Context, string, string) error {
	return nil
}

func (s *heartbeatStoreStub) UpsertRoutingRule(context.Context, PersistedRoutingRule) error {
	return nil
}

func (s *heartbeatStoreStub) LoadRoutingRules(context.Context) ([]PersistedRoutingRule, error) {
	return nil, nil
}

func (s *heartbeatStoreStub) DeactivateRoutingRulesByVertical(context.Context, string) error {
	return nil
}

func (s *heartbeatStoreStub) UpsertEventReceipt(context.Context, string, string, string, string) error {
	return nil
}

func (s *heartbeatStoreStub) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

func (s *heartbeatStoreStub) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

func TestAgentManager_InstallDefaultOpCoHeartbeats(t *testing.T) {
	ctx := context.Background()
	stub := &heartbeatStoreStub{}
	am := NewAgentManager(runtimebus.NewEventBus(runtimebus.InMemoryEventStore{}), nil, stub)

	verticalID := "11111111-1111-1111-1111-111111111111"
	if err := am.installDefaultOpCoHeartbeats(ctx, verticalID); err != nil {
		t.Fatalf("installDefaultOpCoHeartbeats: %v", err)
	}
	if len(stub.schedules) != 3 {
		t.Fatalf("expected 3 heartbeat schedules, got %d", len(stub.schedules))
	}
	if stub.schedules[0].AgentID == "" || stub.schedules[0].VerticalID != verticalID {
		t.Fatalf("unexpected first heartbeat schedule: %+v", stub.schedules[0])
	}

	if err := am.installDefaultOpCoHeartbeats(ctx, ""); err != nil {
		t.Fatalf("empty vertical should be noop, got err=%v", err)
	}
}
