package tools_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	rt "empireai/internal/runtime"
	runtimeactor "empireai/internal/runtime/actorctx"
	runtimebus "empireai/internal/runtime/bus"
	runtimemanager "empireai/internal/runtime/manager"
	runtimepipeline "empireai/internal/runtime/pipeline"
	runtimetools "empireai/internal/runtime/tools"
)

type MailboxItem = runtimetools.MailboxItem
type Schedule = runtimepipeline.Schedule
type InMemoryEventStore = runtimebus.InMemoryEventStore
type EventBus = rt.EventBus
type AgentManager = runtimemanager.AgentManager
type Scheduler = runtimepipeline.Scheduler
type MailboxPersistence = runtimetools.MailboxPersistence
type SchedulePersistence = runtimetools.SchedulePersistence
type Agent = runtimemanager.Agent

func NewEventBus(store runtimebus.EventStore) *rt.EventBus { return rt.NewEventBus(store) }
func NewAgentManager(bus *rt.EventBus, factory runtimemanager.AgentFactory, stores ...runtimemanager.ManagerPersistence) *runtimemanager.AgentManager {
	return runtimemanager.NewAgentManager(bus, factory, stores...)
}
func NewScheduler(callbacks ...func(Schedule)) *runtimepipeline.Scheduler {
	return runtimepipeline.NewScheduler(callbacks...)
}
func NewRuntimeToolExecutor(bus *rt.EventBus, scheduler *runtimepipeline.Scheduler, manager *runtimemanager.AgentManager, stores ...SchedulePersistence) *runtimetools.Executor {
	var publisher runtimetools.EventPublisher
	if bus != nil {
		publisher = bus
	}
	var sched runtimetools.Scheduler
	if scheduler != nil {
		sched = scheduler
	}
	var mgr runtimetools.Manager
	if manager != nil {
		mgr = manager
	}
	return runtimetools.NewExecutor(publisher, sched, mgr, stores...)
}
func WithActor(ctx context.Context, actor models.AgentConfig) context.Context {
	return runtimeactor.WithActor(ctx, actor)
}
func WithInboundEvent(ctx context.Context, evt events.Event) context.Context {
	return runtimebus.WithInboundEvent(ctx, evt)
}

type stubAgent struct {
	id   string
	typ  string
	subs []events.EventType
}

func (s *stubAgent) ID() string                                                    { return s.id }
func (s *stubAgent) Type() string                                                  { return s.typ }
func (s *stubAgent) Subscriptions() []events.EventType                             { return s.subs }
func (s *stubAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) { return nil, nil }

type managerStoreStub struct{}

func (m *managerStoreStub) UpsertAgent(context.Context, runtimemanager.PersistedAgent) error {
	return nil
}
func (m *managerStoreStub) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return nil, nil
}
func (m *managerStoreStub) MarkAgentTerminated(context.Context, string) error  { return nil }
func (m *managerStoreStub) EnsureVerticalSchema(context.Context, string) error { return nil }
func (m *managerStoreStub) LoadLatestOrgTemplate(context.Context) (runtimemanager.OrgTemplateRecord, error) {
	return runtimemanager.OrgTemplateRecord{}, nil
}
func (m *managerStoreStub) LoadOrgTemplate(context.Context, string) (runtimemanager.OrgTemplateRecord, error) {
	return runtimemanager.OrgTemplateRecord{}, nil
}
func (m *managerStoreStub) SetVerticalTemplateVersion(context.Context, string, string) error {
	return nil
}
func (m *managerStoreStub) UpsertRoutingRule(context.Context, runtimemanager.PersistedRoutingRule) error {
	return nil
}
func (m *managerStoreStub) LoadRoutingRules(context.Context) ([]runtimemanager.PersistedRoutingRule, error) {
	return nil, nil
}
func (m *managerStoreStub) DeactivateRoutingRulesByVertical(context.Context, string) error {
	return nil
}
func (m *managerStoreStub) UpsertEventReceipt(context.Context, string, string, string, string) error {
	return nil
}
func (m *managerStoreStub) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (m *managerStoreStub) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

func SafeTelemetryText(v any) string               { return runtimetools.SafeTelemetryText(v) }
func TruncateTelemetry(s string, limit int) string { return runtimetools.TruncateTelemetry(s, limit) }
func DefaultExternalMethod(name string) string     { return runtimetools.DefaultExternalMethod(name) }
func ApplyExternalHeaders(req *http.Request, headers map[string]any) {
	runtimetools.ApplyExternalHeaders(req, headers)
}
func ApplyExternalCredentialHeaders(req *http.Request, creds map[string]any, toolName string) {
	runtimetools.ApplyExternalCredentialHeaders(req, creds, toolName)
}
func ParseExternalResponseBody(body []byte) any { return runtimetools.ParseExternalResponseBody(body) }
func DefaultExternalCredentialEnv(toolName string) map[string]string {
	return runtimetools.DefaultExternalCredentialEnv(toolName)
}
func RedactTelemetryValue(v any) any { return runtimetools.RedactTelemetryValue(v) }
func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return ""
	default:
		return fmt.Sprint(t)
	}
}

func decodeToolInput(input any, out any) error {
	b, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("marshal input: %w", err)
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("decode input: %w", err)
	}
	return nil
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

type captureStore struct {
	events []events.Event
}

func (c *captureStore) AppendEvent(_ context.Context, evt events.Event) error {
	c.events = append(c.events, evt)
	return nil
}

func (c *captureStore) LoadAllEvents(context.Context) ([]events.Event, error) {
	return append([]events.Event(nil), c.events...), nil
}

func (c *captureStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}
