package manager

import (
	"context"
	"encoding/json"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimebus "empireai/internal/runtime/bus"
	runtimepipeline "empireai/internal/runtime/pipeline"
)

type Agent interface {
	ID() string
	Type() string
	Subscriptions() []events.EventType
	OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error)
}

type BoardInteractiveAgent interface {
	BoardStep(ctx context.Context, directive string) (string, error)
}

type AgentFactory func(cfg models.AgentConfig) (Agent, error)

type Bus interface {
	Publish(ctx context.Context, evt events.Event) error
	PublishDirect(ctx context.Context, evt events.Event, recipients []string) error
	Subscribe(agentID string, eventTypes ...events.EventType) <-chan events.Event
	Unsubscribe(agentID string)
	Store() runtimebus.EventStore
	ResetInMemoryState()
	SetRoutingTable(verticalID string, table *runtimebus.RoutingTable) error
	GetRoutingTable(verticalID string) *runtimebus.RoutingTable
	LogRuntime(ctx context.Context, entry runtimepipeline.RuntimeLogEntry)
}

type PersistedAgent struct {
	Config          models.AgentConfig
	ParentAgentID   string
	CoordinatorID   string
	Status          string
	HiredBy         string
	TemplateVersion string
	StartedAt       time.Time
}

type PersistedRoutingRule struct {
	VerticalID       string
	EventPattern     string
	SubscriberID     string
	InstalledBy      string
	Reason           string
	Status           string
	Source           string
	BootstrapVersion int
}

type BootstrapVersionResolver interface {
	ResolveBootstrapVersion(ctx context.Context, templateVersion string) (int, error)
}

type VerticalInfoReader interface {
	GetVerticalInfo(ctx context.Context, verticalID string) (VerticalInfo, bool, error)
}

type VerticalInfo struct {
	ID        string
	Name      string
	Slug      string
	Geography string
	Stage     string
}

type EventReceiptReader interface {
	GetEventReceipt(ctx context.Context, eventID, agentID string) (EventReceipt, bool, error)
}

type EventReceipt struct {
	EventID    string
	AgentID    string
	Status     string
	RetryCount int
	Error      string
}

type PromptOverrideRecord struct {
	AgentID        string
	Prompt         string
	PreviousPrompt string
	Source         string
	Notes          string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type PromptOverridePersistence interface {
	GetPromptOverride(ctx context.Context, agentID string) (PromptOverrideRecord, bool, error)
	UpsertPromptOverride(ctx context.Context, rec PromptOverrideRecord) error
	DeletePromptOverride(ctx context.Context, agentID string) error
}

type OrgTemplateRecord struct {
	Version         string
	Agents          []byte
	BootstrapRoutes []byte
	SeededRoutes    []byte
	CreatedBy       string
	Description     string
	CreatedAt       time.Time
}

type AgentPersistence interface {
	UpsertAgent(ctx context.Context, rec PersistedAgent) error
	LoadAgents(ctx context.Context) ([]PersistedAgent, error)
	MarkAgentTerminated(ctx context.Context, agentID string) error
}

type TemplatePersistence interface {
	EnsureVerticalSchema(ctx context.Context, verticalID string) error
	LoadLatestOrgTemplate(ctx context.Context) (OrgTemplateRecord, error)
	LoadOrgTemplate(ctx context.Context, version string) (OrgTemplateRecord, error)
	SetVerticalTemplateVersion(ctx context.Context, verticalID, version string) error
}

type RoutingPersistence interface {
	UpsertRoutingRule(ctx context.Context, rule PersistedRoutingRule) error
	LoadRoutingRules(ctx context.Context) ([]PersistedRoutingRule, error)
	DeactivateRoutingRulesByVertical(ctx context.Context, verticalID string) error
}

type ReceiptPersistence interface {
	UpsertEventReceipt(ctx context.Context, eventID, agentID, status, errText string) error
}

type PendingEventPersistence interface {
	ListPendingEventsForAgent(ctx context.Context, agentID string, since time.Time, limit int) ([]events.Event, error)
	ListPendingSubscribedEvents(ctx context.Context, agentID string, subscriptions []events.EventType, since time.Time, limit int) ([]events.Event, error)
}

type ManagerPersistence interface {
	AgentPersistence
	TemplatePersistence
	RoutingPersistence
	ReceiptPersistence
	PendingEventPersistence
}

type BudgetGuard interface {
	IsEmergency(verticalID string) bool
	IsThrottle(verticalID string) bool
}

type StrategicContext = json.RawMessage
