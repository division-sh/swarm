package manager

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type Agent interface {
	ID() string
	Type() string
	Subscriptions() []events.EventType
	OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error)
}

type BoardInteractiveAgent interface {
	BoardStep(ctx context.Context, directive runtimeagentcontrol.BoardDirective) (string, error)
}

type AgentFactory func(cfg models.AgentConfig) (Agent, error)

type Bus interface {
	Publish(ctx context.Context, evt events.Event) error
	PublishDirect(ctx context.Context, evt events.Event, recipients []string) error
	PublishPersistedRecipients(ctx context.Context, evt events.Event, recipients []string) error
	Subscribe(agentID string, eventTypes ...events.EventType) <-chan events.Event
	Unsubscribe(agentID string)
	Store() runtimebus.EventStore
	ResetInMemoryState() error
	LogRuntime(ctx context.Context, entry runtimepipeline.RuntimeLogEntry) error
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
	EntityID         string
	EventPattern     string
	SubscriberID     string
	InstalledBy      string
	Reason           string
	Status           string
	Source           string
	BootstrapVersion int
}

func (r PersistedRoutingRule) EffectiveEntityID() string {
	return strings.TrimSpace(r.EntityID)
}

func (r *PersistedRoutingRule) NormalizeEntityID() {
	if r == nil {
		return
	}
	entityID := r.EffectiveEntityID()
	r.EntityID = entityID
}

type EventReceipt struct {
	EventID    string
	AgentID    string
	Status     ReceiptStatus
	RetryCount int
	Error      string
}

type ReceiptStatus string

const (
	ReceiptStatusProcessed  ReceiptStatus = "processed"
	ReceiptStatusError      ReceiptStatus = "error"
	ReceiptStatusDeadLetter ReceiptStatus = "dead_letter"
)

type AgentPersistence interface {
	UpsertAgent(ctx context.Context, rec PersistedAgent) error
	LoadAgents(ctx context.Context) ([]PersistedAgent, error)
	MarkAgentTerminated(ctx context.Context, agentID string) error
}

type EntitySchemaPersistence interface {
	EnsureEntitySchema(ctx context.Context, entityID string) error
}

type ReceiptPersistence interface {
	UpsertEventReceipt(ctx context.Context, eventID, agentID string, status ReceiptStatus, errText string) error
}

type PendingEventPersistence interface {
	ListPendingEventsForAgent(ctx context.Context, agentID string, since time.Time, limit int) ([]events.Event, error)
	ListPendingSubscribedEvents(ctx context.Context, agentID string, subscriptions []events.EventType, since time.Time, limit int) ([]events.Event, error)
}

type ManagerPersistence interface {
	AgentPersistence
	EntitySchemaPersistence
	ReceiptPersistence
	PendingEventPersistence
}

type BudgetGuard interface {
	IsEntityEmergency(entityID string) bool
	IsEntityThrottle(entityID string) bool
	IsEmergency(entityID string) bool
	IsThrottle(entityID string) bool
}

type StrategicContext = json.RawMessage

type AgentManagerOptions struct {
	Workspaces                     workspace.Lifecycle
	Sessions                       sessions.Registry
	SemanticSource                 semanticview.Source
	PromptResolver                 runtimecontracts.PromptResolver
	WorkflowInstances              flowInstancePersistence
	RuntimeMode                    string
	LLMBackend                     string
	ModelAliases                   llmselection.ModelAliases
	RequireModelResolution         bool
	Budget                         BudgetGuard
	ResetRuntimeOwnedState         func()
	RuntimeShutdownAdmissionClosed func() bool
	RuntimeIngressSafetyPause      func(context.Context, string) error
	ThrottleSuppressPrefixes       []string
	DisableSpinupControl           bool
	EnableLegacySpinupControl      bool
}
