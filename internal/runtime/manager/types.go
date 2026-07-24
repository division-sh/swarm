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
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
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
	SweepUndispatched(ctx context.Context, limit int) (int, error)
	PipelineWorkPresence(context.Context) (runtimepipelineobligation.GlobalWorkPresence, error)
	Store() runtimebus.EventStore
	ResetInMemoryState() error
	LogRuntime(ctx context.Context, entry runtimepipeline.RuntimeLogEntry) error
}

type PersistedAgent struct {
	Config              models.AgentConfig
	ParentAgentID       string
	CoordinatorID       string
	Status              string
	HiredBy             string
	TemplateVersion     string
	StartedAt           time.Time
	LifecycleEpoch      int64
	LifecycleGeneration uint64
	LifecyclePhase      AgentLifecyclePhase
	LifecycleRunMode    AgentRunMode
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
	Failure    *runtimefailures.Envelope
}

type ReceiptStatus string

const (
	ReceiptStatusProcessed  ReceiptStatus = "processed"
	ReceiptStatusError      ReceiptStatus = "error"
	ReceiptStatusTerminal   ReceiptStatus = "terminal"
	ReceiptStatusDeadLetter ReceiptStatus = "dead_letter"
)

type AgentPersistence interface {
	UpsertAgent(ctx context.Context, rec PersistedAgent) error
	LoadAgents(ctx context.Context) ([]PersistedAgent, error)
}

type AgentLifecyclePhase string

const (
	AgentLifecycleRegistered AgentLifecyclePhase = "registered"
	AgentLifecycleRunning    AgentLifecyclePhase = "running"
	AgentLifecycleTerminated AgentLifecyclePhase = "terminated"
	AgentLifecycleFailed     AgentLifecyclePhase = "failed"
)

type AgentRunMode string

const (
	AgentRunModeStopped                   AgentRunMode = "stopped"
	AgentRunModeStandard                  AgentRunMode = "standard"
	AgentRunModeAuthoritativeDeliveryOnly AgentRunMode = "authoritative_delivery_only"
)

type AgentLifecycleTransition struct {
	OperationID        string
	OperationKind      string
	RequestHash        string
	AgentID            string
	Trigger            string
	ExpectedEpoch      int64
	ExpectedGeneration uint64
	ExpectedPhase      AgentLifecyclePhase
	TargetEpoch        int64
	TargetGeneration   uint64
	TargetPhase        AgentLifecyclePhase
	ConfigRevision     string
	RunMode            AgentRunMode
	Agent              *PersistedAgent
	Subordinate        sessions.LifecycleMutationPlan
	Now                time.Time
}

type AgentLifecycleTransitionResult struct {
	OperationID        string                            `json:"operation_id"`
	TransitionID       string                            `json:"transition_id"`
	AgentID            string                            `json:"agent_id"`
	PreviousEpoch      int64                             `json:"previous_epoch"`
	RuntimeEpoch       int64                             `json:"runtime_epoch"`
	PreviousGeneration uint64                            `json:"previous_generation"`
	Generation         uint64                            `json:"generation"`
	PreviousPhase      AgentLifecyclePhase               `json:"previous_phase"`
	Phase              AgentLifecyclePhase               `json:"phase"`
	ConfigRevision     string                            `json:"config_revision"`
	RunMode            AgentRunMode                      `json:"run_mode"`
	Subordinate        sessions.LifecycleMutationOutcome `json:"subordinate"`
	Replayed           bool                              `json:"-"`
}

type AgentLifecyclePersistence interface {
	CommitAgentLifecycleTransition(context.Context, AgentLifecycleTransition) (AgentLifecycleTransitionResult, error)
}

type AgentLifecycleDiagnostic struct {
	OutboxID    string
	OperationID string
	AgentID     string
	EventName   string
	Payload     map[string]any
	CreatedAt   time.Time
}

type AgentLifecycleDiagnosticPersistence interface {
	ListPendingAgentLifecycleDiagnostics(context.Context, int) ([]AgentLifecycleDiagnostic, error)
	MarkAgentLifecycleDiagnosticProjected(context.Context, string, time.Time) error
}

type ManagerPersistence interface {
	AgentPersistence
}

type BudgetGuard interface {
	ProjectRecoveryBudgetState(ctx context.Context) error
	IsEntityEmergency(entityID string) bool
	IsEntityThrottle(entityID string) bool
	IsEmergency(entityID string) bool
	IsThrottle(entityID string) bool
}

type StrategicContext = json.RawMessage

type AgentManagerOptions struct {
	BaseContext                    context.Context
	LifecycleStore                 AgentLifecyclePersistence
	DeliveryStore                  runtimedelivery.Store
	TestLifecycleProbe             runtimelifecycleprobe.Observer
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
	WorkOwner                      worklifetime.Occurrence
	RuntimeIngressSafetyPause      func(context.Context, string, *runtimefailures.Envelope) error
	NativeToolAdmissionValidator   func(context.Context, models.AgentConfig) error
	ThrottleSuppressPrefixes       []string
	DisableSpinupControl           bool
	EnableLegacySpinupControl      bool
}
