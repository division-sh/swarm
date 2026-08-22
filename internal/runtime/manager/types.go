package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/google/uuid"
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
	AdmitBundleSourceFact(context.Context) (context.Context, error)
	Publish(ctx context.Context, evt events.Event) error
	PublishDirect(ctx context.Context, evt events.Event, recipients []string) error
	SweepPipelineObligations(ctx context.Context, limit int) (runtimepipelineobligation.SweepResult, error)
	PipelineWorkPresence(context.Context) (runtimepipelineobligation.GlobalWorkPresence, error)
	Store() runtimebus.EventStore
	ResetInMemoryState() error
	LogRuntime(ctx context.Context, entry runtimepipeline.RuntimeLogEntry) error
}

type PersistedAgent struct {
	Config              models.AgentConfig
	Topology            runtimeagenttopology.Admission
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
	ProcessBinding      ProcessExecutionBinding
}

// ProcessExecutionBinding seals a durable lifecycle cell to the exact
// selected-store process and generation grant allowed to execute it.
type ProcessExecutionBinding struct {
	ProcessAuthorityID string `json:"process_authority_id"`
	ProcessOwnerID     string `json:"process_owner_id"`
	ProcessBootID      string `json:"process_boot_id"`
	GenerationGrantID  string `json:"generation_grant_id"`
	BundleHash         string `json:"bundle_hash"`
	BundleSource       string `json:"bundle_source"`
	RuntimeInstanceID  string `json:"runtime_instance_id"`
	RuntimeGeneration  uint64 `json:"runtime_generation"`
}

func (b ProcessExecutionBinding) IsZero() bool {
	return strings.TrimSpace(b.ProcessAuthorityID) == "" && strings.TrimSpace(b.ProcessOwnerID) == "" &&
		strings.TrimSpace(b.ProcessBootID) == "" && strings.TrimSpace(b.GenerationGrantID) == "" &&
		strings.TrimSpace(b.BundleHash) == "" && strings.TrimSpace(b.BundleSource) == "" &&
		strings.TrimSpace(b.RuntimeInstanceID) == "" && b.RuntimeGeneration == 0
}

func (b ProcessExecutionBinding) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(b.ProcessAuthorityID)); err != nil {
		return fmt.Errorf("process execution authority is invalid: %w", err)
	}
	if strings.TrimSpace(b.ProcessOwnerID) == "" {
		return fmt.Errorf("process execution owner is required")
	}
	if _, err := uuid.Parse(strings.TrimSpace(b.ProcessBootID)); err != nil {
		return fmt.Errorf("process execution boot is invalid: %w", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(b.GenerationGrantID)); err != nil {
		return fmt.Errorf("process execution generation grant is invalid: %w", err)
	}
	if err := runtimebundleidentity.ValidateCanonicalHash(strings.TrimSpace(b.BundleHash)); err != nil {
		return fmt.Errorf("process execution bundle hash is invalid: %w", err)
	}
	switch strings.TrimSpace(b.BundleSource) {
	case "persisted", "ephemeral":
	default:
		return fmt.Errorf("process execution bundle source is invalid")
	}
	if _, err := uuid.Parse(strings.TrimSpace(b.RuntimeInstanceID)); err != nil {
		return fmt.Errorf("process execution runtime instance is invalid: %w", err)
	}
	if b.RuntimeGeneration == 0 {
		return fmt.Errorf("process execution runtime generation is required")
	}
	return nil
}

func (b ProcessExecutionBinding) Equal(other ProcessExecutionBinding) bool {
	return strings.TrimSpace(b.ProcessAuthorityID) == strings.TrimSpace(other.ProcessAuthorityID) &&
		strings.TrimSpace(b.ProcessOwnerID) == strings.TrimSpace(other.ProcessOwnerID) &&
		strings.TrimSpace(b.ProcessBootID) == strings.TrimSpace(other.ProcessBootID) &&
		strings.TrimSpace(b.GenerationGrantID) == strings.TrimSpace(other.GenerationGrantID) &&
		strings.TrimSpace(b.BundleHash) == strings.TrimSpace(other.BundleHash) &&
		strings.TrimSpace(b.BundleSource) == strings.TrimSpace(other.BundleSource) &&
		strings.TrimSpace(b.RuntimeInstanceID) == strings.TrimSpace(other.RuntimeInstanceID) &&
		b.RuntimeGeneration == other.RuntimeGeneration
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
	LoadAgents(ctx context.Context) ([]PersistedAgent, error)
}

type AgentLifecyclePhase string

const (
	AgentLifecycleRegistered AgentLifecyclePhase = "registered"
	AgentLifecycleRunning    AgentLifecyclePhase = "running"
	AgentLifecycleDraining   AgentLifecyclePhase = "draining"
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
	Identity           runtimeagentidentity.Identity
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
	Topology           runtimeagenttopology.Admission
	ProcessBinding     ProcessExecutionBinding
	Now                time.Time
}

type AgentLifecycleState struct {
	Identity       runtimeagentidentity.Identity
	AgentID        string
	RuntimeEpoch   int64
	Generation     uint64
	Phase          AgentLifecyclePhase
	ConfigRevision string
	RunMode        AgentRunMode
	Topology       runtimeagenttopology.Admission
	ProcessBinding ProcessExecutionBinding
}

type AgentLifecycleTransitionResult struct {
	OperationID         string                            `json:"operation_id"`
	TransitionID        string                            `json:"transition_id"`
	Identity            runtimeagentidentity.Identity     `json:"identity"`
	AgentID             string                            `json:"agent_id"`
	PreviousEpoch       int64                             `json:"previous_epoch"`
	RuntimeEpoch        int64                             `json:"runtime_epoch"`
	PreviousGeneration  uint64                            `json:"previous_generation"`
	Generation          uint64                            `json:"generation"`
	PreviousPhase       AgentLifecyclePhase               `json:"previous_phase"`
	Phase               AgentLifecyclePhase               `json:"phase"`
	ConfigRevision      string                            `json:"config_revision"`
	RunMode             AgentRunMode                      `json:"run_mode"`
	Topology            runtimeagenttopology.Admission    `json:"topology"`
	ProcessBinding      ProcessExecutionBinding           `json:"process_binding"`
	Subordinate         sessions.LifecycleMutationOutcome `json:"subordinate"`
	ProviderDrainCount  int                               `json:"provider_drain_count"`
	ProviderDrainTarget AgentLifecyclePhase               `json:"provider_drain_target,omitempty"`
	Replayed            bool                              `json:"-"`
}

type AgentLifecyclePersistence interface {
	CommitAgentLifecycleTransition(context.Context, AgentLifecycleTransition) (AgentLifecycleTransitionResult, error)
}

type AgentLifecycleStateReader interface {
	LoadAgentLifecycleState(context.Context, runtimeagentidentity.Identity) (AgentLifecycleState, bool, error)
}

type AgentLifecycleDiagnostic struct {
	OutboxID    string
	OperationID string
	Identity    runtimeagentidentity.Identity
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

type DeliveryRuntimeOwner interface {
	DeliveryAuthority() (runtimedelivery.ExecutionAuthority, error)
	RetainDeliveryContinuation(runtimedelivery.Snapshot) error
	ReleaseDeliveryContinuation(string) error
}

// PersistenceRoles is the exact immutable persistence and route contract
// consumed by AgentManager. No role is discovered from Bus or ManagerStore.
type PersistenceRoles struct {
	AgentRoutes          AgentRouteBus
	FlowActivation       FlowInstanceActivationCommitter
	RouteInstaller       FlowInstanceRouteContextInstaller
	RouteVerifier        FlowInstanceRouteContextVerifier
	RouteRestorer        PersistedFlowInstanceRouteRestorer
	RouteRetirer         PublishedFlowInstanceRouteRetirer
	RouteRemover         FlowInstanceRouteContextRemover
	FlowTermination      FlowInstanceTerminalMutationOwner
	CreationPublisher    runtimepipeline.DynamicFlowRuntimeCreationOccurrencePublisher
	LifecycleState       AgentLifecycleStateReader
	LifecycleEffects     runtimeeffects.Store
	LifecycleDiagnostics AgentLifecycleDiagnosticPersistence
	EffectsRecovery      runtimeeffects.RecoveryStore
	DeliveryQuiescence   ActiveRunDeliveryQuiescenceReader
	DeliveryRuntime      DeliveryRuntimeOwner
	EventExistence       EventExistenceReader
	DirectiveOperations  runtimeagentcontrol.DirectiveOperationStore
	DirectiveTargets     AgentDirectiveRunTargetResolver
	FlowRoutes           runtimebus.FlowInstanceRoutePersistence
}

type StrategicContext = json.RawMessage

type AgentManagerOptions struct {
	ExecutionPosture               executionposture.Posture
	BaseContext                    context.Context
	BundleSourceFact               runtimecorrelation.BundleSourceFact
	LifecycleStore                 AgentLifecyclePersistence
	DeliveryStore                  runtimedelivery.Store
	TestLifecycleProbe             runtimelifecycleprobe.Observer
	Workspaces                     workspace.Lifecycle
	Sessions                       sessions.Registry
	SessionLifecycle               sessions.LifecycleProjection
	SessionResetter                sessions.Resetter
	PersistenceRoles               PersistenceRoles
	SemanticSource                 semanticview.Source
	WorkflowInstances              flowInstancePersistence
	RuntimeMode                    string
	LLMBackend                     string
	ModelAliases                   llmselection.ModelAliases
	RequireModelResolution         bool
	Budget                         BudgetGuard
	ResetRuntimeOwnedState         func()
	RuntimeShutdownAdmissionClosed func() bool
	WorkOwner                      worklifetime.Occurrence
	ReceiverExecution              eventreceiver.ExecutionVariant
	RuntimeIngressSafetyPause      func(context.Context, string, *runtimefailures.Envelope) error
	NativeToolAdmissionValidator   func(context.Context, models.AgentConfig) error
	ThrottleSuppressPrefixes       []string
	DisableSpinupControl           bool
	EnableLegacySpinupControl      bool
}
