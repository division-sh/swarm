package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimeagents "github.com/division-sh/swarm/internal/runtime/agents"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedeliverycontinuation "github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/scenarioderivation"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/google/uuid"
)

type EventPayloadValidationBinder interface {
	SetEventPayloadValidator(func(context.Context, string, []byte) error)
}

type AuthorActivityCatalogRegistrar interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

type RuntimeOptions struct {
	SelfCheck                        bool
	WorkspaceLifecycle               workspace.Lifecycle
	EnableToolGateway                bool
	ToolGatewayBinding               toolgateway.Binding
	BundleSourceFact                 runtimecorrelation.BundleSourceFact
	RuntimeInstanceID                string
	ProcessWorkOwner                 *worklifetime.Process
	WorkflowModule                   runtimepipeline.WorkflowModule
	LLMRuntime                       llm.Runtime
	Credentials                      runtimecredentials.Store
	ManagedCredentials               runtimemanagedcredentials.Store
	ProviderCredentials              runtimecredentials.Store
	ProviderTriggerCatalog           *providertriggers.CatalogSnapshot
	NoticePresentation               runtimetools.InformationalNoticePresentationSink
	ChannelPlans                     []packs.SatisfactionPlan
	ChannelOutboundBindings          []packs.OutboundBindingPlan
	ScenarioDeclarations             []scenarioderivation.Declaration
	BootStartedAt                    time.Time
	BootProgress                     func(BootProgressEvent)
	SystemContainers                 []string
	DisablePersistentStartupRecovery bool
	TestEntityStateHook              func(entityID, state string)
	TestWorkflowNodeHandlerStartHook runtimepipeline.WorkflowNodeHandlerStartHook
	TestLifecycleProbe               runtimelifecycleprobe.Observer
	TestOutboxSweeperConfig          runtimebus.OutboxSweeperConfig
}

// RuntimeDeps is the canonical dependency graph for NewRuntime boot wiring.
type RuntimeDeps struct {
	Config                         *config.Config
	Options                        RuntimeOptions
	EventStore                     runtimebus.EventStore
	EventBusDurable                runtimebus.DurableDependencies
	EventPayloadValidationBinder   EventPayloadValidationBinder
	InboundPayloadValidationBinder EventPayloadValidationBinder
	AuthorActivityRegistrars       []AuthorActivityCatalogRegistrar
	RunControlStore                runtimeruncontrol.Store
	RunLifecycleCandidates         runtimerunlifecycle.CandidateOwner
	RuntimeLogStore                RuntimeLogPersistence
	WorkflowPersistence            runtimepipeline.WorkflowPersistence
	RunBundleAvailability          runtimepipeline.RunBundleAvailabilityReader
	SessionRegistry                sessions.Registry
	LiveSessionAcquirer            llm.LiveSessionAcquirer
	SessionResetter                sessions.Resetter
	ConversationStore              llm.ConversationPersistence
	ManagerStore                   runtimemanager.ManagerPersistence
	ManagerLifecycleDiagnostics    runtimemanager.AgentLifecycleDiagnosticPersistence
	ManagerPersistenceRoles        runtimemanager.PersistenceRoles
	EffectsStore                   runtimeeffects.Store
	CompletionStore                runtimeeffects.CompletionStore
	CompletionHeartbeatStore       runtimeeffects.CompletionHeartbeatStore
	EffectsRecoveryStore           runtimeeffects.RecoveryStore
	ManagedCapabilitiesStore       managedcapabilities.Persistence
	DeliveryStore                  runtimedelivery.Store
	PipelineObligations            runtimepipelineobligation.Store
	GenericScheduleStore           runtimegenericschedule.Store
	TimerObligationReader          runtimetimerobligation.Reader
	MailboxMaterializer            runtimepipeline.MailboxWriteMaterializationStore
	DecisionCards                  decisioncard.Store
	ProposedEffects                decisioncard.ProposedEffectStore
	DecisionCardHumanTasks         decisioncard.HumanTaskStore
	DecisionCardDraftExpiry        runtimepipeline.DecisionCardDraftExpiry
	HumanTaskExpiry                runtimepipeline.HumanTaskExpiry
	StartupGrant                   runtimestartupownership.GenerationGrant
	MailboxStore                   runtimetools.MailboxPersistence
	ToolEntityStore                runtimetools.EntityPersistence
	DataAccessStore                durabledata.ResourceAccessStore
	HumanTaskStore                 runtimetools.HumanTaskCardStore
	BudgetSpendStore               budgetspend.Store
	InboundStore                   InboundPersistence
	RuntimeIngressStore            runtimeingress.Store
	ScenarioExecutionProfiles      runtimepipeline.ScenarioExecutionProfileReader
}

type validatedRuntimeDeps struct {
	Dependencies               RuntimeDeps
	Config                     *config.Config
	Options                    RuntimeOptions
	Source                     semanticview.Source
	Credentials                runtimecredentials.Store
	ManagedCredentials         runtimemanagedcredentials.Store
	MockConnectorResponses     *providerconnectors.MockResponsePlan
	BootEffectReachability     runtimebootverify.SourceBootEffectReachability
	ExecutionPosture           executionposture.Posture
	ProviderCredentialResolver llm.ProviderCredentialResolver
	Authority                  runtimeauthority.Provider
	EmitRegistry               *runtimetools.EmitRegistry
	BundleSourceFact           runtimecorrelation.BundleSourceFact
	EffectiveSourceIdentity    scenarioexecution.EffectiveSourceIdentity
	ScenarioProfileCatalog     *scenarioexecution.Catalog
}

const BootProgressTotalSteps = 22

var canonicalBootProgressNames = [...]string{
	"process_start",
	"config_load",
	"db_connection",
	"bundle_load",
	"startup_ownership_lease",
	"recovery_snapshot_inspection",
	"recovery_decision",
	"pipeline_maintenance",
	"system_nodes_start",
	"manager_recovery_if_enabled",
	"schedule_restoration",
	"static_agents_bootstrap",
	"flow_required_agents",
	"workspace_validation_and_system_containers",
	"mcp_tool_validation",
	"manager_event_loop_start",
	"outbox_sweeper",
	"boot_self_check_optional",
	"platform_boot_event_published",
	"http_listener_bind",
	"health_endpoints_respond",
	"ready",
}

func CanonicalBootProgressName(step int) string {
	if step < 1 || step > len(canonicalBootProgressNames) {
		return ""
	}
	return canonicalBootProgressNames[step-1]
}

const DefaultShutdownGrace = runtimemanager.DefaultShutdownGrace

type ShutdownOptions struct {
	Grace time.Duration
}

func DefaultShutdownOptions() ShutdownOptions {
	return ShutdownOptions{Grace: DefaultShutdownGrace}
}

type BootProgressEvent struct {
	Step   int
	Total  int
	Name   string
	Status string
	Detail string
	At     time.Time
}

type Runtime struct {
	generationMu               sync.Mutex
	lifecycleMu                sync.Mutex
	startupPrepareMu           sync.Mutex
	startCtx                   context.Context
	cancelStart                context.CancelFunc
	startupGrant               runtimestartupownership.GenerationGrant
	startupLifecyclePrepared   bool
	replacementQuiesced        bool
	workOccurrence             *worklifetime.RuntimeOccurrence
	runLifecycleExecutor       *runtimerunlifecycle.Executor
	runLifecycleRegistration   runtimerunlifecycle.CandidateRegistration
	deliveryContinuations      *runtimedeliverycontinuation.Coordinator
	deliverySignalRegistration *runtimepipeline.DeliveryContinuationSignalRegistration
	startupAdmission           managedexecution.Admission
	shutdownGate               shutdownAdmission
	payloadValidator           runtimebus.PayloadValidator
	authorActivityDescriptors  []runtimeauthoractivity.EventDescriptor
	authorActivityScope        runtimeauthoractivity.Scope
	authorActivityLeases       []*runtimeauthoractivity.EventCatalogLease
	authorActivityRegistrars   []AuthorActivityCatalogRegistrar
	eventPayloadBinder         EventPayloadValidationBinder
	inboundPayloadBinder       EventPayloadValidationBinder
	runLifecycleCandidates     runtimerunlifecycle.CandidateOwner
	deliveryStore              runtimedelivery.Store
	timerObligationReader      runtimetimerobligation.Reader
	mailboxStore               runtimetools.MailboxPersistence
	effectsStore               runtimeeffects.Store
	managedCapabilitiesStore   managedcapabilities.Persistence
	EffectiveSourceIdentity    scenarioexecution.EffectiveSourceIdentity
	ScenarioProfileCatalog     *scenarioexecution.Catalog

	Config             *config.Config
	ExecutionPosture   executionposture.Posture
	Options            RuntimeOptions
	Bus                *runtimebus.EventBus
	Logger             *RuntimeLogger
	Pipeline           *runtimepipeline.PipelineCoordinator
	SystemNodes        []runtimepipeline.BackgroundNode
	Scheduler          *runtimepipeline.Scheduler
	GenericSchedules   *runtimegenericschedule.Lifecycle
	Workspace          workspace.Lifecycle
	Budget             *BudgetTracker
	Credentials        runtimecredentials.Store
	ManagedCredentials runtimemanagedcredentials.Store
	LLMRuntimes        *llm.AgentRuntimeSet
	ToolExecutor       *runtimetools.Executor
	Manager            *runtimemanager.AgentManager
	RuntimeIngress     *runtimeingress.Controller
	RunControl         *runtimeruncontrol.Controller
	InboundGateway     *InboundGateway
	ToolGateway        *runtimemcp.Gateway
	MCPTurns           *runtimemcp.TurnContextRegistry
	Authority          runtimeauthority.Provider
	EmitRegistry       *runtimetools.EmitRegistry
}

func (rt *Runtime) emitBootProgress(step int, name, status, detail string) {
	if rt == nil || rt.Options.BootProgress == nil {
		return
	}
	if canonical := CanonicalBootProgressName(step); canonical != "" {
		name = canonical
	}
	rt.Options.BootProgress(BootProgressEvent{
		Step:   step,
		Total:  BootProgressTotalSteps,
		Name:   strings.TrimSpace(name),
		Status: strings.TrimSpace(status),
		Detail: strings.TrimSpace(detail),
		At:     time.Now().UTC(),
	})
}

func (rt *Runtime) shutdownAdmissionClosed() bool {
	if rt == nil {
		return false
	}
	return rt.shutdownGate.Closed()
}

func (rt *Runtime) CloseAdmission() {
	if rt != nil {
		rt.shutdownGate.Close()
		if rt.workOccurrence != nil {
			_ = rt.workOccurrence.Fence()
		}
	}
}

func (rt *Runtime) WorkOccurrence() *worklifetime.RuntimeOccurrence {
	if rt == nil {
		return nil
	}
	return rt.workOccurrence
}

// CurrentStartupGrantEvidence returns the exact generation authority currently
// installed on this runtime. It never exposes process-level release authority.
func (rt *Runtime) CurrentStartupGrantEvidence() (runtimestartupownership.GrantEvidence, error) {
	if rt == nil {
		return runtimestartupownership.GrantEvidence{}, fmt.Errorf("runtime is required")
	}
	rt.lifecycleMu.Lock()
	grant := rt.startupGrant
	rt.lifecycleMu.Unlock()
	if grant == nil {
		return runtimestartupownership.GrantEvidence{}, fmt.Errorf("runtime generation grant is unavailable")
	}
	return grant.Evidence()
}

// InstallStartupGrant binds one non-release-capable generation grant before
// Start. Process composition remains the sole owner of acquisition, topology
// mutation, generation replacement, and final selected-store release.
func (rt *Runtime) InstallStartupGrant(grant runtimestartupownership.GenerationGrant) error {
	if rt == nil || grant == nil {
		return fmt.Errorf("runtime generation grant is required")
	}
	rt.lifecycleMu.Lock()
	defer rt.lifecycleMu.Unlock()
	if rt.cancelStart != nil || rt.startupGrant != nil {
		return fmt.Errorf("runtime already started or has a generation grant")
	}
	evidence, err := grant.Evidence()
	if err != nil {
		return err
	}
	if evidence.BundleHash != rt.Options.BundleSourceFact.BundleHash() || evidence.RuntimeInstanceID != strings.TrimSpace(rt.Options.RuntimeInstanceID) {
		return fmt.Errorf("runtime generation grant does not match runtime source coordinate")
	}
	plan, err := grant.SourceSetPlan(context.Background())
	if err != nil {
		return fmt.Errorf("load generation source-set plan: %w", err)
	}
	admission, err := runtimeagenttopology.StaticAdmission(
		evidence.SourceSetRevision,
		evidence.BundleHash,
		evidence.BundleSource,
		runtimeagenttopology.LifetimeDurableManaged,
	)
	if err != nil {
		return err
	}
	if rt.Manager == nil {
		return errors.New("runtime generation grant requires an agent manager")
	}
	if err := rt.Manager.InstallStartupTopology(grant, admission, plan); err != nil {
		return err
	}
	rt.startupGrant = grant
	rt.startupLifecyclePrepared = false
	return nil
}

// PrepareStartupLifecycle rebinds durable lifecycle execution and reconciles
// topology without hydrating any process-local agent execution.
func (rt *Runtime) PrepareStartupLifecycle(ctx context.Context) error {
	if rt == nil {
		return errors.New("runtime is required")
	}
	if rt.Manager == nil {
		return nil
	}
	rt.startupPrepareMu.Lock()
	defer rt.startupPrepareMu.Unlock()
	rt.lifecycleMu.Lock()
	if rt.cancelStart != nil {
		rt.lifecycleMu.Unlock()
		return errors.New("runtime already started")
	}
	if rt.startupLifecyclePrepared {
		rt.lifecycleMu.Unlock()
		return nil
	}
	grant := rt.startupGrant
	rt.lifecycleMu.Unlock()
	if grant == nil {
		return errors.New("runtime generation grant is required before startup preparation")
	}
	if _, err := grant.Evidence(); err != nil {
		return err
	}
	if err := rt.Manager.RebindLifecycleExecutionForStartup(ctx); err != nil {
		return fmt.Errorf("rebind lifecycle process execution: %w", err)
	}
	if err := rt.Manager.PrepareStaticTopologyForStartup(ctx, rt.Options.WorkflowModule.SemanticSource()); err != nil {
		return fmt.Errorf("prepare static declaration topology: %w", err)
	}
	rt.lifecycleMu.Lock()
	defer rt.lifecycleMu.Unlock()
	if rt.cancelStart != nil || rt.startupGrant != grant {
		return errors.New("runtime generation changed during startup preparation")
	}
	rt.startupLifecyclePrepared = true
	return nil
}

// PreflightDynamicTopologyStartup observes only the current bundle source. It
// is safe to run across every serve context before any startup mutation.
func (rt *Runtime) PreflightDynamicTopologyStartup(ctx context.Context) error {
	if rt == nil || rt.Manager == nil {
		return nil
	}
	projection, err := rt.Manager.InspectDynamicFlowRuntimeStartupProjection(ctx, rt.Options.BundleSourceFact)
	if err != nil {
		return fmt.Errorf("inspect source-scoped dynamic topology startup: %w", err)
	}
	replayAllowed := rt.Config != nil && rt.Config.Runtime.RecoveryOnStartup && !rt.Options.DisablePersistentStartupRecovery
	if len(projection.Pending) > 0 && !replayAllowed {
		return fmt.Errorf("dynamic topology startup requires recovery for %d incomplete source-owned instance(s)", len(projection.Pending))
	}
	return nil
}

// PreparedStaticSourceSetGenerationRefresh reserves one runtime generation and
// all of its static lifecycle cells before process composition mutates the
// complete source set. Commit and Abort both settle every held lock exactly
// once.
type PreparedStaticSourceSetGenerationRefresh struct {
	mu              sync.Mutex
	runtime         *Runtime
	plan            runtimeagenttopology.SourceSetPlan
	current         runtimestartupownership.GenerationGrant
	currentEvidence runtimestartupownership.GrantEvidence
	topology        *runtimemanager.PreparedStaticTopologySourceSetRebind
	done            bool
}

// PrepareStaticSourceSetGenerationRefresh validates the successor topology
// against the live semantic source and reserves generation/static lifecycle
// mutation. It performs no selected-store mutation.
func (rt *Runtime) PrepareStaticSourceSetGenerationRefresh(
	plan runtimeagenttopology.SourceSetPlan,
	source semanticview.Source,
) (_ *PreparedStaticSourceSetGenerationRefresh, prepareErr error) {
	if rt == nil || rt.Manager == nil || source == nil {
		return nil, errors.New("runtime source-set generation refresh requires runtime, manager, and semantic source")
	}
	if err := plan.Validate(); err != nil {
		return nil, fmt.Errorf("runtime source-set generation refresh plan: %w", err)
	}
	rt.generationMu.Lock()
	keepLock := false
	defer func() {
		if !keepLock {
			rt.generationMu.Unlock()
		}
	}()

	rt.lifecycleMu.Lock()
	current := rt.startupGrant
	started := rt.cancelStart != nil
	rt.lifecycleMu.Unlock()
	if current == nil || !started {
		return nil, errors.New("runtime source-set generation refresh requires a running generation")
	}
	evidence, err := current.Evidence()
	if err != nil {
		return nil, err
	}
	if evidence.State != runtimestartupownership.GrantAdmitted {
		return nil, fmt.Errorf("runtime source-set generation refresh requires admitted grant, got %s", evidence.State)
	}
	bundleHash, bundleSource := rt.Options.BundleSourceFact.StorageValues()
	if evidence.BundleHash != bundleHash || evidence.BundleSource != bundleSource ||
		evidence.RuntimeInstanceID != strings.TrimSpace(rt.Options.RuntimeInstanceID) {
		return nil, errors.New("runtime source-set generation refresh grant differs from runtime coordinate")
	}
	admission, err := runtimeagenttopology.StaticAdmission(
		plan.Revision, bundleHash, bundleSource, runtimeagenttopology.LifetimeDurableManaged,
	)
	if err != nil {
		return nil, err
	}
	topology, err := rt.Manager.PrepareStaticTopologySourceSetRebind(admission, plan, source)
	if err != nil {
		return nil, err
	}
	keepLock = true
	return &PreparedStaticSourceSetGenerationRefresh{
		runtime: rt, plan: plan, current: current, currentEvidence: evidence, topology: topology,
	}, nil
}

func (p *PreparedStaticSourceSetGenerationRefresh) Abort() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return
	}
	p.done = true
	if p.topology != nil {
		p.topology.Abort()
	}
	p.runtime.generationMu.Unlock()
}

// Commit rotates the selected-store generation grant and persists every
// static topology admission through the locks acquired by Prepare. The
// successor remains installed on a persistence failure so a later prepared
// retry can replay the deterministic lifecycle transitions.
func (p *PreparedStaticSourceSetGenerationRefresh) Commit(
	ctx context.Context,
	capability runtimestartupownership.ProcessCapability,
) (commitErr error) {
	if p == nil || p.runtime == nil || p.topology == nil {
		return errors.New("prepared runtime source-set generation refresh is incomplete")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return errors.New("prepared runtime source-set generation refresh is already settled")
	}
	p.done = true
	defer p.runtime.generationMu.Unlock()
	if capability == nil {
		p.topology.Abort()
		return errors.New("prepared runtime source-set generation refresh requires process capability")
	}

	successor := p.current
	var predecessor runtimestartupownership.GenerationGrant
	if p.currentEvidence.SourceSetRevision != p.plan.Revision {
		var err error
		successor, err = capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
			BundleHash: p.currentEvidence.BundleHash, BundleSource: p.currentEvidence.BundleSource,
			RuntimeInstanceID: p.currentEvidence.RuntimeInstanceID,
			RuntimeGeneration: p.currentEvidence.RuntimeGeneration + 1, SourceSetRevision: p.plan.Revision,
		})
		if err != nil {
			p.topology.Abort()
			return err
		}
		if _, err = successor.MarkProbesSettled(ctx, p.currentEvidence.ProbeSurfaceIDs); err != nil {
			p.topology.Abort()
			return errors.Join(err, successor.Retire(context.Background()))
		}
		if _, err = successor.AdmitExecution(ctx); err != nil {
			p.topology.Abort()
			return errors.Join(err, successor.Retire(context.Background()))
		}
		predecessor = p.current
		p.runtime.lifecycleMu.Lock()
		if p.runtime.startupGrant != p.current || p.runtime.cancelStart == nil {
			p.runtime.lifecycleMu.Unlock()
			p.topology.Abort()
			return errors.Join(errors.New("runtime generation changed during source-set refresh"), successor.Retire(context.Background()))
		}
		p.runtime.startupGrant = successor
		p.runtime.lifecycleMu.Unlock()
	}

	evidence, err := successor.Evidence()
	if err != nil {
		p.topology.Abort()
		return err
	}
	if evidence.SourceSetRevision != p.plan.Revision || evidence.State != runtimestartupownership.GrantAdmitted {
		p.topology.Abort()
		return errors.New("runtime source-set generation refresh successor is not admitted for the requested plan")
	}
	commitErr = p.topology.Commit(ctx, successor, evidence.GrantID)
	if predecessor != nil {
		commitErr = errors.Join(commitErr, predecessor.Retire(context.Background()))
	}
	return commitErr
}

const bootstrapSelfCheckSubscriberID = "bootstrap-self-check"

func runtimeThrottleSuppressPrefixes(source semanticview.Source) []string {
	if source == nil {
		return nil
	}
	value, ok := semanticview.PolicyValueForFlow(source, "", "throttle_suppress_prefixes")
	if !ok {
		return nil
	}
	switch typed := value.Value.(type) {
	case []string:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if item = strings.TrimSpace(item); item != "" {
				out = append(out, item)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := strings.TrimSpace(asString(item)); text != "" {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

func ensureWorkflowBootWiring(opts RuntimeOptions, profile llmselection.Profile, posture executionposture.Posture) (*providerconnectors.MockResponsePlan, runtimebootverify.SourceBootEffectReachability, error) {
	return ensureWorkflowBootWiringWithHarnessPolicy(opts, profile, posture, false)
}

func ensureWorkflowBootWiringWithHarnessPolicy(opts RuntimeOptions, profile llmselection.Profile, posture executionposture.Posture, allowValidationHarness bool) (*providerconnectors.MockResponsePlan, runtimebootverify.SourceBootEffectReachability, error) {
	if opts.WorkflowModule == nil {
		return nil, runtimebootverify.SourceBootEffectReachability{}, fmt.Errorf("workflow module is required: configure RuntimeOptions.WorkflowModule")
	}
	source := opts.WorkflowModule.SemanticSource()
	if opts.WorkspaceLifecycle != nil {
		if err := opts.WorkspaceLifecycle.ValidateSource(context.Background(), source); err != nil {
			return nil, runtimebootverify.SourceBootEffectReachability{}, fmt.Errorf("workspace validation failed: %w", err)
		}
	}
	validationOpts := DefaultWorkflowContractValidationOptions(opts.Credentials, posture)
	validationOpts.ManagedCredentials = opts.ManagedCredentials
	validationOpts.ProviderCredentials = opts.ProviderCredentials
	validationOpts.ProviderTriggerCatalog = opts.ProviderTriggerCatalog
	validationOpts.LLMProfile = profile
	validationOpts.ChannelPlans = opts.ChannelPlans
	validationOpts.ChannelOutboundBindings = opts.ChannelOutboundBindings
	validationOpts.AllowHarnessInputs = allowValidationHarness
	validationOpts.AllowHarnessOutputs = allowValidationHarness
	result, err := ValidateWorkflowContractSurface(context.Background(), source, validationOpts)
	if err != nil {
		return nil, runtimebootverify.SourceBootEffectReachability{}, err
	}
	return result.mockConnectorResponses, result.bootEffectReachability, nil
}

type connectorPackWorkflowModule struct {
	runtimepipeline.WorkflowModule
	source semanticview.Source
}

func (m connectorPackWorkflowModule) SemanticSource() semanticview.Source {
	return m.source
}

func compiledChannelActivityTools(bindings []packs.OutboundBindingPlan) (map[string]runtimepipeline.ChannelActivityTarget, error) {
	out := map[string]runtimepipeline.ChannelActivityTarget{}
	for _, binding := range bindings {
		for _, operation := range binding.OperationNames() {
			identity, err := binding.RuntimeActivityTarget(operation)
			if err != nil {
				return nil, fmt.Errorf("channel binding %q operation %q private target: %w", binding.BindingID(), operation, err)
			}
			if _, exists := out[identity.ToolID()]; exists {
				return nil, fmt.Errorf("duplicate private channel activity tool %q", identity.ToolID())
			}
			tool, err := binding.OperationTool(operation)
			if err != nil {
				return nil, fmt.Errorf("channel binding %q operation %q: %w", binding.BindingID(), operation, err)
			}
			target, err := runtimepipeline.NewChannelActivityTargetWithCredentials(tool, identity.Generation(), identity.CredentialStoreKeys())
			if err != nil {
				return nil, fmt.Errorf("channel binding %q operation %q private target: %w", binding.BindingID(), operation, err)
			}
			out[identity.ToolID()] = target
		}
	}
	return out, nil
}

func bootWarningsFatal() bool {
	return runtimeEnvBool("SWARM_BOOT_WARNINGS_FATAL", true)
}

func providerCredentialResolverForRuntimeOptions(opts RuntimeOptions) llm.ProviderCredentialResolver {
	return llm.NewProviderCredentialResolver(opts.ProviderCredentials)
}

// Validate checks the NewRuntime boot dependency graph without constructing a runtime.
func (deps RuntimeDeps) Validate() error {
	_, err := deps.validated()
	return err
}

func (deps RuntimeDeps) validated() (validatedRuntimeDeps, error) {
	return deps.validatedWithHarnessPolicy(false)
}

func (deps RuntimeDeps) validatedWithHarnessPolicy(allowValidationHarness bool) (validatedRuntimeDeps, error) {
	cfg := deps.Config
	opts := deps.Options
	if cfg == nil {
		return validatedRuntimeDeps{}, fmt.Errorf("runtime config is required")
	}
	if err := opts.BundleSourceFact.Validate(); err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("runtime bundle source fact: %w", err)
	}
	if opts.WorkflowModule == nil {
		return validatedRuntimeDeps{}, fmt.Errorf("workflow contract validation failed: workflow module is required")
	}
	if err := cfg.ValidateExtensions(); err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("runtime config validation failed: %w", err)
	}
	profile, err := cfg.LLMBackendProfile()
	if err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("runtime config validation failed: %w", err)
	}
	posture, err := cfg.ProcessExecutionPosture()
	if err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("runtime config validation failed: %w", err)
	}
	if deps.WorkflowPersistence.Configured() && !deps.WorkflowPersistence.Valid() {
		return validatedRuntimeDeps{}, fmt.Errorf("selected runtime workflow persistence mutation owner is required")
	}
	if deps.WorkflowPersistence.Valid() {
		requiredWorkflowRoles := []struct {
			name  string
			value any
		}{
			{"delivery lifecycle", deps.DeliveryStore},
			{"pipeline obligation", deps.PipelineObligations},
			{"decision cards", deps.DecisionCards},
			{"proposed effects", deps.ProposedEffects},
			{"human tasks", deps.DecisionCardHumanTasks},
			{"decision-card draft expiry", deps.DecisionCardDraftExpiry},
			{"human-task expiry", deps.HumanTaskExpiry},
			{"run lifecycle", deps.EventBusDurable.RunLifecycle},
		}
		for _, role := range requiredWorkflowRoles {
			if role.value == nil {
				return validatedRuntimeDeps{}, fmt.Errorf("selected runtime workflow %s owner is required", role.name)
			}
		}
	}
	if deps.InboundStore != nil && opts.ProviderTriggerCatalog == nil {
		return validatedRuntimeDeps{}, fmt.Errorf("provider trigger catalog snapshot is required when inbound store is configured")
	}
	projection, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{
		WorkflowModule: opts.WorkflowModule, BundleSourceFact: opts.BundleSourceFact,
		ProviderTriggerCatalog: opts.ProviderTriggerCatalog, ChannelPlans: opts.ChannelPlans,
		ChannelOutboundBindings: opts.ChannelOutboundBindings,
	})
	if err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("admit effective source projection: %w", err)
	}
	opts.WorkflowModule = projection.WorkflowModule()
	source := projection.Source()
	scenarioProfileCatalog, err := scenarioderivation.CompileCatalog(source, projection.Identity(), opts.ScenarioDeclarations...)
	if err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("compile scenario execution profile catalog: %w", err)
	}
	mockConnectorResponses, bootEffectReachability, err := ensureWorkflowBootWiringWithHarnessPolicy(opts, profile, posture, allowValidationHarness)
	if err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("workflow contract validation failed: %w", err)
	}
	if err := validateSelectedBackendModelAliasesForDeclaredAgents(cfg, source); err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("llm model alias validation failed: %w", err)
	}
	providerCredentialResolver := providerCredentialResolverForRuntimeOptions(opts)
	if opts.LLMRuntime == nil {
		if err := validateSelectedBackendCredentialForDeclaredAgents(context.Background(), cfg, opts, source); err != nil {
			return validatedRuntimeDeps{}, fmt.Errorf("llm backend credential validation failed: %w", err)
		}
	}
	if err := validateClaudeStartupConfig(context.Background(), cfg, opts, source); err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("claude runtime startup validation failed: %w", err)
	}
	authorityProvider := runtimeauthority.NewSourceProvider(source)
	emitRegistry := runtimetools.NewEmitRegistry(source, authorityProvider)
	credentials := opts.Credentials
	if credentials == nil {
		credentials = runtimecredentials.NewEnvStore()
	}
	deps.Config = cfg
	deps.Options = opts
	return validatedRuntimeDeps{
		Dependencies:               deps,
		Config:                     cfg,
		Options:                    opts,
		Source:                     source,
		Credentials:                credentials,
		ManagedCredentials:         opts.ManagedCredentials,
		MockConnectorResponses:     mockConnectorResponses,
		BootEffectReachability:     bootEffectReachability,
		ExecutionPosture:           posture,
		ProviderCredentialResolver: providerCredentialResolver,
		Authority:                  authorityProvider,
		EmitRegistry:               emitRegistry,
		BundleSourceFact:           opts.BundleSourceFact,
		EffectiveSourceIdentity:    projection.Identity(),
		ScenarioProfileCatalog:     scenarioProfileCatalog,
	}, nil
}

func (deps validatedRuntimeDeps) payloadValidator(logger *RuntimeLogger) runtimebus.PayloadValidator {
	return newRuntimePayloadValidator(logger, deps.EmitRegistry.EventSchemaSnapshot())
}

func bindRuntimeStorePayloadValidator(eventBinder, inboundBinder EventPayloadValidationBinder, payloadValidator runtimebus.PayloadValidator) {
	if eventBinder != nil {
		eventBinder.SetEventPayloadValidator(payloadValidator)
	}
	if inboundBinder != nil {
		inboundBinder.SetEventPayloadValidator(payloadValidator)
	}
}

// AuthorActivityEventDescriptors projects one semantic source into the exact
// descriptors consumed by every runtime that can persist authored events.
func AuthorActivityEventDescriptors(source semanticview.Source) ([]runtimeauthoractivity.EventDescriptor, error) {
	if source == nil {
		return nil, nil
	}
	resolved := source.ResolvedEventCatalog()
	authored := source.AuthoredResolvedEventCatalog()
	byName := make(map[string]runtimeauthoractivity.EventDescriptor, len(resolved)+len(authored))
	add := func(name string, entry runtimecontracts.EventCatalogEntry, disposition runtimeauthoractivity.StoryDisposition) error {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil
		}
		summaryField := strings.TrimSpace(entry.AuthorSummaryField)
		if summaryField != "" {
			field, ok := entry.Payload.Properties[summaryField]
			if !ok {
				return fmt.Errorf("authored event %q author_summary_field %q is not declared in payload", name, summaryField)
			}
			fieldType := strings.TrimSpace(field.Type)
			if fieldType != "text" && fieldType != "string" {
				return fmt.Errorf("authored event %q author_summary_field %q must be text", name, summaryField)
			}
		}
		descriptor := runtimeauthoractivity.EventDescriptor{EventType: name, Disposition: disposition, AuthorSummaryField: summaryField}
		if previous, ok := byName[name]; ok && previous != descriptor {
			return fmt.Errorf("author activity event descriptor %q resolves to conflicting declarations", name)
		}
		byName[name] = descriptor
		return nil
	}
	for name, entry := range resolved {
		disposition := runtimeauthoractivity.StoryDifferent
		if _, ok := authored[name]; ok {
			disposition = runtimeauthoractivity.StoryAuthored
		}
		if err := add(name, entry, disposition); err != nil {
			return nil, err
		}
	}
	census := semanticview.BuildAuthoredEventEndpointCensus(source)
	endpoints := append(census.Producers(), census.Consumers()...)
	endpoints = append(endpoints, census.InputPins()...)
	endpoints = append(endpoints, census.OutputPins()...)
	for _, endpoint := range endpoints {
		proof := endpoint.Event
		if !proof.HasSchema {
			continue
		}
		disposition := runtimeauthoractivity.StoryDifferent
		if proof.IsAuthored(source) {
			disposition = runtimeauthoractivity.StoryAuthored
		}
		if err := add(proof.EventKey(), proof.Entry, disposition); err != nil {
			return nil, err
		}
	}
	for _, timer := range source.WorkflowTimers() {
		if !timer.StageOwned || strings.TrimSpace(timer.Event) != runtimecontracts.WorkflowStageTimerInternalEvent {
			continue
		}
		if err := add(runtimecontracts.WorkflowStageTimerInternalEvent, runtimecontracts.EventCatalogEntry{}, runtimeauthoractivity.StoryDifferent); err != nil {
			return nil, err
		}
		break
	}
	for toolID, entry := range source.ToolEntries() {
		if entry.Category() != runtimecontracts.ToolCategoryChannelOperation {
			continue
		}
		toolID = strings.TrimSpace(toolID)
		if toolID == "" {
			return nil, fmt.Errorf("channel operation tool requires an exact id")
		}
		for _, suffix := range []string{".succeeded", ".failed"} {
			if err := add(toolID+suffix, runtimecontracts.EventCatalogEntry{}, runtimeauthoractivity.StoryDifferent); err != nil {
				return nil, err
			}
		}
	}
	descriptors := make([]runtimeauthoractivity.EventDescriptor, 0, len(byName))
	for _, descriptor := range byName {
		descriptors = append(descriptors, descriptor)
	}
	sort.Slice(descriptors, func(i, j int) bool { return descriptors[i].EventType < descriptors[j].EventType })
	return descriptors, nil
}

func (rt *Runtime) PrepareAuthorActivityCatalog() error {
	if rt == nil {
		return fmt.Errorf("runtime is required")
	}
	rt.lifecycleMu.Lock()
	defer rt.lifecycleMu.Unlock()
	if len(rt.authorActivityLeases) > 0 {
		return nil
	}
	registrars := rt.authorActivityRegistrars
	if len(registrars) == 0 {
		return nil
	}
	if rt.authorActivityScope.Kind != runtimeauthoractivity.ScopeBundle || strings.TrimSpace(rt.authorActivityScope.RuntimeInstanceID) == "" || strings.TrimSpace(rt.authorActivityScope.BundleHash) == "" {
		return fmt.Errorf("runtime author activity catalog requires runtime_instance_id and bundle_hash")
	}
	leases := make([]*runtimeauthoractivity.EventCatalogLease, 0, len(registrars))
	for _, registrar := range registrars {
		lease, err := registrar.RegisterAuthorActivityEventCatalog(rt.authorActivityScope, rt.authorActivityDescriptors)
		if err != nil {
			for _, acquired := range leases {
				acquired.Release()
			}
			return err
		}
		leases = append(leases, lease)
	}
	rt.authorActivityLeases = leases
	return nil
}

func (rt *Runtime) releaseAuthorActivityCatalog() {
	if rt == nil {
		return
	}
	rt.lifecycleMu.Lock()
	leases := rt.authorActivityLeases
	rt.authorActivityLeases = nil
	rt.lifecycleMu.Unlock()
	for _, lease := range leases {
		lease.Release()
	}
}

func (rt *Runtime) authorActivityContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = worklifetime.WithProcess(ctx, rt.Options.ProcessWorkOwner)
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, rt.Options.RuntimeInstanceID)
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, rt.Options.BundleSourceFact)
	return runtimeauthoractivity.WithScope(ctx, rt.authorActivityScope)
}

func NewRuntime(ctx context.Context, deps RuntimeDeps) (*Runtime, error) {
	return newRuntime(ctx, deps, false)
}

// NewValidationHarnessRuntime is the explicit non-production catalog execution
// surface for contracts that declare source/sink harness pins. It changes only
// admission; harness pins still create no runtime route or delivery authority.
func NewValidationHarnessRuntime(ctx context.Context, deps RuntimeDeps) (*Runtime, error) {
	return newRuntime(ctx, deps, true)
}

func newRuntime(ctx context.Context, deps RuntimeDeps, allowValidationHarness bool) (*Runtime, error) {
	boot, err := deps.validatedWithHarnessPolicy(allowValidationHarness)
	if err != nil {
		return nil, err
	}
	cfg := boot.Config
	runtimeDeps := boot.Dependencies
	opts := boot.Options
	source := boot.Source
	if opts.ProcessWorkOwner == nil {
		return nil, fmt.Errorf("runtime process work owner is required")
	}
	ctx = worklifetime.WithProcess(ctx, opts.ProcessWorkOwner)
	workOccurrence, err := opts.ProcessWorkOwner.NewRuntime(ctx, worklifetime.RuntimeIdentity{
		RuntimeInstanceID: opts.RuntimeInstanceID,
		BundleHash:        boot.BundleSourceFact.BundleHash(),
	})
	if err != nil {
		return nil, fmt.Errorf("create runtime work occurrence: %w", err)
	}
	var managerRef *runtimemanager.AgentManager
	workOccurrenceOwned := true
	defer func() {
		if workOccurrenceOwned {
			if managerRef != nil {
				_ = managerRef.ShutdownWithOptions(runtimemanager.DefaultShutdownOptions())
			}
			_, _ = workOccurrence.RetireAndWait(context.Background())
		}
	}()
	if runtimeDeps.InboundStore != nil {
		if err := runtimeDeps.InboundStore.ValidateInboundPublicationIntegrity(ctx); err != nil {
			return nil, fmt.Errorf("validate inbound publication integrity at startup: %w", err)
		}
	}
	mcpTurns := runtimemcp.NewTurnContextRegistry(runtimeactors.ActorFromContext)
	descriptors, err := AuthorActivityEventDescriptors(source)
	if err != nil {
		return nil, err
	}
	rt := &Runtime{
		Config:                    cfg,
		ExecutionPosture:          boot.ExecutionPosture,
		EffectiveSourceIdentity:   boot.EffectiveSourceIdentity,
		ScenarioProfileCatalog:    boot.ScenarioProfileCatalog,
		Options:                   opts,
		Workspace:                 opts.WorkspaceLifecycle,
		MCPTurns:                  mcpTurns,
		Authority:                 boot.Authority,
		EmitRegistry:              boot.EmitRegistry,
		authorActivityDescriptors: descriptors,
		authorActivityScope:       runtimeauthoractivity.BundleScope(opts.RuntimeInstanceID, boot.BundleSourceFact.BundleHash()),
		authorActivityRegistrars:  append([]AuthorActivityCatalogRegistrar(nil), runtimeDeps.AuthorActivityRegistrars...),
		eventPayloadBinder:        runtimeDeps.EventPayloadValidationBinder,
		inboundPayloadBinder:      runtimeDeps.InboundPayloadValidationBinder,
		runLifecycleCandidates:    runtimeDeps.RunLifecycleCandidates,
		deliveryStore:             runtimeDeps.DeliveryStore,
		timerObligationReader:     runtimeDeps.TimerObligationReader,
		mailboxStore:              runtimeDeps.MailboxStore,
		effectsStore:              runtimeDeps.EffectsStore,
		managedCapabilitiesStore:  runtimeDeps.ManagedCapabilitiesStore,
		Credentials:               boot.Credentials,
		ManagedCredentials:        boot.ManagedCredentials,
		workOccurrence:            workOccurrence,
	}
	if candidateOwner := runtimeDeps.RunLifecycleCandidates; candidateOwner != nil {
		scope := runtimerunlifecycle.CandidateScope{BundleHash: boot.BundleSourceFact.BundleHash()}
		executor, err := runtimerunlifecycle.NewExecutor(
			candidateOwner,
			scope,
			runLifecycleTerminalCatalog(source),
			workOccurrence,
			runtimerunlifecycle.ExecutorOptions{},
		)
		if err != nil {
			return nil, fmt.Errorf("build run lifecycle completion executor: %w", err)
		}
		rt.runLifecycleExecutor = executor
	} else if runtimeDeps.WorkflowPersistence.Configured() {
		return nil, fmt.Errorf("selected runtime store run lifecycle candidate owner is required")
	}

	if runtimeDeps.RuntimeLogStore != nil {
		rt.Logger = NewRuntimeLogger(runtimeDeps.RuntimeLogStore, rt.ExecutionPosture)
	}
	payloadValidator := boot.payloadValidator(rt.Logger)
	rt.payloadValidator = payloadValidator
	bus, err := newRuntimeEventBus(runtimeDeps.EventStore, runtimeDeps.EventBusDurable, runtimeDeps.PipelineObligations, rt.Logger, source, boot.ExecutionPosture, boot.BundleSourceFact, opts.RuntimeInstanceID, workOccurrence, func() []runtimebus.EventInterceptor {
		if rt.Pipeline == nil {
			return nil
		}
		return []runtimebus.EventInterceptor{rt.Pipeline}
	}, payloadValidator, func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
		if managerRef == nil {
			return fmt.Errorf("flow instance activator is required")
		}
		return managerRef.ActivateFlowInstance(ctx, req)
	}, runtimepipeline.FlowInstanceActivationPlannerFunc(func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) (runtimepipeline.FlowInstanceActivationPlan, error) {
		if managerRef == nil {
			return runtimepipeline.FlowInstanceActivationPlan{}, fmt.Errorf("flow instance activation planner is required")
		}
		return managerRef.PrepareFlowInstanceActivation(ctx, req)
	}), runtimepipeline.CommittedFlowInstanceActivationFinalizerFunc(func(ctx context.Context, committed runtimepipeline.CommittedFlowInstanceActivation) error {
		if managerRef == nil {
			return fmt.Errorf("flow instance activation finalizer is required")
		}
		return managerRef.FinalizeCommittedFlowInstanceActivation(ctx, committed)
	}), opts.ProviderTriggerCatalog, opts.TestLifecycleProbe)
	if err != nil {
		return nil, fmt.Errorf("build event bus: %w", err)
	}
	rt.Bus = bus
	rt.RuntimeIngress = runtimeingress.NewController(runtimeDeps.RuntimeIngressStore, rt.Bus, runtimeingress.Options{ExecutionPosture: rt.ExecutionPosture})
	rt.Bus.SetRuntimeIngressDispatchGate(rt.RuntimeIngress)
	rt.Scheduler = runtimepipeline.NewSchedulerWithWorkOwner(workOccurrence)
	if runtimeDeps.GenericScheduleStore != nil {
		genericSchedules, err := runtimegenericschedule.NewLifecycle(
			runtimeDeps.GenericScheduleStore,
			rt.Scheduler,
			rt.Bus,
			rt.Bus.EngineDispatcher(),
			genericScheduleRuntimeLogger{logger: rt.Logger},
			rt.ExecutionPosture,
		)
		if err != nil {
			return nil, fmt.Errorf("build generic schedule lifecycle: %w", err)
		}
		rt.GenericSchedules = genericSchedules
	}
	if runtimeDeps.WorkflowPersistence.Valid() {
		channelActivityTools, err := compiledChannelActivityTools(opts.ChannelOutboundBindings)
		if err != nil {
			return nil, fmt.Errorf("compile channel activity tools: %w", err)
		}
		artifactRoot, err := runtimepipeline.ResolveArtifactRepoRoot("")
		if err != nil {
			return nil, fmt.Errorf("artifact repo root validation failed: %w", err)
		}
		rt.Pipeline = runtimepipeline.NewPipelineCoordinatorWithOptions(rt.Bus, runtimepipeline.PipelineCoordinatorOptions{
			ExecutionPosture:      boot.ExecutionPosture,
			ReceiverExecution:     eventreceiver.NormalExecution(),
			Module:                opts.WorkflowModule,
			Persistence:           runtimeDeps.WorkflowPersistence,
			DeliveryStore:         runtimeDeps.DeliveryStore,
			DeadLetters:           runtimeDeps.EventBusDurable.TargetFailureRecorder,
			PipelineObligations:   runtimeDeps.PipelineObligations,
			RunBundleAvailability: runtimeDeps.RunBundleAvailability,
			InstanceActivator: func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
				if managerRef == nil {
					return fmt.Errorf("flow instance activator is required")
				}
				return managerRef.ActivateFlowInstance(ctx, req)
			},
			InstanceDeactivator: func(ctx context.Context, req runtimepipeline.FlowInstanceDeactivationRequest) error {
				if managerRef == nil {
					return fmt.Errorf("flow instance deactivator is required")
				}
				return managerRef.DeactivateFlowInstanceModel(ctx, req)
			},
			TimerScheduler:            rt.Scheduler,
			GenericSchedules:          rt.GenericSchedules,
			TimerObligationReader:     runtimeDeps.TimerObligationReader,
			MailboxMaterializer:       runtimeDeps.MailboxMaterializer,
			DecisionCards:             runtimeDeps.DecisionCards,
			ProposedEffects:           runtimeDeps.ProposedEffects,
			HumanTasks:                runtimeDeps.DecisionCardHumanTasks,
			DecisionCardDraftExpiry:   runtimeDeps.DecisionCardDraftExpiry,
			HumanTaskExpiry:           runtimeDeps.HumanTaskExpiry,
			DeliveryRuntime:           rt.Bus,
			FlowRoutes:                rt.Bus,
			RunLifecycle:              runtimeDeps.EventBusDurable.RunLifecycle,
			Credentials:               rt.Credentials,
			ManagedCredentials:        rt.ManagedCredentials,
			MockConnectorResponses:    boot.MockConnectorResponses,
			ScenarioExecutionProfiles: runtimeDeps.ScenarioExecutionProfiles,
			EffectiveSourceIdentity:   boot.EffectiveSourceIdentity,
			ChannelActivityTools:      channelActivityTools,
			ArtifactRoot:              artifactRoot,
			BundleSourceFact:          opts.BundleSourceFact,
			DecisionCardCadence: decisioncard.CadencePolicy{
				FirstReminderDelay: rt.Config.Runtime.DecisionCardFirstReminder,
				UrgencyDelay:       rt.Config.Runtime.DecisionCardUrgency,
				ReminderInterval:   rt.Config.Runtime.DecisionCardReminderInterval,
				InputDraftTTL:      rt.Config.Runtime.DecisionCardInputDraftTTL,
			},
			TestEntityStateHook:              opts.TestEntityStateHook,
			TestWorkflowNodeHandlerStartHook: opts.TestWorkflowNodeHandlerStartHook,
			TestLifecycleProbe:               opts.TestLifecycleProbe,
			WorkOwner:                        workOccurrence,
		})

		if rt.Pipeline == nil {
			return nil, fmt.Errorf("runtime workflow persistence construction is required")
		}
		if rt.Pipeline != nil {
			rt.SystemNodes = append(rt.SystemNodes, rt.Pipeline.BackgroundNodes()...)
		}
	}
	if runtimeDeps.RunControlStore != nil {
		var timerCancellations runtimeruncontrol.TimerCancellationReconciler = runtimetimercancellation.NewReconciler(rt.GenericSchedules, nil)
		if rt.Pipeline != nil {
			timerCancellations = rt.Pipeline.TimerCancellationReconciler()
		}
		rt.RunControl = runtimeruncontrol.NewController(runtimeDeps.RunControlStore, rt.Bus, runtimeruncontrol.Options{
			TimerCancellations: timerCancellations,
		})
		rt.Bus.SetRunDispatchGate(rt.RunControl)
	}

	if runtimeDeps.BudgetSpendStore != nil {
		rt.Budget = NewBudgetTracker(runtimeDeps.BudgetSpendStore, rt.Bus, cfg, runtimeDeps.MailboxStore, rt.Logger, source, rt.ExecutionPosture)
	}

	backendProfile, err := cfg.LLMBackendProfile()
	if err != nil {
		return nil, err
	}
	var completionController *runtimeeffects.Controller
	if runtimeDeps.EffectsStore != nil && runtimeDeps.CompletionStore != nil && runtimeDeps.CompletionHeartbeatStore != nil {
		completionController = runtimeeffects.NewCompletionController(runtimeDeps.EffectsStore, runtimeDeps.CompletionStore, runtimeDeps.CompletionHeartbeatStore, rt.Budget).WithExecutionPosture(boot.ExecutionPosture)
	}
	if opts.LLMRuntime == nil && completionController == nil {
		return nil, fmt.Errorf("selected runtime store does not implement completion execution authority")
	}
	rt.LLMRuntimes, err = llm.NewAgentRuntimeSet(backendProfile, llm.RuntimeFactory{
		Cfg:                  cfg,
		Sessions:             runtimeDeps.SessionRegistry,
		LiveSessions:         runtimeDeps.LiveSessionAcquirer,
		Conversations:        runtimeDeps.ConversationStore,
		Workspaces:           rt.Workspace,
		Events:               rt.Bus,
		MCPTurns:             rt.MCPTurns,
		ToolGateway:          opts.ToolGatewayBinding,
		Credentials:          boot.ProviderCredentialResolver.Store,
		CompletionController: completionController,
	}, opts.LLMRuntime)
	if err != nil {
		return nil, fmt.Errorf("build agent runtime resolver: %w", err)
	}
	if warnings, err := runtimetools.ValidateNativeToolBootConfig(ctx, source, rt.Credentials, rt.LLMRuntimes, rt.Workspace); err != nil {
		return nil, fmt.Errorf("native tool validation failed: %w", err)
	} else {
		if bootWarningsFatal() && len(warnings) > 0 {
			parts := make([]string, 0, len(warnings))
			for _, warning := range warnings {
				parts = append(parts, strings.TrimSpace(warning.Error()))
			}
			sort.Strings(parts)
			return nil, fmt.Errorf("native tool validation warnings are fatal: %s", strings.Join(parts, "; "))
		}
		for _, warning := range warnings {
			slog.Warn("native tool validation warning", "warning", warning.Error())
		}
	}

	rt.ToolExecutor = runtimetools.NewExecutorWithOptions(rt.Bus, runtimetools.ExecutorOptions{
		Config:             cfg,
		Credentials:        rt.Credentials,
		ManagedCredentials: rt.ManagedCredentials,
		MailboxStore:       runtimeDeps.MailboxStore,
		NoticePresentation: opts.NoticePresentation,
		EntityStore:        runtimeDeps.ToolEntityStore,
		HumanTaskStore:     runtimeDeps.HumanTaskStore,
		WorkflowInstances:  rt.Pipeline,
		WorkflowSource:     source,
		DataAccessStore:    runtimeDeps.DataAccessStore,
		ChannelBindings:    opts.ChannelOutboundBindings,
		ActivityExecutor:   rt.Pipeline,
		WorkspaceResolver:  rt.Workspace,
		ModelRuntimes:      rt.LLMRuntimes,
		AuthorityProvider:  rt.Authority,
		EmitRegistry:       rt.EmitRegistry,
		GenericSchedules:   rt.GenericSchedules,
		ManagerProvider: func() runtimetools.Manager {
			return managerRef
		},
	})
	credentialValidationOptions := runtimebootverify.Options{
		Credentials:        rt.Credentials,
		ManagedCredentials: rt.ManagedCredentials,
		EffectReachability: boot.BootEffectReachability,
	}
	if missing, err := runtimebootverify.MissingStaticCredentialRequirements(ctx, source, credentialValidationOptions); err != nil {
		return nil, fmt.Errorf("credential validation failed: %w", err)
	} else {
		if bootWarningsFatal() && len(missing) > 0 {
			parts := make([]string, 0, len(missing))
			liveAgentIDs := boot.BootEffectReachability.LiveAgentIDs()
			for _, item := range missing {
				requiredBy := make([]string, 0, len(item.RequiredBy))
				for _, ref := range item.RequiredBy {
					requiredBy = append(requiredBy, strings.TrimSpace(ref.Kind)+":"+strings.TrimSpace(ref.Name))
				}
				sort.Strings(requiredBy)
				message := fmt.Sprintf("%s required by %s", strings.TrimSpace(item.Key), strings.Join(requiredBy, ", "))
				if len(liveAgentIDs) > 0 {
					message += " (reachable from live agents " + strings.Join(liveAgentIDs, ", ") + ")"
				}
				parts = append(parts, message)
			}
			sort.Strings(parts)
			return nil, fmt.Errorf("missing required credentials: %s", strings.Join(parts, "; "))
		}
		for _, item := range missing {
			requiredBy := make([]string, 0, len(item.RequiredBy))
			for _, ref := range item.RequiredBy {
				requiredBy = append(requiredBy, strings.TrimSpace(ref.Kind)+":"+strings.TrimSpace(ref.Name))
			}
			slog.Warn("credential requirement warning", "key", item.Key, "required_by", strings.Join(requiredBy, ", "))
		}
	}
	if missing, err := runtimebootverify.MissingManagedCredentialRequirements(ctx, source, credentialValidationOptions); err != nil {
		return nil, fmt.Errorf("managed credential validation failed: %w", err)
	} else {
		if bootWarningsFatal() && len(missing) > 0 {
			parts := make([]string, 0, len(missing))
			for _, item := range missing {
				requiredBy := make([]string, 0, len(item.RequiredBy))
				for _, ref := range item.RequiredBy {
					requiredBy = append(requiredBy, strings.TrimSpace(ref.Kind)+":"+strings.TrimSpace(ref.Name))
				}
				sort.Strings(requiredBy)
				status := strings.TrimSpace(item.Status)
				if status == "" {
					status = runtimemanagedcredentials.StatusUnconnected
				}
				parts = append(parts, fmt.Sprintf("%s status=%s required by %s", strings.TrimSpace(item.Key), status, strings.Join(requiredBy, ", ")))
			}
			sort.Strings(parts)
			return nil, fmt.Errorf("unusable required managed credentials: %s", strings.Join(parts, "; "))
		}
		for _, item := range missing {
			requiredBy := make([]string, 0, len(item.RequiredBy))
			for _, ref := range item.RequiredBy {
				requiredBy = append(requiredBy, strings.TrimSpace(ref.Kind)+":"+strings.TrimSpace(ref.Name))
			}
			slog.Warn("managed credential requirement warning", "key", item.Key, "status", item.Status, "required_by", strings.Join(requiredBy, ", "))
		}
	}
	factory := runtimeagents.NewLLMAgentFactory(rt.LLMRuntimes, rt.ToolExecutor, runtimeagents.LLMAgentOptions{})
	managerOptions := runtimemanager.AgentManagerOptions{
		ExecutionPosture:   boot.ExecutionPosture,
		BaseContext:        rt.authorActivityContext(context.Background()),
		BundleSourceFact:   rt.Options.BundleSourceFact,
		DeliveryStore:      runtimeDeps.DeliveryStore,
		TestLifecycleProbe: opts.TestLifecycleProbe,
		Workspaces:         rt.Workspace,
		Sessions:           runtimeDeps.SessionRegistry,
		SessionResetter:    runtimeDeps.SessionResetter,
		PersistenceRoles: func() runtimemanager.PersistenceRoles {
			roles := runtimeDeps.ManagerPersistenceRoles
			roles.AgentRoutes = rt.Bus
			roles.FlowActivation = rt.Bus
			roles.RouteInstaller = rt.Bus
			roles.RouteVerifier = rt.Bus
			roles.RouteRestorer = rt.Bus
			roles.RouteRetirer = rt.Bus
			roles.RouteRemover = rt.Bus
			roles.CreationPublisher = rt.Bus
			roles.DeliveryRuntime = rt.Bus
			return roles
		}(),
		SemanticSource:         source,
		LLMBackend:             backendProfile.ID,
		ModelAliases:           cfg.LLM.Models,
		RequireModelResolution: true,
		Budget:                 rt.Budget,
		ResetRuntimeOwnedState: func() {
			if rt.MCPTurns != nil {
				rt.MCPTurns.Reset()
			}
		},
		RuntimeShutdownAdmissionClosed: rt.shutdownAdmissionClosed,
		WorkOwner:                      workOccurrence,
		ReceiverExecution:              eventreceiver.NormalExecution(),
		RuntimeIngressSafetyPause: func(ctx context.Context, reason string, failure *runtimefailures.Envelope) error {
			_, err := rt.RuntimeIngress.SafetyPause(ctx, runtimeingress.TransitionRequest{
				Reason:       reason,
				ControlledBy: "runtime",
				LastFailure:  failure,
			})
			return err
		},
		NativeToolAdmissionValidator: func(ctx context.Context, cfg runtimeactors.AgentConfig) error {
			return rt.ToolExecutor.ValidateNativeToolCapabilityAdmission(ctx, cfg)
		},
		ThrottleSuppressPrefixes: runtimeThrottleSuppressPrefixes(source),
		DisableSpinupControl:     true,
	}
	if rt.Pipeline != nil {
		managerOptions.PersistenceRoles.FlowTermination = rt.Pipeline
		managerOptions.WorkflowInstances = rt.Pipeline
	}
	rt.Manager = runtimemanager.NewAgentManagerWithOptions(rt.Bus, factory, managerOptions, runtimeDeps.ManagerStore)
	managerRef = rt.Manager
	if runtimeDeps.StartupGrant != nil {
		if err := rt.InstallStartupGrant(runtimeDeps.StartupGrant); err != nil {
			return nil, fmt.Errorf("install runtime generation grant: %w", err)
		}
	}

	if runtimeDeps.InboundStore != nil {
		rt.InboundGateway = NewInboundGateway(rt.Bus, rt.Logger, rt.shutdownAdmissionClosed, boot.ExecutionPosture, runtimeDeps.InboundStore)
		rt.InboundGateway.SetChannelPlans(opts.ChannelPlans)
		rt.InboundGateway.SetAdmissionGuard(rt.shutdownGate.BeginContext)
		rt.InboundGateway.SetRuntimeIngress(rt.RuntimeIngress)
		if err := rt.InboundGateway.SetCredentialStore(opts.ProviderCredentials); err != nil {
			return nil, fmt.Errorf("configure inbound gateway provider credentials: %w", err)
		}
	}
	if opts.EnableToolGateway {
		toolGatewayToken := opts.ToolGatewayBinding.AuthToken()
		if toolGatewayToken == "" {
			return nil, fmt.Errorf("tool gateway binding token is required")
		}
		rt.ToolGateway = runtimemcp.NewGateway(rt.ToolExecutor, toolGatewayToken, RuntimeMCPGatewayHooks(rt.Logger, rt.RuntimeIngress, func(agentID string) (runtimeactors.AgentConfig, bool) {
			if rt.Manager == nil {
				return runtimeactors.AgentConfig{}, false
			}
			cfg, err := rt.Manager.ResolveAgentConfig(strings.TrimSpace(agentID), "")
			return cfg, err == nil
		}, rt.shutdownAdmissionClosed, rt.MCPTurns))
	}

	workOccurrenceOwned = false
	return rt, nil
}

func (rt *Runtime) Start(ctx context.Context) error {
	if rt == nil {
		return fmt.Errorf("runtime is nil")
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, rt.Options.BundleSourceFact)
	if err := rt.PreflightDynamicTopologyStartup(ctx); err != nil {
		return err
	}
	if err := rt.PrepareAuthorActivityCatalog(); err != nil {
		return err
	}
	if err := rt.PrepareStartupLifecycle(ctx); err != nil {
		rt.releaseAuthorActivityCatalog()
		return err
	}
	ctx = rt.authorActivityContext(ctx)
	ctx = worklifetime.WithRuntimeOccurrence(ctx, rt.workOccurrence)
	bootStartedAt := rt.Options.BootStartedAt
	if bootStartedAt.IsZero() {
		bootStartedAt = time.Now().UTC()
	}
	if rt.shutdownAdmissionClosed() {
		return fmt.Errorf("runtime shutdown already started")
	}
	rt.lifecycleMu.Lock()
	if rt.cancelStart != nil {
		rt.lifecycleMu.Unlock()
		return fmt.Errorf("runtime already started")
	}
	startCtx, cancelStart := context.WithCancel(ctx)
	grant := rt.startupGrant
	if grant == nil {
		cancelStart()
		rt.lifecycleMu.Unlock()
		rt.releaseAuthorActivityCatalog()
		return fmt.Errorf("runtime generation grant is required before start")
	}
	grantEvidence, grantErr := grant.Evidence()
	if grantErr != nil {
		cancelStart()
		rt.lifecycleMu.Unlock()
		rt.releaseAuthorActivityCatalog()
		return grantErr
	}
	if err := rt.Manager.HydrateStaticTopologyForStartup(ctx); err != nil {
		cancelStart()
		rt.lifecycleMu.Unlock()
		rt.releaseAuthorActivityCatalog()
		return fmt.Errorf("hydrate static declaration topology: %w", err)
	}
	rt.emitBootProgress(5, "startup_ownership_lease", "ok", "grant="+grantEvidence.GrantID)
	rt.startCtx = startCtx
	rt.cancelStart = cancelStart
	rt.replacementQuiesced = false
	rt.lifecycleMu.Unlock()
	started := false
	defer func() {
		if started {
			return
		}
		rt.cleanupStartFailure()
	}()
	if rt.runLifecycleExecutor != nil {
		scope := runtimerunlifecycle.CandidateScope{BundleHash: rt.Options.BundleSourceFact.BundleHash()}
		registration, err := rt.runLifecycleCandidates.RegisterCompletionCandidateSink(
			startCtx,
			scope,
			rt.runLifecycleExecutor,
		)
		if err != nil {
			return fmt.Errorf("register run lifecycle completion executor: %w", err)
		}
		rt.runLifecycleRegistration = registration
		if err := rt.runLifecycleExecutor.Start(startCtx); err != nil {
			return fmt.Errorf("start run lifecycle completion executor: %w", err)
		}
	}
	bindRuntimeStorePayloadValidator(rt.eventPayloadBinder, rt.inboundPayloadBinder, rt.payloadValidator)
	if rt.RuntimeIngress != nil {
		if err := rt.RuntimeIngress.SyncState(ctx); err != nil {
			return fmt.Errorf("sync runtime ingress state: %w", err)
		}
	}

	if rt.Manager != nil {
		if err := rt.Manager.ReconcileDirectiveOperations(ctx); err != nil {
			return fmt.Errorf("required directive operation reconciliation failed: %w", err)
		}
	}

	skipPersistentStartupRecovery := rt.Options.DisablePersistentStartupRecovery
	startupRecoverySnapshot := startupRecoverySnapshot{
		RecoveryOnStartup:  rt != nil && rt.Config != nil && rt.Config.Runtime.RecoveryOnStartup && !skipPersistentStartupRecovery,
		InspectionComplete: true,
	}
	var err error
	if skipPersistentStartupRecovery {
		startupRecoverySnapshot.Delivery, err = rt.inspectDeliveryRecoveryInventory(ctx)
		if err != nil {
			startupRecoverySnapshot.InspectionComplete = false
			rt.emitBootProgress(6, "recovery_snapshot_inspection", "FAILED", err.Error())
		} else {
			rt.emitBootProgress(6, "recovery_snapshot_inspection", "ok", startupRecoverySnapshot.summary())
		}
	} else {
		startupRecoverySnapshot, err = rt.inspectStartupRecoverySnapshot(ctx, bootStartedAt)
		if err != nil {
			rt.emitBootProgress(6, "recovery_snapshot_inspection", "FAILED", err.Error())
		} else {
			rt.emitBootProgress(6, "recovery_snapshot_inspection", "ok", startupRecoverySnapshot.summary())
		}
	}
	startupRecoveryDecision := newStartupRecoveryDecisionReport(startupRecoverySnapshot)
	if err != nil && !startupRecoverySnapshot.RecoveryOnStartup {
		return err
	}
	if err != nil {
		startupRecoveryDecision.Outcome = startupRecoveryOutcomeDegraded
		startupRecoveryDecision.ReasonCode = startupRecoveryReasonInspectFailed
		startupRecoveryDecision.Failure = newStartupRecoveryFailure(runtimefailures.ClassDependencyUnavailable, "startup_recovery_inspection_failed", "inspect_recovery_state", nil, err)
		startupRecoveryDecision.InspectionFailure = runtimefailures.CloneEnvelope(startupRecoveryDecision.Failure)
	}
	if denyErr := startupRecoveryDecision.denialError(); denyErr != nil {
		startupRecoveryDecision.Failure = newStartupRecoveryFailure(runtimefailures.ClassSchemaInvalid, "startup_recovery_disabled_with_work", "admit_recovery", map[string]any{"work_classes": startupRecoveryDecision.Snapshot.StartupBlockingWorkClasses()}, denyErr)
		rt.logStartupRecoveryDecision(ctx, startupRecoveryDecision)
		rt.emitBootProgress(7, "recovery_decision", "FAILED", denyErr.Error())
		return denyErr
	}
	if skipPersistentStartupRecovery {
		rt.emitBootProgress(7, "recovery_decision", string(startupRecoveryDecision.Outcome), string(startupRecoveryDecision.ReasonCode))
	} else {
		rt.emitBootProgress(7, "recovery_decision", string(startupRecoveryDecision.Outcome), string(startupRecoveryDecision.ReasonCode))
	}

	if rt.Pipeline != nil {
		lease, beginErr := rt.workOccurrence.Begin(startCtx)
		if beginErr != nil {
			return fmt.Errorf("admit pipeline maintenance: %w", beginErr)
		}
		go func() {
			defer func() { _ = lease.Done() }()
			rt.Pipeline.RunMaintenance(lease.Context())
		}()
		rt.emitBootProgress(8, "pipeline_maintenance", "started", "")
	} else {
		rt.emitBootProgress(8, "pipeline_maintenance", "skipped", "pipeline unavailable")
	}
	systemNodeCount, err := rt.startSystemNodesAndWaitForSubscriptions(ctx, startCtx)
	if err != nil {
		rt.emitBootProgress(9, "system_nodes_start", "FAILED", err.Error())
		return err
	}
	rt.emitBootProgress(9, "system_nodes_start", "ok", fmt.Sprintf("%d nodes subscribed", systemNodeCount))
	workflowTimerRestoreReady := true
	if skipPersistentStartupRecovery {
		rt.emitBootProgress(10, "manager_recovery_if_enabled", "skipped", "persistent startup recovery disabled")
	} else if !rt.Config.Runtime.RecoveryOnStartup {
		rt.emitBootProgress(10, "manager_recovery_if_enabled", "skipped", "recovery_on_startup disabled")
	} else {
		recovered := make([]string, 0, 2)
		agentHydrationSucceeded := true
		if rt.Manager != nil {
			startupRecoveryDecision.ManagerRecoveryAttempted = true
			_, err := rt.Manager.HydrateForStartup(ctx)
			if err != nil {
				if runtimemanager.IsDynamicFlowRuntimeReadinessFinalizationError(err) {
					rt.emitBootProgress(10, "manager_recovery_if_enabled", "FAILED", err.Error())
					return fmt.Errorf("finalize dynamic flow runtime readiness during startup: %w", err)
				}
				agentHydrationSucceeded = false
				workflowTimerRestoreReady = false
				rt.recordStartupManagerRecoveryFailure(ctx, &startupRecoveryDecision, err)
				if !startupRecoverySnapshot.InspectionComplete || startupRecoverySnapshot.StartupBlockingWorkflowTimers > 0 {
					rt.emitBootProgress(10, "manager_recovery_if_enabled", "FAILED", err.Error())
					return fmt.Errorf("hydrate manager before workflow timer restoration: %w", err)
				}
			} else {
				recovered = append(recovered, "agent state")
			}
		}
		status := "ok"
		if startupRecoveryDecision.Outcome == startupRecoveryOutcomeDegraded {
			status = string(startupRecoveryDecision.Outcome)
		}
		detail := "no executable delivery consumers available"
		if !agentHydrationSucceeded && rt.Pipeline != nil {
			detail = "agent state hydration failed; workflow-node delivery recovery withheld"
		} else if !agentHydrationSucceeded {
			detail = "agent state hydration failed"
		} else if len(recovered) > 0 {
			detail = strings.Join(recovered, " and ") + " hydrated; delivery continuations await execution admission"
		}
		rt.emitBootProgress(10, "manager_recovery_if_enabled", status, detail)
	}
	if skipPersistentStartupRecovery {
		rt.emitBootProgress(11, "schedule_restoration", "skipped", "persistent startup recovery disabled")
	} else if rt.Scheduler != nil {
		restoredFamilies := make([]string, 0, 2)
		scheduleRestoreStatus := "ok"
		if rt.GenericSchedules != nil {
			startupRecoveryDecision.ScheduleRestoreAttempted = true
			reconciled, err := rt.GenericSchedules.Restore(ctx)
			if err != nil {
				rt.emitBootProgress(11, "schedule_restoration", "FAILED", err.Error())
				return fmt.Errorf("restore generic schedules: %w", err)
			}
			startupRecoveryDecision.ScheduleReplayCount = reconciled
			if err := ensureBootWorkflowSchedules(ctx, rt.GenericSchedules, rt.Pipeline, rt.ExecutionPosture); err != nil {
				rt.emitBootProgress(11, "schedule_restoration", "FAILED", err.Error())
				return fmt.Errorf("ensure boot generic schedules: %w", err)
			}
			restoredFamilies = append(restoredFamilies, "generic schedules reconciled")
		}
		if rt.Pipeline != nil {
			if !workflowTimerRestoreReady {
				scheduleRestoreStatus = string(startupRecoveryOutcomeDegraded)
				restoredFamilies = append(restoredFamilies, "workflow timers withheld until manager recovery succeeds")
			} else {
				startupRecoveryDecision.ScheduleRestoreAttempted = true
				if err := rt.Pipeline.RestoreWorkflowTimers(ctx); err != nil {
					rt.emitBootProgress(11, "schedule_restoration", "FAILED", err.Error())
					return fmt.Errorf("restore workflow timers: %w", err)
				}
				restoredFamilies = append(restoredFamilies, "workflow timers restored")
			}
		}
		if len(restoredFamilies) == 0 {
			rt.emitBootProgress(11, "schedule_restoration", "skipped", "no persistent timer owner available")
		} else {
			rt.emitBootProgress(11, "schedule_restoration", scheduleRestoreStatus, strings.Join(restoredFamilies, "; "))
		}
	} else {
		rt.emitBootProgress(11, "schedule_restoration", "skipped", "scheduler unavailable")
	}
	staticAgentIDs := []string{}
	if rt.Manager != nil {
		staticAgentIDs, err = staticBootAgentIDs(rt.Options.WorkflowModule.SemanticSource())
		if err != nil {
			rt.emitBootProgress(12, "static_agents_bootstrap", "FAILED", err.Error())
			return fmt.Errorf("bootstrap static agents: %w", err)
		}
		if err := rt.Manager.VerifyStaticAgents(rt.Options.WorkflowModule.SemanticSource()); err != nil {
			rt.emitBootProgress(12, "static_agents_bootstrap", "FAILED", err.Error())
			return fmt.Errorf("bootstrap static agents: %w", err)
		}
		rt.emitBootProgress(12, "static_agents_bootstrap", "ok", fmt.Sprintf("%d static agents", len(staticAgentIDs)))
	} else {
		rt.emitBootProgress(12, "static_agents_bootstrap", "skipped", "manager unavailable")
	}
	flowRequiredAgentIDs := []string{}
	if rt.Manager != nil {
		flowRequiredAgentIDs, err = staticFlowRequiredBootAgentIDs(rt.Options.WorkflowModule.SemanticSource())
		if err != nil {
			rt.emitBootProgress(13, "flow_required_agents", "FAILED", err.Error())
			return fmt.Errorf("bootstrap static flow required agents: %w", err)
		}
		if err := rt.Manager.VerifyStaticFlowRequiredAgents(rt.Options.WorkflowModule.SemanticSource()); err != nil {
			rt.emitBootProgress(13, "flow_required_agents", "FAILED", err.Error())
			return fmt.Errorf("bootstrap static flow required agents: %w", err)
		}
		rt.emitBootProgress(13, "flow_required_agents", "ok", fmt.Sprintf("%d flow-required agents", len(flowRequiredAgentIDs)))
	} else {
		rt.emitBootProgress(13, "flow_required_agents", "skipped", "manager unavailable")
	}
	source := rt.Options.WorkflowModule.SemanticSource()
	if rt.Options.LLMRuntime == nil {
		if err := validateSelectedBackendCredentialForActiveAgents(ctx, rt.Config, rt.Options, source, rt.Manager); err != nil {
			rt.emitBootProgress(14, "workspace_validation_and_system_containers", "FAILED", err.Error())
			return fmt.Errorf("llm backend credential validation failed: %w", err)
		}
	}
	if err := validateClaudeStartupConfigForActiveAgents(ctx, rt.Config, rt.Options, source, rt.Manager); err != nil {
		rt.emitBootProgress(14, "workspace_validation_and_system_containers", "FAILED", err.Error())
		return fmt.Errorf("claude runtime startup validation failed: %w", err)
	}
	if err := validateClaudeManagedAgentWorkspaces(ctx, rt.Config, source, rt.Workspace, rt.Manager); err != nil {
		rt.emitBootProgress(14, "workspace_validation_and_system_containers", "FAILED", err.Error())
		return fmt.Errorf("claude runtime workspace validation failed: %w", err)
	}
	rt.emitBootProgress(14, "workspace_validation_and_system_containers", "ok", fmt.Sprintf("%d system containers", len(rt.Options.SystemContainers)))
	startupAuthority, err := rt.currentStartupProbeAuthority()
	if err != nil {
		rt.emitBootProgress(15, "mcp_tool_validation", "FAILED", err.Error())
		return err
	}
	var preflightAuthority ManagedProviderPreflightAuthority
	hasManagedAgents, err := workflowSourceOrManagerDeclaresAgents(source, rt.Manager)
	if err != nil {
		return err
	}
	if claudeEnabled, backendErr := isClaudeCLIBackend(rt.Config); backendErr != nil {
		return backendErr
	} else if claudeEnabled && hasManagedAgents {
		preflightAuthority, err = rt.managedProviderPreflightAuthority(startupAuthority)
		if err != nil {
			rt.emitBootProgress(15, "mcp_tool_validation", "FAILED", err.Error())
			return err
		}
	}
	surfaceIDs, err := ValidateManagedProviderPreflight(ctx, rt.Config, source, rt.Options.ToolGatewayBinding, rt.LLMRuntimes, rt.MCPTurns, rt.ToolExecutor, rt.Manager, preflightAuthority)
	if err != nil {
		rt.emitBootProgress(15, "mcp_tool_validation", "FAILED", err.Error())
		return fmt.Errorf("claude runtime mcp validation failed: %w", err)
	}
	settledAuthority, err := rt.settleManagedStartupPreflight(ctx, surfaceIDs)
	if err != nil {
		rt.emitBootProgress(15, "mcp_tool_validation", "FAILED", err.Error())
		return fmt.Errorf("settle managed startup preflight: %w", err)
	}
	rt.emitBootProgress(15, "mcp_tool_validation", "ok", fmt.Sprintf("%d capability surfaces settled", len(surfaceIDs)))
	if rt.Manager != nil {
		if rt.Config.Runtime.RecoveryOnStartup && !skipPersistentStartupRecovery {
			if err := rt.Manager.ReconcileDynamicFlowRuntimeStartupProjection(ctx, rt.Options.BundleSourceFact); err != nil {
				if runtimemanager.IsDynamicFlowRuntimeReadinessFinalizationError(err) {
					return fmt.Errorf("finalize dynamic flow runtime readiness during startup: %w", err)
				}
				return fmt.Errorf("reconcile source-scoped dynamic topology before startup: %w", err)
			}
		}
		projection, err := rt.Manager.InspectDynamicFlowRuntimeStartupProjection(ctx, rt.Options.BundleSourceFact)
		if err != nil {
			return fmt.Errorf("revalidate source-scoped dynamic topology before startup: %w", err)
		}
		if len(projection.Pending) != 0 {
			return fmt.Errorf("dynamic topology startup remains incomplete for %d source-owned instance(s)", len(projection.Pending))
		}
		activation, activateErr := rt.admitManagedExecution(startCtx, settledAuthority, rt.Config.Runtime.RecoveryOnStartup && !skipPersistentStartupRecovery)
		if activateErr != nil {
			if activation.ReplayErr != nil {
				rt.recordStartupManagerRecoveryFailure(ctx, &startupRecoveryDecision, activation.ReplayErr)
				rt.logStartupRecoveryDecision(ctx, startupRecoveryDecision)
			}
			rt.emitBootProgress(16, "manager_event_loop_start", "FAILED", activateErr.Error())
			return fmt.Errorf("activate managed execution: %w", activateErr)
		}
		startCtx = managedexecution.WithAdmission(startCtx, activation.Admission)
		if rt.Config.Runtime.RecoveryOnStartup && !skipPersistentStartupRecovery {
			startupRecoveryDecision.ManagerReplayCount = activation.ReplaySummary.ReplayedCount
			startupRecoveryDecision.ManagerSkipCount = activation.ReplaySummary.SkippedCount
			startupRecoveryDecision.ManagerDropCount = activation.ReplaySummary.DroppedCount
			if activation.ReplayErr != nil {
				rt.recordStartupManagerRecoveryFailure(ctx, &startupRecoveryDecision, activation.ReplayErr)
			} else if startupRecoveryDecision.Outcome != startupRecoveryOutcomeDegraded && startupRecoveryDecision.ManagerDropCount > 0 {
				startupRecoveryDecision.Outcome = startupRecoveryOutcomeDegraded
				startupRecoveryDecision.ReasonCode = startupRecoveryReasonRecoverFailed
				startupRecoveryDecision.Failure = runtimefailures.CloneEnvelope(activation.ReplaySummary.FirstDroppedFailure)
				if startupRecoveryDecision.Failure == nil {
					startupRecoveryDecision.Failure = newStartupRecoveryFailure(runtimefailures.ClassInternalFailure, "startup_manager_replay_dropped_without_failure", "recover_manager", map[string]any{"dropped_count": startupRecoveryDecision.ManagerDropCount}, nil)
				}
			}
		}
		if err := rt.Manager.Run(startCtx); err != nil {
			rt.emitBootProgress(16, "manager_event_loop_start", "FAILED", err.Error())
			return fmt.Errorf("start managed execution loops: %w", err)
		}
		if err := rt.Manager.ReconstructDynamicFlowRuntimeStartupTopology(startCtx, rt.Options.BundleSourceFact); err != nil {
			rt.emitBootProgress(16, "manager_event_loop_start", "FAILED", err.Error())
			return fmt.Errorf("reconstruct source-scoped dynamic topology: %w", err)
		}
		if rt.deliveryContinuations != nil {
			if err := rt.deliveryContinuations.Synchronize(startCtx); err != nil {
				rt.emitBootProgress(16, "manager_event_loop_start", "FAILED", err.Error())
				return fmt.Errorf("converge startup delivery continuations after route activation: %w", err)
			}
		}
		rt.emitBootProgress(16, "manager_event_loop_start", "ok", "")
	} else {
		rt.emitBootProgress(16, "manager_event_loop_start", "skipped", "manager unavailable")
	}
	if rt.Bus != nil {
		if err := rt.startOutboxSweeper(startCtx); err != nil {
			rt.emitBootProgress(17, "outbox_sweeper", "FAILED", err.Error())
			return err
		}
		rt.emitBootProgress(17, "outbox_sweeper", "started", "")
	} else {
		rt.emitBootProgress(17, "outbox_sweeper", "skipped", "event bus unavailable")
	}
	rt.logStartupRecoveryDecision(ctx, startupRecoveryDecision)
	var bootCheck <-chan *worklifetime.EventDelivery
	var bootSubscription worklifetime.InternalSubscription
	if rt.Options.SelfCheck && rt.Bus != nil {
		bootSubscription, err = rt.Bus.SubscribeInternal(startCtx, bootstrapSelfCheckSubscriberID, events.EventType("platform.boot"))
		if err != nil {
			return fmt.Errorf("subscribe platform.boot self-check: %w", err)
		}
		bootSubscription.MarkReady()
		bootCheck = bootSubscription.Deliveries()
		defer func() { _ = bootSubscription.Complete(false) }()
		rt.emitBootProgress(18, "boot_self_check_optional", "ok", "platform.boot self-check subscribed")
	} else {
		rt.emitBootProgress(18, "boot_self_check_optional", "skipped", "self-check disabled or event bus unavailable")
	}
	bootEventID, err := rt.publishBootCompleted(context.Background(), bootCompletedReport{
		StartedAt:                 bootStartedAt,
		RecoveryDecision:          startupRecoveryDecision,
		StaticAgentsStarted:       staticAgentIDs,
		FlowRequiredAgentsStarted: flowRequiredAgentIDs,
		SystemContainersStarted:   rt.Options.SystemContainers,
		SelfCheckRequired:         rt.Options.SelfCheck,
	})
	if err != nil {
		rt.emitBootProgress(19, "platform_boot_event_published", "FAILED", err.Error())
		return fmt.Errorf("publish platform.boot: %w", err)
	}
	rt.emitBootProgress(19, "platform_boot_event_published", "ok", bootEventID)
	if rt.Options.SelfCheck {
		if err := rt.verifyBootPublished(bootCheck); err != nil {
			rt.emitBootProgress(18, "boot_self_check_optional", "FAILED", err.Error())
			return fmt.Errorf("self-check failed: %w", err)
		}
	}
	started = true
	return nil
}

func (rt *Runtime) startOutboxSweeper(ctx context.Context) error {
	if rt == nil || rt.Bus == nil {
		return nil
	}
	sweeperConfig := rt.Options.TestOutboxSweeperConfig
	if sweeperConfig == (runtimebus.OutboxSweeperConfig{}) {
		sweeperConfig = runtimebus.DefaultOutboxSweeperConfig()
	}
	if err := rt.Bus.StartOutboxSweeper(ctx, sweeperConfig); err != nil {
		return fmt.Errorf("start outbox sweeper: %w", err)
	}
	return nil
}

func (rt *Runtime) recordStartupManagerRecoveryFailure(ctx context.Context, decision *startupRecoveryDecisionReport, recoveryErr error) {
	if rt == nil || decision == nil || recoveryErr == nil {
		return
	}
	decision.Outcome = startupRecoveryOutcomeDegraded
	decision.ReasonCode = startupRecoveryReasonRecoverFailed
	decision.Failure = newStartupRecoveryFailure(runtimefailures.ClassDependencyUnavailable, "startup_manager_recovery_failed", "recover_manager", nil, recoveryErr)
	if rt.Logger != nil {
		handleRuntimeLogPersistenceError("runtime", "recovery_failed", rt.Logger.Error(ctx, "runtime", "recovery_failed", nil, recoveryErr))
	}
	decision.ManagerResetAttempted = true
	if rt.Manager != nil {
		if resetErr := rt.Manager.ResetRuntimeStateWithSource("startup_recovery_failed"); resetErr != nil {
			decision.ManagerResetFailure = newStartupRecoveryFailure(runtimefailures.ClassInternalFailure, "startup_manager_reset_failed", "reset_manager", nil, resetErr)
			if rt.Logger != nil {
				handleRuntimeLogPersistenceError("runtime", "recovery_reset_failed", rt.Logger.Error(ctx, "runtime", "recovery_reset_failed", nil, resetErr))
			}
		}
	}
	if rt.mailboxStore != nil {
		ctxPayload := mustJSON(map[string]any{
			"failure":     *decision.Failure,
			"instruction": "Runtime recovery failed. Reinitialize or repair persisted runtime state before restart.",
		})
		if _, mailboxErr := rt.mailboxStore.InsertMailboxItem(ctx, runtimetools.MailboxItem{
			FromAgent: "runtime", Type: "alert", Priority: "critical", Status: "pending", Context: ctxPayload,
			Summary: runtimeTruncateString("Runtime recovery failed: "+recoveryErr.Error(), 200),
		}); mailboxErr != nil && rt.Logger != nil {
			handleRuntimeLogPersistenceError("runtime", "recovery_mailbox_insert_failed", rt.Logger.Error(ctx, "runtime", "recovery_mailbox_insert_failed", nil, mailboxErr))
		}
	}
	payload := mustJSON(map[string]any{
		"failure": *decision.Failure, "failed_event_id": nil, "timestamp": time.Now().UTC().Format(time.RFC3339Nano),
	})
	if rt.Bus != nil {
		recoveryEvent, publishErr := newStandaloneRuntimePlatformDiagnosticEvent(events.EventType("platform.recovery_failed"), payload, events.EventEnvelope{}, time.Now(), rt.ExecutionPosture)
		if publishErr == nil {
			publishErr = rt.Bus.Publish(ctx, recoveryEvent)
		}
		if publishErr != nil && rt.Logger != nil {
			handleRuntimeLogPersistenceError("runtime", "recovery_failed_publish_failed", rt.Logger.Error(ctx, "runtime", "recovery_failed_publish_failed", nil, publishErr))
		}
	}
}

func (rt *Runtime) startSystemNodesAndWaitForSubscriptions(ctx context.Context, startCtx context.Context) (int, error) {
	if rt == nil {
		return 0, fmt.Errorf("runtime is nil")
	}
	nodes := make([]runtimepipeline.BackgroundNode, 0, len(rt.SystemNodes))
	readiness := make(chan string, len(rt.SystemNodes))
	for _, node := range rt.SystemNodes {
		if node == nil {
			continue
		}
		readyNode, ok := node.(runtimepipeline.SubscriptionReadyBackgroundNode)
		if !ok {
			return 0, fmt.Errorf("system node %s cannot report subscription readiness", runtimeBackgroundNodeName(node))
		}
		nodeName := runtimeBackgroundNodeName(node)
		var once sync.Once
		readyNode.AddSubscriptionReadyHook(func() {
			once.Do(func() {
				readiness <- nodeName
			})
		})
		nodes = append(nodes, node)
	}
	for _, node := range nodes {
		lease, err := rt.workOccurrence.Begin(startCtx)
		if err != nil {
			return len(nodes), fmt.Errorf("admit system node %s: %w", runtimeBackgroundNodeName(node), err)
		}
		go func(node runtimepipeline.BackgroundNode) {
			defer func() { _ = lease.Done() }()
			node.Run(lease.Context())
		}(node)
	}
	for subscribed := 0; subscribed < len(nodes); subscribed++ {
		select {
		case <-ctx.Done():
			return len(nodes), fmt.Errorf("wait for system node subscriptions: %w", ctx.Err())
		case <-startCtx.Done():
			return len(nodes), fmt.Errorf("wait for system node subscriptions: %w", startCtx.Err())
		case <-readiness:
		}
	}
	return len(nodes), nil
}

func runtimeBackgroundNodeName(node runtimepipeline.BackgroundNode) string {
	if node == nil {
		return "<nil>"
	}
	if named, ok := node.(fmt.Stringer); ok {
		if name := strings.TrimSpace(named.String()); name != "" {
			return name
		}
	}
	return fmt.Sprintf("%T", node)
}

func (rt *Runtime) Shutdown() error {
	return rt.ShutdownWithOptions(DefaultShutdownOptions())
}

func (rt *Runtime) ShutdownWithOptions(opts ShutdownOptions) error {
	return rt.stopWithOptions(opts)
}

func (rt *Runtime) QuiesceForReplacement(opts ShutdownOptions) error {
	return rt.stopWithOptions(opts)
}

func (rt *Runtime) stopWithOptions(opts ShutdownOptions) error {
	if rt == nil {
		return nil
	}
	rt.generationMu.Lock()
	defer rt.generationMu.Unlock()
	grace, err := runtimemanager.ResolveShutdownGrace(opts.Grace)
	if err != nil {
		return err
	}
	rt.shutdownGate.Close()
	if rt.workOccurrence != nil {
		_ = rt.workOccurrence.Fence()
	}
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), grace)
	defer cancelDrain()
	var shutdownErr error
	if rt.runLifecycleExecutor != nil {
		if err := rt.runLifecycleExecutor.Retire(drainCtx); err != nil {
			shutdownErr = errors.Join(
				shutdownErr,
				fmt.Errorf("run lifecycle executor retirement timed out after %s: %w", grace, err),
			)
		}
	}
	if err := rt.shutdownGate.Wait(drainCtx); err != nil {
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("runtime ingress admission drain timed out after %s: %w", grace, err))
	}
	if rt.Manager != nil {
		deadline, _ := drainCtx.Deadline()
		remaining := time.Until(deadline)
		if remaining <= 0 {
			remaining = time.Nanosecond
		}
		if err := rt.Manager.ShutdownWithOptions(runtimemanager.ShutdownOptions{Grace: remaining}); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("agent manager shutdown: %w", err))
		}
	}
	rt.lifecycleMu.Lock()
	cancelStart := rt.cancelStart
	grant := rt.startupGrant
	rt.cancelStart = nil
	rt.startCtx = nil
	rt.lifecycleMu.Unlock()
	if cancelStart != nil {
		cancelStart()
	}
	if err := rt.shutdownGate.Wait(context.Background()); err != nil {
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("runtime ingress admission join: %w", err))
	}
	if rt.Pipeline != nil {
		if err := rt.Pipeline.StopWorkflowTimerLifecycle(drainCtx); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("workflow timer lifecycle shutdown: %w", err))
		}
	}
	if rt.GenericSchedules != nil {
		if err := rt.GenericSchedules.Stop(drainCtx); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("generic schedule lifecycle shutdown: %w", err))
		}
	}
	if rt.Scheduler != nil {
		rt.Scheduler.Stop()
		if err := rt.Scheduler.Wait(drainCtx); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("scheduler shutdown: %w", err))
			_ = rt.Scheduler.Wait(context.Background())
		}
	}
	if rt.Bus != nil {
		if rt.deliveryContinuations != nil {
			if err := rt.deliveryContinuations.Retire(drainCtx); err != nil {
				shutdownErr = errors.Join(shutdownErr, fmt.Errorf("delivery continuation coordinator shutdown: %w", err))
			}
		}
		if rt.deliverySignalRegistration != nil {
			rt.deliverySignalRegistration.Release()
			rt.deliverySignalRegistration = nil
		}
		if err := rt.Bus.WaitForOutboxSweeper(drainCtx); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("outbox sweeper shutdown: %w", err))
			_ = rt.Bus.WaitForOutboxSweeper(context.Background())
		}
		// Producers are stopped. Retire every retained route and internal
		// subscriber queue before joining the runtime occurrence so buffered
		// delivery carriers cannot strand generation ownership.
		if err := rt.Bus.ResetInMemoryState(); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("event bus local delivery retirement: %w", err))
		}
	}
	if rt.workOccurrence != nil {
		rt.workOccurrence.Retire()
		if _, err := rt.workOccurrence.RetireAndWait(drainCtx); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("runtime work retirement: %w", err))
			diaglog.ProcessLog(diaglog.LevelError, "runtime", "runtime work retirement exceeded shutdown budget",
				"active_leases", rt.workOccurrence.ActiveCount(),
				"error", err.Error(),
			)
			_, _ = rt.workOccurrence.RetireAndWait(context.Background())
		}
	}
	if rt.runLifecycleRegistration != nil {
		rt.runLifecycleRegistration.Release()
		rt.runLifecycleRegistration = nil
	}
	if grant != nil {
		grantRetireCtx, cancelGrantRetire := context.WithTimeout(context.Background(), grace)
		if err := grant.Retire(grantRetireCtx); err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
		}
		cancelGrantRetire()
		rt.lifecycleMu.Lock()
		if rt.startupGrant == grant {
			rt.startupGrant = nil
		}
		rt.lifecycleMu.Unlock()
	}
	rt.lifecycleMu.Lock()
	rt.replacementQuiesced = true
	rt.lifecycleMu.Unlock()
	rt.releaseAuthorActivityCatalog()
	return shutdownErr
}

func (rt *Runtime) cleanupStartFailure() {
	_ = rt.stopWithOptions(DefaultShutdownOptions())
}

func (rt *Runtime) Wait(ctx context.Context) {
	<-ctx.Done()
}

type bootCompletedReport struct {
	StartedAt                 time.Time
	RecoveryDecision          startupRecoveryDecisionReport
	StaticAgentsStarted       []string
	FlowRequiredAgentsStarted []string
	SystemContainersStarted   []string
	SelfCheckRequired         bool
}

func staticBootAgentIDs(source semanticview.Source) ([]string, error) {
	records, err := runtimemanager.StaticAgentMaterializationRecords(source)
	if err != nil {
		return nil, err
	}
	return persistedBootAgentIDs(records), nil
}

func staticFlowRequiredBootAgentIDs(source semanticview.Source) ([]string, error) {
	records, err := runtimemanager.StaticFlowRequiredAgentMaterializationRecords(source)
	if err != nil {
		return nil, err
	}
	return persistedBootAgentIDs(records), nil
}

func persistedBootAgentIDs(records []runtimemanager.PersistedAgent) []string {
	out := make([]string, 0, len(records))
	for _, rec := range records {
		if id := strings.TrimSpace(rec.Config.ID); id != "" {
			out = append(out, id)
		}
	}
	return sortedNonEmptyStrings(out)
}

func sortedNonEmptyStrings(in []string) []string {
	out := make([]string, 0, len(in))
	for _, item := range in {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	sort.Strings(out)
	return out
}

func (rt *Runtime) publishBootCompleted(ctx context.Context, report bootCompletedReport) (string, error) {
	if rt == nil || rt.Bus == nil {
		return "", nil
	}
	t := events.EventType("platform.boot")
	var flowCount, nodeCount, agentCount, eventCount int
	if rt != nil && rt.Options.WorkflowModule != nil {
		if source := rt.Options.WorkflowModule.SemanticSource(); source != nil {
			flowCount = len(source.FlowSchemaEntries())
			nodeCount = len(source.ExecutableNodeRecords())
			agentCount = len(semanticview.AgentDeclarations(source))
			eventCount = len(source.ResolvedEventCatalog())
		}
	}
	completedAt := time.Now().UTC()
	startedAt := report.StartedAt
	if startedAt.IsZero() {
		startedAt = completedAt
	}
	durationMS := completedAt.Sub(startedAt).Milliseconds()
	if durationMS < 0 {
		durationMS = 0
	}
	payload := mustJSON(map[string]any{
		"flow_count":                   flowCount,
		"node_count":                   nodeCount,
		"agent_count":                  agentCount,
		"event_count":                  eventCount,
		"timestamp":                    completedAt.Format(time.RFC3339Nano),
		"boot_started_at":              startedAt.Format(time.RFC3339Nano),
		"boot_completed_at":            completedAt.Format(time.RFC3339Nano),
		"duration_ms":                  durationMS,
		"bundle_hash":                  rt.Options.BundleSourceFact.BundleHash(),
		"recovery_decision":            report.RecoveryDecision.bootPayload(),
		"static_agents_started":        sortedNonEmptyStrings(report.StaticAgentsStarted),
		"flow_required_agents_started": sortedNonEmptyStrings(report.FlowRequiredAgentsStarted),
		"system_containers_started":    sortedNonEmptyStrings(report.SystemContainersStarted),
		"self_check_required":          report.SelfCheckRequired,
		"self_check_passed":            nil,
	})
	eventID := uuid.NewString()
	evt, err := events.NewStandaloneRuntimeControlEvent(events.StandaloneRuntimeEventInput{Facts: events.EventFacts{
		ID: eventID, Type: t, Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"},
		Payload: payload, RoutingSource: events.NewPlatformControlRoutingSource(), CreatedAt: time.Now(), ExecutionMode: rt.ExecutionPosture.RootMode(),
	}})
	if err != nil {
		return "", err
	}
	return eventID, rt.Bus.Publish(ctx, evt)
}

func (rt *Runtime) verifyBootPublished(ch <-chan *worklifetime.EventDelivery) error {
	if rt == nil || !rt.Options.SelfCheck {
		return nil
	}
	if ch == nil {
		return fmt.Errorf("platform.boot subscription is not configured")
	}
	select {
	case delivery := <-ch:
		if delivery != nil {
			_ = delivery.Complete()
		}
	case <-time.After(1 * time.Second):
		return fmt.Errorf("eventbus publish/subscribe timeout")
	}
	return nil
}

var bootWorkflowTimerPolicyPlaceholder = regexp.MustCompile(`\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}`)

func ensureBootWorkflowSchedules(ctx context.Context, schedules *runtimegenericschedule.Lifecycle, workflow runtimepipeline.WorkflowRuntime, posture executionposture.Posture) error {
	if schedules == nil || workflow == nil {
		return nil
	}
	source := workflow.SemanticSource()
	if source == nil {
		return nil
	}
	for _, timer := range source.WorkflowTimers() {
		command, ok, err := bootWorkflowTimerSchedule(source, timer, posture)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if _, err := schedules.Admit(ctx, command); err != nil {
			return err
		}
	}
	return nil
}

func bootWorkflowTimerSchedule(source semanticview.Source, timer runtimecontracts.WorkflowTimerContract, posture executionposture.Posture) (runtimegenericschedule.AdmissionCommand, bool, error) {
	startTrigger, err := timeridentity.ParseStartTrigger(timer.StartOn)
	if err != nil || !startTrigger.IsBoot() {
		return runtimegenericschedule.AdmissionCommand{}, false, nil
	}
	cancelTrigger, err := timeridentity.ParseCancelTrigger(timer.CancelOn)
	if err != nil || cancelTrigger.Valid() {
		return runtimegenericschedule.AdmissionCommand{}, false, nil
	}
	owner := strings.TrimSpace(timer.Owner)
	eventType := strings.TrimSpace(timer.Event)
	if owner == "" || eventType == "" {
		return runtimegenericschedule.AdmissionCommand{}, false, nil
	}
	interval := bootWorkflowTimerDuration(source, timer)
	if interval <= 0 {
		return runtimegenericschedule.AdmissionCommand{}, false, nil
	}
	handle := timeridentity.WorkflowTimerHandle(timer.ID)
	payload, err := canonicaljson.FromGo(workflowTimerPayloadMap(timer))
	if err != nil {
		return runtimegenericschedule.AdmissionCommand{}, false, fmt.Errorf("admit boot timer payload: %w", err)
	}
	command := runtimegenericschedule.AdmissionCommand{
		ScheduleKey:   handle.TaskID(),
		OwnerID:       owner,
		OwnerKind:     runtimegenericschedule.OwnerSystem,
		EventType:     eventType,
		TaskID:        handle.TaskID(),
		Payload:       payload,
		RoutingSource: events.NewPlatformControlRoutingSource(),
		ExecutionMode: posture.RootMode(),
		Due:           runtimegenericschedule.DelayDue(interval),
	}
	if timer.Recurring {
		command.Due = runtimegenericschedule.EveryDue(interval)
	}
	return command, true, nil
}

func bootWorkflowTimerDuration(source semanticview.Source, timer runtimecontracts.WorkflowTimerContract) time.Duration {
	if delay := bootWorkflowTimerRenderedDelay(source, timer, timer.Delay); delay != "" && !strings.Contains(delay, "{") {
		if parsed, ok := timeridentity.ParseDelayDuration(delay); ok {
			return parsed
		}
	}
	return 0
}

func bootWorkflowTimerRenderedDelay(source semanticview.Source, timer runtimecontracts.WorkflowTimerContract, delay string) string {
	delay = strings.TrimSpace(delay)
	if delay == "" || !strings.Contains(delay, "{{") {
		return delay
	}
	flowID := strings.TrimSpace(timer.FlowID)
	return bootWorkflowTimerPolicyPlaceholder.ReplaceAllStringFunc(delay, func(token string) string {
		match := bootWorkflowTimerPolicyPlaceholder.FindStringSubmatch(token)
		if len(match) != 2 {
			return token
		}
		value, ok := semanticview.PolicyValueForFlow(source, flowID, match[1])
		if !ok || value.Value == nil {
			return token
		}
		return fmt.Sprint(value.Value)
	})
}

func workflowTimerPayloadMap(timer runtimecontracts.WorkflowTimerContract) map[string]any {
	handle := timeridentity.WorkflowTimerHandle(timer.ID)
	if !handle.Valid() {
		return map[string]any{}
	}
	return handle.PayloadMetadata()
}

func runtimeEnvBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if raw == "" {
		return fallback
	}
	switch raw {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func runtimeTruncateString(v string, max int) string {
	v = strings.TrimSpace(v)
	if max <= 0 {
		return ""
	}
	if len(v) <= max {
		return v
	}
	return v[:max]
}
