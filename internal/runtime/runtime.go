package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
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
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimeagents "github.com/division-sh/swarm/internal/runtime/agents"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
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
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/google/uuid"
)

type Stores struct {
	SQLDB               *sql.DB
	ConstructionBlocker string
	EventStore          runtimebus.EventStore
	RuntimeLogStore     RuntimeLogPersistence
	PipelineStore       *runtimepipeline.WorkflowInstanceStore
	SessionRegistry     sessions.Registry
	ConversationStore   llm.ConversationPersistence
	ManagerStore        runtimemanager.ManagerPersistence
	DeliveryStore       runtimedelivery.Store
	PipelineObligations runtimepipelineobligation.Store
	ScheduleStore       runtimepipeline.SchedulePersistence
	MailboxMaterializer runtimepipeline.MailboxWriteMaterializationStore
	DecisionCards       decisioncard.Store
	StartupOwnership    runtimestartupownership.Store
	MailboxStore        runtimetools.MailboxPersistence
	ToolEntityStore     runtimetools.EntityPersistence
	HumanTaskStore      runtimetools.HumanTaskCardStore
	BudgetSpendStore    budgetspend.Store
	InboundStore        InboundPersistence
	RuntimeIngressStore runtimeingress.Store
}

type RuntimeOptions struct {
	SelfCheck                        bool
	WorkspaceLifecycle               workspace.Lifecycle
	EnableToolGateway                bool
	ToolGatewayBinding               toolgateway.Binding
	BundleFingerprint                string
	BundleSourceFact                 runtimecorrelation.BundleSourceFact
	RuntimeInstanceID                string
	ProcessWorkOwner                 *worklifetime.Process
	WorkflowModule                   runtimepipeline.WorkflowModule
	LLMRuntime                       llm.Runtime
	Credentials                      runtimecredentials.Store
	ManagedCredentials               runtimemanagedcredentials.Store
	ProviderCredentials              runtimecredentials.Store
	ProviderTriggerCatalog           *providertriggers.CatalogSnapshot
	ChannelPlans                     []packs.SatisfactionPlan
	ChannelOutboundBindings          []packs.OutboundBindingPlan
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
	Config  *config.Config
	Stores  Stores
	Options RuntimeOptions
}

type validatedRuntimeDeps struct {
	Config                     *config.Config
	Stores                     Stores
	Options                    RuntimeOptions
	Source                     semanticview.Source
	PromptResolver             runtimecontracts.PromptResolver
	Credentials                runtimecredentials.Store
	ManagedCredentials         runtimemanagedcredentials.Store
	MockConnectorResponses     *providerconnectors.MockResponsePlan
	ExecutionMode              runtimeeffects.ExecutionMode
	ProviderCredentialResolver llm.ProviderCredentialResolver
	Authority                  runtimeauthority.Provider
	EmitRegistry               *runtimetools.EmitRegistry
	TrimmedBundleFingerprint   string
	BundleSourceFact           runtimecorrelation.BundleSourceFact
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
	"schedule_restoration",
	"manager_recovery_if_enabled",
	"outbox_sweeper",
	"static_agents_bootstrap",
	"flow_required_agents",
	"workspace_validation_and_system_containers",
	"mcp_tool_validation",
	"manager_event_loop_start",
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
	lifecycleMu               sync.Mutex
	startCtx                  context.Context
	cancelStart               context.CancelFunc
	ownershipLease            runtimestartupownership.Lease
	ownershipLeaseBorrowed    bool
	pendingOwnershipLease     runtimestartupownership.Lease
	pendingOwnershipOwned     bool
	ownershipHandoffPending   bool
	replacementQuiesced       bool
	workOccurrence            *worklifetime.RuntimeOccurrence
	ownerID                   string
	bootID                    string
	startupAdmission          managedexecution.Admission
	pendingOwnershipHandoff   runtimestartupownership.Handoff
	shutdownGate              shutdownAdmission
	payloadValidator          runtimebus.PayloadValidator
	authorActivityDescriptors []runtimeauthoractivity.EventDescriptor
	authorActivityScope       runtimeauthoractivity.Scope
	authorActivityLeases      []*runtimeauthoractivity.EventCatalogLease

	Config             *config.Config
	Stores             Stores
	Options            RuntimeOptions
	Bus                *runtimebus.EventBus
	Logger             *RuntimeLogger
	Pipeline           *runtimepipeline.PipelineCoordinator
	SystemNodes        []runtimepipeline.BackgroundNode
	Scheduler          *runtimepipeline.Scheduler
	Workspace          workspace.Lifecycle
	Budget             *BudgetTracker
	Credentials        runtimecredentials.Store
	ManagedCredentials runtimemanagedcredentials.Store
	LLM                llm.Runtime
	ToolExecutor       *runtimetools.Executor
	Manager            *runtimemanager.AgentManager
	RuntimeIngress     *runtimeingress.Controller
	RunControl         *runtimeruncontrol.Controller
	InboundGateway     *InboundGateway
	ToolGateway        *runtimemcp.Gateway
	MCPTurns           *runtimemcp.TurnContextRegistry
	Authority          runtimeauthority.Provider
	EmitRegistry       *runtimetools.EmitRegistry
	PromptResolver     runtimecontracts.PromptResolver
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

// PrepareInitialStartupOwnership acquires the selected-store lease before
// serve-level recovery and desired-state reconciliation mutate durable state.
// Start consumes the prepared lease instead of acquiring a second owner.
func (rt *Runtime) PrepareInitialStartupOwnership(ctx context.Context) error {
	if rt == nil || rt.Stores.StartupOwnership == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rt.lifecycleMu.Lock()
	defer rt.lifecycleMu.Unlock()
	if rt.cancelStart != nil || rt.ownershipLease != nil || rt.pendingOwnershipLease != nil {
		return fmt.Errorf("runtime already started or has pending startup ownership")
	}
	if strings.TrimSpace(rt.ownerID) == "" {
		rt.ownerID = newRuntimeOwnerID()
	}
	if _, err := uuid.Parse(strings.TrimSpace(rt.bootID)); err != nil {
		rt.bootID = uuid.NewString()
	}
	lease, err := rt.Stores.StartupOwnership.AcquireRuntimeStartupOwnership(ctx, runtimestartupownership.AcquireRequest{
		OwnerID:           rt.ownerID,
		BootID:            rt.bootID,
		BundleFingerprint: coalesceRuntimeIdentity(rt.Options.BundleFingerprint),
	})
	if err != nil {
		return err
	}
	rt.pendingOwnershipLease = lease
	rt.pendingOwnershipOwned = lease != nil
	return nil
}

// ReleasePreparedStartupOwnership releases an initial lease when serve-level
// work fails before Start consumes it. It never releases replacement handoffs.
func (rt *Runtime) ReleasePreparedStartupOwnership(ctx context.Context) error {
	if rt == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rt.lifecycleMu.Lock()
	if !rt.pendingOwnershipOwned || rt.pendingOwnershipLease == nil {
		rt.lifecycleMu.Unlock()
		return nil
	}
	lease := rt.pendingOwnershipLease
	rt.pendingOwnershipLease = nil
	rt.pendingOwnershipOwned = false
	rt.lifecycleMu.Unlock()
	return lease.Release(ctx)
}

type StartupOwnershipHandoff struct {
	predecessor *Runtime
	candidate   *Runtime
	lease       runtimestartupownership.Lease
	typed       runtimestartupownership.Handoff
	active      bool
	committed   bool
}

// PrepareStartupOwnershipHandoff authorizes a candidate only after the
// predecessor's shared-store consumers have quiesced under its retained lease.
func (rt *Runtime) PrepareStartupOwnershipHandoff(predecessor *Runtime) (*StartupOwnershipHandoff, error) {
	if rt == nil || predecessor == nil || rt == predecessor {
		return nil, nil
	}
	predecessor.lifecycleMu.Lock()
	defer predecessor.lifecycleMu.Unlock()
	rt.lifecycleMu.Lock()
	defer rt.lifecycleMu.Unlock()
	if predecessor.ownershipHandoffPending {
		return nil, fmt.Errorf("runtime startup ownership handoff is already pending")
	}
	if !predecessor.replacementQuiesced {
		return nil, fmt.Errorf("replacement predecessor must quiesce before startup ownership handoff")
	}
	if rt.cancelStart != nil || rt.ownershipLease != nil || rt.pendingOwnershipLease != nil {
		return nil, fmt.Errorf("replacement runtime already started or has pending ownership")
	}
	lease := predecessor.ownershipLease
	if lease == nil {
		if rt.Stores.StartupOwnership != nil {
			return nil, fmt.Errorf("replacement runtime requires predecessor startup ownership lease")
		}
		return nil, nil
	}
	if rt.Stores.StartupOwnership == nil {
		return nil, fmt.Errorf("replacement runtime cannot consume predecessor startup ownership lease")
	}
	predecessor.ownershipHandoffPending = true
	rt.pendingOwnershipLease = lease
	rt.pendingOwnershipOwned = false
	typed, err := lease.PrepareHandoff(context.Background(), runtimestartupownership.HandoffRequest{
		CandidateOwnerID: rt.ownerID, CandidateBootID: rt.bootID,
		CandidateBundleFingerprint: coalesceRuntimeIdentity(rt.Options.BundleFingerprint),
	})
	if err != nil {
		predecessor.ownershipHandoffPending = false
		rt.pendingOwnershipLease = nil
		return nil, err
	}
	rt.pendingOwnershipHandoff = typed
	return &StartupOwnershipHandoff{predecessor: predecessor, candidate: rt, lease: lease, typed: typed, active: true}, nil
}

func (h *StartupOwnershipHandoff) Commit() error {
	if h == nil || !h.active {
		return nil
	}
	if h.committed {
		return nil
	}
	h.predecessor.lifecycleMu.Lock()
	h.candidate.lifecycleMu.Lock()
	if h.predecessor.ownershipLease != h.lease || h.candidate.ownershipLease != h.lease || !h.candidate.ownershipLeaseBorrowed {
		h.candidate.lifecycleMu.Unlock()
		h.predecessor.lifecycleMu.Unlock()
		return fmt.Errorf("runtime startup ownership handoff state changed before commit")
	}
	authority, err := h.typed.Commit(context.Background())
	if err != nil {
		h.candidate.lifecycleMu.Unlock()
		h.predecessor.lifecycleMu.Unlock()
		return err
	}
	h.predecessor.ownershipLease = nil
	h.candidate.ownershipLeaseBorrowed = false
	h.candidate.pendingOwnershipLease = nil
	h.candidate.pendingOwnershipHandoff = nil
	h.candidate.pendingOwnershipOwned = false
	h.committed = true
	startCtx := h.candidate.startCtx
	h.candidate.lifecycleMu.Unlock()
	h.predecessor.lifecycleMu.Unlock()
	if _, err := h.candidate.admitManagedExecution(startCtx, authority, false); err != nil {
		return fmt.Errorf("activate replacement managed execution after ownership commit: %w", err)
	}
	if h.candidate.Manager != nil {
		if err := h.candidate.Manager.Run(h.candidate.startCtx); err != nil {
			return fmt.Errorf("start replacement managed execution after ownership commit: %w", err)
		}
	}
	return nil
}

func (h *StartupOwnershipHandoff) Finalize() error {
	if h == nil || !h.active || !h.committed {
		return nil
	}
	if _, err := h.typed.Finalize(context.Background()); err != nil {
		return err
	}
	h.predecessor.lifecycleMu.Lock()
	h.predecessor.ownershipHandoffPending = false
	h.predecessor.lifecycleMu.Unlock()
	h.active = false
	return nil
}

func (h *StartupOwnershipHandoff) Rollback() error {
	if h == nil || !h.active {
		return nil
	}
	h.predecessor.lifecycleMu.Lock()
	defer h.predecessor.lifecycleMu.Unlock()
	h.candidate.lifecycleMu.Lock()
	defer h.candidate.lifecycleMu.Unlock()
	if h.committed {
		if !h.candidate.replacementQuiesced {
			return fmt.Errorf("replacement candidate must quiesce before committed ownership rollback")
		}
		if h.candidate.ownershipLease == h.lease {
			h.candidate.ownershipLease = nil
			h.candidate.ownershipLeaseBorrowed = false
		}
		h.predecessor.ownershipLease = h.lease
		if _, err := h.typed.Rollback(context.Background()); err != nil {
			return err
		}
		h.predecessor.ownershipHandoffPending = false
		h.candidate.pendingOwnershipLease = nil
		h.candidate.pendingOwnershipOwned = false
		h.committed = false
		h.active = false
		return nil
	}
	if h.candidate.cancelStart == nil && h.candidate.ownershipLeaseBorrowed && h.candidate.ownershipLease == h.lease {
		h.candidate.ownershipLease = nil
		h.candidate.ownershipLeaseBorrowed = false
	}
	if h.candidate.pendingOwnershipLease == h.lease {
		h.candidate.pendingOwnershipLease = nil
		h.candidate.pendingOwnershipOwned = false
	}
	if h.candidate.pendingOwnershipHandoff == h.typed {
		h.candidate.pendingOwnershipHandoff = nil
	}
	if _, err := h.typed.Rollback(context.Background()); err != nil {
		return err
	}
	h.predecessor.ownershipHandoffPending = false
	h.active = false
	return nil
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

func ensureWorkflowBootWiring(opts RuntimeOptions, executionMode runtimeeffects.ExecutionMode) (*providerconnectors.MockResponsePlan, error) {
	if opts.WorkflowModule == nil {
		return nil, fmt.Errorf("workflow module is required: configure RuntimeOptions.WorkflowModule")
	}
	source := opts.WorkflowModule.SemanticSource()
	if opts.WorkspaceLifecycle != nil {
		if err := opts.WorkspaceLifecycle.ValidateSource(context.Background(), source); err != nil {
			return nil, fmt.Errorf("workspace validation failed: %w", err)
		}
	}
	validationOpts := DefaultWorkflowContractValidationOptions(opts.Credentials)
	validationOpts.ManagedCredentials = opts.ManagedCredentials
	validationOpts.ProviderTriggerCatalog = opts.ProviderTriggerCatalog
	validationOpts.ExecutionMode = executionMode
	validationOpts.ChannelPlans = opts.ChannelPlans
	validationOpts.ChannelOutboundBindings = opts.ChannelOutboundBindings
	result, err := ValidateWorkflowContractSurface(context.Background(), source, validationOpts)
	if err != nil {
		return nil, err
	}
	return result.mockConnectorResponses, nil
}

type connectorPackWorkflowModule struct {
	runtimepipeline.WorkflowModule
	source semanticview.Source
}

func (m connectorPackWorkflowModule) SemanticSource() semanticview.Source {
	return m.source
}

func workflowModuleWithProviderPacks(module runtimepipeline.WorkflowModule, triggerCatalog *providertriggers.CatalogSnapshot, channelBindings []packs.OutboundBindingPlan) (runtimepipeline.WorkflowModule, semanticview.Source, error) {
	if module == nil {
		return nil, nil, nil
	}
	source, err := providerconnectors.SourceWithConnectorPackImports(module.SemanticSource())
	if err != nil {
		return nil, nil, err
	}
	source, err = SourceWithProviderTriggerEvents(source, triggerCatalog)
	if err != nil {
		return nil, nil, err
	}
	channelTools := map[string]runtimecontracts.ToolSchemaEntry{}
	for _, binding := range channelBindings {
		tools, err := binding.RuntimeTools()
		if err != nil {
			return nil, nil, fmt.Errorf("compile channel binding %q runtime tools: %w", binding.ID, err)
		}
		for id, tool := range tools {
			if _, exists := channelTools[id]; exists {
				return nil, nil, fmt.Errorf("duplicate channel runtime tool %q", id)
			}
			channelTools[id] = tool
		}
	}
	source, err = semanticview.WithRuntimeTools(source, channelTools)
	if err != nil {
		return nil, nil, err
	}
	return connectorPackWorkflowModule{WorkflowModule: module, source: source}, source, nil
}

func compiledChannelActivityTools(bindings []packs.OutboundBindingPlan) (map[string]runtimepipeline.ChannelActivityTarget, error) {
	out := map[string]runtimepipeline.ChannelActivityTarget{}
	for _, binding := range bindings {
		operations := make([]string, 0, len(binding.Structural.Operations))
		for operation := range binding.Structural.Operations {
			operations = append(operations, operation)
		}
		sort.Strings(operations)
		for _, operation := range operations {
			id, generation, err := binding.RuntimeActivityTarget(operation)
			if err != nil {
				return nil, fmt.Errorf("channel binding %q operation %q private target: %w", binding.ID, operation, err)
			}
			if _, exists := out[id]; exists {
				return nil, fmt.Errorf("duplicate private channel activity tool %q", id)
			}
			tool, err := binding.Structural.OperationTool(operation)
			if err != nil {
				return nil, fmt.Errorf("channel binding %q operation %q: %w", binding.ID, operation, err)
			}
			out[id] = runtimepipeline.ChannelActivityTarget{Tool: tool, PlanGeneration: generation}
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

func newRuntimePromptResolver(source semanticview.Source) (runtimecontracts.PromptResolver, error) {
	if source == nil {
		return nil, fmt.Errorf("semantic source is required")
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		return nil, fmt.Errorf("bundle-backed semantic source is required for contract prompt resolution")
	}
	return runtimecontracts.NewBundlePromptResolverWithOptions(bundle, runtimecontracts.BundlePromptResolverOptions{
		PolicyResolver: func(itemSource runtimecontracts.ContractItemSource) runtimecontracts.PolicyDocument {
			if flowID := strings.TrimSpace(itemSource.FlowID); flowID != "" {
				return semanticview.ResolvePolicyForFlow(source, flowID)
			}
			return semanticview.ResolvePolicyForFlow(source, "")
		},
	}), nil
}

// Validate checks the NewRuntime boot dependency graph without constructing a runtime.
func (deps RuntimeDeps) Validate() error {
	_, err := deps.validated()
	return err
}

func (deps RuntimeDeps) validated() (validatedRuntimeDeps, error) {
	cfg := deps.Config
	stores := deps.Stores
	opts := deps.Options
	if cfg == nil {
		return validatedRuntimeDeps{}, fmt.Errorf("runtime config is required")
	}
	if err := cfg.ValidateExtensions(); err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("runtime config validation failed: %w", err)
	}
	profile, err := cfg.LLMBackendProfile()
	if err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("runtime config validation failed: %w", err)
	}
	executionMode, err := llmselection.ExecutionModeForProfile(profile)
	if err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("runtime execution mode validation failed: %w", err)
	}
	if blocker := strings.TrimSpace(stores.ConstructionBlocker); blocker != "" {
		return validatedRuntimeDeps{}, fmt.Errorf("runtime store boundary is not construction-ready: %s", blocker)
	}
	if stores.InboundStore != nil && opts.ProviderTriggerCatalog == nil {
		return validatedRuntimeDeps{}, fmt.Errorf("provider trigger catalog snapshot is required when inbound store is configured")
	}
	var source semanticview.Source
	if opts.WorkflowModule != nil {
		workflowModule, wrappedSource, err := workflowModuleWithProviderPacks(opts.WorkflowModule, opts.ProviderTriggerCatalog, opts.ChannelOutboundBindings)
		if err != nil {
			return validatedRuntimeDeps{}, fmt.Errorf("provider connector pack import failed: %w", err)
		}
		opts.WorkflowModule = workflowModule
		source = wrappedSource
	}
	mockConnectorResponses, err := ensureWorkflowBootWiring(opts, executionMode)
	if err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("workflow contract validation failed: %w", err)
	}
	if source == nil {
		source = opts.WorkflowModule.SemanticSource()
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
	promptResolver, err := newRuntimePromptResolver(source)
	if err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("build prompt resolver: %w", err)
	}
	authorityProvider := runtimeauthority.NewSourceProvider(source)
	emitRegistry := runtimetools.NewEmitRegistry(source, authorityProvider)
	credentials := opts.Credentials
	if credentials == nil {
		credentials = runtimecredentials.NewEnvStore()
	}
	return validatedRuntimeDeps{
		Config:                     cfg,
		Stores:                     stores,
		Options:                    opts,
		Source:                     source,
		PromptResolver:             promptResolver,
		Credentials:                credentials,
		ManagedCredentials:         opts.ManagedCredentials,
		MockConnectorResponses:     mockConnectorResponses,
		ExecutionMode:              executionMode,
		ProviderCredentialResolver: providerCredentialResolver,
		Authority:                  authorityProvider,
		EmitRegistry:               emitRegistry,
		TrimmedBundleFingerprint:   strings.TrimSpace(opts.BundleFingerprint),
		BundleSourceFact:           opts.BundleSourceFact.Normalized(),
	}, nil
}

func (deps validatedRuntimeDeps) payloadValidator(logger *RuntimeLogger) runtimebus.PayloadValidator {
	return newRuntimePayloadValidator(logger, deps.EmitRegistry.EventSchemaSnapshot())
}

func bindRuntimeStorePayloadValidator(stores Stores, payloadValidator runtimebus.PayloadValidator) {
	type eventPayloadValidationBinder interface {
		SetEventPayloadValidator(func(context.Context, string, []byte) error)
	}
	if binder, ok := stores.EventStore.(eventPayloadValidationBinder); ok && binder != nil {
		binder.SetEventPayloadValidator(payloadValidator)
	}
	if binder, ok := stores.InboundStore.(eventPayloadValidationBinder); ok && binder != nil {
		binder.SetEventPayloadValidator(payloadValidator)
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
		if _, ok := authored[strings.TrimSpace(proof.CatalogKey)]; ok {
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
	descriptors := make([]runtimeauthoractivity.EventDescriptor, 0, len(byName))
	for _, descriptor := range byName {
		descriptors = append(descriptors, descriptor)
	}
	sort.Slice(descriptors, func(i, j int) bool { return descriptors[i].EventType < descriptors[j].EventType })
	return descriptors, nil
}

type authorActivityCatalogRegistrar interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
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
	if rt.Stores.EventStore == nil && rt.Stores.InboundStore == nil {
		return nil
	}
	targets := []any{rt.Stores.EventStore, rt.Stores.InboundStore}
	registrars := make([]authorActivityCatalogRegistrar, 0, len(targets))
	for _, target := range targets {
		if registrar, ok := target.(authorActivityCatalogRegistrar); ok && registrar != nil {
			registrars = append(registrars, registrar)
		}
	}
	if len(registrars) == 0 {
		if rt.Stores.SQLDB != nil {
			return fmt.Errorf("selected store does not implement the author activity event catalog registry")
		}
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
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, rt.Options.RuntimeInstanceID)
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, rt.Options.BundleSourceFact)
	return runtimeauthoractivity.WithScope(ctx, rt.authorActivityScope)
}

func NewRuntime(ctx context.Context, deps RuntimeDeps) (*Runtime, error) {
	boot, err := deps.validated()
	if err != nil {
		return nil, err
	}
	cfg := boot.Config
	stores := boot.Stores
	opts := boot.Options
	source := boot.Source
	if opts.ProcessWorkOwner == nil {
		return nil, fmt.Errorf("runtime process work owner is required")
	}
	workOccurrence, err := opts.ProcessWorkOwner.NewRuntime(ctx, worklifetime.RuntimeIdentity{
		RuntimeInstanceID: opts.RuntimeInstanceID,
		BundleHash:        boot.BundleSourceFact.BundleHash,
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
	if stores.InboundStore != nil {
		if err := stores.InboundStore.ValidateInboundPublicationIntegrity(ctx); err != nil {
			return nil, fmt.Errorf("validate inbound publication integrity at startup: %w", err)
		}
	}
	mcpTurns := runtimemcp.NewTurnContextRegistry(runtimeactors.ActorFromContext)
	descriptors, err := AuthorActivityEventDescriptors(source)
	if err != nil {
		return nil, err
	}
	rt := &Runtime{
		ownerID:                   newRuntimeOwnerID(),
		bootID:                    uuid.NewString(),
		Config:                    cfg,
		Stores:                    stores,
		Options:                   opts,
		Workspace:                 opts.WorkspaceLifecycle,
		MCPTurns:                  mcpTurns,
		Authority:                 boot.Authority,
		EmitRegistry:              boot.EmitRegistry,
		authorActivityDescriptors: descriptors,
		authorActivityScope:       runtimeauthoractivity.BundleScope(opts.RuntimeInstanceID, boot.BundleSourceFact.BundleHash),
		PromptResolver:            boot.PromptResolver,
		Credentials:               boot.Credentials,
		ManagedCredentials:        boot.ManagedCredentials,
		workOccurrence:            workOccurrence,
	}

	if stores.RuntimeLogStore != nil {
		rt.Logger = NewRuntimeLogger(stores.RuntimeLogStore)
	}
	payloadValidator := boot.payloadValidator(rt.Logger)
	rt.payloadValidator = payloadValidator
	bus, err := newRuntimeEventBus(stores.EventStore, stores.PipelineObligations, rt.Logger, source, boot.TrimmedBundleFingerprint, boot.BundleSourceFact, opts.RuntimeInstanceID, workOccurrence, func() []runtimebus.EventInterceptor {
		if rt.Pipeline == nil {
			return nil
		}
		return []runtimebus.EventInterceptor{rt.Pipeline}
	}, payloadValidator, func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
		if managerRef == nil {
			return fmt.Errorf("flow instance activator is required")
		}
		return managerRef.ActivateFlowInstance(ctx, req)
	}, opts.ProviderTriggerCatalog, opts.TestLifecycleProbe)
	if err != nil {
		return nil, fmt.Errorf("build event bus: %w", err)
	}
	rt.Bus = bus
	rt.RuntimeIngress = runtimeingress.NewController(stores.RuntimeIngressStore, rt.Bus, runtimeingress.Options{})
	rt.Bus.SetRuntimeIngressDispatchGate(rt.RuntimeIngress)
	if runControlStore, ok := stores.EventStore.(runtimeruncontrol.Store); ok && runControlStore != nil {
		rt.RunControl = runtimeruncontrol.NewController(runControlStore, rt.Bus, runtimeruncontrol.Options{})
		rt.Bus.SetRunDispatchGate(rt.RunControl)
	}
	rt.Scheduler = runtimepipeline.NewSchedulerWithWorkOwner(workOccurrence, func(taskCtx context.Context, sc runtimepipeline.Schedule) {
		callbackCtx, cancel := context.WithTimeout(taskCtx, 10*time.Second)
		defer cancel()
		callbackCtx = events.WithDeliveryContext(callbackCtx, sc.Context)
		if rt.Pipeline != nil && rt.Pipeline.IsWorkflowTimerSchedule(sc) {
			outcome, err := rt.Pipeline.FireWorkflowTimer(callbackCtx, sc)
			if err != nil && rt.Logger != nil {
				handleRuntimeLogPersistenceError("scheduler", "workflow_timer_fire_failed", rt.Logger.Error(callbackCtx, "scheduler", "workflow_timer_fire_failed", map[string]any{
					"timer_id": sc.EffectiveTimerID(),
					"task_id":  sc.TaskID,
					"outcome":  outcome,
				}, err))
			}
			return
		}
		scheduled, constructErr := scheduledEvent(sc)
		if constructErr == nil {
			constructErr = rt.Bus.Publish(callbackCtx, scheduled)
		}
		if constructErr != nil {
			if rt.Logger != nil {
				handleRuntimeLogPersistenceError("scheduler", "publish_failed", rt.Logger.Error(callbackCtx, "scheduler", "publish_failed", map[string]any{
					"agent_id":   sc.AgentID,
					"event_type": sc.EventType,
					"run_id":     sc.EffectiveRunID(),
					"entity_id":  sc.EffectiveEntityID(),
				}, constructErr))
			}
		}
		if stores.ScheduleStore != nil {
			if err := stores.ScheduleStore.CompleteScheduleFireExact(callbackCtx, sc); err != nil {
				if rt.Logger != nil {
					handleRuntimeLogPersistenceError("scheduler", "mark_fired_failed", rt.Logger.Error(callbackCtx, "scheduler", "mark_fired_failed", map[string]any{
						"agent_id":   sc.AgentID,
						"event_type": sc.EventType,
						"run_id":     sc.EffectiveRunID(),
						"entity_id":  sc.EffectiveEntityID(),
					}, err))
				}
			}
		}
	})
	pipelineStore := stores.PipelineStore
	if pipelineStore == nil && stores.SQLDB != nil {
		return nil, fmt.Errorf("runtime pipeline store must be provided by selected runtime store construction")
	}
	if pipelineStore != nil && pipelineStore.Enabled() {
		channelActivityTools, err := compiledChannelActivityTools(opts.ChannelOutboundBindings)
		if err != nil {
			return nil, fmt.Errorf("compile channel activity tools: %w", err)
		}
		artifactRoot, err := runtimepipeline.ResolveArtifactRepoRoot("")
		if err != nil {
			return nil, fmt.Errorf("artifact repo root validation failed: %w", err)
		}
		rt.Pipeline = runtimepipeline.NewPipelineCoordinatorWithOptions(rt.Bus, stores.SQLDB, runtimepipeline.PipelineCoordinatorOptions{
			Module:              opts.WorkflowModule,
			WorkflowStore:       pipelineStore,
			DeliveryStore:       stores.DeliveryStore,
			PipelineObligations: stores.PipelineObligations,
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
			TimerScheduler:         rt.Scheduler,
			TimerScheduleStore:     stores.ScheduleStore,
			MailboxMaterializer:    stores.MailboxMaterializer,
			DecisionCards:          stores.DecisionCards,
			Credentials:            rt.Credentials,
			ManagedCredentials:     rt.ManagedCredentials,
			MockConnectorResponses: boot.MockConnectorResponses,
			ChannelActivityTools:   channelActivityTools,
			ArtifactRoot:           artifactRoot,
			BundleHash:             opts.BundleSourceFact.BundleHash,
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
		if rt.Pipeline != nil {
			rt.SystemNodes = append(rt.SystemNodes, rt.Pipeline.BackgroundNodes(rt.Bus, stores.SQLDB)...)
		}
	}

	if stores.BudgetSpendStore != nil {
		rt.Budget = NewBudgetTracker(stores.BudgetSpendStore, rt.Bus, cfg, stores.MailboxStore, rt.Logger, source)
	}

	backendProfile, err := cfg.LLMBackendProfile()
	if err != nil {
		return nil, err
	}
	modelRuntime := opts.LLMRuntime
	if modelRuntime == nil {
		effectStore, ok := stores.ManagerStore.(runtimeeffects.Store)
		if !ok || effectStore == nil {
			return nil, fmt.Errorf("selected runtime store does not implement completion execution authority")
		}
		modelRuntime, err = llm.RuntimeFactory{
			Cfg:                  cfg,
			Sessions:             stores.SessionRegistry,
			Conversations:        stores.ConversationStore,
			Workspaces:           rt.Workspace,
			Events:               rt.Bus,
			MCPTurns:             rt.MCPTurns,
			ToolGateway:          opts.ToolGatewayBinding,
			Credentials:          boot.ProviderCredentialResolver.Store,
			CompletionController: runtimeeffects.NewCompletionController(effectStore, rt.Budget),
		}.Build()
		if err != nil {
			return nil, fmt.Errorf("build runtime: %w", err)
		}
	}
	rt.LLM = modelRuntime
	if warnings, err := runtimetools.ValidateNativeToolBootConfig(ctx, source, rt.Credentials, modelRuntime, rt.Workspace); err != nil {
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

	rt.ToolExecutor = runtimetools.NewExecutorWithOptions(rt.Bus, rt.Scheduler, runtimetools.ExecutorOptions{
		Config:             cfg,
		Credentials:        rt.Credentials,
		ManagedCredentials: rt.ManagedCredentials,
		MailboxStore:       stores.MailboxStore,
		EntityStore:        stores.ToolEntityStore,
		HumanTaskStore:     stores.HumanTaskStore,
		WorkflowInstances:  pipelineStore,
		WorkflowSource:     source,
		ChannelBindings:    opts.ChannelOutboundBindings,
		ActivityExecutor:   rt.Pipeline,
		WorkspaceResolver:  rt.Workspace,
		ModelRuntime:       rt.LLM,
		AuthorityProvider:  rt.Authority,
		EmitRegistry:       rt.EmitRegistry,
		ManagerProvider: func() runtimetools.Manager {
			return managerRef
		},
	}, stores.ScheduleStore)
	credentialValidationOptions := runtimebootverify.Options{
		Credentials:            rt.Credentials,
		ManagedCredentials:     rt.ManagedCredentials,
		ExecutionMode:          boot.ExecutionMode,
		MockConnectorResponses: boot.MockConnectorResponses,
	}
	if missing, err := runtimebootverify.MissingStaticCredentialRequirements(ctx, source, credentialValidationOptions); err != nil {
		return nil, fmt.Errorf("credential validation failed: %w", err)
	} else {
		if bootWarningsFatal() && len(missing) > 0 {
			parts := make([]string, 0, len(missing))
			for _, item := range missing {
				requiredBy := make([]string, 0, len(item.RequiredBy))
				for _, ref := range item.RequiredBy {
					requiredBy = append(requiredBy, strings.TrimSpace(ref.Kind)+":"+strings.TrimSpace(ref.Name))
				}
				sort.Strings(requiredBy)
				parts = append(parts, fmt.Sprintf("%s required by %s", strings.TrimSpace(item.Key), strings.Join(requiredBy, ", ")))
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
	factory := runtimeagents.NewLLMAgentFactory(rt.LLM, rt.ToolExecutor, rt.ToolExecutor.ToolDefinitions(), runtimeagents.LLMAgentOptions{
		PromptResolver:    rt.PromptResolver,
		AuthorityProvider: rt.Authority,
		EmitRegistry:      rt.EmitRegistry,
	})
	var workflowInstances runtimepipeline.WorkflowInstancePersistence
	if rt.Pipeline != nil {
		workflowInstances = rt.Pipeline.WorkflowInstanceStore()
	}
	lifecycleStore, lifecycleStoreOK := stores.ManagerStore.(runtimemanager.AgentLifecyclePersistence)
	if stores.SQLDB != nil && (!lifecycleStoreOK || lifecycleStore == nil) {
		return nil, fmt.Errorf("selected runtime store does not implement agent lifecycle persistence")
	}
	rt.Manager = runtimemanager.NewAgentManagerWithOptions(rt.Bus, factory, runtimemanager.AgentManagerOptions{
		BaseContext:            rt.authorActivityContext(context.Background()),
		LifecycleStore:         lifecycleStore,
		DeliveryStore:          stores.DeliveryStore,
		TestLifecycleProbe:     opts.TestLifecycleProbe,
		Workspaces:             rt.Workspace,
		Sessions:               stores.SessionRegistry,
		SemanticSource:         source,
		PromptResolver:         rt.PromptResolver,
		WorkflowInstances:      workflowInstances,
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
		RuntimeIngressSafetyPause: func(ctx context.Context, reason string, failure *runtimefailures.Envelope) error {
			_, err := rt.RuntimeIngress.SafetyPause(ctx, runtimeingress.TransitionRequest{
				Reason:       reason,
				ControlledBy: "runtime",
				LastFailure:  failure,
			})
			return err
		},
		NativeToolAdmissionValidator: func(ctx context.Context, cfg runtimeactors.AgentConfig) error {
			return rt.ToolExecutor.ValidateNativeToolAdmission(ctx, cfg)
		},
		ThrottleSuppressPrefixes: runtimeThrottleSuppressPrefixes(source),
		DisableSpinupControl:     true,
	}, stores.ManagerStore)
	managerRef = rt.Manager

	if stores.InboundStore != nil {
		rt.InboundGateway = NewInboundGateway(rt.Bus, rt.Logger, rt.shutdownAdmissionClosed, stores.InboundStore)
		rt.InboundGateway.SetAdmissionGuard(rt.shutdownGate.BeginContext)
		rt.InboundGateway.SetRuntimeIngress(rt.RuntimeIngress)
		rt.InboundGateway.SetCredentialStore(opts.ProviderCredentials)
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
			return rt.Manager.GetAgentConfig(strings.TrimSpace(agentID))
		}, rt.shutdownAdmissionClosed, rt.EmitRegistry, rt.MCPTurns))
	}

	workOccurrenceOwned = false
	return rt, nil
}

func scheduleEventPayload(sc runtimepipeline.Schedule) []byte {
	payload := sc.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil || decoded == nil {
		return payload
	}
	delete(decoded, "__schedule_task_id")
	if _, present := decoded["timer_handle"]; present {
		handle, ok := timeridentity.ParseTimerHandle(decoded)
		if !ok || handle.Kind == timeridentity.TimerHandleWorkflowTimer {
			delete(decoded, "timer_handle")
		}
	}
	entityID := strings.TrimSpace(sc.EffectiveEntityID())
	if _, ok := decoded["entity_id"]; !ok {
		if entityID != "" {
			decoded["entity_id"] = entityID
		}
	}
	flowInstance := strings.TrimSpace(sc.EffectiveFlowInstance())
	if _, ok := decoded["flow_instance"]; !ok {
		if flowInstance != "" {
			decoded["flow_instance"] = flowInstance
		}
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return payload
	}
	return encoded
}

func scheduledEvent(sc runtimepipeline.Schedule) (events.Event, error) {
	return events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{Facts: events.EventFacts{
		ID: uuid.NewString(), Type: events.EventType(sc.EventType), Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime.scheduler"},
		TaskID: sc.TaskID, Payload: scheduleEventPayload(sc), CreatedAt: time.Now(), ExecutionMode: executionmode.Live,
		Envelope: events.EventEnvelope{
			EntityID:     sc.EffectiveEntityID(),
			FlowInstance: sc.EffectiveFlowInstance(),
		},
	}, RunID: sc.EffectiveRunID()})
}

func (rt *Runtime) Start(ctx context.Context) error {
	if rt == nil {
		return fmt.Errorf("runtime is nil")
	}
	if strings.TrimSpace(rt.ownerID) == "" {
		rt.ownerID = newRuntimeOwnerID()
	}
	if _, err := uuid.Parse(strings.TrimSpace(rt.bootID)); err != nil {
		rt.bootID = uuid.NewString()
	}
	if err := rt.PrepareAuthorActivityCatalog(); err != nil {
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
	if rt.cancelStart != nil || rt.ownershipLease != nil {
		rt.lifecycleMu.Unlock()
		return fmt.Errorf("runtime already started")
	}
	startCtx, cancelStart := context.WithCancel(ctx)
	lease := rt.pendingOwnershipLease
	preparedOwnedLease := lease != nil && rt.pendingOwnershipOwned
	borrowedLease := lease != nil && !preparedOwnedLease
	if rt.Stores.StartupOwnership != nil && lease == nil {
		var err error
		lease, err = rt.Stores.StartupOwnership.AcquireRuntimeStartupOwnership(ctx, runtimestartupownership.AcquireRequest{
			OwnerID: rt.ownerID, BootID: rt.bootID, BundleFingerprint: coalesceRuntimeIdentity(rt.Options.BundleFingerprint),
		})
		if err != nil {
			rt.emitBootProgress(5, "startup_ownership_lease", "FAILED", err.Error())
			cancelStart()
			rt.lifecycleMu.Unlock()
			rt.releaseAuthorActivityCatalog()
			return err
		}
		rt.emitBootProgress(5, "startup_ownership_lease", "ok", "owner="+rt.ownerID)
	} else if borrowedLease {
		rt.emitBootProgress(5, "startup_ownership_lease", "ok", "handoff_owner="+rt.ownerID)
	} else if preparedOwnedLease {
		rt.emitBootProgress(5, "startup_ownership_lease", "ok", "prepared_owner="+rt.ownerID)
	} else {
		authority, err := runtimestartupownership.NewColdAuthority(runtimestartupownership.AcquireRequest{
			OwnerID: rt.ownerID, BootID: rt.bootID, BundleFingerprint: coalesceRuntimeIdentity(rt.Options.BundleFingerprint),
		}, "memory")
		if err != nil {
			cancelStart()
			rt.lifecycleMu.Unlock()
			return err
		}
		lease, err = runtimestartupownership.NewLease(authority, nil, nil)
		if err != nil {
			cancelStart()
			rt.lifecycleMu.Unlock()
			return err
		}
		rt.emitBootProgress(5, "startup_ownership_lease", "ok", "memory_owner="+rt.ownerID)
	}
	rt.startCtx = startCtx
	rt.cancelStart = cancelStart
	rt.ownershipLease = lease
	rt.ownershipLeaseBorrowed = borrowedLease
	if preparedOwnedLease {
		rt.pendingOwnershipLease = nil
		rt.pendingOwnershipOwned = false
	}
	rt.replacementQuiesced = false
	rt.lifecycleMu.Unlock()
	started := false
	defer func() {
		if started {
			return
		}
		rt.cleanupStartFailure()
	}()
	bindRuntimeStorePayloadValidator(rt.Stores, rt.payloadValidator)
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
		rt.emitBootProgress(6, "recovery_snapshot_inspection", "skipped", "persistent startup recovery disabled")
	} else {
		startupRecoverySnapshot, err = rt.inspectStartupRecoverySnapshot(ctx)
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
		rt.emitBootProgress(7, "recovery_decision", "skipped", "persistent startup recovery disabled")
	} else {
		rt.emitBootProgress(7, "recovery_decision", string(startupRecoveryDecision.Outcome), string(startupRecoveryDecision.ReasonCode))
	}

	if rt.Pipeline != nil {
		if _, err := rt.Pipeline.RepairContractEntityTypes(ctx); err != nil {
			rt.emitBootProgress(8, "pipeline_maintenance", "FAILED", err.Error())
			return fmt.Errorf("repair contract entity types: %w", err)
		}
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
	if skipPersistentStartupRecovery {
		rt.emitBootProgress(10, "schedule_restoration", "skipped", "persistent startup recovery disabled")
	} else if rt.Scheduler != nil && rt.Stores.ScheduleStore != nil {
		schedules, err := rt.Stores.ScheduleStore.LoadActiveSchedules(ctx)
		if err != nil {
			rt.emitBootProgress(10, "schedule_restoration", "FAILED", err.Error())
			return fmt.Errorf("load schedules failed: %w", err)
		}
		startupRecoveryDecision.ScheduleRestoreAttempted = len(schedules) > 0
		results := restoreStartupTimerSchedules(ctx, rt.Stores.ScheduleStore, rt.Scheduler, rt.Logger, schedules)
		timerReplayCount, timerSkipCount, timerDropCount, _ := summarizeStartupTimerRecovery(results)
		startupRecoveryDecision.ScheduleReplayCount = timerReplayCount
		startupRecoveryDecision.ScheduleSkipCount = timerSkipCount
		startupRecoveryDecision.ScheduleDropCount = timerDropCount
		if rt.Pipeline != nil {
			if err := rt.Pipeline.RestoreWorkflowTimers(ctx); err != nil {
				rt.emitBootProgress(10, "schedule_restoration", "FAILED", err.Error())
				return fmt.Errorf("restore workflow timers: %w", err)
			}
		}
		if err := ensureBootWorkflowSchedules(ctx, rt.Stores.ScheduleStore, rt.Scheduler, rt.Pipeline, schedules); err != nil {
			if rt.Logger != nil {
				handleRuntimeLogPersistenceError("scheduler", "ensure_boot_timers_failed", rt.Logger.Error(ctx, "scheduler", "ensure_boot_timers_failed", nil, err))
			}
		}
		if startupRecoveryDecision.Outcome != startupRecoveryOutcomeDegraded && startupRecoveryDecision.ScheduleDropCount > 0 {
			startupRecoveryDecision.Outcome = startupRecoveryOutcomeDegraded
			startupRecoveryDecision.ReasonCode = startupRecoveryReasonScheduleRestore
			startupRecoveryDecision.Failure = newStartupRecoveryFailure(runtimefailures.ClassDependencyUnavailable, "schedule_restore_failed", "restore_schedules", map[string]any{"dropped_count": startupRecoveryDecision.ScheduleDropCount}, nil)
		}
		rt.emitBootProgress(10, "schedule_restoration", "ok", fmt.Sprintf("%d schedules restored, %d skipped, %d dropped", startupRecoveryDecision.ScheduleReplayCount, startupRecoveryDecision.ScheduleSkipCount, startupRecoveryDecision.ScheduleDropCount))
	} else {
		rt.emitBootProgress(10, "schedule_restoration", "skipped", "scheduler or schedule store unavailable")
	}
	if skipPersistentStartupRecovery {
		rt.emitBootProgress(11, "manager_recovery_if_enabled", "skipped", "persistent startup recovery disabled")
	} else if !rt.Config.Runtime.RecoveryOnStartup {
		rt.emitBootProgress(11, "manager_recovery_if_enabled", "skipped", "recovery_on_startup disabled")
	} else {
		recovered := make([]string, 0, 2)
		agentHydrationSucceeded := true
		if rt.Manager != nil {
			startupRecoveryDecision.ManagerRecoveryAttempted = true
			_, err := rt.Manager.HydrateForStartup(ctx)
			if err != nil {
				agentHydrationSucceeded = false
				rt.recordStartupManagerRecoveryFailure(ctx, &startupRecoveryDecision, err)
			} else {
				recovered = append(recovered, "agent state")
			}
		}
		if rt.Pipeline != nil && agentHydrationSucceeded {
			if err := rt.Pipeline.RecoverNodeDeliveries(ctx); err != nil {
				rt.emitBootProgress(11, "manager_recovery_if_enabled", "FAILED", err.Error())
				return fmt.Errorf("recover executable workflow-node deliveries: %w", err)
			}
			recovered = append(recovered, "workflow-node deliveries")
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
			detail = strings.Join(recovered, " and ") + " hydrated; managed replay awaits execution admission"
		}
		rt.emitBootProgress(11, "manager_recovery_if_enabled", status, detail)
	}
	if rt.Bus != nil {
		sweeperConfig := rt.Options.TestOutboxSweeperConfig
		if sweeperConfig == (runtimebus.OutboxSweeperConfig{}) {
			sweeperConfig = runtimebus.DefaultOutboxSweeperConfig()
		}
		if err := rt.Bus.StartOutboxSweeper(startCtx, sweeperConfig); err != nil {
			return fmt.Errorf("start outbox sweeper: %w", err)
		}
		rt.emitBootProgress(12, "outbox_sweeper", "started", "")
	} else {
		rt.emitBootProgress(12, "outbox_sweeper", "skipped", "event bus unavailable")
	}
	staticAgentIDs := []string{}
	if rt.Manager != nil {
		staticAgentIDs, err = staticBootAgentIDs(rt.Options.WorkflowModule.SemanticSource())
		if err != nil {
			rt.emitBootProgress(13, "static_agents_bootstrap", "FAILED", err.Error())
			return fmt.Errorf("bootstrap static agents: %w", err)
		}
		if err := rt.Manager.EnsureStaticAgents(ctx, rt.Options.WorkflowModule.SemanticSource()); err != nil {
			rt.emitBootProgress(13, "static_agents_bootstrap", "FAILED", err.Error())
			return fmt.Errorf("bootstrap static agents: %w", err)
		}
		rt.emitBootProgress(13, "static_agents_bootstrap", "ok", fmt.Sprintf("%d static agents", len(staticAgentIDs)))
	} else {
		rt.emitBootProgress(13, "static_agents_bootstrap", "skipped", "manager unavailable")
	}
	flowRequiredAgentIDs := []string{}
	if rt.Manager != nil {
		flowRequiredAgentIDs, err = staticFlowRequiredBootAgentIDs(rt.Options.WorkflowModule.SemanticSource())
		if err != nil {
			rt.emitBootProgress(14, "flow_required_agents", "FAILED", err.Error())
			return fmt.Errorf("bootstrap static flow required agents: %w", err)
		}
		if err := rt.Manager.EnsureStaticFlowRequiredAgents(ctx, rt.Options.WorkflowModule.SemanticSource()); err != nil {
			rt.emitBootProgress(14, "flow_required_agents", "FAILED", err.Error())
			return fmt.Errorf("bootstrap static flow required agents: %w", err)
		}
		rt.emitBootProgress(14, "flow_required_agents", "ok", fmt.Sprintf("%d flow-required agents", len(flowRequiredAgentIDs)))
	} else {
		rt.emitBootProgress(14, "flow_required_agents", "skipped", "manager unavailable")
	}
	source := rt.Options.WorkflowModule.SemanticSource()
	if rt.Options.LLMRuntime == nil {
		if err := validateSelectedBackendCredentialForActiveAgents(ctx, rt.Config, rt.Options, source, rt.Manager); err != nil {
			rt.emitBootProgress(15, "workspace_validation_and_system_containers", "FAILED", err.Error())
			return fmt.Errorf("llm backend credential validation failed: %w", err)
		}
	}
	if err := validateClaudeStartupConfigForActiveAgents(ctx, rt.Config, rt.Options, source, rt.Manager); err != nil {
		rt.emitBootProgress(15, "workspace_validation_and_system_containers", "FAILED", err.Error())
		return fmt.Errorf("claude runtime startup validation failed: %w", err)
	}
	if err := validateClaudeManagedAgentWorkspaces(ctx, rt.Config, source, rt.Workspace, rt.Manager); err != nil {
		rt.emitBootProgress(15, "workspace_validation_and_system_containers", "FAILED", err.Error())
		return fmt.Errorf("claude runtime workspace validation failed: %w", err)
	}
	rt.emitBootProgress(15, "workspace_validation_and_system_containers", "ok", fmt.Sprintf("%d system containers", len(rt.Options.SystemContainers)))
	startupProbe, _ := llm.StartupVisibleToolSurfaceProberForRuntime(rt.LLM)
	startupAuthority, err := rt.currentStartupProbeAuthority()
	if err != nil {
		rt.emitBootProgress(16, "mcp_tool_validation", "FAILED", err.Error())
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
			rt.emitBootProgress(16, "mcp_tool_validation", "FAILED", err.Error())
			return err
		}
	}
	surfaceIDs, err := ValidateManagedProviderPreflight(ctx, rt.Config, source, rt.Options.ToolGatewayBinding, rt.LLM, startupProbe, rt.MCPTurns, rt.ToolExecutor, rt.Manager, preflightAuthority)
	if err != nil {
		rt.emitBootProgress(16, "mcp_tool_validation", "FAILED", err.Error())
		return fmt.Errorf("claude runtime mcp validation failed: %w", err)
	}
	settledAuthority, handoffPending, err := rt.settleManagedStartupPreflight(ctx, surfaceIDs)
	if err != nil {
		rt.emitBootProgress(16, "mcp_tool_validation", "FAILED", err.Error())
		return fmt.Errorf("settle managed startup preflight: %w", err)
	}
	rt.emitBootProgress(16, "mcp_tool_validation", "ok", fmt.Sprintf("%d capability surfaces settled", len(surfaceIDs)))
	if rt.Manager != nil && !handoffPending {
		activation, activateErr := rt.admitManagedExecution(startCtx, settledAuthority, rt.Config.Runtime.RecoveryOnStartup && !skipPersistentStartupRecovery)
		if activateErr != nil {
			rt.emitBootProgress(17, "manager_event_loop_start", "FAILED", activateErr.Error())
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
			rt.emitBootProgress(17, "manager_event_loop_start", "FAILED", err.Error())
			return fmt.Errorf("start managed execution loops: %w", err)
		}
		rt.emitBootProgress(17, "manager_event_loop_start", "ok", "")
	} else if handoffPending {
		rt.emitBootProgress(17, "manager_event_loop_start", "deferred", "replacement execution awaits startup ownership commit")
	} else {
		rt.emitBootProgress(17, "manager_event_loop_start", "skipped", "manager unavailable")
	}
	rt.logStartupRecoveryDecision(ctx, startupRecoveryDecision)
	if rt.Stores.SQLDB != nil && rt.Logger != nil {
	}
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
	if rt.Stores.MailboxStore != nil {
		ctxPayload := mustJSON(map[string]any{
			"failure":     *decision.Failure,
			"instruction": "Runtime recovery failed. Reinitialize or repair persisted runtime state before restart.",
		})
		if _, mailboxErr := rt.Stores.MailboxStore.InsertMailboxItem(ctx, runtimetools.MailboxItem{
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
		recoveryEvent, publishErr := newStandaloneRuntimePlatformDiagnosticEvent(events.EventType("platform.recovery_failed"), payload, events.EventEnvelope{}, time.Now())
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
	return rt.stopWithOptions(opts, true)
}

func (rt *Runtime) QuiesceForReplacement(opts ShutdownOptions) error {
	return rt.stopWithOptions(opts, false)
}

func (rt *Runtime) stopWithOptions(opts ShutdownOptions, releaseOwnership bool) error {
	if rt == nil {
		return nil
	}
	grace, err := runtimemanager.ResolveShutdownGrace(opts.Grace)
	if err != nil {
		return err
	}
	rt.lifecycleMu.Lock()
	if rt.ownershipHandoffPending {
		rt.lifecycleMu.Unlock()
		return fmt.Errorf("runtime startup ownership handoff is pending")
	}
	rt.lifecycleMu.Unlock()
	rt.shutdownGate.Close()
	if rt.workOccurrence != nil {
		_ = rt.workOccurrence.Fence()
	}
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), grace)
	defer cancelDrain()
	var shutdownErr error
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
	lease := rt.ownershipLease
	borrowedLease := rt.ownershipLeaseBorrowed
	rt.cancelStart = nil
	rt.startCtx = nil
	if releaseOwnership && borrowedLease {
		rt.ownershipLease = nil
		rt.ownershipLeaseBorrowed = false
	}
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
	if rt.Scheduler != nil {
		rt.Scheduler.Stop()
		if err := rt.Scheduler.Wait(drainCtx); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("scheduler shutdown: %w", err))
			_ = rt.Scheduler.Wait(context.Background())
		}
	}
	if rt.Bus != nil {
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
	if rt.Stores.ScheduleStore != nil {
		if err := rt.Stores.ScheduleStore.ReleaseScheduleClaims(context.Background()); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("release schedule claims: %w", err))
		}
	}
	if releaseOwnership && lease != nil && !borrowedLease {
		if err := lease.Release(context.Background()); err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
		} else {
			rt.lifecycleMu.Lock()
			if rt.ownershipLease == lease {
				rt.ownershipLease = nil
				rt.ownershipLeaseBorrowed = false
			}
			rt.lifecycleMu.Unlock()
		}
	}
	rt.lifecycleMu.Lock()
	rt.replacementQuiesced = true
	rt.lifecycleMu.Unlock()
	rt.releaseAuthorActivityCatalog()
	return shutdownErr
}

func (rt *Runtime) cleanupStartFailure() {
	_ = rt.stopWithOptions(DefaultShutdownOptions(), true)
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
			nodeCount = len(source.NodeEntries())
			agentCount = len(source.AgentEntries())
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
		"bundle_fingerprint":           strings.TrimSpace(rt.Options.BundleFingerprint),
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
		Payload: payload, CreatedAt: time.Now(), ExecutionMode: executionmode.Live,
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
	_ = rt.LLM
	return nil
}

var bootWorkflowTimerPolicyPlaceholder = regexp.MustCompile(`\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}`)

func ensureBootWorkflowSchedules(ctx context.Context, store runtimepipeline.SchedulePersistence, scheduler *runtimepipeline.Scheduler, workflow runtimepipeline.WorkflowRuntime, activeSnapshots ...[]runtimepipeline.Schedule) error {
	if store == nil || workflow == nil {
		return nil
	}
	source := workflow.SemanticSource()
	if source == nil {
		return nil
	}
	activeSchedules, err := activeScheduleSnapshot(ctx, store, activeSnapshots...)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, timer := range source.WorkflowTimers() {
		sc, ok := bootWorkflowTimerSchedule(source, timer, now)
		if !ok {
			continue
		}
		if active, ok := activeScheduleExactMatch(activeSchedules, sc); ok {
			if _, err := runtimepipeline.ClaimAndRegisterSchedule(ctx, store, scheduler, active); err != nil {
				return err
			}
			continue
		}
		if err := store.UpsertSchedule(ctx, sc); err != nil {
			return err
		}
		if _, err := runtimepipeline.ClaimAndRegisterSchedule(ctx, store, scheduler, sc); err != nil {
			return err
		}
	}
	return nil
}

func activeScheduleSnapshot(ctx context.Context, store runtimepipeline.SchedulePersistence, snapshots ...[]runtimepipeline.Schedule) ([]runtimepipeline.Schedule, error) {
	if len(snapshots) > 0 {
		return append([]runtimepipeline.Schedule(nil), snapshots[0]...), nil
	}
	if store == nil {
		return nil, nil
	}
	return store.LoadActiveSchedules(ctx)
}

func activeScheduleExactMatch(active []runtimepipeline.Schedule, target runtimepipeline.Schedule) (runtimepipeline.Schedule, bool) {
	normalizeScheduleIdentity(&target)
	for _, candidate := range active {
		normalizeScheduleIdentity(&candidate)
		if strings.TrimSpace(candidate.AgentID) != strings.TrimSpace(target.AgentID) {
			continue
		}
		if strings.TrimSpace(candidate.EventType) != strings.TrimSpace(target.EventType) {
			continue
		}
		if strings.TrimSpace(candidate.TaskID) != strings.TrimSpace(target.TaskID) {
			continue
		}
		if candidate.RunID != target.RunID {
			continue
		}
		if candidate.EntityID != target.EntityID {
			continue
		}
		if candidate.FlowInstance != target.FlowInstance {
			continue
		}
		return candidate, true
	}
	return runtimepipeline.Schedule{}, false
}

func normalizeScheduleIdentity(sc *runtimepipeline.Schedule) {
	if sc == nil {
		return
	}
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	sc.NormalizeTimerID()
}

func bootWorkflowTimerSchedule(source semanticview.Source, timer runtimecontracts.WorkflowTimerContract, now time.Time) (runtimepipeline.Schedule, bool) {
	startTrigger, err := timeridentity.ParseStartTrigger(timer.StartOn)
	if err != nil || !startTrigger.IsBoot() {
		return runtimepipeline.Schedule{}, false
	}
	cancelTrigger, err := timeridentity.ParseCancelTrigger(timer.CancelOn)
	if err != nil || cancelTrigger.Valid() {
		return runtimepipeline.Schedule{}, false
	}
	owner := strings.TrimSpace(timer.Owner)
	eventType := strings.TrimSpace(timer.Event)
	if owner == "" || eventType == "" {
		return runtimepipeline.Schedule{}, false
	}
	interval := bootWorkflowTimerDuration(source, timer)
	if interval <= 0 {
		return runtimepipeline.Schedule{}, false
	}
	handle := timeridentity.WorkflowTimerHandle(timer.ID)
	sc := runtimepipeline.Schedule{
		AgentID:   owner,
		EventType: eventType,
		Mode:      "once",
		At:        now.Add(interval),
		TaskID:    handle.TaskID(),
		Payload:   workflowTimerPayload(timer),
	}
	if timer.Recurring {
		sc.Mode = "cron"
		sc.Cron = "@every " + interval.String()
		sc.At = time.Time{}
	}
	return sc, true
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

func workflowTimerPayload(timer runtimecontracts.WorkflowTimerContract) []byte {
	handle := timeridentity.WorkflowTimerHandle(timer.ID)
	if !handle.Valid() {
		return mustJSON(map[string]any{})
	}
	return mustJSON(handle.PayloadMetadata())
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
