package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/config"
	swaruntime "github.com/division-sh/swarm/internal/runtime"
	runtimeagents "github.com/division-sh/swarm/internal/runtime/agents"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

const selectedContractAgentRuntimeDefaultQuiescenceTimeout = 2 * time.Minute

type SelectedContractAgentRuntimeOptions struct {
	Config              *config.Config
	ExecutionPosture    executionposture.Posture
	EntityStore         runtimetools.EntityPersistence
	HumanTaskStore      runtimetools.HumanTaskCardStore
	SessionRegistry     runtimesessions.Registry
	ConversationStore   runtimellm.ConversationPersistence
	MailboxStore        runtimetools.MailboxPersistence
	NoticePresentation  runtimetools.InformationalNoticePresentationSink
	Workspace           workspace.Lifecycle
	Credentials         runtimecredentials.Store
	ManagedCredentials  runtimemanagedcredentials.Store
	ProviderCredentials runtimecredentials.Store
	LLMRuntime          runtimellm.Runtime
	MCPClient           *runtimemcp.Client
	AgentFactory        runtimemanager.AgentFactory
	AgentManagerOptions runtimemanager.AgentManagerOptions
	ProcessCapability   runtimestartupownership.ProcessCapability
	QuiescenceTimeout   time.Duration
}

type SelectedContractAgentRuntimeMaterialization struct {
	Owner                      string                   `json:"owner"`
	RecipientPlanningOwner     string                   `json:"recipient_planning_owner"`
	ExecutionOwner             string                   `json:"execution_owner"`
	AgentRecipientPlans        []agentidentity.Plan     `json:"agent_recipient_plans,omitempty"`
	ConfiguredAgentPlans       []agentidentity.Plan     `json:"configured_agent_plans,omitempty"`
	MissingAgentRecipientPlans []agentidentity.Plan     `json:"missing_agent_recipient_plans,omitempty"`
	AgentRecipients            []agentidentity.Identity `json:"agent_recipients,omitempty"`
	ConfiguredAgentIdentities  []agentidentity.Identity `json:"configured_agent_identities,omitempty"`
	MissingAgentRecipients     []agentidentity.Identity `json:"missing_agent_recipients,omitempty"`
	MaterializationRequired    bool                     `json:"materialization_required"`
	MaterializationSupported   bool                     `json:"materialization_supported"`
	EphemeralForkLocal         bool                     `json:"ephemeral_fork_local"`
}

type selectedContractAgentRuntimePlan struct {
	SourceRunID         string
	Declarations        runtimeagenttopology.SelectedDeclarationPlan
	Proof               SelectedContractAgentRuntimeMaterialization
	Blueprints          []runtimemanager.AgentMaterializationBlueprint
	Flows               []runtimemanager.FlowInstanceMaterializationPlan
	ConfiguredPlans     []agentidentity.Plan
	Records             []runtimemanager.PersistedAgent
	Options             SelectedContractAgentRuntimeOptions
	workspaceProjection *selectedContractWorkspaceProjection
}

func (p selectedContractAgentRuntimePlan) bindRun(runID string, committed []runfork.RunForkSelectedContractAgentTopology) (selectedContractAgentRuntimePlan, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return selectedContractAgentRuntimePlan{}, errors.New("selected-contract agent runtime requires fork run_id")
	}
	selectedTopology, err := runtimeagenttopology.SelectedDeclarationAdmission(runID, p.Declarations)
	if err != nil {
		return selectedContractAgentRuntimePlan{}, err
	}
	declarations := make(map[agentidentity.Plan]struct{}, len(p.Declarations.Agents))
	for _, desired := range p.Declarations.Agents {
		declarations[desired.Identity] = struct{}{}
	}
	bindPlans := func(in []agentidentity.Plan) ([]agentidentity.Identity, error) {
		out := make([]agentidentity.Identity, 0, len(in))
		for _, plan := range in {
			bound, err := plan.Live(runID)
			if err != nil {
				return nil, err
			}
			out = append(out, bound)
		}
		sortAgentIdentities(out)
		return out, nil
	}
	p.Proof.AgentRecipients, err = bindPlans(p.Proof.AgentRecipientPlans)
	if err != nil {
		return selectedContractAgentRuntimePlan{}, err
	}
	p.Proof.ConfiguredAgentIdentities, err = bindPlans(p.ConfiguredPlans)
	if err != nil {
		return selectedContractAgentRuntimePlan{}, err
	}
	p.Proof.MissingAgentRecipients, err = bindPlans(p.Proof.MissingAgentRecipientPlans)
	if err != nil {
		return selectedContractAgentRuntimePlan{}, err
	}
	topologyByIdentity := make(map[agentidentity.Identity]runtimeagenttopology.Admission, len(committed))
	for _, evidence := range committed {
		identity := evidence.Identity.Normalize()
		if err := identity.Validate(); err != nil || identity.RunID != runID {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract committed agent topology has invalid fork identity")
		}
		if err := evidence.Admission.Validate(); err != nil {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract committed agent topology %s: %w", identity.Description(), err)
		}
		topologyByIdentity[identity] = evidence.Admission
	}
	records := make([]runtimemanager.PersistedAgent, 0, len(p.Blueprints))
	for _, blueprint := range p.Blueprints {
		record, materializeErr := blueprint.Materialize(runID)
		if materializeErr != nil {
			return selectedContractAgentRuntimePlan{}, materializeErr
		}
		identity, identityErr := record.Config.ConcreteIdentity()
		if identityErr != nil {
			return selectedContractAgentRuntimePlan{}, identityErr
		}
		if topology, ok := topologyByIdentity[identity]; ok {
			record.Topology = topology
		} else if _, declared := declarations[blueprint.Identity]; declared {
			record.Topology = selectedTopology
		} else {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract agent %s has no admitted declaration or committed readiness topology", identity.Description())
		}
		if err := record.Topology.Validate(); err != nil {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract agent %s topology: %w", identity.Description(), err)
		}
		records = append(records, record)
	}
	p.Records = records
	p.Flows = append([]runtimemanager.FlowInstanceMaterializationPlan(nil), p.Flows...)
	for i := range p.Flows {
		parent, err := runfork.ProjectParentRoute(p.SourceRunID, runID, p.Flows[i].Instance.ParentRoute)
		if err != nil {
			return selectedContractAgentRuntimePlan{}, err
		}
		p.Flows[i].Instance.ParentRoute, p.Flows[i].Instance.ParentEntityID = parent, parent.EntityID
	}
	for _, identity := range p.Proof.AgentRecipients {
		if _, ok := topologyByIdentity[identity]; ok {
			continue
		}
		found := false
		for _, record := range records {
			recordIdentity, identityErr := record.Config.ConcreteIdentity()
			if identityErr == nil && recordIdentity == identity {
				found = true
				break
			}
		}
		if !found {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract agent %s was not materialized", identity.Description())
		}
	}
	return p, nil
}

func sortAgentPlans(plans []agentidentity.Plan) {
	sort.Slice(plans, func(i, j int) bool {
		return agentidentity.LessPlan(plans[i], plans[j])
	})
}

func agentPlanDescriptions(plans []agentidentity.Plan) []string {
	out := make([]string, 0, len(plans))
	for _, plan := range plans {
		out = append(out, plan.Description())
	}
	return out
}

type selectedContractAgentRuntime struct {
	operationContext       context.Context
	mu                     sync.Mutex
	shutdownMu             sync.Mutex
	manager                *runtimemanager.AgentManager
	generationGrant        runtimestartupownership.GenerationGrant
	cleanup                func()
	workspaceProjection    *selectedContractWorkspaceProjection
	bus                    *runtimebus.EventBus
	pipeline               selectedFlowActivationRetirementStore
	pendingActivations     []selectedFlowActivation
	executionLease         *worklifetime.Lease
	controlContext         context.Context
	cancelExecution        context.CancelCauseFunc
	executionDone          chan error
	scheduler              *runtimepipeline.Scheduler
	timerLifecycle         *runtimepipeline.PipelineCoordinator
	genericSchedules       *genericschedule.Lifecycle
	fanOutServing          *runtimestartupownership.FanOutServingRegistration
	completionExecutor     *runlifecycle.Executor
	completionRegistration runlifecycle.CandidateRegistration
	completionDiagnostics  *selectedForkCommitDiagnostics
}

type selectedContractWorkspaceProjection struct {
	lifecycle   workspace.Lifecycle
	mu          sync.Mutex
	initialized bool
	released    bool
}

func (p *selectedContractWorkspaceProjection) Initialize(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released {
		return errors.New("selected workspace projection is released")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.initialized {
		return nil
	}
	if err := p.lifecycle.EnsurePrereqs(ctx); err != nil {
		return fmt.Errorf("initialize selected workspace prerequisites: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.lifecycle.EnsureSystemWorkspaces(ctx); err != nil {
		return fmt.Errorf("initialize selected system workspace: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.initialized = true
	return nil
}

func (p *selectedContractWorkspaceProjection) Release() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released {
		return nil
	}
	if err := p.lifecycle.ReleaseSourceProjection(context.Background()); err != nil {
		return err
	}
	p.released = true
	return nil
}

func (p *selectedContractAgentRuntimePlan) releaseWorkspaceProjection() error {
	if p == nil {
		return nil
	}
	return p.workspaceProjection.Release()
}

type selectedContractAgentRuntimeFactory struct {
	factory     runtimemanager.AgentFactory
	options     runtimemanager.AgentManagerOptions
	bindManager func(*runtimemanager.AgentManager)
	cleanup     func()
	runtimes    *runtimellm.AgentRuntimeSet
	tools       *runtimetools.Executor
}

func selectedContractManagerOptions(options runtimemanager.AgentManagerOptions, lifecycle runtimemanager.AgentLifecyclePersistence, bus *runtimebus.EventBus, ports *selectedContractExecutionPorts, pipeline *runtimepipeline.PipelineCoordinator) runtimemanager.AgentManagerOptions {
	roles := ports.managerRoles
	options.LifecycleStore = lifecycle
	roles.AgentRoutes = bus
	roles.FlowActivation = bus
	roles.FlowTermination = pipeline
	roles.CreationPublisher = bus
	roles.DeliveryRuntime = bus
	options.PersistenceRoles = roles
	return options
}

func selectedContractAgentModelOptions(options SelectedContractAgentRuntimeOptions) (runtimemanager.AgentManagerOptions, llmselection.Profile, error) {
	managerOptions := options.AgentManagerOptions
	managerOptions.ExecutionPosture = options.ExecutionPosture
	var backendProfile llmselection.Profile
	if options.Config == nil {
		return managerOptions, backendProfile, nil
	}
	backendProfile, err := options.Config.LLMBackendProfile()
	if err != nil {
		return runtimemanager.AgentManagerOptions{}, llmselection.Profile{}, err
	}
	if configured := strings.TrimSpace(managerOptions.LLMBackend); configured != "" && configured != backendProfile.ID {
		return runtimemanager.AgentManagerOptions{}, llmselection.Profile{}, fmt.Errorf("selected-contract manager llm backend %q conflicts with runtime default %q", configured, backendProfile.ID)
	}
	managerOptions.LLMBackend = backendProfile.ID
	managerOptions.ModelAliases = options.Config.LLM.Models
	return managerOptions, backendProfile, nil
}

func prepareSelectedContractAgentRuntimeMaterialization(ctx context.Context, loaded LoadedSelectedContractSource, planning runfork.RunForkSelectedContractRecipientPlanning, blueprints []runtimemanager.AgentMaterializationBlueprint, options SelectedContractAgentRuntimeOptions) (_ selectedContractAgentRuntimePlan, resultErr error) {
	if err := ctx.Err(); err != nil {
		return selectedContractAgentRuntimePlan{}, err
	}
	if strings.TrimSpace(planning.Owner) != runfork.RunForkSelectedContractRecipientPlanningOwner {
		return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract agent runtime materialization requires %s; got %q", runfork.RunForkSelectedContractRecipientPlanningOwner, planning.Owner)
	}
	agentPlans, err := planning.SelectedAgentPlans()
	if err != nil {
		return selectedContractAgentRuntimePlan{}, err
	}
	actors, err := runforkreadiness.PreparedActorCensus(blueprints)
	if err != nil {
		return selectedContractAgentRuntimePlan{}, err
	}
	declarations, err := prepareSelectedContractDeclarations(loaded, blueprints)
	if err != nil {
		return selectedContractAgentRuntimePlan{}, err
	}
	proof := SelectedContractAgentRuntimeMaterialization{
		Owner:                    runfork.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner,
		RecipientPlanningOwner:   planning.Owner,
		ExecutionOwner:           runfork.RunForkSelectedContractExecutionOwner,
		AgentRecipientPlans:      append([]agentidentity.Plan(nil), agentPlans...),
		MaterializationRequired:  len(actors) > 0,
		MaterializationSupported: len(actors) == 0,
		EphemeralForkLocal:       true,
	}
	if len(actors) == 0 && len(agentPlans) == 0 && !semanticview.SourceDeclaresAgents(loaded.Source) {
		return selectedContractAgentRuntimePlan{Declarations: declarations, Proof: proof, Options: options}, nil
	}
	options, workspaceProjection, err := bindSelectedContractWorkspaceProjection(loaded, options)
	if err != nil {
		return selectedContractAgentRuntimePlan{Proof: proof, Options: options}, err
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, workspaceProjection.Release())
		}
	}()
	blueprintsByPlan := map[agentidentity.Plan]runtimemanager.AgentMaterializationBlueprint{}
	configured := make([]agentidentity.Plan, 0, len(actors))
	for _, blueprint := range actors {
		plan := blueprint.Identity
		blueprint.Status = "ephemeral"
		blueprint.HiredBy = "selected-contract-fork-agent-runtime"
		blueprintsByPlan[plan] = blueprint
		configured = append(configured, plan)
	}
	proof.ConfiguredAgentPlans = append([]agentidentity.Plan(nil), configured...)

	selected := make([]runtimemanager.AgentMaterializationBlueprint, 0, len(actors))
	for _, plan := range configured {
		selected = append(selected, blueprintsByPlan[plan])
	}
	missing := []agentidentity.Plan{}
	for _, plan := range agentPlans {
		_, ok := blueprintsByPlan[plan.Normalize()]
		if !ok {
			missing = append(missing, plan)
			continue
		}
	}
	if len(missing) > 0 {
		sortAgentPlans(missing)
		proof.MissingAgentRecipientPlans = missing
		return selectedContractAgentRuntimePlan{Proof: proof, Blueprints: selected, ConfiguredPlans: configured, Options: options}, selectedContractAgentRuntimeUnsupportedPlanError(missing, "missing selected-source declaration-owned agent materialization blueprint")
	}
	if options.AgentFactory == nil && options.Config == nil {
		return selectedContractAgentRuntimePlan{Proof: proof, Blueprints: selected, ConfiguredPlans: configured, Options: options}, selectedContractAgentRuntimeUnsupportedPlanError(configured, "missing selected-fork agent factory/runtime configuration")
	}
	proof.MaterializationSupported = true
	return selectedContractAgentRuntimePlan{Declarations: declarations, Proof: proof, Blueprints: selected, ConfiguredPlans: configured, Options: options, workspaceProjection: workspaceProjection}, nil
}

func prepareSelectedContractDeclarations(loaded LoadedSelectedContractSource, blueprints []runtimemanager.AgentMaterializationBlueprint) (runtimeagenttopology.SelectedDeclarationPlan, error) {
	if err := loaded.RootedSource.Validate(); err != nil {
		return runtimeagenttopology.SelectedDeclarationPlan{}, err
	}
	coordinate, err := runtimeagenttopology.NewSourceCoordinate(loaded.RootedSource.Admission())
	if err != nil {
		return runtimeagenttopology.SelectedDeclarationPlan{}, err
	}
	static, err := runforkreadiness.StaticAgentBlueprints(loaded.Source)
	if err != nil {
		return runtimeagenttopology.SelectedDeclarationPlan{}, err
	}
	actors, err := runforkreadiness.PreparedActorCensus(blueprints)
	if err != nil {
		return runtimeagenttopology.SelectedDeclarationPlan{}, err
	}
	configured := make(map[agentidentity.Plan]runtimemanager.AgentMaterializationBlueprint, len(actors))
	for _, blueprint := range actors {
		configured[blueprint.Identity] = blueprint
	}
	desired := make([]runtimeagenttopology.DesiredAgent, 0, len(static))
	seen := make(map[agentidentity.Plan]struct{}, len(static))
	for _, declaration := range static {
		if _, exists := seen[declaration.Identity]; exists {
			continue
		}
		seen[declaration.Identity] = struct{}{}
		blueprint, exists := configured[declaration.Identity]
		if !exists {
			return runtimeagenttopology.SelectedDeclarationPlan{}, fmt.Errorf("selected declaration %s has no admitted configuration", declaration.Identity.Description())
		}
		revision, err := runtimemanager.AgentConfigPlanRevision(blueprint.Config, blueprint.Identity)
		if err != nil {
			return runtimeagenttopology.SelectedDeclarationPlan{}, err
		}
		desired = append(desired, runtimeagenttopology.DesiredAgent{Identity: blueprint.Identity, ConfigRevision: revision,
			Source: coordinate})
	}
	return runtimeagenttopology.NewSelectedDeclarationPlan(loaded.SourceArtifactFact.BundleHash(), desired)
}

func bindSelectedContractWorkspaceProjection(loaded LoadedSelectedContractSource, options SelectedContractAgentRuntimeOptions) (SelectedContractAgentRuntimeOptions, *selectedContractWorkspaceProjection, error) {
	if options.Workspace == nil {
		return options, nil, nil
	}
	if loaded.RuntimeProjection == nil {
		if err := workspace.RequireSourceProjectionBinding(options.Workspace, loaded.SourceArtifactFact.BundleHash()); err != nil {
			return options, nil, fmt.Errorf("prove selected-contract workspace source projection: %w", err)
		}
		return options, nil, nil
	}
	rebound, err := workspace.RebindSourceProjection(options.Workspace, loaded.RuntimeProjection, loaded.Source)
	if err != nil {
		return options, nil, fmt.Errorf("bind selected-contract workspace source projection: %w", err)
	}
	options.Workspace = rebound
	return options, &selectedContractWorkspaceProjection{lifecycle: rebound}, nil
}

func selectedContractAgentRuntimeUnsupportedPlanError(plans []agentidentity.Plan, reason string) error {
	plans = append([]agentidentity.Plan(nil), plans...)
	sortAgentPlans(plans)
	return fmt.Errorf("%s: %s requires selected-fork handler materialization for authoritative agent recipients before fork mutation; %s for %s",
		runfork.RunForkBlockerSelectedContractAgentHandlerMaterializationUnsupported,
		runfork.RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner,
		strings.TrimSpace(reason),
		strings.Join(agentPlanDescriptions(plans), ","),
	)
}

func selectedContractAgentRuntimeUnsupportedError(agents []agentidentity.Identity, reason string) error {
	agents = append([]agentidentity.Identity(nil), agents...)
	sortAgentIdentities(agents)
	return fmt.Errorf("%s: %s requires selected-fork handler materialization for authoritative agent recipients before fork mutation; %s for %s",
		runfork.RunForkBlockerSelectedContractAgentHandlerMaterializationUnsupported,
		runfork.RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner,
		strings.TrimSpace(reason),
		strings.Join(agentIdentityDescriptions(agents), ","),
	)
}

func sortAgentIdentities(identities []agentidentity.Identity) {
	sort.Slice(identities, func(i, j int) bool {
		return agentidentity.Less(identities[i], identities[j])
	})
}

func agentIdentityDescriptions(identities []agentidentity.Identity) []string {
	out := make([]string, 0, len(identities))
	for _, identity := range identities {
		out = append(out, identity.Description())
	}
	return out
}

func prepareSelectedContractAgentRuntime(ctx context.Context, req publishSelectedContractForkEventsRequest, bus *runtimebus.EventBus, pipeline *runtimepipeline.PipelineCoordinator, diagnostics *selectedForkCommitDiagnostics) (_ *selectedContractAgentRuntime, _ managedexecution.Admission, resultErr error) {
	ports, err := req.Owner.require()
	if err != nil {
		return nil, managedexecution.Admission{}, err
	}
	admission, authority, err := selectedContractManagedExecutionAuthority(ctx)
	if err != nil {
		return nil, managedexecution.Admission{}, err
	}
	ctx = runtimecorrelation.WithRunID(ctx, authority.SelectedFork.ForkRunID)
	if pipeline == nil {
		return nil, managedexecution.Admission{}, fmt.Errorf("selected-contract workflow lifecycle store is required")
	}
	req.Prepared.bindMu.Lock()
	defer req.Prepared.bindMu.Unlock()
	if req.Prepared.closed || req.Prepared.retainedRuntime != nil {
		return nil, managedexecution.Admission{}, errors.New("selected preparation cannot acquire another runtime")
	}
	generationGrant, err := issueSelectedContractAgentRuntimeGenerationGrant(ctx, req, authority)
	if err != nil {
		return nil, managedexecution.Admission{}, err
	}
	runtimeOwner := &selectedContractAgentRuntime{
		generationGrant: generationGrant, workspaceProjection: req.AgentRuntime.workspaceProjection,
		bus: bus, pipeline: pipeline,
	}
	req.Prepared.retainedRuntime = runtimeOwner
	defer func() {
		if panicked := recover(); panicked != nil {
			cleanupErr := runtimeOwner.Shutdown()
			panic(errors.Join(fmt.Errorf("selected runtime startup panicked: %v", panicked), cleanupErr))
		}
		if resultErr != nil {
			resultErr = errors.Join(resultErr, runtimeOwner.Shutdown())
		}
	}()
	contextOwner, ok := generationGrant.(runtimestartupownership.SelectedForkMutationContextOwner)
	if !ok {
		return nil, managedexecution.Admission{}, errors.New("selected runtime grant cannot construct its child operation context")
	}
	if !runtimestartupownership.HasMutationAuthority(ctx) {
		// Supply the preparation's actual retained original only for the new
		// child handoff, never as ambient authority for publication work.
		ctx, err = runtimestartupownership.WithProcessMutationAuthority(ctx, req.Prepared.processOwner)
		if err != nil {
			return nil, managedexecution.Admission{}, err
		}
	}
	ctx, err = contextOwner.BindSelectedForkMutationContext(ctx)
	if err != nil {
		return nil, managedexecution.Admission{}, err
	}
	binding, err := generationGrant.ProcessExecutionBinding()
	if err != nil {
		return nil, managedexecution.Admission{}, err
	}
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, binding.RuntimeInstanceID)
	runtimeOwner.operationContext = ctx
	admitActivations := func() error {
		for _, flow := range req.AgentRuntime.Flows {
			owner, err := runtimeflowidentity.NewRunScopedFlowInstance(authority.SelectedFork.ForkRunID, flow.Instance.Route())
			if err != nil {
				return err
			}
			if err := admitSelectedContractFlowActivation(ctx, pipeline, binding, owner, diagnostics, &runtimeOwner.pendingActivations); err != nil {
				return err
			}
		}
		return nil
	}
	if len(req.AgentRuntime.Records) == 0 && !semanticview.SourceDeclaresAgents(req.LoadedSource.Source) {
		options := selectedContractManagerOptions(runtimemanager.AgentManagerOptions{
			LifecycleDiagnosticOrigin: runtimemanager.LifecycleDiagnosticOrigin{
				Owner: runtimemanager.LifecycleDiagnosticSelectedFork, Causality: runtimemanager.LifecycleDiagnosticObservation,
				SelectedFork: authority.SelectedFork, SourceRunID: req.Admission.SourceRunID,
				ForkPoint: req.Admission.ForkPoint, ForkEventID: req.Admission.ForkPoint.EventID,
			},
			ExecutionPosture:   req.AgentRuntime.Options.ExecutionPosture,
			BaseContext:        context.WithoutCancel(ctx),
			SourceArtifactFact: req.LoadedSource.SourceArtifactFact,
			SemanticSource:     req.LoadedSource.Source,
			WorkflowInstances:  pipeline,
			DeliveryStore:      ports.busDurable.DeliveryLifecycle,
			WorkOwner:          req.AgentRuntime.Options.AgentManagerOptions.WorkOwner,
			ReceiverExecution:  req.AgentRuntime.Options.AgentManagerOptions.ReceiverExecution,
		}, generationGrant, bus, ports, pipeline)
		manager := runtimemanager.NewAgentManagerWithOptions(bus, nil, options, ports.manager)
		runtimeOwner.manager = manager
		if _, err := generationGrant.MarkProbesSettled(ctx, nil); err != nil {
			return nil, managedexecution.Admission{}, err
		}
		admitted, err := generationGrant.AdmitExecution(ctx)
		if err != nil {
			return nil, managedexecution.Admission{}, err
		}
		if err := runtimestartupownership.InstallActivityJournal(ctx, generationGrant, admitted, pipeline); err != nil {
			return nil, managedexecution.Admission{}, fmt.Errorf("install selected activity journal: %w", err)
		}
		if err := admitActivations(); err != nil {
			return nil, managedexecution.Admission{}, err
		}
		return runtimeOwner, admission, nil
	}
	receiverExecution, err := req.AgentRuntime.Options.AgentManagerOptions.ReceiverExecution.WithSelectedAdmission(admission)
	if err != nil {
		return nil, managedexecution.Admission{}, fmt.Errorf("bind selected-contract gateway receiver execution: %w", err)
	}
	req.AgentRuntime.Options.AgentManagerOptions.ReceiverExecution = receiverExecution
	builder, err := buildSelectedContractAgentRuntimeFactory(req, generationGrant, bus, pipeline)
	if err != nil {
		return nil, managedexecution.Admission{}, err
	}
	runtimeOwner.cleanup = builder.cleanup
	if builder.runtimes != nil {
		actual, err := swaruntime.PrepareSelectedForkProviderCatalog(ctx, builder.runtimes, builder.tools, req.AgentRuntime.Blueprints)
		if err != nil || actual.Fingerprint() != req.Prepared.catalog.Fingerprint() {
			return nil, managedexecution.Admission{}, errors.Join(errors.New("selected execution provider catalog differs from preparation"), err)
		}
	}
	builder.options.BaseContext = context.WithoutCancel(ctx)
	builder.options.LifecycleDiagnosticOrigin = runtimemanager.LifecycleDiagnosticOrigin{
		Owner: runtimemanager.LifecycleDiagnosticSelectedFork, Causality: runtimemanager.LifecycleDiagnosticObservation,
		SelectedFork: authority.SelectedFork, SourceRunID: req.Admission.SourceRunID,
		ForkPoint: req.Admission.ForkPoint, ForkEventID: req.Admission.ForkPoint.EventID,
	}
	builder.options.DeliveryStore = ports.busDurable.DeliveryLifecycle
	manager := runtimemanager.NewAgentManagerWithOptions(bus, builder.factory, builder.options, ports.manager)
	runtimeOwner.manager = manager
	if builder.bindManager != nil {
		builder.bindManager(manager)
	}
	if _, err := generationGrant.MarkProbesSettled(ctx, admission.CapabilitySurfaceIDs); err != nil {
		return nil, managedexecution.Admission{}, fmt.Errorf("settle selected-contract runtime generation probes: %w", err)
	}
	admitted, err := generationGrant.AdmitExecution(ctx)
	if err != nil {
		return nil, managedexecution.Admission{}, fmt.Errorf("admit selected-contract runtime generation: %w", err)
	}
	if err := runtimestartupownership.InstallActivityJournal(ctx, generationGrant, admitted, pipeline); err != nil {
		return nil, managedexecution.Admission{}, fmt.Errorf("install selected activity journal: %w", err)
	}
	if err := admitActivations(); err != nil {
		return nil, managedexecution.Admission{}, err
	}
	for recordIndex, rec := range req.AgentRuntime.Records {
		if rec.Topology.Authority.Kind == runtimeagenttopology.AuthorityFlowReadinessPlan {
			identity, err := rec.Config.ConcreteIdentity()
			if err != nil {
				return nil, managedexecution.Admission{}, err
			}
			matched := false
			for _, activation := range runtimeOwner.pendingActivations {
				if identity.RunID != activation.attempt.RunID() || identity.FlowInstance() != activation.attempt.InstancePath() {
					continue
				}
				rec.Topology, err = rec.Topology.WithFlowActivationAttempt(activation.attempt.ID())
				if err != nil {
					return nil, managedexecution.Admission{}, err
				}
				matched = true
				break
			}
			if !matched {
				return nil, managedexecution.Admission{}, errors.New("selected flow agent has no admitted activation attempt")
			}
			if req.Prepared.recoveryFromExecutionID != "" {
				continue
			}
		}
		var materializeErr error
		if req.Prepared.recoveryFromExecutionID != "" && rec.Topology.Authority.Kind == runtimeagenttopology.AuthoritySelectedForkDeclarationPlan {
			materializeErr = manager.MaterializeSelectedDeclarationAgentForRecovery(ctx, rec)
		} else {
			materializeErr = manager.MaterializeAdmittedAgent(ctx, rec)
		}
		if err := materializeErr; err != nil {
			return nil, managedexecution.Admission{}, fmt.Errorf("%s materialize agent %s (record %d/%d, authority %s): %w", runfork.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner, strings.TrimSpace(rec.Config.ID), recordIndex+1, len(req.AgentRuntime.Records), rec.Topology.Authority.Kind, err)
		}
		identity, err := rec.Config.ConcreteIdentity()
		if err != nil {
			return nil, managedexecution.Admission{}, fmt.Errorf("%s concrete agent identity: %w", runfork.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner, err)
		}
		bus.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{
			Identity: identity,
			EntityID: rec.Config.EffectiveEntityID(),
		})
	}
	receiverExecution, err = builder.options.ReceiverExecution.WithSelectedAdmission(admission)
	if err != nil {
		return nil, managedexecution.Admission{}, fmt.Errorf("finalize selected-contract manager receiver execution: %w", err)
	}
	if err := manager.SetReceiverExecution(receiverExecution); err != nil {
		return nil, managedexecution.Admission{}, fmt.Errorf("install selected-contract manager receiver execution: %w", err)
	}
	return runtimeOwner, admission, nil
}

func (r *selectedContractAgentRuntime) startExecution(ctx context.Context, pipeline *runtimepipeline.PipelineCoordinator, diagnostics *selectedForkCommitDiagnostics, recoverTopology bool) error {
	if r == nil || r.manager == nil {
		return errors.New("selected execution requires its prepared lifecycle manager")
	}
	var startup runtimemanager.DynamicFlowRuntimeStartupReadiness
	topologyCtx := ctx
	if recoverTopology {
		var err error
		topologyCtx, err = r.bus.AdmitSourceArtifactFact(ctx)
		if err != nil {
			return fmt.Errorf("bind recovered selected readiness source: %w", err)
		}
		source, present := runtimecorrelation.SourceArtifactFactFromContext(topologyCtx)
		if !present {
			return errors.New("recovered selected readiness lacks its admitted source")
		}
		startup, err = r.manager.CanonicalizeDynamicFlowRuntimeStartupReadiness(topologyCtx, source, true)
		if err != nil {
			return fmt.Errorf("admit recovered selected child readiness: %w", err)
		}
	}
	if err := completeSelectedContractFlowActivations(ctx, pipeline, r.pendingActivations, diagnostics); err != nil {
		return err
	}
	for len(r.pendingActivations) > 0 {
		activation := r.pendingActivations[0]
		if err := r.manager.AdoptSelectedFlowActivation(ctx, activation.identity, activation.attempt, activation.timersProjected); err != nil {
			return err
		}
		r.pendingActivations = r.pendingActivations[1:]
	}
	if err := r.manager.RunAuthoritativeDeliveryOnly(ctx); err != nil {
		return err
	}
	if recoverTopology {
		if err := r.manager.CompleteDynamicFlowRuntimeStartupTopology(topologyCtx, startup); err != nil {
			return fmt.Errorf("restore recovered selected child topology: %w", err)
		}
	}
	return nil
}

func issueSelectedContractAgentRuntimeGenerationGrant(
	ctx context.Context,
	req publishSelectedContractForkEventsRequest,
	authority runtimeeffects.Authority,
) (runtimestartupownership.GenerationGrant, error) {
	if req.Prepared == nil || !req.Prepared.bound {
		return nil, errors.New("selected generation grant requires bound preparation")
	}
	if req.Prepared.operation == nil || req.Prepared.operation.grantIssuance == nil {
		return nil, errors.New("selected generation grant requires its original operation context")
	}
	capability := req.AgentRuntime.Options.ProcessCapability
	if capability == nil {
		return nil, errors.New("selected-contract agent runtime requires the process topology capability")
	}
	processAuthority, err := capability.Evidence()
	if err != nil {
		return nil, fmt.Errorf("load selected-contract process authority: %w", err)
	}
	bundleHash := req.LoadedSource.SourceArtifactFact.BundleHash()
	ports, err := req.Owner.require()
	if err != nil {
		return nil, err
	}
	binding, err := ports.fork.RequireRunForkSelectedContractBinding(ctx, authority.SelectedFork.ForkRunID)
	if err != nil {
		return nil, err
	}
	grant, err := capability.IssueSelectedForkGenerationGrant(req.Prepared.operation.grantIssuance, runtimestartupownership.SelectedForkGrantRequest{
		BundleHash:        bundleHash,
		RuntimeInstanceID: processAuthority.RuntimeInstanceID,
		Binding: runtimestartupownership.SelectedForkGrantBinding{
			BindingID: binding.BindingID, ForkRunID: authority.SelectedFork.ForkRunID,
			ExecutionID: authority.SelectedFork.ExecutionID, ExecutionGeneration: authority.SelectedFork.Generation,
			ExecutionOwner: authority.ExecutionOwner, FenceGeneration: authority.FenceGeneration,
			AdmissionFingerprint:       authority.SelectedFork.AdmissionFingerprint,
			ContainerPlanFingerprint:   authority.SelectedFork.ContainerPlanFingerprint,
			ActorCensusFingerprint:     authority.SelectedFork.ActorCensusFingerprint,
			EffectiveConfigFingerprint: authority.SelectedFork.EffectiveConfigFingerprint,
			DeclarationPlanFingerprint: req.AgentRuntime.Declarations.Revision,
			PreparationFingerprint:     req.Prepared.bindingFingerprint,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("issue selected-contract runtime generation grant: %w", err)
	}
	return grant, nil
}

func selectedContractManagedExecutionAuthority(ctx context.Context) (managedexecution.Admission, runtimeeffects.Authority, error) {
	admission, ok := managedexecution.FromContext(ctx)
	if !ok || admission.Kind != managedexecution.KindSelectedContractFork {
		return managedexecution.Admission{}, runtimeeffects.Authority{}, fmt.Errorf("selected-fork managed execution admission is required")
	}
	authority, ok := runtimeeffects.AuthorityFromContext(ctx)
	if !ok || authority.Kind != runtimeeffects.AuthoritySelectedContractFork || !authority.Valid() {
		return managedexecution.Admission{}, runtimeeffects.Authority{}, fmt.Errorf("selected-fork effect authority is required")
	}
	if !admission.AuthorizesSelected(authority.SelectedFork.ExecutionID, authority.SelectedFork.ForkRunID, authority.SelectedFork.Generation) {
		return managedexecution.Admission{}, runtimeeffects.Authority{}, fmt.Errorf("selected-fork managed execution admission does not match effect authority")
	}
	return admission, authority, nil
}

func buildSelectedContractAgentRuntimeFactory(req publishSelectedContractForkEventsRequest, lifecycle runtimemanager.AgentLifecyclePersistence, bus *runtimebus.EventBus, pipeline *runtimepipeline.PipelineCoordinator) (selectedContractAgentRuntimeFactory, error) {
	ports, err := req.Owner.require()
	if err != nil {
		return selectedContractAgentRuntimeFactory{}, err
	}
	if pipeline == nil {
		return selectedContractAgentRuntimeFactory{}, fmt.Errorf("selected-contract workflow lifecycle store is required")
	}
	options := req.AgentRuntime.Options
	source := req.LoadedSource.Source
	managerOptions, backendProfile, err := selectedContractAgentModelOptions(options)
	if err != nil {
		return selectedContractAgentRuntimeFactory{}, err
	}
	if managerOptions.SemanticSource == nil {
		managerOptions.SemanticSource = source
	}
	managerOptions.SourceArtifactFact = req.LoadedSource.SourceArtifactFact
	managerOptions.WorkflowInstances = pipeline
	managerOptions = selectedContractManagerOptions(managerOptions, lifecycle, bus, ports, pipeline)
	if managerOptions.Sessions == nil {
		managerOptions.Sessions = options.SessionRegistry
	}
	if managerOptions.Workspaces == nil {
		managerOptions.Workspaces = options.Workspace
	}
	budget := swaruntime.NewBudgetTracker(ports.budget, bus, options.Config, options.MailboxStore, nil, source, options.ExecutionPosture, swaruntime.BudgetRecoveryStageSources{})
	managerOptions.Budget = budget
	if options.AgentFactory != nil {
		return selectedContractAgentRuntimeFactory{factory: options.AgentFactory, options: managerOptions}, nil
	}
	if options.Config == nil {
		return selectedContractAgentRuntimeFactory{}, selectedContractAgentRuntimeUnsupportedError(req.AgentRuntime.Proof.AgentRecipients, "missing selected-fork agent factory/runtime configuration")
	}

	mcpTurns := runtimemcp.NewTurnContextRegistry(runtimeactors.ActorFromContext)
	var managerRef *runtimemanager.AgentManager
	exec := newSelectedContractToolExecutor(options, source, bus, pipeline, func() runtimetools.Manager {
		return managerRef
	})
	if managerOptions.WorkOwner == nil {
		return selectedContractAgentRuntimeFactory{}, errors.New("selected-fork gateway requires work occurrence")
	}
	gatewayWork, err := managerOptions.WorkOwner.BeginStanding(context.Background())
	if err != nil {
		return selectedContractAgentRuntimeFactory{}, err
	}
	custody, err := selectedContractGatewayCustody(req, lifecycle, managerOptions)
	if err != nil {
		_ = gatewayWork.Done()
		return selectedContractAgentRuntimeFactory{}, err
	}
	binding, cleanup, err := startSelectedContractAgentRuntimeGateway(exec, mcpTurns, gatewayWork, func(identity agentidentity.Identity) (runtimeactors.AgentConfig, bool) {
		if managerRef == nil {
			return runtimeactors.AgentConfig{}, false
		}
		cfg, err := managerRef.ResolveAgentConfig(identity.RunID, identity.AgentID(), identity.FlowInstance())
		return cfg, err == nil
	}, func(token runtimeeffects.LifecycleToken) error {
		if managerRef == nil {
			return errors.New("selected activation has no lifecycle owner")
		}
		return managerRef.ProveUnpublishedActivation(token)
	}, custody)
	if err != nil {
		return selectedContractAgentRuntimeFactory{}, fmt.Errorf("start selected-fork tool gateway: %w", err)
	}
	runtimes, err := runtimellm.NewAgentRuntimeSet(backendProfile, runtimellm.RuntimeFactory{
		Cfg:                  options.Config,
		Sessions:             options.SessionRegistry,
		LiveSessions:         ports.liveSessions,
		Conversations:        options.ConversationStore,
		Workspaces:           options.Workspace,
		Events:               bus,
		MCPTurns:             mcpTurns,
		ToolGateway:          binding,
		Credentials:          options.ProviderCredentials,
		CompletionController: runtimeeffects.NewCompletionController(ports.effects, ports.completion, ports.completionHeartbeat, budget).WithExecutionPosture(managerOptions.ExecutionPosture),
	}, options.LLMRuntime)
	if err != nil {
		if cleanup != nil {
			cleanup()
		}
		return selectedContractAgentRuntimeFactory{}, fmt.Errorf("build selected-fork agent runtime resolver: %w", err)
	}
	exec.SetModelRuntimes(runtimes)
	managerOptions.WorkspaceGatewayAdmission = func(ctx context.Context, actor runtimeactors.AgentConfig) error {
		return observeSelectedActivationActor(ctx, req, managerRef, runtimes, exec, binding, mcpTurns, actor)
	}
	factory := runtimeagents.NewLLMAgentFactory(runtimes, exec, runtimeagents.LLMAgentOptions{})
	return selectedContractAgentRuntimeFactory{
		factory: factory,
		options: managerOptions,
		bindManager: func(manager *runtimemanager.AgentManager) {
			managerRef = manager
		},
		cleanup:  cleanup,
		runtimes: runtimes,
		tools:    exec,
	}, nil
}

func newSelectedContractToolExecutor(options SelectedContractAgentRuntimeOptions, source semanticview.Source, bus *runtimebus.EventBus, pipeline *runtimepipeline.PipelineCoordinator, managerProvider func() runtimetools.Manager) *runtimetools.Executor {
	authority := runtimeauthority.NewSourceProvider(source)
	credentials := options.Credentials
	if credentials == nil {
		credentials = runtimecredentials.NewEnvStore()
	}
	return runtimetools.NewExecutorWithOptions(bus, runtimetools.ExecutorOptions{
		Config:             options.Config,
		Credentials:        credentials,
		ManagedCredentials: options.ManagedCredentials,
		MailboxStore:       options.MailboxStore,
		NoticePresentation: options.NoticePresentation,
		MCPClient:          options.MCPClient,
		EntityStore:        options.EntityStore,
		EntityWriter:       pipeline,
		HumanTaskStore:     options.HumanTaskStore,
		WorkflowInstances:  pipeline,
		WorkflowSource:     source,
		WorkspaceResolver:  options.Workspace,
		AuthorityProvider:  authority,
		EmitRegistry:       runtimetools.NewEmitRegistry(source, authority),
		ManagerProvider:    managerProvider,
	})
}

func startSelectedContractAgentRuntimeGateway(exec *runtimetools.Executor, mcpTurns *runtimemcp.TurnContextRegistry, lease *worklifetime.Lease, resolveActorConfig func(agentidentity.Identity) (runtimeactors.AgentConfig, bool), validateActivation func(runtimeeffects.LifecycleToken) error, custody *selectedGatewayCustody) (_ toolgateway.Binding, _ func(), finalErr error) {
	if lease == nil {
		return toolgateway.Binding{}, nil, errors.New("selected-fork gateway requires admitted work")
	}
	transferred := false
	defer func() {
		if !transferred {
			finalErr = errors.Join(finalErr, lease.Done())
		}
	}()
	if err := lease.Context().Err(); err != nil {
		return toolgateway.Binding{}, nil, err
	}
	if exec == nil {
		return toolgateway.Binding{}, nil, errors.New("selected-fork gateway requires tool executor")
	}

	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return toolgateway.Binding{}, nil, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	hostURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	containerURL := fmt.Sprintf("http://host.docker.internal:%d", port)
	gatewayToken, err := toolgateway.GenerateAuthToken()
	if err != nil {
		_ = ln.Close()
		return toolgateway.Binding{}, nil, fmt.Errorf("generate selected-fork tool gateway token: %w", err)
	}
	binding, err := toolgateway.NewRuntimeOwnedBinding(
		toolgateway.TransportHTTP,
		hostURL,
		containerURL,
		gatewayToken,
		toolgateway.LifecycleOwnerSelectedForkRuntime,
		toolgateway.SourceSelectedForkEphemeralGateway,
	)
	if err != nil {
		_ = ln.Close()
		return toolgateway.Binding{}, nil, err
	}

	hooks := swaruntime.RuntimeMCPGatewayHooks(nil, nil, resolveActorConfig, nil, mcpTurns)
	hooks.ValidateActivationProbe = validateActivation
	if custody != nil {
		hooks = custody.hooks(hooks)
	}
	gateway := runtimemcp.NewGateway(exec, binding.AuthToken(), hooks)
	server := serveSelectedContractGateway(ln, gateway.Handler(), lease)
	transferred = true
	return binding, server.Close, nil
}

type selectedGatewayCustody struct {
	grant       runtimestartupownership.GenerationGrant
	admission   managedexecution.Admission
	preparation runfork.SelectedForkPreparation
}

func selectedContractGatewayCustody(req publishSelectedContractForkEventsRequest, lifecycle runtimemanager.AgentLifecyclePersistence, options runtimemanager.AgentManagerOptions) (*selectedGatewayCustody, error) {
	if req.Prepared == nil {
		return nil, nil
	}
	grant, ok := lifecycle.(runtimestartupownership.GenerationGrant)
	if !ok {
		return nil, errors.New("selected gateway requires its retained native child grant")
	}
	bound, err := options.ReceiverExecution.Bind(context.Background(), req.AgentRuntime.Options.ExecutionPosture.RootMode())
	if err != nil {
		return nil, err
	}
	admission, ok := managedexecution.FromContext(bound)
	if !ok {
		return nil, errors.New("selected gateway requires its retained child admission")
	}
	return &selectedGatewayCustody{grant: grant, admission: admission, preparation: req.Prepared.evidence()}, nil
}

func (c *selectedGatewayCustody) hooks(hooks runtimemcp.GatewayHooks) runtimemcp.GatewayHooks {
	resolve := hooks.ResolveTurnContext
	hooks.ResolveTurnContext = func(token string) (runtimemcp.TurnContext, bool) {
		turn, ok := resolve(token)
		if !ok || !c.matchesTurn(turn, hooks) {
			return runtimemcp.TurnContext{}, false
		}
		return turn, true
	}
	epoch := hooks.WithCurrentRuntimeEpoch
	hooks.WithCurrentRuntimeEpoch = func(ctx context.Context) context.Context {
		ctx = epoch(ctx)
		if err := c.grant.ProveCurrent(ctx); err != nil {
			return refusedSelectedGatewayContext(ctx, err)
		}
		binder, ok := c.grant.(interface {
			BindRuntimeMutationContext(context.Context) (context.Context, error)
		})
		if !ok {
			return refusedSelectedGatewayContext(ctx, errors.New("selected gateway grant cannot forward its original custody"))
		}
		bound, err := binder.BindRuntimeMutationContext(ctx)
		if err != nil {
			return refusedSelectedGatewayContext(ctx, err)
		}
		owner, ok := c.grant.(runtimestartupownership.SelectedForkMutationContextOwner)
		if !ok {
			return refusedSelectedGatewayContext(ctx, errors.New("selected gateway grant cannot bind its retained child"))
		}
		bound, err = owner.BindSelectedForkMutationContext(bound)
		if err != nil {
			return refusedSelectedGatewayContext(ctx, err)
		}
		return bound
	}
	return hooks
}

func refusedSelectedGatewayContext(ctx context.Context, err error) context.Context {
	refused, _ := runtimestartupownership.WithRuntimeMutationAuthority(ctx, nil)
	failed, cancel := context.WithCancelCause(refused)
	cancel(err)
	return failed
}

func (c *selectedGatewayCustody) matchesTurn(turn runtimemcp.TurnContext, hooks runtimemcp.GatewayHooks) bool {
	evidence, err := c.grant.Evidence()
	if err != nil || evidence.SelectedFork == nil || turn.CapabilitySurface == nil ||
		!turn.HasSourceArtifactFact || turn.SourceArtifactFact.BundleHash() != evidence.BundleHash ||
		!turn.HasAuthorActivityScope || turn.AuthorActivityScope != runtimeauthoractivity.BundleScope(evidence.RuntimeInstanceID, evidence.BundleHash) ||
		turn.RunID != evidence.SelectedFork.ForkRunID || !turn.HasEffectAuthority || !turn.HasExecutionAdmission {
		return false
	}
	admission := turn.ExecutionAdmission
	if admission.Validate() != nil || admission.ID != c.admission.ID ||
		!admission.AuthorizesSelected(evidence.SelectedFork.ExecutionID, turn.RunID, evidence.SelectedFork.ExecutionGeneration) ||
		admission.ActorCensusFingerprint != evidence.SelectedFork.ActorCensusFingerprint || admission.BundleHash != evidence.BundleHash {
		return false
	}
	if !selectedGatewayEffectMatches(turn.EffectAuthority, *evidence.SelectedFork) {
		return false
	}
	return c.matchesTurnActor(turn, evidence, hooks)
}

func selectedGatewayEffectMatches(authority runtimeeffects.Authority, binding runtimestartupownership.SelectedForkGrantBinding) bool {
	selected := authority.SelectedFork
	return authority.Valid() && authority.Kind == runtimeeffects.AuthoritySelectedContractFork &&
		authority.ID == binding.ExecutionID && authority.ExecutionOwner == binding.ExecutionOwner && authority.FenceGeneration == binding.FenceGeneration &&
		selected.ForkRunID == binding.ForkRunID && selected.Generation == binding.ExecutionGeneration &&
		selected.AdmissionFingerprint == binding.AdmissionFingerprint && selected.ContainerPlanFingerprint == binding.ContainerPlanFingerprint &&
		selected.ActorCensusFingerprint == binding.ActorCensusFingerprint && selected.EffectiveConfigFingerprint == binding.EffectiveConfigFingerprint
}

func (c *selectedGatewayCustody) matchesTurnActor(turn runtimemcp.TurnContext, evidence runtimestartupownership.GrantEvidence, hooks runtimemcp.GatewayHooks) bool {
	surface := turn.CapabilitySurface
	if surface.Validate() != nil {
		return false
	}
	identity := turn.Actor.Identity
	if surface.Authority.Kind == managedcapabilities.AuthorityStartupProbe {
		if !turn.HasLifecycleToken || !turn.LifecycleToken.Valid() || surface.Authority.ExecutionKind != managedcapabilities.ExecutionSelectedForkPreparation ||
			surface.Authority.Preparation == nil {
			return false
		}
		identity = turn.LifecycleToken.Identity
	} else if surface.Authority.Kind != managedcapabilities.AuthorityProviderTurn || evidence.State != runtimestartupownership.GrantAdmitted ||
		surface.Authority.ExecutionKind != managedcapabilities.ExecutionSelectedContractFork ||
		surface.Authority.ExecutionAuthorityID != evidence.SelectedFork.ExecutionID || surface.Authority.RunID != turn.RunID ||
		!runtimeeffects.ProviderTurnTargetMatchesCapabilitySurface(turn.EffectAuthority.Target, *surface) {
		return false
	}
	if identity.Validate() != nil || identity.RunID != turn.RunID || identity.AgentID() != turn.Actor.ID {
		return false
	}
	plan, err := identity.Plan()
	if err != nil {
		return false
	}
	if surface.ActorPlan != plan && surface.ActorIdentity != identity {
		return false
	}
	for _, actor := range c.preparation.Actors {
		if actor.Plan == plan {
			if surface.Authority.Kind == managedcapabilities.AuthorityStartupProbe &&
				(surface.Authority.ExecutionAuthorityID != c.preparation.PreparationID || surface.Authority.Preparation.SelectedForkPreparationCoordinates != c.preparation.Coordinates) {
				return false
			}
			return actor.Mode == turn.Actor.ExecutionMode
		}
	}
	return c.matchesProspectiveTurnActor(turn, identity, hooks)
}

func (c *selectedGatewayCustody) matchesProspectiveTurnActor(turn runtimemcp.TurnContext, identity agentidentity.Identity, hooks runtimemcp.GatewayHooks) bool {
	if turn.CapabilitySurface.Authority.Kind == managedcapabilities.AuthorityStartupProbe {
		// The registered surface belongs to the prospective catalog already
		// checked by observeSelectedActivationActor, not the initial actor census.
		coordinates := turn.CapabilitySurface.Authority.Preparation.SelectedForkPreparationCoordinates
		coordinates.AdmittedPlanFingerprint = c.preparation.Coordinates.AdmittedPlanFingerprint
		coordinates.CatalogFingerprint = c.preparation.Coordinates.CatalogFingerprint
		return coordinates == c.preparation.Coordinates && hooks.ValidateActivationProbe != nil && hooks.ValidateActivationProbe(turn.LifecycleToken) == nil
	}
	if hooks.ResolveActorConfig == nil {
		return false
	}
	actor, ok := hooks.ResolveActorConfig(identity)
	return ok && actor.Identity == identity && actor.ExecutionMode == turn.Actor.ExecutionMode
}

type selectedContractGatewayServer struct {
	server    *http.Server
	stopped   chan struct{}
	lease     *worklifetime.Lease
	closeOnce sync.Once
}

func serveSelectedContractGateway(listener net.Listener, handler http.Handler, lease *worklifetime.Lease) *selectedContractGatewayServer {
	server := &selectedContractGatewayServer{server: &http.Server{Handler: handler}, stopped: make(chan struct{}), lease: lease}
	go func() {
		defer close(server.stopped)
		// Even on a listener failure, Shutdown must retain accepted connections
		// until their handlers return. Close would erase that join accounting.
		_ = server.server.Serve(listener)
	}()
	return server
}

func (s *selectedContractGatewayServer) Close() {
	s.closeOnce.Do(func() {
		// Serve returning does not join accepted HTTP handlers. The owner
		// releases its work only after Shutdown and the serving loop join.
		defer func() { _ = s.lease.Done() }()
		_ = s.server.Shutdown(context.Background())
		<-s.stopped
	})
}

func (r *selectedContractAgentRuntime) Shutdown() (result error) {
	if r == nil {
		return nil
	}
	r.shutdownMu.Lock()
	defer r.shutdownMu.Unlock()
	defer func() {
		if panicked := recover(); panicked != nil {
			result = errors.Join(result, fmt.Errorf("selected runtime cleanup panicked: %v", panicked))
		}
	}()
	r.mu.Lock()
	cancel, done := r.cancelExecution, r.executionDone
	r.cancelExecution = nil
	r.controlContext = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel(worklifetime.ErrRetired)
	}
	if done != nil {
		// Serving reports its business failure through runtime diagnostics.
		// Receiving it here proves the join, not a failed resource release.
		<-done
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.executionDone = nil
	if err := r.stopExecutionWork(); err != nil {
		return errors.Join(result, err)
	}
	if r.executionLease != nil {
		result = errors.Join(result, r.executionLease.Done())
		r.executionLease = nil
	}
	if r.cleanup != nil {
		r.cleanup()
		r.cleanup = nil
	}
	if r.generationGrant != nil {
		if err := r.generationGrant.Retire(context.Background()); err != nil {
			return errors.Join(result, err)
		}
		r.generationGrant = nil
	}
	return errors.Join(result, r.workspaceProjection.Release())
}

func (r *selectedContractAgentRuntime) stopExecutionWork() error {
	if r.fanOutServing != nil {
		r.fanOutServing.Close()
		r.fanOutServing = nil
	}
	if r.timerLifecycle != nil {
		if err := r.timerLifecycle.StopWorkflowTimerLifecycle(context.Background()); err != nil {
			return err
		}
		r.timerLifecycle = nil
	}
	if r.genericSchedules != nil {
		if err := r.genericSchedules.Stop(context.Background()); err != nil {
			return err
		}
		r.genericSchedules = nil
	}
	if r.scheduler != nil {
		r.scheduler.Stop()
		if err := r.scheduler.Wait(context.Background()); err != nil {
			return err
		}
		r.scheduler = nil
	}
	if r.manager != nil {
		if err := r.manager.Shutdown(); err != nil {
			return err
		}
		r.manager = nil
	}
	if r.completionExecutor != nil {
		retireErr := r.completionExecutor.Retire(context.Background())
		joined := r.completionExecutor.Wait(context.Background())
		if !runlifecycle.CompletionJoinSucceeded(joined) {
			return errors.Join(retireErr, joined)
		}
		r.completionExecutor = nil
		if r.completionDiagnostics != nil {
			r.completionDiagnostics.add(joined)
		}
	}
	if r.completionRegistration != nil {
		r.completionRegistration.Release()
		r.completionRegistration = nil
	}
	if len(r.pendingActivations) != 0 {
		var err error
		r.pendingActivations, err = retireSelectedFlowActivations(context.WithoutCancel(r.operationContext), r.pipeline, r.pendingActivations)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *selectedContractAgentRuntime) WaitForQuiescence(ctx context.Context, bus *runtimebus.EventBus) error {
	if r == nil {
		return nil
	}
	if r.manager != nil {
		if err := r.manager.WaitForQuiescence(ctx); err != nil {
			return err
		}
	}
	if bus != nil {
		return bus.WaitForQuiescence(ctx)
	}
	return nil
}
