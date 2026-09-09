package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/config"
	swaruntime "github.com/division-sh/swarm/internal/runtime"
	runtimeagents "github.com/division-sh/swarm/internal/runtime/agents"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
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
	Declarations        runtimeagenttopology.SelectedDeclarationPlan
	Proof               SelectedContractAgentRuntimeMaterialization
	Blueprints          []runtimemanager.AgentMaterializationBlueprint
	Flows               []runtimemanager.TemplateFlowMaterializationPlan
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
	manager             *runtimemanager.AgentManager
	generationGrant     runtimestartupownership.GenerationGrant
	cleanup             func()
	workspaceProjection *selectedContractWorkspaceProjection
}

type selectedContractWorkspaceProjection struct {
	lifecycle workspace.Lifecycle
	mu        sync.Mutex
	released  bool
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
	bindManager func(runtimetools.Manager)
	cleanup     func()
	runtimes    *runtimellm.AgentRuntimeSet
	tools       *runtimetools.Executor
}

func selectedContractManagerOptions(options runtimemanager.AgentManagerOptions, lifecycle runtimemanager.AgentLifecyclePersistence, bus *runtimebus.EventBus, ports *selectedContractExecutionPorts, pipeline *runtimepipeline.PipelineCoordinator) runtimemanager.AgentManagerOptions {
	roles := ports.managerRoles
	options.LifecycleStore = lifecycle
	roles.AgentRoutes = bus
	roles.FlowActivation = bus
	roles.RouteInstaller = bus
	roles.RouteVerifier = bus
	roles.RouteRestorer = bus
	roles.RouteRetirer = bus
	roles.RouteRemover = bus
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
	declarations, err := prepareSelectedContractDeclarations(loaded, blueprints)
	if err != nil {
		return selectedContractAgentRuntimePlan{}, err
	}
	proof := SelectedContractAgentRuntimeMaterialization{
		Owner:                    runfork.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner,
		RecipientPlanningOwner:   planning.Owner,
		ExecutionOwner:           runfork.RunForkSelectedContractExecutionOwner,
		AgentRecipientPlans:      append([]agentidentity.Plan(nil), agentPlans...),
		MaterializationRequired:  len(agentPlans) > 0,
		MaterializationSupported: len(agentPlans) == 0,
		EphemeralForkLocal:       true,
	}
	if len(agentPlans) == 0 {
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
	configured := make([]agentidentity.Plan, 0, len(blueprints))
	for _, blueprint := range blueprints {
		plan := blueprint.Identity.Normalize()
		if err := plan.Validate(); err != nil {
			return selectedContractAgentRuntimePlan{Proof: proof, Options: options}, fmt.Errorf("selected-contract agent declaration plan: %w", err)
		}
		if _, exists := blueprintsByPlan[plan]; exists {
			continue
		}
		blueprint.Status = "ephemeral"
		blueprint.HiredBy = "selected-contract-fork-agent-runtime"
		blueprintsByPlan[plan] = blueprint
		configured = append(configured, plan)
	}
	sortAgentPlans(configured)
	proof.ConfiguredAgentPlans = append([]agentidentity.Plan(nil), configured...)

	selected := make([]runtimemanager.AgentMaterializationBlueprint, 0, len(agentPlans))
	missing := []agentidentity.Plan{}
	for _, plan := range agentPlans {
		blueprint, ok := blueprintsByPlan[plan.Normalize()]
		if !ok {
			missing = append(missing, plan)
			continue
		}
		selected = append(selected, blueprint)
	}
	if len(missing) > 0 {
		sortAgentPlans(missing)
		proof.MissingAgentRecipientPlans = missing
		return selectedContractAgentRuntimePlan{Proof: proof, Blueprints: selected, ConfiguredPlans: configured, Options: options}, selectedContractAgentRuntimeUnsupportedPlanError(missing, "missing selected-source declaration-owned agent materialization blueprint")
	}
	if options.AgentFactory == nil && options.Config == nil {
		return selectedContractAgentRuntimePlan{Proof: proof, Blueprints: selected, ConfiguredPlans: configured, Options: options}, selectedContractAgentRuntimeUnsupportedPlanError(agentPlans, "missing selected-fork agent factory/runtime configuration")
	}
	proof.MaterializationSupported = true
	return selectedContractAgentRuntimePlan{Declarations: declarations, Proof: proof, Blueprints: selected, ConfiguredPlans: configured, Options: options, workspaceProjection: workspaceProjection}, nil
}

func prepareSelectedContractDeclarations(loaded LoadedSelectedContractSource, blueprints []runtimemanager.AgentMaterializationBlueprint) (runtimeagenttopology.SelectedDeclarationPlan, error) {
	static, err := runforkreadiness.StaticAgentBlueprints(loaded.Source)
	if err != nil {
		return runtimeagenttopology.SelectedDeclarationPlan{}, err
	}
	configured := make(map[agentidentity.Plan]runtimemanager.AgentMaterializationBlueprint, len(blueprints))
	for _, blueprint := range blueprints {
		if previous, exists := configured[blueprint.Identity]; exists {
			left, err := runtimemanager.AgentConfigPlanRevision(previous.Config, previous.Identity)
			if err != nil {
				return runtimeagenttopology.SelectedDeclarationPlan{}, err
			}
			right, err := runtimemanager.AgentConfigPlanRevision(blueprint.Config, blueprint.Identity)
			if err != nil || left != right {
				return runtimeagenttopology.SelectedDeclarationPlan{}, errors.New("selected declaration repeats a conflicting configuration")
			}
		}
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
			Source: runtimeagenttopology.SourceCoordinate{BundleHash: loaded.SourceArtifactFact.BundleHash()}})
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

func selectedContractStaticAgentRecords(runID string, source semanticview.Source) ([]runtimemanager.PersistedAgent, error) {
	blueprints, err := runforkreadiness.StaticAgentBlueprints(source)
	if err != nil {
		return nil, err
	}
	records := make([]runtimemanager.PersistedAgent, 0, len(blueprints))
	for _, blueprint := range blueprints {
		record, err := blueprint.Materialize(runID)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
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

func startSelectedContractAgentRuntime(ctx context.Context, req publishSelectedContractForkEventsRequest, bus *runtimebus.EventBus, pipeline *runtimepipeline.PipelineCoordinator) (_ *selectedContractAgentRuntime, _ managedexecution.Admission, resultErr error) {
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
	generationGrant, err := issueSelectedContractAgentRuntimeGenerationGrant(ctx, req, authority)
	if err != nil {
		return nil, managedexecution.Admission{}, err
	}
	grantOwned := true
	defer func() {
		if grantOwned {
			resultErr = errors.Join(resultErr, generationGrant.Retire(context.Background()))
		}
	}()
	var published []runtimeflowidentity.RunScopedFlowInstance
	defer func() {
		if resultErr != nil {
			for _, owner := range published {
				resultErr = errors.Join(resultErr, bus.RetirePublishedFlowInstanceRoute(owner))
			}
		}
	}()
	for _, flow := range req.AgentRuntime.Flows {
		owner, err := runtimeflowidentity.NewRunScopedFlowInstance(authority.SelectedFork.ForkRunID, flow.Instance.Route())
		if err != nil {
			return nil, managedexecution.Admission{}, err
		}
		route := runtimebus.FlowInstanceRouteMaterializationRequest{Identity: owner, ActivationVariables: flow.ActivationVariables}
		if err := bus.StageFlowInstanceRouteContext(ctx, route); err != nil {
			return nil, managedexecution.Admission{}, err
		}
		published = append(published, owner)
		if err := bus.PublishPersistedFlowInstanceRoute(route); err != nil {
			return nil, managedexecution.Admission{}, err
		}
		if err := bus.VerifyFlowInstanceRoute(ctx, owner); err != nil {
			return nil, managedexecution.Admission{}, err
		}
	}
	if len(req.AgentRuntime.Records) == 0 {
		options := selectedContractManagerOptions(runtimemanager.AgentManagerOptions{
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
		if _, err := generationGrant.MarkProbesSettled(ctx, nil); err != nil {
			return nil, managedexecution.Admission{}, err
		}
		if _, err := generationGrant.AdmitExecution(ctx); err != nil {
			return nil, managedexecution.Admission{}, err
		}
		grantOwned = false
		return &selectedContractAgentRuntime{manager: manager, generationGrant: generationGrant, workspaceProjection: req.AgentRuntime.workspaceProjection}, admission, nil
	}
	builder, err := buildSelectedContractAgentRuntimeFactory(req, generationGrant, bus, pipeline)
	if err != nil {
		return nil, managedexecution.Admission{}, err
	}
	if builder.runtimes != nil {
		actual, err := swaruntime.PrepareSelectedForkProviderCatalog(ctx, builder.runtimes, builder.tools, req.AgentRuntime.Blueprints)
		if err != nil || actual.Fingerprint() != req.Prepared.catalog.Fingerprint() {
			if builder.cleanup != nil {
				builder.cleanup()
			}
			return nil, managedexecution.Admission{}, errors.Join(errors.New("selected execution provider catalog differs from preparation"), err)
		}
	}
	builder.options.BaseContext = context.WithoutCancel(ctx)
	builder.options.DeliveryStore = ports.busDurable.DeliveryLifecycle
	manager := runtimemanager.NewAgentManagerWithOptions(bus, builder.factory, builder.options, ports.manager)
	if builder.bindManager != nil {
		builder.bindManager(manager)
	}
	started := false
	cleanup := func() {
		_ = manager.Shutdown()
		if builder.cleanup != nil {
			builder.cleanup()
			builder.cleanup = nil
		}
	}
	defer func() {
		if !started {
			cleanup()
		}
	}()
	for _, rec := range req.AgentRuntime.Records {
		if err := manager.MaterializeAdmittedAgent(ctx, rec); err != nil {
			return nil, managedexecution.Admission{}, fmt.Errorf("%s materialize agent %s: %w", runfork.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner, strings.TrimSpace(rec.Config.ID), err)
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
	if _, err := generationGrant.MarkProbesSettled(ctx, admission.CapabilitySurfaceIDs); err != nil {
		return nil, managedexecution.Admission{}, fmt.Errorf("settle selected-contract runtime generation probes: %w", err)
	}
	if _, err := generationGrant.AdmitExecution(ctx); err != nil {
		return nil, managedexecution.Admission{}, fmt.Errorf("admit selected-contract runtime generation: %w", err)
	}
	receiverExecution, err := builder.options.ReceiverExecution.WithSelectedAdmission(admission)
	if err != nil {
		return nil, managedexecution.Admission{}, fmt.Errorf("finalize selected-contract manager receiver execution: %w", err)
	}
	if err := manager.SetReceiverExecution(receiverExecution); err != nil {
		return nil, managedexecution.Admission{}, fmt.Errorf("install selected-contract manager receiver execution: %w", err)
	}
	if err := manager.RunAuthoritativeDeliveryOnly(ctx); err != nil {
		return nil, managedexecution.Admission{}, err
	}
	started = true
	grantOwned = false
	return &selectedContractAgentRuntime{manager: manager, generationGrant: generationGrant, cleanup: builder.cleanup, workspaceProjection: req.AgentRuntime.workspaceProjection}, admission, nil
}

func issueSelectedContractAgentRuntimeGenerationGrant(
	ctx context.Context,
	req publishSelectedContractForkEventsRequest,
	authority runtimeeffects.Authority,
) (runtimestartupownership.GenerationGrant, error) {
	if req.Prepared == nil || !req.Prepared.bound {
		return nil, errors.New("selected generation grant requires bound preparation")
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
	grant, err := capability.IssueSelectedForkGenerationGrant(ctx, runtimestartupownership.SelectedForkGrantRequest{
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
	budget := swaruntime.NewBudgetTracker(ports.budget, bus, options.Config, options.MailboxStore, nil, source, options.ExecutionPosture)
	managerOptions.Budget = budget
	if options.AgentFactory != nil {
		return selectedContractAgentRuntimeFactory{factory: options.AgentFactory, options: managerOptions}, nil
	}
	if options.Config == nil {
		return selectedContractAgentRuntimeFactory{}, selectedContractAgentRuntimeUnsupportedError(req.AgentRuntime.Proof.AgentRecipients, "missing selected-fork agent factory/runtime configuration")
	}

	mcpTurns := runtimemcp.NewTurnContextRegistry(runtimeactors.ActorFromContext)
	var managerRef runtimetools.Manager
	exec := newSelectedContractToolExecutor(options, source, bus, pipeline, func() runtimetools.Manager {
		return managerRef
	})
	if managerOptions.WorkOwner == nil {
		return selectedContractAgentRuntimeFactory{}, errors.New("selected-fork gateway requires work occurrence")
	}
	gatewayWork, err := managerOptions.WorkOwner.Begin(context.Background())
	if err != nil {
		return selectedContractAgentRuntimeFactory{}, err
	}
	binding, cleanup, err := startSelectedContractAgentRuntimeGateway(exec, mcpTurns, gatewayWork, func(identity agentidentity.Identity) (runtimeactors.AgentConfig, bool) {
		if managerRef == nil {
			return runtimeactors.AgentConfig{}, false
		}
		cfg, err := managerRef.ResolveAgentConfig(identity.RunID, identity.AgentID(), identity.FlowInstance())
		return cfg, err == nil
	})
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
	factory := runtimeagents.NewLLMAgentFactory(runtimes, exec, runtimeagents.LLMAgentOptions{})
	return selectedContractAgentRuntimeFactory{
		factory: factory,
		options: managerOptions,
		bindManager: func(manager runtimetools.Manager) {
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
		HumanTaskStore:     options.HumanTaskStore,
		WorkflowInstances:  pipeline,
		WorkflowSource:     source,
		WorkspaceResolver:  options.Workspace,
		AuthorityProvider:  authority,
		EmitRegistry:       runtimetools.NewEmitRegistry(source, authority),
		ManagerProvider:    managerProvider,
	})
}

func startSelectedContractAgentRuntimeGateway(exec *runtimetools.Executor, mcpTurns *runtimemcp.TurnContextRegistry, lease *worklifetime.Lease, resolveActorConfig func(agentidentity.Identity) (runtimeactors.AgentConfig, bool)) (_ toolgateway.Binding, _ func(), finalErr error) {
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
	if strings.TrimSpace(os.Getenv(toolgateway.RetiredAuthTokenEnvName)) != "" {
		_ = ln.Close()
		return toolgateway.Binding{}, nil, toolgateway.RetiredAuthTokenEnvError()
	}
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

	gateway := runtimemcp.NewGateway(exec, binding.AuthToken(), swaruntime.RuntimeMCPGatewayHooks(nil, nil, resolveActorConfig, nil, mcpTurns))
	server := serveSelectedContractGateway(ln, gateway.Handler(), lease)
	transferred = true
	return binding, server.Close, nil
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

func (r *selectedContractAgentRuntime) Shutdown() error {
	if r == nil {
		return nil
	}
	var err error
	if r.manager != nil {
		err = r.manager.Shutdown()
	}
	if r.cleanup != nil {
		r.cleanup()
		r.cleanup = nil
	}
	if r.generationGrant != nil {
		err = errors.Join(err, r.generationGrant.Retire(context.Background()))
		r.generationGrant = nil
	}
	err = errors.Join(err, r.workspaceProjection.Release())
	return err
}

func (r *selectedContractAgentRuntime) WaitForQuiescence(ctx context.Context, bus *runtimebus.EventBus) error {
	if r == nil || r.manager == nil {
		return nil
	}
	if err := r.manager.WaitForQuiescence(ctx); err != nil {
		return err
	}
	if bus != nil {
		return bus.WaitForQuiescence(ctx)
	}
	return nil
}
