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
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	swaruntime "github.com/division-sh/swarm/internal/runtime"
	runtimeagents "github.com/division-sh/swarm/internal/runtime/agents"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
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
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
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
	Proof           SelectedContractAgentRuntimeMaterialization
	Blueprints      []runtimemanager.AgentMaterializationBlueprint
	ConfiguredPlans []agentidentity.Plan
	Records         []runtimemanager.PersistedAgent
	Options         SelectedContractAgentRuntimeOptions
}

func (p selectedContractAgentRuntimePlan) bindRun(runID string, committed []runfork.RunForkSelectedContractAgentTopology) (selectedContractAgentRuntimePlan, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return selectedContractAgentRuntimePlan{}, errors.New("selected-contract agent runtime requires fork run_id")
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
	var err error
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
		if err := record.Topology.Validate(); err != nil {
			topology, ok := topologyByIdentity[identity]
			if !ok {
				return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract agent %s has no committed topology admission", identity.Description())
			}
			record.Topology = topology
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
	manager         *runtimemanager.AgentManager
	generationGrant runtimestartupownership.GenerationGrant
	cleanup         func()
}

type selectedContractAgentRuntimeFactory struct {
	factory     runtimemanager.AgentFactory
	options     runtimemanager.AgentManagerOptions
	bindManager func(runtimetools.Manager)
	cleanup     func()
	preflight   *selectedContractAgentRuntimePreflight
}

type selectedContractAgentRuntimePreflight struct {
	config   *config.Config
	source   semanticview.Source
	gateway  toolgateway.Binding
	runtimes *runtimellm.AgentRuntimeSet
	turns    runtimellm.MCPTurnContextStore
	tools    *runtimetools.Executor
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

func prepareSelectedContractAgentRuntimeMaterialization(ctx context.Context, loaded LoadedSelectedContractSource, planning runfork.RunForkSelectedContractRecipientPlanning, forkPlan runfork.RunForkPlan, options SelectedContractAgentRuntimeOptions) (selectedContractAgentRuntimePlan, error) {
	if err := ctx.Err(); err != nil {
		return selectedContractAgentRuntimePlan{}, err
	}
	if strings.TrimSpace(planning.Owner) != runfork.RunForkSelectedContractRecipientPlanningOwner {
		return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract agent runtime materialization requires %s; got %q", runfork.RunForkSelectedContractRecipientPlanningOwner, planning.Owner)
	}
	agentPlans, err := selectedContractPlannedAgentRecipientPlans(planning)
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
		return selectedContractAgentRuntimePlan{Proof: proof, Options: options}, nil
	}
	blueprints, err := selectedContractAgentBlueprints(loaded.Source, planning, forkPlan)
	if err != nil {
		return selectedContractAgentRuntimePlan{Proof: proof, Options: options}, err
	}
	modelOptions, _, err := selectedContractAgentModelOptions(options)
	if err != nil {
		return selectedContractAgentRuntimePlan{Proof: proof, Options: options}, err
	}
	for i := range blueprints {
		blueprints[i], err = runtimemanager.ResolveAgentMaterializationBlueprint(modelOptions, blueprints[i])
		if err != nil {
			return selectedContractAgentRuntimePlan{Proof: proof, Options: options}, fmt.Errorf("resolve selected-contract agent materialization blueprint: %w", err)
		}
	}
	if options.ProcessCapability == nil {
		return selectedContractAgentRuntimePlan{Proof: proof, Options: options}, errors.New("selected-contract declaration provenance requires the process topology capability")
	}
	plan, exists, err := options.ProcessCapability.CurrentSourceSet(ctx)
	if err != nil {
		return selectedContractAgentRuntimePlan{Proof: proof, Options: options}, fmt.Errorf("prove selected-contract source-set authority: %w", err)
	}
	if !exists {
		return selectedContractAgentRuntimePlan{Proof: proof, Options: options}, errors.New("selected-contract declaration provenance requires an installed source-set plan")
	}
	bundleHash, bundleSource := loaded.BundleSourceFact.StorageValues()
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: bundleHash, BundleSource: bundleSource}.Normalize()
	if err := coordinate.Validate(); err != nil {
		return selectedContractAgentRuntimePlan{Proof: proof, Options: options}, fmt.Errorf("selected-contract declaration source coordinate: %w", err)
	}
	sourceCurrent := false
	for _, source := range plan.Sources {
		if source.Normalize().Key() == coordinate.Key() {
			sourceCurrent = true
			break
		}
	}
	if !sourceCurrent {
		return selectedContractAgentRuntimePlan{Proof: proof, Options: options}, errors.New("selected-contract declaration source is not current in the process source-set plan")
	}
	staticTopology, err := runtimeagenttopology.StaticAdmission(
		plan.Revision,
		coordinate.BundleHash,
		coordinate.BundleSource,
		runtimeagenttopology.LifetimeDurableManaged,
	)
	if err != nil {
		return selectedContractAgentRuntimePlan{Proof: proof, Options: options}, fmt.Errorf("selected-contract static declaration topology: %w", err)
	}
	staticPlans := make(map[agentidentity.Plan]struct{}, len(plan.Agents))
	for _, desired := range plan.Agents {
		staticPlans[desired.Identity.Normalize()] = struct{}{}
	}
	for i := range blueprints {
		if _, declared := staticPlans[blueprints[i].Identity.Normalize()]; declared {
			blueprints[i].Topology = staticTopology
		}
	}
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
	return selectedContractAgentRuntimePlan{Proof: proof, Blueprints: selected, ConfiguredPlans: configured, Options: options}, nil
}

func selectedContractStaticAgentBlueprints(source semanticview.Source) ([]runtimemanager.AgentMaterializationBlueprint, error) {
	staticRecords, err := runtimemanager.StaticAgentMaterializationBlueprints(source)
	if err != nil {
		return nil, err
	}
	requiredRecords, err := runtimemanager.StaticFlowRequiredAgentMaterializationBlueprints(source)
	if err != nil {
		return nil, err
	}
	return append(staticRecords, requiredRecords...), nil
}

func selectedContractStaticAgentRecords(runID string, source semanticview.Source) ([]runtimemanager.PersistedAgent, error) {
	blueprints, err := selectedContractStaticAgentBlueprints(source)
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

func selectedContractAgentBlueprints(source semanticview.Source, planning runfork.RunForkSelectedContractRecipientPlanning, forkPlan runfork.RunForkPlan) ([]runtimemanager.AgentMaterializationBlueprint, error) {
	blueprints, err := selectedContractStaticAgentBlueprints(source)
	if err != nil {
		return nil, err
	}
	blueprintsByPlan := make(map[agentidentity.Plan]runtimemanager.AgentMaterializationBlueprint, len(blueprints))
	for _, blueprint := range blueprints {
		blueprintsByPlan[blueprint.Identity.Normalize()] = blueprint
	}
	recipients, err := selectedContractPlannedAgentRecipientPlans(planning)
	if err != nil {
		return nil, err
	}
	for _, recipient := range recipients {
		if _, exists := blueprintsByPlan[recipient.Normalize()]; exists {
			continue
		}
		var target events.RouteIdentity
		for _, pending := range forkPlan.PendingWork {
			identity := pending.DeliveryRoute.AgentIdentity.Normalize()
			identityPlan, planErr := identity.Plan()
			if planErr != nil || identity.RunID != strings.TrimSpace(forkPlan.SourceRunID) || identityPlan.Normalize() != recipient.Normalize() {
				continue
			}
			candidate := pending.DeliveryRoute.Target.Route()
			if target.Empty() {
				target = candidate
				continue
			}
			if !events.SameRouteIdentity(target, candidate) {
				return nil, fmt.Errorf("selected-contract agent %s has conflicting fixed-revision targets", recipient.Description())
			}
		}
		if target.Empty() || strings.TrimSpace(target.FlowID) == "" || strings.TrimSpace(target.EntityID) == "" ||
			strings.Trim(strings.TrimSpace(target.FlowInstance), "/") != recipient.FlowInstance() {
			continue
		}
		derived, err := runtimemanager.TemplateFlowAgentMaterializationBlueprints(
			source, target.FlowID, target.FlowInstance, target.EntityID,
		)
		if err != nil {
			return nil, fmt.Errorf("derive selected-contract template agent %s: %w", recipient.Description(), err)
		}
		for _, blueprint := range derived {
			plan := blueprint.Identity.Normalize()
			if existing, duplicate := blueprintsByPlan[plan]; duplicate {
				if existing.Config.ID != blueprint.Config.ID {
					return nil, fmt.Errorf("selected-contract agent %s has conflicting declaration blueprints", plan.Description())
				}
				continue
			}
			blueprintsByPlan[plan] = blueprint
			blueprints = append(blueprints, blueprint)
		}
	}
	return blueprints, nil
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

func startSelectedContractAgentRuntime(ctx context.Context, req publishSelectedContractForkEventsRequest, bus *runtimebus.EventBus, pipeline *runtimepipeline.PipelineCoordinator) (*selectedContractAgentRuntime, managedexecution.Admission, error) {
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
	if len(req.AgentRuntime.Records) == 0 {
		options := selectedContractManagerOptions(runtimemanager.AgentManagerOptions{
			ExecutionPosture:  req.AgentRuntime.Options.ExecutionPosture,
			BaseContext:       context.WithoutCancel(ctx),
			BundleSourceFact:  req.LoadedSource.BundleSourceFact,
			SemanticSource:    req.LoadedSource.Source,
			WorkflowInstances: pipeline,
			DeliveryStore:     ports.busDurable.DeliveryLifecycle,
			WorkOwner:         req.AgentRuntime.Options.AgentManagerOptions.WorkOwner,
			ReceiverExecution: req.AgentRuntime.Options.AgentManagerOptions.ReceiverExecution,
		}, nil, bus, ports, pipeline)
		manager := runtimemanager.NewAgentManagerWithOptions(bus, nil, options, ports.manager)
		return &selectedContractAgentRuntime{manager: manager}, admission, nil
	}
	generationGrant, err := issueSelectedContractAgentRuntimeGenerationGrant(ctx, req, authority)
	if err != nil {
		return nil, managedexecution.Admission{}, err
	}
	grantOwned := true
	defer func() {
		if grantOwned {
			_ = generationGrant.Retire(context.Background())
		}
	}()
	builder, err := buildSelectedContractAgentRuntimeFactory(req, generationGrant, bus, pipeline)
	if err != nil {
		return nil, managedexecution.Admission{}, err
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
	readinessOwners := map[runtimeflowidentity.RunScopedFlowInstance]struct{}{}
	for _, rec := range req.AgentRuntime.Records {
		if rec.Topology.Authority.Kind != runtimeagenttopology.AuthorityFlowReadinessPlan {
			continue
		}
		readiness := rec.Topology.Authority.Readiness
		if readiness == nil {
			return nil, managedexecution.Admission{}, fmt.Errorf("selected-contract flow readiness topology is incomplete")
		}
		owner, err := runtimeflowidentity.NewRunScopedFlowInstance(
			readiness.RunID,
			runtimeflowidentity.RouteForInstancePath(readiness.InstancePath),
		)
		if err != nil {
			return nil, managedexecution.Admission{}, fmt.Errorf("selected-contract flow readiness owner: %w", err)
		}
		readinessOwners[owner.Normalize()] = struct{}{}
	}
	orderedReadinessOwners := make([]runtimeflowidentity.RunScopedFlowInstance, 0, len(readinessOwners))
	for owner := range readinessOwners {
		orderedReadinessOwners = append(orderedReadinessOwners, owner)
	}
	sort.Slice(orderedReadinessOwners, func(i, j int) bool {
		return orderedReadinessOwners[i].Key() < orderedReadinessOwners[j].Key()
	})
	for _, owner := range orderedReadinessOwners {
		if err := manager.PreparePersistedDynamicFlowRuntimeProcessTopology(ctx, owner); err != nil {
			return nil, managedexecution.Admission{}, fmt.Errorf("prepare selected-contract flow process topology %s: %w", owner.Route.InstancePath, err)
		}
	}
	for _, rec := range req.AgentRuntime.Records {
		if rec.Topology.Authority.Kind != runtimeagenttopology.AuthorityFlowReadinessPlan {
			if err := manager.MaterializeAdmittedAgent(ctx, rec); err != nil {
				return nil, managedexecution.Admission{}, fmt.Errorf("%s materialize agent %s: %w", runfork.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner, strings.TrimSpace(rec.Config.ID), err)
			}
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
	if builder.preflight != nil {
		controller, ok := runtimeeffects.ControllerFromContext(ctx)
		if !ok {
			return nil, managedexecution.Admission{}, fmt.Errorf("selected-fork managed provider preflight requires the existing effect controller")
		}
		surfaceIDs, err := swaruntime.ValidateManagedProviderPreflight(
			ctx,
			builder.preflight.config,
			builder.preflight.source,
			builder.preflight.gateway,
			builder.preflight.runtimes,
			builder.preflight.turns,
			builder.preflight.tools,
			manager,
			swaruntime.ManagedProviderPreflightAuthority{
				ExecutionKind:        managedcapabilities.ExecutionSelectedContractFork,
				ExecutionAuthorityID: authority.SelectedFork.ExecutionID,
				RunID:                authority.SelectedFork.ForkRunID,
				StartupOwnerID:       authority.ExecutionOwner,
				StartupGeneration:    authority.SelectedFork.Generation,
				EffectController:     controller,
				CapabilityStore:      ports.managedCapabilities,
				EffectAuthority: func(string, string) (runtimeeffects.Authority, error) {
					return authority, nil
				},
			},
		)
		if err != nil {
			return nil, managedexecution.Admission{}, err
		}
		admission, err = admission.WithCapabilitySurfaces(surfaceIDs)
		if err != nil {
			return nil, managedexecution.Admission{}, err
		}
		ctx = managedexecution.WithAdmission(ctx, admission)
		if err := bus.FinalizeSelectedReceiverAdmission(admission); err != nil {
			return nil, managedexecution.Admission{}, err
		}
		if err := pipeline.FinalizeSelectedReceiverAdmission(admission); err != nil {
			return nil, managedexecution.Admission{}, err
		}
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
	return &selectedContractAgentRuntime{manager: manager, generationGrant: generationGrant, cleanup: builder.cleanup}, admission, nil
}

func issueSelectedContractAgentRuntimeGenerationGrant(
	ctx context.Context,
	req publishSelectedContractForkEventsRequest,
	authority runtimeeffects.Authority,
) (runtimestartupownership.GenerationGrant, error) {
	capability := req.AgentRuntime.Options.ProcessCapability
	if capability == nil {
		return nil, errors.New("selected-contract agent runtime requires the process topology capability")
	}
	plan, exists, err := capability.CurrentSourceSet(ctx)
	if err != nil {
		return nil, fmt.Errorf("load selected-contract runtime source set: %w", err)
	}
	if !exists {
		return nil, errors.New("selected-contract agent runtime requires a current complete source set")
	}
	processAuthority, err := capability.Evidence()
	if err != nil {
		return nil, fmt.Errorf("load selected-contract process authority: %w", err)
	}
	bundleHash, bundleSource := req.LoadedSource.BundleSourceFact.StorageValues()
	grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash:        bundleHash,
		BundleSource:      bundleSource,
		RuntimeInstanceID: processAuthority.RuntimeInstanceID,
		RuntimeGeneration: authority.SelectedFork.Generation,
		SourceSetRevision: plan.Revision,
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
	managerOptions.BundleSourceFact = req.LoadedSource.BundleSourceFact
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

	authority := runtimeauthority.NewSourceProvider(source)
	emitRegistry := runtimetools.NewEmitRegistry(source, authority)
	mcpTurns := runtimemcp.NewTurnContextRegistry(runtimeactors.ActorFromContext)
	credentials := options.Credentials
	if credentials == nil {
		credentials = runtimecredentials.NewEnvStore()
	}
	var managerRef runtimetools.Manager
	exec := runtimetools.NewExecutorWithOptions(bus, runtimetools.ExecutorOptions{
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
		EmitRegistry:       emitRegistry,
		ManagerProvider: func() runtimetools.Manager {
			return managerRef
		},
	})
	binding, cleanup, err := startSelectedContractAgentRuntimeGateway(exec, mcpTurns, managerOptions.WorkOwner, func(identity agentidentity.Identity) (runtimeactors.AgentConfig, bool) {
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
		cleanup: cleanup,
		preflight: &selectedContractAgentRuntimePreflight{
			config:   options.Config,
			source:   source,
			gateway:  binding,
			runtimes: runtimes,
			turns:    mcpTurns,
			tools:    exec,
		},
	}, nil
}

func startSelectedContractAgentRuntimeGateway(exec *runtimetools.Executor, mcpTurns *runtimemcp.TurnContextRegistry, owner worklifetime.Occurrence, resolveActorConfig func(agentidentity.Identity) (runtimeactors.AgentConfig, bool)) (toolgateway.Binding, func(), error) {
	if exec == nil {
		return toolgateway.Binding{}, nil, nil
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
	server := &http.Server{Handler: gateway.Handler()}
	if owner == nil {
		_ = ln.Close()
		return toolgateway.Binding{}, nil, fmt.Errorf("selected-fork gateway requires work occurrence")
	}
	lease, err := owner.Begin(context.Background())
	if err != nil {
		_ = ln.Close()
		return toolgateway.Binding{}, nil, fmt.Errorf("admit selected-fork gateway: %w", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = lease.Done() }()
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			_ = server.Close()
		}
	}()
	return binding, func() {
		if err := server.Shutdown(context.Background()); err != nil {
			_ = server.Close()
		}
		<-done
	}, nil
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
