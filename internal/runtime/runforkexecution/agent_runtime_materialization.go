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
	swaruntime "github.com/division-sh/swarm/internal/runtime"
	runtimeagents "github.com/division-sh/swarm/internal/runtime/agents"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

const selectedContractAgentRuntimeDefaultQuiescenceTimeout = 2 * time.Minute

type SelectedContractAgentRuntimeOptions struct {
	Config              *config.Config
	EntityStore         runtimetools.EntityPersistence
	HumanTaskStore      runtimetools.HumanTaskCardStore
	SessionRegistry     runtimesessions.Registry
	ConversationStore   runtimellm.ConversationPersistence
	MailboxStore        runtimetools.MailboxPersistence
	Workspace           workspace.Lifecycle
	Credentials         runtimecredentials.Store
	ManagedCredentials  runtimemanagedcredentials.Store
	ProviderCredentials runtimecredentials.Store
	LLMRuntime          runtimellm.Runtime
	MCPClient           *runtimemcp.Client
	AgentFactory        runtimemanager.AgentFactory
	AgentManagerOptions runtimemanager.AgentManagerOptions
	QuiescenceTimeout   time.Duration
}

type SelectedContractAgentRuntimeMaterialization struct {
	Owner                     string                   `json:"owner"`
	RecipientPlanningOwner    string                   `json:"recipient_planning_owner"`
	ExecutionOwner            string                   `json:"execution_owner"`
	AgentRecipients           []agentidentity.Identity `json:"agent_recipients,omitempty"`
	ConfiguredAgentIdentities []agentidentity.Identity `json:"configured_agent_identities,omitempty"`
	MissingAgentRecipients    []agentidentity.Identity `json:"missing_agent_recipients,omitempty"`
	MaterializationRequired   bool                     `json:"materialization_required"`
	MaterializationSupported  bool                     `json:"materialization_supported"`
	EphemeralForkLocal        bool                     `json:"ephemeral_fork_local"`
}

type selectedContractAgentRuntimePlan struct {
	Proof   SelectedContractAgentRuntimeMaterialization
	Records []runtimemanager.PersistedAgent
	Options SelectedContractAgentRuntimeOptions
}

type selectedContractAgentRuntime struct {
	manager *runtimemanager.AgentManager
	cleanup func()
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

func selectedContractManagerOptions(options runtimemanager.AgentManagerOptions, bus *runtimebus.EventBus, ports *selectedContractExecutionPorts, pipeline *runtimepipeline.PipelineCoordinator) runtimemanager.AgentManagerOptions {
	roles := ports.managerRoles
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

func prepareSelectedContractAgentRuntimeMaterialization(ctx context.Context, loaded LoadedSelectedContractSource, planning runfork.RunForkSelectedContractRecipientPlanning, options SelectedContractAgentRuntimeOptions) (selectedContractAgentRuntimePlan, error) {
	if err := ctx.Err(); err != nil {
		return selectedContractAgentRuntimePlan{}, err
	}
	if strings.TrimSpace(planning.Owner) != runfork.RunForkSelectedContractRecipientPlanningOwner {
		return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract agent runtime materialization requires %s; got %q", runfork.RunForkSelectedContractRecipientPlanningOwner, planning.Owner)
	}
	agents, err := selectedContractPlannedAgentRecipients(planning)
	if err != nil {
		return selectedContractAgentRuntimePlan{}, err
	}
	proof := SelectedContractAgentRuntimeMaterialization{
		Owner:                    runfork.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner,
		RecipientPlanningOwner:   planning.Owner,
		ExecutionOwner:           runfork.RunForkSelectedContractExecutionOwner,
		AgentRecipients:          agents,
		MaterializationRequired:  len(agents) > 0,
		MaterializationSupported: len(agents) == 0,
		EphemeralForkLocal:       true,
	}
	if len(agents) == 0 {
		return selectedContractAgentRuntimePlan{Proof: proof, Options: options}, nil
	}
	records, err := selectedContractStaticAgentRecords(loaded.Source)
	if err != nil {
		return selectedContractAgentRuntimePlan{Proof: proof, Options: options}, err
	}
	recordsByIdentity := map[agentidentity.Identity]runtimemanager.PersistedAgent{}
	configured := make([]agentidentity.Identity, 0, len(records))
	for _, rec := range records {
		identity, identityErr := rec.Config.ConcreteIdentity()
		if identityErr != nil {
			return selectedContractAgentRuntimePlan{Proof: proof, Options: options}, fmt.Errorf("selected-contract static agent identity: %w", identityErr)
		}
		if _, exists := recordsByIdentity[identity]; exists {
			continue
		}
		rec.Status = "ephemeral"
		rec.HiredBy = "selected-contract-fork-agent-runtime"
		recordsByIdentity[identity] = rec
		configured = append(configured, identity)
	}
	sortAgentIdentities(configured)
	proof.ConfiguredAgentIdentities = configured

	selected := make([]runtimemanager.PersistedAgent, 0, len(agents))
	missing := []agentidentity.Identity{}
	for _, identity := range agents {
		rec, ok := recordsByIdentity[identity]
		if !ok {
			missing = append(missing, identity)
			continue
		}
		selected = append(selected, rec)
	}
	if len(missing) > 0 {
		sortAgentIdentities(missing)
		proof.MissingAgentRecipients = missing
		return selectedContractAgentRuntimePlan{Proof: proof, Records: selected, Options: options}, selectedContractAgentRuntimeUnsupportedError(missing, "missing selected-source static contract-agent materialization record")
	}
	if options.AgentFactory == nil && options.Config == nil {
		return selectedContractAgentRuntimePlan{Proof: proof, Records: selected, Options: options}, selectedContractAgentRuntimeUnsupportedError(agents, "missing selected-fork agent factory/runtime configuration")
	}
	proof.MaterializationSupported = true
	return selectedContractAgentRuntimePlan{Proof: proof, Records: selected, Options: options}, nil
}

func selectedContractStaticAgentRecords(source semanticview.Source) ([]runtimemanager.PersistedAgent, error) {
	staticRecords, err := runtimemanager.StaticAgentMaterializationRecords(source)
	if err != nil {
		return nil, err
	}
	requiredRecords, err := runtimemanager.StaticFlowRequiredAgentMaterializationRecords(source)
	if err != nil {
		return nil, err
	}
	return append(staticRecords, requiredRecords...), nil
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
	if pipeline == nil {
		return nil, managedexecution.Admission{}, fmt.Errorf("selected-contract workflow lifecycle store is required")
	}
	if len(req.AgentRuntime.Records) == 0 {
		options := selectedContractManagerOptions(runtimemanager.AgentManagerOptions{
			BaseContext:       context.WithoutCancel(ctx),
			BundleSourceFact:  req.LoadedSource.BundleSourceFact,
			SemanticSource:    req.LoadedSource.Source,
			WorkflowInstances: pipeline,
			DeliveryStore:     ports.busDurable.DeliveryLifecycle,
			WorkOwner:         req.AgentRuntime.Options.AgentManagerOptions.WorkOwner,
			ReceiverExecution: req.AgentRuntime.Options.AgentManagerOptions.ReceiverExecution,
		}, bus, ports, pipeline)
		manager := runtimemanager.NewAgentManagerWithOptions(bus, nil, options, ports.manager)
		return &selectedContractAgentRuntime{manager: manager}, admission, nil
	}
	builder, err := buildSelectedContractAgentRuntimeFactory(req, bus, pipeline)
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
	for _, rec := range req.AgentRuntime.Records {
		if err := manager.RegisterEphemeralAgentForExecution(ctx, rec); err != nil && !errors.Is(err, runtimemanager.ErrAgentAlreadyExists) {
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
	return &selectedContractAgentRuntime{manager: manager, cleanup: builder.cleanup}, admission, nil
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

func buildSelectedContractAgentRuntimeFactory(req publishSelectedContractForkEventsRequest, bus *runtimebus.EventBus, pipeline *runtimepipeline.PipelineCoordinator) (selectedContractAgentRuntimeFactory, error) {
	ports, err := req.Owner.require()
	if err != nil {
		return selectedContractAgentRuntimeFactory{}, err
	}
	if pipeline == nil {
		return selectedContractAgentRuntimeFactory{}, fmt.Errorf("selected-contract workflow lifecycle store is required")
	}
	options := req.AgentRuntime.Options
	source := req.LoadedSource.Source
	promptResolver, err := selectedContractPromptResolver(source)
	if err != nil {
		return selectedContractAgentRuntimeFactory{}, err
	}
	managerOptions := options.AgentManagerOptions
	if managerOptions.SemanticSource == nil {
		managerOptions.SemanticSource = source
	}
	managerOptions.BundleSourceFact = req.LoadedSource.BundleSourceFact
	managerOptions.WorkflowInstances = pipeline
	managerOptions = selectedContractManagerOptions(managerOptions, bus, ports, pipeline)
	if managerOptions.PromptResolver == nil {
		managerOptions.PromptResolver = promptResolver
	}
	if managerOptions.Sessions == nil {
		managerOptions.Sessions = options.SessionRegistry
	}
	if managerOptions.Workspaces == nil {
		managerOptions.Workspaces = options.Workspace
	}
	var backendProfile llmselection.Profile
	if options.Config != nil {
		backendProfile, err = options.Config.LLMBackendProfile()
		if err != nil {
			return selectedContractAgentRuntimeFactory{}, err
		}
		if configured := strings.TrimSpace(managerOptions.LLMBackend); configured != "" && configured != backendProfile.ID {
			return selectedContractAgentRuntimeFactory{}, fmt.Errorf("selected-contract manager llm backend %q conflicts with runtime default %q", configured, backendProfile.ID)
		}
		managerOptions.LLMBackend = backendProfile.ID
	}
	budget := swaruntime.NewBudgetTracker(ports.budget, bus, options.Config, options.MailboxStore, nil, source)
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
	binding, cleanup, err := startSelectedContractAgentRuntimeGateway(exec, mcpTurns, managerOptions.WorkOwner, func(agentID string) (runtimeactors.AgentConfig, bool) {
		if managerRef == nil {
			return runtimeactors.AgentConfig{}, false
		}
		cfg, err := managerRef.ResolveAgentConfig(agentID, "")
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
		CompletionController: runtimeeffects.NewCompletionController(ports.effects, ports.completion, ports.completionHeartbeat, budget),
	}, options.LLMRuntime)
	if err != nil {
		if cleanup != nil {
			cleanup()
		}
		return selectedContractAgentRuntimeFactory{}, fmt.Errorf("build selected-fork agent runtime resolver: %w", err)
	}
	exec.SetModelRuntimes(runtimes)
	factory := runtimeagents.NewLLMAgentFactory(runtimes, exec, runtimeagents.LLMAgentOptions{
		PromptResolver: promptResolver,
	})
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

func startSelectedContractAgentRuntimeGateway(exec *runtimetools.Executor, mcpTurns *runtimemcp.TurnContextRegistry, owner worklifetime.Occurrence, resolveActorConfig func(string) (runtimeactors.AgentConfig, bool)) (toolgateway.Binding, func(), error) {
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

func selectedContractPromptResolver(source semanticview.Source) (runtimecontracts.PromptResolver, error) {
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		return nil, nil
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

func (r *selectedContractAgentRuntime) Shutdown() error {
	if r == nil || r.manager == nil {
		return nil
	}
	err := r.manager.Shutdown()
	if r.cleanup != nil {
		r.cleanup()
		r.cleanup = nil
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
