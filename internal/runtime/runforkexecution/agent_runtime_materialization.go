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
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
)

const selectedContractAgentRuntimeDefaultQuiescenceTimeout = 2 * time.Minute

type SelectedContractAgentRuntimeOptions struct {
	Config              *config.Config
	EntityStore         runtimetools.EntityPersistence
	HumanTaskStore      runtimetools.HumanTaskCardStore
	SessionRegistry     runtimesessions.Registry
	ConversationStore   runtimellm.ConversationPersistence
	ScheduleStore       runtimepipeline.SchedulePersistence
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
	Owner                    string   `json:"owner"`
	RecipientPlanningOwner   string   `json:"recipient_planning_owner"`
	ExecutionOwner           string   `json:"execution_owner"`
	AgentRecipients          []string `json:"agent_recipients,omitempty"`
	ConfiguredAgentIDs       []string `json:"configured_agent_ids,omitempty"`
	MissingAgentRecipients   []string `json:"missing_agent_recipients,omitempty"`
	MaterializationRequired  bool     `json:"materialization_required"`
	MaterializationSupported bool     `json:"materialization_supported"`
	EphemeralForkLocal       bool     `json:"ephemeral_fork_local"`
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
	config       *config.Config
	source       semanticview.Source
	gateway      toolgateway.Binding
	modelRuntime runtimellm.Runtime
	probe        runtimellm.StartupVisibleToolSurfaceProber
	turns        runtimellm.MCPTurnContextStore
	tools        *runtimetools.Executor
}

func prepareSelectedContractAgentRuntimeMaterialization(ctx context.Context, loaded LoadedSelectedContractSource, planning store.RunForkSelectedContractRecipientPlanning, options SelectedContractAgentRuntimeOptions) (selectedContractAgentRuntimePlan, error) {
	if err := ctx.Err(); err != nil {
		return selectedContractAgentRuntimePlan{}, err
	}
	if strings.TrimSpace(planning.Owner) != store.RunForkSelectedContractRecipientPlanningOwner {
		return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract agent runtime materialization requires %s; got %q", store.RunForkSelectedContractRecipientPlanningOwner, planning.Owner)
	}
	agents := selectedContractPlannedAgentRecipients(planning)
	proof := SelectedContractAgentRuntimeMaterialization{
		Owner:                    store.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner,
		RecipientPlanningOwner:   planning.Owner,
		ExecutionOwner:           store.RunForkSelectedContractExecutionOwner,
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
	recordsByID := map[string]runtimemanager.PersistedAgent{}
	configured := make([]string, 0, len(records))
	for _, rec := range records {
		id := strings.TrimSpace(rec.Config.ID)
		if id == "" {
			continue
		}
		if _, exists := recordsByID[id]; exists {
			continue
		}
		rec.Status = "ephemeral"
		rec.HiredBy = "selected-contract-fork-agent-runtime"
		recordsByID[id] = rec
		configured = append(configured, id)
	}
	sort.Strings(configured)
	proof.ConfiguredAgentIDs = configured

	selected := make([]runtimemanager.PersistedAgent, 0, len(agents))
	missing := []string{}
	for _, id := range agents {
		rec, ok := recordsByID[id]
		if !ok {
			missing = append(missing, id)
			continue
		}
		selected = append(selected, rec)
	}
	if len(missing) > 0 {
		sort.Strings(missing)
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

func selectedContractAgentRuntimeUnsupportedError(agents []string, reason string) error {
	agents = append([]string(nil), agents...)
	sort.Strings(agents)
	return fmt.Errorf("%s: %s requires selected-fork handler materialization for authoritative agent recipients before fork mutation; %s for %s",
		store.RunForkBlockerSelectedContractAgentHandlerMaterializationUnsupported,
		store.RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner,
		strings.TrimSpace(reason),
		strings.Join(agents, ","),
	)
}

func startSelectedContractAgentRuntime(ctx context.Context, req publishSelectedContractForkEventsRequest, bus *runtimebus.EventBus) (*selectedContractAgentRuntime, managedexecution.Admission, error) {
	admission, authority, err := selectedContractManagedExecutionAuthority(ctx)
	if err != nil {
		return nil, managedexecution.Admission{}, err
	}
	if len(req.AgentRuntime.Records) == 0 {
		manager := runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
			SemanticSource:    req.LoadedSource.Source,
			WorkflowInstances: runtimepipeline.NewWorkflowInstanceStore(req.Store.DB),
		}, req.Store)
		return &selectedContractAgentRuntime{manager: manager}, admission, nil
	}
	builder, err := buildSelectedContractAgentRuntimeFactory(req, bus)
	if err != nil {
		return nil, managedexecution.Admission{}, err
	}
	manager := runtimemanager.NewAgentManagerWithOptions(bus, builder.factory, builder.options, req.Store)
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
			return nil, managedexecution.Admission{}, fmt.Errorf("%s materialize agent %s: %w", store.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner, strings.TrimSpace(rec.Config.ID), err)
		}
		bus.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{
			AgentID:      rec.Config.ID,
			EntityID:     rec.Config.EffectiveEntityID(),
			FlowInstance: rec.Config.CanonicalFlowPath(),
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
			builder.preflight.modelRuntime,
			builder.preflight.probe,
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
				CapabilityStore:      req.Store,
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

func buildSelectedContractAgentRuntimeFactory(req publishSelectedContractForkEventsRequest, bus *runtimebus.EventBus) (selectedContractAgentRuntimeFactory, error) {
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
	if managerOptions.WorkflowInstances == nil {
		managerOptions.WorkflowInstances = runtimepipeline.NewWorkflowInstanceStore(req.Store.DB)
	}
	if managerOptions.PromptResolver == nil {
		managerOptions.PromptResolver = promptResolver
	}
	if managerOptions.Sessions == nil {
		managerOptions.Sessions = options.SessionRegistry
	}
	if managerOptions.Workspaces == nil {
		managerOptions.Workspaces = options.Workspace
	}
	if options.Config != nil && strings.TrimSpace(managerOptions.LLMBackend) == "" {
		profile, err := options.Config.LLMBackendProfile()
		if err != nil {
			return selectedContractAgentRuntimeFactory{}, err
		}
		managerOptions.LLMBackend = profile.ID
	}
	budget := swaruntime.NewBudgetTracker(req.Store, bus, options.Config, options.MailboxStore, nil, source)
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
	modelRuntime := options.LLMRuntime
	var managerRef runtimetools.Manager
	exec := runtimetools.NewExecutorWithOptions(bus, nil, runtimetools.ExecutorOptions{
		Config:             options.Config,
		Credentials:        credentials,
		ManagedCredentials: options.ManagedCredentials,
		MailboxStore:       options.MailboxStore,
		MCPClient:          options.MCPClient,
		EntityStore:        options.EntityStore,
		HumanTaskStore:     options.HumanTaskStore,
		WorkflowSource:     source,
		WorkspaceResolver:  options.Workspace,
		ModelRuntime:       modelRuntime,
		AuthorityProvider:  authority,
		EmitRegistry:       emitRegistry,
		ManagerProvider: func() runtimetools.Manager {
			return managerRef
		},
	}, options.ScheduleStore)
	binding, cleanup, err := startSelectedContractAgentRuntimeGateway(exec, emitRegistry, mcpTurns, func(agentID string) (runtimeactors.AgentConfig, bool) {
		if managerRef == nil {
			return runtimeactors.AgentConfig{}, false
		}
		return managerRef.GetAgentConfig(agentID)
	})
	if err != nil {
		return selectedContractAgentRuntimeFactory{}, fmt.Errorf("start selected-fork tool gateway: %w", err)
	}
	if modelRuntime == nil {
		modelRuntime, err = runtimellm.RuntimeFactory{
			Cfg:                  options.Config,
			Sessions:             options.SessionRegistry,
			Conversations:        options.ConversationStore,
			Workspaces:           options.Workspace,
			Events:               bus,
			MCPTurns:             mcpTurns,
			ToolGateway:          binding,
			Credentials:          options.ProviderCredentials,
			CompletionController: runtimeeffects.NewCompletionController(req.Store, budget),
		}.Build()
		if err != nil {
			if cleanup != nil {
				cleanup()
			}
			return selectedContractAgentRuntimeFactory{}, fmt.Errorf("build selected-fork agent runtime: %w", err)
		}
	}
	exec.SetModelRuntime(modelRuntime)
	factory := runtimeagents.NewLLMAgentFactory(modelRuntime, exec, exec.ToolDefinitions(), runtimeagents.LLMAgentOptions{
		PromptResolver:    promptResolver,
		AuthorityProvider: authority,
		EmitRegistry:      emitRegistry,
	})
	return selectedContractAgentRuntimeFactory{
		factory: factory,
		options: managerOptions,
		bindManager: func(manager runtimetools.Manager) {
			managerRef = manager
		},
		cleanup: cleanup,
		preflight: &selectedContractAgentRuntimePreflight{
			config:       options.Config,
			source:       source,
			gateway:      binding,
			modelRuntime: modelRuntime,
			probe: func() runtimellm.StartupVisibleToolSurfaceProber {
				probe, _ := runtimellm.StartupVisibleToolSurfaceProberForRuntime(modelRuntime)
				return probe
			}(),
			turns: mcpTurns,
			tools: exec,
		},
	}, nil
}

func startSelectedContractAgentRuntimeGateway(exec *runtimetools.Executor, emitRegistry *runtimetools.EmitRegistry, mcpTurns *runtimemcp.TurnContextRegistry, resolveActorConfig func(string) (runtimeactors.AgentConfig, bool)) (toolgateway.Binding, func(), error) {
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

	gateway := runtimemcp.NewGateway(exec, binding.AuthToken(), swaruntime.RuntimeMCPGatewayHooks(nil, nil, resolveActorConfig, nil, emitRegistry, mcpTurns))
	server := &http.Server{Handler: gateway.Handler()}
	go func() {
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			_ = server.Close()
		}
	}()
	return binding, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = server.Shutdown(ctx)
		cancel()
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
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	stable := 0
	for {
		if bus != nil {
			if err := bus.WaitForQuiescence(ctx); err != nil {
				return err
			}
		}
		if err := r.manager.WaitForQuiescence(ctx); err != nil {
			return err
		}
		pending := 0
		if bus != nil {
			pending = bus.PendingAgentDeliveries()
		}
		if pending == 0 {
			stable++
			if stable >= 3 {
				return nil
			}
		} else {
			stable = 0
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
