package runforkexecution

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"swarm/internal/config"
	swaruntime "swarm/internal/runtime"
	runtimeagents "swarm/internal/runtime/agents"
	runtimeauthority "swarm/internal/runtime/authority"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimecredentials "swarm/internal/runtime/credentials"
	runtimellm "swarm/internal/runtime/llm"
	runtimemanager "swarm/internal/runtime/manager"
	runtimemcp "swarm/internal/runtime/mcp"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	runtimesessions "swarm/internal/runtime/sessions"
	runtimetools "swarm/internal/runtime/tools"
	workspace "swarm/internal/runtime/workspace"
	"swarm/internal/store"
)

const selectedContractAgentRuntimeDefaultQuiescenceTimeout = 2 * time.Minute

var selectedContractAgentRuntimeGatewayEnvMu sync.Mutex

type SelectedContractAgentRuntimeOptions struct {
	Config            *config.Config
	SQLDB             *sql.DB
	SessionRegistry   runtimesessions.Registry
	ConversationStore runtimellm.ConversationPersistence
	TurnStore         runtimellm.TurnPersistence
	ScheduleStore     runtimepipeline.SchedulePersistence
	MailboxStore      runtimetools.MailboxPersistence
	Workspace         workspace.Lifecycle
	Credentials       runtimecredentials.Store
	LLMRuntime        runtimellm.Runtime
	MCPClient         *runtimemcp.Client

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

func startSelectedContractAgentRuntime(ctx context.Context, req publishSelectedContractForkEventsRequest, bus *runtimebus.EventBus) (*selectedContractAgentRuntime, error) {
	if len(req.AgentRuntime.Records) == 0 {
		return nil, nil
	}
	builder, err := buildSelectedContractAgentRuntimeFactory(req, bus)
	if err != nil {
		return nil, err
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
		if err := manager.RegisterEphemeralAgentForExecution(ctx, rec); err != nil && !strings.Contains(err.Error(), "agent already exists") {
			return nil, fmt.Errorf("%s materialize agent %s: %w", store.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner, strings.TrimSpace(rec.Config.ID), err)
		}
		bus.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{
			AgentID:      rec.Config.ID,
			EntityID:     rec.Config.EffectiveEntityID(),
			FlowInstance: rec.Config.CanonicalFlowPath(),
		})
	}
	manager.RunAuthoritativeDeliveryOnly(ctx)
	started = true
	return &selectedContractAgentRuntime{manager: manager, cleanup: builder.cleanup}, nil
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
	if modelRuntime == nil {
		modelRuntime, err = runtimellm.RuntimeFactory{
			Cfg:           options.Config,
			Sessions:      options.SessionRegistry,
			Turns:         options.TurnStore,
			Conversations: options.ConversationStore,
			Workspaces:    options.Workspace,
			Events:        bus,
			MCPTurns:      mcpTurns,
		}.Build()
		if err != nil {
			return selectedContractAgentRuntimeFactory{}, fmt.Errorf("build selected-fork agent runtime: %w", err)
		}
	}
	var managerRef runtimetools.Manager
	exec := runtimetools.NewExecutorWithOptions(bus, nil, runtimetools.ExecutorOptions{
		Config:            options.Config,
		Credentials:       credentials,
		MailboxStore:      options.MailboxStore,
		MCPClient:         options.MCPClient,
		SQLDB:             options.SQLDB,
		WorkflowSource:    source,
		WorkspaceResolver: options.Workspace,
		AuthorityProvider: authority,
		EmitRegistry:      emitRegistry,
		ManagerProvider: func() runtimetools.Manager {
			return managerRef
		},
	}, options.ScheduleStore)
	cleanup, err := startSelectedContractAgentRuntimeGateway(exec, emitRegistry, mcpTurns, func(agentID string) (runtimeactors.AgentConfig, bool) {
		if managerRef == nil {
			return runtimeactors.AgentConfig{}, false
		}
		return managerRef.GetAgentConfig(agentID)
	})
	if err != nil {
		return selectedContractAgentRuntimeFactory{}, fmt.Errorf("start selected-fork tool gateway: %w", err)
	}
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
	}, nil
}

func startSelectedContractAgentRuntimeGateway(exec *runtimetools.Executor, emitRegistry *runtimetools.EmitRegistry, mcpTurns *runtimemcp.TurnContextRegistry, resolveActorConfig func(string) (runtimeactors.AgentConfig, bool)) (func(), error) {
	if exec == nil {
		return nil, nil
	}
	selectedContractAgentRuntimeGatewayEnvMu.Lock()
	unlock := true
	defer func() {
		if unlock {
			selectedContractAgentRuntimeGatewayEnvMu.Unlock()
		}
	}()

	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return nil, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	hostURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	containerURL := fmt.Sprintf("http://host.docker.internal:%d", port)
	prevHostURL, prevHostSet := os.LookupEnv("SWARM_TOOL_GATEWAY_URL")
	prevContainerURL, prevContainerSet := os.LookupEnv("SWARM_TOOL_GATEWAY_CONTAINER_URL")
	if err := os.Setenv("SWARM_TOOL_GATEWAY_URL", hostURL); err != nil {
		_ = ln.Close()
		return nil, err
	}
	if err := os.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", containerURL); err != nil {
		restoreEnv("SWARM_TOOL_GATEWAY_URL", prevHostURL, prevHostSet)
		_ = ln.Close()
		return nil, err
	}

	gateway := runtimemcp.NewGateway(exec, strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_TOKEN")), swaruntime.RuntimeMCPGatewayHooks(nil, nil, resolveActorConfig, nil, emitRegistry, mcpTurns))
	server := &http.Server{Handler: gateway.Handler()}
	go func() {
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			_ = server.Close()
		}
	}()
	unlock = false
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = server.Shutdown(ctx)
		cancel()
		restoreEnv("SWARM_TOOL_GATEWAY_CONTAINER_URL", prevContainerURL, prevContainerSet)
		restoreEnv("SWARM_TOOL_GATEWAY_URL", prevHostURL, prevHostSet)
		selectedContractAgentRuntimeGatewayEnvMu.Unlock()
	}, nil
}

func restoreEnv(key, value string, existed bool) {
	if existed {
		_ = os.Setenv(key, value)
		return
	}
	_ = os.Unsetenv(key)
}

func selectedContractPromptResolver(source semanticview.Source) (runtimecontracts.PromptResolver, error) {
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		return nil, nil
	}
	return runtimecontracts.NewBundlePromptResolver(bundle), nil
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
