package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type managedNativeStartupProbeRuntime struct {
	llm.NoopRuntime
	probe startupVisibleSurfaceProbeStub
}

func (*managedNativeStartupProbeRuntime) ProviderContract() llm.ProviderContract {
	return llm.ClaudeCLIProviderContract()
}

func (r *managedNativeStartupProbeRuntime) ProbeStartupVisibleToolSurface(
	ctx context.Context,
	actor runtimeactors.AgentConfig,
	systemPrompt string,
	tools []llm.ToolDefinition,
) (*llm.Response, error) {
	return r.probe.ProbeStartupVisibleToolSurface(ctx, actor, systemPrompt, tools)
}

type managedNativeLifecycleStore struct {
	*startupRecoveryManagerStore
	*startupCapabilityStore
	*startupEffectStore
}

func newManagedNativeLifecycleStore(agent runtimemanager.PersistedAgent) *managedNativeLifecycleStore {
	return &managedNativeLifecycleStore{
		startupRecoveryManagerStore: &startupRecoveryManagerStore{
			agents: []runtimemanager.PersistedAgent{agent},
		},
		startupCapabilityStore: &startupCapabilityStore{},
		startupEffectStore:     &startupEffectStore{},
	}
}

func (s *managedNativeLifecycleStore) surface(id string) (managedcapabilities.Surface, error) {
	s.startupCapabilityStore.mu.Lock()
	defer s.startupCapabilityStore.mu.Unlock()
	surface, ok := s.startupCapabilityStore.surfaces[id]
	if !ok {
		return managedcapabilities.Surface{}, fmt.Errorf("managed capability surface %s was not persisted", id)
	}
	return surface.Clone(), nil
}

type managedNativeRecoveryDeliveryStore struct {
	runtimedelivery.Store
	claimCalls atomic.Int32
	onClaim    func(context.Context) error
}

func (s *managedNativeRecoveryDeliveryStore) ClaimAgentBacklog(
	ctx context.Context,
	agentID string,
	limit int,
) ([]runtimedelivery.AgentExecution, error) {
	s.claimCalls.Add(1)
	if s.onClaim != nil {
		if err := s.onClaim(ctx); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func TestRuntimeStart_RecoveryHydratesManagedNativePreflightBeforeReplayAdmission(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	ctx := testAuthorActivityContext(context.Background())
	managerStore := newManagedNativeLifecycleStore(managedNativeLifecycleAgent("recovered-native-agent"))
	delivery := &managedNativeRecoveryDeliveryStore{}
	lease := &fakeRuntimeStartupOwnershipLease{}
	ownership := fakeRuntimeStartupOwnershipStore{
		acquire: func(context.Context, string) (runtimestartupownership.Lease, error) {
			return lease, nil
		},
	}
	probeRuntime := &managedNativeStartupProbeRuntime{}
	cfg := managedNativeLifecycleConfig(true)
	rt, err := newScopedTestRuntime(t, ctx, RuntimeDeps{
		Config: cfg,
		Stores: managedNativeLifecycleStores(
			managerStore,
			delivery,
			ownership,
		),
		Options: managedNativeLifecycleOptions(t, "recovered-native-agent", probeRuntime),
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := rt.PrepareAuthorActivityCatalog(); err != nil {
		t.Fatalf("PrepareAuthorActivityCatalog: %v", err)
	}

	delivery.onClaim = func(claimCtx context.Context) error {
		admission, ok := managedexecution.FromContext(claimCtx)
		if !ok {
			return fmt.Errorf("recovery backlog claim started without managed execution admission")
		}
		authority, err := rt.ownershipLease.Authority()
		if err != nil {
			return fmt.Errorf("load recovery startup authority: %w", err)
		}
		if authority.State != runtimestartupownership.StateAdmitted {
			return fmt.Errorf("recovery backlog claim observed startup authority state %q, want admitted", authority.State)
		}
		if !slices.Equal(admission.CapabilitySurfaceIDs, authority.ProbeSurfaceIDs) || len(admission.CapabilitySurfaceIDs) != 1 {
			return fmt.Errorf("recovery admission surfaces %v do not match settled startup surfaces %v", admission.CapabilitySurfaceIDs, authority.ProbeSurfaceIDs)
		}
		surface, err := managerStore.surface(admission.CapabilitySurfaceIDs[0])
		if err != nil {
			return fmt.Errorf("load persisted recovery startup surface before replay: %w", err)
		}
		return validateManagedNativeLifecycleSurface(surface, "recovered-native-agent", authority)
	}

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := delivery.claimCalls.Load(); got == 0 {
		t.Fatal("recovery start did not enter backlog replay after managed preflight")
	}
	if got := probeRuntime.probe.calls; !slices.Equal(got, []string{"recovered-native-agent"}) {
		t.Fatalf("startup probe calls = %v, want recovered agent", got)
	}
	authority, err := rt.ownershipLease.Authority()
	if err != nil {
		t.Fatalf("startup authority: %v", err)
	}
	if authority.State != runtimestartupownership.StateAdmitted {
		t.Fatalf("startup authority state = %q, want admitted", authority.State)
	}
	if !slices.Equal(rt.startupAdmission.CapabilitySurfaceIDs, authority.ProbeSurfaceIDs) {
		t.Fatalf("startup admission surfaces = %v, settled authority surfaces = %v", rt.startupAdmission.CapabilitySurfaceIDs, authority.ProbeSurfaceIDs)
	}
	requireManagedNativeLifecycleSurface(t, managerStore, authority.ProbeSurfaceIDs[0], "recovered-native-agent", authority)
}

func TestRuntimeStart_PreparedReplacementSettlesManagedNativePreflightBeforeCommitAdmission(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	ctx := testAuthorActivityContext(context.Background())
	managerStore := newManagedNativeLifecycleStore(managedNativeLifecycleAgent("replacement-native-agent"))
	delivery := &managedNativeRecoveryDeliveryStore{}
	lease := &fakeRuntimeStartupOwnershipLease{}
	ownership := fakeRuntimeStartupOwnershipStore{
		acquire: func(context.Context, string) (runtimestartupownership.Lease, error) {
			return lease, nil
		},
	}

	predecessor, err := newScopedTestRuntime(t, ctx, RuntimeDeps{
		Config: &config.Config{},
		Stores: managedNativeLifecycleStores(
			managerStore,
			delivery,
			ownership,
		),
		Options: RuntimeOptions{
			SelfCheck:                        false,
			WorkflowModule:                   loadAgentFreeRuntimeWorkflowModule(t),
			LLMRuntime:                       llm.NoopRuntime{},
			DisablePersistentStartupRecovery: true,
		},
	})
	if err != nil {
		t.Fatalf("NewRuntime(predecessor): %v", err)
	}
	if err := predecessor.PrepareAuthorActivityCatalog(); err != nil {
		t.Fatalf("PrepareAuthorActivityCatalog(predecessor): %v", err)
	}
	if err := predecessor.Start(ctx); err != nil {
		t.Fatalf("Start(predecessor): %v", err)
	}
	if err := predecessor.QuiesceForReplacement(DefaultShutdownOptions()); err != nil {
		t.Fatalf("QuiesceForReplacement(predecessor): %v", err)
	}

	probeRuntime := &managedNativeStartupProbeRuntime{}
	candidate, err := newScopedTestRuntime(t, ctx, RuntimeDeps{
		Config: managedNativeLifecycleConfig(true),
		Stores: managedNativeLifecycleStores(
			managerStore,
			delivery,
			ownership,
		),
		Options: managedNativeLifecycleOptions(t, "replacement-native-agent", probeRuntime),
	})
	if err != nil {
		t.Fatalf("NewRuntime(candidate): %v", err)
	}
	if err := candidate.PrepareAuthorActivityCatalog(); err != nil {
		t.Fatalf("PrepareAuthorActivityCatalog(candidate): %v", err)
	}
	handoff, err := candidate.PrepareStartupOwnershipHandoff(predecessor)
	if err != nil {
		t.Fatalf("PrepareStartupOwnershipHandoff: %v", err)
	}
	if err := candidate.Start(ctx); err != nil {
		t.Fatalf("Start(candidate): %v", err)
	}

	authority, err := handoff.typed.Authority()
	if err != nil {
		t.Fatalf("handoff authority: %v", err)
	}
	if authority.State != runtimestartupownership.StateProbeSettled {
		t.Fatalf("handoff authority state = %q, want probe_settled before commit", authority.State)
	}
	if len(authority.ProbeSurfaceIDs) != 1 {
		t.Fatalf("handoff probe surface IDs = %v, want one", authority.ProbeSurfaceIDs)
	}
	if candidate.startupAdmission.ID != "" {
		t.Fatalf("candidate received managed execution admission before handoff commit: %#v", candidate.startupAdmission)
	}
	if got := probeRuntime.probe.calls; !slices.Equal(got, []string{"replacement-native-agent"}) {
		t.Fatalf("startup probe calls = %v, want replacement agent", got)
	}
	requireManagedNativeLifecycleSurface(t, managerStore, authority.ProbeSurfaceIDs[0], "replacement-native-agent", authority)

	if err := handoff.Commit(); err != nil {
		t.Fatalf("Commit handoff: %v", err)
	}
	if candidate.startupAdmission.ID == "" {
		t.Fatal("candidate did not receive managed execution admission after handoff commit")
	}
	if !slices.Equal(candidate.startupAdmission.CapabilitySurfaceIDs, authority.ProbeSurfaceIDs) {
		t.Fatalf("candidate admission surfaces = %v, pre-commit handoff surfaces = %v", candidate.startupAdmission.CapabilitySurfaceIDs, authority.ProbeSurfaceIDs)
	}
	if err := handoff.Finalize(); err != nil {
		t.Fatalf("Finalize handoff: %v", err)
	}
	if err := predecessor.Shutdown(); err != nil {
		t.Fatalf("Shutdown(predecessor): %v", err)
	}
}

func managedNativeLifecycleConfig(recovery bool) *config.Config {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	cfg.Runtime.RecoveryOnStartup = recovery
	return cfg
}

func managedNativeLifecycleOptions(t *testing.T, agentID string, modelRuntime llm.Runtime) RuntimeOptions {
	t.Helper()
	return RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: loadAgentFreeRuntimeWorkflowModule(t),
		LLMRuntime:     modelRuntime,
		WorkspaceLifecycle: claudeStartupWorkspaceStub{
			target: &workspace.Target{Container: "swarm-agent-" + agentID, Workdir: "/workspace"},
		},
		EnableToolGateway:  true,
		ToolGatewayBinding: testToolGatewayBinding("http://127.0.0.1:18081", "http://host.docker.internal:18081", "gateway-token"),
		ProviderCredentials: testProviderCredentialStore(
			t,
			"CLAUDE_CODE_OAUTH_TOKEN",
			"oauth-token",
		),
	}
}

func managedNativeLifecycleStores(
	managerStore *managedNativeLifecycleStore,
	delivery runtimedelivery.Store,
	ownership runtimestartupownership.Store,
) Stores {
	eventStore := startupRecoveryMinimalEventStore{}
	return Stores{
		PipelineObligations: eventStore.PipelineObligations(),
		EventStore:          eventStore,
		DeliveryStore:       delivery,
		ManagerStore:        managerStore,
		StartupOwnership:    ownership,
	}
}

func managedNativeLifecycleAgent(agentID string) runtimemanager.PersistedAgent {
	return runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            agentID,
			Role:          "researcher",
			FlowID:        "global",
			Type:          "stub",
			Model:         "regular",
			ExecutionMode: "live",
			Config:        json.RawMessage(`{"system_prompt":"Research current facts."}`),
			NativeTools:   runtimeactors.NativeToolConfig{WebSearch: true},
		},
		Status:    "active",
		HiredBy:   "managed-native-lifecycle-test",
		StartedAt: time.Now().UTC(),
	}
}

func requireManagedNativeLifecycleSurface(
	t *testing.T,
	store *managedNativeLifecycleStore,
	surfaceID string,
	agentID string,
	authority runtimestartupownership.Authority,
) {
	t.Helper()
	surface, err := store.surface(surfaceID)
	if err != nil {
		t.Fatalf("load managed native lifecycle surface: %v", err)
	}
	if err := validateManagedNativeLifecycleSurface(surface, agentID, authority); err != nil {
		t.Fatal(err)
	}
}

func validateManagedNativeLifecycleSurface(
	surface managedcapabilities.Surface,
	agentID string,
	authority runtimestartupownership.Authority,
) error {
	if surface.ActorID != agentID {
		return fmt.Errorf("startup surface actor = %q, want %q", surface.ActorID, agentID)
	}
	if surface.Authority.Kind != managedcapabilities.AuthorityStartupProbe ||
		surface.Authority.ExecutionKind != managedcapabilities.ExecutionNormalAgent ||
		surface.Authority.ExecutionAuthorityID != authority.AuthorityID ||
		surface.Authority.StartupOwnerID != authority.OwnerID ||
		surface.Authority.StartupGeneration != authority.Generation {
		return fmt.Errorf("startup surface authority = %#v, want authority id %s owner %s generation %d", surface.Authority, authority.AuthorityID, authority.OwnerID, authority.Generation)
	}
	if got := surface.EffectiveNames(); !slices.Equal(got, []string{"web_search"}) {
		return fmt.Errorf("effective startup capabilities = %v, want [web_search]", got)
	}
	if got := surface.PlannedBindingNames(managedcapabilities.BindingProviderBuiltin); !slices.Equal(got, []string{"WebFetch", "WebSearch"}) {
		return fmt.Errorf("provider builtin bindings = %v, want [WebFetch WebSearch]", got)
	}
	for _, kind := range []managedcapabilities.BindingKind{
		managedcapabilities.BindingAPIDefinition,
		managedcapabilities.BindingLocalRuntime,
		managedcapabilities.BindingMCPTool,
		managedcapabilities.BindingMCPProvider,
	} {
		if got := surface.PlannedBindingNames(kind); len(got) != 0 {
			return fmt.Errorf("startup surface contains forbidden %s fallback bindings %v", kind, got)
		}
	}
	return nil
}
