package runtime

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
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

type managedNativeDurableRoles struct {
	runtimerunlifecycle.OperationOwner
}

func (managedNativeDurableRoles) RunRuntimeMutationContext(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}
func (managedNativeDurableRoles) RequestCompletionCandidate(context.Context, runtimerunlifecycle.CandidateRequest) (runtimerunlifecycle.CandidateRequestDisposition, error) {
	return "", fmt.Errorf("unexpected managed-native completion candidate")
}
func (managedNativeDurableRoles) TransitionActiveRun(context.Context, runtimerunlifecycle.ActiveTransitionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return "", fmt.Errorf("unexpected managed-native active run transition")
}
func (managedNativeDurableRoles) MarkTerminalRun(context.Context, runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return runtimerunlifecycle.Snapshot{}, "", fmt.Errorf("unexpected managed-native terminal run transition")
}
func (managedNativeDurableRoles) UpsertFlowInstanceRoute(context.Context, runtimebus.FlowInstanceRouteRecord) error {
	return nil
}
func (managedNativeDurableRoles) DeleteFlowInstanceRoute(context.Context, runtimeflowidentity.Route) error {
	return nil
}
func (managedNativeDurableRoles) ListFlowInstanceRoutes(context.Context) ([]runtimeflowidentity.Route, error) {
	return nil, nil
}
func (managedNativeDurableRoles) ReplaceFlowInstanceRouteRecords(context.Context, runtimeflowidentity.Route, []runtimebus.FlowInstanceRouteRecord) error {
	return nil
}
func (managedNativeDurableRoles) ReplaceFlowInstanceRouteTopology(context.Context, []runtimebus.FlowInstanceRouteRecordSet) error {
	return nil
}
func (managedNativeDurableRoles) ListFlowInstanceRouteRecords(context.Context, runtimeflowidentity.Route) ([]runtimebus.FlowInstanceRouteRecord, error) {
	return nil, nil
}
func (managedNativeDurableRoles) RollbackFlowInstanceRoute(context.Context, runtimeflowidentity.Route) error {
	return nil
}
func (managedNativeDurableRoles) ListActiveAgentDescriptors(context.Context) ([]runtimebus.ActiveAgentDescriptor, error) {
	return nil, nil
}
func (managedNativeDurableRoles) ListActiveFlowInstanceDescriptors(context.Context) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	return nil, nil
}
func (managedNativeDurableRoles) ListSelectedRunTargetOwners(context.Context) ([]runtimebus.ActiveTargetDescriptor, error) {
	return nil, nil
}
func (managedNativeDurableRoles) ListEventDeliveryRoutes(context.Context, string) ([]events.DeliveryRoute, error) {
	return nil, nil
}
func (managedNativeDurableRoles) RecordDeadLetter(context.Context, runtimedeadletters.Record) error {
	return nil
}
func (managedNativeDurableRoles) LoadRunOrigin(context.Context, string) (runtimerunlifecycle.RunOrigin, error) {
	return runtimerunlifecycle.ScenarioSetupRunOrigin(), nil
}

func runtimeTestSyntheticDurableDependencies(delivery runtimedelivery.Store) runtimebus.DurableDependencies {
	roles := managedNativeDurableRoles{}
	return runtimebus.DurableDependencies{
		RunLifecycle: roles, DeliveryLifecycle: delivery,
		FlowRoutes: roles, FlowRouteRecords: roles, FlowRouteSets: roles,
		FlowRouteTopology: roles, FlowRouteRollback: roles, ActiveAgents: roles, ActiveFlows: roles, TargetOwners: roles,
		DeliveryRouteSets:     roles,
		TargetFailureRecorder: roles, RunOrigins: roles,
	}
}

func (*managedNativeRecoveryDeliveryStore) ActivateDeliveryAuthority(
	context.Context,
	runtimedelivery.ExecutionAuthority,
) error {
	return nil
}

func (*managedNativeRecoveryDeliveryStore) InspectDeliveryRecovery(
	context.Context,
	runtimecorrelation.BundleSourceFact,
) (runtimedelivery.RecoveryInventory, error) {
	return runtimedelivery.RecoveryInventory{}, nil
}

func (*managedNativeRecoveryDeliveryStore) ObserveDeliveryContinuation(
	context.Context,
	runtimedelivery.ExecutionAuthority,
	string,
) (runtimedelivery.ContinuationObservation, error) {
	return runtimedelivery.ContinuationObservation{Disposition: runtimedelivery.ClaimAbsent}, nil
}

func (s *managedNativeRecoveryDeliveryStore) ScanDeliveryContinuations(
	ctx context.Context,
	_ runtimedelivery.ExecutionAuthority,
	_ runtimedelivery.ContinuationCursor,
	limit int,
) (runtimedelivery.ContinuationPage, error) {
	s.claimCalls.Add(1)
	if s.onClaim != nil {
		if err := s.onClaim(ctx); err != nil {
			return runtimedelivery.ContinuationPage{}, err
		}
	}
	return runtimedelivery.ContinuationPage{Exhausted: true}, nil
}

func TestRuntimeStart_RecoveryHydratesManagedNativePreflightBeforeReplayAdmission(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	ctx := testAuthorActivityContext(context.Background())
	managerStore := newManagedNativeLifecycleStore(managedNativeLifecycleAgent(t, "recovered-native-agent"))
	delivery := &managedNativeRecoveryDeliveryStore{}
	lease := &fakeRuntimeStartupOwnershipLease{}
	ownership := fakeRuntimeStartupOwnershipStore{
		acquire: func(context.Context, string) (runtimestartupownership.Lease, error) {
			return lease, nil
		},
	}
	probeRuntime := &managedNativeStartupProbeRuntime{}
	cfg := managedNativeLifecycleConfig(true)
	deps := managedNativeLifecycleDeps(
		managerStore,
		delivery,
		ownership,
	)
	deps.Config = cfg
	deps.Options = managedNativeLifecycleOptions(t, "recovered-native-agent", probeRuntime)
	rt, err := newScopedTestRuntime(t, ctx, deps)
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
	managerStore := newManagedNativeLifecycleStore(managedNativeLifecycleAgent(t, "replacement-native-agent"))
	delivery := &managedNativeRecoveryDeliveryStore{}
	lease := &fakeRuntimeStartupOwnershipLease{}
	ownership := fakeRuntimeStartupOwnershipStore{
		acquire: func(context.Context, string) (runtimestartupownership.Lease, error) {
			return lease, nil
		},
	}

	predecessorDeps := managedNativeLifecycleDeps(
		managerStore,
		delivery,
		ownership,
	)
	predecessorDeps.Config = &config.Config{}
	predecessorDeps.Options = RuntimeOptions{
		SelfCheck:                        false,
		WorkflowModule:                   loadAgentFreeRuntimeWorkflowModule(t),
		LLMRuntime:                       llm.NoopRuntime{},
		DisablePersistentStartupRecovery: true,
	}
	predecessor, err := newScopedTestRuntime(t, ctx, predecessorDeps)
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
	candidateDeps := managedNativeLifecycleDeps(
		managerStore,
		delivery,
		ownership,
	)
	candidateDeps.Config = managedNativeLifecycleConfig(true)
	candidateDeps.Options = managedNativeLifecycleOptions(t, "replacement-native-agent", probeRuntime)
	candidate, err := newScopedTestRuntime(t, ctx, candidateDeps)
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

func managedNativeLifecycleDeps(
	managerStore *managedNativeLifecycleStore,
	delivery runtimedelivery.Store,
	ownership runtimestartupownership.Store,
) RuntimeDeps {
	eventStore := startupRecoveryMinimalEventStore{}
	return RuntimeDeps{
		EventStore:               eventStore,
		EventBusDurable:          runtimeTestSyntheticDurableDependencies(delivery),
		PipelineObligations:      eventStore.PipelineObligations(),
		DeliveryStore:            delivery,
		ManagerStore:             managerStore,
		EffectsStore:             managerStore,
		ManagedCapabilitiesStore: managerStore,
		StartupOwnership:         ownership,
	}
}

func managedNativeLifecycleAgent(t testing.TB, agentID string) runtimemanager.PersistedAgent {
	t.Helper()
	return runtimemanager.PersistedAgent{
		Config: runtimeTestAgentConfig(t, runtimeactors.AgentConfig{
			ID:            agentID,
			Identity:      agentidentitytest.RootRuntime(t, agentID, "runtime-test/managed-native-lifecycle"),
			Role:          "researcher",
			FlowID:        "global",
			Type:          "stub",
			Model:         "regular",
			ExecutionMode: "live",
			NativeTools:   runtimeactors.NativeToolConfig{WebSearch: true},
		}),
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
