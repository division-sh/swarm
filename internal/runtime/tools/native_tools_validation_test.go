package tools

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type nativeCapabilityRuntimeStub struct {
	llm.NoopRuntime
	caps   llm.NativeToolCapabilities
	strict bool
}

func (s nativeCapabilityRuntimeStub) ProviderContract() llm.ProviderContract {
	contract := llm.AnthropicAPIProviderContract()
	contract.NativeTools.Capabilities = s.caps
	contract.NativeTools.StrictProviderNativeSupport = s.strict
	contract.NativeTools.FallbackToolsAllowed = !s.strict
	return contract
}

type staticAgentRuntimeResolver struct {
	runtime llm.Runtime
}

type nativeCapabilityAdmissionWorkspace struct {
	regularCalls   *int
	admissionCalls *int
	target         *workspace.Target
}

func (s nativeCapabilityAdmissionWorkspace) ResolveWorkspace(context.Context, models.AgentConfig) (*workspace.Target, error) {
	(*s.regularCalls)++
	return nil, fmt.Errorf("execution resolver was called during boot admission")
}

func (s nativeCapabilityAdmissionWorkspace) ResolveWorkspaceForCapabilityAdmission(context.Context, models.AgentConfig) (*workspace.Target, error) {
	(*s.admissionCalls)++
	return s.target, nil
}

func (r staticAgentRuntimeResolver) ResolveAgentRuntime(actor models.AgentConfig) (llm.AgentRuntimeResolution, error) {
	return llm.AgentRuntimeResolution{Actor: actor, Runtime: r.runtime}, nil
}

type mappedAgentRuntimeResolver map[string]llm.Runtime

func (r mappedAgentRuntimeResolver) ResolveAgentRuntime(actor models.AgentConfig) (llm.AgentRuntimeResolution, error) {
	return llm.AgentRuntimeResolution{Actor: actor, Runtime: r[strings.TrimSpace(actor.ID)]}, nil
}

type mockCapabilityRuntimeStub struct {
	llm.NoopRuntime
}

func (mockCapabilityRuntimeStub) ProviderContract() llm.ProviderContract {
	return llm.MockProviderContract()
}

func (mockCapabilityRuntimeStub) ProbeStartupVisibleToolSurface(context.Context, models.AgentConfig, string, []llm.ToolDefinition) (*llm.Response, error) {
	return nil, errors.New("native capability fixture cannot prove workspace startup transport")
}

func nativeCapabilityRuntimeSet(t testing.TB, runtime llm.Runtime) *llm.AgentRuntimeSet {
	t.Helper()
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendAnthropic)
	if err != nil {
		t.Fatalf("resolve anthropic profile: %v", err)
	}
	runtimes, err := llm.NewAgentRuntimeSet(profile, llm.RuntimeFactory{}, runtime)
	if err != nil {
		t.Fatalf("build native capability runtime set: %v", err)
	}
	return runtimes
}

func TestNativeToolBootAdmissionDoesNotConstructProviderOrWorkspaceRuntime(t *testing.T) {
	source := wrapRootAgentBundle(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent": {ID: "agent", NativeTools: map[string]any{"bash": true, "file_io": true, "web_search": true}},
		},
	})
	profile, err := llmselection.ResolveLiveBackend(llmselection.BackendClaudeCLI)
	if err != nil {
		t.Fatal(err)
	}
	providers, err := llm.NewAgentProviderContracts(profile)
	if err != nil {
		t.Fatal(err)
	}
	runtimes, err := llm.NewAgentRuntimeSet(profile, llm.RuntimeFactory{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	regularCalls, admissionCalls := 0, 0
	workspaces := nativeCapabilityAdmissionWorkspace{regularCalls: &regularCalls, admissionCalls: &admissionCalls}
	for _, resolver := range []llm.AgentProviderContractResolver{providers, runtimes} {
		if _, err := ValidateNativeToolBootConfig(context.Background(), executionposture.Live, nil, source, nil, resolver, workspaces); err != nil {
			t.Fatalf("static native admission with %T: %v", resolver, err)
		}
	}
	if regularCalls != 0 || admissionCalls != 0 {
		t.Fatalf("provider-native capabilities resolved workspace: execution=%d, admission=%d", regularCalls, admissionCalls)
	}
}

func TestNativeToolAdmissionRejectsInvalidContractBeforeCapabilityOrFallback(t *testing.T) {
	actor := models.AgentConfig{ID: "actor", NativeTools: models.NativeToolConfig{Bash: true}}
	contract := llm.AnthropicAPIProviderContract()
	contract.Provider = ""
	contract.NativeTools.Capabilities.Bash = true
	if err := ValidateNativeToolAgentAdmission(context.Background(), actor, NativeToolAdmissionOptions{ProviderContract: contract}); err == nil || !strings.Contains(err.Error(), "provider is required") {
		t.Fatalf("invalid provider contract admitted native capability: %v", err)
	}
}

func TestNativeToolBootAdmissionConsumesReadOnlyWorkspaceInspection(t *testing.T) {
	source := wrapRootAgentBundle(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent": {ID: "agent", NativeTools: map[string]any{"bash": true, "file_io": true}},
		},
	})
	profile, err := llmselection.ResolveLiveBackend(llmselection.BackendAnthropic)
	if err != nil {
		t.Fatal(err)
	}
	providers, err := llm.NewAgentProviderContracts(profile)
	if err != nil {
		t.Fatal(err)
	}
	cfg := workspace.DefaultHostConfig()
	cfg.WorkspaceRoot = filepath.Join(t.TempDir(), "not-created")
	inspection, err := workspace.NewHostCapabilityInspection(cfg, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateNativeToolBootConfig(context.Background(), executionposture.Live, nil, source, nil, providers, inspection); err != nil {
		t.Fatalf("read-only native fallback admission: %v", err)
	}
	if _, err := os.Stat(cfg.WorkspaceRoot); !os.IsNotExist(err) {
		t.Fatalf("native admission created workspace root: %v", err)
	}
}

func TestNativeToolBootAdmissionRejectsMissingSourceAndPreservesCancellation(t *testing.T) {
	if _, err := ValidateNativeToolBootConfig(context.Background(), executionposture.Live, nil, nil, nil, nil, nil); err == nil || !strings.Contains(err.Error(), "requires semantic source") {
		t.Fatalf("missing source was admitted: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ValidateNativeToolBootConfig(ctx, executionposture.Live, nil, nil, nil, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation was hidden: %v", err)
	}
}

func TestNativeToolStaticInspectionUsesLoadedScopedDeclarations(t *testing.T) {
	profile, err := llmselection.ResolveLiveBackend(llmselection.BackendAnthropic)
	if err != nil {
		t.Fatal(err)
	}
	providers, err := llm.NewAgentProviderContracts(profile)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		name         string
		load         func(*testing.T) semanticview.Source
		declarations int
	}{
		{"project-and-flow", scopedNativeToolAgentFixture, 4},
		{"same-flow-projects", sameFlowScopedNativeToolAgentFixture, 2},
		{"per-instance-route", scopedFlowWorkspaceNativeToolFixture, 1},
	} {
		t.Run(row.name, func(t *testing.T) {
			source := row.load(t)
			if got := len(semanticview.AgentDeclarations(source)); got != row.declarations {
				t.Fatalf("loaded declaration census = %d, want %d", got, row.declarations)
			}
			cfg := workspace.DefaultHostConfig()
			cfg.WorkspaceRoot = filepath.Join(t.TempDir(), "not-created")
			inspection, err := workspace.NewHostCapabilityInspection(cfg, source)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ValidateNativeToolBootConfig(context.Background(), executionposture.Live, nil, source, nil, providers, inspection); err != nil {
				t.Fatalf("loaded scoped native admission: %v", err)
			}
			if _, err := os.Stat(cfg.WorkspaceRoot); !os.IsNotExist(err) {
				t.Fatalf("loaded admission materialized a workspace: %v", err)
			}
		})
	}
}

func TestExecutorNativeToolAdmissionUsesActorSelectedProviderContract(t *testing.T) {
	if response, err := (mockCapabilityRuntimeStub{}).ProbeStartupVisibleToolSurface(context.Background(), models.AgentConfig{}, "", nil); err == nil || response != nil {
		t.Fatalf("native capability fixture claimed startup proof: response=%#v err=%v", response, err)
	}
	mockActor := models.AgentConfig{ID: "mock-agent", NativeTools: models.NativeToolConfig{FileIO: true}}
	liveActor := models.AgentConfig{ID: "live-agent", NativeTools: models.NativeToolConfig{FileIO: true}}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{
		ModelRuntimes: mappedAgentRuntimeResolver{
			mockActor.ID: mockCapabilityRuntimeStub{},
			liveActor.ID: nativeCapabilityRuntimeStub{},
		},
		WorkspaceResolver: relayWorkspaceResolverStub{
			target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()},
		},
	})

	_, mockOpts, err := exec.nativeToolAdmissionOptions(mockActor)
	if err != nil {
		t.Fatalf("mock nativeToolAdmissionOptions: %v", err)
	}
	_, liveOpts, err := exec.nativeToolAdmissionOptions(liveActor)
	if err != nil {
		t.Fatalf("live nativeToolAdmissionOptions: %v", err)
	}
	mockContract := mockOpts.ProviderContract
	liveContract := liveOpts.ProviderContract
	if mockContract.Provider != llmselection.ProviderMock || liveContract.Provider != llmselection.ProviderAnthropic {
		t.Fatalf("actor provider contracts = mock:%q live:%q", mockContract.Provider, liveContract.Provider)
	}
	if err := ValidateNativeToolAgentAdmission(unmanagedToolTestContext(), mockActor, mockOpts); err == nil || !strings.Contains(err.Error(), "does not allow native tool fallback") {
		t.Fatalf("mock native admission error = %v, want mock fallback denial", err)
	}
	if err := ValidateNativeToolAgentAdmission(unmanagedToolTestContext(), liveActor, liveOpts); err != nil {
		t.Fatalf("live native admission: %v", err)
	}
}

func TestExecutorNativeToolCapabilityAdmissionUsesNonExecutingResolver(t *testing.T) {
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "live-agent",
		NativeTools:   models.NativeToolConfig{FileIO: true},
	}
	regularCalls := 0
	admissionCalls := 0
	exec := NewExecutorWithOptions(nil, ExecutorOptions{
		ModelRuntimes: mappedAgentRuntimeResolver{actor.ID: nativeCapabilityRuntimeStub{}},
		WorkspaceResolver: nativeCapabilityAdmissionWorkspace{
			regularCalls: &regularCalls, admissionCalls: &admissionCalls,
			target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()},
		},
	})

	if err := exec.ValidateNativeToolCapabilityAdmission(unmanagedToolTestContext(), actor); err != nil {
		t.Fatalf("ValidateNativeToolCapabilityAdmission: %v", err)
	}
	if regularCalls != 0 || admissionCalls != 2 {
		t.Fatalf("workspace resolutions = regular:%d admission:%d, want 0/2", regularCalls, admissionCalls)
	}
}

func TestValidateNativeToolBootConfig_FailsClosedWhenRuntimeLacksNativeCapability(t *testing.T) {
	source := wrapRootAgentBundle(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {
				ID: "agent-1",
				NativeTools: map[string]any{
					"web_search": true,
				},
			},
		},
	})

	_, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), executionposture.Live, nil, source, nil, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{strict: true}), nil)
	if err == nil || !strings.Contains(err.Error(), "selected runtime is strict provider-native and does not support provider-native capability") {
		t.Fatalf("expected unsupported native capability error, got %v", err)
	}
}

func TestValidateNativeToolBootConfig_CLINativeWebSearchDoesNotRequireFallbackProviderPolicy(t *testing.T) {
	source := wrapRootAgentBundle(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {
				ID: "agent-1",
				NativeTools: map[string]any{
					"web_search": true,
				},
			},
		},
	})

	warnings, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), executionposture.Live, nil, source, nil, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{
		caps:   llm.NativeToolCapabilities{WebSearch: true},
		strict: true,
	}), nil)
	if err != nil {
		t.Fatalf("ValidateNativeToolBootConfig: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
}

func TestValidateNativeToolBootConfig_NonCLIRuntimeRequiresWebSearchFallbackCredential(t *testing.T) {
	source := wrapRootAgentBundle(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {
				ID: "agent-1",
				NativeTools: map[string]any{
					"web_search": true,
				},
			},
		},
		Policy: runtimecontracts.PolicyDocument{
			Values: map[string]runtimecontracts.PolicyValue{
				"web_search_provider": {
					Value: map[string]any{
						"provider":        "brave",
						"credentials_key": "brave_search_api_key",
					},
				},
			},
		},
	})

	emptyStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "empty-credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore empty: %v", err)
	}
	_, err = ValidateNativeToolBootConfig(unmanagedToolTestContext(), executionposture.Live, nil, source, emptyStore, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{}), nil)
	if err == nil || !strings.Contains(err.Error(), `missing credential "brave_search_api_key"`) {
		t.Fatalf("ValidateNativeToolBootConfig error = %v, want missing web_search credential", err)
	}

	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(unmanagedToolTestContext(), "brave_search_api_key", "secret"); err != nil {
		t.Fatalf("Set credential: %v", err)
	}
	warnings, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), executionposture.Live, nil, source, store, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{}), nil)
	if err != nil {
		t.Fatalf("ValidateNativeToolBootConfig with credential: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
}

func TestValidateNativeToolBootConfig_CommandPurposeOwnsNativeAdmission(t *testing.T) {
	source := wrapRootAgentBundle(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {
				ID: "agent-1",
				Mock: mockperformance.Performance{
					Kind:   mockperformance.KindPython,
					Module: "mocks/agent.py",
					Source: []byte("def handle(input):\n    return {'text': 'mock'}\n"),
					Digest: "sha256:native-tool-boot-mock",
				},
				NativeTools: map[string]any{
					"web_search": true,
				},
			},
		},
		Policy: runtimecontracts.PolicyDocument{
			Values: map[string]runtimecontracts.PolicyValue{
				"web_search_provider": {
					Value: map[string]any{
						"provider":        "brave",
						"credentials_key": "brave_search_api_key",
					},
				},
			},
		},
	})
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendAnthropic)
	if err != nil {
		t.Fatalf("ResolveActiveBackend: %v", err)
	}
	harness := effecttest.New()
	runtimes, err := llm.NewAgentRuntimeSet(profile, llm.RuntimeFactory{
		Cfg: &config.Config{LLM: config.LLMConfig{Backend: llmselection.BackendAnthropic}},
		CompletionController: liveTestCompletionController(
			harness,
			harness,
			harness,
			harness,
		),
	}, nativeCapabilityRuntimeStub{})
	if err != nil {
		t.Fatalf("NewAgentRuntimeSet: %v", err)
	}

	warnings, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), executionposture.MockOnly, nil, source, nil, runtimes, nil)
	if err != nil {
		t.Fatalf("ValidateNativeToolBootConfig: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
	if _, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), executionposture.Live, nil, source, nil, runtimes, nil); err == nil || !strings.Contains(err.Error(), "credential store is not configured") {
		t.Fatalf("same authored double must not waive live credentials: %v", err)
	}
	if _, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), executionposture.MockOnly, nil, source, nil, runtimes, nil); err != nil {
		t.Fatalf("live admission mutated the source double: %v", err)
	}
}

func TestValidateNativeToolBootConfig_FallbackFileIORequiresWorkspaceExecutionTarget(t *testing.T) {
	source := wrapRootAgentBundle(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {
				ID: "agent-1",
				NativeTools: map[string]any{
					"file_io": true,
				},
			},
		},
	})

	_, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), executionposture.Live, nil, source, nil, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{}), nil)
	if err == nil || !strings.Contains(err.Error(), "workspace resolver is not configured") {
		t.Fatalf("ValidateNativeToolBootConfig error = %v, want missing workspace resolver", err)
	}

	warnings, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), executionposture.Live, nil, source, nil, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{}), relayWorkspaceResolverStub{
		target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()},
	})
	if err != nil {
		t.Fatalf("ValidateNativeToolBootConfig with workspace: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
}

func TestValidateNativeToolBootConfigUsesNonExecutingCapabilityAdmission(t *testing.T) {
	source := wrapRootAgentBundle(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {
				ID: "agent-1",
				NativeTools: map[string]any{
					"file_io": true,
				},
			},
		},
	})
	regularCalls := 0
	admissionCalls := 0
	resolver := nativeCapabilityAdmissionWorkspace{
		regularCalls: &regularCalls, admissionCalls: &admissionCalls,
		target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()},
	}

	if _, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), executionposture.Live, nil, source, nil, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{}), resolver); err != nil {
		t.Fatalf("ValidateNativeToolBootConfig: %v", err)
	}
	if regularCalls != 0 || admissionCalls != 2 {
		t.Fatalf("workspace resolutions = regular:%d admission:%d, want 0/2", regularCalls, admissionCalls)
	}
}

func TestValidateNativeToolBootConfigCensusesScopedAgentsHiddenByAmbiguousAlias(t *testing.T) {
	source := scopedNativeToolAgentFixture(t)
	_, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), executionposture.Live, nil, source, nil, nil, nil)
	if err == nil {
		t.Fatal("ValidateNativeToolBootConfig unexpectedly ignored scoped native-tool agents")
	}
	for _, want := range []string{
		"flow departments/project-a agent shared-worker",
		"flow departments/project-b agent shared-worker",
		"flow flow-a agent shared-worker",
		"flow flow-b agent shared-worker",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("native admission error = %v, want %q", err, want)
		}
	}
}

func TestValidateNativeToolBootConfigResolvesDistinctProjectAndFlowOwners(t *testing.T) {
	source := scopedNativeToolAgentFixture(t)
	warnings, err := ValidateNativeToolBootConfig(
		unmanagedToolTestContext(), executionposture.Live, nil,
		source,
		nil,
		nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{}),
		relayWorkspaceResolverStub{target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()}},
	)
	if err != nil {
		t.Fatalf("ValidateNativeToolBootConfig: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
}

func TestValidateNativeToolBootConfigValidatesScopedFlowRouteWithoutMaterializingWorkspace(t *testing.T) {
	source := scopedFlowWorkspaceNativeToolFixture(t)
	root := filepath.Join(t.TempDir(), "workspaces")
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "schema.yaml"), []byte("name: scoped-flow-native-tool-workspace\n"), 0o644); err != nil {
		t.Fatalf("write flow schema: %v", err)
	}
	projection := toolTestRuntimeSourceProjection(t, contractsDir)
	workspaces := workspace.NewHostManager()
	workspaces.SetConfigForTest(workspace.HostConfig{
		WorkspaceRoot:    root,
		SourceProjection: projection,
		SourceMountPoint: "/opt/swarm/source",
	})
	bindToolTestHostProjection(t, workspaces, projection)
	workspaces.SetSemanticSource(source)

	warnings, err := ValidateNativeToolBootConfig(
		unmanagedToolTestContext(), executionposture.Live, nil,
		source,
		nil,
		nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{}),
		workspaces,
	)
	if err != nil {
		t.Fatalf("ValidateNativeToolBootConfig: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
	backingKey, err := workspace.DurableBackingKey(projection.BundleHash(), workspace.DurableBackingFlow, "review")
	if err != nil {
		t.Fatal(err)
	}
	bundleScope := "bundle-" + strings.TrimPrefix(projection.BundleHash(), "bundle-v2:sha256:")
	if _, err := os.Stat(filepath.Join(root, bundleScope, backingKey)); !os.IsNotExist(err) {
		t.Fatalf("capability admission materialized run-bound scoped flow workspace: %v", err)
	}
}

func TestValidateNativeToolBootConfigResolvesProjectOwnersWithinOneFlow(t *testing.T) {
	source := sameFlowScopedNativeToolAgentFixture(t)
	declarations := semanticview.AgentDeclarations(source)
	if len(declarations) != 2 {
		t.Fatalf("declarations = %#v, want two scoped project agents", declarations)
	}
	for _, declaration := range declarations {
		if owner, ok := semanticview.ScopedAgentDeclarationOwner(source, declaration); !ok || strings.TrimSpace(owner) == "" {
			t.Fatalf("declaration %#v did not resolve an exact owner", declaration)
		}
	}

	warnings, err := ValidateNativeToolBootConfig(
		unmanagedToolTestContext(), executionposture.Live, nil,
		source,
		nil,
		nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{}),
		relayWorkspaceResolverStub{target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()}},
	)
	if err != nil {
		t.Fatalf("ValidateNativeToolBootConfig: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
}

func sameFlowScopedNativeToolAgentFixture(t *testing.T) semanticview.Source {
	t.Helper()
	root := t.TempDir()

	writeToolFlowDataFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: same-flow-scoped-native-tool-census\n")
	flowDir := filepath.Join(root, "operating")

	writeToolFlowDataFixtureFile(t, filepath.Join(flowDir, "schema.yaml"), "name: operating\nstages:\n  active: {}\n")
	for _, project := range []string{"project-a", "project-b"} {
		dir := filepath.Join(flowDir, "departments", project)

		writeToolFlowDataFixtureFile(t, filepath.Join(dir, "agents.yaml"), scopedNativeToolAgentYAML())
	}
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func scopedFlowWorkspaceNativeToolFixture(t *testing.T) semanticview.Source {
	t.Helper()
	root := t.TempDir()

	writeToolFlowDataFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: scoped-flow-native-tool-workspace\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "policy.yaml"), `
workspace_classes:
  shared_flow:
    workspace_scope: per-flow-instance
`)
	flowDir := filepath.Join(root, "review")
	writeToolFlowDataFixtureFile(t, filepath.Join(flowDir, "schema.yaml"), "name: review\nstages:\n  active: {}\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(flowDir, "agents.yaml"), `
scoped-worker:
  model: regular
  intent:
    inline: Validate native-tool workspace admission for this scoped worker.
  workspace_class: shared_flow
  native_tools:
    file_io: true
`)
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func scopedNativeToolAgentFixture(t *testing.T) semanticview.Source {
	t.Helper()
	root := t.TempDir()

	writeToolFlowDataFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: scoped-native-tool-census\n")
	for _, project := range []string{"project-a", "project-b"} {
		dir := filepath.Join(root, "departments", project)

		writeToolFlowDataFixtureFile(t, filepath.Join(dir, "agents.yaml"), scopedNativeToolAgentYAML())
	}
	for _, flowID := range []string{"flow-a", "flow-b"} {
		dir := filepath.Join(root, flowID)
		writeToolFlowDataFixtureFile(t, filepath.Join(dir, "schema.yaml"), "name: "+flowID+"\n")
		writeToolFlowDataFixtureFile(t, filepath.Join(dir, "agents.yaml"), scopedNativeToolAgentYAML())
	}
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func scopedNativeToolAgentYAML() string {
	return `
shared-worker:
  model: regular
  intent:
    inline: Validate native-tool admission for this scoped worker.
  native_tools:
    file_io: true
`
}
