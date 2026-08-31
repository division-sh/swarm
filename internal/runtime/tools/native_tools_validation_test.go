package tools

import (
	"context"
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

func TestExecutorNativeToolAdmissionUsesActorSelectedProviderContract(t *testing.T) {
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
	mockContract, _ := llm.ProviderContractForRuntime(mockOpts.Runtime)
	liveContract, _ := llm.ProviderContractForRuntime(liveOpts.Runtime)
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

	_, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), source, nil, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{strict: true}), nil)
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

	warnings, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), source, nil, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{
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
	_, err = ValidateNativeToolBootConfig(unmanagedToolTestContext(), source, emptyStore, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{}), nil)
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
	warnings, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), source, store, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{}), nil)
	if err != nil {
		t.Fatalf("ValidateNativeToolBootConfig with credential: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
}

func TestValidateNativeToolBootConfig_ExactlyMockedAgentSkipsLiveNativeAdmission(t *testing.T) {
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

	warnings, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), source, nil, runtimes, nil)
	if err != nil {
		t.Fatalf("ValidateNativeToolBootConfig: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
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

	_, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), source, nil, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{}), nil)
	if err == nil || !strings.Contains(err.Error(), "workspace resolver is not configured") {
		t.Fatalf("ValidateNativeToolBootConfig error = %v, want missing workspace resolver", err)
	}

	warnings, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), source, nil, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{}), relayWorkspaceResolverStub{
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

	if _, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), source, nil, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{}), resolver); err != nil {
		t.Fatalf("ValidateNativeToolBootConfig: %v", err)
	}
	if regularCalls != 0 || admissionCalls != 2 {
		t.Fatalf("workspace resolutions = regular:%d admission:%d, want 0/2", regularCalls, admissionCalls)
	}
}

func TestValidateNativeToolBootConfigCensusesScopedAgentsHiddenByAmbiguousAlias(t *testing.T) {
	source := scopedNativeToolAgentFixture(t)
	_, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), source, nil, nil, nil)
	if err == nil {
		t.Fatal("ValidateNativeToolBootConfig unexpectedly ignored scoped native-tool agents")
	}
	for _, want := range []string{
		"project packages/project-a agent shared-worker",
		"project packages/project-b agent shared-worker",
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
		unmanagedToolTestContext(),
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
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: scoped-flow-native-tool-workspace\n"), 0o644); err != nil {
		t.Fatalf("write contracts package: %v", err)
	}
	workspaces := workspace.NewHostManager()
	workspaces.SetConfigForTest(workspace.HostConfig{
		WorkspaceRoot:       root,
		ContractsSource:     contractsDir,
		ContractsMountPoint: "/opt/swarm/contracts",
	})
	workspaces.SetSemanticSource(source)

	warnings, err := ValidateNativeToolBootConfig(
		unmanagedToolTestContext(),
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
	if _, err := os.Stat(filepath.Join(root, "flows", "review")); !os.IsNotExist(err) {
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
		unmanagedToolTestContext(),
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
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: same-flow-scoped-native-tool-census
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: operating
    flow: operating
    mode: static
`)
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: same-flow-scoped-native-tool-census\n")
	flowDir := filepath.Join(root, "flows", "operating")
	writeToolFlowDataFixtureFile(t, filepath.Join(flowDir, "package.yaml"), `
name: operating
version: "1.0.0"
packages:
  - path: packages/project-a
  - path: packages/project-b
flows: []
`)
	writeToolFlowDataFixtureFile(t, filepath.Join(flowDir, "schema.yaml"), "name: operating\nmode: static\ninitial_state: active\nstates: [active]\n")
	for _, project := range []string{"project-a", "project-b"} {
		dir := filepath.Join(flowDir, "packages", project)
		writeToolFlowDataFixtureFile(t, filepath.Join(dir, "package.yaml"), "name: "+project+"\nversion: \"1.0.0\"\nflows: []\n")
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
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: scoped-flow-native-tool-workspace
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: review
    flow: review
    mode: static
`)
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: scoped-flow-native-tool-workspace\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "policy.yaml"), `
workspace_classes:
  shared_flow:
    workspace_scope: per-flow-instance
`)
	flowDir := filepath.Join(root, "flows", "review")
	writeToolFlowDataFixtureFile(t, filepath.Join(flowDir, "schema.yaml"), "name: review\nmode: static\ninitial_state: active\nstates: [active]\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(flowDir, "agents.yaml"), `
scoped-worker:
  id: scoped-worker
  model: regular
  memory: false
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
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: scoped-native-tool-census
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - path: packages/project-a
  - path: packages/project-b
flows:
  - id: flow-a
    flow: flow-a
    mode: static
  - id: flow-b
    flow: flow-b
    mode: static
`)
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: scoped-native-tool-census\n")
	for _, project := range []string{"project-a", "project-b"} {
		dir := filepath.Join(root, "packages", project)
		writeToolFlowDataFixtureFile(t, filepath.Join(dir, "package.yaml"), "name: "+project+"\nversion: \"1.0.0\"\nflows: []\n")
		writeToolFlowDataFixtureFile(t, filepath.Join(dir, "agents.yaml"), scopedNativeToolAgentYAML())
	}
	for _, flowID := range []string{"flow-a", "flow-b"} {
		dir := filepath.Join(root, "flows", flowID)
		writeToolFlowDataFixtureFile(t, filepath.Join(dir, "schema.yaml"), "name: "+flowID+"\nmode: static\ninitial_state: active\nstates: [active]\n")
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
  id: shared-worker
  model: regular
  memory: false
  intent:
    inline: Validate native-tool admission for this scoped worker.
  native_tools:
    file_io: true
`
}
