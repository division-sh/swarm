package workspace

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestWorkspaceClassesForSource(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"workspace_classes": {
				Value: map[string]any{
					"dedicated":   map[string]any{"workspace_scope": "per-agent"},
					"shared_flow": map[string]any{"workspace_scope": "per-flow-instance"},
				},
			},
		}},
	})
	classes, err := workspaceClassesForSource(source)
	if err != nil {
		t.Fatalf("workspaceClassesForSource: %v", err)
	}
	if got := classes["dedicated"]; got != "per-agent" {
		t.Fatalf("dedicated scope = %q, want per-agent", got)
	}
	if got := classes["shared_flow"]; got != "per-flow-instance" {
		t.Fatalf("shared_flow scope = %q, want per-flow-instance", got)
	}
}

func TestValidateSource_RejectsUndefinedWorkspaceClass(t *testing.T) {
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.SharedDataSource = dataDir
	cfg.ContractsSource = contractsDir
	cfg.WorkspaceNetwork = ""
	cfg.WorkspaceImage = "test-image"
	manager.SetConfig(cfg)

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"workspace_classes": {
				Value: map[string]any{
					"dedicated": map[string]any{"workspace_scope": "per-agent"},
				},
			},
		}},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"coordinator": {ID: "coordinator", WorkspaceClass: "missing"},
		},
	})
	err := manager.ValidateSource(context.Background(), source)
	if err == nil || !strings.Contains(err.Error(), `undefined workspace_class "missing"`) {
		t.Fatalf("expected undefined workspace_class error, got %v", err)
	}
}

func TestResolveWorkspace_PerAgentMountsStandardPaths(t *testing.T) {
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.SharedDataSource = dataDir
	cfg.ContractsSource = contractsDir
	cfg.WorkspaceNetwork = ""
	cfg.WorkspaceImage = "test-image"
	manager.SetConfig(cfg)
	manager.SetSemanticSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"workspace_classes": {
				Value: map[string]any{
					"dedicated": map[string]any{"workspace_scope": "per-agent"},
				},
			},
		}},
	}))

	var created []string
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			return "", fmt.Errorf("no such object")
		case "create":
			created = append([]string{}, args...)
			return "", nil
		case "start":
			return "", nil
		default:
			return "", nil
		}
	})
	ctx := runtimecorrelation.WithRunID(context.Background(), "11111111-1111-1111-1111-111111111111")
	target, err := manager.ResolveWorkspace(ctx, models.AgentConfig{
		ID:             "dedicated-agent",
		WorkspaceClass: "dedicated",
	})
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	if target == nil || target.Container != "swarm-agent-dedicated-agent" {
		t.Fatalf("target = %#v, want swarm-agent-dedicated-agent", target)
	}
	joined := strings.Join(created, " ")
	for _, expected := range []string{
		dataDir + ":/data:ro",
		contractsDir + ":/opt/swarm/contracts:ro",
		"workspaces_agent_dedicated-agent:/workspace",
		"--label dev.swarm.container.kind=agent",
		"--label dev.swarm.reset.eligible=true",
		"--label dev.swarm.agent_id=dedicated-agent",
		"--label dev.swarm.run_id=11111111-1111-1111-1111-111111111111",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("create args missing %q: %v", expected, created)
		}
	}
}

func TestResolveWorkspace_BundleScopeDisambiguatesContainersVolumesAndLabels(t *testing.T) {
	const bundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.SharedDataSource = dataDir
	cfg.ContractsSource = contractsDir
	cfg.WorkspaceNetwork = ""
	cfg.WorkspaceImage = "test-image"
	manager.SetConfig(cfg)
	manager.SetBundleScope(bundleHash)
	manager.SetSemanticSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"workspace_classes": {
				Value: map[string]any{
					"dedicated": map[string]any{"workspace_scope": "per-agent"},
				},
			},
		}},
	}))

	var creates [][]string
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			return "", fmt.Errorf("no such object")
		case "create":
			creates = append(creates, append([]string{}, args...))
			return "", nil
		case "start":
			return "", nil
		default:
			return "", nil
		}
	})
	ctx := runtimecorrelation.WithRunID(context.Background(), "11111111-1111-1111-1111-111111111111")
	target, err := manager.ResolveWorkspace(ctx, models.AgentConfig{
		ID:             "dedicated-agent",
		WorkspaceClass: "dedicated",
	})
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	if target == nil || target.Container != "swarm-bundle-aaaaaaaaaaaa-agent-dedicated-agent" {
		t.Fatalf("target = %#v, want bundle-scoped agent container", target)
	}
	joined := flattenDockerCalls(creates)
	for _, expected := range []string{
		"workspaces_swarm_bundle_aaaaaaaaaaaa_agent_dedicated_agent:/workspace",
		"--label dev.swarm.bundle_hash=" + bundleHash,
		"--label dev.swarm.container.name=swarm-bundle-aaaaaaaaaaaa-agent-dedicated-agent",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("bundle-scoped agent workspace create args missing %q:\n%s", expected, joined)
		}
	}

	creates = nil
	if err := manager.EnsureEntityWorkspace(ctx, "acme"); err != nil {
		t.Fatalf("EnsureEntityWorkspace: %v", err)
	}
	joined = flattenDockerCalls(creates)
	for _, expected := range []string{
		"create --name swarm-bundle-aaaaaaaaaaaa-acme",
		"entities_swarm_bundle_aaaaaaaaaaaa_entity_acme:/workspace",
		"--label dev.swarm.bundle_hash=" + bundleHash,
		"--label dev.swarm.container.name=swarm-bundle-aaaaaaaaaaaa-acme",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("bundle-scoped entity workspace create args missing %q:\n%s", expected, joined)
		}
	}
}

func TestResolveWorkspace_PerFlowInstanceSharesByFlowPath(t *testing.T) {
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.SharedDataSource = dataDir
	cfg.ContractsSource = contractsDir
	cfg.WorkspaceNetwork = ""
	cfg.WorkspaceImage = "test-image"
	manager.SetConfig(cfg)
	manager.SetSemanticSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"workspace_classes": {
				Value: map[string]any{
					"shared_flow": map[string]any{"workspace_scope": "per-flow-instance"},
				},
			},
		}},
	}))

	var created []string
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			return "", fmt.Errorf("no such object")
		case "create":
			created = append([]string{}, args...)
			return "", nil
		case "start":
			return "", nil
		default:
			return "", nil
		}
	})
	ctx := runtimecorrelation.WithRunID(context.Background(), "22222222-2222-2222-2222-222222222222")
	target, err := manager.ResolveWorkspace(ctx, models.AgentConfig{
		ID:             "shared-work-lead",
		WorkspaceClass: "shared_flow",
		FlowPath:       "shared/work-001",
	})
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	if target == nil || target.Container != "swarm-flow-shared-work-001" {
		t.Fatalf("target = %#v, want swarm-flow-shared-work-001", target)
	}
	joined := strings.Join(created, " ")
	if !strings.Contains(joined, "workspaces_flow_shared-work-001:/workspace") {
		t.Fatalf("expected shared flow workspace volume, got %v", created)
	}
	for _, expected := range []string{
		"--label dev.swarm.container.kind=flow",
		"--label dev.swarm.reset.eligible=true",
		"--label dev.swarm.flow_instance=shared/work-001",
		"--label dev.swarm.run_id=22222222-2222-2222-2222-222222222222",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("create args missing %q: %v", expected, created)
		}
	}
}

func TestRuntimeWorkspaceContainersWithoutRunContextReturnsStaticContainers(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	manager := NewDockerManager(db)
	containers, err := manager.RuntimeWorkspaceContainers(context.Background())
	if err != nil {
		t.Fatalf("RuntimeWorkspaceContainers: %v", err)
	}
	got := strings.Join(containers, ",")
	for _, expected := range []string{"swarm-scaffold", "swarm-system"} {
		if !strings.Contains(got, expected) {
			t.Fatalf("containers = %v, want %s", containers, expected)
		}
	}
}

func TestResolveWorkspace_UsesInjectedSemanticSourceForRoleLookup(t *testing.T) {
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(contractsDir, "package.yaml"), []byte("name: test\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.SharedDataSource = dataDir
	cfg.ContractsSource = contractsDir
	cfg.WorkspaceNetwork = ""
	cfg.WorkspaceImage = "test-image"
	manager.SetConfig(cfg)

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"workspace_classes": {
				Value: map[string]any{
					"shared_flow": map[string]any{"workspace_scope": "per-flow-instance"},
				},
			},
		}},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"worker": {Role: "worker", WorkspaceClass: "shared_flow"},
		},
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{{
				Paths: runtimecontracts.FlowContractPaths{ID: "ops", Flow: "ops"},
				Path:  "ops",
				Agents: map[string]runtimecontracts.AgentRegistryEntry{
					"worker": {Role: "worker", WorkspaceClass: "shared_flow"},
				},
			}}},
			ByID: map[string]*runtimecontracts.FlowContractView{},
		},
	})
	if err := manager.ValidateSource(context.Background(), source); err != nil {
		t.Fatalf("ValidateSource: %v", err)
	}

	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			return "", fmt.Errorf("no such object")
		case "create", "start":
			return "", nil
		default:
			return "", nil
		}
	})

	target, err := manager.ResolveWorkspace(context.Background(), models.AgentConfig{
		ID:       "worker-1",
		Role:     "worker",
		FlowPath: "ops/instance-1",
	})
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	if target == nil || target.Container != "swarm-flow-ops-instance-1" {
		t.Fatalf("target = %#v, want swarm-flow-ops-instance-1", target)
	}
}

func TestResolveWorkspace_FailsClosedWithoutInjectedSourceForWorkspaceClassScope(t *testing.T) {
	manager := NewDockerManager(nil)
	_, err := manager.ResolveWorkspace(context.Background(), models.AgentConfig{
		ID:             "worker-1",
		WorkspaceClass: "dedicated",
	})
	if err == nil || !strings.Contains(err.Error(), `semantic source is required for workspace_class "dedicated"`) {
		t.Fatalf("expected missing semantic source error, got %v", err)
	}
}

func TestDefaultDockerConfigDoesNotDeriveSourceRootMounts(t *testing.T) {
	t.Setenv("SWARM_WORKSPACE_DATA_SOURCE", "")
	t.Setenv("SWARM_WORKSPACE_CONTRACTS_SOURCE", "")

	cfg := DefaultDockerConfig()
	if cfg.SharedDataSource != "" {
		t.Fatalf("SharedDataSource = %q, want no source-root default", cfg.SharedDataSource)
	}
	if cfg.ContractsSource != "" {
		t.Fatalf("ContractsSource = %q, want no source-root default", cfg.ContractsSource)
	}
}

func TestDefaultDockerConfigLeavesDataSourceToCommandResolver(t *testing.T) {
	dataDir := t.TempDir()
	contractsDir := t.TempDir()
	t.Setenv("SWARM_WORKSPACE_DATA_SOURCE", dataDir)
	t.Setenv("SWARM_WORKSPACE_CONTRACTS_SOURCE", contractsDir)

	cfg := DefaultDockerConfig()
	if cfg.SharedDataSource != "" {
		t.Fatalf("SharedDataSource = %q, want command-level resolver to own SWARM_WORKSPACE_DATA_SOURCE", cfg.SharedDataSource)
	}
	if cfg.ContractsSource != contractsDir {
		t.Fatalf("ContractsSource = %q, want %q", cfg.ContractsSource, contractsDir)
	}
}

func TestEnsurePrereqs_CreatesMissingNetworkAndFailsClosedForMissingImage(t *testing.T) {
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.WorkspaceNetwork = "test-network"
	cfg.WorkspaceImage = "test-image:latest"
	manager.SetConfig(cfg)

	var calls [][]string
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		calls = append(calls, append([]string{}, args...))
		switch {
		case len(args) >= 3 && args[0] == "version":
			return "26.1.0", nil
		case len(args) >= 3 && args[0] == "network" && args[1] == "inspect":
			return "", fmt.Errorf("no such network")
		case len(args) >= 3 && args[0] == "network" && args[1] == "create":
			return "created", nil
		case len(args) >= 3 && args[0] == "image" && args[1] == "inspect":
			return "", fmt.Errorf("no such image")
		case len(args) >= 6 && args[0] == "build":
			return "built", nil
		default:
			return "", nil
		}
	})

	err := manager.EnsurePrereqs(context.Background())
	if err == nil {
		t.Fatal("EnsurePrereqs unexpectedly succeeded with missing workspace image")
	}
	if !strings.Contains(err.Error(), "workspace image test-image:latest is not available") {
		t.Fatalf("EnsurePrereqs error = %v, want missing image diagnostic", err)
	}
	if !strings.Contains(err.Error(), "set SWARM_WORKSPACE_IMAGE") {
		t.Fatalf("EnsurePrereqs error = %v, want configured image remediation", err)
	}

	joined := flattenDockerCalls(calls)
	for _, expected := range []string{
		"version --format {{.Server.Version}}",
		"network inspect test-network",
		"network create test-network",
		"image inspect test-image:latest",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("EnsurePrereqs calls missing %q: %s", expected, joined)
		}
	}
	if strings.Contains(joined, "build ") || strings.Contains(joined, "Dockerfile.workspace") {
		t.Fatalf("EnsurePrereqs still attempted source-root image build:\n%s", joined)
	}
}

func TestEnsureSystemWorkspaces_CreatesScaffoldAndSystemContainers(t *testing.T) {
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.WorkspaceNetwork = ""
	cfg.WorkspaceImage = "test-image"
	manager.SetConfig(cfg)

	var calls [][]string
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		calls = append(calls, append([]string{}, args...))
		switch args[0] {
		case "inspect":
			return "", fmt.Errorf("no such object")
		case "create", "start":
			return "", nil
		default:
			return "", nil
		}
	})

	if err := manager.EnsureSystemWorkspaces(context.Background()); err != nil {
		t.Fatalf("EnsureSystemWorkspaces: %v", err)
	}

	joined := flattenDockerCalls(calls)
	for _, expected := range []string{
		"create --name swarm-scaffold",
		"create --name swarm-system",
		"--label dev.swarm.container.kind=scaffold",
		"--label dev.swarm.container.kind=system",
		"--label dev.swarm.reset.eligible=false",
		"test-image sleep infinity",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("EnsureSystemWorkspaces calls missing %q: %s", expected, joined)
		}
	}
}

func TestSystemWorkspaceContainersUsesConfiguredNames(t *testing.T) {
	manager := NewDockerManager(nil)
	manager.SetConfigForTest(DockerConfig{
		ScaffoldContainer: "custom-scaffold",
		SystemContainer:   "custom-system",
	})

	got := manager.SystemWorkspaceContainers()
	want := []string{"custom-scaffold", "custom-system"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SystemWorkspaceContainers = %#v, want %#v", got, want)
	}
}

func TestManagedResetContainerInventoryConsumesTypedLabels(t *testing.T) {
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)

	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		joined := strings.Join(args, " ")
		switch {
		case joined == "container ls --all --filter label=dev.swarm.owner=runtime --filter label=dev.swarm.reset.eligible=true --format {{.Names}}":
			return "swarm-agent-agent-a\nswarm-system\nswarm-malformed\nswarm-stale-name\n", nil
		case len(args) >= 4 && args[0] == "inspect" && args[len(args)-1] == "swarm-agent-agent-a":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":           "runtime",
				"dev.swarm.container.kind":  "agent",
				"dev.swarm.reset.eligible":  "true",
				"dev.swarm.creation_source": "workspace.ResolveWorkspace",
				"dev.swarm.container.name":  "swarm-agent-agent-a",
				"dev.swarm.workspace.scope": "per-agent",
				"dev.swarm.run_id":          "33333333-3333-3333-3333-333333333333",
				"dev.swarm.agent_id":        "agent-a",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[len(args)-1] == "swarm-system":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":           "runtime",
				"dev.swarm.container.kind":  "system",
				"dev.swarm.reset.eligible":  "false",
				"dev.swarm.creation_source": "workspace.EnsureSystemWorkspaces",
				"dev.swarm.container.name":  "swarm-system",
				"dev.swarm.workspace.scope": "system",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[len(args)-1] == "swarm-malformed":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":          "runtime",
				"dev.swarm.container.kind": "agent",
				"dev.swarm.reset.eligible": "true",
				"dev.swarm.container.name": "different-container-name",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[len(args)-1] == "swarm-stale-name":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":           "runtime",
				"dev.swarm.container.kind":  "agent",
				"dev.swarm.reset.eligible":  "true",
				"dev.swarm.creation_source": "workspace.ResolveWorkspace",
				"dev.swarm.container.name":  "old-valid-container-name",
				"dev.swarm.workspace.scope": "per-agent",
				"dev.swarm.run_id":          "44444444-4444-4444-4444-444444444444",
				"dev.swarm.agent_id":        "agent-stale",
			}, true), nil
		default:
			return "", nil
		}
	})

	refs, err := manager.ManagedResetContainerInventory(context.Background())
	if err != nil {
		t.Fatalf("ManagedResetContainerInventory: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("refs = %#v, want one reset-eligible managed container", refs)
	}
	ref := refs[0]
	if ref.Name != "swarm-agent-agent-a" || ref.Kind != "agent" || !ref.ResetEligible || ref.AgentID != "agent-a" || ref.RunID == "" {
		t.Fatalf("ref = %#v, want agent identity with run lineage", ref)
	}
}

func TestCleanupDevEntityContainersStopsOnlyIdentityProvenEntityContainers(t *testing.T) {
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)

	var calls [][]string
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		calls = append(calls, append([]string{}, args...))
		joined := strings.Join(args, " ")
		switch {
		case joined == "container ls --all --filter label=dev.swarm.owner=runtime --filter label=dev.swarm.reset.eligible=true --format {{.Names}}":
			return strings.Join([]string{
				"swarm-entity-acme",
				"swarm-agent-agent-a",
				"swarm-flow-flow-a",
				"swarm-system",
				"swarm-unlabeled",
				"swarm-operator",
				"swarm-stale-name",
			}, "\n"), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{json .}}" && args[len(args)-1] == "swarm-entity-acme":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":           "runtime",
				"dev.swarm.container.kind":  "entity",
				"dev.swarm.reset.eligible":  "true",
				"dev.swarm.creation_source": "workspace.EnsureEntityWorkspace",
				"dev.swarm.container.name":  "swarm-entity-acme",
				"dev.swarm.workspace.scope": "entity",
				"dev.swarm.entity_id":       "entity-1",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{json .}}" && args[len(args)-1] == "swarm-agent-agent-a":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":           "runtime",
				"dev.swarm.container.kind":  "agent",
				"dev.swarm.reset.eligible":  "true",
				"dev.swarm.creation_source": "workspace.ResolveWorkspace",
				"dev.swarm.container.name":  "swarm-agent-agent-a",
				"dev.swarm.workspace.scope": "per-agent",
				"dev.swarm.agent_id":        "agent-a",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{json .}}" && args[len(args)-1] == "swarm-flow-flow-a":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":           "runtime",
				"dev.swarm.container.kind":  "flow",
				"dev.swarm.reset.eligible":  "true",
				"dev.swarm.creation_source": "workspace.ResolveWorkspace",
				"dev.swarm.container.name":  "swarm-flow-flow-a",
				"dev.swarm.workspace.scope": "per-flow-instance",
				"dev.swarm.flow_instance":   "flow-a",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{json .}}" && args[len(args)-1] == "swarm-system":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":           "runtime",
				"dev.swarm.container.kind":  "system",
				"dev.swarm.reset.eligible":  "false",
				"dev.swarm.creation_source": "workspace.EnsureSystemWorkspaces",
				"dev.swarm.container.name":  "swarm-system",
				"dev.swarm.workspace.scope": "system",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{json .}}" && args[len(args)-1] == "swarm-unlabeled":
			return managedContainerInspectJSON(nil, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{json .}}" && args[len(args)-1] == "swarm-operator":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":           "operator",
				"dev.swarm.container.kind":  "entity",
				"dev.swarm.reset.eligible":  "true",
				"dev.swarm.container.name":  "swarm-operator",
				"dev.swarm.workspace.scope": "entity",
				"dev.swarm.entity_id":       "operator-entity",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{json .}}" && args[len(args)-1] == "swarm-stale-name":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":          "runtime",
				"dev.swarm.container.kind": "entity",
				"dev.swarm.reset.eligible": "true",
				"dev.swarm.container.name": "different-container-name",
				"dev.swarm.entity_id":      "stale-entity",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{.State.Running}}" && args[len(args)-1] == "swarm-entity-acme":
			return "true", nil
		case joined == "stop swarm-entity-acme":
			return "", nil
		default:
			return "", nil
		}
	})

	result, err := manager.CleanupDevEntityContainers(context.Background())
	if err != nil {
		t.Fatalf("CleanupDevEntityContainers: %v", err)
	}
	if result.OperationName != DevEntityCleanupOperationName {
		t.Fatalf("operation = %q, want %q", result.OperationName, DevEntityCleanupOperationName)
	}
	if len(result.Stopped) != 1 || result.Stopped[0].Name != "swarm-entity-acme" || result.Stopped[0].Kind != "entity" {
		t.Fatalf("stopped = %#v, want only entity container", result.Stopped)
	}
	if len(result.Preserved) != 2 {
		t.Fatalf("preserved = %#v, want agent and flow reset-eligible containers preserved", result.Preserved)
	}
	joined := flattenDockerCalls(calls)
	for _, forbidden := range []string{"stop swarm-agent-agent-a", "stop swarm-flow-flow-a", "stop swarm-system", "stop swarm-unlabeled", "stop swarm-operator", "stop swarm-stale-name"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("dev cleanup stopped forbidden container %q:\n%s", forbidden, joined)
		}
	}
}

func TestCleanupDevEntityContainersReportsStopFailures(t *testing.T) {
	manager := NewDockerManager(nil)
	cfg := DefaultDockerConfig()
	cfg.WorkspaceNetwork = ""
	manager.SetConfig(cfg)

	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		joined := strings.Join(args, " ")
		switch {
		case joined == "container ls --all --filter label=dev.swarm.owner=runtime --filter label=dev.swarm.reset.eligible=true --format {{.Names}}":
			return "swarm-entity-acme", nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{json .}}" && args[len(args)-1] == "swarm-entity-acme":
			return managedContainerInspectJSON(map[string]string{
				"dev.swarm.owner":           "runtime",
				"dev.swarm.container.kind":  "entity",
				"dev.swarm.reset.eligible":  "true",
				"dev.swarm.creation_source": "workspace.EnsureEntityWorkspace",
				"dev.swarm.container.name":  "swarm-entity-acme",
				"dev.swarm.workspace.scope": "entity",
				"dev.swarm.entity_id":       "entity-1",
			}, true), nil
		case len(args) >= 4 && args[0] == "inspect" && args[2] == "{{.State.Running}}" && args[len(args)-1] == "swarm-entity-acme":
			return "true", nil
		case joined == "stop swarm-entity-acme":
			return "", fmt.Errorf("docker stop failed")
		default:
			return "", nil
		}
	})

	result, err := manager.CleanupDevEntityContainers(context.Background())
	if err == nil || !strings.Contains(err.Error(), "dev entity container cleanup failed: 1 container(s)") {
		t.Fatalf("CleanupDevEntityContainers err = %v, want stop failure", err)
	}
	if len(result.Selected) != 1 || result.Selected[0].Name != "swarm-entity-acme" {
		t.Fatalf("selected = %#v, want failed entity selected", result.Selected)
	}
	if len(result.Stopped) != 0 {
		t.Fatalf("stopped = %#v, want none after failure", result.Stopped)
	}
	if len(result.Failed) != 1 || result.Failed[0].Container.Name != "swarm-entity-acme" || !strings.Contains(result.Failed[0].Error, "docker stop failed") {
		t.Fatalf("failed = %#v, want entity stop failure", result.Failed)
	}
}

func flattenDockerCalls(calls [][]string) string {
	lines := make([]string, 0, len(calls))
	for _, call := range calls {
		lines = append(lines, strings.Join(call, " "))
	}
	return strings.Join(lines, "\n")
}

func managedContainerInspectJSON(labels map[string]string, running bool) string {
	labelParts := make([]string, 0, len(labels))
	for key, value := range labels {
		labelParts = append(labelParts, fmt.Sprintf("%q:%q", key, value))
	}
	return fmt.Sprintf(`{"State":{"Running":%t},"Config":{"Labels":{%s}}}`, running, strings.Join(labelParts, ","))
}
