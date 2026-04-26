package workspace

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/testutil"
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
	target, err := manager.ResolveWorkspace(context.Background(), models.AgentConfig{
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
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("create args missing %q: %v", expected, created)
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
	target, err := manager.ResolveWorkspace(context.Background(), models.AgentConfig{
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

func TestEnsurePrereqs_CreatesMissingNetworkAndBuildsMissingImage(t *testing.T) {
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

	if err := manager.EnsurePrereqs(context.Background()); err != nil {
		t.Fatalf("EnsurePrereqs: %v", err)
	}

	joined := flattenDockerCalls(calls)
	for _, expected := range []string{
		"version --format {{.Server.Version}}",
		"network inspect test-network",
		"network create test-network",
		"image inspect test-image:latest",
		"build -t test-image:latest",
		"Dockerfile.workspace",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("EnsurePrereqs calls missing %q: %s", expected, joined)
		}
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
		"test-image sleep infinity",
	} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("EnsureSystemWorkspaces calls missing %q: %s", expected, joined)
		}
	}
}

func flattenDockerCalls(calls [][]string) string {
	lines := make([]string, 0, len(calls))
	for _, call := range calls {
		lines = append(lines, strings.Join(call, " "))
	}
	return strings.Join(lines, "\n")
}
