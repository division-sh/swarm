package workspace

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/semanticview"
)

func TestWorkspaceClassesForSource(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"workspace_classes": {
				Value: map[string]any{
					"factory": map[string]any{"workspace_scope": "per-agent"},
					"opco":    map[string]any{"workspace_scope": "per-flow-instance"},
				},
			},
		}},
	})
	classes, err := workspaceClassesForSource(source)
	if err != nil {
		t.Fatalf("workspaceClassesForSource: %v", err)
	}
	if got := classes["factory"]; got != "per-agent" {
		t.Fatalf("factory scope = %q, want per-agent", got)
	}
	if got := classes["opco"]; got != "per-flow-instance" {
		t.Fatalf("opco scope = %q, want per-flow-instance", got)
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
					"factory": map[string]any{"workspace_scope": "per-agent"},
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
					"factory": map[string]any{"workspace_scope": "per-agent"},
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
	raw, _ := json.Marshal(map[string]any{"workspace_class": "factory"})
	target, err := manager.ResolveWorkspace(context.Background(), models.AgentConfig{
		ID:     "factory-agent",
		Config: raw,
	})
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	if target == nil || target.Container != "swarm-agent-factory-agent" {
		t.Fatalf("target = %#v, want swarm-agent-factory-agent", target)
	}
	joined := strings.Join(created, " ")
	for _, expected := range []string{
		dataDir + ":/data:ro",
		contractsDir + ":/opt/swarm/contracts:ro",
		"workspaces_agent_factory-agent:/workspace",
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
					"opco": map[string]any{"workspace_scope": "per-flow-instance"},
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
	raw, _ := json.Marshal(map[string]any{
		"workspace_class": "opco",
		"flow_path":       "operating/opco-001",
	})
	target, err := manager.ResolveWorkspace(context.Background(), models.AgentConfig{
		ID:     "opco-ceo",
		Config: raw,
	})
	if err != nil {
		t.Fatalf("ResolveWorkspace: %v", err)
	}
	if target == nil || target.Container != "swarm-flow-operating-opco-001" {
		t.Fatalf("target = %#v, want swarm-flow-operating-opco-001", target)
	}
	joined := strings.Join(created, " ")
	if !strings.Contains(joined, "workspaces_flow_operating-opco-001:/workspace") {
		t.Fatalf("expected shared flow workspace volume, got %v", created)
	}
}
