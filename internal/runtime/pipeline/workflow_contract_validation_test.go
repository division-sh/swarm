package pipeline

import (
	"os"
	"path/filepath"
	"testing"

	runtimecontracts "empireai/internal/runtime/contracts"
)

func TestValidateWorkflowContracts_CurrentBundle(t *testing.T) {
	pc := NewFactoryPipelineCoordinator(pipelineTestBus{}, nil)
	if err := pc.ValidateWorkflowContracts(); err != nil {
		t.Fatalf("expected current workflow contracts to validate: %v", err)
	}
}

func TestValidateWorkflowContracts_V220Bundle(t *testing.T) {
	repoRoot := workflowValidationProjectRoot(t)
	tmp := t.TempDir()
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "workflow-schema.yaml"), filepath.Join(tmp, "contracts", "workflow-schema.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "guard-action-registry.yaml"), filepath.Join(tmp, "contracts", "guard-action-registry.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "system-nodes.yaml"), filepath.Join(tmp, "contracts", "system-nodes.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "event-catalog.yaml"), filepath.Join(tmp, "contracts", "event-catalog.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "agent-tools.yaml"), filepath.Join(tmp, "contracts", "agent-tools.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "tool-schemas.yaml"), filepath.Join(tmp, "contracts", "tool-schemas.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "prompt-variables.yaml"), filepath.Join(tmp, "contracts", "prompt-variables.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "platform", "platform-spec.yaml"), filepath.Join(tmp, "contracts", "platform", "platform-spec.yaml"))

	bundle, err := runtimecontracts.LoadWorkflowContractBundle(tmp)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	if err := ValidateWorkflowContracts(bundle); err != nil {
		t.Fatalf("expected v2.2.0 workflow contracts to validate: %v", err)
	}
}

func TestValidateWorkflowContracts_FailsWhenOwningNodeHasNoRuntimeExecutor(t *testing.T) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(workflowValidationProjectRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	entry := bundle.Events["build_complete"]
	entry.OwningNode = "unsupported-orchestrator"
	bundle.Events["build_complete"] = entry
	bundle.Nodes["unsupported-orchestrator"] = runtimecontracts.SystemNodeContract{
		ID:             "unsupported-orchestrator",
		ExecutionType:  "system_node",
		Implementation: "internal/runtime/pipeline/unsupported_orchestrator.go",
		SubscribesTo:   []string{"build_complete"},
	}
	if err := ValidateWorkflowContracts(bundle); err == nil {
		t.Fatal("expected validation failure for unsupported runtime owning_node")
	}
}

func TestValidateWorkflowContracts_FailsWhenDataAccumulationWritesFieldOutsideEntitySchema(t *testing.T) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(workflowValidationProjectRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	transitions := bundle.Workflow.Workflow.Transitions
	if len(transitions) == 0 {
		t.Fatal("expected workflow transitions")
	}
	transitions[0].DataAccumulation.Writes = append(transitions[0].DataAccumulation.Writes, "not_in_entity_schema")
	bundle.Workflow.Workflow.Transitions = transitions
	if err := ValidateWorkflowContracts(bundle); err == nil {
		t.Fatal("expected validation failure for undeclared entity_schema write")
	}
}

func copyWorkflowValidationFixture(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(dst), err)
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", dst, err)
	}
}

func workflowValidationProjectRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}
