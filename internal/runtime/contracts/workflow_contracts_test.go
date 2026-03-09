package contracts

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveWorkflowContractPaths_PrefersWorkflowScopedLayout(t *testing.T) {
	repoRoot := t.TempDir()
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "system-nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "event-catalog.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "agent-tools.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "workflow-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "hooks-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "nodes-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "events-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "agents-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "tools-empire.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "empire", "policy-empire.yaml"))
	if err := os.MkdirAll(filepath.Join(repoRoot, "contracts", "empire", "prompts"), 0o755); err != nil {
		t.Fatalf("mkdir prompts dir: %v", err)
	}

	paths := ResolveWorkflowContractPaths(repoRoot)
	if want := filepath.Join(repoRoot, "contracts", "empire", "workflow-empire.yaml"); paths.WorkflowSchemaFile != want {
		t.Fatalf("workflow schema path = %s, want %s", paths.WorkflowSchemaFile, want)
	}
	if want := filepath.Join(repoRoot, "contracts", "empire", "hooks-empire.yaml"); paths.GuardRegistryFile != want {
		t.Fatalf("guard registry path = %s, want %s", paths.GuardRegistryFile, want)
	}
	if want := filepath.Join(repoRoot, "contracts", "empire", "prompts"); paths.PromptsDir != want {
		t.Fatalf("prompts dir = %s, want %s", paths.PromptsDir, want)
	}
}

func TestResolveWorkflowContractPaths_FallsBackToLegacyLayout(t *testing.T) {
	repoRoot := t.TempDir()
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "system-nodes.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "event-catalog.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "agent-tools.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"))
	mustWriteTestFile(t, filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"))
	if err := os.MkdirAll(filepath.Join(repoRoot, "contracts", "prompts"), 0o755); err != nil {
		t.Fatalf("mkdir legacy prompts dir: %v", err)
	}

	paths := ResolveWorkflowContractPaths(repoRoot)
	if want := filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"); paths.WorkflowSchemaFile != want {
		t.Fatalf("workflow schema path = %s, want %s", paths.WorkflowSchemaFile, want)
	}
	if want := filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"); paths.GuardRegistryFile != want {
		t.Fatalf("guard registry path = %s, want %s", paths.GuardRegistryFile, want)
	}
	if want := filepath.Join(repoRoot, "contracts", "prompts"); paths.PromptsDir != want {
		t.Fatalf("prompts dir = %s, want %s", paths.PromptsDir, want)
	}
}

func TestLoadWorkflowContractBundle_LoadsV220Fields(t *testing.T) {
	repoRoot := projectRootFromContractsTest(t)
	tmp := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmp, "contracts"), 0o755); err != nil {
		t.Fatalf("mkdir contracts dir: %v", err)
	}
	copyContractFileForTest(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "workflow-schema.yaml"), filepath.Join(tmp, "contracts", "workflow-schema.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "guard-action-registry.yaml"), filepath.Join(tmp, "contracts", "guard-action-registry.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "system-nodes.yaml"), filepath.Join(tmp, "contracts", "system-nodes.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "event-catalog.yaml"), filepath.Join(tmp, "contracts", "event-catalog.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "agent-tools.yaml"), filepath.Join(tmp, "contracts", "agent-tools.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "tool-schemas.yaml"), filepath.Join(tmp, "contracts", "tool-schemas.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "prompt-variables.yaml"), filepath.Join(tmp, "contracts", "prompt-variables.yaml"))
	copyContractFileForTest(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "platform", "platform-spec.yaml"), filepath.Join(tmp, "contracts", "platform", "platform-spec.yaml"))

	bundle, err := LoadWorkflowContractBundle(tmp)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	if got := bundle.Workflow.Workflow.Version; got != "2.2.0" {
		t.Fatalf("workflow version = %q, want 2.2.0", got)
	}
	if bundle.Workflow.Workflow.EntitySchema == nil {
		t.Fatal("expected entity_schema to load")
	}
	foundAccumulation := false
	for _, tr := range bundle.Workflow.Workflow.Transitions {
		if tr.ID != "discovered_to_scoring" {
			continue
		}
		foundAccumulation = true
		if len(tr.DataAccumulation.Writes) == 0 || tr.DataAccumulation.SourceEvent != "vertical.discovered" {
			t.Fatalf("expected data_accumulation on discovered_to_scoring, got %+v", tr.DataAccumulation)
		}
	}
	if !foundAccumulation {
		t.Fatal("expected discovered_to_scoring transition in v2.2.0 workflow")
	}
	node, ok := bundle.Nodes["scan-orchestrator"]
	if !ok {
		t.Fatal("expected scan-orchestrator node")
	}
	if len(node.EventHandlers) == 0 {
		t.Fatal("expected event_handlers on scan-orchestrator")
	}
	if node.StateSchema == nil {
		t.Fatal("expected state_schema on scan-orchestrator")
	}
	event, ok := bundle.Events["scan.requested"]
	if !ok {
		t.Fatal("expected scan.requested event")
	}
	if event.RuntimeHandling != "consuming" {
		t.Fatalf("scan.requested runtime_handling = %q, want consuming", event.RuntimeHandling)
	}
	if event.OwningNode != "scan-orchestrator" {
		t.Fatalf("scan.requested owning_node = %q, want scan-orchestrator", event.OwningNode)
	}
}

func mustWriteTestFile(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte("test: true\n"), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func copyContractFileForTest(t *testing.T, src, dst string) {
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

func projectRootFromContractsTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}
