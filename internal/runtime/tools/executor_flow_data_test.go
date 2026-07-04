package tools

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/llm"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestExecutorReadFlowDataReadsDeclaredFlowFile(t *testing.T) {
	source, _ := loadFlowDataToolSource(t)
	actor := flowDataActor()
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source})

	defs := exec.ToolDefinitionsForActor(actor)
	if !containsToolName(toolDefinitionNames(defs), "read_flow_data") {
		t.Fatalf("read_flow_data missing from actor definitions: %v", toolDefinitionNames(defs))
	}
	if containsToolName(toolDefinitionNames(defs), "read_file") || containsToolName(toolDefinitionNames(defs), "write_file") {
		t.Fatalf("flow_data_access implied native file tools: %v", toolDefinitionNames(defs))
	}
	caps := exec.ToolCapabilitiesForActor(actor, []string{"read_flow_data"}, nil)
	cap, ok := caps.Capability("read_flow_data")
	if !ok || !cap.Visible || !cap.Callable || cap.AuthorizationClass != "flow_data_access" {
		t.Fatalf("read_flow_data capability = %#v, want visible/callable flow_data_access", cap)
	}

	out, err := exec.Execute(models.WithActor(context.Background(), actor), "read_flow_data", map[string]any{"filename": "exclusions.yaml"})
	if err != nil {
		t.Fatalf("Execute(read_flow_data): %v", err)
	}
	result, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", out)
	}
	if got := strings.TrimSpace(asString(result["content"])); got != "blocked: true" {
		t.Fatalf("content = %q, want blocked YAML", got)
	}
	if got := strings.TrimSpace(asString(result["content_type"])); got != "yaml" {
		t.Fatalf("content_type = %q, want yaml", got)
	}
	if got, ok := result["size_bytes"].(int); !ok || got == 0 {
		t.Fatalf("size_bytes = %#v, want non-zero int", result["size_bytes"])
	}
}

func TestExecutorReadFlowDataFailsClosedForUndeclaredAndEscapingFiles(t *testing.T) {
	source, root := loadFlowDataToolSourceWithAccess(t, []string{"exclusions.yaml", "escape.md"})
	actor := flowDataActor()
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source})

	if _, err := exec.Execute(models.WithActor(context.Background(), actor), "read_flow_data", map[string]any{"filename": "missing.yaml"}); err == nil {
		t.Fatal("Execute(read_flow_data missing.yaml) succeeded, want undeclared failure")
	}
	if _, err := exec.Execute(models.WithActor(context.Background(), actor), "read_flow_data", map[string]any{"filename": "../other/secret.yaml"}); err == nil {
		t.Fatal("Execute(read_flow_data traversal) succeeded, want traversal failure")
	}

	if err := os.MkdirAll(filepath.Join(root, "flows", "other", "data"), 0o755); err != nil {
		t.Fatalf("mkdir other data: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "flows", "other", "data", "secret.md"), []byte("secret\n"), 0o644); err != nil {
		t.Fatalf("write secret: %v", err)
	}
	linkPath := filepath.Join(root, "flows", "support", "data", "escape.md")
	if err := os.Symlink(filepath.Join(root, "flows", "other", "data", "secret.md"), linkPath); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if _, err := exec.Execute(models.WithActor(context.Background(), actor), "read_flow_data", map[string]any{"filename": "escape.md"}); err == nil {
		t.Fatal("Execute(read_flow_data symlink escape) succeeded, want failure")
	}
}

func TestExecutorReadFlowDataNotVisibleWithoutDeclaration(t *testing.T) {
	source, _ := loadFlowDataToolSource(t)
	actor := models.AgentConfig{
		ID:             "other-agent",
		Role:           "other",
		Mode:           "support",
		FlowPath:       "support",
		FlowDataAccess: []string{"exclusions.yaml"},
	}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source})

	if containsToolName(toolDefinitionNames(exec.ToolDefinitionsForActor(actor)), "read_flow_data") {
		t.Fatal("read_flow_data visible without flow_data_access declaration")
	}
	if _, err := exec.Execute(models.WithActor(context.Background(), actor), "read_flow_data", map[string]any{"filename": "exclusions.yaml"}); err == nil {
		t.Fatal("Execute(read_flow_data) succeeded without declaration")
	}
}

func TestExecutorReadFlowDataRejectsRoleModeImpersonation(t *testing.T) {
	source, _ := loadFlowDataToolSource(t)
	actor := models.AgentConfig{
		ID:       "impostor",
		Role:     "factory_cto",
		Mode:     "static",
		FlowPath: "support",
	}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source})

	if containsToolName(toolDefinitionNames(exec.ToolDefinitionsForActor(actor)), "read_flow_data") {
		t.Fatal("read_flow_data visible through role/mode fallback impersonation")
	}
	caps := exec.ToolCapabilitiesForActor(actor, []string{"read_flow_data"}, nil)
	cap, ok := caps.Capability("read_flow_data")
	if !ok || cap.Visible || cap.Callable || cap.AuthorizationClass != "flow_data_access" {
		t.Fatalf("capability = %#v, want denied flow_data_access for role/mode impersonation", cap)
	}
	if _, err := exec.Execute(models.WithActor(context.Background(), actor), "read_flow_data", map[string]any{"filename": "exclusions.yaml"}); err == nil {
		t.Fatal("Execute(read_flow_data) succeeded through role/mode fallback impersonation")
	}
}

func TestExecutorReadFlowDataIgnoresMutableActorFlowDataAccess(t *testing.T) {
	source, root := loadFlowDataToolSource(t)
	actor := flowDataActor()
	actor.FlowDataAccess = []string{"escape.md"}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source})

	if err := os.WriteFile(filepath.Join(root, "flows", "support", "data", "escape.md"), []byte("mutable grant\n"), 0o644); err != nil {
		t.Fatalf("write escape.md: %v", err)
	}
	if _, err := exec.Execute(models.WithActor(context.Background(), actor), "read_flow_data", map[string]any{"filename": "escape.md"}); err == nil || !strings.Contains(err.Error(), "invalid enum value") {
		t.Fatalf("Execute(read_flow_data escape.md) error = %v, want contract enum rejection", err)
	}
}

func TestExecutorReadFlowDataUsesContractOwnedFlowRoot(t *testing.T) {
	source, root := loadFlowDataToolSource(t)
	if err := os.MkdirAll(filepath.Join(root, "flows", "other", "data"), 0o755); err != nil {
		t.Fatalf("mkdir other data: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "flows", "other", "data", "exclusions.yaml"), []byte("blocked: other-flow\n"), 0o644); err != nil {
		t.Fatalf("write other exclusions: %v", err)
	}
	actor := flowDataActor()
	actor.Mode = "other"
	actor.FlowPath = "other"
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{WorkflowSource: source})

	out, err := exec.Execute(models.WithActor(context.Background(), actor), "read_flow_data", map[string]any{"filename": "exclusions.yaml"})
	if err != nil {
		t.Fatalf("Execute(read_flow_data): %v", err)
	}
	result, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", out)
	}
	if got := strings.TrimSpace(asString(result["content"])); got != "blocked: true" {
		t.Fatalf("content = %q, want support-flow contract-owned data root", got)
	}
}

func TestExecutorReadFlowDataDiagnosticsUseFlowDataAuthorization(t *testing.T) {
	source, _ := loadFlowDataToolSource(t)
	actor := flowDataActor()
	bus := &telemetryBusStub{}
	exec := NewExecutorWithOptions(bus, nil, ExecutorOptions{WorkflowSource: source})

	if _, err := exec.Execute(models.WithActor(context.Background(), actor), "read_flow_data", map[string]any{"filename": "exclusions.yaml"}); err != nil {
		t.Fatalf("Execute(read_flow_data): %v", err)
	}
	if len(bus.logs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(bus.logs))
	}
	detail, _ := bus.logs[0].Detail.(map[string]any)
	if got := strings.TrimSpace(asString(detail["authorization_class"])); got != "flow_data_access" {
		t.Fatalf("authorization_class = %q, want flow_data_access (detail=%#v)", got, detail)
	}
	if got := strings.TrimSpace(asString(detail["context_requirement"])); got != "actor_context" {
		t.Fatalf("context_requirement = %q, want actor_context", got)
	}
}

func TestExecutorReadFlowDataRequiresWorkflowSource(t *testing.T) {
	actor := flowDataActor()
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{})

	if containsToolName(toolDefinitionNames(exec.ToolDefinitionsForActor(actor)), "read_flow_data") {
		t.Fatal("read_flow_data visible without workflow source")
	}
	caps := exec.ToolCapabilitiesForActor(actor, []string{"read_flow_data"}, nil)
	cap, ok := caps.Capability("read_flow_data")
	if !ok || cap.Visible || cap.Callable || cap.AuthorizationClass != "flow_data_access" {
		t.Fatalf("capability = %#v, want denied flow_data_access without source", cap)
	}
	if _, err := exec.Execute(models.WithActor(context.Background(), actor), "read_flow_data", map[string]any{"filename": "exclusions.yaml"}); err == nil {
		t.Fatal("Execute(read_flow_data) succeeded without workflow source")
	}
}

func flowDataActor() models.AgentConfig {
	return models.AgentConfig{
		ID:             "factory-cto",
		Role:           "factory_cto",
		Mode:           "support",
		FlowPath:       "support",
		FlowDataAccess: []string{"exclusions.yaml"},
	}
}

func loadFlowDataToolSource(t *testing.T) (semanticview.Source, string) {
	t.Helper()
	return loadFlowDataToolSourceWithAccess(t, []string{"exclusions.yaml"})
}

func loadFlowDataToolSourceWithAccess(t *testing.T, access []string) (semanticview.Source, string) {
	t.Helper()
	root := t.TempDir()
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: flow-data-test
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: support
    flow: support
    mode: static
  - id: other
    flow: other
    mode: static
`)
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: flow-data-test\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), "name: support\nmode: static\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), `
factory-cto:
  id: factory-cto
  role: factory_cto
  mode: task
`+toolFlowDataAccessYAML(access))
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), "{}\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "flows", "support", "data", "exclusions.yaml"), "blocked: true\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "flows", "other", "schema.yaml"), "name: other\nmode: static\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "flows", "other", "agents.yaml"), "{}\n")
	writeToolFlowDataFixtureFile(t, filepath.Join(root, "flows", "other", "events.yaml"), "{}\n")

	repoRoot := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", root, err)
	}
	return semanticview.Wrap(bundle), root
}

func toolFlowDataAccessYAML(access []string) string {
	if len(access) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("  flow_data_access:\n")
	for _, item := range access {
		b.WriteString("    - ")
		b.WriteString(item)
		b.WriteString("\n")
	}
	return b.String()
}

func writeToolFlowDataFixtureFile(t *testing.T, path string, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func toolDefinitionNames(defs []llm.ToolDefinition) []string {
	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, def.Name)
	}
	return names
}
