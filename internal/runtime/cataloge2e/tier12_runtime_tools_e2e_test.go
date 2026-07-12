package cataloge2e

import (
	"context"
	"github.com/division-sh/swarm/internal/testutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
)

var tier12RuntimeToolsFixtures = []string{
	"test-flow-data-access",
}

func TestTier12RuntimeTools_FlowDataAccessFixture(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier12-runtime-tools", "test-flow-data-access")

	h := newRuntimeHarness(t, fixtureRoot, true, testutil.PostgresRowState())
	cfg, ok := h.rt.Manager.GetAgentConfig("reference-agent")
	if !ok {
		t.Fatal("reference-agent config was not registered")
	}
	if cfg.FlowPath != "support" {
		t.Fatalf("flow path = %q, want support", cfg.FlowPath)
	}
	if len(cfg.FlowDataAccess) != 1 || cfg.FlowDataAccess[0] != "exclusions.yaml" {
		t.Fatalf("flow data access = %#v, want [exclusions.yaml]", cfg.FlowDataAccess)
	}

	defs := h.rt.ToolExecutor.ToolDefinitionsForActor(cfg)
	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, strings.TrimSpace(def.Name))
	}
	if !containsTier12String(names, "read_flow_data") {
		t.Fatalf("runtime tool definitions = %#v, want read_flow_data", names)
	}
	if containsTier12String(names, "read_file") || containsTier12String(names, "write_file") {
		t.Fatalf("flow_data_access exposed native file tools: %#v", names)
	}

	out, err := h.rt.ToolExecutor.Execute(models.WithActor(context.Background(), cfg), "read_flow_data", map[string]any{
		"filename": "exclusions.yaml",
	})
	if err != nil {
		t.Fatalf("read_flow_data(exclusions.yaml): %v", err)
	}
	result, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("read_flow_data result = %T, want map[string]any", out)
	}
	content := strings.TrimSpace(asString(result["content"]))
	if !strings.Contains(content, "unmanaged-host-file-reads") || !strings.Contains(content, "cross-flow-data-access") {
		t.Fatalf("content = %q, want declared fixture data", content)
	}
	if got := strings.TrimSpace(asString(result["content_type"])); got != "yaml" {
		t.Fatalf("content_type = %q, want yaml", got)
	}

	if _, err := h.rt.ToolExecutor.Execute(models.WithActor(context.Background(), cfg), "read_flow_data", map[string]any{
		"filename": "undeclared.yaml",
	}); err == nil {
		t.Fatal("undeclared read unexpectedly succeeded")
	} else if failure, ok := runtimefailures.As(err); !ok || failure.Failure.Class != runtimefailures.ClassSchemaInvalid || failure.Failure.Detail.Code != "invalid_tool_input" {
		t.Fatalf("undeclared read failure = %#v, want fail-closed schema rejection", failure)
	}
	if _, err := h.rt.ToolExecutor.Execute(models.WithActor(context.Background(), cfg), "read_flow_data", map[string]any{
		"filename": "../support/data/exclusions.yaml",
	}); err == nil {
		t.Fatal("traversal read succeeded, want fail-closed error")
	}

	other := cfg
	other.ID = "other-agent"
	other.Role = "other"
	other.FlowDataAccess = nil
	if defs := h.rt.ToolExecutor.ToolDefinitionsForActor(other); containsTier12String(toolNamesForTier12RuntimeTools(defs), "read_flow_data") {
		t.Fatalf("undeclared actor saw read_flow_data definitions: %#v", toolNamesForTier12RuntimeTools(defs))
	}
	if _, err := h.rt.ToolExecutor.Execute(models.WithActor(context.Background(), other), "read_flow_data", map[string]any{
		"filename": "exclusions.yaml",
	}); err == nil {
		t.Fatal("undeclared actor read flow data, want fail-closed error")
	}

}

func TestTier12RuntimeToolsFixtures_AreExplicitlyClassified(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	entries, err := os.ReadDir(filepath.Join(repoRoot, "tests", "tier12-runtime-tools"))
	if err != nil {
		t.Fatalf("read tier12 runtime-tools fixture dir: %v", err)
	}
	supported := map[string]struct{}{}
	for _, name := range tier12RuntimeToolsFixtures {
		supported[name] = struct{}{}
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := strings.TrimSpace(entry.Name())
		if _, ok := supported[name]; !ok {
			t.Fatalf("tier12 runtime-tools fixture %q is not explicitly classified", name)
		}
	}
}

func toolNamesForTier12RuntimeTools(defs []llm.ToolDefinition) []string {
	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, strings.TrimSpace(def.Name))
	}
	return names
}
