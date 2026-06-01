package swarmflowtest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestCatalogPromptSemanticSourceAndMode_UsesFlowRefModeAndPackageKey(t *testing.T) {
	dir := catalogPromptResolutionFixture(t)
	bundle := catalogLoadBootBundle(t, dir)
	semanticBundle, ok := semanticview.Bundle(bundle.Source)
	if !ok {
		t.Fatal("expected semantic bundle")
	}

	source, mode := catalogPromptSemanticSourceAndMode(semanticBundle, catalogBootScope{Name: "support"}, "support-agent")
	if source.PackageKey != "extras" {
		t.Fatalf("PackageKey = %q, want extras", source.PackageKey)
	}
	if source.FlowID != "support" {
		t.Fatalf("FlowID = %q, want support", source.FlowID)
	}
	if mode != "review" {
		t.Fatalf("mode = %q, want review", mode)
	}
}

func TestCatalogPromptIssues_UsesSemanticPromptScope(t *testing.T) {
	dir := catalogPromptResolutionFixture(t)
	bundle := catalogLoadBootBundle(t, dir)
	scope := catalogBootScope{Name: "support"}
	agent := map[string]any{
		"id":                "support-agent",
		"prompt_ref":        "shared",
		"model":             "regular",
		"conversation_mode": "session",
	}

	if issues := catalogPromptIssues(bundle, scope, "support-agent", agent); len(issues) != 0 {
		t.Fatalf("catalogPromptIssues returned %#v, want no issues", issues)
	}
}

func TestCatalogPromptIssues_PrefersSchemaModeOverFlowRefMode(t *testing.T) {
	dir := catalogPromptResolutionFixture(t)
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "flows", "support", "schema.yaml"), `
name: support
mode: schema-review
initial_state: waiting
states: [waiting, done]
terminal_states: [done]
`)
	if err := os.Remove(filepath.Join(dir, "extras", "prompts", "shared.review.md")); err != nil {
		t.Fatalf("remove flow-ref prompt: %v", err)
	}
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "prompts", "shared.schema-review.md"), "You are the schema-mode support agent.\n")

	bundle := catalogLoadBootBundle(t, dir)
	semanticBundle, ok := semanticview.Bundle(bundle.Source)
	if !ok {
		t.Fatal("expected semantic bundle")
	}
	_, mode := catalogPromptSemanticSourceAndMode(semanticBundle, catalogBootScope{Name: "support"}, "support-agent")
	if mode != "schema-review" {
		t.Fatalf("mode = %q, want schema-review", mode)
	}

	agent := map[string]any{
		"id":                "support-agent",
		"prompt_ref":        "shared",
		"model":             "regular",
		"conversation_mode": "session",
	}
	if issues := catalogPromptIssues(bundle, catalogBootScope{Name: "support"}, "support-agent", agent); len(issues) != 0 {
		t.Fatalf("catalogPromptIssues returned %#v, want no issues", issues)
	}
}

func catalogPromptResolutionFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "package.yaml"), `
name: catalog-prompt-resolution
version: "1.0.0"
platform_version: ">=1.0.0"
packages:
  - path: extras
flows: []
`)
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "schema.yaml"), "name: catalog-prompt-resolution\n")
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "agents.yaml"), "{}\n")
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "events.yaml"), "{}\n")
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "nodes.yaml"), "{}\n")
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "policy.yaml"), "{}\n")
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "tools.yaml"), "{}\n")
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "prompts", "shared.md"), "<!-- TODO: wrong root fallback -->\n")

	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "package.yaml"), `
name: extras
version: "1.0.0"
platform_version: ">=1.0.0"
flows:
  - id: support
    flow: support
    mode: review
`)
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "agents.yaml"), "{}\n")
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "events.yaml"), "{}\n")
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "nodes.yaml"), "{}\n")
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "policy.yaml"), "{}\n")
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "tools.yaml"), "{}\n")
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "prompts", "shared.review.md"), "You are the scoped support agent.\n")

	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
states: [waiting, done]
terminal_states: [done]
`)
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "flows", "support", "agents.yaml"), `
support-agent:
  id: support-agent
  prompt_ref: shared
  model: regular
  conversation_mode: session
`)
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "flows", "support", "events.yaml"), "{}\n")
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "flows", "support", "nodes.yaml"), "{}\n")
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "flows", "support", "policy.yaml"), "{}\n")
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "flows", "support", "tools.yaml"), "{}\n")
	return dir
}

func writeCatalogPromptResolutionFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
