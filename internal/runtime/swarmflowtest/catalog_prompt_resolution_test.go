package swarmflowtest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCatalogIntentIssues_UsesSemanticScopedArtifact(t *testing.T) {
	dir := catalogPromptResolutionFixture(t)
	bundle := catalogLoadBootBundle(t, dir)
	scope := catalogBootScope{Name: "support"}
	agent := map[string]any{
		"id":     "support-agent",
		"intent": "intent/shared.md",
		"model":  "regular",
	}

	if issues := catalogPromptIssues(bundle, scope, "support-agent", agent); len(issues) != 0 {
		t.Fatalf("catalogPromptIssues returned %#v, want no issues", issues)
	}
}

func TestCatalogIntentIssues_ReportsTODOFromResolvedArtifact(t *testing.T) {
	dir := catalogPromptResolutionFixture(t)
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "flows", "support", "intent", "shared.md"), "<!-- TODO: finish scoped business intent -->\n")

	bundle := catalogLoadBootBundle(t, dir)
	agent := map[string]any{
		"id":     "support-agent",
		"intent": "intent/shared.md",
		"model":  "regular",
	}
	issues := catalogPromptIssues(bundle, catalogBootScope{Name: "support"}, "support-agent", agent)
	if len(issues) != 1 || issues[0].Category != "INTENT-TODO" {
		t.Fatalf("catalogPromptIssues returned %#v, want INTENT-TODO", issues)
	}
}

func catalogPromptResolutionFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "package.yaml"), `
name: catalog-prompt-resolution
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
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
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "intent", "shared.md"), "<!-- TODO: unreferenced root intent -->\n")

	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "package.yaml"), `
name: extras
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
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

	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
states: [waiting, done]
terminal_states: [done]
`)
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "flows", "support", "agents.yaml"), `
support-agent:
  id: support-agent
  intent: intent/shared.md
  model: regular
  memory: true
`)
	writeCatalogPromptResolutionFile(t, filepath.Join(dir, "extras", "flows", "support", "intent", "shared.md"), "You are the scoped support agent.\n")
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
