package flowownedprojectagent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func LoadSource(t testing.TB, mode string, duplicateRole bool) semanticview.Source {
	return loadSource(t, mode, duplicateRole, false)
}

func LoadExplicitRequiredSource(t testing.TB, mode string, duplicateRole bool) semanticview.Source {
	return loadSource(t, mode, duplicateRole, true)
}

func loadSource(t testing.TB, mode string, duplicateRole, explicitRequired bool) semanticview.Source {
	t.Helper()
	repoRoot := repoRoot(t)
	root := t.TempDir()
	packages := "  - {path: flows/support/left}\n"
	if duplicateRole {
		packages += "  - {path: flows/support/right}\n"
	}
	write(t, filepath.Join(root, "package.yaml"), `
name: flow-owned-project-agent
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
`+packages+`flows:
  - {id: support, flow: support, mode: `+mode+`}
`)
	write(t, filepath.Join(root, "schema.yaml"), "name: flow-owned-project-agent\n")
	for _, name := range []string{"agents.yaml", "entities.yaml", "events.yaml", "nodes.yaml", "policy.yaml", "tools.yaml"} {
		write(t, filepath.Join(root, name), "{}\n")
	}

	flowRoot := filepath.Join(root, "flows", "support")
	write(t, filepath.Join(flowRoot, "package.yaml"), "name: support\nversion: \"1.0.0\"\nflows: []\n")
	requiredAgents := ""
	if explicitRequired {
		requiredAgents = "required_agents:\n  - role: worker\n"
	}
	write(t, filepath.Join(flowRoot, "schema.yaml"), `
name: support
mode: `+mode+`
initial_state: active
states: [active]
`+requiredAgents+`
pins:
  inputs:
    events: [work.requested]
`)
	write(t, filepath.Join(flowRoot, "events.yaml"), "work.requested: {}\nwork.completed: {}\n")
	for _, name := range []string{"agents.yaml", "nodes.yaml", "policy.yaml", "tools.yaml"} {
		write(t, filepath.Join(flowRoot, name), "{}\n")
	}
	writeAgentPackage(t, filepath.Join(flowRoot, "left"), "public-worker-left")
	if duplicateRole {
		writeAgentPackage(t, filepath.Join(flowRoot, "right"), "public-worker-right")
	}

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func writeAgentPackage(t testing.TB, root, publicID string) {
	t.Helper()
	write(t, filepath.Join(root, "package.yaml"), "name: "+filepath.Base(root)+"\nversion: \"1.0.0\"\nflows: []\n")
	write(t, filepath.Join(root, "agents.yaml"), `
worker:
  id: `+publicID+`
  role: worker
  intent: {inline: Handle flow-owned project work.}
  model: regular
  memory: false
  subscriptions: [work.requested]
  emit_events: [work.completed]
`)
}

func write(t testing.TB, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", path, err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(body, "\n")), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func repoRoot(t testing.TB) string {
	t.Helper()
	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			return root
		}
		parent := filepath.Dir(root)
		if parent == root {
			t.Fatal("repository root not found")
		}
		root = parent
	}
}
