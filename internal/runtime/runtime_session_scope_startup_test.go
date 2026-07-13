package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRuntimeStart_PackageBackedFlowOwnedStaticAgentsCarryCanonicalFlowPath(t *testing.T) {
	source := loadPackageBackedRuntimeSessionScopeSource(t)
	assertRuntimeStartCarriesFlowPath(t, source)
}

func TestRuntimeStart_SoleParentFlowPackageAgentsCarryCanonicalFlowPath(t *testing.T) {
	source := loadSoleParentFlowRuntimeSessionScopeSource(t)
	assertRuntimeStartCarriesFlowPath(t, source)
}

func assertRuntimeStartCarriesFlowPath(t *testing.T, source semanticview.Source) {
	t.Helper()
	rt, err := NewRuntime(context.Background(), RuntimeDeps{Config: &config.Config{}, Stores: Stores{}, Options: RuntimeOptions{
		SelfCheck:      false,
		LLMRuntime:     noopLLMRuntime{},
		WorkflowModule: semanticOnlyWorkflowRuntime{source: source},
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Shutdown()
	})

	if err := rt.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cfg, ok := rt.Manager.GetAgentConfig("backend-{vertical_id}")
	if !ok {
		t.Fatal("expected package-backed static flow agent config")
	}
	if cfg.FlowPath != "support" {
		t.Fatalf("FlowPath = %q, want support", cfg.FlowPath)
	}
	if cfg.Mode != "support" {
		t.Fatalf("Mode = %q, want support", cfg.Mode)
	}
}

func loadPackageBackedRuntimeSessionScopeSource(t *testing.T) semanticview.Source {
	// routing-example-census: different-concept issue=none owner=runtime.session_scope_startup proof=internal/runtime/runtime_session_scope_startup_test.go:TestRuntimeStart_PackageBackedFlowOwnedStaticAgentsCarryCanonicalFlowPath
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()

	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: session-scope-validation
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: support
    flow: support
    mode: static
`)
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id:
    type: string
    _unused_reason: startup scope fixture field
`)
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  swarm:
    source: external (bootstrap fixture)
  entity_id: string
`)
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "flows", "support", "package.yaml"), `
name: support
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
states:
  - waiting
`)
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "flows", "support", "prompts", "backend.md"), "Handle support events.\n")
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
support/item.created:
  entity_id: string
`)
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), `
backend:
  id: backend-{vertical_id}
  type: generic
  role: backend
  model: regular
  mode: session
  subscriptions:
    - support/item.created
  emit_events:
    - support/item.created
`)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func loadSoleParentFlowRuntimeSessionScopeSource(t *testing.T) semanticview.Source {
	// routing-example-census: different-concept issue=none owner=runtime.session_scope_startup proof=internal/runtime/runtime_session_scope_startup_test.go:TestRuntimeStart_SoleParentFlowPackageAgentsCarryCanonicalFlowPath
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()

	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: session-scope-validation
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - path: extras
flows:
  - id: support
    flow: support
    mode: static
`)
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id:
    type: string
    _unused_reason: startup scope fixture field
`)
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  swarm:
    source: external (bootstrap fixture)
  entity_id: string
`)
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
states:
  - waiting
`)
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
support/item.created:
  entity_id: string
`)
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "extras", "prompts", "backend.md"), "Handle support events.\n")
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "extras", "package.yaml"), `
name: extras
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeRuntimeSessionScopeFixtureFile(t, filepath.Join(root, "extras", "agents.yaml"), `
backend:
  id: backend-{vertical_id}
  type: generic
  role: backend
  model: regular
  mode: session
  subscriptions:
    - support/item.created
  emit_events:
    - support/item.created
`)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func writeRuntimeSessionScopeFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
