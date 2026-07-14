package runtime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type wrappedSemanticSource struct {
	semanticview.Source
}

func TestNewRuntimePromptResolver_RejectsNonBundleSemanticSource(t *testing.T) {
	source := wrappedSemanticSource{
		Source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
	}

	_, err := newRuntimePromptResolver(source)
	if err == nil || !strings.Contains(err.Error(), "bundle-backed semantic source is required") {
		t.Fatalf("newRuntimePromptResolver err = %v, want bundle-backed source error", err)
	}
}

func TestNewRuntimePromptResolver_UsesImportBoundaryPolicyResolution(t *testing.T) {
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()

	writeRuntimePromptResolverFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: prompt-import-policy
version: "1.0.0"
flows:
  - id: worker
    flow: worker
    mode: static
    bind:
      policy:
        threshold: parent.policy.tenant_threshold
`)
	writeRuntimePromptResolverFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: prompt-import-policy\n")
	writeRuntimePromptResolverFixtureFile(t, filepath.Join(root, "policy.yaml"), `
threshold: 999
tenant_threshold: 42
`)
	writeRuntimePromptResolverFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeRuntimePromptResolverFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeRuntimePromptResolverFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeRuntimePromptResolverFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeRuntimePromptResolverFixtureFile(t, filepath.Join(root, "flows", "worker", "package.yaml"), `
name: worker-package
version: "1.0.0"
requires:
  policy: [threshold]
`)
	writeRuntimePromptResolverFixtureFile(t, filepath.Join(root, "flows", "worker", "schema.yaml"), "name: worker\nmode: static\n")
	writeRuntimePromptResolverFixtureFile(t, filepath.Join(root, "flows", "worker", "policy.yaml"), "threshold: 10\n")
	writeRuntimePromptResolverFixtureFile(t, filepath.Join(root, "flows", "worker", "tools.yaml"), "{}\n")
	writeRuntimePromptResolverFixtureFile(t, filepath.Join(root, "flows", "worker", "agents.yaml"), `
worker-agent:
  id: worker-agent
  role: worker
  model: regular
`)
	writeRuntimePromptResolverFixtureFile(t, filepath.Join(root, "flows", "worker", "events.yaml"), "{}\n")
	writeRuntimePromptResolverFixtureFile(t, filepath.Join(root, "flows", "worker", "prompts", "worker-agent.md"), "threshold={{threshold}}\n")

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	resolver, err := newRuntimePromptResolver(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("newRuntimePromptResolver: %v", err)
	}
	prompt, found, err := resolver.LoadPromptForAgent(models.AgentConfig{ID: "worker-agent"}, "")
	if err != nil {
		t.Fatalf("LoadPromptForAgent: %v", err)
	}
	if !found {
		t.Fatal("prompt not found")
	}
	if got, want := strings.TrimSpace(prompt), "threshold=42"; got != want {
		t.Fatalf("prompt = %q, want %q", got, want)
	}
}

func writeRuntimePromptResolverFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}
