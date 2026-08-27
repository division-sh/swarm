package bootverify

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRun_ValidatesFlowDataAccessDeclarations(t *testing.T) {
	t.Run("valid declaration", func(t *testing.T) {
		root := writeFlowDataAccessFixture(t, []string{"exclusions.yaml"}, map[string]string{"exclusions.yaml": "blocked: true\n"}, false)
		report := Run(context.Background(), semanticview.Wrap(loadFlowDataAccessBundle(t, root)), Options{})
		if reportContains(report.Errors(), "flow_data_access_validation", "") {
			t.Fatalf("unexpected flow_data_access_validation error: %#v", report.Errors())
		}
	})

	tests := []struct {
		name       string
		access     []string
		files      map[string]string
		rootAgent  bool
		wantSubstr string
	}{
		{
			name:       "missing file",
			access:     []string{"missing.yaml"},
			files:      map[string]string{"other.yaml": "ok\n"},
			wantSubstr: "missing from the exact bundle catalog",
		},
		{
			name:       "absolute path",
			access:     []string{"/etc/passwd"},
			wantSubstr: "portable relative path",
		},
		{
			name:       "traversal",
			access:     []string{"../other.yaml"},
			wantSubstr: "path traversal",
		},
		{
			name:       "backslash",
			access:     []string{`dir\\file.yaml`},
			wantSubstr: "portable relative path",
		},
		{
			name:       "project agent",
			access:     []string{"exclusions.yaml"},
			rootAgent:  true,
			wantSubstr: "only valid on flow-scoped agents",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := writeFlowDataAccessFixture(t, tc.access, tc.files, tc.rootAgent)
			repoRoot := repoRootForBootverifyTest(t)
			_, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
			if err == nil || !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("contract load error = %v, want %q", err, tc.wantSubstr)
			}
		})
	}
}

func TestCatalogRejectsNestedPhysicalFlowDataDeclarationExactlyOnce(t *testing.T) {
	root := writeNestedBootFlowDataAccessFixture(t)
	repoRoot := repoRootForBootverifyTest(t)
	_, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err == nil || strings.Count(err.Error(), "missing.md") != 1 || !strings.Contains(err.Error(), "package parent/child") {
		t.Fatalf("contract load error = %v, want one exact nested declaration rejection", err)
	}
}

func TestBootCheckRegistry_HasFlowDataAccessCheckCount(t *testing.T) {
	if got := len(bootCheckRegistry); got != 78 {
		t.Fatalf("bootCheckRegistry count = %d, want 78", got)
	}
}

func writeFlowDataAccessFixture(t *testing.T, access []string, files map[string]string, rootAgent bool) string {
	t.Helper()
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: flow-data-access
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: support
    flow: support
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: flow-data-access\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), "name: support\nmode: static\n")
	for name, content := range files {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "data", filepath.FromSlash(name)), content)
	}

	accessYAML := flowDataAccessYAML(access)
	if rootAgent {
		writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "root-agent:\n  id: root-agent\n  role: root_agent\n  intent: {inline: 'Exercise root flow data access.'}\n  memory: false\n"+accessYAML)
		return root
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), "factory-cto:\n  id: factory-cto\n  role: factory_cto\n  intent: {inline: 'Exercise flow data access.'}\n  memory: false\n"+accessYAML)
	return root
}

func writeNestedBootFlowDataAccessFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: nested-flow-data-access
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - {path: parent}
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: nested-flow-data-access\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "parent", "package.yaml"), `
name: parent
version: "1.0.0"
packages:
  - {path: child}
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "parent", "child", "package.yaml"), `
name: child
version: "1.0.0"
flows:
  - {id: support, flow: support, mode: static}
`)
	flowRoot := filepath.Join(root, "parent", "child", "flows", "support")
	writeBootverifyFixtureFile(t, filepath.Join(flowRoot, "package.yaml"), "name: support\nversion: \"1.0.0\"\nflows: []\n")
	writeBootverifyFixtureFile(t, filepath.Join(flowRoot, "schema.yaml"), "name: support\nmode: static\ninitial_state: active\nstates: [active]\n")
	writeBootverifyFixtureFile(t, filepath.Join(flowRoot, "agents.yaml"), `
worker:
  role: worker
  intent: {inline: "<!-- TODO validate one nested physical declaration. -->"}
  model: regular
  memory: false
  flow_data_access: [missing.md]
`)
	writeBootverifyFixtureFile(t, filepath.Join(flowRoot, "data", "placeholder.md"), "placeholder\n")
	return root
}

func flowDataAccessYAML(access []string) string {
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

func loadFlowDataAccessBundle(t *testing.T, root string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := repoRootForBootverifyTest(t)
	return loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
}
