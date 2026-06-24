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
			wantSubstr: "not readable",
		},
		{
			name:       "absolute path",
			access:     []string{"/etc/passwd"},
			wantSubstr: "absolute paths",
		},
		{
			name:       "traversal",
			access:     []string{"../other.yaml"},
			wantSubstr: "path traversal",
		},
		{
			name:       "backslash",
			access:     []string{`dir\\file.yaml`},
			wantSubstr: "platform-specific",
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
			report := Run(context.Background(), semanticview.Wrap(loadFlowDataAccessBundle(t, root)), Options{})
			if !reportContains(report.Errors(), "flow_data_access_validation", tc.wantSubstr) {
				t.Fatalf("expected flow_data_access_validation containing %q, got %#v", tc.wantSubstr, report.Errors())
			}
		})
	}
}

func TestBootCheckRegistry_HasFlowDataAccessCheckCount(t *testing.T) {
	if got := len(bootCheckRegistry); got != 56 {
		t.Fatalf("bootCheckRegistry count = %d, want 56", got)
	}
}

func writeFlowDataAccessFixture(t *testing.T, access []string, files map[string]string, rootAgent bool) string {
	t.Helper()
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: flow-data-access
version: "1.0.0"
platform_version: ">=1.0.0"
flows:
  - id: support
    flow: support
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: flow-data-access\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), "name: support\nmode: static\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), "{}\n")
	for name, content := range files {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "data", filepath.FromSlash(name)), content)
	}

	accessYAML := flowDataAccessYAML(access)
	if rootAgent {
		writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "root-agent:\n  id: root-agent\n  role: root_agent\n"+accessYAML)
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), "{}\n")
		return root
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), "factory-cto:\n  id: factory-cto\n  role: factory_cto\n"+accessYAML)
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
