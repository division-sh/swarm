package flowdata

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestValidateSourceEvaluatesNestedPhysicalAgentDeclarationExactlyOnce(t *testing.T) {
	for _, tc := range []struct {
		name         string
		declaredFile string
		writeFile    bool
		wantFindings int
		wantLoadErr  string
	}{
		{name: "valid", declaredFile: "resume.md", writeFile: true},
		{name: "missing", declaredFile: "missing.md", wantLoadErr: "is missing from the admitted source artifact"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := writeNestedFlowDataFixture(t, tc.declaredFile, tc.writeFile)
			repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
			if tc.wantLoadErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantLoadErr) {
					t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want %q", err, tc.wantLoadErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
			}
			rawOccurrences := 0
			for _, flow := range bundle.FlowViews() {
				if _, ok := flow.Agents["worker"]; ok {
					rawOccurrences++
				}
			}
			if rawOccurrences != 1 {
				t.Fatalf("raw nested declaration occurrences = %d, want one filesystem-owned flow declaration", rawOccurrences)
			}

			source := semanticview.Wrap(bundle)
			if declarations := semanticview.AgentDeclarations(source); len(declarations) != 1 {
				t.Fatalf("canonical declarations = %#v, want one physical declaration", declarations)
			}
			findings := ValidateSource(source)
			if len(findings) != tc.wantFindings {
				t.Fatalf("flow-data findings = %#v, want %d", findings, tc.wantFindings)
			}
		})
	}
}

func writeNestedFlowDataFixture(t *testing.T, declaredFile string, writeFile bool) string {
	t.Helper()
	root := t.TempDir()
	writeFlowDataFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: nested-flow-data\n")
	writeEmptyFlowDataContractFiles(t, root)
	flowRoot := filepath.Join(root, "support")
	writeFlowDataFixtureFile(t, filepath.Join(flowRoot, "schema.yaml"), "name: support\nmode: static\ninitial_state: active\nstates: [active]\n")
	writeFlowDataFixtureFile(t, filepath.Join(flowRoot, "agents.yaml"), `
worker:
  role: worker
  intent: {inline: Validate one nested physical declaration.}
  model: regular
  memory: false
  flow_data_access: [`+declaredFile+`]
`)
	writeEmptyFlowDataContractFiles(t, flowRoot)
	writeFlowDataFixtureFile(t, filepath.Join(flowRoot, "data", "placeholder.md"), "placeholder\n")
	if writeFile {
		writeFlowDataFixtureFile(t, filepath.Join(flowRoot, "data", filepath.FromSlash(declaredFile)), "available\n")
	}
	return root
}

func writeEmptyFlowDataContractFiles(t *testing.T, root string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(root, "agents.yaml")); os.IsNotExist(err) {
	}
}

func writeFlowDataFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", path, err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}
