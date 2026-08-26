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
	}{
		{name: "valid", declaredFile: "resume.md", writeFile: true},
		{name: "missing", declaredFile: "missing.md", wantFindings: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := writeNestedFlowDataFixture(t, tc.declaredFile, tc.writeFile)
			repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
			if err != nil {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
			}
			rawOccurrences := 0
			for _, project := range bundle.ProjectViews() {
				if _, ok := project.Agents["worker"]; ok {
					rawOccurrences++
				}
			}
			for _, flow := range bundle.FlowViews() {
				if _, ok := flow.Agents["worker"]; ok {
					rawOccurrences++
				}
			}
			if rawOccurrences < 2 {
				t.Fatalf("raw nested declaration occurrences = %d, want at least two loader views", rawOccurrences)
			}

			source := semanticview.Wrap(bundle)
			if declarations := semanticview.AgentDeclarations(source); len(declarations) != 1 {
				t.Fatalf("canonical declarations = %#v, want one physical declaration", declarations)
			}
			findings := ValidateSource(source)
			if len(findings) != tc.wantFindings {
				t.Fatalf("flow-data findings = %#v, want %d", findings, tc.wantFindings)
			}
			if tc.wantFindings == 1 && (findings[0].Filename != tc.declaredFile || !strings.Contains(findings[0].Message, "not readable")) {
				t.Fatalf("flow-data finding = %#v, want one exact missing-file result", findings[0])
			}
		})
	}
}

func writeNestedFlowDataFixture(t *testing.T, declaredFile string, writeFile bool) string {
	t.Helper()
	root := t.TempDir()
	writeFlowDataFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: nested-flow-data
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - {path: parent}
`)
	writeFlowDataFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: nested-flow-data\n")
	writeEmptyFlowDataContractFiles(t, root)
	writeFlowDataFixtureFile(t, filepath.Join(root, "parent", "package.yaml"), `
name: parent
version: "1.0.0"
packages:
  - {path: child}
`)
	writeFlowDataFixtureFile(t, filepath.Join(root, "parent", "child", "package.yaml"), `
name: child
version: "1.0.0"
flows:
  - {id: support, flow: support, mode: static}
`)
	flowRoot := filepath.Join(root, "parent", "child", "flows", "support")
	writeFlowDataFixtureFile(t, filepath.Join(flowRoot, "package.yaml"), "name: support\nversion: \"1.0.0\"\nflows: []\n")
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
