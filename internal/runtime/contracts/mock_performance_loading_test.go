package contracts

import (
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/mockperformance"
)

func TestMaterializeAgentMockPerformancesCapturesExactGenerationBytes(t *testing.T) {
	root := t.TempDir()
	module := filepath.Join(root, "mocks", "assistant.py")
	writeAgentMockTestFile(t, filepath.Join(root, "agents.yaml"), "assistant: {}\n")
	original := []byte("def handle(input):\n    return {'text': 'first'}\n")
	writeAgentMockTestFile(t, module, string(original))
	entries, err := materializeAgentMockPerformances(agentMockTestSource(root, root, ".", filepath.Join(root, "agents.yaml")), map[string]AgentRegistryEntry{
		"assistant": {Mock: mockperformance.Performance{Kind: "python", Module: "mocks/assistant.py"}},
	})
	if err != nil {
		t.Fatalf("materialize mock performance: %v", err)
	}
	performance := entries["assistant"].Mock
	if performance.Module != "mocks/assistant.py" || string(performance.Source) != string(original) || !strings.HasPrefix(performance.Digest, "sha256:") || performance.SourcePath != "mocks/assistant.py" {
		t.Fatalf("materialized performance = %#v", performance)
	}
	if err := os.WriteFile(module, []byte("def handle(input):\n    return {'text': 'second'}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if string(performance.Source) != string(original) {
		t.Fatalf("compiled generation reread ambient module: %q", performance.Source)
	}
}

func TestMaterializeAgentMockPerformancesRejectsHostileModulePaths(t *testing.T) {
	tests := []struct {
		name   string
		module string
		setup  func(*testing.T, string)
		want   string
	}{
		{name: "blank", module: "   ", want: "path is required"},
		{name: "leading whitespace", module: " mocks/assistant.py", want: "whitespace"},
		{name: "trailing whitespace", module: "mocks/assistant.py ", want: "whitespace"},
		{name: "dot", module: ".", want: "dot"},
		{name: "dot segment", module: "mocks/./assistant.py", want: "dot"},
		{name: "traversal outside package inside root", module: "../sibling/outside.py", setup: func(t *testing.T, root string) {
			writeAgentMockTestFile(t, filepath.Join(root, "sibling", "outside.py"), validAgentMockSource("outside"))
		}, want: "traversal"},
		{name: "empty segment", module: "mocks//assistant.py", want: "empty"},
		{name: "absolute posix", module: "/tmp/assistant.py", want: "relative"},
		{name: "absolute windows slash", module: "C:/mocks/assistant.py", want: "relative"},
		{name: "absolute windows backslash", module: `C:\mocks\assistant.py`, want: "backslashes"},
		{name: "unc", module: `\\server\share\assistant.py`, want: "backslashes"},
		{name: "nul", module: "mocks/assistant\x00.py", want: "NUL"},
		{name: "missing", module: "mocks/missing.py", want: "cannot be read"},
		{name: "directory", module: "mocks", setup: func(t *testing.T, root string) {
			if err := os.MkdirAll(filepath.Join(root, "child", "mocks"), 0o755); err != nil {
				t.Fatal(err)
			}
		}, want: "regular file"},
		{name: "unreadable", module: "mocks/unreadable.py", setup: func(t *testing.T, root string) {
			path := filepath.Join(root, "child", "mocks", "unreadable.py")
			writeAgentMockTestFile(t, path, validAgentMockSource("unreadable"))
			if err := os.Chmod(path, 0); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(path, 0o600) })
		}, want: "not readable"},
		{name: "final symlink", module: "mocks/link.py", setup: func(t *testing.T, root string) {
			target := filepath.Join(root, "child", "mocks", "target.py")
			writeAgentMockTestFile(t, target, validAgentMockSource("target"))
			if err := os.Symlink(target, filepath.Join(root, "child", "mocks", "link.py")); err != nil {
				t.Skipf("symlink unavailable: %v", err)
			}
		}, want: "symlinks"},
		{name: "intermediate symlink", module: "linked/assistant.py", setup: func(t *testing.T, root string) {
			targetDir := filepath.Join(root, "child", "actual")
			writeAgentMockTestFile(t, filepath.Join(targetDir, "assistant.py"), validAgentMockSource("target"))
			if err := os.Symlink(targetDir, filepath.Join(root, "child", "linked")); err != nil {
				t.Skipf("symlink unavailable: %v", err)
			}
		}, want: "symlinks"},
		{name: "non regular socket", module: "mocks/socket.py", setup: func(t *testing.T, root string) {
			path := filepath.Join(root, "child", "mocks", "socket.py")
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			listener, err := net.Listen("unix", path)
			if err != nil {
				t.Skipf("unix socket unavailable: %v", err)
			}
			t.Cleanup(func() { _ = listener.Close() })
		}, want: "regular file"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			packageRoot := filepath.Join(root, "child")
			declaration := filepath.Join(packageRoot, "agents.yaml")
			writeAgentMockTestFile(t, declaration, "assistant: {}\n")
			if tc.setup != nil {
				tc.setup(t, root)
			}
			_, err := materializeAgentMockPerformances(agentMockTestSource(root, packageRoot, "child", declaration), map[string]AgentRegistryEntry{
				"assistant": {Mock: mockperformance.Performance{Kind: "python", Module: tc.module}},
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestMaterializeAgentMockPerformancesRejectsDeclarationProvenanceContradictions(t *testing.T) {
	root := t.TempDir()
	child := filepath.Join(root, "child")
	declaration := filepath.Join(child, "agents.yaml")
	writeAgentMockTestFile(t, declaration, "assistant: {}\n")
	writeAgentMockTestFile(t, filepath.Join(child, "mocks", "assistant.py"), validAgentMockSource("child"))
	writeAgentMockTestFile(t, filepath.Join(root, "agents.yaml"), "assistant: {}\n")

	tests := []struct {
		name   string
		source agentMockMaterializationSource
		want   string
	}{
		{name: "missing package key", source: agentMockTestSource(root, child, "", declaration), want: "package key is required"},
		{name: "package root disagrees", source: agentMockTestSource(root, root, "child", declaration), want: "disagrees"},
		{name: "declaration outside package", source: agentMockTestSource(root, child, "child", filepath.Join(root, "agents.yaml")), want: "outside declaring package"},
		{name: "package traversal", source: agentMockTestSource(root, child, "../child", declaration), want: "traversal"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := materializeAgentMockPerformances(tc.source, map[string]AgentRegistryEntry{
				"assistant": {Mock: mockperformance.Performance{Kind: "python", Module: "mocks/assistant.py"}},
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestLoadWorkflowContractBundleResolvesMockModulesFromExactDeclaringPackages(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root, child := writeImportedAgentMockFixture(t, []string{"child"})
	platform := DefaultPlatformSpecFile(repo)

	standalone, err := LoadWorkflowContractBundleWithOverrides(repo, child, platform)
	if err != nil {
		t.Fatalf("load standalone child: %v", err)
	}
	imported, err := LoadWorkflowContractBundleWithOverrides(repo, root, platform)
	if err != nil {
		t.Fatalf("load imported child: %v", err)
	}

	rootProject, ok := imported.ProjectViewByKey(".")
	if !ok {
		t.Fatal("root project view missing")
	}
	assertAgentMockPerformance(t, rootProject.Agents["project-agent"].Mock, "mocks/root-project.py", "mocks/root-project.py", validAgentMockSource("root-project"))
	rootFlow, ok := imported.FlowViewByID("root-flow")
	if !ok {
		t.Fatal("root flow view missing")
	}
	assertAgentMockPerformance(t, rootFlow.Agents["flow-agent"].Mock, "mocks/root-flow.py", "mocks/root-flow.py", validAgentMockSource("root-flow"))

	standaloneProject, ok := standalone.ProjectViewByKey(".")
	if !ok {
		t.Fatal("standalone project view missing")
	}
	importedProject, ok := imported.ProjectViewByKey("child")
	if !ok {
		t.Fatal("imported project view missing")
	}
	standaloneProjectMock := standaloneProject.Agents["project-agent"].Mock
	importedProjectMock := importedProject.Agents["project-agent"].Mock
	assertAgentMockPerformance(t, standaloneProjectMock, "mocks/child-project.py", "mocks/child-project.py", validAgentMockSource("child-project"))
	assertAgentMockPerformance(t, importedProjectMock, "mocks/child-project.py", "child/mocks/child-project.py", validAgentMockSource("child-project"))
	if standaloneProjectMock.Digest != importedProjectMock.Digest || !reflect.DeepEqual(standaloneProjectMock.Source, importedProjectMock.Source) {
		t.Fatalf("standalone/imported project artifacts differ: standalone=%#v imported=%#v", standaloneProjectMock, importedProjectMock)
	}

	standaloneFlow, ok := standalone.FlowViewByID("child-flow")
	if !ok {
		t.Fatal("standalone child flow missing")
	}
	importedFlow, ok := imported.FlowViewByID("child-flow")
	if !ok {
		t.Fatal("imported child flow missing")
	}
	standaloneFlowMock := standaloneFlow.Agents["flow-agent"].Mock
	importedFlowMock := importedFlow.Agents["flow-agent"].Mock
	assertAgentMockPerformance(t, standaloneFlowMock, "mocks/child-flow.py", "mocks/child-flow.py", validAgentMockSource("child-flow"))
	assertAgentMockPerformance(t, importedFlowMock, "mocks/child-flow.py", "child/mocks/child-flow.py", validAgentMockSource("child-flow"))
	if standaloneFlowMock.Digest != importedFlowMock.Digest || !reflect.DeepEqual(standaloneFlowMock.Source, importedFlowMock.Source) {
		t.Fatalf("standalone/imported flow artifacts differ: standalone=%#v imported=%#v", standaloneFlowMock, importedFlowMock)
	}
}

func TestLoadWorkflowContractBundleMockModuleOwnershipIsPackageOrderIndependent(t *testing.T) {
	for _, order := range [][]string{{"left", "right"}, {"right", "left"}} {
		t.Run(strings.Join(order, "-then-"), func(t *testing.T) {
			repo := repoRootForContractsTest(t)
			root, _ := writeImportedAgentMockFixture(t, order)
			bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatalf("load package order %v: %v", order, err)
			}
			for _, packageKey := range order {
				view, ok := bundle.ProjectViewByKey(packageKey)
				if !ok {
					t.Fatalf("project package %q missing", packageKey)
				}
				assertAgentMockPerformance(t, view.Agents["project-agent"].Mock, "mocks/child-project.py", packageKey+"/mocks/child-project.py", validAgentMockSource(packageKey+"-project"))
			}
		})
	}
}

func agentMockTestSource(contractsRoot, packageRoot, packageKey, declarationFile string) agentMockMaterializationSource {
	return agentMockMaterializationSource{
		ContractsRoot: contractsRoot,
		PackageRoot:   packageRoot,
		Declaration:   ContractItemSource{PackageKey: packageKey, Layer: "project", File: declarationFile},
	}
}

func assertAgentMockPerformance(t *testing.T, got mockperformance.Performance, module, sourcePath, source string) {
	t.Helper()
	if got.Module != module || got.SourcePath != sourcePath || string(got.Source) != source || !strings.HasPrefix(got.Digest, "sha256:") {
		t.Fatalf("mock performance = %#v, want module=%q source_path=%q source=%q", got, module, sourcePath, source)
	}
}

func validAgentMockSource(text string) string {
	return "def handle(input):\n    return {'text': '" + text + "'}\n"
}

func writeImportedAgentMockFixture(t *testing.T, childOrder []string) (string, string) {
	t.Helper()
	root := t.TempDir()
	var packages strings.Builder
	if len(childOrder) > 0 {
		packages.WriteString("packages:\n")
		for _, child := range childOrder {
			packages.WriteString("  - id: " + child + "\n    path: " + child + "\n")
		}
	}
	writeAgentMockTestFile(t, filepath.Join(root, "package.yaml"), `name: outer
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
`+packages.String()+`flows:
  - id: root-flow
    flow: root-flow
    mode: static
`)
	writeAgentMockTestFile(t, filepath.Join(root, "agents.yaml"), agentMockRegistryYAML("project-agent", "root-project.py", "Root project intent."))
	writeAgentMockTestFile(t, filepath.Join(root, "mocks", "root-project.py"), validAgentMockSource("root-project"))
	writeAgentMockTestFile(t, filepath.Join(root, "mocks", "root-flow.py"), validAgentMockSource("root-flow"))
	writeAgentMockTestFile(t, filepath.Join(root, "flows", "root-flow", "schema.yaml"), "name: root-flow\nmode: static\n")
	writeAgentMockTestFile(t, filepath.Join(root, "flows", "root-flow", "agents.yaml"), agentMockRegistryYAML("flow-agent", "root-flow.py", "Root flow intent."))

	for _, child := range childOrder {
		childRoot := filepath.Join(root, child)
		writeAgentMockTestFile(t, filepath.Join(childRoot, "package.yaml"), `name: `+child+`
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: `+child+`-flow
    flow: child-flow
    mode: static
`)
		writeAgentMockTestFile(t, filepath.Join(childRoot, "agents.yaml"), agentMockRegistryYAML("project-agent", "child-project.py", "Child project intent."))
		writeAgentMockTestFile(t, filepath.Join(childRoot, "mocks", "child-project.py"), validAgentMockSource(child+"-project"))
		writeAgentMockTestFile(t, filepath.Join(childRoot, "mocks", "child-flow.py"), validAgentMockSource(child+"-flow"))
		writeAgentMockTestFile(t, filepath.Join(childRoot, "flows", "child-flow", "schema.yaml"), "name: child-flow\nmode: static\n")
		writeAgentMockTestFile(t, filepath.Join(childRoot, "flows", "child-flow", "agents.yaml"), agentMockRegistryYAML("flow-agent", "child-flow.py", "Child flow intent."))
	}
	child := ""
	if len(childOrder) > 0 {
		child = filepath.Join(root, childOrder[0])
	}
	return root, child
}

func agentMockRegistryYAML(agentID, module, intent string) string {
	return agentID + `:
  id: ` + agentID + `
  role: helper
  model: regular
  intent: {inline: "` + intent + `"}
  mock:
    kind: python
    module: mocks/` + module + `
`
}

func writeAgentMockTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
