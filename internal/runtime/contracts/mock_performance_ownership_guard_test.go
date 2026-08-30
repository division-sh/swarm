package contracts

import (
	"go/ast"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

type mockPerformanceCoordinateReader struct {
	Path      string
	Enclosing string
	Field     string
}

func (r mockPerformanceCoordinateReader) key() string {
	return strings.Join([]string{r.Path, r.Enclosing, r.Field}, "::")
}

func TestMockPerformanceCoordinateReaderBoundary(t *testing.T) {
	readers := loadMockPerformanceCoordinateReaders(t, nil)
	var unclassified []string
	for _, reader := range readers {
		if _, ok := allowedMockPerformanceCoordinateReaders()[reader.key()]; !ok {
			unclassified = append(unclassified, reader.key())
		}
	}
	if len(unclassified) > 0 {
		t.Fatalf("unclassified mock performance coordinate readers:\n%s", strings.Join(unclassified, "\n"))
	}
}

func TestMockPerformanceCoordinateReaderBoundaryRejectsHostileReaders(t *testing.T) {
	root := mockPerformanceGuardRepoRoot(t)
	path := filepath.Join(root, "internal", "runtime", "contracts", "mock_performance_guard_hostile.go")
	overlay := map[string][]byte{path: []byte(`package contracts

import (
    "os"
    "path/filepath"

    "github.com/division-sh/swarm/internal/runtime/mockperformance"
)

func hostileAuthoredModuleReader(root string, performance mockperformance.Performance) string {
    return filepath.Join(root, performance.Module)
}

func hostileCompiledSourcePathReader(root string, performance mockperformance.Performance) ([]byte, error) {
    return os.ReadFile(filepath.Join(root, performance.SourcePath))
}
`)}
	readers := loadMockPerformanceCoordinateReaders(t, overlay)
	want := map[string]bool{"Module": false, "SourcePath": false}
	for _, reader := range readers {
		if strings.Contains(reader.Enclosing, "hostile") {
			want[reader.Field] = true
		}
	}
	for field, found := range want {
		if !found {
			t.Fatalf("hostile ownership guard did not detect %s reader: %#v", field, readers)
		}
	}
}

func allowedMockPerformanceCoordinateReaders() map[string]string {
	return map[string]string{
		"internal/runtime/contracts/bundle_hash.go::(*bundleHashEntryBuilder).addAgentMockModuleFiles::SourcePath":          "canonical compiled path identity and exact-byte bundle input owner",
		"internal/runtime/contracts/mock_performance_loading.go::materializeAgentMockPerformancesFromSource::Module":        "sole admitted-artifact module-label interpreter",
		"internal/runtime/core/actors/agent_config.go::(*AgentConfig).NormalizeRuntimeDescriptor::Module":                   "immutable runtime carrier normalization",
		"internal/runtime/core/actors/agent_config.go::(*AgentConfig).NormalizeRuntimeDescriptor::SourcePath":               "immutable runtime carrier normalization",
		"internal/runtime/llm/mock_runtime.go::executeMockCompletionWithExecutor::SourcePath":                               "diagnostic Python row identity; executes captured Source and Digest",
		"internal/runtime/mockperformance/model.go::(Performance).Configured::Module":                                       "authored artifact presence predicate",
		"internal/store/internal/backend/agentpersistence/projection.go::decodePersistedAgentRuntimeDescriptor::Module":     "immutable persistence carrier normalization",
		"internal/store/internal/backend/agentpersistence/projection.go::decodePersistedAgentRuntimeDescriptor::SourcePath": "immutable persistence carrier normalization",
	}
}

func loadMockPerformanceCoordinateReaders(t *testing.T, overlay map[string][]byte) []mockPerformanceCoordinateReader {
	t.Helper()
	root := mockPerformanceGuardRepoRoot(t)
	cfg := &packages.Config{
		Dir: root,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
		Tests:   false,
		Overlay: overlay,
	}
	pkgs, err := packages.Load(cfg, "./internal/...")
	if err != nil {
		t.Fatalf("load mock performance ownership packages: %v", err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		t.Fatal("load mock performance ownership packages reported type errors")
	}
	var readers []mockPerformanceCoordinateReader
	for _, pkg := range pkgs {
		for index, file := range pkg.Syntax {
			if index >= len(pkg.CompiledGoFiles) || strings.HasSuffix(pkg.CompiledGoFiles[index], "_test.go") {
				continue
			}
			rel, err := filepath.Rel(root, pkg.CompiledGoFiles[index])
			if err != nil {
				t.Fatal(err)
			}
			readers = append(readers, collectMockPerformanceCoordinateReaders(filepath.ToSlash(rel), file, pkg.TypesInfo)...)
		}
	}
	sort.Slice(readers, func(i, j int) bool { return readers[i].key() < readers[j].key() })
	return readers
}

func collectMockPerformanceCoordinateReaders(path string, file *ast.File, info *types.Info) []mockPerformanceCoordinateReader {
	var readers []mockPerformanceCoordinateReader
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		enclosing := mockPerformanceGuardFunctionName(function)
		ast.Inspect(function.Body, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok || !isMockPerformanceCoordinateField(selector, info) {
				return true
			}
			readers = append(readers, mockPerformanceCoordinateReader{Path: path, Enclosing: enclosing, Field: selector.Sel.Name})
			return true
		})
	}
	return readers
}

func isMockPerformanceCoordinateField(selector *ast.SelectorExpr, info *types.Info) bool {
	if selector == nil || selector.Sel == nil || info == nil || (selector.Sel.Name != "Module" && selector.Sel.Name != "SourcePath") {
		return false
	}
	field, ok := info.Uses[selector.Sel].(*types.Var)
	return ok && field.IsField() && field.Pkg() != nil &&
		field.Pkg().Path() == "github.com/division-sh/swarm/internal/runtime/mockperformance"
}

func mockPerformanceGuardFunctionName(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) == 0 {
		return function.Name.Name
	}
	receiver := function.Recv.List[0].Type
	switch typed := receiver.(type) {
	case *ast.Ident:
		return "(" + typed.Name + ")." + function.Name.Name
	case *ast.StarExpr:
		if name, ok := typed.X.(*ast.Ident); ok {
			return "(*" + name.Name + ")." + function.Name.Name
		}
	}
	return function.Name.Name
}

func mockPerformanceGuardRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}
