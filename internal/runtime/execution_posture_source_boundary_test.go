package runtime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func TestProcessExecutionPostureOwnsProductionLiveAuthorityLiterals(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	allowed := map[string]bool{
		"internal/runtime/agents/agent_llm.go":                                    true,
		"internal/runtime/effects/authority.go":                                   true,
		"internal/runtime/executionposture/posture.go":                            true,
		"internal/runtime/llm/selection/profile.go":                               true,
		"internal/store/internal/backend/agentpersistence/projection.go":          true,
		"internal/store/internal/backend/pipelinepersistence/standing_service.go": true,
	}
	skippedPrefixes := []string{
		"internal/events/eventtest/",
		"internal/runtime/effects/effecttest/",
		"internal/store/eventfixture/",
	}
	var violations []string
	for _, root := range []string{"internal", "cmd"} {
		err := checkoutsource.WalkDir(repoRoot, filepath.Join(repoRoot, root), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			for _, prefix := range skippedPrefixes {
				if strings.HasPrefix(rel, prefix) {
					return nil
				}
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			aliases := map[string]string{}
			for _, imported := range parsed.Imports {
				importPath, err := strconv.Unquote(imported.Path.Value)
				if err != nil {
					return err
				}
				if importPath != "github.com/division-sh/swarm/internal/runtime/executionmode" &&
					importPath != "github.com/division-sh/swarm/internal/runtime/effects" {
					continue
				}
				name := filepath.Base(importPath)
				if imported.Name != nil {
					name = imported.Name.Name
				}
				aliases[name] = importPath
			}
			comparisons := liveAuthorityComparisonSelectors(rel, parsed)
			ast.Inspect(parsed, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				qualifier, ok := selector.X.(*ast.Ident)
				if !ok {
					return true
				}
				importPath := aliases[qualifier.Name]
				isLive := importPath == "github.com/division-sh/swarm/internal/runtime/executionmode" && selector.Sel.Name == "Live"
				isEffectsLive := importPath == "github.com/division-sh/swarm/internal/runtime/effects" && selector.Sel.Name == "ExecutionModeLive"
				if (isLive || isEffectsLive) && !allowed[rel] && !comparisons[selector] {
					violations = append(violations, rel+":"+selector.Sel.Name)
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("scan production source under %s: %v", root, err)
		}
	}
	sort.Strings(violations)
	if len(violations) > 0 {
		t.Fatalf("production source minted live authority outside the process-posture owner/comparison allowlist:\n%s", strings.Join(violations, "\n"))
	}
}

func liveAuthorityComparisonSelectors(path string, file *ast.File) map[*ast.SelectorExpr]bool {
	comparisons := map[*ast.SelectorExpr]bool{}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		probe := path == "internal/runtime/runfork/preparation.go" && fn.Name.Name == "RequiresProbe"
		history := path == "internal/store/internal/runtimepersistence/test_activity_fork_history_fixture.go" && fn.Name.Name == "SeedHistoricalActivityForkAttemptForTest"
		if !probe && !history {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			comparison, ok := node.(*ast.BinaryExpr)
			if !ok {
				return true
			}
			if probe && comparison.Op == token.EQL || history && historicalLiveModeComparison(comparison) {
				if selector, ok := comparison.Y.(*ast.SelectorExpr); ok {
					comparisons[selector] = true
				}
			}
			return true
		})
	}
	return comparisons
}

func historicalLiveModeComparison(comparison *ast.BinaryExpr) bool {
	field, ok := comparison.X.(*ast.SelectorExpr)
	if !ok || comparison.Op != token.NEQ || field.Sel.Name != "ExecutionMode" {
		return false
	}
	record, ok := field.X.(*ast.Ident)
	return ok && record.Name == "record"
}

func TestHistoricalLiveModeGuardAllowsOnlyExactRefusalComparison(t *testing.T) {
	const path = "internal/store/internal/runtimepersistence/test_activity_fork_history_fixture.go"
	for _, row := range []struct {
		name, declaration, body, path string
		want                          int
	}{
		{"exact_refusal", "SeedHistoricalActivityForkAttemptForTest", "if record.ExecutionMode != executionmode.Live { return };", path, 1},
		{"assignment", "SeedHistoricalActivityForkAttemptForTest", "record.ExecutionMode = executionmode.Live", path, 0},
		{"return", "SeedHistoricalActivityForkAttemptForTest", "return executionmode.Live", path, 0},
		{"different_operand", "SeedHistoricalActivityForkAttemptForTest", "if other.ExecutionMode != executionmode.Live { return }", path, 0},
		{"different_field", "SeedHistoricalActivityForkAttemptForTest", "if record.Mode != executionmode.Live { return }", path, 0},
		{"different_operator", "SeedHistoricalActivityForkAttemptForTest", "if record.ExecutionMode == executionmode.Live { return }", path, 0},
		{"different_declaration", "InventLiveAuthority", "if record.ExecutionMode != executionmode.Live { return }", path, 0},
		{"foreign_file", "SeedHistoricalActivityForkAttemptForTest", "if record.ExecutionMode != executionmode.Live { return }", "internal/runtime/foreign.go", 0},
	} {
		t.Run(row.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), row.path, "package probe\nfunc "+row.declaration+"() {"+row.body+"}\n", 0)
			if err != nil {
				t.Fatal(err)
			}
			if got := len(liveAuthorityComparisonSelectors(row.path, file)); got != row.want {
				t.Fatalf("comparison exceptions=%d want=%d", got, row.want)
			}
		})
	}
}

func TestValidatedRuntimeDepsDoesNotProjectProfileExecutionMode(t *testing.T) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "runtime.go", nil, 0)
	if err != nil {
		t.Fatalf("parse runtime.go: %v", err)
	}
	ast.Inspect(parsed, func(node ast.Node) bool {
		typeSpec, ok := node.(*ast.TypeSpec)
		if !ok || typeSpec.Name.Name != "validatedRuntimeDeps" {
			return true
		}
		structure, ok := typeSpec.Type.(*ast.StructType)
		if !ok {
			t.Fatal("validatedRuntimeDeps is not a struct")
		}
		for _, field := range structure.Fields.List {
			for _, name := range field.Names {
				if name.Name == "ExecutionMode" {
					t.Fatal("validatedRuntimeDeps retained profile-derived ExecutionMode authority")
				}
			}
		}
		return false
	})
}
