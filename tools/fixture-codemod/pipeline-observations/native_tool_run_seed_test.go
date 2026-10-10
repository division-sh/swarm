package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeToolRunFixture(t *testing.T, source string) string {
	t.Helper()
	var fixture *ast.CompositeLit
	ast.Inspect(projectionShapeFunction(t, source), func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok || literal.Type == nil {
			return true
		}
		kind := formattedNativeReadNode(literal.Type)
		if kind != "storetest.RunFixture" && kind != "runlifecyclefixture.Fixture" {
			return true
		}
		if fixture != nil {
			t.Fatal("ambiguous run fixture")
		}
		fixture = literal
		return false
	})
	if fixture == nil {
		t.Fatal("missing exact run fixture")
	}
	fixture.Type = ast.NewIdent("canonicalRunFixture")
	ast.Inspect(fixture, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok && (formattedNativeReadNode(call.Fun) == "runlifecyclefixture.ScenarioSetupOrigin" || formattedNativeReadNode(call.Fun) == "storetest.ScenarioSetupOrigin") {
			call.Fun = ast.NewIdent("scenarioSetupOrigin")
		}
		return true
	})
	return formattedNativeReadNode(fixture)
}

func nativeToolRunSeedWorkload(t *testing.T, source string) string {
	t.Helper()
	source = nativeToolSparseHistorySource(source)
	body := projectionShapeFunction(t, source).Body
	astutil.Apply(body, func(cursor *astutil.Cursor) bool {
		statement, ok := cursor.Node().(ast.Stmt)
		if !ok {
			return true
		}
		text := formattedNativeReadNode(statement)
		if strings.HasPrefix(text, "fixture := runlifecyclefixture.Fixture{") ||
			strings.HasPrefix(text, "switch selected.(type) {") ||
			strings.HasPrefix(text, `if backend == "sqlite" {`) && strings.Contains(text, "runlifecyclefixture.RequireSQLite(t, ctx, db, fixture)") ||
			text == "runner, ok := selected.(storetest.RunFixtureStore)" || text == "runner, ok := persistence.(storetest.RunFixtureStore)" ||
			text == formattedNativeReadNode(projectionShapeStatement(t, `if !ok {t.Fatalf("entity test store %T has no selected run lifecycle owner",selected)}`)) ||
			text == formattedNativeReadNode(projectionShapeStatement(t, `if !ok {t.Fatal("sparse entity fixture requires selected run lifecycle owner")}`)) ||
			strings.HasPrefix(text, "if err := storetest.MaterializeRun(ctx, runner, storetest.RunFixture{") {
			cursor.Delete()
			return false
		}
		return true
	}, nil)
	return formattedNativeReadNode(body)
}

func TestNativeToolRunSeedPreservesSourceContextAndCompleteConsumers(t *testing.T) {
	for _, family := range []string{"native-tool-source-run-seed", "native-tool-sparse-run-bootstrap"} {
		row := nativeMissingHeaderRecipe(t, family)
		if nativeToolRunFixture(t, row.Before) != nativeToolRunFixture(t, row.After) || nativeToolRunSeedWorkload(t, row.Before) != nativeToolRunSeedWorkload(t, row.After) {
			t.Fatalf("run facts, context, import owner, source, workload or assertions changed: %s", family)
		}
		if !strings.Contains(row.After, "storetest.RunFixtureStore") || !strings.Contains(row.After, "storetest.MaterializeRun(ctx, runner,") {
			t.Fatal("native run owner missing")
		}
	}
}

func TestNativeToolRunSeedOracleRejectsChangedIdentityArtifactAndAssertions(t *testing.T) {
	for _, probe := range []struct{ family, from, to string }{
		{"native-tool-source-run-seed", "RunID: entityToolTestRunID", "RunID: anotherRun"},
		{"native-tool-source-run-seed", "Artifact: bundle.SourceArtifact", "Artifact: nil"},
		{"native-tool-source-run-seed", "effects.OwnerBuildTestInfrastructure", "effects.OwnerRuntime"},
		{"native-tool-sparse-run-bootstrap", "BundleHash: fact.BundleHash()", `BundleHash: "other"`},
		{"native-tool-sparse-run-bootstrap", "!reflect.DeepEqual(before, after)", "reflect.DeepEqual(before, after)"},
	} {
		row := nativeMissingHeaderRecipe(t, probe.family)
		changed := strings.Replace(row.After, probe.from, probe.to, 1)
		if changed == row.After || (nativeToolRunFixture(t, row.Before) == nativeToolRunFixture(t, changed) && nativeToolRunSeedWorkload(t, row.Before) == nativeToolRunSeedWorkload(t, changed)) {
			t.Fatalf("lost run/input/assertion contract admitted: %s", probe.from)
		}
	}
}

func TestNativeToolRunSeedCallerInventoryIsComplete(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "internal", "runtime", "tools", "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, path := range paths {
		bytes, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, bytes, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if ok && formattedNativeReadNode(call.Fun) == "seedEntityToolSourceRun" {
					counts[fn.Name.Name]++
				}
				return true
			})
		}
	}
	for name, want := range map[string]int{
		"TestRetiredCreateEntityCannotMutateEitherStore":                               1,
		"TestEntityTools_SQLiteBackendNeutralEntityPersistence":                        1,
		"TestEntityTools_ReadImportedCanonicalEntityContractOnBothStores":              1,
		"TestSQLiteEntityPersistence_MarshalsStructuredFilterValues":                   1,
		"TestRoleScopedEntityTools_SQLiteCurrentEntityPersistence":                     1,
		"TestSaveEntityFieldAcknowledgedErrorReturnsCommittedToolResponseOnBothStores": 2,
		"newEntityToolTestHarnessWithBundleAndLegacyAccess":                            1,
		"TestEntityOperationSurfaceFreshIndexAdmissionOnBothStores":                   1,
	} {
		if counts[name] != want {
			t.Fatalf("run setup caller %s=%d, want%d", name, counts[name], want)
		}
		delete(counts, name)
	}
	if len(counts) != 0 {
		t.Fatalf("unclassified run setup callers:%v", counts)
	}
}

func TestNativeToolRunSeedCandidateOverlayRejectsExternalTestTypeErrors(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "internal", "runtime", "tools", "entity_sparse_mutation_test.go")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	broken := append(source, []byte("\nvar sourceRunOverlayMustTypeCheck int = \"invalid\"\n")...)
	if err := checkTypes(root, []pendingFile{{path: path, data: broken}}); err == nil || !strings.Contains(err.Error(), "candidate type checking failed") {
		t.Fatalf("external tool test overlay was not independently typed: %v", err)
	}
}
