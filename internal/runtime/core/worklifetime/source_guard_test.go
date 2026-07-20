package worklifetime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

var retiredLifetimeOwners = []string{
	"inFlightPublishes",
	"inFlightEventIDs",
	"runtimeQuiescenceStableChecks",
	"PendingAgentRouteDeliveries",
	"PendingAgentDeliveries",
}

func TestProductionAsyncWorkUsesCanonicalTypedOwners(t *testing.T) {
	repoRoot := workLifetimeRepositoryRoot(t)
	for _, rootName := range []string{"cmd", "internal/runtime", "internal/serveapp", "internal/apiv1", "internal/builder", "internal/cliapp"} {
		root := filepath.Join(repoRoot, rootName)
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			relative, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			checkProductionWorkLifetimeFile(t, path, relative)
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", rootName, err)
		}
	}
}

func checkProductionWorkLifetimeFile(t *testing.T, path, relative string) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", relative, err)
	}
	eventAliases := importAliases(file, "github.com/division-sh/swarm/internal/events")
	workAliases := importAliases(file, "github.com/division-sh/swarm/internal/runtime/core/worklifetime")

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", relative, err)
	}
	for _, retired := range retiredLifetimeOwners {
		if strings.Contains(string(raw), retired) {
			t.Fatalf("%s retains retired process-local lifetime owner %q", relative, retired)
		}
	}

	ast.Inspect(file, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.BlockStmt:
			for index := 0; index+1 < len(value.List); index++ {
				settle, settleOK := value.List[index].(*ast.DeferStmt)
				signal, signalOK := value.List[index+1].(*ast.DeferStmt)
				if settleOK && signalOK && deferredCallContainsDone(settle) && deferredCallIsClose(signal) {
					t.Fatalf("%s:%d defers work settlement before completion signaling; defer execution would signal first", relative, fset.Position(settle.Pos()).Line)
				}
			}
		case *ast.ChanType:
			if isImportedType(value.Value, eventAliases, "Event") {
				t.Fatalf("%s:%d uses raw events.Event as an asynchronous carrier", relative, fset.Position(value.Pos()).Line)
			}
		case *ast.CallExpr:
			selector, ok := value.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "NewProcess" || !isPackageIdent(selector.X, workAliases) {
				return true
			}
			if relative != "internal/serveapp/main.go" {
				t.Fatalf("%s:%d creates a private process work owner outside the serve root", relative, fset.Position(value.Pos()).Line)
			}
		}
		return true
	})
}

func deferredCallContainsDone(statement *ast.DeferStmt) bool {
	found := false
	ast.Inspect(statement.Call, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == "Done" {
			found = true
			return false
		}
		return true
	})
	return found
}

func deferredCallIsClose(statement *ast.DeferStmt) bool {
	identifier, ok := statement.Call.Fun.(*ast.Ident)
	return ok && identifier.Name == "close"
}

func importAliases(file *ast.File, importPath string) map[string]struct{} {
	aliases := map[string]struct{}{}
	for _, imported := range file.Imports {
		if strings.Trim(imported.Path.Value, `"`) != importPath {
			continue
		}
		name := filepath.Base(importPath)
		if imported.Name != nil {
			name = imported.Name.Name
		}
		if name != "_" && name != "." {
			aliases[name] = struct{}{}
		}
	}
	return aliases
}

func isImportedType(expr ast.Expr, aliases map[string]struct{}, name string) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == name && isPackageIdent(selector.X, aliases)
}

func isPackageIdent(expr ast.Expr, aliases map[string]struct{}) bool {
	identifier, ok := expr.(*ast.Ident)
	if !ok {
		return false
	}
	_, ok = aliases[identifier.Name]
	return ok
}

func workLifetimeRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve work lifetime source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}
