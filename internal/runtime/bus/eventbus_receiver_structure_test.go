package bus

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEventBusReceiverOwnershipBoundaryStructuralGuard(t *testing.T) {
	files := parseReceiverOwnershipProductionFiles(t)
	preparedPublishFound := false
	receiverProjectionFound := false
	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			typeSpec, ok := node.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != "PreparedPublish" {
				return true
			}
			preparedPublishFound = true
			structure, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				t.Fatalf("%s: PreparedPublish is not a struct", path)
			}
			for _, field := range structure.Fields.List {
				for _, name := range field.Names {
					if name.Name == "dispatchContext" || isContextType(field.Type) {
						t.Errorf("%s: PreparedPublish must not retain raw context field %q", path, name.Name)
					}
					if name.Name == "receiver" && expressionName(field.Type) == "receiverDispatchProjection" {
						receiverProjectionFound = true
					}
				}
			}
			return false
		})
	}
	if !preparedPublishFound {
		t.Fatal("PreparedPublish owner was not found")
	}
	if !receiverProjectionFound {
		t.Fatal("PreparedPublish is not bound to the closed receiver projection")
	}

	forbiddenFunctions := map[string]bool{
		"preparedPublishDispatchContext": false,
		"localDeliveryContext":           false,
	}
	guardedFunctions := map[string]bool{
		"beginReceiverDispatch":               false,
		"receiverRouteContext":                false,
		"completeCommittedPublishDispatch":    false,
		"publishPersistedRecipientsWithScope": false,
		"DispatchDeliveryContinuation":        false,
		"dispatchIntent":                      false,
		"agentRouteHandle.send":               false,
		"internalSubscriptionHandle.send":     false,
		"agentDeliveryExecutionContext":       false,
		"agentLifecycleCoordinator.terminateIdentityWithTopologyCommitHooks": false,
	}
	for path, file := range files {
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if _, forbidden := forbiddenFunctions[fn.Name.Name]; forbidden {
				t.Errorf("%s: legacy receiver carrier %s returned", path, fn.Name.Name)
			}
			key := receiverFunctionKey(fn)
			if _, guarded := guardedFunctions[key]; !guarded {
				if _, guarded = guardedFunctions[fn.Name.Name]; !guarded {
					continue
				}
				key = fn.Name.Name
			}
			guardedFunctions[key] = true
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if key != "agentLifecycleCoordinator.terminateIdentityWithTopologyCommitHooks" && selectorCall(call, "context", "WithoutCancel") {
					t.Errorf("%s: %s reintroduced value-bearing cancellation detachment", path, key)
				}
				if key == "agentDeliveryExecutionContext" && selectorCall(call, "runtimeeffects", "WithAuthority") {
					t.Errorf("%s: %s reintroduced receiver-side authority shadowing", path, key)
				}
				if key == "agentLifecycleCoordinator.terminateIdentityWithTopologyCommitHooks" && selectorCall(call, "runtimeeffects", "LifecycleTokenFromContext") {
					t.Errorf("%s: %s reintroduced publisher-token retirement authority", path, key)
				}
				return true
			})
		}
	}
	for name, found := range guardedFunctions {
		if !found {
			t.Errorf("receiver ownership guard lost production function %s", name)
		}
	}
}

func parseReceiverOwnershipProductionFiles(t testing.TB) map[string]*ast.File {
	t.Helper()
	files := make(map[string]*ast.File)
	for _, dir := range []string{".", filepath.Join("..", "manager")} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			files[path] = file
		}
	}
	return files
}

func isContextType(expression ast.Expr) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Context" {
		return false
	}
	identifier, ok := selector.X.(*ast.Ident)
	return ok && identifier.Name == "context"
}

func expressionName(expression ast.Expr) string {
	identifier, _ := expression.(*ast.Ident)
	if identifier == nil {
		return ""
	}
	return identifier.Name
}

func receiverFunctionKey(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return fn.Name.Name
	}
	receiver := expressionName(fn.Recv.List[0].Type)
	if pointer, ok := fn.Recv.List[0].Type.(*ast.StarExpr); ok {
		receiver = expressionName(pointer.X)
	}
	return receiver + "." + fn.Name.Name
}

func selectorCall(call *ast.CallExpr, packageName, functionName string) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != functionName {
		return false
	}
	identifier, ok := selector.X.(*ast.Ident)
	return ok && identifier.Name == packageName
}
