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
		"beginReceiverDispatch":                                           false,
		"receiverRouteContext":                                            false,
		"completeCommittedPublishDispatch":                                false,
		"publishPersistedRecipientsWithScope":                             false,
		"DispatchDeliveryContinuation":                                    false,
		"dispatchIntent":                                                  false,
		"agentRouteHandle.send":                                           false,
		"internalSubscriptionHandle.send":                                 false,
		"agentDeliveryExecutionContext":                                   false,
		"agentLifecycleCoordinator.terminateIdentityWithTopologyExpected": false,
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
				if key != "agentLifecycleCoordinator.terminateIdentityWithTopologyExpected" && selectorCall(call, "context", "WithoutCancel") {
					t.Errorf("%s: %s reintroduced value-bearing cancellation detachment", path, key)
				}
				if key == "agentDeliveryExecutionContext" && selectorCall(call, "runtimeeffects", "WithAuthority") {
					t.Errorf("%s: %s reintroduced receiver-side authority shadowing", path, key)
				}
				if key == "agentLifecycleCoordinator.terminateIdentityWithTopologyExpected" && selectorCall(call, "runtimeeffects", "LifecycleTokenFromContext") {
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

func TestEventBusReceiverOwnerAdmissionStructuralGuard(t *testing.T) {
	files := parseReceiverOwnershipProductionFiles(t)
	requiredValidation := map[string]string{
		"newEventBusWithOptions":            "Validate",
		"NewAgentManagerWithOptions":        "Validate",
		"newPipelineCoordinatorWithOptions": "Validate",
		"PipelineCoordinator.intercept":     "ValidateBound",
	}
	found := make(map[string]bool, len(requiredValidation)+1)
	receiverOwnerSelected := false
	for path, file := range files {
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			key := receiverFunctionKey(fn)
			if key == "EventBus.receiverProjection" {
				found[key] = true
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					switch typed := node.(type) {
					case *ast.CallExpr:
						name := calledFunctionName(typed)
						if name == "workOwnerForContext" || name == "OccurrenceFromContext" {
							t.Errorf("%s: receiverProjection selected ambient occurrence through %s", path, name)
						}
					case *ast.SelectorExpr:
						identifier, ok := typed.X.(*ast.Ident)
						if ok && identifier.Name == "eb" && typed.Sel.Name == "workOwner" {
							receiverOwnerSelected = true
						}
					}
					return true
				})
			}
			requiredCall, guarded := requiredValidation[key]
			if guarded {
				found[key] = true
				validationFound := false
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					name := calledFunctionName(call)
					if name == requiredCall {
						validationFound = true
					}
					if name == "Configured" {
						t.Errorf("%s: %s conditionally bypasses receiver execution validation", path, key)
					}
					if name == "NormalExecution" {
						t.Errorf("%s: %s defaults missing receiver execution to normal", path, key)
					}
					return true
				})
				if !validationFound {
					t.Errorf("%s: %s no longer calls %s", path, key, requiredCall)
				}
			}

			if key == "ExecutionVariant.Kind" {
				found[key] = true
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					identifier, ok := node.(*ast.Ident)
					if ok && identifier.Name == "ExecutionNormal" {
						t.Errorf("%s: ExecutionVariant.Kind inferred normal from missing ownership", path)
					}
					return true
				})
			}
		}
	}
	for name := range requiredValidation {
		if !found[name] {
			t.Errorf("receiver fail-closed guard lost production function %s", name)
		}
	}
	for _, name := range []string{"EventBus.receiverProjection", "ExecutionVariant.Kind"} {
		if !found[name] {
			t.Errorf("receiver fail-closed guard lost production function %s", name)
		}
	}
	if !receiverOwnerSelected {
		t.Fatal("receiverProjection no longer selects the exact EventBus work owner")
	}
}

func TestDeliveryTargetOwnerAuthorityStructuralGuard(t *testing.T) {
	forbidden := map[string]bool{
		"AllowStructuralOwner":                                  false,
		"StructuralTargetOwnerEligible":                         false,
		"structuralDescriptor":                                  false,
		"routedScopedNoTargetNodeDeliveryIntents":               false,
		"routedStaticCrossFlowInstanceTarget":                   false,
		"routedDescendantStaticFlowInstanceTarget":              false,
		"routedWildcardStaticServiceNoTargetNodeDeliveryRoutes": false,
	}
	requiredCalls := map[string]bool{
		"connectRoutePlanDeliveryIntents":                        false,
		"selectedRunTargetOwnerProjection.pinRoutingDescriptors": false,
	}
	for path, file := range parseTargetOwnerAuthorityProductionFiles(t) {
		ast.Inspect(file, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if ok {
				if _, banned := forbidden[identifier.Name]; banned {
					forbidden[identifier.Name] = true
					t.Errorf("%s: retired target-owner authority %s returned", path, identifier.Name)
				}
			}
			return true
		})
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			key := receiverFunctionKey(fn)
			if _, guarded := requiredCalls[key]; !guarded {
				if _, guarded = requiredCalls[fn.Name.Name]; !guarded {
					continue
				}
				key = fn.Name.Name
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if ok && calledFunctionName(call) == "ProveStructuralTargetOwner" {
					requiredCalls[key] = true
				}
				return true
			})
		}
	}
	for name, found := range requiredCalls {
		if !found {
			t.Errorf("structural target-owner guard lost compiled proof consumption in %s", name)
		}
	}
}

func parseReceiverOwnershipProductionFiles(t testing.TB) map[string]*ast.File {
	t.Helper()
	files := make(map[string]*ast.File)
	for _, dir := range []string{
		".",
		filepath.Join("..", "manager"),
		filepath.Join("..", "pipeline"),
		filepath.Join("..", "core", "eventreceiver"),
	} {
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

func parseTargetOwnerAuthorityProductionFiles(t testing.TB) map[string]*ast.File {
	t.Helper()
	files := make(map[string]*ast.File)
	for _, dir := range []string{
		".",
		filepath.Join("..", "pipeline"),
		filepath.Join("..", "core", "pinrouting"),
	} {
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

func calledFunctionName(call *ast.CallExpr) string {
	switch function := call.Fun.(type) {
	case *ast.Ident:
		return function.Name
	case *ast.SelectorExpr:
		return function.Sel.Name
	default:
		return ""
	}
}
