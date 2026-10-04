package effects

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

var mockWorkerPrimitives = []string{
	"internal/runtime/workspace/worker_execution.go:RunWorker:process_launch:1",
	"internal/runtime/workspace/worker_remote_execution.go:runDockerWorker:process_launch:2",
}

// RunWorker also launches free identity/list observations and transported tool
// calls. Their owners must not be inferred from a mock completion registration.
// Only the exact model delegation below consumes the completion attempt.
func delegatedMockWorkerPrimitive(key string, adapters []string, owner primitiveOwner) bool {
	if owner != ownerRuntimeDependency || !reflect.DeepEqual(adapters, []string{"mock_python"}) {
		return false
	}
	for _, expected := range mockWorkerPrimitives {
		if key == expected {
			return true
		}
	}
	return false
}

func verifyMockWorkerDelegation(root, primitiveKey string) error {
	raw, err := os.ReadFile(filepath.Join(root, "internal/runtime/llm/mock_runtime.go"))
	if err != nil {
		return err
	}
	if err := verifyMockModelAdmission(raw); err != nil {
		return err
	}
	actual, err := collectDirectPrimitives(root)
	if err != nil {
		return err
	}
	if _, ok := actual[primitiveKey]; !ok {
		return fmt.Errorf("delegated worker primitive is not live: %s", primitiveKey)
	}
	workspaceRaw, err := os.ReadFile(filepath.Join(root, "internal/runtime/workspace/worker_execution.go"))
	if err != nil {
		return err
	}
	run, err := mockManifestFunction(workspaceRaw, "RunWorker")
	if err != nil {
		return err
	}
	if !functionHasCall(run, func(call *ast.CallExpr) bool { return isLocalCall(call, "runDockerWorker") }) {
		return fmt.Errorf("RunWorker does not delegate its Docker launch")
	}
	return nil
}

func verifyMockModelAdmission(raw []byte) error {
	completion, err := mockManifestFunction(raw, "executeMockCompletionWithExecutor")
	if err != nil {
		return err
	}
	var begin, heartbeat, launch, execute token.Pos
	ast.Inspect(completion.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch {
		case isLocalCall(call, "beginProviderCompletion"):
			begin = call.Pos()
		case isLocalCall(call, "startCompletionAttemptHeartbeat"):
			heartbeat = call.Pos()
		case isMethodCall(call, "MarkLaunched"):
			launch = call.Pos()
		case isLocalCall(call, "execute"):
			execute = call.Pos()
		}
		return true
	})
	if begin == token.NoPos || heartbeat == token.NoPos || launch == token.NoPos || execute == token.NoPos || !(begin < heartbeat && heartbeat < launch && launch < execute) {
		return fmt.Errorf("mock model requires Begin < heartbeat < MarkLaunched < delegated execution")
	}
	continuation, err := mockManifestFunction(raw, "continueSession")
	if err != nil {
		return err
	}
	if !functionHasCall(continuation, func(call *ast.CallExpr) bool {
		if !isLocalCall(call, "executeMockCompletionWithExecutor") || len(call.Args) != 8 {
			return false
		}
		callback, ok := call.Args[7].(*ast.FuncLit)
		if !ok || len(callback.Body.List) != 1 {
			return false
		}
		ret, ok := callback.Body.List[0].(*ast.ReturnStmt)
		if !ok || len(ret.Results) != 1 {
			return false
		}
		delegate, ok := ret.Results[0].(*ast.CallExpr)
		return ok && isMethodCall(delegate, "executeWorkspaceMockModel")
	}) {
		return fmt.Errorf("mock continuation is not bound to its managed model callback")
	}
	model, err := mockManifestFunction(raw, "executeWorkspaceMockModel")
	if err != nil {
		return err
	}
	if !functionHasCall(model, func(call *ast.CallExpr) bool {
		if !isMethodCall(call, "RunWorker") || len(call.Args) != 4 {
			return false
		}
		request, ok := call.Args[3].(*ast.CompositeLit)
		if !ok {
			return false
		}
		mode, module := false, false
		for _, field := range request.Elts {
			pair, ok := field.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := pair.Key.(*ast.Ident)
			if !ok {
				continue
			}
			if key.Name == "Mode" {
				literal, ok := pair.Value.(*ast.BasicLit)
				mode = ok && literal.Value == `"model"`
			}
			if key.Name == "Module" {
				address, ok := pair.Value.(*ast.UnaryExpr)
				if ok && address.Op == token.AND {
					argument, ok := address.X.(*ast.Ident)
					module = ok && argument.Name == "request"
				}
			}
		}
		return mode && module
	}) {
		return fmt.Errorf("managed callback does not pass the exact model request to RunWorker")
	}
	return nil
}

func mockManifestFunction(raw []byte, name string) (*ast.FuncDecl, error) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "mock-contract.go", raw, 0)
	if err != nil {
		return nil, err
	}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == name && function.Body != nil {
			return function, nil
		}
	}
	return nil, fmt.Errorf("function %s is missing", name)
}

func functionHasCall(function *ast.FuncDecl, matches func(*ast.CallExpr) bool) bool {
	found := false
	ast.Inspect(function.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok && matches(call) {
			found = true
		}
		return true
	})
	return found
}

func TestMockWorkerManagedDelegationRejectsAdmissionBypasses(t *testing.T) {
	registration, ok := RegistrationFor("mock_python")
	if !ok || !reflect.DeepEqual(registration.PrimitiveKeys, mockWorkerPrimitives) {
		t.Fatalf("model launch branches = %+v", registration)
	}
	raw, err := os.ReadFile("../llm/mock_runtime.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyMockModelAdmission(raw); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct{ name, from, to string }{
		{"begin", "beginProviderCompletion(", "bypassCompletion("},
		{"heartbeat", "startCompletionAttemptHeartbeat(", "bypassHeartbeat("},
		{"launched", "attempt.MarkLaunched(", "attempt.MarkResponseObserved("},
		{"callback", "r.executeWorkspaceMockModel(ctx, target, request)", "r.unmanagedModel(ctx, target, request)"},
		{"mode", `Mode: "model", Module: &request`, `Mode: "call", Module: &request`},
		{"input", `Mode: "model", Module: &request`, `Mode: "model", Module: &otherRequest`},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			changed := strings.Replace(string(raw), mutation.from, mutation.to, 1)
			if changed == string(raw) {
				t.Fatal("mutation did not reach its binding")
			}
			if err := verifyMockModelAdmission([]byte(changed)); err == nil {
				t.Fatal("managed model admission bypass was accepted")
			}
		})
	}
	if delegatedMockWorkerPrimitive(mockWorkerPrimitives[0], []string{"claude_cli"}, ownerRuntimeDependency) || delegatedMockWorkerPrimitive("other:launch:1", []string{"mock_python"}, ownerRuntimeDependency) {
		t.Fatal("shared transport exception admitted another adapter or launch site")
	}
}
