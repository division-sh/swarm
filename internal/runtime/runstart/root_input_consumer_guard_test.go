package runstart

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func TestValidateInputEventsConsumersAreExhaustivelyRegistered(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current test file")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", "..", ".."))
	consumerCalls := make(map[string]map[string]bool)
	err := filepath.WalkDir(filepath.Join(repoRoot, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			relative, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			functionName := filepath.ToSlash(relative) + ":" + function.Name.Name
			calls := make(map[string]bool)
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch called := call.Fun.(type) {
				case *ast.SelectorExpr:
					calls[called.Sel.Name] = true
				case *ast.Ident:
					calls[called.Name] = true
				}
				return true
			})
			if calls["ValidateInputEvents"] || functionName == "internal/apiv1/operator_event_publish.go:rootInputApplicationError" {
				consumerCalls[functionName] = calls
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan ValidateInputEvents consumers: %v", err)
	}
	consumers := make([]string, 0, len(consumerCalls))
	for consumer := range consumerCalls {
		if consumer != "internal/apiv1/operator_event_publish.go:rootInputApplicationError" {
			consumers = append(consumers, consumer)
		}
	}
	sort.Strings(consumers)
	want := []string{
		"internal/apiv1/operator_event_publish.go:validateEventPublication",
		"internal/builder/handler_rpc.go:dispatchRPC",
	}
	if !reflect.DeepEqual(consumers, want) {
		t.Fatalf("ValidateInputEvents consumers = %#v, want audited registry %#v; classify and preserve typed root-input facts before adding a consumer", consumers, want)
	}
	projectionRequirements := map[string]string{
		"internal/apiv1/operator_event_publish.go:validateEventPublication":  "rootInputApplicationError",
		"internal/apiv1/operator_event_publish.go:rootInputApplicationError": "AsRootInputValidationError",
		"internal/builder/handler_rpc.go:dispatchRPC":                        "AsRootInputValidationError",
	}
	for consumer, requiredCall := range projectionRequirements {
		if !consumerCalls[consumer][requiredCall] {
			t.Errorf("%s must call %s so typed root-input facts cannot be flattened", consumer, requiredCall)
		}
	}
}
