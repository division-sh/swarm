package runstart

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
)

var auditedRootInputConsumers = []string{
	"internal/apiv1/operator_event_publish.go:validateEventPublication",
	"internal/builder/handler_rpc.go:dispatchRPC",
}

var rootInputProjectionRequirements = map[string]string{
	"internal/apiv1/operator_event_publish.go:validateEventPublication":  "rootInputApplicationError",
	"internal/apiv1/operator_event_publish.go:rootInputApplicationError": "AsRootInputValidationError",
	"internal/builder/handler_rpc.go:dispatchRPC":                        "AsRootInputValidationError",
}

func TestValidateInputEventsConsumersAreExhaustivelyRegistered(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current test file")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", "..", ".."))
	consumerCalls, err := scanRootInputConsumers(repoRoot)
	if err != nil {
		t.Fatalf("scan ValidateInputEvents consumers: %v", err)
	}
	if err := checkRootInputConsumerAudit(consumerCalls); err != nil {
		t.Fatal(err)
	}
}

func TestValidateInputEventsConsumerGuardRejectsCommandCaller(t *testing.T) {
	repoRoot := t.TempDir()
	writeGuardFixture := func(relative, source string) {
		t.Helper()
		path := filepath.Join(repoRoot, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("create fixture directory: %v", err)
		}
		if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
	}
	writeGuardFixture("internal/apiv1/operator_event_publish.go", `package apiv1
func validateEventPublication() {
	runstart.ValidateInputEvents()
	rootInputApplicationError()
}
func rootInputApplicationError() { runstart.AsRootInputValidationError() }
`)
	writeGuardFixture("internal/builder/handler_rpc.go", `package builder
func dispatchRPC() {
	runstart.ValidateInputEvents()
	runstart.AsRootInputValidationError()
}
`)
	writeGuardFixture("internal/cliapp/synthetic.go", `package cliapp
func syntheticCommandConsumer() { runstart.ValidateInputEvents() }
`)

	consumerCalls, err := scanRootInputConsumers(repoRoot)
	if err != nil {
		t.Fatalf("scan fixture consumers: %v", err)
	}
	err = checkRootInputConsumerAudit(consumerCalls)
	if err == nil || !strings.Contains(err.Error(), "internal/cliapp/synthetic.go:syntheticCommandConsumer") {
		t.Fatalf("guard error = %v, want unregistered command consumer", err)
	}
}

func scanRootInputConsumers(repoRoot string) (map[string]map[string]bool, error) {
	consumerCalls := make(map[string]map[string]bool)
	excludedDirectories := map[string]bool{
		".git":     true,
		"testdata": true,
		"vendor":   true,
	}
	err := filepath.WalkDir(repoRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != repoRoot && excludedDirectories[entry.Name()] {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
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
		return nil, err
	}
	return consumerCalls, nil
}

func checkRootInputConsumerAudit(consumerCalls map[string]map[string]bool) error {
	consumers := make([]string, 0, len(consumerCalls))
	for consumer := range consumerCalls {
		if consumer != "internal/apiv1/operator_event_publish.go:rootInputApplicationError" {
			consumers = append(consumers, consumer)
		}
	}
	sort.Strings(consumers)
	if !reflect.DeepEqual(consumers, auditedRootInputConsumers) {
		return fmt.Errorf("ValidateInputEvents consumers = %#v, want audited registry %#v; classify and preserve typed root-input facts before adding a consumer", consumers, auditedRootInputConsumers)
	}
	for consumer, requiredCall := range rootInputProjectionRequirements {
		if !consumerCalls[consumer][requiredCall] {
			return fmt.Errorf("%s must call %s so typed root-input facts cannot be flattened", consumer, requiredCall)
		}
	}
	return nil
}
