package conformance

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestLifecycleDiagnosticAcknowledgmentAndConsumerOwnership(t *testing.T) {
	snapshot := mustConformanceRepoSnapshot(t, conformanceRepoRoot(t))
	writes := regexp.MustCompile(`(?i)\bUPDATE\s+agent_lifecycle_diagnostic_outbox\b`)
	consumers := make(map[string]int)
	for _, file := range snapshot.fileList() {
		if !strings.HasSuffix(file.Path, ".go") || strings.HasSuffix(file.Path, "_test.go") {
			continue
		}
		if bytes.Contains(file.Raw, []byte("MarkAgentLifecycleDiagnosticProjected")) {
			t.Errorf("standalone lifecycle acknowledgment restored in %s", file.Path)
		}
		if writes.Match(file.Raw) && file.Path != "internal/store/internal/backend/eventpersistence/lifecycle_diagnostic.go" {
			t.Errorf("lifecycle acknowledgment bypasses exact log transaction: %s", file.Path)
		}
		if !bytes.Contains(file.Raw, []byte("projectLifecycleDiagnostics")) {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file.Path, file.Raw, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if selector.Sel.Name == "projectLifecycleDiagnostics" {
					consumers[file.Path+":"+function.Name.Name]++
				}
				if function.Name.Name == "projectLifecycleDiagnostics" && selector.Sel.Name == "LogRuntime" {
					t.Error("lifecycle projector returned to generic non-atomic logging")
				}
				return true
			})
		}
	}
	want := map[string]int{
		"internal/runtime/manager/runtime.go:HydrateForStartup":                       1,
		"internal/runtime/manager/runtime.go:launchExecutionLoop":                     2,
		"internal/runtime/manager/agent_manager.go:registerExecutableAgentLifecycle":  1,
		"internal/runtime/manager/agent_manager.go:teardownIdentityWithTopology":      1,
		"internal/runtime/manager/terminal_retirement.go:completeTerminalRetirements": 1,
	}
	if !reflect.DeepEqual(consumers, want) {
		t.Fatalf("diagnostic consumer census changed; update execution proof, not only this list:\ngot=%v\nwant=%v", consumers, want)
	}
}
