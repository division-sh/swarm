package manager

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDynamicFlowRuntimeReadinessProductionConsumersStatic(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	calls := map[string]map[string]int{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if ident, ok := call.Fun.(*ast.Ident); ok {
				if calls[ident.Name] == nil {
					calls[ident.Name] = map[string]int{}
				}
				calls[ident.Name][name]++
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if calls[selector.Sel.Name] == nil {
				calls[selector.Sel.Name] = map[string]int{}
			}
			calls[selector.Sel.Name][name]++
			return true
		})
	}
	requireStaticReadinessCalls(t, calls, "reconcileDynamicFlowRuntimeReadiness", map[string]int{
		"flow_runtime_readiness.go": 1,
	})
	requireStaticReadinessCalls(t, calls, "reconcileDynamicFlowRuntimeReadinessPlan", map[string]int{
		"flow_activation.go":        1,
		"flow_runtime_readiness.go": 1,
	})
	requireStaticReadinessCalls(t, calls, "dynamicFlowRuntimeReadinessSource", map[string]int{
		"flow_activation.go":        2,
		"flow_runtime_readiness.go": 8,
		"runtime.go":                1,
	})
	requireStaticReadinessCalls(t, calls, "dynamicFlowRuntimeReadinessSourceCoordinate", map[string]int{
		"flow_activation.go":        1,
		"flow_runtime_readiness.go": 1,
	})
	requireStaticReadinessCalls(t, calls, "validateDynamicFlowRuntimeReadinessCallbackSource", map[string]int{
		"flow_runtime_readiness.go": 6,
	})
	requireStaticReadinessCalls(t, calls, "registerExecutableAgentLifecycle", map[string]int{
		"agent_manager.go": 2,
	})
	requireStaticReadinessCalls(t, calls, "ensureExecutableAgentLifecycle", map[string]int{
		"agent_manager.go":          1,
		"flow_runtime_readiness.go": 2,
		"static_topology.go":        3,
	})
	requireStaticReadinessCalls(t, calls, "registerExecutionWithTopology", map[string]int{
		"agent_manager.go":         1,
		"lifecycle_coordinator.go": 1,
	})
	requireStaticReadinessCalls(t, calls, "registerExecution", map[string]int{
		"agent_manager.go": 1,
	})
	for _, ownerCall := range []string{
		"LoadDynamicFlowRuntimeReadiness",
		"MarkDynamicFlowRuntimeTopologyReady",
		"CommitDynamicFlowRuntimeCreationOccurrence",
	} {
		for file, count := range calls[ownerCall] {
			if file != "flow_runtime_readiness.go" || count == 0 {
				t.Fatalf("%s has non-owner production consumer %s (%d calls)", ownerCall, file, count)
			}
		}
	}
	if got := calls["MarkDynamicFlowRuntimeCreationEventEmitted"]; len(got) != 0 {
		t.Fatalf("split creation completion writer remains in manager: %#v", got)
	}
	if got := calls["StageFlowInstanceRouteContext"]; len(got) != 1 || got["flow_activation.go"] != 1 {
		t.Fatalf("route staging consumers = %#v, want one manager adapter", got)
	}
	if got := calls["AddFlowInstanceRouteContext"]; len(got) != 0 {
		t.Fatalf("legacy route publication consumers remain in manager: %#v", got)
	}
}

func requireStaticReadinessCalls(t *testing.T, calls map[string]map[string]int, name string, want map[string]int) {
	t.Helper()
	got := calls[name]
	if len(got) != len(want) {
		t.Fatalf("%s consumer files = %#v, want %#v", name, got, want)
	}
	for file, count := range want {
		if got[file] != count {
			t.Fatalf("%s calls in %s = %d, want %d", name, file, got[file], count)
		}
	}
}
