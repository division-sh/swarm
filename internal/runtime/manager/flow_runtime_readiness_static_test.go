package manager

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDynamicFlowLegacyExecutionAuthorityAPIsAbsent(t *testing.T) {
	retiredConstructors := map[string]bool{
		"MaterializeInitialEntry": true, "CommitWorkflowInitialMaterialization": true,
		"commitWorkflowInitialMaterialization": true, "WorkflowInitialMaterializationResult": true,
		"WorkflowInitialMaterializationUnknown": true, "WorkflowInitialMaterializationCreated": true,
		"WorkflowInitialMaterializationAlreadyExists": true, "WorkflowInitialMaterializationCommand": true,
		"WorkflowInitialMaterializationRecord": true, "CommittedWorkflowInitialMaterialization": true,
		"WorkflowInitialMaterializationCommitOwner": true, "workflowInitialMaterializationRecord": true,
		"newWorkflowInitialMaterializationProjection": true, "initialCommits": true,
	}
	forbidden := map[string]bool{
		"dynamicFlowRuntimeReadinessStillEligible": true,
		"MarkDynamicFlowRuntimeTopologyReady":      true,
		"markDynamicFlowRuntimeTopologyReady":      true,
		"PublishPersistedFlowInstanceRoute":        true,
		"RetirePublishedFlowInstanceRoute":         true,
		"AddFlowInstanceRouteContext":              true,
		"RemoveFlowInstanceRouteContext":           true,
	}
	for name := range retiredConstructors {
		forbidden[name] = true
	}
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	for _, dir := range []string{"internal/runtime", "internal/store", "internal/testutil"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			fileForbidden := forbidden
			isTest := strings.HasSuffix(path, "_test.go")
			if isTest {
				fileForbidden = retiredConstructors
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			ast.Inspect(parsed, func(node ast.Node) bool {
				switch declaration := node.(type) {
				case *ast.FuncDecl:
					name := declaration.Name.Name
					if fileForbidden[name] || (!isTest && eventBusMethod(declaration) && (name == "AddFlowInstanceRoute" || name == "RemoveFlowInstanceRoute")) {
						t.Errorf("retired flow execution authority %s reintroduced in %s", name, path)
					}
				case *ast.Field:
					for _, name := range declaration.Names {
						if fileForbidden[name.Name] {
							t.Errorf("retired flow execution authority %s reintroduced in %s", name.Name, path)
						}
					}
				case *ast.TypeSpec:
					if fileForbidden[declaration.Name.Name] {
						t.Errorf("retired flow execution authority %s reintroduced in %s", declaration.Name.Name, path)
					}
				case *ast.ValueSpec:
					for _, name := range declaration.Names {
						if fileForbidden[name.Name] {
							t.Errorf("retired flow execution authority %s reintroduced in %s", name.Name, path)
						}
					}
				case *ast.SelectorExpr:
					if fileForbidden[declaration.Sel.Name] {
						t.Errorf("retired flow execution authority %s consumed in %s", declaration.Sel.Name, path)
					}
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func eventBusMethod(declaration *ast.FuncDecl) bool {
	if declaration.Recv == nil || len(declaration.Recv.List) != 1 {
		return false
	}
	receiver := declaration.Recv.List[0].Type
	if pointer, ok := receiver.(*ast.StarExpr); ok {
		receiver = pointer.X
	}
	name, ok := receiver.(*ast.Ident)
	return ok && name.Name == "EventBus"
}

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
		"flow_readiness_startup.go": 1,
	})
	requireStaticReadinessCalls(t, calls, "reconcileDynamicFlowRuntimeReadinessPlan", map[string]int{
		"flow_readiness_plan.go": 1,
	})
	requireStaticReadinessCalls(t, calls, "dynamicFlowRuntimeReadinessSource", map[string]int{
		"flow_activation.go":          3, // Includes process-only standing preparation before executable publication.
		"flow_readiness_admission.go": 4,
		"flow_readiness_plan.go":      2,
		"flow_readiness_startup.go":   4,
		"runtime.go":                  1,
	})
	requireStaticReadinessCalls(t, calls, "dynamicFlowRuntimeReadinessSourceCoordinate", map[string]int{
		"flow_activation.go": 1,
	})
	requireStaticReadinessCalls(t, calls, "validateDynamicFlowRuntimeReadinessCallbackSource", map[string]int{
		"flow_readiness_admission.go": 3,
		"flow_readiness_startup.go":   3,
		"flow_runtime_readiness.go":   1,
	})
	requireStaticReadinessCalls(t, calls, "registerExecutableAgentLifecycle", map[string]int{
		"agent_manager.go": 2,
	})
	requireStaticReadinessCalls(t, calls, "ensureExecutableAgentLifecycle", map[string]int{
		"agent_manager.go":             1,
		"flow_attachment_resources.go": 2,
		"static_topology.go":           1,
	})
	requireStaticReadinessCalls(t, calls, "registerExecutionWithTopology", map[string]int{
		"agent_manager.go":         1,
		"lifecycle_coordinator.go": 1,
	})
	requireStaticReadinessCalls(t, calls, "registerExecution", map[string]int{
		"agent_manager.go": 1,
	})
	requireStaticReadinessCalls(t, calls, "LoadDynamicFlowRuntimeReadiness", map[string]int{
		"flow_attachment_execution.go": 1,
		"flow_readiness_admission.go":  2,
		"flow_readiness_plan.go":       1,
		"flow_readiness_startup.go":    1,
		"flow_runtime_readiness.go":    1,
	})
	for _, ownerCall := range []string{
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
	for _, retired := range []string{"StageFlowInstanceRouteContext", "PublishPersistedFlowInstanceRouteForAttempt", "RetireFlowInstanceRouteForAttempt", "VerifyFlowInstanceRoute"} {
		if got := calls[retired]; len(got) != 0 {
			t.Fatalf("retired attachment route consumer %s remains: %#v", retired, got)
		}
	}
	requireStaticReadinessCalls(t, calls, "VerifyDynamicFlowRuntimeActivationAttempt", map[string]int{
		"dynamic_flow_activation_owner.go": 2,
		"flow_readiness_startup.go":        1,
		"flow_runtime_readiness.go":        1,
	})
	if got := calls["AddFlowInstanceRouteContext"]; len(got) != 0 {
		t.Fatalf("legacy route publication consumers remain in manager: %#v", got)
	}
	body, err := os.ReadFile("flow_runtime_readiness.go")
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(body), "\n"); lines >= 500 {
		t.Fatalf("phase reconciler is %d lines; it must stay below 500", lines)
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
