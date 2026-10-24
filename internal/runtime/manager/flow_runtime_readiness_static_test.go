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
