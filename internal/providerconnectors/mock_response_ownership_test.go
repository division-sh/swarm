package providerconnectors

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestMockResponsePlanHasOneProductionProducer(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve mock response ownership test path")
	}
	repo := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	allowedProducer := "internal/providerconnectors/mock_response_compiler.go"
	forbiddenFields := map[string]string{
		"RuntimeOptions":                      "MockConnectorResponses",
		"WorkflowContractValidationOptions":   "MockConnectorResponses",
		"SelectedContractAgentRuntimeOptions": "MockConnectorResponses",
		"selectedAPICapabilityRequest":        "MockConnectorResponses",
		"ServeOptions":                        "TestMockConnectorResponses",
	}

	for _, root := range []string{filepath.Join(repo, "cmd"), filepath.Join(repo, "internal")} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(repo, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			ast.Inspect(parsed, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.CallExpr:
					if mockResponseConstructorName(typed.Fun) == "NewMockResponsePlan" && rel != allowedProducer {
						t.Errorf("production mock response producer survives outside canonical compiler: %s", rel)
					}
				case *ast.TypeSpec:
					forbidden, watched := forbiddenFields[typed.Name.Name]
					if !watched {
						break
					}
					structure, ok := typed.Type.(*ast.StructType)
					if !ok {
						break
					}
					for _, field := range structure.Fields.List {
						for _, name := range field.Names {
							if name.Name == forbidden {
								t.Errorf("caller-selectable production response plan field %s.%s survives in %s", typed.Name.Name, forbidden, rel)
							}
						}
					}
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("scan production source under %s: %v", root, err)
		}
	}
}

func mockResponseConstructorName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		return typed.Sel.Name
	default:
		return ""
	}
}
