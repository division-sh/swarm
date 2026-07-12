package swarmflowtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
)

func TestCatalogCanonicalAuthoringGuardUsesCanonicalYAMLOwner(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "catalog_runner_validation_test.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var target *ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name.Name == "TestCatalogFixtures_UseCanonicalCreateFlowInstanceAuthoring" {
			target = fn
			break
		}
	}
	if target == nil {
		t.Fatal("catalog canonical-authoring guard function is missing")
	}

	imports := map[string]string{}
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Fatal(err)
		}
		name := ""
		if spec.Name != nil {
			name = spec.Name.Name
		}
		if name == "" {
			switch path {
			case "os":
				name = "os"
			case "gopkg.in/yaml.v3":
				name = "yaml"
			case "github.com/division-sh/swarm/internal/yamlsource":
				name = "yamlsource"
			}
		}
		if name != "" {
			imports[name] = path
		}
	}

	foundOwner := false
	ast.Inspect(target.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok {
			return true
		}
		if imports[pkg.Name] == "github.com/division-sh/swarm/internal/yamlsource" && selector.Sel.Name == "LoadFile" {
			foundOwner = true
		}
		return true
	})
	if !foundOwner {
		t.Fatal("catalog canonical-authoring guard does not consume yamlsource.LoadFile")
	}

	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok {
			return true
		}
		path := imports[pkg.Name]
		if (path == "os" && selector.Sel.Name == "ReadFile") ||
			(path == "gopkg.in/yaml.v3" && (selector.Sel.Name == "Unmarshal" || selector.Sel.Name == "NewDecoder")) {
			t.Errorf("catalog canonical-authoring file restored forbidden parser call %s.%s", pkg.Name, selector.Sel.Name)
		}
		return true
	})
}
