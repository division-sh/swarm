package conformance

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestNodeAdmissionUsesSourceValueWithoutRawDecoderFallback(t *testing.T) {
	root := conformanceRepoRoot(t)
	dir := filepath.Join(root, "internal/runtime/contracts")
	files, err := filepath.Glob(filepath.Join(dir, "workflow_contract_nodes_*.go"))
	if err != nil || len(files) == 0 {
		t.Fatalf("node projector inventory: %v", err)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, imported := range file.Imports {
			name, err := strconv.Unquote(imported.Path.Value)
			if err != nil || name == "gopkg.in/yaml.v3" || name == "reflect" {
				t.Errorf("node admission %s imports a raw/reflection decoder: %s", path, imported.Path.Value)
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch selector.Sel.Name {
			case "Decode", "DecodeYAML", "UnmarshalYAML", "ValueFromNode", "Content":
				t.Errorf("node admission %s bypasses source Value: %s", path, selector.Sel.Name)
			}
			return true
		})
	}
	for _, path := range []string{"optional_declaration_admission.go", "workflow_contract_loading.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, path), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || (function.Name.Name != "loadOptionalNodeDeclarationsFromSource" && function.Name.Name != "loadOptionalNodeDeclarations") {
				continue
			}
			projector := false
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "projectNodeDeclarationsValue" {
					projector = true
				}
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "DecodeYAML" {
					t.Errorf("node root %s restored raw DecodeYAML", function.Name.Name)
				}
				return true
			})
			if !projector {
				t.Errorf("node root %s does not consume the source-value projector", function.Name.Name)
			}
		}
	}
	actual, err := collectCustomYAMLDecoders(root)
	if err != nil {
		t.Fatal(err)
	}
	for identity := range actual {
		parts := strings.Split(identity, ":")
		switch parts[len(parts)-1] {
		case "SystemNodeContract", "SystemNodeEventHandler", "HandlerRuleEntry", "NodeStateSchema", "NodeGateStateSchema", "WorkflowTimerContract", "HandlerOnSuccessSpec", "ActivitySpec", "GuardSpec", "GateSpec", "AccumulateSpec", "FanOutSpec", "GroupBySpec", "FilterSpec", "ReduceSpec", "CountSpec", "QuerySpec", "WorkflowDataWrite", "WorkflowDataAccumulation", "ComputeSpec", "JoinSpec", "JoinMembersSpec", "JoinWindowSpec", "JoinDeadlineSpec", "EventEmission":
			t.Errorf("retired node decoder restored: %s", identity)
		}
	}
}
