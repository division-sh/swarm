package pipelinepersistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestWorkflowTargetPersistenceReadersUseOneAggregateStatement(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve workflow target snapshot guard path")
	}
	sourcePath := filepath.Join(filepath.Dir(currentFile), "workflow_instance_read.go")
	parsed, err := parser.ParseFile(token.NewFileSet(), sourcePath, nil, 0)
	if err != nil {
		t.Fatalf("parse workflow target persistence reader: %v", err)
	}

	checked := map[string]bool{}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "LoadWorkflowTargetPersistence" || function.Recv == nil || len(function.Recv.List) != 1 {
			continue
		}
		receiver, ok := function.Recv.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		receiverType, ok := receiver.X.(*ast.Ident)
		if !ok || (receiverType.Name != "PipelinePostgresOwner" && receiverType.Name != "PipelineSQLiteOwner") {
			continue
		}
		queryCalls := 0
		forbiddenSplitCalls := make([]string, 0, 2)
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch selector.Sel.Name {
			case "QueryRowContext":
				queryCalls++
			case "LoadWorkflowEntityState", "LoadWorkflowInstance":
				forbiddenSplitCalls = append(forbiddenSplitCalls, selector.Sel.Name)
			}
			return true
		})
		if queryCalls != 1 || len(forbiddenSplitCalls) != 0 {
			t.Fatalf("%s.LoadWorkflowTargetPersistence query calls = %d split calls = %v, want one aggregate statement", receiverType.Name, queryCalls, forbiddenSplitCalls)
		}
		checked[receiverType.Name] = true
	}
	for _, receiver := range []string{"PipelinePostgresOwner", "PipelineSQLiteOwner"} {
		if !checked[receiver] {
			t.Fatalf("workflow target snapshot guard did not inspect %s", receiver)
		}
	}

	constants := map[string]string{}
	for _, declaration := range parsed.Decls {
		generic, ok := declaration.(*ast.GenDecl)
		if !ok || generic.Tok != token.CONST {
			continue
		}
		for _, specification := range generic.Specs {
			value, ok := specification.(*ast.ValueSpec)
			if !ok || len(value.Names) != 1 || len(value.Values) != 1 {
				continue
			}
			literal, ok := value.Values[0].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				continue
			}
			decoded, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Fatalf("decode %s query: %v", value.Names[0].Name, err)
			}
			constants[value.Names[0].Name] = decoded
		}
	}
	for _, name := range []string{"postgresWorkflowTargetPersistenceSelect", "sqliteWorkflowTargetPersistenceSelect"} {
		query := constants[name]
		for _, required := range []string{"LEFT JOIN entity_state", "LEFT JOIN flow_instances"} {
			if !strings.Contains(query, required) {
				t.Fatalf("%s must read both aggregate halves, missing %q", name, required)
			}
		}
	}
}
