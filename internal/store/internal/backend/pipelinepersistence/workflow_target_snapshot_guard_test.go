package pipelinepersistence

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
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

	if err := checkWorkflowTargetAggregateConsumers(parsed); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(currentFile), "receiver_materialization.go"))
	if err != nil {
		t.Fatal(err)
	}
	materialization, err := parser.ParseFile(token.NewFileSet(), "receiver_materialization.go", raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	foundMaterializer := false
	for _, decl := range materialization.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "receiverMaterializedTx" {
			continue
		}
		foundMaterializer = true
		if err := requireAggregateDelegation(fn); err != nil {
			t.Fatal(err)
		}
	}
	if !foundMaterializer {
		t.Fatal("transactional materializer consumer missing")
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
func requireAggregateDelegation(fn *ast.FuncDecl) error {
	shared, queries, split := 0, 0, 0
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch callee := call.Fun.(type) {
		case *ast.Ident:
			if callee.Name == "loadWorkflowTargetPersistence" {
				shared++
			}
		case *ast.SelectorExpr:
			switch callee.Sel.Name {
			case "QueryRowContext", "QueryContext":
				queries++
			case "LoadWorkflowEntityState", "LoadWorkflowInstance":
				split++
			}
		}
		return true
	})
	if shared != 1 || queries != 0 || split != 0 {
		return fmt.Errorf("%s must delegate once to aggregate owner: shared=%d queries=%d split=%d", fn.Name.Name, shared, queries, split)
	}
	return nil
}

func checkWorkflowTargetAggregateConsumers(file *ast.File) error {
	checked := map[string]bool{}
	var owner *ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Name.Name == "loadWorkflowTargetPersistence" && fn.Recv == nil {
			owner = fn
		}
		if fn.Name.Name != "LoadWorkflowTargetPersistence" || fn.Recv == nil || len(fn.Recv.List) != 1 {
			continue
		}
		ptr, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		typ, ok := ptr.X.(*ast.Ident)
		if !ok {
			continue
		}
		if typ.Name != "PipelinePostgresOwner" && typ.Name != "PipelineSQLiteOwner" {
			continue
		}
		if err := requireAggregateDelegation(fn); err != nil {
			return err
		}
		checked[typ.Name] = true
	}
	for _, name := range []string{"PipelinePostgresOwner", "PipelineSQLiteOwner"} {
		if !checked[name] {
			return fmt.Errorf("aggregate guard did not inspect %s", name)
		}
	}
	if owner == nil {
		return fmt.Errorf("aggregate owner missing")
	}
	// Each dialect's query must be enclosed in its terminating return. There is
	// exactly one dialect branch, followed by the other dialect's return.
	queries, returningQueries := 0, 0
	ast.Inspect(owner.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && (sel.Sel.Name == "QueryRowContext" || sel.Sel.Name == "QueryContext") {
				queries++
			}
		}
		if ret, ok := n.(*ast.ReturnStmt); ok {
			ast.Inspect(ret, func(child ast.Node) bool {
				call, ok := child.(*ast.CallExpr)
				if !ok {
					return true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "QueryRowContext" {
					returningQueries++
				}
				return true
			})
		}
		return true
	})
	if queries != 2 || returningQueries != 2 {
		return fmt.Errorf("aggregate owner must return one query per dialect, queries=%d returning=%d", queries, returningQueries)
	}
	if len(owner.Body.List) < 2 {
		return fmt.Errorf("aggregate dialect branch missing")
	}
	branch, ok := owner.Body.List[len(owner.Body.List)-2].(*ast.IfStmt)
	if !ok || branch.Else != nil || len(branch.Body.List) != 1 {
		return fmt.Errorf("aggregate dialect must terminate in one query return")
	}
	cond, ok := branch.Cond.(*ast.Ident)
	if !ok || cond.Name != "sqlite" {
		return fmt.Errorf("aggregate dialect branch must use explicit sqlite discriminator")
	}
	for _, item := range []struct {
		statement ast.Stmt
		query     string
	}{
		{branch.Body.List[0], "sqliteWorkflowTargetPersistenceSelect"},
		{owner.Body.List[len(owner.Body.List)-1], "postgresWorkflowTargetPersistenceSelect"},
	} {
		ret, ok := item.statement.(*ast.ReturnStmt)
		if !ok || len(ret.Results) != 1 {
			return fmt.Errorf("dialect query must terminate its execution path")
		}
		count := 0
		ast.Inspect(ret, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "QueryRowContext" {
				return true
			}
			if len(call.Args) < 2 {
				return true
			}
			query, ok := call.Args[1].(*ast.Ident)
			if ok && query.Name == item.query {
				count++
			}
			return true
		})
		if count != 1 {
			return fmt.Errorf("dialect return must execute exact aggregate %s", item.query)
		}
	}
	return nil
}

func TestWorkflowTargetAggregateGuardRejectsSplitReaders(t *testing.T) {
	_, current, _, _ := runtime.Caller(0)
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(current), "workflow_instance_read.go"))
	if err != nil {
		t.Fatal(err)
	}
	base := string(raw)
	for _, tc := range []struct{ name, from, to string }{
		{"arbitrary receiver", "return loadWorkflowTargetPersistence(ctx, s.backend, identity, entityID, false)", "s.LoadWorkflowInstance(ctx, identity); return loadWorkflowTargetPersistence(ctx, s.backend, identity, entityID, false)"},
		{"extra query in approved facade", "return loadWorkflowTargetPersistence(ctx, s.backend, identity, entityID, true)", "s.backend.QueryRowContext(ctx, \"SELECT 1\"); return loadWorkflowTargetPersistence(ctx, s.backend, identity, entityID, true)"},
		{"extra query in shared owner", "if sqlite {\n\t\treturn scanSQLiteWorkflowTargetPersistence", "q.QueryRowContext(ctx, \"SELECT 1\"); if sqlite {\n\t\treturn scanSQLiteWorkflowTargetPersistence"},
		{"facade bypass", "return loadWorkflowTargetPersistence(ctx, s.backend, identity, entityID, false)", "return otherOwner(ctx, s.backend, identity, entityID, false)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(base, tc.from) {
				t.Fatal("hostile injection missed")
			}
			changed := strings.Replace(base, tc.from, tc.to, 1)
			changed = strings.ReplaceAll(changed, "(s *Pipeline", "(arbitraryReceiver *Pipeline")
			changed = strings.ReplaceAll(changed, "s.", "arbitraryReceiver.")
			parsed, err := parser.ParseFile(token.NewFileSet(), "workflow_instance_read.go", changed, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := checkWorkflowTargetAggregateConsumers(parsed); err == nil {
				t.Fatal("hostile reader passed aggregate guard")
			}
		})
	}
}
