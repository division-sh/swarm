package mutationprotocol

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

var mutationOwnerFamilies = []string{
	"activityjournal", "agentpersistence", "decisionpersistence", "delivery",
	"effectpersistence", "entityruntime", "eventpersistence", "pipelinepersistence",
	"runlifecycle", "llmpersistence", "genericschedule", "replycontext",
	"runforkpersistence",
}

func visitMutationOwners(t *testing.T, visit func(string, *token.FileSet, *ast.File)) {
	t.Helper()
	paths := make([]string, 0, len(mutationOwnerFamilies)+2)
	for _, family := range mutationOwnerFamilies {
		paths = append(paths, filepath.Join("..", family))
	}
	paths = append(paths, filepath.Join("..", "..", "startupownership"), filepath.Join("..", "..", "runtimepersistence"))
	for _, path := range paths {
		entries, err := os.ReadDir(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			filePath := filepath.Join(path, entry.Name())
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, filePath, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			visit(filePath, fset, file)
		}
	}
}

func TestSelectedStoreMutationProtocolHasNoOldAssemblers(t *testing.T) {
	visitMutationOwners(t, func(_ string, fset *token.FileSet, file *ast.File) {
		bannedImports := map[string]map[string]bool{}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			var methods map[string]bool
			switch importPath {
			case "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity":
				methods = map[string]bool{"Begin": true, "FenceMutationOrder": true}
			case "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision":
				methods = map[string]bool{"FinalizePostgres": true, "FinalizeSQLite": true}
			case "github.com/division-sh/swarm/internal/store/internal/runhandoff":
				methods = map[string]bool{"ReserveCandidateHandoff": true, "WithCandidateHandoffOutcome": true, "WithCandidateHandoffOutcomeResult": true}
			}
			if methods == nil {
				continue
			}
			alias := filepath.Base(importPath)
			if spec.Name != nil {
				alias = spec.Name.Name
			}
			bannedImports[alias] = methods
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
			if selector.Sel.Name == "AttemptReset" || selector.Sel.Name == "ResetAttempt" || selector.Sel.Name == "Finalize" {
				t.Errorf("%s: old mutation finalization/reset survives", fset.Position(call.Pos()))
			}
			name, ok := selector.X.(*ast.Ident)
			if ok && bannedImports[name.Name][selector.Sel.Name] {
				t.Errorf("%s: direct %s.%s bypasses mutation protocol", fset.Position(call.Pos()), name.Name, selector.Sel.Name)
			}
			return true
		})
	})
}

type loanViolation struct {
	pos token.Pos
	msg string
}

func loanIdent(node ast.Node, tx *ast.Ident) bool {
	for {
		wrapped, ok := node.(*ast.ParenExpr)
		if !ok {
			break
		}
		node = wrapped.X
	}
	ident, ok := node.(*ast.Ident)
	return ok && ident.Obj != nil && ident.Obj == tx.Obj
}

func loanReferenced(node ast.Node, owned map[*ast.Object]bool) bool {
	found := false
	ast.Inspect(node, func(child ast.Node) bool {
		if ident, ok := child.(*ast.Ident); ok && owned[ident.Obj] {
			found = true
			return false
		}
		return !found
	})
	return found
}

// Calls are opaque to this AST check; direct storage is checked outside their arguments.
func loanStored(expr ast.Expr, owned map[*ast.Object]bool) bool {
	switch value := expr.(type) {
	case *ast.Ident:
		return owned[value.Obj]
	case *ast.ParenExpr:
		return loanStored(value.X, owned)
	case *ast.UnaryExpr:
		return loanStored(value.X, owned)
	case *ast.StarExpr:
		return loanStored(value.X, owned)
	case *ast.CompositeLit:
		for _, element := range value.Elts {
			if loanStored(element, owned) {
				return true
			}
		}
	case *ast.KeyValueExpr:
		return loanStored(value.Key, owned) || loanStored(value.Value, owned)
	case *ast.CallExpr:
		// These are type conversions, not named helpers.
		switch value.Fun.(type) {
		case *ast.InterfaceType, *ast.StarExpr, *ast.ArrayType, *ast.MapType, *ast.ChanType:
			for _, arg := range value.Args {
				if loanStored(arg, owned) {
					return true
				}
			}
		}
		if name, ok := value.Fun.(*ast.Ident); ok && name.Name == "any" {
			for _, arg := range value.Args {
				if loanStored(arg, owned) {
					return true
				}
			}
		}
	}
	return false
}

func localLoanWrapper(assign *ast.AssignStmt, tx *ast.Ident) *ast.Ident {
	if assign.Tok != token.DEFINE || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
		return nil
	}
	name, ok := assign.Lhs[0].(*ast.Ident)
	if !ok {
		return nil
	}
	literal, ok := assign.Rhs[0].(*ast.CompositeLit)
	if !ok || !loanStored(literal, map[*ast.Object]bool{tx.Obj: true}) {
		return nil
	}
	typeName, ok := literal.Type.(*ast.Ident)
	if !ok || (typeName.Name != "postgresRunLifecycleMutation" && typeName.Name != "sqliteRunLifecycleMutation") {
		return nil
	}
	return name
}

func localSourceAdapter(assign *ast.AssignStmt, tx *ast.Ident, body *ast.BlockStmt, parents map[ast.Node]ast.Node) *ast.Ident {
	if assign.Tok != token.DEFINE || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
		return nil
	}
	name, ok := assign.Lhs[0].(*ast.Ident)
	if !ok || name.Obj == nil {
		return nil
	}
	call, ok := assign.Rhs[0].(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return nil
	}
	adapter, ok := call.Fun.(*ast.Ident)
	if !ok || adapter.Name != "activeRunSourceOwnerFunc" {
		return nil
	}
	callback, ok := call.Args[0].(*ast.FuncLit)
	if !ok || !loanReferenced(callback, map[*ast.Object]bool{tx.Obj: true}) {
		return nil
	}
	used, safe := false, true
	ast.Inspect(body, func(node ast.Node) bool {
		ident, ok := node.(*ast.Ident)
		if !ok || ident == name || ident.Obj != name.Obj {
			return true
		}
		used = true
		conversion, ok := parents[ident].(*ast.CallExpr)
		if !ok {
			safe = false
			return false
		}
		converter, ok := conversion.Fun.(*ast.Ident)
		if ok && converter.Name == "materializeRunForkEntityState" {
			return true
		}
		if !ok || converter.Name != "runForkSourceOwnerFunc" || len(conversion.Args) != 1 || conversion.Args[0] != ident {
			safe = false
			return false
		}
		consumer, ok := parents[conversion].(*ast.CallExpr)
		if !ok {
			safe = false
			return false
		}
		function, ok := consumer.Fun.(*ast.Ident)
		if !ok || (function.Name != "resolveRunForkBundleInsertIdentity" && function.Name != "requireOriginalFanOutCarriage") {
			safe = false
			return false
		}
		return true
	})
	if !used || !safe {
		return nil
	}
	return name
}

func synchronousLoanCallback(fn *ast.FuncLit, tx *ast.Ident, body *ast.BlockStmt, parents map[ast.Node]ast.Node) bool {
	parent := parents[fn]
	for {
		wrapped, ok := parent.(*ast.ParenExpr)
		if !ok {
			break
		}
		parent = parents[wrapped]
	}
	call, called := parent.(*ast.CallExpr)
	if !called {
		return false
	}
	if call.Fun == fn {
		return true // Immediate invocation stays inside the loan.
	}
	name, ok := call.Fun.(*ast.Ident)
	if !ok {
		return false
	}
	switch name.Name {
	case "withEventStoreRetry":
		return true
	case "activeRunSourceOwnerFunc", "runForkSourceOwnerFunc":
		// Direct consumption or a tracked callback-local source adapter is allowed.
		_, immediatelyConsumed := parents[call].(*ast.CallExpr)
		if immediatelyConsumed {
			return true
		}
		if name.Name == "activeRunSourceOwnerFunc" {
			if assign, ok := parents[call].(*ast.AssignStmt); ok {
				return localSourceAdapter(assign, tx, body, parents) != nil
			}
		}
		return false
	default:
		return false
	}
}

func scopedSQLLoanViolations(file *ast.File) []loanViolation {
	var violations []loanViolation
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "WithSQL" {
			return true
		}
		if len(call.Args) != 2 {
			violations = append(violations, loanViolation{call.Pos(), "WithSQL must use an inline callback with two arguments"})
			return true
		}
		loan, ok := call.Args[1].(*ast.FuncLit)
		if !ok || len(loan.Type.Params.List) != 2 || len(loan.Type.Params.List[1].Names) != 1 {
			violations = append(violations, loanViolation{call.Pos(), "WithSQL must use an inline callback with a named SQL transaction"})
			return true
		}
		tx := loan.Type.Params.List[1].Names[0]
		if tx.Obj == nil {
			violations = append(violations, loanViolation{tx.Pos(), "WithSQL transaction must have a resolved binding"})
			return true
		}
		owned := map[*ast.Object]bool{tx.Obj: true}
		parents := make(map[ast.Node]ast.Node)
		var stack []ast.Node
		ast.Inspect(loan.Body, func(inner ast.Node) bool {
			if inner == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			if len(stack) != 0 {
				parents[inner] = stack[len(stack)-1]
			}
			stack = append(stack, inner)
			return true
		})
		ast.Inspect(loan.Body, func(inner ast.Node) bool {
			if inner == nil {
				return true
			}
			if nested, ok := inner.(*ast.FuncLit); ok {
				if loanReferenced(nested, owned) && !synchronousLoanCallback(nested, tx, loan.Body, parents) {
					violations = append(violations, loanViolation{nested.Pos(), "scoped SQL transaction captured by a closure"})
					return false
				}
				return true
			}
			switch item := inner.(type) {
			case *ast.SelectorExpr:
				if (item.Sel.Name == "Commit" || item.Sel.Name == "Rollback") && loanIdent(item.X, tx) {
					violations = append(violations, loanViolation{item.Pos(), "scoped SQL transaction cannot Commit or Rollback"})
				}
			case *ast.AssignStmt:
				if wrapper := localLoanWrapper(item, tx); wrapper != nil {
					owned[wrapper.Obj] = true
					break
				}
				if adapter := localSourceAdapter(item, tx, loan.Body, parents); adapter != nil {
					owned[adapter.Obj] = true
					break
				}
				for _, value := range item.Rhs {
					if loanStored(value, owned) {
						violations = append(violations, loanViolation{item.Pos(), "scoped SQL transaction cannot be assigned"})
						break
					}
				}
			case *ast.ValueSpec:
				for _, value := range item.Values {
					if loanStored(value, owned) {
						violations = append(violations, loanViolation{item.Pos(), "scoped SQL transaction cannot be stored in a declaration"})
						break
					}
				}
			case *ast.ReturnStmt:
				for _, value := range item.Results {
					if loanStored(value, owned) {
						violations = append(violations, loanViolation{item.Pos(), "scoped SQL transaction cannot be returned"})
						break
					}
				}
			case *ast.SendStmt:
				if loanStored(item.Value, owned) {
					violations = append(violations, loanViolation{item.Pos(), "scoped SQL transaction cannot be sent on a channel"})
				}
			case *ast.GoStmt:
				if loanReferenced(item.Call, owned) {
					violations = append(violations, loanViolation{item.Pos(), "scoped SQL transaction cannot enter a goroutine"})
					return false
				}
			}
			return true
		})
		return true
	})
	return violations
}

func TestScopedSQLLoansCannotCommitOrEscape(t *testing.T) {
	visitMutationOwners(t, func(_ string, fset *token.FileSet, file *ast.File) {
		for _, violation := range scopedSQLLoanViolations(file) {
			t.Errorf("%s: %s", fset.Position(violation.pos), violation.msg)
		}
	})
}

func TestNoNewExportedRawTxAttemptPairs(t *testing.T) {
	visitMutationOwners(t, func(path string, fset *token.FileSet, file *ast.File) {
		aliases := map[string]string{}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if importPath != "database/sql" && importPath != "github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol" {
				continue
			}
			alias := filepath.Base(importPath)
			if spec.Name != nil {
				alias = spec.Name.Name
			}
			aliases[importPath] = alias
		}
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || !fn.Name.IsExported() || fn.Type.Params == nil {
				continue
			}
			hasSQL, hasAttempt := false, false
			for _, parameter := range fn.Type.Params.List {
				ptr, ok := parameter.Type.(*ast.StarExpr)
				if !ok {
					continue
				}
				selector, ok := ptr.X.(*ast.SelectorExpr)
				if !ok {
					continue
				}
				qualifier, ok := selector.X.(*ast.Ident)
				if !ok {
					continue
				}
				hasSQL = hasSQL || qualifier.Name == aliases["database/sql"] && selector.Sel.Name == "Tx"
				hasAttempt = hasAttempt || qualifier.Name == aliases["github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"] && selector.Sel.Name == "Attempt"
			}
			if !hasSQL || !hasAttempt {
				continue
			}
			name := filepath.Base(filepath.Dir(path)) + "." + fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				receiver := fn.Recv.List[0].Type
				if ptr, ok := receiver.(*ast.StarExpr); ok {
					receiver = ptr.X
				}
				if ident, ok := receiver.(*ast.Ident); ok {
					name = filepath.Base(filepath.Dir(path)) + "." + ident.Name + "." + fn.Name.Name
				}
			}
			t.Errorf("%s: exported raw SQL transaction and Attempt pair %s", fset.Position(fn.Pos()), name)
		}
	})
}

func TestOwnerSQLTxParametersHaveNoDirectNativeControlOrEscape(t *testing.T) {
	visitMutationOwners(t, func(path string, fset *token.FileSet, file *ast.File) {
		sqlAlias := "sql"
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if importPath == "database/sql" && spec.Name != nil {
				sqlAlias = spec.Name.Name
			}
		}
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Type.Params == nil || fn.Body == nil {
				continue
			}
			owned := map[*ast.Object]bool{}
			for _, parameter := range fn.Type.Params.List {
				ptr, ok := parameter.Type.(*ast.StarExpr)
				if !ok {
					continue
				}
				selector, ok := ptr.X.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Tx" {
					continue
				}
				qualifier, ok := selector.X.(*ast.Ident)
				if !ok || qualifier.Name != sqlAlias {
					continue
				}
				for _, name := range parameter.Names {
					if name.Obj != nil {
						owned[name.Obj] = true
					}
				}
			}
			if len(owned) == 0 {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch item := node.(type) {
				case *ast.SelectorExpr:
					if item.Sel.Name != "Commit" && item.Sel.Name != "Rollback" {
						break
					}
					receiver := ast.Node(item.X)
					for {
						wrapped, ok := receiver.(*ast.ParenExpr)
						if !ok {
							break
						}
						receiver = wrapped.X
					}
					if ident, ok := receiver.(*ast.Ident); ok && owned[ident.Obj] {
						t.Errorf("%s: SQL transaction helper cannot %s", fset.Position(item.Pos()), item.Sel.Name)
					}
				case *ast.ReturnStmt:
					if filepath.Base(filepath.Dir(path)) == "eventpersistence" && filepath.Base(path) == "events.go" && fn.Name.Name == "chooseRowQueryer" {
						break // Returns a query interface only to its scoped caller.
					}
					for _, value := range item.Results {
						if loanStored(value, owned) {
							t.Errorf("%s: SQL transaction helper returns native authority", fset.Position(item.Pos()))
							break
						}
					}
				case *ast.GoStmt:
					if loanReferenced(item.Call, owned) {
						t.Errorf("%s: SQL transaction helper starts asynchronous use", fset.Position(item.Pos()))
					}
				case *ast.SendStmt:
					if loanStored(item.Value, owned) {
						t.Errorf("%s: SQL transaction helper sends native authority", fset.Position(item.Pos()))
					}
				}
				return true
			})
		}
	})
}

func TestScopedSQLLoanGuardCases(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		{"commit", "tx.Commit()", true},
		{"rollback", "tx.Rollback()", true},
		{"parenthesized commit", "(tx).Commit()", true},
		{"method value", "f := tx.Commit; _ = f", true},
		{"assign", "saved = tx", true},
		{"field assign", "saved.tx = tx", true},
		{"declare", "var saved any = tx; _ = saved", true},
		{"composite", "saved = []any{tx}", true},
		{"struct composite", "saved = struct{ tx *sql.Tx }{tx: tx}", true},
		{"address", "saved = &tx", true},
		{"convert", "saved = any(tx)", true},
		{"interface convert", "saved = interface{}(tx)", true},
		{"return", "return tx", true},
		{"return address", "return &tx", true},
		{"channel", "ch <- tx", true},
		{"goroutine", "go helper(tx)", true},
		{"goroutine callback", "go func() { helper(tx) }()", true},
		{"capture", "f := func() { helper(tx) }; _ = f", true},
		{"audited callback helper", "return withEventStoreRetry(ctx, tx, func() error { return helper(tx) })", false},
		{"audited callback control", "return withEventStoreRetry(ctx, tx, func() error { return tx.Commit() })", true},
		{"arbitrary callback helper", "return keep(func() error { return helper(tx) })", true},
		{"stored source adapter", "saved = activeRunSourceOwnerFunc(func() error { return helper(tx) })", true},
		{"consumed source adapter", "return use(activeRunSourceOwnerFunc(func() error { return helper(tx) }))", false},
		{"local source adapter", "source := activeRunSourceOwnerFunc(func() error { return helper(tx) }); return resolveRunForkBundleInsertIdentity(runForkSourceOwnerFunc(source))", false},
		{"escaped source adapter", "source := activeRunSourceOwnerFunc(func() error { return helper(tx) }); return source", true},
		{"retained source adapter", "source := activeRunSourceOwnerFunc(func() error { return helper(tx) }); return keep(source)", true},
		{"deferred control", "defer func() { tx.Rollback() }()", true},
		{"deferred helper", "defer helper(tx)", false},
		{"local wrapper", "mutation := postgresRunLifecycleMutation{tx: tx}; mutation.requireSource()", false},
		{"generic wrapper", "holder := holderType{tx: tx}; _ = holder", true},
		{"wrapper return", "mutation := postgresRunLifecycleMutation{tx: tx}; return mutation", true},
		{"helper", "return helper(tx)", false},
		{"helper assignment", "err := helper(tx); return err", false},
		{"shadowed closure", "f := func(tx *sql.Tx) { helper(tx) }; _ = f; return nil", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := "package fixture\nimport (\"context\"; \"database/sql\")\nfunc fixture(attempt interface{ WithSQL(context.Context, func(context.Context, *sql.Tx) error) error }, ctx context.Context) error { return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error { " + tc.body + "; return nil }) }"
			file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			if got := len(scopedSQLLoanViolations(file)) > 0; got != tc.want {
				t.Fatalf("guard violation = %t, want %t", got, tc.want)
			}
		})
	}
}
