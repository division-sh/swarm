// Command migrate-bus-outbox propagates the native owner through the finite
// descriptor-only cohort. Fault, routing and ambient-context cases are separate.
package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strings"

	"golang.org/x/tools/go/ast/astutil"
)

var roots = map[string]bool{
	"TestCommittedReadinessFailureBlocksOnlyItsExactOutput":                            true,
	"TestEngineOutboxPersistsEventsAndDeliveriesInTransaction":                         true,
	"TestEngineOutboxExactDuplicateDispatchIsOperationNoOp":                            true,
	"TestEngineOutboxPreservesAppendOutcomeForEveryIntentInBatch":                      true,
	"TestEngineOutboxPreexistingExactDuplicateBatchDispatchesZero":                     true,
	"TestEngineOutboxConflictingSameIDBatchRollsBackOrderedOutcomes":                   true,
	"TestEngineOutboxDistinctIntentBatchDispatchesEachOnce":                            true,
	"TestEngineOutboxAndDispatcher_UseCanonicalDirectRecipientManifest":                true,
	"TestEngineOutboxAndDispatcher_DeliverInternalSubscribersOutsidePersistedManifest": true,
	"TestEnginePublicationRejectsFilteredDirectIntentBeforePersistence":                true,
	"TestPublishDirectRejectsFilteredExplicitRecipient":                                true,
}

func main() {
	const path = "internal/runtime/bus/outbox_test.go"
	source, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	if len(os.Args) == 2 && os.Args[1] == "--reviewed-source" {
		source, err = exec.Command("git", "show", "a525874ba4:"+path).Output()
		if err != nil {
			panic(err)
		}
	}
	output, err := rewrite(source)
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(path, output, 0o644); err != nil {
		panic(err)
	}
}

func rewrite(source []byte) ([]byte, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "outbox_test.go", source, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	type edit struct {
		start, end int
		text       []byte
	}
	var edits []edit
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || !roots[fn.Name.Name] {
			continue
		}
		if strings.Contains(render(fn.Body), "newNativeOutboxFixture") {
			continue
		}
		if err := rewriteBlocks(fn.Body); err != nil {
			return nil, fmt.Errorf("%s: %w", fn.Name, err)
		}
		astutil.Apply(fn.Body, rewriteExpression, nil)
		removeUnusedSetup(fn.Body)
		clearPositions(fn)
		body := render(fn.Body)
		wrapper := statements("for _, backend := range []string{\"sqlite\", \"postgres\"} { t.Run(backend, func(t *testing.T) " + body + ") }")
		fn.Body.List = wrapper
		clearPositions(fn)
		// Locate the original declaration in the parsed source, before rewriting
		// positions, rather than mixing source coordinates from parsed snippets.
		original, err := parser.ParseFile(token.NewFileSet(), "", source, 0)
		if err != nil {
			return nil, err
		}
		for _, value := range original.Decls {
			if root, ok := value.(*ast.FuncDecl); ok && root.Name.Name == fn.Name.Name {
				edits = append(edits, edit{int(root.Pos()) - 1, int(root.End()) - 1, []byte(render(fn))})
			}
		}
	}
	sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
	for _, edit := range edits {
		source = append(append(append([]byte(nil), source[:edit.start]...), edit.text...), source[edit.end:]...)
	}
	return format.Source(source)
}

func rewriteBlocks(body *ast.BlockStmt) error {
	var failure error
	ast.Inspect(body, func(node ast.Node) bool {
		block, ok := node.(*ast.BlockStmt)
		if !ok {
			return true
		}
		var result []ast.Stmt
		for index := 0; index < len(block.List); index++ {
			statement := block.List[index]
			text := render(statement)
			setup, matched, err := descriptorSetup(statement)
			if err != nil {
				failure = err
				return false
			}
			if matched {
				result = append(result, setup...)
				continue
			}
			if strings.HasPrefix(text, "db, mock, err := sqlmock.New()") || strings.HasPrefix(text, "tx, err := db.Begin()") || strings.HasPrefix(text, "eb, err := newScopedTestEventBus(") {
				index++ // The immediately following error check belongs to this setup.
				continue
			}
			if obsoleteStatement(text) {
				continue
			}
			if strings.HasPrefix(text, "ctx :=") {
				continue
			}
			if text == "ctx = runtimepipelinefixture.WithPostCommitActions(ctx, &postCommitActions)" {
				continue
			}
			if strings.HasPrefix(text, "err = eb.PublishDirect(") {
				statement.(*ast.AssignStmt).Tok = token.DEFINE
			}
			result = append(result, statement)
		}
		block.List = result
		return true
	})
	return failure
}

func descriptorSetup(statement ast.Stmt) ([]ast.Stmt, bool, error) {
	assignment, ok := statement.(*ast.AssignStmt)
	if !ok || len(assignment.Rhs) != 1 {
		return nil, false, nil
	}
	literal, ok := assignment.Rhs[0].(*ast.UnaryExpr)
	if !ok {
		return nil, false, nil
	}
	value, ok := literal.X.(*ast.CompositeLit)
	if !ok || render(value.Type) != "directRecipientTransactionalStore" {
		return nil, false, nil
	}
	var descriptors string
	for _, element := range value.Elts {
		pair, ok := element.(*ast.KeyValueExpr)
		if !ok || render(pair.Key) != "descriptors" {
			return nil, true, fmt.Errorf("non-descriptor fixture field %s", render(element))
		}
		descriptors = ", " + render(pair.Value) + "..."
	}
	name := render(assignment.Lhs[0])
	return statements("fixture := newNativeOutboxFixture(t, backend" + descriptors + "); " + name + " := fixture.store; eb := fixture.bus"), true, nil
}

func removeUnusedSetup(body *ast.BlockStmt) {
	counts := map[string]int{}
	astutil.Apply(body, func(cursor *astutil.Cursor) bool {
		if id, ok := cursor.Node().(*ast.Ident); ok {
			if selector, ok := cursor.Parent().(*ast.SelectorExpr); !ok || selector.Sel != id {
				counts[id.Name]++
			}
		}
		return true
	}, nil)
	ast.Inspect(body, func(node ast.Node) bool {
		block, ok := node.(*ast.BlockStmt)
		if !ok {
			return true
		}
		var result []ast.Stmt
		for _, statement := range block.List {
			if assignment, ok := statement.(*ast.AssignStmt); ok && len(assignment.Lhs) == 1 && assignment.Tok == token.DEFINE {
				name := render(assignment.Lhs[0])
				if (name == "store" || name == "recordingStore" || name == "eb" || name == "ctx") && counts[name] == 1 {
					continue
				}
			}
			result = append(result, statement)
		}
		block.List = result
		return true
	})
}

func clearPositions(node ast.Node) {
	positionType := reflect.TypeOf(token.NoPos)
	ast.Inspect(node, func(node ast.Node) bool {
		if node == nil {
			return true
		}
		value := reflect.ValueOf(node).Elem()
		for i := 0; i < value.NumField(); i++ {
			if field := value.Field(i); field.Type() == positionType {
				if value.Type().Field(i).Name == "Ellipsis" && field.Int() != 0 {
					field.SetInt(1)
				} else {
					field.SetInt(0)
				}
			}
		}
		return true
	})
}

func obsoleteStatement(text string) bool {
	for _, prefix := range []string{"defer db.Close()", "mock.Expect", "if err := tx.Commit()", "if err := tx.Rollback()", "if err := mock.ExpectationsWereMet()", "_ = tx.Rollback()", "runtimepipelinefixture.FlushRollbackActions(", "runtimepipelinefixture.FlushPostCommitActions(", "rollbackActions :=", "postCommitActions :="} {
		if strings.HasPrefix(text, prefix) {
			return true
		}
	}
	return false
}

func rewriteExpression(cursor *astutil.Cursor) bool {
	if id, ok := cursor.Node().(*ast.Ident); ok && id.Name == "ctx" {
		if selector, ok := cursor.Parent().(*ast.SelectorExpr); !ok || selector.Sel != id {
			cursor.Replace(expression("fixture.ctx"))
			return false
		}
	}
	call, ok := cursor.Node().(*ast.CallExpr)
	if !ok {
		return true
	}
	switch render(call.Fun) {
	case "commitEnginePublicationsForTest":
		call.Fun = expression("fixture.commitError")
		call.Args = call.Args[3:]
	case "runtimepipelinefixture.WithSQLTx", "runtimepipelinefixture.WithRollbackActions", "context.Background":
		cursor.Replace(expression("fixture.ctx"))
		return false
	case "eventtest.RunCreatingRootIngress":
		if render(call.Args[7]) != `""` {
			panic("nonempty parent requires explicit migration")
		}
		call.Fun = expression("eventtest.ExistingRunRootIngress")
		call.Args = append(call.Args[:7], call.Args[8:]...)
	case "len":
		if len(call.Args) == 1 && strings.HasSuffix(render(call.Args[0]), ".events") {
			cursor.Replace(expression("fixture.eventCount(t)"))
			return false
		}
	}
	return true
}

func statements(body string) []ast.Stmt {
	file, err := parser.ParseFile(token.NewFileSet(), "", "package p; func f(){"+body+"}", 0)
	if err != nil {
		panic(err)
	}
	return file.Decls[0].(*ast.FuncDecl).Body.List
}

func expression(source string) ast.Expr {
	result, err := parser.ParseExpr(source)
	if err != nil {
		panic(err)
	}
	return result
}

func render(node ast.Node) string {
	var buffer bytes.Buffer
	if err := format.Node(&buffer, token.NewFileSet(), node); err != nil {
		panic(err)
	}
	return buffer.String()
}
