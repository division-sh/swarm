package main

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"testing"
)

func TestOrderedRuleModelSuccessorsPreserveEveryExecutionStatement(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	want := map[string]int{
		"TestExecuteNodeContractHandlerOnSuccessRulesEmitsBothInOrder":                            1,
		"TestExecuteNodeContractHandlerRulesEmitTemplatePublishesOneMergedEvent":                  3,
		"TestExecuteNodeContractHandler_RejectsUndeclaredBusinessPayloadAcrossImmediateEmitSites": 2,
	}
	seen := map[string]bool{}
	for _, row := range rows {
		count, required := want[row.Function]
		if !required || row.File != "internal/runtime/pipeline/handler_engine_transaction_test.go" {
			continue
		}
		t.Run(row.Function, func(t *testing.T) {
			if seen[row.Function] || row.Successor == "" {
				t.Fatal("missing or ambiguous finite rule-model successor")
			}
			seen[row.Function] = true
			file, err := parser.ParseFile(token.NewFileSet(), "reviewed.go", "package proof\n"+row.After, 0)
			if err != nil || len(file.Decls) != 1 {
				t.Fatalf("invalid immutable reviewed body: %v", err)
			}
			removed := 0
			ast.Inspect(file, func(node ast.Node) bool {
				literal, ok := node.(*ast.CompositeLit)
				if !ok {
					return true
				}
				if isRuleModelType(literal.Type) {
					removed += removeRuleModelNames(literal)
				}
				if array, ok := literal.Type.(*ast.ArrayType); ok && isRuleModelType(array.Elt) {
					for _, element := range literal.Elts {
						if row, ok := element.(*ast.CompositeLit); ok {
							removed += removeRuleModelNames(row)
						}
					}
				}
				return true
			})
			if removed != count {
				t.Fatalf("removed row-name fields=%d, want exactly%d", removed, count)
			}
			var expected bytes.Buffer
			if err := format.Node(&expected, token.NewFileSet(), file.Decls[0]); err != nil {
				t.Fatal(err)
			}
			actual, err := canonicalFunction(row.Successor)
			if err != nil || actual != expected.String() {
				t.Fatalf("successor changes more than the retired typed row fields: %v\nwant:\n%s\ngot:\n%s", err, expected.String(), actual)
			}
		})
	}
	if len(seen) != len(want) {
		t.Fatalf("proved%d successors, want all%d", len(seen), len(want))
	}
}

func isRuleModelType(expr ast.Expr) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "HandlerRuleEntry" {
		return false
	}
	owner, ok := selector.X.(*ast.Ident)
	return ok && owner.Name == "runtimecontracts"
}

func removeRuleModelNames(row *ast.CompositeLit) int {
	var kept []ast.Expr
	removed := 0
	for _, field := range row.Elts {
		if kv, ok := field.(*ast.KeyValueExpr); ok {
			if name, ok := kv.Key.(*ast.Ident); ok && (name.Name == "ID" || name.Name == "Description") {
				removed++
				continue
			}
		}
		kept = append(kept, field)
	}
	row.Elts = kept
	return removed
}
