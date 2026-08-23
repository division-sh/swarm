package contracts

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestHandlerRuleStringLabelProductionCensus(t *testing.T) {
	expected := map[string]int{
		"internal/runtime/activity_validation.go":                                3,
		"internal/runtime/bootverify/workflow_compute_module_checks.go":          3,
		"internal/runtime/bootverify/workflow_payload_completeness_checks.go":    7,
		"internal/runtime/bootverify/workflow_policy_sheet_lookup_checks.go":     3,
		"internal/runtime/bootverify/workflow_policy_sheet_validation_checks.go": 3,
		"internal/runtime/contracts/workflow_contract_activity.go":               3,
		"internal/runtime/contracts/workflow_contract_emit.go":                   3,
		"internal/runtime/contracts/workflow_transition_carriers.go":             2,
		"internal/runtime/engine/executor.go":                                    1,
		"internal/runtime/pipeline/workflow_handler_preview.go":                  2,
		"internal/runtime/semanticview/authored_emit_sites.go":                   3,
	}
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	got := map[string]int{}
	for _, subtree := range []string{"internal/runtime", "internal/store", "internal/operatorread"} {
		err := filepath.WalkDir(filepath.Join(root, subtree), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			ast.Inspect(parsed, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.Field:
					for _, name := range typed.Names {
						if name.Name == "RuleID" {
							got[relative]++
						}
					}
				case *ast.SelectorExpr:
					if typed.Sel.Name == "RuleID" {
						got[relative]++
					}
				case *ast.KeyValueExpr:
					if name, ok := typed.Key.(*ast.Ident); ok && name.Name == "RuleID" {
						got[relative]++
					}
				case *ast.BinaryExpr:
					if (ruleIDExpression(typed.X) || ruleIDExpression(typed.Y)) && !ruleIDDisplayPresenceCheck(typed) {
						t.Errorf("%s compares display-only RuleID at %s", relative, typed.Op)
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
	if !equalRuleIDProductionCensus(got, expected) {
		t.Fatalf("production RuleID census changed\n got: %#v\nwant: %#v\nRuleID is display-only; add typed ContractElementRef authority before updating this ledger", sortedRuleIDCensus(got), sortedRuleIDCensus(expected))
	}
}

func ruleIDDisplayPresenceCheck(expression *ast.BinaryExpr) bool {
	if expression == nil || (expression.Op != token.EQL && expression.Op != token.NEQ) {
		return false
	}
	return ruleIDExpression(expression.X) && emptyStringLiteral(expression.Y) ||
		ruleIDExpression(expression.Y) && emptyStringLiteral(expression.X)
}

func emptyStringLiteral(expression ast.Expr) bool {
	literal, ok := expression.(*ast.BasicLit)
	return ok && literal.Kind == token.STRING && literal.Value == `""`
}

func ruleIDExpression(expression ast.Expr) bool {
	found := false
	ast.Inspect(expression, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == "RuleID" {
			found = true
			return false
		}
		return !found
	})
	return found
}

func equalRuleIDProductionCensus(left, right map[string]int) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func sortedRuleIDCensus(values map[string]int) []string {
	out := make([]string, 0, len(values))
	for path, count := range values {
		out = append(out, path+"="+string(rune('0'+count)))
	}
	sort.Strings(out)
	return out
}
