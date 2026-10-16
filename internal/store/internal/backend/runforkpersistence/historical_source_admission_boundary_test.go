package runforkpersistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"
)

func TestHistoricalSourceAdmissionDelegationsStayExact(t *testing.T) {
	historicalSourceAdmissionBoundaryHostile(t)
}

func historicalSourceAdmissionBoundaryHostile(t *testing.T) {
	t.Helper()
	for _, resolver := range []string{"resolveRunForkRevisionPoint", "resolveSQLiteRunForkRevisionPoint"} {
		for _, cell := range []string{"exact", "duplicate_reference", "foreign_caller"} {
			t.Run(resolver+"/"+cell, func(t *testing.T) {
				caller, extra := "admitSelectedForkSource", ""
				if cell == "duplicate_reference" {
					extra = "alias := " + resolver + "; _ = alias"
				}
				if cell == "foreign_caller" {
					caller = "foreignCaller"
				}
				source := "package runforkpersistence\nfunc " + resolver + "() {}\nfunc " + caller + "() { " + resolver + "(); " + extra + " }\n"
				fset := token.NewFileSet()
				file, err := parser.ParseFile(fset, "run_fork_selected_contract_source_admission.go", source, 0)
				if err != nil {
					t.Fatal(err)
				}
				info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
				pkg, err := (&types.Config{}).Check(historicalBoundaryModule+"store/internal/backend/runforkpersistence", fset, []*ast.File{file}, info)
				if err != nil {
					t.Fatalf("exact source-admission fixture must type-check: %v", err)
				}
				problems := historicalBoundaryProblems(historicalBoundaryCollect(pkg, info, fset, file), historicalBoundaryAllowances(), false)
				if cell == "exact" {
					if len(problems) != 0 {
						t.Fatalf("exact admitted delegation refused: %v", problems)
					}
					return
				}
				want := historicalBoundaryOwner + caller + "/reference:" + historicalBoundaryOwner + resolver
				if len(problems) != 1 || !strings.Contains(problems[0], want) {
					t.Fatalf("extra or foreign delegation was not refused exactly: %v", problems)
				}
				if cell == "duplicate_reference" && !strings.Contains(problems[0], "observed 2, audited 1") {
					t.Fatalf("duplicate did not reach the exact count fence: %v", problems)
				}
			})
		}
	}
}
