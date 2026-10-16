package runforkpersistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"
)

func TestMigrationPhysicalLedgerCollectorRejectsReintroducedDeliverySQL(t *testing.T) {
	const source = `package delivery
func ReadRepeatedFanOutSnapshotTx() string { return "SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1" }
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "read_projections.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}}
	pkg, err := (&types.Config{}).Check(historicalBoundaryModule+"store/internal/backend/delivery", fset, []*ast.File{file}, info)
	if err != nil {
		t.Fatal(err)
	}
	problems := historicalBoundaryProblems(historicalBoundaryCollect(pkg, info, fset, file), historicalBoundaryAllowances(), false)
	if len(problems) != 1 || !strings.Contains(problems[0], "delivery::ReadRepeatedFanOutSnapshotTx/ledger_sql") {
		t.Fatalf("reintroduced delivery SQL escaped actual collector: %v", problems)
	}
}

func TestMigrationPhysicalLedgerInventoryRejectsForeignMissingAndDuplicateEdges(t *testing.T) {
	allowed := historicalBoundaryAllowances()
	for key, allowance := range allowed {
		if !strings.Contains(allowance.Reason, "fixed physical ledger witness") && !strings.Contains(allowance.Reason, "slot six") {
			continue
		}
		t.Run(key, func(t *testing.T) {
			cut := strings.LastIndex(key, "/")
			finding := historicalBoundaryFinding{Scope: key[:cut], Kind: key[cut+1:], Site: "exact fixture"}
			one := map[string]historicalBoundaryAllowance{key: allowance}
			if got := historicalBoundaryProblems([]historicalBoundaryFinding{finding}, one, true); len(got) != 0 {
				t.Fatalf("exact physical witness refused: %v", got)
			}
			for name, findings := range map[string][]historicalBoundaryFinding{
				"missing":          nil,
				"duplicate":        {finding, finding},
				"wrong caller":     {{Scope: finding.Scope + "Sibling", Kind: finding.Kind}},
				"extra same owner": {finding, {Scope: finding.Scope, Kind: finding.Kind + "_other"}},
				"delivery ledger":  {finding, {Scope: "store/internal/backend/delivery::ReadRepeatedFanOutSnapshotTx", Kind: "ledger_sql"}},
			} {
				t.Run(name, func(t *testing.T) {
					if got := historicalBoundaryProblems(findings, one, true); len(got) == 0 {
						t.Fatal("physical-owner inventory accepted hostile replacement")
					}
				})
			}
		})
	}
}
