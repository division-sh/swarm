package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestFamily145GateRecoveryNamedFaultSQLMatchesOriginalTemporalCuts(t *testing.T) {
	var rows []struct{ Function, Before string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("../../../internal/store/internal/backend/decisionpersistence/gate_recovery_fixture.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, keyword := range []string{"UPDATE", "CREATE"} {
		owned := family145GateRecoveryStatements(t, string(data), keyword)
		for _, row := range rows {
			if row.Function != "makeGateRecoveryRouteDue" && row.Function != "setGateRecoveryRouteAttempt" && row.Function != "installProposedEffectCreateFailure" {
				continue
			}
			for _, statement := range family145GateRecoveryStatements(t, "package proof\n"+row.Before, keyword) {
				found := false
				for _, candidate := range owned {
					found = found || statement == candidate
				}
				if !found {
					t.Fatalf("named gate fault changed table/phase/columns/predicate/payload: %s", row.Function)
				}
			}
		}
	}
}

func family145GateRecoveryStatements(t *testing.T, source, keyword string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "proof.go", source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(strings.TrimSpace(value), keyword+" ") {
			query := "package proof; const query = " + strconv.Quote("SELECT "+value)
			out = append(out, strings.TrimPrefix(family145ConnectorSQL(t, query)[0], "SELECT"))
		}
		return true
	})
	return out
}
