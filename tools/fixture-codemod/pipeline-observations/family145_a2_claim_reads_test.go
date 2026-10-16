package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

func TestFamily145A2ClaimReadPhysicalOwnersPreserveEveryOriginalDialectQuery(t *testing.T) {
	for _, tc := range []struct{ file, function, caller string }{
		{"eventrecord/template_storage_observation.go", "ReadA2WorkerOutputsTx", "TestA2KnownTargetWorkIssuedBeforeArmPublishesOutputBoundToActualArmOnBothStores"},
		{"eventrecord/template_storage_observation.go", "ReadA2WorkerOutputsTx", "TestA2SameBusinessCommitTransitionArmAndPublicationOnBothStores"},
		{"delivery/read_projections.go", "ReadA2EarlyRefusalTx", "a2KnownTargetEarlyRefusal"},
		{"delivery/read_projections.go", "ReadGateNodeDeliveryIdentityTx", "TestA2SameBusinessCommitTransitionArmAndPublicationOnBothStores"},
		{"delivery/read_projections.go", "CountGateNodeSettledAttemptsTx", "TestA2SameBusinessCommitTransitionArmAndPublicationOnBothStores"},
	} {
		physical := family145ConversationForkOwnerSource(t, "internal/backend/"+tc.file, tc.function)
		queries := family145A2ClaimLiteralQueries(t, physical)
		var rows []struct{ Function, Before string }
		if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
			t.Fatal(err)
		}
		var original []string
		for _, row := range rows {
			if row.Function == tc.caller {
				original = family145A2ClaimLiteralQueries(t, row.Before)
			}
		}
		if len(queries) != 2 || !strings.Contains(physical, "if postgres {") {
			t.Fatalf("%s lost an original dialect", tc.function)
		}
		for _, query := range queries {
			matches := 0
			for _, want := range original {
				if query == want {
					matches++
				}
			}
			if matches != 1 {
				t.Fatalf("%s changed original scope, joins, NULL/grouping, casts or aggregate: %s", tc.function, query)
			}
		}
	}
}

func family145A2ClaimLiteralQueries(t *testing.T, source string) []string {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), "queries.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var queries []string
	ast.Inspect(parsed, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatal(err)
		}
		normalized := strings.Join(strings.Fields(value), " ")
		if strings.HasPrefix(normalized, "SELECT ") {
			queries = append(queries, normalized)
		}
		return true
	})
	return queries
}
