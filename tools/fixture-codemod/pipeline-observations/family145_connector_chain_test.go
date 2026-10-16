package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestFamily145ConnectorReadQueriesRetainExactHistoricalPredicates(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	original := map[string][]string{}
	for _, row := range rows {
		original[row.Function] = family145ConnectorSQL(t, "package proof\n"+row.Before)
	}
	for _, row := range []struct{ file, function, predecessor string }{
		{"eventrecord/connector_storage_observation.go", "ReadConnectorNormalizedProviderEventIDTx", "loadNormalizedProviderEventID"},
		{"eventrecord/connector_storage_observation.go", "CountConnectorNamedSourceEventTx", "countSlackManagedConnectorFailureEventsForSource"},
		{"eventrecord/connector_storage_observation.go", "CountConnectorNamedEventIdentityTx", "slackManagedConnectorEventExists"},
		{"eventrecord/connector_storage_observation.go", "ReadConnectorEventChronologyTx", "telegramConnectorSupportedSurfaceDiagnostics"},
		{"delivery/read_projections.go", "CountTemplateNodeDeliveryTx", "countTelegramConnectorSupportedSurfaceNodeDeliveries"},
		{"delivery/read_projections.go", "CountConnectorRunNodeDeliveriesTx", "telegramConnectorSupportedSurfaceDiagnostics"},
		{"delivery/read_projections.go", "ReadConnectorLatestNodeDeliveryDiagnosticTx", "telegramConnectorSupportedSurfaceDiagnostics"},
		{"delivery/read_projections.go", "CountConnectorEntityDeadLettersTx", "telegramConnectorSupportedSurfaceDiagnostics"},
		{"delivery/read_projections.go", "ReadConnectorLatestEntityDeadLetterDiagnosticTx", "telegramConnectorSupportedSurfaceDiagnostics"},
		{"activityjournal/fork_observation.go", "CountConnectorRunActivityAttemptsTx", "telegramConnectorSupportedSurfaceDiagnostics"},
		{"pipelinepersistence/connector_receipt_observation.go", "ReadConnectorLatestActivityReceiptDiagnosticTx", "telegramConnectorSupportedSurfaceDiagnostics"},
		{"eventrecord/connector_storage_observation.go", "ReadChannelFirstActivitySourceRouteTx", "assertConfiguredChannelJournal"},
		{"activityjournal/fork_observation.go", "CountChannelSucceededToolAttemptsTx", "assertConfiguredChannelJournal"},
	} {
		data, err := os.ReadFile(filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", row.file))
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), row.file, data, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		var queries []string
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Name.Name == row.function {
				queries = family145ConnectorSQL(t, "package proof\n"+family145NodeString(fn))
			}
		}
		if len(queries) == 0 {
			t.Fatalf("missing closed read owner: %s/%s", row.file, row.function)
		}
		for _, query := range queries {
			found := false
			for _, before := range original[row.predecessor] {
				if query == before {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("closed owner changed historical SQL scope/order/NULL semantics: %s: %s", row.function, query)
			}
		}
	}
}

func family145ConnectorSQL(t testing.TB, source string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "sql-proof.go", source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var queries []string
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatal(err)
		}
		words := strings.Fields(value)
		if len(words) == 0 || !strings.EqualFold(words[0], "select") {
			return true
		}
		for i := 1; strings.Contains(value, "?"); i++ {
			value = strings.Replace(value, "?", "$"+strconv.Itoa(i), 1)
		}
		value = strings.ReplaceAll(value, "::uuid", "")
		var compact strings.Builder
		quoted := false
		for _, character := range value {
			if character == '\'' {
				quoted = !quoted
			}
			if !quoted && (character == ' ' || character == '\n' || character == '\t' || character == '\r') {
				continue
			}
			compact.WriteRune(character)
		}
		queries = append(queries, compact.String())
		return true
	})
	return queries
}

func TestFamily145ConnectorSQLCorrespondencePreservesQuotedCaseAndSpaces(t *testing.T) {
	original := family145ConnectorSQL(t, "package proof; const query = \"SELECT status FROM attempts WHERE status='succeeded'\"")[0]
	for _, hostile := range []string{"SUCCEEDED", "suc ceeded"} {
		query := family145ConnectorSQL(t, "package proof; const query = \"SELECT status FROM attempts WHERE status='"+hostile+"'\"")[0]
		if original == query {
			t.Fatalf("SQL correspondence hid changed quoted value: %s", hostile)
		}
	}
}
