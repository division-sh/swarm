package main

import (
	"encoding/json"
	"go/ast"
	"strconv"
	"strings"
	"testing"
)

func nativeObservabilityConsumerSignature(source string) string {
	return strings.ReplaceAll(source, "selected observabilityFixtureStore, db *sql.DB, dialect authoractivityfixture.Dialect", "selected observabilityFixtureStore")
}

func nativeObservabilityConstructionSource(source string) string {
	for _, cut := range [][2]string{
		{"observabilityFixtureStore, *sql.DB, authoractivityfixture.Dialect", "observabilityFixtureStore"},
		{"\t\t\tvar db *sql.DB\n", ""},
		{"\t\t\tdialect := authoractivityfixture.DialectSQLite\n", ""},
		{"selected, db = s, storetest.DatabaseForTest(s)", "selected = s"},
		{"\t\t\t\t_, db, _ = testutil.StartPostgres(t)\n\t\t\t\tselected = storetest.AdmitPostgresRuntimeStore(t, db)\n\t\t\t\tdialect = authoractivityfixture.DialectPostgres", "\t\t\t\tselected = storetest.StartPostgresRuntimeStore(t)"},
		{"proof(t, ctx, selected, db, dialect)", "proof(t, ctx, selected)"},
	} {
		source = strings.ReplaceAll(source, cut[0], cut[1])
	}
	return source
}

func nativeObservabilityConflictSource(source string) string {
	for _, cut := range [][2]string{
		{"selected observabilityFixtureStore, db *sql.DB, dialect authoractivityfixture.Dialect", "selected observabilityFixtureStore"},
		{"\t\tquery := `INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, side_effects, failure, processed_at) VALUES (?, 'platform', ?, ?, ?, ?, ?)`\n\t\tif dialect == authoractivityfixture.DialectPostgres {\n\t\t\tquery = `INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, side_effects, failure, processed_at) VALUES ($1::uuid, 'platform', $2, $3, $4::jsonb, $5::jsonb, $6)`\n\t\t}\n\t\tfailure, err := json.Marshal(testFailure(\"receipt_should_not_win\"))\n\t\tif err != nil {\n\t\t\tt.Fatal(err)\n\t\t}\n\t\tfor _, receipt := range []struct {\n\t\t\tagent, outcome, effects string\n\t\t\tfailure                 any\n\t\t}{\n\t\t\t{\"agent-pending\", \"dead_letter\", `{\"retry_count\":9}`, string(failure)},\n\t\t\t{\"agent-failed\", \"success\", `{\"retry_count\":0}`, nil},\n\t\t} {\n\t\t\tif _, err := db.ExecContext(ctx, query, eventID, receipt.agent, receipt.outcome, receipt.effects, receipt.failure, time.Now().UTC()); err != nil {\n\t\t\t\tt.Fatalf(\"conflicting receipt: %v\", err)\n\t\t\t}\n\t\t}\n", "\t\tfor _, receipt := range []storetest.ObservabilityReceiptConflict{\n\t\t\tstoretest.PendingDeliveryWithDeadLetterReceipt,\n\t\t\tstoretest.FailedDeliveryWithSuccessReceipt,\n\t\t} {\n\t\t\tif err := storetest.SetObservabilityReceiptConflict(ctx, selected, eventID, receipt, time.Now().UTC()); err != nil {\n\t\t\t\tt.Fatalf(\"conflicting receipt: %v\", err)\n\t\t\t}\n\t\t}\n"},
		{"if _, err := db.ExecContext(ctx, query, detailID, \"agent-failed\", \"dead_letter\", `{\"retry_count\":7,\"error\":\"receipt-loses\"}`, nil, time.Now().UTC()); err != nil {", "if err := storetest.SetObservabilityReceiptConflict(ctx, selected, detailID, storetest.FailedDeliveryWithDeadLetterReceipt, time.Now().UTC()); err != nil {"},
	} {
		source = strings.ReplaceAll(source, cut[0], cut[1])
	}
	return source
}

func observabilityReceiptInsertLiterals(t *testing.T, source string) []string {
	t.Helper()
	var queries []string
	ast.Inspect(projectionShapeFunction(t, source), func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil || !strings.HasPrefix(value, "INSERT INTO event_receipts ") {
			return true
		}
		for i := 1; i <= 6; i++ {
			value = strings.ReplaceAll(value, "$"+strconv.Itoa(i), "?")
		}
		queries = append(queries, value)
		return true
	})
	return queries
}

func TestNativeObservabilityHarnessConfinesEveryReceiptFaultAndConsumer(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-observability-harness" {
			continue
		}
		matched++
		before := nativeObservabilityConstructionSource(row.Before)
		if row.Function != "withNativeObservabilityStores" {
			before = nativeObservabilityConflictSource(row.Before)
			port := selectedCausalObservationBody(t, "internal/store/internal/backend/pipelinepersistence/test_observability_receipt_conflict.go", "InsertObservabilityReceiptConflictForTest")
			oldQueries, newQueries := observabilityReceiptInsertLiterals(t, row.Before), observabilityReceiptInsertLiterals(t, port)
			if len(oldQueries) != 2 || len(newQueries) != 2 || oldQueries[0] != newQueries[0] || oldQueries[1] != newQueries[1] {
				t.Fatal("receipt SQL columns, dialect casts or fixed platform subtype changed")
			}
			for _, cut := range []string{"\"agent-pending\", \"dead_letter\", `{\"retry_count\":9}`", "\"agent-failed\", \"success\", `{\"retry_count\":0}`", "`{\"retry_count\":7,\"error\":\"receipt-loses\"}`", "\"receipt_should_not_win\""} {
				if !strings.Contains(row.Before, cut) || !strings.Contains(port, cut) {
					t.Fatalf("receipt fault data drift: %s", cut)
				}
			}
		}
		want, err := canonicalFunction(before)
		got, afterErr := canonicalFunction(row.After)
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		value, sourceErr := canonicalFunction(actual)
		if err != nil || afterErr != nil || sourceErr != nil || want != got || value != got {
			t.Fatalf("observation harness drift: %s", row.Function)
		}
		for _, raw := range []string{"*sql.DB", "authoractivityfixture.", "testutil.StartPostgres", "AdmitPostgresRuntimeStore", "storetest.Database", "db.Exec", "db.Query"} {
			if strings.Contains(actual, raw) {
				t.Fatalf("harness retains raw authority: %s", raw)
			}
		}
		for _, cut := range []string{"storetest.StartPostgresRuntimeStore(t)", "proof(t, ctx, selected)", "storetest.PendingDeliveryWithDeadLetterReceipt", "storetest.FailedDeliveryWithSuccessReceipt", "storetest.FailedDeliveryWithDeadLetterReceipt", "item.RetryCount != 1", "row[\"status\"] != \"failed\"", "payload invented entity filter match", "typed pair filter", "t.Fatalf("} {
			mutant := strings.Replace(row.After, cut, "unreviewedObservabilityCut", 1)
			if mutant == row.After {
				continue
			}
			changed, mutantErr := canonicalFunction(mutant)
			if mutantErr == nil && changed == want {
				t.Fatalf("receipt/workload/owner mutation accepted: %s", cut)
			}
		}
	}
	for _, function := range []string{"TestCanonicalEventReadbackOverridesConflictingReceiptsBothStores", "TestRetiredDashboardObservabilityAssertionsThroughV1BothStores", "TestCanonicalObservabilityCorruptionRefusesBothStores"} {
		actual := selectedCausalObservationBody(t, "internal/apiv1/retired_dashboard_preservation_test.go", function)
		if strings.Contains(actual, "*sql.DB") || strings.Contains(actual, "dialect") {
			t.Fatalf("raw harness consumer survives: %s", function)
		}
	}
	if matched != 2 {
		t.Fatalf("observation harness recipes=%d,want2", matched)
	}
}
