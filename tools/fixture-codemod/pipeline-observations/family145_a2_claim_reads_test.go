package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

var family145A2ClaimReadCuts = map[string][]string{
	"TestA2KnownTargetWorkIssuedBeforeArmPublishesOutputBoundToActualArmOnBothStores": {
		"storage, err := storetest.ReadA2WorkerOutputs(ctx, selected.events, runID, triggerID)",
		"return storage.Count, storage.EventID",
	},
	"a2KnownTargetEarlyRefusal": {
		"storage, err := storetest.ReadA2EarlyRefusal(ctx, selected.events, eventID, nodeKey)",
		"status, raw, outcome := storage.Status, storage.Failure, storage.Outcome",
		"attempts := storage.SettledAttempts",
	},
	"TestA2SameBusinessCommitTransitionArmAndPublicationOnBothStores": {
		"storage, err := storetest.ReadA2WorkerOutputs(ctx, selected.events, runID, trigger.ID())",
		"return storage.Count, storage.EventID",
		"id, err := storetest.ReadGateNodeDeliveryIdentity(ctx, selected.events, eventID, nodeKey)",
		"return id",
		"attempts, err := storetest.CountGateNodeSettledAttempts(ctx, selected.events, delivery.eventID, delivery.nodeKey)",
	},
}

func family145A2ClaimReadProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("A2 publication/claim read changed original business, receipt, refusal or replay assertions: %s: %v", function, err)
	}
	cursor := -1
	for _, cut := range family145A2ClaimReadCuts[function] {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("A2 publication/claim read changed exact scope, destination or cut order: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "selected.db") || strings.Contains(after, "selected.postgres") || strings.Contains(after, "QueryRowContext") {
		return fmt.Errorf("A2 publication/claim retained a second SQL/dialect owner: %s", function)
	}
	return nil
}

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
