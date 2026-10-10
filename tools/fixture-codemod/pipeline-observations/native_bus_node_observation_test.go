package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

func TestNativeBusNodeObservationPreservesAllPhysicalDiagnosticQueries(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-node-observation" {
			continue
		}
		matched++
		want := eventDeliveryDiagnosticSQL(t, row.Before)
		got := []string{}
		for _, owner := range [][2]string{
			{"internal/store/internal/backend/delivery/read_projections.go", "ReadNodeDeliveryDiagnosticLinesTx"},
			{"internal/store/internal/backend/delivery/read_projections.go", "ReadNodeDeadLetterDiagnosticLinesTx"},
			{"internal/store/internal/backend/pipelinepersistence/owner_operations.go", "ReadNodeReceiptDiagnosticLinesTx"},
		} {
			queries := eventDeliveryDiagnosticSQL(t, selectedCausalObservationBody(t, owner[0], owner[1]))
			got = append(got, queries[1])
		}
		if len(want) != 4 || !reflect.DeepEqual(want[1:], got) {
			t.Fatal("node diagnostic columns, ordering, predicates or formatting SQL changed")
		}
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		value, err := canonicalFunction(actual)
		expected, expectedErr := canonicalFunction(row.After)
		if err != nil || expectedErr != nil || value != expected {
			t.Fatal("node observation diverged from finite migration")
		}
		for _, exact := range []string{
			"5 * time.Second", "10 * time.Millisecond",
			"ReadServedDeliveryStatusCount(context.Background(), selected, eventID, \"node\", nodeID, want)",
			"if count == 1 || time.Now().After(deadline)",
			"ReadNodeDeliveryDiagnosticStorage(context.Background(), selected, eventID)",
			"diagnostic.Rows, diagnostic.DeadLetters, diagnostic.Receipts",
		} {
			if !strings.Contains(actual, exact) {
				t.Fatalf("node observation lost exact cut %q", exact)
			}
		}
	}
	if matched != 1 {
		t.Fatalf("node observation recipes=%d,want1", matched)
	}
}

func nativeNodeObservationCallsUseOriginalOwner(source, callee string) (int, bool) {
	file, err := parser.ParseFile(token.NewFileSet(), "proof.go", "package proof\n"+source, 0)
	if err != nil {
		return 0, false
	}
	count, valid := 0, true
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if !ok || name.Name != callee {
			return true
		}
		count++
		if len(call.Args) != 5 {
			valid = false
			return true
		}
		owner, ok := call.Args[1].(*ast.Ident)
		valid = valid && ok && owner.Name == "pg"
		return true
	})
	return count, valid
}

func TestNativeBusNodeObservationAllSevenConsumersUseOriginalOwner(t *testing.T) {
	total := 0
	for _, name := range []string{
		"TestEventBusPublish_MixedEmptyAndTargetedNodeRoutesExecuteAndSettle",
		"TestEventBusPublish_NestedThreeLevelConnectChainExecutesEndToEnd",
	} {
		source := selectedCausalObservationBody(t, "internal/runtime/bus/eventbus_publish_test.go", name)
		count, valid := nativeNodeObservationCallsUseOriginalOwner(source, "assertNodeDeliveryStatus")
		if !valid || count == 0 {
			t.Fatal("node status consumer retains raw/foreign authority")
		}
		total += count
		mutant := strings.Replace(source, "assertNodeDeliveryStatus(t, pg,", "assertNodeDeliveryStatus(t, db,", 1)
		if _, accepted := nativeNodeObservationCallsUseOriginalOwner(mutant, "assertNodeDeliveryStatus"); mutant == source || accepted {
			t.Fatal("node status negative control failed to reject raw-pool substitution")
		}
	}
	if total != 7 {
		t.Fatalf("node status consumers=%d,want7", total)
	}
}
