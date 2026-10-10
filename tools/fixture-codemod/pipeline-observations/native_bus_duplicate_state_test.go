package main

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

const busDuplicateWitnessShape = `func readEventBusExactDuplicateState(ctx context.Context,selected any,runID,eventID string)(eventBusExactDuplicateState,error) {
evidence,err := storetest.ObserveSemanticEventFixtureEvidence(ctx,selected,runID,eventID)
if err != nil { return eventBusExactDuplicateState{},err }
eventRows := 0
if evidence.RecordFound { eventRows = 1 }
return eventBusExactDuplicateState{Status:evidence.RunStatus,RunEventCount:evidence.RunEventCount,EventRows:eventRows,DeliveryRows:len(evidence.DeliveryProjections),OutcomeRows:evidence.SettledDeliveryAttemptCount},nil
}`

func normalizedBusDuplicateRoot(t *testing.T, source string) string {
	t.Helper()
	source = strings.Replace(source, "_, db, _ := testutil.StartPostgres(t)\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "pg := storetest.StartPostgresRuntimeStore(t)", 1)
	fn := projectionShapeFunction(t, source)
	matched := 0
	ast.Inspect(fn, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if !ok || name.Name != "assertEventBusExactDuplicateIsOperationNoOp" {
			return true
		}
		matched++
		owner, ok := call.Args[1].(*ast.Ident)
		if !ok || (owner.Name != "pg" && owner.Name != "sqliteStore") {
			t.Fatal("duplicate witness lost original selected owner")
		}
		closure, ok := call.Args[3].(*ast.FuncLit)
		if !ok {
			t.Fatal("duplicate witness lost its read callback")
		}
		expr, err := parser.ParseExpr("func()(eventBusExactDuplicateState,error){return readEventBusExactDuplicateState(ctx," + owner.Name + ",runID,evt.ID())}")
		if err != nil {
			t.Fatal(err)
		}
		closure.Body = expr.(*ast.FuncLit).Body
		return false
	})
	if matched != 1 {
		t.Fatalf("duplicate read callbacks=%d, want1", matched)
	}
	var out bytes.Buffer
	if err := format.Node(&out, token.NewFileSet(), fn); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func TestNativeBusDuplicateStatePreservesPublicationSettlementAndTerminalCuts(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-exact-duplicate-state" {
			continue
		}
		matched++
		if normalizedBusDuplicateRoot(t, row.Before) != normalizedBusDuplicateRoot(t, row.After) {
			t.Fatal("actual event/routes, duplicate publication, held claims, settlement or terminalization changed")
		}
		if !strings.Contains(row.After, "return readEventBusExactDuplicateState(ctx,") || !strings.Contains(row.After, ", runID, evt.ID())") {
			t.Fatal("duplicate observation lost original keys/owner")
		}
	}
	if matched != 2 {
		t.Fatalf("duplicate recipes=%d, want2", matched)
	}
	actual := selectedCausalObservationBody(t, "internal/runtime/bus/eventbus_publish_test.go", "readEventBusExactDuplicateState")
	want, err := canonicalFunction(busDuplicateWitnessShape)
	got, parseErr := canonicalFunction(actual)
	if err != nil || parseErr != nil || want != got {
		t.Fatal("stored counter, event presence, all deliveries, settled attempts or fail-closed error changed")
	}
	for _, pair := range [][2]string{
		{"evidence.RunEventCount", "0"}, {"len(evidence.DeliveryProjections)", "0"},
		{"evidence.SettledDeliveryAttemptCount", "0"}, {"if evidence.RecordFound", "if false"},
		{"ctx, selected, runID, eventID", "ctx, selected, runID, otherEvent"},
		{"eventBusExactDuplicateState{}, err", "eventBusExactDuplicateState{}, nil"},
	} {
		mutant := strings.Replace(actual, pair[0], pair[1], 1)
		changed, mutantErr := canonicalFunction(mutant)
		if mutant == actual || (mutantErr == nil && changed == want) {
			t.Fatalf("weakened duplicate witness admitted: %v", pair)
		}
	}
	bridge := selectedCausalObservationBody(t, "internal/store/storetest/event_readback.go", "ObserveSemanticEventFixtureEvidence")
	if !strings.Contains(bridge, "private.ReadSemanticEventFixtureEvidenceForTest(ctx, selected, runID, eventID)") {
		t.Fatal("error-returning bridge introduced another physical evidence owner")
	}
}
