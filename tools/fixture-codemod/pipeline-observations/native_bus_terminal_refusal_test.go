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

const busTerminalWitnessShape = `func readEventBusTerminalRefusalState(ctx context.Context,selected runtimebus.RunLifecycleReadPersistence,runID,eventID string)(string,int,int,error) {
snapshot,err := selected.LoadRunLifecycleSnapshot(ctx,runID)
if err != nil { return "",0,0,err }
if snapshot.RunID != runID { return "",0,0,fmt.Errorf("terminal refusal witness borrowed run %s instead of %s",snapshot.RunID,runID) }
_,found,err := storetest.ReadCanonicalEventRecord(ctx,selected,eventID)
if err != nil { return "",0,0,err }
deliveries,err := storetest.ReadEventDeliveryDiagnosticRows(ctx,selected,eventID)
if err != nil { return "",0,0,err }
eventCount := 0
if found { eventCount = 1 }
return snapshot.Status,eventCount,len(deliveries),nil
}`

func normalizedBusTerminalRoot(t *testing.T, source string) string {
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
		if !ok || name.Name != "assertEventBusTerminalRunRefusal" {
			return true
		}
		matched++
		owner, ok := call.Args[1].(*ast.Ident)
		if !ok || (owner.Name != "pg" && owner.Name != "sqliteStore") {
			t.Fatal("terminal assertion lost its original selected owner")
		}
		closure, ok := call.Args[len(call.Args)-1].(*ast.FuncLit)
		if !ok {
			t.Fatal("terminal assertion lost its detached read callback")
		}
		expr, err := parser.ParseExpr("func(eventID string)(string,int,int,error){return readEventBusTerminalRefusalState(ctx," + owner.Name + ",runID,eventID)}")
		if err != nil {
			t.Fatal(err)
		}
		closure.Body = expr.(*ast.FuncLit).Body
		return false
	})
	if matched != 1 {
		t.Fatalf("terminal read callbacks=%d, want1", matched)
	}
	var out bytes.Buffer
	if err := format.Node(&out, token.NewFileSet(), fn); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func TestNativeBusTerminalRefusalPreservesWholeLifecycleSetupAndAssertions(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-terminal-refusal-state" {
			continue
		}
		matched++
		if normalizedBusTerminalRoot(t, row.Before) != normalizedBusTerminalRoot(t, row.After) {
			t.Fatal("run creation, actual cancellation, publish-owner assertions or selected identity changed")
		}
		for _, required := range []string{"return readEventBusTerminalRefusalState(ctx,", ", runID, eventID)"} {
			if !strings.Contains(row.After, required) {
				t.Fatalf("terminal read owner/key changed: %s", required)
			}
		}
	}
	if matched != 2 {
		t.Fatalf("terminal recipes=%d, want2", matched)
	}
	actual := selectedCausalObservationBody(t, "internal/runtime/bus/eventbus_publish_test.go", "readEventBusTerminalRefusalState")
	want, err := canonicalFunction(busTerminalWitnessShape)
	got, parseErr := canonicalFunction(actual)
	if err != nil || parseErr != nil || want != got {
		t.Fatal("terminal witness lost exact keys, full physical cardinality or refusal of partial evidence")
	}
	for _, pair := range [][2]string{
		{"snapshot.RunID != runID", "false"}, {"ctx, selected, eventID", "ctx, selected, otherEvent"},
		{"len(deliveries)", "0"}, {"if found", "if false"},
		{"return \"\", 0, 0, err", "return snapshot.Status, 0, 0, nil"},
	} {
		mutant := strings.Replace(actual, pair[0], pair[1], 1)
		changed, mutantErr := canonicalFunction(mutant)
		if mutant == actual || (mutantErr == nil && changed == want) {
			t.Fatalf("weakened terminal read admitted: %v", pair)
		}
	}
}
