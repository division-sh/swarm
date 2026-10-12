package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
	"testing"
)

func family145GateNativeSetupProofError(function, before, after string) error {
	want := before
	switch function {
	case "openSQLiteGateRecoveryStore":
		want = strings.Replace(want, "db: storetest.Database(selected), ", "", 1)
	case "openPostgresGateRecoveryStore":
		want = strings.Replace(want, "\t_, db, cleanup := testutil.StartPostgres(t)\n\tt.Cleanup(cleanup)\n\tselected := storetest.AdmitPostgresRuntimeStore(t, db)", "\tselected := storetest.StartPostgresRuntimeStore(t)", 1)
		want = strings.Replace(want, "postgres: true, db: db, ", "", 1)
	default:
		return fmt.Errorf("unknown gate native setup %s", function)
	}
	if want != after { return fmt.Errorf("gate native setup changed original owner bindings, persistence, trace assertion or return: %s", function) }
	return family145DescriptorReadinessEscape([]byte("package proof\n"+after))
}

func TestFamily145GateNativeSetupRetiresBothCarriersWithoutChangingNamedOwners(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil { t.Fatal(err) }
	var found int
	for _, row := range rows {
		if row.Function != "openSQLiteGateRecoveryStore" && row.Function != "openPostgresGateRecoveryStore" { continue }
		found++
		if err := family145GateNativeSetupProofError(row.Function, row.Before, row.After); err != nil { t.Fatal(err) }
		for _, pair := range [][2]string{
			{"events: selected", "events: foreign"}, {"cards: selected", "cards: foreign"},
			{"lifecycle: selected", "lifecycle: foreign"}, {"trace: selected", "trace: foreign"},
			{"NewWorkflowPersistence(selected)", "NewWorkflowPersistence(foreign)"},
			{"result.trace != selected", "false"}, {"return result", "return gateRecoveryStoreCase{}"},
		} {
			if family145GateNativeSetupProofError(row.Function, row.Before, strings.Replace(row.After, pair[0], pair[1], 1)) == nil { t.Fatalf("gate setup lost %s without rejection", pair[0]) }
		}
	}
	if found != 2 { t.Fatalf("gate setup inventory: %d, want both native backends", found) }
}

func TestFamily145GateRecoverySharedCaseCannotReintroducePoolOrDialectCapabilities(t *testing.T) {
	data, err := os.ReadFile("../../../internal/runtime/pipeline/workflow_gate_recovery_external_test.go"); if err != nil { t.Fatal(err) }
	file, err := parser.ParseFile(token.NewFileSet(), "gate.go", data, parser.AllErrors); if err != nil { t.Fatal(err) }
	want := map[string]string{"name":"string", "events":"gateRecoverySelectedStore", "lifecycle":"runtimerunlifecycle.CandidateStore", "cards":"gateRecoveryDecisionStore", "persistence":"runtimepipeline.WorkflowPersistence", "trace":"gateRecoveryTraceStore"}
	var got map[string]string
	ast.Inspect(file, func(node ast.Node) bool {
		declaration, ok := node.(*ast.TypeSpec); if !ok || declaration.Name.Name != "gateRecoveryStoreCase" { return true }
		structure, ok := declaration.Type.(*ast.StructType); if !ok { t.Fatal("gate fixture case stopped being a fixed named-port value") }
		got = map[string]string{}
		for _, field := range structure.Fields.List { for _, name := range field.Names { got[name.Name] = family145NodeString(field.Type) } }
		return false
	})
	if !reflect.DeepEqual(want, got) { t.Fatalf("gate shared case gained raw/dialect or lost named owner: %v", got) }
	if err := family145DescriptorReadinessEscape(data); err != nil { t.Fatal(err) }
}
