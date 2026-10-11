package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func TestFamily145SourceRevisionCompleteFixtureHasNoRawAuthority(t *testing.T) {
	data, err := os.ReadFile("../../../internal/runtime/conformance/notify_all_children_runtime_conformance_test.go")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", data, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, function := range []string{"TestDynamicFlowSourceRevisionConvergesExactAgentSetAndFencesPredecessorsOnBothBackends", "proveDynamicFlowSourceRevisionConvergence"} {
		fn, err := uniqueFunction(file, function)
		if err != nil {
			t.Fatal(err)
		}
		source := family145NodeString(fn)
		if strings.Contains(source, "*sql.DB") || family145PublicationRawEscape([]byte("package proof\n"+source)) != nil {
			t.Fatalf("%s retained a raw source-revision lifetime or observation", function)
		}
	}
	data, err = os.ReadFile("../../../internal/runtime/conformance/lifecycle_diagnostic_overlap_test.go")
	if err != nil || family145PublicationRawEscape(data) != nil {
		t.Fatalf("complete lifecycle diagnostic fixture retained SQL/pool escape: %v", err)
	}
}

func TestFamily145SourceRevisionDiagnosticBusinessRefusalsRemainExact(t *testing.T) {
	data, err := os.ReadFile("../../../internal/runtime/conformance/lifecycle_diagnostic_overlap_test.go")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", data, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	fn, err := uniqueFunction(file, "requireSourceRevisionLifecycleDiagnosticReceipts")
	if err != nil {
		t.Fatal(err)
	}
	source := family145NodeString(fn)
	assertions, err := family145DiagnosticBusinessAssertions(source)
	if err != nil || len(assertions) != 6 {
		t.Fatalf("query/pending/empty/decode/cardinality refusals changed: %v/%v", assertions, err)
	}
	for _, assertion := range assertions {
		hostile := strings.Replace(source, assertion, "ignore()", 1)
		changed, err := family145DiagnosticBusinessAssertions(hostile)
		if err != nil || strings.Join(assertions, "\n") == strings.Join(changed, "\n") {
			t.Fatalf("source diagnostic refusal removal was invisible: %s/%v", assertion, err)
		}
	}
	if !strings.Contains(source, "!row.Projected || len(receipt) == 0") {
		t.Fatal("physical pending receipt refusal was weakened")
	}
}

func TestFamily145SourceRevisionProjectedMarkerCannotSynthesizeClockPresence(t *testing.T) {
	data, err := os.ReadFile("../../../internal/store/internal/backend/eventpersistence/lifecycle_diagnostic_observation.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := family145SourceProjectedMarker(data); err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{"true", "false", "len(row.Projection) != 0"} {
		hostile := strings.Replace(string(data), "row.Projected = at != nil", "row.Projected = "+marker, 1)
		if err := family145SourceProjectedMarker([]byte(hostile)); err == nil {
			t.Fatalf("synthesized projected-at presence accepted: %s", marker)
		}
	}
}

func family145SourceProjectedMarker(source []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", source, parser.AllErrors)
	if err != nil {
		return err
	}
	fn, err := uniqueFunction(file, "ReadSourceRevisionDiagnosticStorageTx")
	if err != nil {
		return err
	}
	assignments := 0
	var correct bool
	ast.Inspect(fn, func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if !ok || len(assignment.Lhs) != 1 || family145NodeString(assignment.Lhs[0]) != "row.Projected" {
			return true
		}
		assignments++
		correct = len(assignment.Rhs) == 1 && family145NodeString(assignment.Rhs[0]) == "at != nil"
		return true
	})
	if assignments != 1 || !correct {
		return fmt.Errorf("physical projected-at NULL presence must have one exact source")
	}
	return nil
}
