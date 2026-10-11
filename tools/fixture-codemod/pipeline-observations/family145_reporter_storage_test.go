package main

import (
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func TestFamily145ReporterWitnessesAndOpeningHaveNoRawAuthority(t *testing.T) {
	for _, group := range []struct {
		file      string
		functions []string
	}{
		{"notify_all_children_runtime_conformance_test.go", []string{"runNumericFanOutReporterShape", "assertNotifyAllChildrenRunEventCount", "loadNotifyAllChildrenSemanticRejections"}},
		{"fan_out_chunk_history_test.go", []string{"assertReporterFanOutSingleChunkHistory"}},
		{"fan_out_serving_matrix_helpers_test.go", []string{"assertServingMatrixHistory"}},
	} {
		data, err := os.ReadFile("../../../internal/runtime/conformance/" + group.file)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), group.file, data, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		for _, function := range group.functions {
			fn, err := uniqueFunction(file, function)
			if err != nil {
				t.Fatal(err)
			}
			source := family145NodeString(fn)
			if strings.Contains(source, "*sql.DB") || family145PublicationRawEscape([]byte("package proof\n"+source)) != nil {
				t.Fatalf("%s retained raw construction, SQL or a pool carrier", function)
			}
		}
	}
}

func TestFamily145ReporterStorageBusinessOraclesRejectDroppedAssertions(t *testing.T) {
	for _, entry := range []struct {
		file, function string
		assertions     int
	}{
		{"notify_all_children_runtime_conformance_test.go", "loadNotifyAllChildrenSemanticRejections", 3},
		{"fan_out_chunk_history_test.go", "assertReporterFanOutSingleChunkHistory", 5},
		{"fan_out_serving_matrix_helpers_test.go", "assertServingMatrixHistory", 5},
	} {
		data, err := os.ReadFile("../../../internal/runtime/conformance/" + entry.file)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), entry.file, data, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		fn, err := uniqueFunction(file, entry.function)
		if err != nil {
			t.Fatal(err)
		}
		source := family145NodeString(fn)
		assertions, err := family145DiagnosticBusinessAssertions(source)
		if err != nil || len(assertions) != entry.assertions {
			t.Fatalf("%s business assertions changed: %v/%v", entry.function, assertions, err)
		}
		for _, assertion := range assertions {
			hostile := strings.Replace(source, assertion, "ignore()", 1)
			changed, err := family145DiagnosticBusinessAssertions(hostile)
			if err != nil || strings.Join(changed, "\n") == strings.Join(assertions, "\n") {
				t.Fatalf("%s lost assertion was invisible: %s/%v", entry.function, assertion, err)
			}
		}
	}
}
