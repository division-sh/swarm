package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func family145ReporterRebaseProofError(before, after string) error {
	original, beforeErr := runSetupAssertions(before)
	current, afterErr := runSetupAssertions(after)
	if beforeErr != nil || afterErr != nil {
		return fmt.Errorf("parse rebased reporter assertions: %v/%v", beforeErr, afterErr)
	}
	// These are the complete assertion calls in master f8ffc42420a93b6dfb3575c4a3dddeee9d858693.
	encoded, err := json.Marshal(current)
	if err != nil || len(current) != 30 || fmt.Sprintf("%x", sha256.Sum256(encoded)) != "647e224519541aa6a026e67429c4d51c29c0b18d3ba28d5bc771fc3b845ad866" {
		return fmt.Errorf("reporter lost an original or master-added assertion")
	}
	next := 0
	for _, assertion := range current {
		if next < len(original) && original[next] == assertion {
			next++
		}
	}
	if next != len(original) || strings.Count(after, "storetest.StartPostgresRuntimeStore(t)") != 1 ||
		strings.Count(after, "storetest.StartSQLiteRuntimeStore(t)") != 1 || strings.Contains(after, "*sql.DB") ||
		family145PublicationRawEscape([]byte("package proof\n"+after)) != nil {
		return fmt.Errorf("reporter changed original assertion order or native ownership")
	}
	return nil
}

func TestFamily145ReporterRebasePreservesOriginalAndMasterAssertions(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.File != "internal/runtime/conformance/notify_all_children_runtime_conformance_test.go" || row.Function != "runNumericFanOutReporterShape" {
			continue
		}
		if err := family145ReporterRebaseProofError(row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		assertions, err := runSetupAssertions(row.After)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			if err := family145ReporterRebaseProofError(row.Before, strings.Replace(row.After, assertion, "ignore()", 1)); err == nil {
				t.Fatalf("reporter accepted removed assertion: %s", assertion)
			}
		}
		if err := family145ReporterRebaseProofError(row.Before, strings.Replace(row.After, "storetest.StartPostgresRuntimeStore(t)", "storetest.AdmitPostgresRuntimeStore(t, db)", 1)); err == nil {
			t.Fatal("reporter accepted raw construction")
		}
		return
	}
	t.Fatal("rebased reporter transition missing")
}

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
