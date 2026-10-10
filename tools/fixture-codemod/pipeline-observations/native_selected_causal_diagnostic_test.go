package main

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const selectedCausalDiagnosticReadbackShape = `func assertSelectedCausalDiagnosticReadback(t *testing.T,h *runtimeHarness,item diaglog.LifecycleDiagnostic,kind,parent string) {
t.Helper()
var selected any = h.sqlite
if h.pg != nil { selected = h.pg }
observed,err := storetest.ReadSelectedCausalDiagnosticStorage(context.Background(),selected,item.OutboxID)
if err != nil { t.Fatal(err) }
count := 0
for _,row := range observed.Events {
run,cause := row.RunID,row.SourceEventID
if run != item.Identity.RunID || cause != parent { t.Fatalf("selected event lineage: %s/%s",run,cause) }
count++
}
raw := observed.Projection
var receipt struct {
RunID string ` + "`json:\"run_id\"`" + `
ParentEventID string ` + "`json:\"parent_event_id\"`" + `
LineageDisposition string ` + "`json:\"lineage_disposition\"`" + `
}
if err := json.Unmarshal(raw,&receipt); err != nil { t.Fatal(err) }
if count != 1 || receipt.RunID != item.Identity.RunID || receipt.ParentEventID != parent || receipt.LineageDisposition != "causal_"+kind {
t.Fatalf("selected diagnostic receipt/count: %s/%d",raw,count)
}
}`

func selectedCausalDiagnosticPreserved(source string) bool {
	want, err := canonicalFunction(selectedCausalDiagnosticReadbackShape)
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeSelectedCausalDiagnosticReadbackPreservesExactAssertions(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-selected-causal-diagnostic-readback")
	if !selectedCausalDiagnosticPreserved(row.After) {
		t.Fatal("causal readback lost original owner, complete multiplicity, lineage or stored receipt assertions")
	}
	for _, pair := range [][2]string{
		{"selected = h.pg", "selected = foreignOwner"},
		{"selected, item.OutboxID", "selected, otherOutbox"},
		{"for _, row := range observed.Events", "for _, row := range observed.Events[:1]"},
		{"row.SourceEventID", "row.RunID"}, {"run != item.Identity.RunID", "false"},
		{"cause != parent", "false"}, {"count != 1", "count < 1"},
		{"receipt.RunID != item.Identity.RunID", "false"},
		{"receipt.ParentEventID != parent", "false"},
		{"receipt.LineageDisposition != \"causal_\"+kind", "false"},
		{"raw := observed.Projection", "raw := []byte(`{}`)"},
	} {
		mutant := strings.Replace(row.After, pair[0], pair[1], 1)
		if mutant == row.After || selectedCausalDiagnosticPreserved(mutant) {
			t.Fatalf("weakened causal diagnostic admitted: %v", pair)
		}
	}
}

func TestNativeSelectedCausalDiagnosticRootRetainsEveryLifecycleCut(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-selected-causal-diagnostic-original-owner")
	before, err := canonicalFunction(row.Before)
	if err != nil {
		t.Fatal(err)
	}
	after, err := canonicalFunction(row.After)
	if err != nil {
		t.Fatal(err)
	}
	if before != strings.Replace(after, "\t\t\t\th.db = nil\n", "", 1) {
		t.Fatal("actual causal fork, activation, refusal or twice-projected receipt workload changed")
	}
}

func selectedCausalObservationBody(t *testing.T, path, name string) string {
	t.Helper()
	path = filepath.Join("..", "..", "..", path)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, body, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn, err := uniqueFunction(file, name)
	if err != nil {
		t.Fatal(err)
	}
	return string(body[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset])
}

func TestNativeSelectedCausalDiagnosticOwnersPreserveEveryOriginalQuery(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-selected-causal-diagnostic-readback")
	callback := nativeMissingHeaderRecipe(t, "native-selected-causal-parent-read")
	want := append(eventDeliveryDiagnosticSQL(t, row.Before), eventDeliveryDiagnosticSQL(t, callback.Before)...)
	var got []string
	for _, operation := range [][2]string{
		{"internal/store/internal/backend/eventrecord/lifecycle_diagnostic_observation.go", "ReadLifecycleDiagnosticEventLineage"},
		{"internal/store/internal/backend/eventpersistence/lifecycle_diagnostic_observation.go", "ReadLifecycleDiagnosticProjection"},
		{"internal/store/internal/backend/eventrecord/lifecycle_diagnostic_observation.go", "CountLifecycleDiagnosticEvents"},
		{"internal/store/internal/backend/eventpersistence/lifecycle_diagnostic_observation.go", "CountPendingLifecycleDiagnostic"},
	} {
		got = append(got, eventDeliveryDiagnosticSQL(t, selectedCausalObservationBody(t, operation[0], operation[1]))...)
	}
	// Parent publication already consumes the full canonical event owner; its
	// older entry-point count is not one of this cohort's physical outbox reads.
	var original []string
	for _, query := range want {
		if !strings.Contains(query, "WHEREevent_id=$1ANDrun_id=$2") {
			original = append(original, query)
		}
	}
	if len(original) != 6 || !reflect.DeepEqual(got, original) {
		t.Fatalf("global predicates, casts or projected-at cut changed: got=%v want=%v", got, original)
	}
	lineage := selectedCausalObservationBody(t, "internal/store/internal/backend/eventrecord/lifecycle_diagnostic_observation.go", "ReadLifecycleDiagnosticEventLineage")
	for _, required := range []string{"tx.QueryContext(ctx, query, outboxID)", "rows.Scan(&row.RunID, &row.SourceEventID)", "rows.Err()", "rows.Close()"} {
		if !strings.Contains(lineage, required) {
			t.Fatalf("strict lineage or complete read boundary missing: %s", required)
		}
	}
	for _, name := range []string{"ReadSelectedCausalDiagnosticStorageForTest", "ReadSelectedCausalDiagnosticConservationForTest"} {
		adapter := selectedCausalObservationBody(t, "internal/store/internal/runtimepersistence/test_event_support.go", name)
		for _, required := range []string{"validateSelectedForkStorageIdentity(outboxID)", "validateChannelObservationOwner(selected)", "readServedDeliveryObservation(ctx, selected,"} {
			if !strings.Contains(adapter, required) {
				t.Fatalf("original selected owner boundary missing in %s: %s", name, required)
			}
		}
	}
}
