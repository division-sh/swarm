package main

import (
	"strings"
	"testing"
)

const selectedRecoverySnapshotShape = `func selectedForkRecoveredPhysicalSnapshot(t *testing.T,ctx context.Context,h *runtimeHarness,runID string) string {
t.Helper()
var selected any = h.sqlite
if h.pg != nil { selected = h.pg }
snapshot,err := storetest.ReadSelectedForkRecoveredStorageSnapshot(ctx,selected,runID)
if err != nil { t.Fatal(err) }
data,err := json.Marshal(snapshot)
if err != nil { t.Fatal(err) }
return string(data)
}`

func selectedRecoverySnapshotPreserved(source string) bool {
	want, err := canonicalFunction(selectedRecoverySnapshotShape)
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeSelectedRecoverySnapshotPreservesConsumerRefusalsAndIdentity(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-selected-recovery-snapshot")
	if !selectedRecoverySnapshotPreserved(row.After) {
		t.Fatal("recovered snapshot lost its exact original owner, context, run, complete encoding or fail-closed error")
	}
	for _, pair := range [][2]string{
		{"selected = h.pg", "selected = foreignOwner"},
		{"ctx, selected, runID", "ctx, selected, otherRun"},
		{"ctx, selected, runID", "context.Background(), selected, runID"},
		{"json.Marshal(snapshot)", "json.Marshal(snapshot[\"events\"])"},
		{"if err != nil", "if false"}, {"return string(data)", "return \"\""},
	} {
		mutant := strings.Replace(row.After, pair[0], pair[1], 1)
		if mutant == row.After || selectedRecoverySnapshotPreserved(mutant) {
			t.Fatalf("weakened recovered snapshot admitted: %v", pair)
		}
	}
}

func TestNativeSelectedRecoverySnapshotReusesCompletePhysicalEncoderAndTableScope(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-selected-recovery-snapshot")
	owner := selectedCausalObservationBody(t, "internal/store/internal/runtimepersistence/test_selected_fork_application_snapshot.go", "ReadSelectedForkRecoveredStorageSnapshotForTest")
	for _, original := range []string{
		`[]string{"entity_state", "flow_instances", "flow_instance_runtime_readiness", "events"}`,
		`"SELECT * FROM "+table+" WHERE run_id = $1", runID`,
	} {
		if !strings.Contains(row.Before, original) || !strings.Contains(owner, original) {
			t.Fatalf("fixed original tables, complete columns or exact run predicate changed: %s", original)
		}
	}
	for _, required := range []string{
		"validateSelectedForkStorageIdentity(runID)", "selectedForkSnapshotObserverInitialized(selected)",
		"readServedDeliveryObservation(ctx, selected,", "readSelectedForkSnapshotRows(rows)",
		"snapshot[table] = evidence", "return nil, err",
	} {
		if !strings.Contains(owner, required) {
			t.Fatalf("original coherent owner, shared typed encoding or partial-read refusal missing: %s", required)
		}
	}
	if strings.Contains(row.After, "h.db") || strings.Contains(owner, "LIMIT ") || strings.Contains(owner, "DISTINCT ") {
		t.Fatal("raw consumer or narrowed/duplicate-collapsing snapshot returned")
	}
}
