package main

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const nativeCatalogApplicationSnapshotShape = `func snapshotCatalogApplication(t *testing.T,h *runtimeHarness) map[string]storetest.SelectedForkStorageTableSnapshot {
t.Helper()
reader,err := h.catalogOperatorEventLister()
if err != nil { t.Fatal(err) }
snapshot,err := storetest.ReadSelectedForkApplicationStorageSnapshot(context.Background(),reader)
if err != nil { t.Fatal(err) }
return snapshot
}`

func catalogApplicationSnapshotConsumerPreserved(source string) bool {
	want, err := canonicalFunction(nativeCatalogApplicationSnapshotShape)
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeCatalogApplicationSnapshotConsumesCanonicalTypedReceiptWithoutFiltering(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-catalog-application-snapshot-observation" {
			continue
		}
		count++
		if !catalogApplicationSnapshotConsumerPreserved(row.After) {
			t.Fatal("whole-store witness changed context, original owner, errors or canonical typed receipt")
		}
		for _, pair := range [][2]string{
			{"if err != nil", "if false"}, {"context.Background(), reader", "h.ctx, reader"},
			{"h.catalogOperatorEventLister()", "foreignHarness.catalogOperatorEventLister()"},
			{"return snapshot", "delete(snapshot, \"events\"); return snapshot"},
		} {
			mutant := strings.Replace(row.After, pair[0], pair[1], 1)
			if mutant == row.After || catalogApplicationSnapshotConsumerPreserved(mutant) {
				t.Fatalf("filtered/refused whole-store evidence accepted: %v", pair)
			}
		}
	}
	if count != 1 {
		t.Fatalf("whole application snapshot recipes=%d, want one consumer", count)
	}
}

func TestNativeCatalogApplicationSnapshotRetainsJoinedDoubleRefusalAndFutureArtifacts(t *testing.T) {
	path := filepath.Join("..", "..", "..", "internal", "runtime", "cataloge2e", "connected_fork_history_test.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, data, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn, err := uniqueFunction(file, "TestConnectedForkCompletedDynamicHistoryRefusalBothStores")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset])
	for _, part := range []string{
		"h.rt.Shutdown()", "before := snapshotCatalogApplication(t, h)", "attempt < 2",
		"storetest.RequireRunForkReplayResumeBlocker(t, err, runfork.RunForkBlockerFlowRouteHistoryUnproven, runfork.RunForkReplayResumeFactRouteHistory)",
		"selected-contract route resolution requires complete static and dynamic topology proof",
		"result.Materialization.ForkRunID != \"\"", "result.Activation.Activated", "result.ExecutedEventCount != 0",
		"len(result.ForkEvents) != 0", "result.ForkLocalRuntimeContainer != nil",
		"!reflect.DeepEqual(before, after)", "refused connected-history fork changed application state",
	} {
		if !strings.Contains(source, part) {
			t.Fatalf("unchanged connected-fork lifecycle/refusal/work/conservation assertion missing: %q", part)
		}
	}
	if strings.Index(source, "h.rt.Shutdown()") >= strings.Index(source, "before := snapshotCatalogApplication") {
		t.Fatal("whole-store read precedes runtime join")
	}
	for _, raw := range []string{".BeginTx(", "tx.Query(", "tx.Commit(", "database/sql"} {
		if strings.Contains(string(data), raw) {
			t.Fatalf("local whole-store SQL interpreter survives: %s", raw)
		}
	}
}
