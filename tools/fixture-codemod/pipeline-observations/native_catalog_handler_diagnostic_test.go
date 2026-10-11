package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const nativeCatalogHandlerDiagnostic = `diagnostic, readErr := storetest.ReadLatestHandlerErrorLog(testAuthorActivityContext(context.Background()), reader)
t.Fatalf("handler_outcome = %q, want %q; failure=%s; diagnostic=%s; diagnostic_read_error=%v", got, want, failure, diagnostic, readErr)`

func catalogDiagnosticProjectionPredecessor(row recipe) (string, bool) {
	source := row.After
	if row.Function != "assertHandlerOutcomeForEntity" && row.Function != "assertTriggerReceipt" {
		return source, true
	}
	name, fatal := "handler_outcome", "t.Fatal"
	if row.Function == "assertTriggerReceipt" {
		name, fatal = "trigger receipt", "h.t.Fatal"
	}
	current := "\tif h == nil {\n\t\t" + fatal + "(\"runtime harness is required for " + name + " assertions\")\n\t}"
	previous := "\tif h == nil || h.db == nil {\n\t\t" + fatal + "(\"database is required for " + name + " assertions\")\n\t}"
	if strings.Count(source, current) != 1 {
		return "", false
	}
	source = strings.Replace(source, current, previous, 1)
	if row.Function == "assertTriggerReceipt" {
		return source, true
	}
	start := strings.Index(source, "\t\t\tdiagnostic, readErr :=")
	if start < 0 {
		return "", false
	}
	end := strings.Index(source[start:], "\n\t\t}")
	oldStart := strings.Index(row.Before, "\t\t\tvar diagnostic []byte")
	if end < 0 || oldStart < 0 {
		return "", false
	}
	oldEnd := strings.Index(row.Before[oldStart:], "\n\t\t}")
	if oldEnd < 0 {
		return "", false
	}
	newBlock := source[start : start+end]
	want, err := canonicalFunction("func witness() {" + nativeCatalogHandlerDiagnostic + "}")
	got, parseErr := canonicalFunction("func witness() {" + newBlock + "}")
	if err != nil || parseErr != nil || want != got {
		return "", false
	}
	return strings.Replace(source, newBlock, row.Before[oldStart:oldStart+oldEnd], 1), true
}

func TestNativeCatalogHandlerDiagnosticRetainsFailureAndDropsOnlyRawPresence(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Function == "assertTriggerReceipt" || row.Function == "assertHandlerOutcomeForEntity" {
			count++
			if !catalogReceiptRecipePreservesAssertions(row) {
				t.Fatal("diagnostic migration changed receipt authority or primary assertions")
			}
			for _, pair := range [][2]string{
				{"ReadLatestHandlerErrorLog(testAuthorActivityContext(context.Background()), reader)", "ReadLatestHandlerErrorLog(context.Background(), foreignOwner)"},
				{"failure, diagnostic, readErr", "nil, diagnostic, nil"},
				{"got != \"success\"", "got == \"success\""},
			} {
				mutant := row
				mutant.After = strings.Replace(row.After, pair[0], pair[1], 1)
				if mutant.After != row.After && catalogReceiptRecipePreservesAssertions(mutant) {
					t.Fatalf("lost diagnostic owner/context/error or primary failure accepted: %v", pair)
				}
			}
		}
		if row.Family == "native-catalog-handler-diagnostic" {
			if row.Function != "TestCatalogLatestPipelineReceiptUsesOriginalReadOwnerBothStores" || strings.Count(row.After, "h.db = nil") != 1 {
				t.Fatal("unreviewed raw-presence control")
			}
			want, err := canonicalFunction(row.Before)
			got, parseErr := canonicalFunction(strings.Replace(row.After, "\th.db = nil\n", "", 1))
			if err != nil || parseErr != nil || got != want {
				t.Fatal("raw-presence control changed its original workload/assertions")
			}
		}
	}
	if count != 2 {
		t.Fatalf("diagnostic/presence consumer inventory=%d, want two", count)
	}
}

func TestNativeCatalogHandlerDiagnosticOwnerRetainsExactPhysicalSelection(t *testing.T) {
	path := filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", "eventrecord", "causal_observation.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"SELECT payload FROM events WHERE event_name='platform.runtime_log'",
		"AND payload->'details'->>'action'='handler_error' ORDER BY created_at DESC LIMIT 1",
		"AND json_extract(payload,'$.details.action')='handler_error' ORDER BY created_at DESC LIMIT 1",
		"errors.Is(err, sql.ErrNoRows)", "return append(json.RawMessage(nil), out...), nil",
	} {
		if !strings.Contains(string(data), fragment) {
			t.Fatalf("handler-error physical witness missing %q", fragment)
		}
	}
}
