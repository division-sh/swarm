package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// #2707/6213567352 preserves the complete previous row and reverses only the
// observer installation and the pre-snapshot completion acknowledgment.
func capabilityCompletionPredecessor2707(row recipe, actual string) (string, error) {
	return completionRebase2698Predecessor("../../..", row, actual)
}

func TestCapabilityCompletionSuccessorPreservesHistoryAndRefusesDrift(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Function != "TestSelectedForkReceiverAcquisitionCapabilityRefusalBothStores" {
			continue
		}
		matched++
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		previous, err := capabilityCompletionPredecessor2707(row, actual)
		if err != nil || fmt.Sprintf("%x", sha256.Sum256([]byte(previous))) != "b4a274d165a2a25741990e51271988ce29c466038a8cfe4c22d3a3ecc67322be" {
			t.Fatalf("exact prior capability diagnostic was not preserved: %v", err)
		}
		const observer = "\t\t\tcompletion := installReceiverCompletionBoundary(t)\n"
		const join = "\t\t\trequireReceiverCompletionBoundary(t, ctx, completion, rt.BundleHash, seed.RunID)\n"
		const snapshot = "\t\t\tbefore := snapshotForkReceiverApplication(t, rt.ReadApplicationSnapshot)\n"
		for _, cut := range []string{observer, join, snapshot} {
			if strings.Count(actual, cut) != 1 {
				t.Fatalf("missing or repeated exact capability cut: %q", cut)
			}
		}
		mutants := map[string]string{
			"missing_observer":    strings.Replace(actual, observer, "", 1),
			"duplicate_observer":  strings.Replace(actual, observer, observer+observer, 1),
			"missing_join":        strings.Replace(actual, join, "", 1),
			"duplicate_join":      strings.Replace(actual, join, join+join, 1),
			"foreign_run":         strings.Replace(actual, join, strings.Replace(join, "seed.RunID", "seed.EventID", 1), 1),
			"foreign_bundle":      strings.Replace(actual, join, strings.Replace(join, "rt.BundleHash", "seed.EventID", 1), 1),
			"changed_wait_bound":  strings.Replace(actual, "context.WithTimeout(t.Context(), servedEventPublishLifecycleProbeWaitTimeout)", "context.WithCancel(t.Context())", 1),
			"join_after_snapshot": strings.Replace(strings.Replace(actual, join, "", 1), snapshot, snapshot+join, 1),
			"changed_refusal":     strings.Replace(actual, "rpcErr.Code != -32603", "rpcErr.Code != 0", 1),
			"excluded_run_fields": strings.Replace(actual, snapshot, snapshot+"\t\t\tdelete(before, \"runs\")\n", 1),
			"historical_endpoint": previous,
		}
		for name, source := range mutants {
			t.Run(name, func(t *testing.T) {
				if source == actual {
					t.Fatal("capability completion hostile did not change source")
				}
				selfAuthorized := row
				selfAuthorized.Successor = source
				if _, err := capabilityCompletionPredecessor2707(selfAuthorized, source); err == nil {
					t.Fatal("unreviewed source/successor pair accepted")
				}
			})
		}
		for _, field := range []string{"family", "file", "function", "before", "after", "removed", "mechanical"} {
			t.Run("history/"+field, func(t *testing.T) {
				crossed := row
				switch field {
				case "family":
					crossed.Family += "-foreign"
				case "file":
					crossed.File = "unlisted.go"
				case "function":
					crossed.Function += "Foreign"
				case "before":
					crossed.Before = crossed.After
				case "after":
					crossed.After = previous
				case "removed":
					crossed.Removed = true
				case "mechanical":
					crossed.Mechanical = &mechanicalSnapshot{}
				}
				if _, err := capabilityCompletionPredecessor2707(crossed, actual); err == nil {
					t.Fatal("crossed capability completion history accepted")
				}
			})
		}
	}
	if matched != 1 {
		t.Fatalf("capability completion endpoints=%d, want exactly one", matched)
	}
}
