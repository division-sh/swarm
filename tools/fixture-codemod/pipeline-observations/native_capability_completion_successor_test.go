package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// #2707/6213567352 preserves the complete previous row and reverses only the
// observer installation and the pre-snapshot completion acknowledgment.
//
//go:embed testdata/capability_completion_successor_2707.json
var capabilityCompletionSuccessor2707 []byte

func capabilityCompletionPredecessor2707(row recipe, actual string) (string, error) {
	if fmt.Sprintf("%x", sha256.Sum256(capabilityCompletionSuccessor2707)) != "4b531e52419e81158f1faed4ad8c3c63f367d30d13a3ddb79fd62302a61bb9ff" {
		return "", fmt.Errorf("capability completion history changed")
	}
	var pin struct {
		SourceCommit, Review string
		Original             recipe
		Candidate            string
		Edits                []reviewedSuccessorEdit
	}
	if err := json.Unmarshal(capabilityCompletionSuccessor2707, &pin); err != nil {
		return "", err
	}
	if pin.SourceCommit != "80a7850bb8b6108b46eb4a7b3dc1e1594cf543cb" || pin.Review != "https://github.com/division-sh/swarm/pull/2707#issuecomment-6213567352" ||
		pin.Original.Family != "native-served-delivery-quiescence-caller" || pin.Original.File != "internal/serveapp/fork_receiver_ownership_test.go" ||
		pin.Original.Function != "TestSelectedForkReceiverAcquisitionCapabilityRefusalBothStores" || len(pin.Edits) != 2 {
		return "", fmt.Errorf("unlisted capability completion history")
	}
	history := row
	history.Successor = pin.Original.Successor
	if !reflect.DeepEqual(history, pin.Original) || row.Successor != pin.Candidate {
		return "", fmt.Errorf("capability completion successor crossed its exact predecessor")
	}
	current, err := canonicalFunction(actual)
	want, wantErr := canonicalFunction(pin.Candidate)
	if err != nil || wantErr != nil || current != want {
		return "", fmt.Errorf("capability completion current source changed")
	}
	previous, err := reverseReviewedSuccessor2617(pin.Candidate, pin.Edits)
	if err != nil || previous != pin.Original.Successor {
		return "", fmt.Errorf("capability completion does not restore its exact predecessor: %v", err)
	}
	return previous, nil
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
		const snapshot = "\t\t\tbefore := snapshotForkReceiverApplication(t, rt)\n"
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
