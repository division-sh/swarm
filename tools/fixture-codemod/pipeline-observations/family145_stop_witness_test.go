package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
)

const stopWitnessDigest = "87b00ec86800b2f78e9af3947b503d9c60bee78640bef7e20d52d01966441400"

//go:embed family145-stop-witness-amendment.json
var stopWitnessBytes []byte

type stopWitnessPin struct{ File, Function, Before, After, SHA256 string }

func stopWitnessPins(t *testing.T) []stopWitnessPin {
	t.Helper()
	if fmt.Sprintf("%x", sha256.Sum256(stopWitnessBytes)) != stopWitnessDigest {
		t.Fatal("exact stop witness amendment changed")
	}
	var pins []stopWitnessPin
	if err := json.Unmarshal(stopWitnessBytes, &pins); err != nil || len(pins) != 15 {
		t.Fatalf("stop witness amendment inventory=%d, err=%v", len(pins), err)
	}
	return pins
}

// This is historical source correspondence for an authorized semantic repair,
// not a claim that the obsolete and corrected status predicates are equivalent.
func family145StopWitnessPrevious(t *testing.T, file, function, current string) string {
	t.Helper()
	for _, pin := range stopWitnessPins(t) {
		if file == pin.File && function == pin.Function && current == pin.After {
			return pin.Before
		}
	}
	return current
}

func family145CheckStopWitnessAmendment(t *testing.T, file, function, current string) bool {
	t.Helper()
	for _, pin := range stopWitnessPins(t) {
		if file == pin.File && function == pin.Function {
			if current != pin.After {
				t.Fatal("stop witness escaped its amended count, original cuts or assertions:", file, function)
			}
			return true
		}
	}
	return false
}

func TestFamily145StopSnapshotKeepsExactPhysicalQueriesAndNoRawCarrier(t *testing.T) {
	var original stopWitnessPin
	for _, pin := range stopWitnessPins(t) {
		if pin.Function == "readIssue2394StopSnapshot" {
			original = pin
		}
	}
	queries := eventDeliveryDiagnosticSQL(t, original.Before)
	if len(queries) != 5 || queries[2] != "SELECTCOUNT(*)FROM" {
		t.Fatal("historical snapshot lost its exact two row, three counter, nonterminal and clock cuts")
	}
	expected := append([]string{}, queries[:2]...)
	for _, table := range []string{"events", "run_fork_revisions", "run_fork_fact_revisions"} {
		expected = append(expected, "SELECTCOUNT(*)FROM"+table+"WHERErun_id=$1")
	}
	expected = append(expected, queries[3:]...)
	var actual []string
	for _, owner := range [][2]string{
		{"internal/store/internal/backend/pipelinepersistence/stop_snapshot_observation.go", "ReadStopFanOutIntentsStorageTx"},
		{"internal/store/internal/backend/pipelinepersistence/stop_snapshot_observation.go", "ReadStopFanOutOutcomesStorageTx"},
		{"internal/store/internal/runtimepersistence/test_api_event_storage.go", "CountPhysicalRunEventsForTest"},
		{"internal/store/internal/backend/runforkrevision/postgres.go", "CountActivityJournalRevisionsForTest"},
		{"internal/store/internal/backend/runforkrevision/postgres.go", "CountRunFactRevisionRowsForTest"},
		{"internal/store/internal/backend/delivery/read_projections.go", "ReadStopPendingDeliveryCountTx"},
		{"internal/store/internal/backend/runlifecycle/standalone_fixture_storage.go", "ReadStopRunControlClockStorageTx"},
	} {
		body := selectedCausalObservationBody(t, owner[0], owner[1])
		if owner[1] == "ReadStopPendingDeliveryCountTx" {
			body = requestKeyDeclaration(t, []byte("package proof\n"+body), owner[1])
			body = family145StopWitnessPrevious(t, owner[0], owner[1], body)
		}
		actual = append(actual, eventDeliveryDiagnosticSQL(t, body)...)
	}
	if !slices.Equal(actual, expected) {
		t.Fatalf("fixed physical snapshot predicates changed: want=%v, got=%v", expected, actual)
	}
	for _, raw := range []string{"sql.", ".DB", "Query(", "Scan(", ".Next(", ".Close(", "Reconstruct"} {
		if strings.Contains(original.After, raw) {
			t.Fatal("complete served stop snapshot retained raw authority:", raw)
		}
	}
}
