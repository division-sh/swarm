package main

import (
	"strings"
	"testing"
)

func TestQ10CurrentEndpointEvidenceRejectsChangedPacket(t *testing.T) {
	for _, field := range []string{"Source", "Scope", "Candidate", "Original", "Edits"} {
		mutant := strings.Replace(string(q10CurrentEndpointBytes), `"`+field+`"`, `"unreviewed`+field+`"`, 1)
		if mutant == string(q10CurrentEndpointBytes) {
			t.Fatalf("missing evidence hostile control: %s", field)
		}
		if _, err := decodeQ10CurrentEndpoints([]byte(mutant)); err == nil {
			t.Fatalf("changed Q10 packet accepted: %s", field)
		}
	}
}

func TestQ10MissingCompanionSuccessorPreservesConservationAndHistory(t *testing.T) {
	packet, err := decodeQ10CurrentEndpoints(q10CurrentEndpointBytes)
	if err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, pin := range packet.Rows {
		if pin.Original.Family != "native-invalid-lifecycle-target" {
			continue
		}
		matched++
		row, err := q10CurrentEndpointRecipe(pin.Original, pin)
		if err != nil {
			t.Fatal(err)
		}
		const old = "if after.Presence != WorkflowTargetPersistenceCompleteFieldless ||"
		const current = "if after.Presence != WorkflowTargetPersistenceLifecycleOnly ||"
		if strings.Count(pin.Candidate, old) != 1 || strings.Count(row.Successor, current) != 1 ||
			strings.Replace(row.Successor, current, old, 1) != pin.Candidate {
			t.Fatal("missing-companion repair changed more than its exact presence assertion")
		}
		guard := recipe{File: row.File, Function: row.Function, Before: row.Successor, After: row.Successor}
		for _, stale := range []string{pin.Candidate, strings.Replace(row.Successor, "!reflect.DeepEqual(before, after)", "false", 1)} {
			if _, changed, err := rewriteFunction(row.File, []byte("package proof\n"+stale), guard); err == nil || changed {
				t.Fatal("stale presence or weakened conservation admitted as the current endpoint")
			}
		}
	}
	if matched != 1 {
		t.Fatalf("missing-companion successor count=%d, want 1", matched)
	}
}
