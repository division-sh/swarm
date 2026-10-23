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

func TestQ10CurrentEndpointsRetainExistingSemanticCompositions(t *testing.T) {
	packet, err := decodeQ10CurrentEndpoints(q10CurrentEndpointBytes)
	if err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, pin := range packet.Rows {
		var current string
		switch row := pin.Original; row.Family {
		case "native-served-delivery-quiescence-caller":
			current, err = q10DeliveryCallerCurrentSource(row)
		case "native-scenario-ack-fixture":
			current, err = q10ScenarioAckCurrentSource(row)
		case "native-api-physical-cardinality-consumer":
			current = q10APIPhysicalCardinalityCurrentRecipe(t, row).Successor
		case "native-gate-recovery-owner-caller":
			current, err = q10FixtureSetupCurrentSource(row)
		case "native-catalog-run-named-event":
			current, err = reviewedCatalogNamedIdentityCurrentSource(row)
		case "native-shared-coordinator-authority":
			var found bool
			current, found, err = q10SharedCoordinatorCurrentSource(row)
			if !found {
				t.Fatal("unlisted shared-coordinator endpoint in Q10 evidence")
			}
		case "native-run-setup-family144":
			if row.Function == "TestEntityTools_SaveEntityFieldAfterForkActivationUsesForkRunID" {
				current, err = q10FixtureSetupCurrentSource(row)
			}
		}
		if err != nil {
			t.Fatalf("existing semantic composition %s: %v", pin.Original.Function, err)
		}
		if current == "" {
			continue
		}
		matched++
		want, expectedErr := canonicalFunction(current)
		got, candidateErr := canonicalFunction(pin.Candidate)
		if expectedErr != nil || candidateErr != nil || want != got {
			t.Fatalf("Q10 packet diverged from the existing exact semantic composition: %s", pin.Original.Function)
		}
	}
	if matched != 22 {
		t.Fatalf("Q10 independently composed endpoints=%d, want22", matched)
	}
}
