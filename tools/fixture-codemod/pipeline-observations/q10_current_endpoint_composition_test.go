package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// The two whole-corpus proofs use the same immutable 893 recipes as before.
// Only the independently enumerated 43 current endpoints compose their exact
// Q10 cuts; the other 850 rows and every historical packet are unchanged.
func q10CurrentEndpointRecipes(t *testing.T) []recipe {
	t.Helper()
	packet, err := decodeQ10CurrentEndpoints(q10CurrentEndpointBytes)
	if err != nil {
		t.Fatal(err)
	}
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 893 {
		t.Fatal("Q10 endpoint composition changed its original corpus")
	}
	matched := make(map[int]bool)
	for _, pin := range packet.Rows {
		index := -1
		for i, row := range rows {
			if sameReviewedRecipeKey(row, pin.Original) {
				if index >= 0 {
					t.Fatal("duplicate original Q10 endpoint")
				}
				index = i
			}
		}
		if index < 0 || matched[index] {
			t.Fatal("missing, unlisted or duplicate Q10 endpoint")
		}
		current, err := q10CurrentEndpointRecipe(rows[index], pin)
		if err != nil {
			t.Fatalf("%s: %v", pin.Original.Function, err)
		}
		rows[index], matched[index] = current, true
	}
	if len(matched) != 43 {
		t.Fatal("Q10 current endpoint coverage differs from the retained census")
	}
	return rows
}

func TestQ10CurrentEndpointsPreserveExactHistoryAndRejectCrossedPairs(t *testing.T) {
	packet, err := decodeQ10CurrentEndpoints(q10CurrentEndpointBytes)
	if err != nil {
		t.Fatal(err)
	}
	rows := q10CurrentEndpointRecipes(t)
	for _, pin := range packet.Rows {
		t.Run(pin.Original.File+"/"+pin.Original.Function, func(t *testing.T) {
			var selected []recipe
			for _, row := range rows {
				if sameReviewedRecipeKey(row, pin.Original) {
					selected = append(selected, row)
				}
			}
			if len(selected) != 1 {
				t.Fatal("missing or duplicate composed endpoint")
			}
			history := selected[0]
			history.Successor = pin.Original.Successor
			if !reflect.DeepEqual(history, pin.Original) {
				t.Fatal("current endpoint overwrote historical recipe fields")
			}
			if files, changes, err := prepareFiles("../../..", selected); err != nil || len(files) != 0 || len(changes) != 0 {
				t.Fatalf("exact current endpoint is not inert: %v", err)
			}
			for _, field := range []string{"family", "file", "function", "before", "after", "successor", "removed", "mechanical"} {
				mutant := pin.Original
				switch field {
				case "family":
					mutant.Family += "-unlisted"
				case "file":
					mutant.File = "unlisted.go"
				case "function":
					mutant.Function += "Unlisted"
				case "before":
					mutant.Before = pin.Candidate
				case "after":
					mutant.After = pin.Candidate
				case "successor":
					mutant.Successor = pin.Candidate
				case "removed":
					mutant.Removed = true
				case "mechanical":
					mutant.Mechanical = &mechanicalSnapshot{SourceCommit: strings.Repeat("0", 40), BeforeSHA256: strings.Repeat("0", 64), After: mutant.After}
				}
				if _, err := q10CurrentEndpointRecipe(mutant, pin); err == nil {
					t.Fatalf("crossed Q10 source/history pair accepted: %s", field)
				}
			}
			guard := recipe{File: pin.Original.File, Function: pin.Original.Function, Before: pin.Candidate, After: pin.Candidate}
			for _, source := range []string{pin.Original.Before, pin.Original.After, pin.Original.Successor,
				strings.Replace(pin.Candidate, "{", "{ unreviewedQ10Cut();", 1)} {
				if source == "" {
					continue
				}
				if _, changed, err := rewriteFunction(guard.File, []byte("package proof\n"+source), guard); err == nil || changed {
					t.Fatal("old or unreviewed source admitted as the exact Q10 endpoint")
				}
			}
			for index := range pin.Edits {
				mutant := pin
				mutant.Edits = append([]reviewedSuccessorEdit(nil), pin.Edits...)
				mutant.Edits[index].Start++
				if _, err := q10CurrentEndpointRecipe(pin.Original, mutant); err == nil {
					t.Fatal("moved Q10 edit accepted without exact reversal")
				}
			}
		})
	}
}

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

func TestQ10CurrentEndpointsUseTheOriginalMigrationPreparationOwner(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	if files, changes, err := prepareFiles("../../..", rows); err != nil || len(files) != 0 || len(changes) != 0 {
		t.Fatalf("real migration preparation differs from exact Q10 endpoints: %v", err)
	}
	packet, err := decodeQ10CurrentEndpoints(q10CurrentEndpointBytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range packet.Rows {
		projected, err := q10CurrentRecipe(pin.Original)
		want := pin.Candidate
		if pin.Original.Family == "native-invalid-lifecycle-target" {
			want = strings.Replace(want, "if after.Presence != WorkflowTargetPersistenceCompleteFieldless ||", "if after.Presence != WorkflowTargetPersistenceLifecycleOnly ||", 1)
		}
		if err != nil || projected.Successor != want {
			t.Fatalf("original preparation omitted fixed endpoint %s: %v", pin.Original.Function, err)
		}
		unlisted := pin.Original
		unlisted.Function += "Unlisted"
		if got, err := q10CurrentRecipe(unlisted); err != nil || !reflect.DeepEqual(got, unlisted) {
			t.Fatal("finite endpoint owner interpreted an unlisted recipe")
		}
		crossed := pin.Original
		crossed.Before = pin.Candidate
		if got, err := q10CurrentRecipe(crossed); err != nil || !reflect.DeepEqual(got, crossed) {
			t.Fatal("finite endpoint owner interpreted crossed historical input")
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
