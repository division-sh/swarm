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

// This is append-only evidence for the 43 endpoints in the accepted source,
// not a change to recipes.json or the codemod's production admission rules.
//
//go:embed testdata/q10_current_endpoints.json
var q10CurrentEndpointBytes []byte

type q10CurrentEndpointPacket struct {
	Source, Scope string
	Rows          []fixtureSetupSuccessor2617
}

func decodeQ10CurrentEndpoints(raw []byte) (q10CurrentEndpointPacket, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != "7e26bea7902fd5e502a33c2513303bc3cddf7b42aca9f4086c0b9fd6cd1b02a1" {
		return q10CurrentEndpointPacket{}, fmt.Errorf("Q10 finite endpoint evidence changed")
	}
	var packet q10CurrentEndpointPacket
	if err := json.Unmarshal(raw, &packet); err != nil {
		return q10CurrentEndpointPacket{}, err
	}
	if packet.Source != "6abe659c42a5cbb02742956a17b606631f702887" ||
		packet.Scope != "https://github.com/division-sh/swarm/issues/2593#issuecomment-6192639877" || len(packet.Rows) != 43 {
		return q10CurrentEndpointPacket{}, fmt.Errorf("wrong Q10 source, scope or finite endpoint set")
	}
	return packet, nil
}

func q10CurrentEndpointRecipe(row recipe, pin fixtureSetupSuccessor2617) (recipe, error) {
	if !reflect.DeepEqual(row, pin.Original) || row.Removed || len(pin.Edits) == 0 {
		return recipe{}, fmt.Errorf("Q10 endpoint changed original history or identity")
	}
	if err := validateRecipeSnapshots(row); err != nil {
		return recipe{}, err
	}
	prior := row.Successor
	if prior == "" {
		prior = row.After
	}
	want, err := canonicalFunction(prior)
	if err != nil {
		return recipe{}, err
	}
	current, err := canonicalFunction(pin.Candidate)
	if err != nil || current == want {
		return recipe{}, fmt.Errorf("Q10 endpoint is not a distinct complete function")
	}
	reversed, err := reverseReviewedSuccessor2617(pin.Candidate, pin.Edits)
	if err != nil || reversed != want {
		return recipe{}, fmt.Errorf("Q10 endpoint failed exact historical reversal: %v", err)
	}
	row.Successor = pin.Candidate
	return row, nil
}

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
