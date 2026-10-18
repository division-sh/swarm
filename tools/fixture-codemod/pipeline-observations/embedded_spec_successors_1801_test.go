package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var errUnlistedEmbeddedSpecSuccessor1801 = errors.New("unlisted embedded-spec successor")

func embeddedSpecPriorEndpoint1801(row recipe, actual string) (recipe, string, error) {
	packet, err := loadEmbeddedSpecSuccessors1801(embeddedSpecSuccessorBytes1801)
	if err != nil {
		return recipe{}, "", err
	}
	for _, pin := range packet.Rows {
		if sameReviewedRecipeKey(row, pin.Original) {
			return reverseEmbeddedSpecSuccessor1801(row, actual, pin)
		}
	}
	return recipe{}, "", errUnlistedEmbeddedSpecSuccessor1801
}

func embeddedSpecHistoricalPair1801(row recipe, actual string) (recipe, string, error) {
	if row.Family == "native-run-setup-family144" &&
		((row.File == "internal/runtime/runforkexecution/constructed_agent_input_execution_test.go" && row.Function == "proveSelectedConstructedAgentInput") ||
			(row.File == "internal/runtime/runforkexecution/execution_test.go" && row.Function == "TestSelectedContractForkRejectsSyntheticCarryDynamicCreationBeforeMutation")) {
		original, err := fixtureSetupCurrentEndpoint2617(row, actual)
		if err != nil {
			return recipe{}, "", err
		}
		row, actual = original, original.Successor
	}
	original, prior, err := embeddedSpecPriorEndpoint1801(row, actual)
	if errors.Is(err, errUnlistedEmbeddedSpecSuccessor1801) {
		return row, actual, nil
	}
	return original, prior, err
}

func reverseEmbeddedSpecSuccessor1801(row recipe, actual string, pin fixtureSetupSuccessor2617) (recipe, string, error) {
	history := row
	history.Successor = pin.Original.Successor
	if !reflect.DeepEqual(history, pin.Original) || row.Successor == "" ||
		!canonicalComparatorEqual2617(row.Successor, pin.Candidate) {
		return recipe{}, "", fmt.Errorf("embedded-spec current source or historical receipt differs")
	}
	current, err := q10CurrentRecipe(row)
	if err != nil || !canonicalComparatorEqual2617(actual, current.Successor) {
		return recipe{}, "", fmt.Errorf("embedded-spec current source or exact composed successor differs: %v", err)
	}
	prior := pin.Original.After
	if pin.Original.Successor != "" {
		prior = pin.Original.Successor
	}
	expected, err := embeddedSpecFieldDeletion1801(prior)
	if err != nil || !canonicalComparatorEqual2617(expected, pin.Candidate) {
		return recipe{}, "", fmt.Errorf("embedded-spec successor changes more than selector plumbing: %v", err)
	}
	reversed, err := reverseReviewedSuccessor2617(pin.Candidate, pin.Edits)
	if err != nil || !canonicalComparatorEqual2617(reversed, prior) {
		return recipe{}, "", fmt.Errorf("embedded-spec successor lost its prior current endpoint: %v", err)
	}
	return pin.Original, prior, nil
}

func TestEmbeddedSpecHistoricalPairPreservesOnlyGenuinelyUnlistedPaths(t *testing.T) {
	unlisted := recipe{Family: "unlisted-family", File: "unlisted.go", Function: "unlisted", After: "unlisted source"}
	got, source, err := embeddedSpecHistoricalPair1801(unlisted, unlisted.After)
	if err != nil || !reflect.DeepEqual(got, unlisted) || source != unlisted.After {
		t.Fatalf("genuinely unlisted history path changed: %v", err)
	}
	packet, err := loadEmbeddedSpecSuccessors1801(embeddedSpecSuccessorBytes1801)
	if err != nil {
		t.Fatal(err)
	}
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, pin := range packet.Rows {
		t.Run(pin.Original.Function, func(t *testing.T) {
			var row recipe
			matches := 0
			for _, candidate := range rows {
				if sameReviewedRecipeKey(candidate, pin.Original) {
					row = candidate
					matches++
				}
			}
			if matches != 1 {
				t.Fatal("embedded-spec current recipe is not unique")
			}
			actual := selectedCausalObservationBody(t, row.File, row.Function)
			original, prior, err := embeddedSpecHistoricalPair1801(row, actual)
			want := pin.Original.Successor
			if want == "" {
				want = pin.Original.After
			}
			if err != nil || !reflect.DeepEqual(original, pin.Original) || !canonicalComparatorEqual2617(prior, want) {
				t.Fatalf("verified listed source did not recover its exact prior pair: %v", err)
			}
			if _, _, err := embeddedSpecHistoricalPair1801(pin.Original, want); err == nil {
				t.Fatal("listed prior endpoint silently gained the unlisted fallback")
			}
			if row.Successor != pin.Candidate {
				former := pin.Original
				former.Successor = pin.Candidate
				if _, _, err := embeddedSpecHistoricalPair1801(former, pin.Candidate); err == nil {
					t.Fatal("former embedded-spec endpoint was accepted as the latest selected child source")
				}
			}
		})
	}
}

func TestEmbeddedSpecSuccessorPacketRejectsChangedHistoryAndSemanticEdits(t *testing.T) {
	packet, err := loadEmbeddedSpecSuccessors1801(embeddedSpecSuccessorBytes1801)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"Source", "Authorization", "PopulationSHA256", "Before", "After", "Successor", "Candidate", "Edits"} {
		mutant := strings.Replace(string(embeddedSpecSuccessorBytes1801), `"`+change+`"`, `"foreign`+change+`"`, 1)
		if mutant == string(embeddedSpecSuccessorBytes1801) {
			t.Fatalf("missing packet field %s", change)
		}
		if _, err := loadEmbeddedSpecSuccessors1801([]byte(mutant)); err == nil {
			t.Fatalf("changed packet %s admitted", change)
		}
	}
	pin := packet.Rows[0]
	row := pin.Original
	for _, cut := range []struct{ before, after string }{
		{"t.Fatalf(", "t.Logf("}, {"family.Execute(", "foreign.Execute("},
		{"servedparity.BackendExplicitPostgres", "servedparity.BackendDefaultSQLite"},
		{"rt.DB.QueryRow(", "foreign.QueryRow("}, {"t.Cleanup(", "t.Run("},
	} {
		mutant := strings.Replace(pin.Candidate, cut.before, cut.after, 1)
		if mutant == pin.Candidate {
			t.Fatalf("semantic negative stopped exercising %s", cut.before)
		}
		row.Successor = mutant
		if _, _, err := embeddedSpecPriorEndpoint1801(row, mutant); err == nil {
			t.Fatalf("changed assertion/execution/store/cleanup admitted: %s", cut.before)
		}
	}
}
