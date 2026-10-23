package main

import (
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
