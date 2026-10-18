package main

import (
	"crypto/sha256"
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

func TestEmbeddedSpecSuccessorsPreserveFiniteHistoryAndAllOtherWork(t *testing.T) {
	packet, err := loadEmbeddedSpecSuccessors1801(embeddedSpecSuccessorBytes1801)
	if err != nil {
		t.Fatal(err)
	}
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 893 {
		t.Fatal("embedded-spec deletion changed the finite recipe population")
	}
	restored := append([]recipe(nil), rows...)
	completionSuccessors := 0
	for i, row := range rows {
		if row.Function != "TestSelectedForkReceiverAcquisitionCapabilityRefusalBothStores" {
			continue
		}
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		prior, err := capabilityCompletionPredecessor2707(row, actual)
		if err != nil {
			t.Fatalf("preserve independently reviewed completion successor: %v", err)
		}
		restored[i].Successor = prior
		completionSuccessors++
	}
	if completionSuccessors != 1 {
		t.Fatalf("completion successors=%d, want the one independently reviewed row", completionSuccessors)
	}
	seen := map[string]bool{}
	priorSuccessors := 0
	q10Compositions := 0
	for _, pin := range packet.Rows {
		key := pin.Original.Family + "/" + pin.Original.File + "/" + pin.Original.Function
		if seen[key] {
			t.Fatal("duplicate embedded-spec successor key")
		}
		seen[key] = true
		if pin.Original.Successor != "" {
			priorSuccessors++
		}
		matches := 0
		for i, row := range rows {
			if !sameReviewedRecipeKey(row, pin.Original) {
				continue
			}
			matches++
			actual := selectedCausalObservationBody(t, row.File, row.Function)
			original, _, err := embeddedSpecPriorEndpoint1801(row, actual)
			if err != nil {
				t.Fatalf("%s: %v", key, err)
			}
			restored[i] = original
			assertEmbeddedSpecSuccessorRefusals1801(t, row, actual, pin)
			if row.File == "internal/serveapp/receiver_restart_test.go" && row.Function == "TestReceiverCompositionRestartBothStores" {
				assertQ10EmbeddedSpecComposition1801(t, row, actual, pin)
				q10Compositions++
			}
		}
		if matches != 1 {
			t.Fatalf("%s recipe multiplicity=%d", key, matches)
		}
	}
	population, err := json.Marshal(restored)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(population)) != packet.PopulationSHA256 || priorSuccessors != 12 || q10Compositions != 1 {
		t.Fatal("embedded-spec successor overwrote historical evidence, prior chains or an unrelated row")
	}
}

func assertQ10EmbeddedSpecComposition1801(t *testing.T, row recipe, actual string, embedded fixtureSetupSuccessor2617) {
	t.Helper()
	packet, err := q10CurrentEndpoints()
	if err != nil {
		t.Fatal(err)
	}
	var matches []fixtureSetupSuccessor2617
	for _, pin := range packet.Rows {
		if reflect.DeepEqual(pin.Original, embedded.Original) {
			matches = append(matches, pin)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("Q10/embedded restart intersection=%d, want exactly one full original row", len(matches))
	}
	pin := matches[0]
	current, err := q10EmbeddedSpecCurrentRecipe(row, pin)
	if err != nil || !canonicalComparatorEqual2617(current.Successor, actual) {
		t.Fatalf("complete current restart composition differs: %v", err)
	}
	// Prove both orders, rather than assuming selector deletion and the header
	// read commute. Each complete earlier endpoint remains in its frozen packet.
	reversed := actual
	for i := len(pin.Edits) - 1; i >= 0; i-- {
		edit := pin.Edits[i]
		from, to := strings.Join(edit.Current, ""), strings.Join(edit.Prior, "")
		if from == "" || strings.Count(reversed, from) != 1 {
			t.Fatal("missing or ambiguous Q10 restart edit after embedded-spec deletion")
		}
		reversed = strings.Replace(reversed, from, to, 1)
	}
	if !canonicalComparatorEqual2617(reversed, embedded.Candidate) {
		t.Fatal("Q10 restart reversal did not recover the exact delivered embedded-spec endpoint")
	}
	prior, err := reverseReviewedSuccessor2617(pin.Candidate, pin.Edits)
	historical := embedded.Original.After
	if embedded.Original.Successor != "" {
		historical = embedded.Original.Successor
	}
	if err != nil || !canonicalComparatorEqual2617(prior, historical) {
		t.Fatalf("Q10 restart lost its exact pre-embedded historical endpoint: %v", err)
	}
	for _, field := range []string{"family", "file", "function", "before", "after", "successor", "removed", "mechanical", "old-row"} {
		t.Run("q10-composition/row/"+field, func(t *testing.T) {
			mutant := row
			switch field {
			case "family":
				mutant.Family += "-foreign"
			case "file":
				mutant.File = "foreign.go"
			case "function":
				mutant.Function += "Foreign"
			case "before":
				mutant.Before += " "
			case "after":
				mutant.After += " "
			case "successor":
				mutant.Successor = actual
			case "removed":
				mutant.Removed = true
			case "mechanical":
				mutant.Mechanical = &mechanicalSnapshot{}
			case "old-row":
				mutant = embedded.Original
			}
			if _, err := q10EmbeddedSpecCurrentRecipe(mutant, pin); err == nil {
				t.Fatal("changed or historical full predecessor admitted as the delivered restart row")
			}
		})
	}
	mutants := map[string]string{
		"run-key":       strings.Replace(actual, "fi.run_id=es.run_id", "fi.run_id=fi.run_id", 1),
		"path-key":      strings.Replace(actual, "fi.instance_path=es.flow_instance", "fi.instance_path=fi.instance_path", 1),
		"entity-key":    strings.Replace(actual, "fi.entity_id=es.entity_id", "fi.entity_id=fi.entity_id", 1),
		"stage":         strings.Replace(actual, "fi.current_state='done'", "fi.current_state='ready'", 1),
		"exact-count":   strings.Replace(actual, "entities != 2", "entities != 1", 1),
		"construction":  strings.Replace(actual, "!reflect.DeepEqual(constructionBefore, receipts)", "!reflect.DeepEqual(receipts, receipts)", 1),
		"q10-only":      pin.Candidate,
		"embedded-only": embedded.Candidate,
		"historical":    historical,
	}
	for name, source := range mutants {
		t.Run("q10-composition/source/"+name, func(t *testing.T) {
			if source == actual {
				t.Fatal("restart composition hostile control did not change source")
			}
			if _, _, err := embeddedSpecHistoricalPair1801(row, source); err == nil {
				t.Fatal("changed membership, revision/conservation assertion or partial composition fell through")
			}
		})
	}
	for _, field := range []string{"history", "candidate"} {
		t.Run("q10-composition/pin/"+field, func(t *testing.T) {
			mutant := pin
			if field == "history" {
				mutant.Original.Before += " "
			} else {
				mutant.Candidate = strings.Replace(pin.Candidate, "entities != 2", "entities != 1", 1)
			}
			if _, err := q10EmbeddedSpecCurrentRecipe(row, mutant); err == nil {
				t.Fatal("crossed immutable history or changed Q10 current pin admitted")
			}
		})
	}
}

func assertEmbeddedSpecSuccessorRefusals1801(t *testing.T, row recipe, actual string, pin fixtureSetupSuccessor2617) {
	t.Helper()
	for _, field := range []string{"family", "file", "function", "before", "after", "mechanical", "removed", "missing-successor", "wrong-successor", "prior-endpoint", "current-source", "paired-source"} {
		mutant, source := row, actual
		switch field {
		case "family":
			mutant.Family += "-unlisted"
		case "file":
			mutant.File = "unlisted.go"
		case "function":
			mutant.Function += "Unlisted"
		case "before":
			mutant.Before += " "
		case "after":
			mutant.After += " "
		case "mechanical":
			mutant.Mechanical = &mechanicalSnapshot{}
		case "removed":
			mutant.Removed = true
		case "missing-successor":
			mutant.Successor = ""
		case "wrong-successor":
			mutant.Successor = pin.Original.After
		case "prior-endpoint":
			source = pin.Original.After
			if pin.Original.Successor != "" {
				source = pin.Original.Successor
			}
		case "current-source":
			source = strings.Replace(actual, "{", "{ unreviewedAuthority();", 1)
		case "paired-source":
			mutant.Successor = strings.Replace(row.Successor, "{", "{ unreviewedAuthority();", 1)
			source = mutant.Successor
		}
		if _, _, err := embeddedSpecPriorEndpoint1801(mutant, source); err == nil {
			t.Fatalf("%s: changed %s admitted", row.Function, field)
		}
		if sameReviewedRecipeKey(mutant, row) {
			if _, _, err := embeddedSpecHistoricalPair1801(mutant, source); err == nil {
				t.Fatalf("%s: invalid listed %s fell through to an older comparator", row.Function, field)
			}
		}
	}
	changedPin := pin
	changedPin.Original.Successor += "\nfunc foreignEndpoint() {}"
	if _, _, err := reverseEmbeddedSpecSuccessor1801(row, actual, changedPin); err == nil {
		t.Fatal("altered prior endpoint admitted")
	}
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
	for _, pin := range packet.Rows {
		t.Run(pin.Original.Function, func(t *testing.T) {
			row := pin.Original
			row.Successor = pin.Candidate
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
