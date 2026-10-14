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

// Append-only fixture evidence under reviewer-c6160153832; prior source
// Source checkpoint daea24400 retains eleven reviewed fixture endpoints;
// checkpoint211293506 adds the exact admitted-parent barrier setup and
// checkpoint4b2c02e90 retains the admitted count-join source through recovery.
// reviewer-c6163224137 adds four exact compiled-mode gate endpoints.
// reviewer-c6164135426 adds the five explicit-parent join endpoints; the
// three admitted-source gate repairs append to their prior Mode receipts.
// reviewer-c6164473807 retains the two exact recovery-port successors; its
// separate dead-letter timing RED remains unqualified.
//
//go:embed testdata/fixture_setup_successors_2617.json
var fixtureSetupSuccessorBytes2617 []byte

type fixtureSetupSuccessor2617 struct {
	Original  recipe
	Candidate string
	Edits     []reviewedSuccessorEdit
}

func TestFixtureSetupSuccessors2617PreservePriorHistoryAndExactCurrentOwners(t *testing.T) {
	if fmt.Sprintf("%x", sha256.Sum256(fixtureSetupSuccessorBytes2617)) != "7164139f6a316f47c0f2bb6752a015417796a9740e1f650437b9ccea1236bb2c" {
		t.Fatal("append-only fixture setup evidence changed")
	}
	var pins []fixtureSetupSuccessor2617
	var rows []recipe
	if err := json.Unmarshal(fixtureSetupSuccessorBytes2617, &pins); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	if len(pins) != 27 {
		t.Fatal("fixture successor row count changed")
	}
	for index, pin := range pins {
		t.Run(pin.Original.Function, func(t *testing.T) {
			latest := fixtureSetupLatestSuccessor2617(t, pins, index)
			var matches []recipe
			for _, row := range rows {
				if sameReviewedRecipeKey(row, pin.Original) {
					matches = append(matches, row)
				}
			}
			if len(matches) != 1 {
				t.Fatalf("fixture successor multiplicity=%d", len(matches))
			}
			row := matches[0]
			actual := selectedCausalObservationBody(t, row.File, projectionShapeFunction(t, latest.Candidate).Name.Name)
			expected, err := canonicalFunction(latest.Candidate)
			if err != nil {
				t.Fatal(err)
			}
			valid := func(candidate recipe, source string) bool {
				history := candidate
				history.Successor = pin.Original.Successor
				current, sourceErr := canonicalFunction(source)
				endpoint, endpointErr := canonicalFunction(candidate.Successor)
				return reflect.DeepEqual(history, pin.Original) && sourceErr == nil && endpointErr == nil &&
					current == expected && endpoint == expected
			}
			if !valid(row, actual) {
				t.Fatal("current fixture source/Successor differs from exact settled endpoint")
			}
			prior := pin.Original.Successor
			if prior == "" {
				prior = pin.Original.After
			}
			reversed, err := reverseReviewedSuccessor2617(pin.Candidate, pin.Edits)
			want, wantErr := canonicalFunction(prior)
			got, gotErr := canonicalFunction(reversed)
			if err != nil || wantErr != nil || gotErr != nil || want != got {
				t.Fatalf("fixture setup reversal failed: %v", err)
			}
			if files, changes, err := prepareFiles("../../..", []recipe{row}); err != nil || len(files) != 0 || len(changes) != 0 {
				t.Fatalf("current fixture endpoint is not inert: %v", err)
			}
			for _, field := range []string{"history", "key", "missing", "body"} {
				mutant := row
				source := actual
				switch field {
				case "history":
					mutant.After += " changed"
				case "key":
					mutant.Function += "Unlisted"
				case "missing":
					mutant.Successor = ""
				case "body":
					source = strings.Replace(actual, "{", "{ unreviewedSetup();", 1)
					mutant.Successor = source
				}
				if valid(mutant, source) {
					t.Fatalf("hostile fixture source/Successor pair accepted: %s", field)
				}
			}
		})
	}
}

func fixtureSetupLatestSuccessor2617(t *testing.T, pins []fixtureSetupSuccessor2617, index int) fixtureSetupSuccessor2617 {
	t.Helper()
	latest := pins[index]
	for _, next := range pins[index+1:] {
		if !sameReviewedRecipeKey(next.Original, latest.Original) {
			continue
		}
		original := next.Original
		original.Successor = latest.Original.Successor
		if !reflect.DeepEqual(original, latest.Original) || next.Original.Successor != latest.Candidate {
			t.Fatal("finite fixture receipt chain crossed or rewrote its prior endpoint")
		}
		latest = next
	}
	return latest
}
