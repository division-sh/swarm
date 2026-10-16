package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func Test1745StateSpellingSuccessorsPreserveHistoricalRecipes(t *testing.T) {
	if fmt.Sprintf("%x", sha256.Sum256(recipeBytes)) != "4fd3948cf98e828528dd5681e1f93cf79102ac5a0887573a65893e27dd531c70" {
		t.Fatal("#1745 must append evidence, never change a historical recipe")
	}
	if fmt.Sprintf("%x", sha256.Sum256(stateSpellingSuccessors1745)) != "9513198ab2f0c83f25801ef5a8544005f1bb4be512281d8a96d232c578b984b3" {
		t.Fatal("finite source spelling evidence changed; regenerate and review its exact diff")
	}
	var pins []stateSpellingSuccessor1745
	if err := json.Unmarshal(stateSpellingSuccessors1745, &pins); err != nil || len(pins) != 26 {
		t.Fatalf("wrong source spelling successor set: count=%d err=%v", len(pins), err)
	}
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, pin := range pins {
		row := pin.Original
		key := row.File + "/" + row.Function + "/" + row.Family
		if seen[key] {
			t.Fatalf("duplicate source spelling successor %s", key)
		}
		seen[key] = true
		found := 0
		for _, original := range rows {
			if reflect.DeepEqual(original, row) {
				found++
			}
		}
		if found != 1 {
			t.Fatalf("missing or changed historical source %s", key)
		}
		prior := row.Successor
		if prior == "" {
			prior = row.After
		}
		spelling := strings.ReplaceAll(prior, "entities.yaml", "state.yaml")
		spelling = strings.ReplaceAll(spelling, `\n  key:`, `\n  _key:`)
		spelling = strings.ReplaceAll(spelling, "\n  key:", "\n  _key:")
		want, err := canonicalFunction(spelling)
		candidate, candidateErr := canonicalFunction(pin.Candidate)
		if err != nil || candidateErr != nil || want != candidate || canonicalComparatorEqual2617(prior, candidate) {
			t.Fatalf("successor changes more than authored spellings: %s", key)
		}
		set := token.NewFileSet()
		source, err := os.ReadFile(filepath.Join("../../..", row.File))
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(set, row.File, source, 0)
		if err != nil {
			t.Fatal(err)
		}
		fn, err := uniqueRecipeFunction(file, row)
		if err != nil {
			t.Fatal(err)
		}
		actual := string(source[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset])
		if !canonicalComparatorEqual2617(actual, candidate) {
			t.Fatalf("current source diverged: %s", key)
		}
		if got := stateSpellingHistoricalBody1745(row.File, row.Function, actual); got != prior {
			t.Fatalf("lost exact historical endpoint: %s", key)
		}
		files, edits, err := prepareFiles("../../..", []recipe{row})
		if err != nil || len(files) != 0 || len(edits) != 0 {
			t.Fatalf("current source is not idempotent: %s %v", key, err)
		}
		for _, field := range []string{"before", "after", "successor", "family"} {
			changed := row
			switch field {
			case "before":
				changed.Before += "\n// changed history"
			case "after":
				changed.After += "\n// changed history"
			case "successor":
				changed.Successor += "\n// changed history"
			case "family":
				changed.Family += "-other"
			}
			if _, err := stateSpellingEndpoint1745(changed); field != "family" && err == nil {
				t.Fatalf("changed historical %s accepted: %s", field, key)
			}
		}
		changedSource := strings.Replace(actual, "state.yaml", "other.yaml", 1)
		if changedSource == actual {
			changedSource = strings.Replace(actual, "_key:", "other:", 1)
		}
		if changedSource == actual || stateSpellingHistoricalBody1745(row.File, row.Function, changedSource) != changedSource {
			t.Fatalf("different current spelling accepted: %s", key)
		}
	}
}
