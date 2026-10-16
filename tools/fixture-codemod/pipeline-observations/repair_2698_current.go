package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"reflect"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

// Adapt only an authenticated current source for in-memory idempotence/type
// checking. Immutable recipe bytes and every historical endpoint stay intact.
func repair2698CurrentRecipes(root, path string, source []byte, rows []recipe) ([]recipe, error) {
	prior, err := selectedreadrole.Repair2698HistoricalSource(root, path, source)
	if err != nil || bytes.Equal(prior, source) {
		return rows, err
	}
	var originals []recipe
	if err := json.Unmarshal(recipeBytes, &originals); err != nil {
		return nil, err
	}
	var historical []recipe
	for _, row := range originals {
		if row.File == path {
			historical = append(historical, row)
		}
	}
	historical, err = checkpointRecipes(prior, historical)
	if err != nil {
		return nil, err
	}
	immutable := map[string][]recipe{}
	for _, row := range historical {
		immutable[row.Function] = append(immutable[row.Function], row)
	}
	set := token.NewFileSet()
	oldFile, err := parser.ParseFile(set, path, prior, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	currentSet := token.NewFileSet()
	currentFile, err := parser.ParseFile(currentSet, path, source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	current := append([]recipe(nil), rows...)
	for index, row := range current {
		if !consumeRepair2698Recipe(immutable, row) {
			return nil, fmt.Errorf("native repair recipe changed history: %s/%s", path, row.Function)
		}
		if row.Removed {
			continue
		}
		fn, err := uniqueRecipeFunction(oldFile, row)
		if err != nil {
			return nil, err
		}
		before, err := canonicalFunction(string(prior[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset]))
		endpoint := row.Successor
		if endpoint == "" {
			endpoint = row.After
		}
		expected, parseErr := canonicalFunction(endpoint)
		if err != nil || parseErr != nil || before != expected {
			return nil, fmt.Errorf("native repair is not its exact prior recipe endpoint: %s/%s", path, row.Function)
		}
		actualFunction, err := uniqueRecipeFunction(currentFile, row)
		if err != nil {
			return nil, err
		}
		actual, err := canonicalFunction(string(source[currentSet.Position(actualFunction.Pos()).Offset:currentSet.Position(actualFunction.End()).Offset]))
		if err != nil {
			return nil, err
		}
		if row.Successor != "" {
			current[index].Successor = actual
		} else {
			current[index].After = actual
		}
	}
	return current, nil
}

// Method names can repeat under different exact receivers. Compare the complete
// receipt and consume one occurrence rather than collapsing them by name.
func consumeRepair2698Recipe(rows map[string][]recipe, row recipe) bool {
	for index, original := range rows[row.Function] {
		if reflect.DeepEqual(original, row) {
			rows[row.Function] = append(rows[row.Function][:index], rows[row.Function][index+1:]...)
			return true
		}
	}
	return false
}
