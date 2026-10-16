package main

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	immutable := map[string]recipe{}
	for _, row := range originals {
		if row.File == path {
			immutable[row.Function] = row
		}
	}
	current := append([]recipe(nil), rows...)
	for index, row := range current {
		if !reflect.DeepEqual(immutable[row.Function], row) {
			return nil, fmt.Errorf("native repair recipe changed history: %s/%s", path, row.Function)
		}
		if row.Removed {
			continue
		}
		before, err := selectedreadrole.Body(prior, row.Function)
		endpoint := row.Successor
		if endpoint == "" {
			endpoint = row.After
		}
		expected, parseErr := canonicalFunction(endpoint)
		if err != nil || parseErr != nil || before != expected {
			return nil, fmt.Errorf("native repair is not its exact prior recipe endpoint: %s/%s", path, row.Function)
		}
		actual, err := selectedreadrole.Body(source, row.Function)
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
