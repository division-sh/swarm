package main

import (
	"encoding/json"
	"testing"
)

// Diagnostic only: enumerate every stale endpoint instead of stopping at the
// first file in the whole-family guard. This admits or rewrites no source.
func TestQ10RemainingRecipeEndpointDiagnostic(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	stale := 0
	for _, row := range rows {
		files, changes, err := prepareFiles("../../..", []recipe{row})
		if err != nil || len(files) != 0 || len(changes) != 0 {
			stale++
			t.Logf("Q10_ENDPOINT file=%s function=%s family=%s files=%d changes=%d err=%v", row.File, row.Function, row.Family, len(files), len(changes), err)
		}
	}
	t.Logf("Q10_ENDPOINT_TOTAL stale=%d recipes=%d", stale, len(rows))
}
