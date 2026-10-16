package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/sourcefork"
)

func sourceForkInvocationPins(t *testing.T, proof sourceBlobDeclarationInvocation) []sourcefork.Pin {
	t.Helper()
	pins := sourceForkPins(t)
	for _, pin := range pins {
		raw, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err == nil {
			raw, err = selectedreadrole.CatchUp2542HistoricalSource("../../..", pin.File, raw)
		}
		if err != nil || proof.observationPrevious(t, pin.File, pin.Function, requestKeyDeclaration(t, raw, pin.Function)) != pin.After {
			t.Fatal("source fork live endpoint changed:", pin.Function, err)
		}
	}
	return pins
}

func sourceForkPriorHistory(t *testing.T, recipes bool) []byte {
	t.Helper()
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	raw, err := exec.Command("git", "show", sourcefork.HistoryPredecessor+":"+path).Output()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func restoreSourceForkHistory(t *testing.T, rows []map[string]any, recipes bool) []map[string]any {
	t.Helper()
	proof := sourceBlobDeclarationsForInvocation(t)
	raw, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	raw = proof.deliveryHistory(t, raw, recipes)
	raw, err = sourcefork.RestoreHistory(raw, sourceForkPriorHistory(t, recipes), sourceForkInvocationPins(t, proof), recipes)
	if err != nil {
		t.Fatal(err)
	}
	rows = nil
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestFamily145SourceForkHistoryRestoresCompletePriorLedgersAndTimerSuccessor(t *testing.T) {
	proof := sourceBlobDeclarationsForInvocation(t)
	pins := sourceForkInvocationPins(t, proof)
	for _, recipes := range []bool{true, false} {
		prior := sourceForkPriorHistory(t, recipes)
		expected, err := sourcefork.ComposeHistory(prior, pins, recipes)
		if err != nil {
			t.Fatal(err)
		}
		current := family145TransitionBytes
		wantCount := 822
		if recipes {
			current, wantCount = recipeBytes, 1517
		}
		current = proof.deliveryHistory(t, current, recipes)
		var before, after, composed []map[string]any
		for _, input := range []struct {
			raw  []byte
			rows *[]map[string]any
		}{{prior, &before}, {current, &after}, {expected, &composed}} {
			if err := json.Unmarshal(input.raw, input.rows); err != nil {
				t.Fatal(err)
			}
		}
		if len(after) != wantCount || !reflect.DeepEqual(after, composed) {
			t.Fatal("source fork composition changed records/count/order")
		}
		restoredBytes, err := sourcefork.RestoreHistory(current, prior, pins, recipes)
		if err != nil {
			t.Fatal(err)
		}
		var restored []map[string]any
		if err := json.Unmarshal(restoredBytes, &restored); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, restored) {
			t.Fatal("source fork lost complete immutable history or prior timer Successor")
		}
		sourceForkHistoryHostility(t, current, prior, pins, recipes)
	}
}

func sourceForkHistoryHostility(t *testing.T, current, prior []byte, pins []sourcefork.Pin, recipes bool) {
	t.Helper()
	// The complete-ledger conservation check above stays whole. Mutate the six
	// affected records here, rather than copying thousands of unrelated rows for
	// each hostile field; the same finite inverse validates both inputs.
	rows := sourceForkHistoryRecords(t, current, pins)
	prior, err := json.Marshal(sourceForkHistoryRecords(t, prior, pins))
	if err != nil {
		t.Fatal(err)
	}
	for index, row := range rows {
		for _, pin := range pins {
			if row["File"] != pin.File || row["Function"] != pin.Function {
				continue
			}
			for _, field := range []string{"File", "Function", "Before", "After", "Successor", "Mechanical", "Family", "Removed"} {
				mutant := cloneRollbackHistory(t, rows)
				mutant[index][field] = "unreviewedSourceForkHistory"
				sourceForkRefuseHistory(t, mutant, prior, pins, recipes)
			}
			sourceForkRefuseHistory(t, append(cloneRollbackHistory(t, rows), row), prior, pins, recipes)
			missing := append(cloneRollbackHistory(t, rows[:index]), rows[index+1:]...)
			sourceForkRefuseHistory(t, missing, prior, pins, recipes)
		}
	}
}

func sourceForkHistoryRecords(t *testing.T, raw []byte, pins []sourcefork.Pin) []map[string]any {
	t.Helper()
	var rows, result []map[string]any
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		for _, pin := range pins {
			if row["File"] == pin.File && row["Function"] == pin.Function {
				result = append(result, row)
			}
		}
	}
	return result
}

func sourceForkRefuseHistory(t *testing.T, mutant []map[string]any, prior []byte, pins []sourcefork.Pin, recipes bool) {
	t.Helper()
	raw, err := json.Marshal(mutant)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sourcefork.RestoreHistory(raw, prior, pins, recipes); err == nil {
		t.Fatal("foreign/missing/duplicate/changed source fork history accepted")
	}
}
