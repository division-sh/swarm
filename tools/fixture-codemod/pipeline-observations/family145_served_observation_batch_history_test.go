package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
)

func servedObservationBatchPins(t *testing.T) []runphysical.Pin {
	t.Helper()
	pins, err := servedsettlement.ReadBatchPins("../../..")
	if err != nil {
		t.Fatal(err)
	}
	if err := servedsettlement.ValidateBatchCurrentSource("../../..", pins); err != nil {
		t.Fatal(err)
	}
	return pins
}

func servedObservationPriorHistory(t *testing.T, recipes bool) []byte {
	t.Helper()
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	raw, err := exec.Command("git", "show", servedsettlement.BatchPredecessor+":"+path).Output()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func servedObservationEarlierHistory(t *testing.T, current []byte, recipes bool) []byte {
	t.Helper()
	current = servedSetupEarlierHistory(t, current, recipes)
	return restoreServedObservationHistory(t, current, servedObservationPriorHistory(t, recipes), servedObservationBatchPins(t), recipes)
}

func restoreServedObservationHistory(t *testing.T, current, prior []byte, pins []runphysical.Pin, recipes bool) []byte {
	t.Helper()
	raw, err := servedsettlement.RestoreBatchHistory(current, prior, pins, recipes)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func servedObservationEarlierSource(t *testing.T, file, function, current string) string {
	t.Helper()
	current = servedSetupEarlierSource(t, file, function, current)
	for _, pin := range servedObservationInventoryPins {
		if pin.File == file && pin.Function == function {
			previous, err := servedsettlement.EarlierSource(servedObservationBatchPins(t), file, function, current)
			if err != nil {
				t.Fatal(err)
			}
			return previous
		}
	}
	return current
}

func servedObservationWholeRuleMatches(t *testing.T, pins []runphysical.Pin, file, function, current string) bool {
	t.Helper()
	for _, pin := range pins {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("served batch transition escaped its complete finite stages:", function)
			}
			return true
		}
	}
	return false
}

func TestFamily145ServedObservationBatchPreservesCompleteHistoryAndEveryIntermediateLink(t *testing.T) {
	pins := servedObservationBatchPins(t)
	raw, err := json.Marshal(pins)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("served batch merged pin digest=%x", sha256.Sum256(raw))
	for _, recipes := range []bool{true, false} {
		prior := servedObservationPriorHistory(t, recipes)
		expected, err := servedsettlement.ComposeBatchHistory(prior, pins, recipes)
		if err != nil {
			t.Fatal(err)
		}
		current, count := family145TransitionBytes, 837
		if recipes {
			current, count = recipeBytes, 1532
		}
		current = servedSetupEarlierHistory(t, current, recipes)
		var rows, composed, old, restored []map[string]any
		for _, input := range []struct {
			raw []byte
			out *[]map[string]any
		}{{current, &rows}, {expected, &composed}, {prior, &old}, {restoreServedObservationHistory(t, current, prior, pins, recipes), &restored}} {
			if err := json.Unmarshal(input.raw, input.out); err != nil {
				t.Fatal(err)
			}
		}
		if len(rows) != count || !reflect.DeepEqual(rows, composed) || !reflect.DeepEqual(old, restored) {
			t.Fatal("served batch lost complete historical fields, prior Successors or unrelated order")
		}
		servedObservationHistoryHostility(t, prior, expected, pins, recipes)
	}
}

func servedObservationHistoryHostility(t *testing.T, prior, current []byte, pins []runphysical.Pin, recipes bool) {
	t.Helper()
	var rows, old []map[string]any
	if err := json.Unmarshal(current, &rows); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(prior, &old); err != nil {
		t.Fatal(err)
	}
	// Full-ledger conservation is checked above. Field mutations need only the
	// entire finite 48-row family, not thousands of unchanged unrelated rows.
	finiteRows := servedObservationHistoryRows(pins, rows)
	finitePrior, err := json.Marshal(servedObservationHistoryRows(pins, old))
	if err != nil {
		t.Fatal(err)
	}
	for index, row := range finiteRows {
		for _, field := range []string{"File", "Function", "Before", "After", "Successor", "Mechanical", "Family", "Removed"} {
			mutant := cloneRollbackHistory(t, finiteRows)
			mutant[index][field] = "foreignServedBatchHistory"
			refuseServedObservationHistory(t, mutant, finitePrior, pins, recipes)
		}
		refuseServedObservationHistory(t, append(cloneRollbackHistory(t, finiteRows), row), finitePrior, pins, recipes)
		missing := append(cloneRollbackHistory(t, finiteRows[:index]), finiteRows[index+1:]...)
		refuseServedObservationHistory(t, missing, finitePrior, pins, recipes)
	}
	mutant := cloneRollbackHistory(t, rows)
	mutant[0], mutant[1] = mutant[1], mutant[0]
	refuseServedObservationHistory(t, mutant, prior, pins, recipes)
	mutant = cloneRollbackHistory(t, rows)
	mutant[0]["Before"] = "unrelatedChangedHistory"
	refuseServedObservationHistory(t, mutant, prior, pins, recipes)
}

func servedObservationHistoryRows(pins []runphysical.Pin, rows []map[string]any) []map[string]any {
	var filtered []map[string]any
	for _, row := range rows {
		if servedObservationPinnedRow(pins, row) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func servedObservationPinnedRow(pins []runphysical.Pin, row map[string]any) bool {
	for _, pin := range pins {
		if row["File"] == pin.File && row["Function"] == pin.Function {
			return true
		}
	}
	return false
}

func refuseServedObservationHistory(t *testing.T, rows []map[string]any, prior []byte, pins []runphysical.Pin, recipes bool) {
	t.Helper()
	raw, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := servedsettlement.RestoreBatchHistory(raw, prior, pins, recipes); err == nil {
		t.Fatal("served batch restoration erased mutated/missing/duplicate/foreign history")
	}
}

func TestFamily145ServedObservationBatchCurrentnessRefusesMissingDuplicateForeignAndStaleSources(t *testing.T) {
	pins := servedObservationBatchPins(t)
	for _, pin := range pins {
		for _, source := range []string{"", pin.After + "\n" + pin.After, strings.Replace(pin.After, "{", "{foreignSource();", 1), pin.Before} {
			body, err := servedsettlement.UniqueBody([]byte("package proof\n"+source), pin.Function)
			if err == nil && body == pin.After {
				t.Fatal("changed/missing/duplicate/stale current source accepted:", pin.Function)
			}
		}
		if _, err := servedsettlement.EarlierSource(pins, pin.File, pin.Function, pin.Before); err == nil {
			t.Fatal("supplied old source substituted for final live source:", pin.Function)
		}
		if got, err := servedsettlement.EarlierSource(pins, "foreign.go", pin.Function, pin.After); err != nil || got != pin.After {
			t.Fatal("foreign declaration received a batch inverse:", pin.Function, err)
		}
		actual, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil || servedSetupEarlierSource(t, pin.File, pin.Function, requestKeyDeclaration(t, actual, pin.Function)) != pin.After {
			t.Fatal("actual source changed:", pin.Function, err)
		}
	}
	for index, raw := range [][]byte{globalEventNameBytes, servedSettlementBytes, servedPublicationLookupBytes} {
		inputs := [][]byte{globalEventNameBytes, servedSettlementBytes, servedPublicationLookupBytes}
		inputs[index] = append(append([]byte{}, raw...), ' ')
		if _, err := servedsettlement.BatchPins(inputs[0], inputs[1], inputs[2]); err == nil {
			t.Fatal("changed immutable stage manifest accepted:", fmt.Sprint(index))
		}
	}
}
