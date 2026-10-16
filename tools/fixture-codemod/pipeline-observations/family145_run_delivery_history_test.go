package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/rundelivery"
)

func runDeliveryLivePins(t *testing.T) []rundelivery.Pin {
	t.Helper()
	pins, err := rundelivery.Decode(runDeliveryBytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		raw, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil {
			t.Fatal("run delivery live endpoint changed:", pin.Function, err)
		}
		previous := servedSetupEarlierSource(t, pin.File, pin.Function, requestKeyDeclaration(t, raw, pin.Function))
		if err := rundelivery.ValidateCurrentSource(pin, []byte("package proof\n"+previous)); err != nil {
			t.Fatal(err)
		}
	}
	return pins
}

func runDeliveryInvocationPins(t *testing.T, proof sourceBlobDeclarationInvocation) []rundelivery.Pin {
	t.Helper()
	pins, err := rundelivery.Decode(runDeliveryBytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		raw, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil {
			t.Fatal(err)
		}
		previous := proof.setupPrevious(t, pin.File, pin.Function, requestKeyDeclaration(t, raw, pin.Function))
		if err := rundelivery.ValidateCurrentSource(pin, []byte("package proof\n"+previous)); err != nil {
			t.Fatal(err)
		}
	}
	return pins
}

func TestFamily145RunDeliveryHistoryRefusesMissingDuplicateForeignAndChangedCurrentSource(t *testing.T) {
	for _, pin := range runDeliveryLivePins(t) {
		if err := rundelivery.ValidateCurrentSource(pin, []byte("package proof\n"+pin.After)); err != nil {
			t.Fatal(err)
		}
		for _, source := range []string{"package proof\n", "package proof\n" + pin.After + "\n" + pin.After, "package proof\n" + strings.Replace(pin.After, "{", "{foreignRead();", 1)} {
			if err := rundelivery.ValidateCurrentSource(pin, []byte(source)); err == nil {
				t.Fatal("missing/duplicate/changed live source accepted:", pin.Function)
			}
		}
		foreign := pin
		foreign.File = "foreign.go"
		if err := rundelivery.ValidateCurrentSource(foreign, []byte("package proof\n"+pin.After)); err == nil {
			t.Fatal("foreign source pin accepted")
		}
	}
}

func runDeliveryPriorHistory(t *testing.T, recipes bool) []byte {
	t.Helper()
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	raw, err := exec.Command("git", "show", rundelivery.HistoryPredecessor+":"+path).Output()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func runDeliveryEarlierHistory(t *testing.T, current []byte, recipes bool) []byte {
	t.Helper()
	current = sourceBlobEarlierHistory(t, current, recipes)
	raw, err := rundelivery.RestoreHistory(current, runDeliveryPriorHistory(t, recipes), runDeliveryLivePins(t), recipes)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestFamily145RunDeliveryHistoryRestoresCompletePriorLedgersAndCollisionSuccessor(t *testing.T) {
	proof := sourceBlobDeclarationsForInvocation(t)
	pins := runDeliveryInvocationPins(t, proof)
	for _, recipes := range []bool{true, false} {
		prior := runDeliveryPriorHistory(t, recipes)
		expected, err := rundelivery.ComposeHistory(prior, pins, recipes)
		if err != nil {
			t.Fatal(err)
		}
		current, wantCount := family145TransitionBytes, 824
		if recipes {
			current, wantCount = recipeBytes, 1519
		}
		current = proof.blobHistory(t, current, recipes)
		restoredHistory, err := rundelivery.RestoreHistory(current, prior, pins, recipes)
		if err != nil {
			t.Fatal(err)
		}
		var before, after, composed, restored []map[string]any
		for _, input := range []struct {
			raw  []byte
			rows *[]map[string]any
		}{{prior, &before}, {current, &after}, {expected, &composed}, {restoredHistory, &restored}} {
			if err := json.Unmarshal(input.raw, input.rows); err != nil {
				t.Fatal(err)
			}
		}
		if len(after) != wantCount || !reflect.DeepEqual(after, composed) || !reflect.DeepEqual(before, restored) {
			t.Fatal("run delivery changed immutable history/order/prior Successor or unrelated records")
		}
		runDeliveryHistoryHostility(t, current, prior, pins, recipes)
	}
}

func runDeliveryHistoryHostility(t *testing.T, current, prior []byte, pins []rundelivery.Pin, recipes bool) {
	t.Helper()
	rows := runDeliveryHistoryRecords(t, current, pins)
	prior, err := json.Marshal(runDeliveryHistoryRecords(t, prior, pins))
	if err != nil {
		t.Fatal(err)
	}
	for index := range rows {
		for _, field := range []string{"File", "Function", "Before", "After", "Successor", "Mechanical", "Family", "Removed"} {
			mutant := cloneRollbackHistory(t, rows)
			mutant[index][field] = "unreviewedRunDeliveryHistory"
			runDeliveryRefuseHistory(t, mutant, prior, pins, recipes)
		}
		runDeliveryRefuseHistory(t, append(cloneRollbackHistory(t, rows), rows[index]), prior, pins, recipes)
		missing := append(cloneRollbackHistory(t, rows[:index]), rows[index+1:]...)
		runDeliveryRefuseHistory(t, missing, prior, pins, recipes)
	}
}

func runDeliveryHistoryRecords(t *testing.T, raw []byte, pins []rundelivery.Pin) []map[string]any {
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

func runDeliveryRefuseHistory(t *testing.T, rows []map[string]any, prior []byte, pins []rundelivery.Pin, recipes bool) {
	t.Helper()
	raw, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rundelivery.RestoreHistory(raw, prior, pins, recipes); err == nil {
		t.Fatal("changed/missing/duplicate/foreign run delivery history accepted")
	}
}
