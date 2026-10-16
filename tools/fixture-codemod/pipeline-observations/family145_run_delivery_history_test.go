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

func (proof sourceBlobDeclarationInvocation) deliveryHistory(t *testing.T, current []byte, recipes bool) []byte {
	t.Helper()
	current = proof.blobHistory(t, current, recipes)
	raw, err := rundelivery.RestoreHistory(current, runDeliveryPriorHistory(t, recipes), runDeliveryInvocationPins(t, proof), recipes)
	if err != nil {
		t.Fatal(err)
	}
	return raw
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
