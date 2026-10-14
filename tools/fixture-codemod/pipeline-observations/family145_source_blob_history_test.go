package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/sourceblob"
)

func sourceBlobLivePins(t *testing.T) []sourceblob.Pin {
	t.Helper()
	pins, err := sourceblob.Decode(sourceBlobBytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		raw, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil {
			t.Fatal("source blob live endpoint changed:", pin.Function, err)
		}
		current := servedObservationEarlierSource(t, pin.File, pin.Function, requestKeyDeclaration(t, raw, pin.Function))
		if err := sourceblob.ValidateCurrentSource(pin, []byte("package proof\n"+current)); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile("../../../internal/serveapp/main_runtime_test.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceblob.ValidateFixtureRole(raw); err != nil {
		t.Fatal(err)
	}
	return pins
}

func TestFamily145SourceBlobHistoryRefusesMissingDuplicateForeignAndChangedCurrentSource(t *testing.T) {
	for _, pin := range sourceBlobLivePins(t) {
		if err := sourceblob.ValidateCurrentSource(pin, []byte("package proof\n"+pin.After)); err != nil {
			t.Fatal(err)
		}
		for _, source := range []string{"package proof\n", "package proof\n" + pin.After + "\n" + pin.After, "package proof\n" + strings.Replace(pin.After, "{", "{foreignRead();", 1)} {
			if err := sourceblob.ValidateCurrentSource(pin, []byte(source)); err == nil {
				t.Fatal("missing/duplicate/changed live source accepted:", pin.Function)
			}
		}
		foreign := pin
		foreign.File = "foreign.go"
		if err := sourceblob.ValidateCurrentSource(foreign, []byte("package proof\n"+pin.After)); err == nil {
			t.Fatal("foreign source pin accepted")
		}
	}
	raw, err := os.ReadFile("../../../internal/serveapp/main_runtime_test.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"SourceArtifacts", "sourceArtifactReader", "servedControlProofRuntime"} {
		mutant := strings.ReplaceAll(string(raw), role, "foreignSourceBlobRole")
		if mutant == string(raw) || sourceblob.ValidateFixtureRole([]byte(mutant)) == nil {
			t.Fatal("changed/missing fixture role/type accepted:", role)
		}
	}
}

func sourceBlobPriorHistory(t *testing.T, recipes bool) []byte {
	t.Helper()
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	raw, err := exec.Command("git", "show", sourceblob.Predecessor+":"+path).Output()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func sourceBlobEarlierHistory(t *testing.T, current []byte, recipes bool) []byte {
	t.Helper()
	current = servedObservationEarlierHistory(t, current, recipes)
	raw, err := sourceblob.RestoreHistory(current, sourceBlobPriorHistory(t, recipes), sourceBlobLivePins(t), recipes)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func family145CheckSourceBlobWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	pins, err := sourceblob.Decode(sourceBlobBytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		if pin.File == file && pin.Function == function {
			sourceBlobLivePins(t)
			if current != pin.After {
				t.Fatal("source blob current transition escaped its exact rule:", function)
			}
			return true
		}
	}
	return false
}

func sourceBlobPreviousDeclaration(t *testing.T, file, function, current string) string {
	t.Helper()
	current = servedObservationEarlierSource(t, file, function, current)
	for _, pin := range sourceBlobInventoryPins {
		if pin.File == file && pin.Function == function {
			sourceBlobLivePins(t)
			if current != pin.After {
				t.Fatal("source blob exact current declaration changed:", function)
			}
			return pin.Before
		}
	}
	return current
}

func sourceBlobPinnedInventory() []sourceblob.Pin {
	pins, err := sourceblob.Decode(sourceBlobBytes)
	if err != nil {
		panic(err)
	}
	return pins
}

var sourceBlobInventoryPins = sourceBlobPinnedInventory()

var servedObservationInventoryPins = servedObservationPinnedInventory()

func servedObservationPinnedInventory() []runphysical.Pin {
	pins, err := servedsettlement.ReadBatchPins("../../..")
	if err != nil {
		panic(err)
	}
	return pins
}

func sourceBlobEarlierInventoryEndpoint(file, function, current string) string {
	var err error
	current, err = servedsettlement.EarlierSource(servedObservationInventoryPins, file, function, current)
	if err != nil {
		panic(err)
	}
	for _, pin := range sourceBlobInventoryPins {
		if pin.File == file && pin.Function == function && pin.After == current {
			return pin.Before
		}
	}
	return current
}

func TestFamily145SourceBlobHistoryRestoresCompletePriorLedgersAndAllThreePriorSuccessors(t *testing.T) {
	pins := sourceBlobLivePins(t)
	for _, recipes := range []bool{true, false} {
		prior := sourceBlobPriorHistory(t, recipes)
		expected, err := sourceblob.ComposeHistory(prior, pins, recipes)
		if err != nil {
			t.Fatal(err)
		}
		current, wantCount := family145TransitionBytes, 825
		if recipes {
			current, wantCount = recipeBytes, 1520
		}
		live := current
		current = servedObservationEarlierHistory(t, current, recipes)
		var before, after, composed, restored []map[string]any
		for _, input := range []struct {
			raw  []byte
			rows *[]map[string]any
		}{{prior, &before}, {current, &after}, {expected, &composed}, {sourceBlobEarlierHistory(t, live, recipes), &restored}} {
			if err := json.Unmarshal(input.raw, input.rows); err != nil {
				t.Fatal(err)
			}
		}
		if len(after) != wantCount || !reflect.DeepEqual(after, composed) || !reflect.DeepEqual(before, restored) {
			t.Fatal("source blob changed immutable history/order/prior Successor or unrelated records")
		}
		sourceBlobHistoryHostility(t, current, prior, pins, recipes)
	}
}

func sourceBlobHistoryHostility(t *testing.T, current, prior []byte, pins []sourceblob.Pin, recipes bool) {
	t.Helper()
	rows := sourceBlobHistoryRecords(t, current, pins)
	prior, err := json.Marshal(sourceBlobHistoryRecords(t, prior, pins))
	if err != nil {
		t.Fatal(err)
	}
	for index := range rows {
		for _, field := range []string{"File", "Function", "Before", "After", "Successor", "Mechanical", "Family", "Removed"} {
			mutant := cloneRollbackHistory(t, rows)
			mutant[index][field] = "unreviewedSourceBlobHistory"
			sourceBlobRefuseHistory(t, mutant, prior, pins, recipes)
		}
		sourceBlobRefuseHistory(t, append(cloneRollbackHistory(t, rows), rows[index]), prior, pins, recipes)
		missing := append(cloneRollbackHistory(t, rows[:index]), rows[index+1:]...)
		sourceBlobRefuseHistory(t, missing, prior, pins, recipes)
	}
}

func sourceBlobHistoryRecords(t *testing.T, raw []byte, pins []sourceblob.Pin) []map[string]any {
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

func sourceBlobRefuseHistory(t *testing.T, rows []map[string]any, prior []byte, pins []sourceblob.Pin, recipes bool) {
	t.Helper()
	raw, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sourceblob.RestoreHistory(raw, prior, pins, recipes); err == nil {
		t.Fatal("changed/missing/duplicate/foreign source blob history accepted")
	}
}
