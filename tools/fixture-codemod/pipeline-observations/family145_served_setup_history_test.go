package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelsettlement"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
)

func servedSetupComparisonDeclaration(t *testing.T, pins []runphysical.Pin, path string, fn *ast.FuncDecl) *ast.FuncDecl {
	t.Helper()
	current := requestkey.Node(fn)
	actual := current
	var err error
	current, err = channelsettlement.EarlierSource("../../..", path, fn.Name.Name, current)
	if err != nil {
		t.Fatal(err)
	}
	prior, err := servedsettlement.EarlierSource(pins, path, fn.Name.Name, current)
	if err != nil {
		t.Fatal(err)
	}
	if actual == prior {
		return fn
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, "package proof\n"+prior, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		t.Fatal("setup comparison changed declaration cardinality:", err)
	}
	previous, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok || previous.Name.Name != fn.Name.Name {
		t.Fatal("setup comparison changed declaration identity")
	}
	return previous
}

func TestFamily145ServedSetupEveryComparisonHasItsExactSignedPredecessor(t *testing.T) {
	files := map[string][]byte{}
	for _, pin := range servedSetupPins(t) {
		if files[pin.File] == nil {
			raw, err := exec.Command("git", "show", servedsettlement.SetupPredecessor+":"+pin.File).Output()
			if err != nil {
				t.Fatal(err)
			}
			files[pin.File] = raw
		}
		before, err := servedsettlement.UniqueBody(files[pin.File], pin.Function)
		if err != nil || before != pin.Before {
			t.Fatal("setup comparison does not restore its complete signed source:", pin.Function, err)
		}
	}
}

func TestFamily145ServedSetupHistoryPreservesAllFieldsOrderAndExactAdvances(t *testing.T) {
	pins := servedSetupPins(t)
	for _, recipes := range []bool{false, true} {
		prior := servedSetupPriorHistory(t, recipes)
		current, count := family145TransitionBytes, 871
		if recipes {
			current, count = recipeBytes, 1564
		}
		restored := servedSetupEarlierHistory(t, current, recipes)
		if !reflect.DeepEqual(restored, prior) {
			t.Fatal("setup history lost original bytes, metadata or order")
		}
		current, err := channelsettlement.EarlierHistory("../../..", current, recipes)
		if err != nil {
			t.Fatal(err)
		}
		var rows []map[string]any
		if err := json.Unmarshal(current, &rows); err != nil || len(rows) != count {
			t.Fatal("setup complete history inventory changed:", err)
		}
		for _, field := range []string{"File", "Function", "Before", "After", "Successor", "Mechanical", "Family", "Removed"} {
			mutant := cloneRollbackHistory(t, rows)
			mutant[0][field] = "foreignSetupHistory"
			refuseServedSetupHistory(t, mutant, prior, pins, recipes)
		}
		for _, pin := range pins {
			for index, row := range rows {
				if row["File"] != pin.File || row["Function"] != pin.Function {
					continue
				}
				mutant := cloneRollbackHistory(t, rows)
				mutant[index]["After"] = "foreignSetupEndpoint"
				refuseServedSetupHistory(t, mutant, prior, pins, recipes)
			}
		}
		refuseServedSetupHistory(t, rows[:len(rows)-1], prior, pins, recipes)
		refuseServedSetupHistory(t, append(rows, rows[0]), prior, pins, recipes)
		rows[0], rows[1] = rows[1], rows[0]
		refuseServedSetupHistory(t, rows, prior, pins, recipes)
	}
}

func refuseServedSetupHistory(t *testing.T, rows []map[string]any, prior []byte, pins []runphysical.Pin, recipes bool) {
	t.Helper()
	raw, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := servedsettlement.RestoreSetupHistory(raw, prior, pins, recipes); err == nil {
		t.Fatal("setup comparison erased changed/missing/duplicate/foreign history")
	}
}

func TestFamily145ServedSetupCurrentnessRejectsEveryStaleDuplicateOrForeignBody(t *testing.T) {
	pins := servedSetupPins(t)
	for _, pin := range pins {
		for _, source := range []string{"", pin.Before, pin.After + "\n" + pin.After, strings.Replace(pin.After, "{", "{foreignSetup();", 1)} {
			body, err := servedsettlement.UniqueBody([]byte("package proof\n"+source), pin.Function)
			if err == nil && body == pin.After {
				t.Fatal("invalid setup current source accepted:", pin.Function)
			}
		}
		if _, err := servedsettlement.EarlierSource(pins, pin.File, pin.Function, pin.Before); err == nil {
			t.Fatal("historical input substituted for final current source")
		}
		if got, err := servedsettlement.EarlierSource(pins, "foreign.go", pin.Function, pin.After); err != nil || got != pin.After {
			t.Fatal("foreign setup declaration received an inverse:", err)
		}
	}
	for _, path := range []string{servedsettlement.SetupOwnerPath, servedsettlement.SetupRolePath} {
		root := t.TempDir()
		for _, input := range []string{servedsettlement.SetupOwnerPath, servedsettlement.SetupRolePath} {
			raw, err := os.ReadFile(filepath.Join("../../..", input))
			if err != nil {
				t.Fatal(err)
			}
			if input == path {
				raw = append(raw, ' ')
			}
			if err := os.MkdirAll(filepath.Dir(filepath.Join(root, input)), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, input), raw, 0644); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := servedsettlement.ReadSetupPins(root); err == nil {
			t.Fatal("changed immutable setup stage accepted")
		}
	}
}

func servedSetupPins(t *testing.T) []runphysical.Pin {
	t.Helper()
	pins, err := servedsettlement.ReadSetupPins("../../..")
	if err != nil {
		t.Fatal(err)
	}
	if err := servedsettlement.ValidateSetupCurrentSource("../../..", pins); err != nil {
		t.Fatal(err)
	}
	return pins
}

func servedSetupPriorHistory(t *testing.T, recipes bool) []byte {
	t.Helper()
	path := "tools/fixture-codemod/pipeline-observations/family145-transitions.json"
	if recipes {
		path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	}
	raw, err := exec.Command("git", "show", servedsettlement.SetupPredecessor+":"+path).Output()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func servedSetupEarlierHistory(t *testing.T, current []byte, recipes bool) []byte {
	t.Helper()
	var err error
	current, err = channelsettlement.EarlierHistory("../../..", current, recipes)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := servedsettlement.RestoreSetupHistory(current, servedSetupPriorHistory(t, recipes), servedSetupPins(t), recipes)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func servedSetupEarlierSource(t *testing.T, file, function, current string) string {
	t.Helper()
	var err error
	current, err = channelsettlement.EarlierSource("../../..", file, function, current)
	if err != nil {
		t.Fatal(err)
	}
	pins, err := servedsettlement.ReadSetupPins("../../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		if pin.File != file || pin.Function != function {
			continue
		}
		if err := servedsettlement.ValidateSetupCurrentSource("../../..", pins); err != nil {
			t.Fatal(err)
		}
		prior, err := servedsettlement.EarlierSource(pins, file, function, current)
		if err != nil {
			t.Fatal(err)
		}
		return prior
	}
	return current
}
