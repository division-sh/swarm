package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

const forkProofTruthFile = "internal/serveapp/issue2277_audit_probe_test.go"
const forkProofTruthOldName = "TestAudit2277ForkCompletionLoss"
const forkProofTruthNewName = "TestAudit2277ForkDurableResultBypassesGenericCompletionBothStores"

var forkProofTruthEdits = []struct{ before, after string }{
	{"func " + forkProofTruthOldName + "(t *testing.T)", "func " + forkProofTruthNewName + "(t *testing.T)"},
	{`t.Logf("AFTER LOSS response=%+v error=%+v forks=%d completions=%d", first, first.Error, forks, completions)`, `t.Logf("GENERIC COMPLETION TRIPWIRE response=%+v error=%+v forks=%d completions=%d", first, first.Error, forks, completions)`},
	{`t.Fatal("durable fork result depended on expiring API completion")`, `t.Fatal("durable fork result depended on forbidden generic completion authority")`},
	{`t.Fatal("selected execution did not finish before injected completion loss")`, `t.Fatal("selected execution did not finish before the first response")`},
}

func validateForkProofTruthSuccessor(row recipe, candidate string) error {
	expected, restored := row.After, candidate
	for _, edit := range forkProofTruthEdits {
		if strings.Count(expected, edit.before) != 1 || strings.Count(candidate, edit.after) != 1 {
			return fmt.Errorf("missing or duplicated exact fork proof edit")
		}
		expected = strings.Replace(expected, edit.before, edit.after, 1)
		restored = strings.Replace(restored, edit.after, edit.before, 1)
	}
	want, wantErr := canonicalFunction(expected)
	actual, actualErr := canonicalFunction(candidate)
	original, originalErr := canonicalFunction(row.After)
	reversed, reversedErr := canonicalFunction(restored)
	if wantErr != nil || actualErr != nil || originalErr != nil || reversedErr != nil || actual != want || reversed != original {
		return fmt.Errorf("fork proof changed more than its declaration and exact diagnostics")
	}
	return nil
}

func TestForkProofTruthSuccessorPreservesWholeHistoricalCaller(t *testing.T) {
	requireForkProofLiveIdentity(t)
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected *recipe
	for i := range rows {
		if rows[i].File == forkProofTruthFile && rows[i].Function == forkProofTruthOldName {
			if selected != nil {
				t.Fatal("ambiguous historical fork recipe")
			}
			selected = &rows[i]
		}
	}
	if selected == nil || selected.Family != "native-served-delivery-quiescence-caller" || selected.Removed || selected.Mechanical != nil {
		t.Fatal("historical fork recipe identity changed")
	}
	row := *selected
	if fmt.Sprintf("%x", sha256.Sum256([]byte(row.Before))) != "c205dbc515d172b290e459393d2d0f17a37f52534bbecd78530783322636c0f7" ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(row.After))) != "8a77bfcf4a078afc07b19c2bf238c9d3b50fd6591de92b24224bda662bca1fe3" {
		t.Fatal("historical fork Before/After changed")
	}
	history, historicalSource, err := catchUp2542PriorMasterPair(row, row.Successor)
	if err != nil || history.Before != row.Before || history.After != row.After {
		t.Fatal("fork proof historical pair changed:", err)
	}
	if err := validateForkProofTruthSuccessor(history, historicalSource); err != nil {
		t.Fatal(err)
	}
	actual := repair2698HistoricalBody(t, row.File, forkProofTruthNewName)
	_, historicalActual, err := catchUp2542PriorMasterPair(row, actual)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateForkProofTruthSuccessor(history, historicalActual); err != nil {
		t.Fatal(err)
	}
	if files, changes, err := prepareFiles("../../..", []recipe{row}); err != nil || len(files) != 0 || len(changes) != 0 {
		t.Fatalf("renamed source is not inert: %v", err)
	}

	for _, cut := range []struct {
		name, source string
		changed      bool
	}{
		{"old-after-to-successor", row.After, true},
		{"renamed-idempotent", row.Successor, false},
	} {
		t.Run(cut.name, func(t *testing.T) {
			source := []byte("package probe\n" + cut.source)
			current, err := checkpointRecipes(source, []recipe{row})
			if err != nil {
				t.Fatal(err)
			}
			result, changed, err := rewriteFunction(row.File, source, current[0])
			if err != nil || changed != cut.changed || !strings.Contains(string(result), row.Successor) {
				t.Fatalf("exact historical rename changed=%v error=%v", changed, err)
			}
		})
	}
	t.Run("old-and-new-coexist", func(t *testing.T) {
		source := []byte("package probe\n" + row.After + "\n" + row.Successor)
		current, err := checkpointRecipes(source, []recipe{row})
		if err != nil {
			t.Fatal(err)
		}
		if _, changed, err := rewriteFunction(row.File, source, current[0]); err == nil || changed {
			t.Fatal("coexisting historical/current declarations admitted")
		}
	})
	for _, mutation := range []struct{ name, before, after string }{
		{"first-result", "first.Error != nil || forks != 1 || completions != 0", "first.Error != nil"},
		{"execution-closed", `executionState != "closed"`, `executionState != "running"`},
		{"replay-result", "!reflect.DeepEqual(first.Result, retry.Result)", "false"},
		{"complete-snapshot", "!reflect.DeepEqual(before, repeatedStaticRunSnapshot(t, rt.DB, forkID))", "false"},
		{"delivery-owner", "rt.ReadRunDeliveries", "rt.ForeignRunDeliveries"},
		{"missing-wait", "\t\t\twaitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)\n", ""},
		{"sql", "COUNT(*) FROM runs WHERE forked_from_run_id=$1", "COUNT(*) FROM runs"},
		{"trigger", "NEW.method='run.fork'", "NEW.method='other'"},
		{"arguments", "params, 30*time.Second", "foreignParams, 30*time.Second"},
		{"clock", "30*time.Second", "31*time.Second"},
		{"foreign-name", forkProofTruthNewName, "TestForeignForkProof"},
		{"extra-diagnostic", "COMMITTED DOMAIN fork=", "ALTERED DOMAIN fork="},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			candidate := historicalSource
			mutant := strings.Replace(candidate, mutation.before, mutation.after, 1)
			if mutant == candidate || validateForkProofTruthSuccessor(history, mutant) == nil {
				t.Fatal("hostile edit admitted by exact proof-truth reversal")
			}
		})
	}
	t.Run("current-source-cardinality-owner", func(t *testing.T) {
		mutant := strings.Replace(row.Successor, "readServedSourceForkCardinality(rt.ReceiverStateReader, seed.RunID)", "readServedSourceForkCardinality(foreignReader, seed.RunID)", 1)
		if mutant == row.Successor {
			t.Fatal("current source cardinality hostile did not change")
		}
		if _, _, err := catchUp2542PriorMasterPair(row, mutant); err == nil {
			t.Fatal("foreign current source cardinality owner acquired a historical pair")
		}
	})
}

func requireForkProofLiveIdentity(t *testing.T) {
	t.Helper()
	paths, err := filepath.Glob("../../../internal/serveapp/*.go")
	if err != nil || len(paths) == 0 {
		t.Fatalf("missing served proof sources: %v", err)
	}
	counts := map[string]int{}
	for _, path := range paths {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && (fn.Name.Name == forkProofTruthOldName || fn.Name.Name == forkProofTruthNewName) {
				counts[fn.Name.Name]++
			}
		}
	}
	if counts[forkProofTruthOldName] != 0 || counts[forkProofTruthNewName] != 1 {
		t.Fatalf("live fork proof declarations: %v", counts)
	}
}
