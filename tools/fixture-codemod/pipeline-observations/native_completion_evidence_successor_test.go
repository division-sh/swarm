package main

import (
	_ "embed"
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
)

//go:embed completion-2270-successor.json
var completionEvidenceSuccessorBytes []byte

func TestNativeCompletionEvidenceSuccessorPreservesHistoryAndRefusals(t *testing.T) {
	var history struct{ SourceCommit, Gate, File, Function, Before, After string }
	if err := json.Unmarshal(completionEvidenceSuccessorBytes, &history); err != nil {
		t.Fatal(err)
	}
	if history.SourceCommit != "ccaed7295df0bf8551e3a4e60c55ecda7f812c16" || history.Gate != "https://github.com/division-sh/swarm/pull/2659#issuecomment-6161724853" {
		t.Fatal("completion fixture successor lost its reviewed provenance")
	}
	frozen, err := exec.Command("git", "show", history.SourceCommit+":tools/fixture-codemod/pipeline-observations/recipes.json").Output()
	if err != nil {
		t.Fatal(err)
	}
	var before, current []recipe
	if err := json.Unmarshal(frozen, &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(recipeBytes, &current); err != nil {
		t.Fatal(err)
	}
	if len(before) != len(current) {
		t.Fatal("completion successor changed the finite recipe population")
	}
	found := 0
	for i, row := range current {
		if row.File != history.File || row.Function != history.Function {
			continue
		}
		found++
		prior := before[i]
		if prior.Before != row.Before || prior.After != row.After || prior.Successor != history.Before || row.Successor != history.After {
			t.Fatal("completion successor overwrote prior transformation history")
		}
		if _, changed, err := rewriteFunction(row.File, []byte("package proof\n"+history.After), recipe{File: row.File, Function: row.Function, Before: history.Before, After: history.After}); err != nil || changed {
			t.Fatalf("current completion successor is not inert: changed=%v err=%v", changed, err)
		}
		hostile := strings.Replace(history.After, "{", "{ unreviewedAuthority();", 1)
		if _, _, err := rewriteFunction(row.File, []byte("package proof\n"+hostile), recipe{File: row.File, Function: row.Function, Before: history.Before, After: history.After}); err == nil {
			t.Fatal("completion successor admitted unreviewed authority")
		}
	}
	if found != 1 {
		t.Fatalf("completion successor has %d recipe owners, want1", found)
	}
}
