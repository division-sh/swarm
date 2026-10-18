package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

//go:embed completion-2270-successor.json
var completionEvidenceSuccessorBytes []byte

func TestNativeCompletionEvidenceSuccessorPreservesHistoryAndRefusals(t *testing.T) {
	var history struct {
		SourceCommit, Gate, File, Function, Family string
		BeforeSHA256, AfterSHA256                  string
		FrozenBeforeSHA256, FrozenAfterSHA256      string
	}
	if err := json.Unmarshal(completionEvidenceSuccessorBytes, &history); err != nil {
		t.Fatal(err)
	}
	if history.SourceCommit != "ccaed7295df0bf8551e3a4e60c55ecda7f812c16" || history.Gate != "https://github.com/division-sh/swarm/pull/2659#issuecomment-6161724853" || history.BeforeSHA256 != "1c028407640323f18259955a47d357a75ac8a6cb5ceddec8632b38fbc7fe7279" {
		t.Fatal("completion fixture successor lost its reviewed provenance")
	}
	var current []recipe
	if err := json.Unmarshal(recipeBytes, &current); err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, row := range current {
		if row.File != history.File || row.Function != history.Function {
			continue
		}
		found++
		if row.Family != history.Family || fmt.Sprintf("%x", sha256.Sum256([]byte(row.Before))) != history.FrozenBeforeSHA256 || fmt.Sprintf("%x", sha256.Sum256([]byte(row.After))) != history.FrozenAfterSHA256 || fmt.Sprintf("%x", sha256.Sum256([]byte(row.Successor))) != history.AfterSHA256 {
			t.Fatal("completion successor overwrote prior transformation history")
		}
		transition := recipe{File: row.File, Function: row.Function, Before: row.After, After: row.Successor}
		if _, changed, err := rewriteFunction(row.File, []byte("package proof\n"+row.After), transition); err != nil || !changed {
			t.Fatalf("frozen completion endpoint is not migratable: changed=%v err=%v", changed, err)
		}
		if _, changed, err := rewriteFunction(row.File, []byte("package proof\n"+row.Successor), transition); err != nil || changed {
			t.Fatalf("current completion successor is not inert: changed=%v err=%v", changed, err)
		}
		hostile := strings.Replace(row.Successor, "{", "{ unreviewedAuthority();", 1)
		if _, _, err := rewriteFunction(row.File, []byte("package proof\n"+hostile), transition); err == nil {
			t.Fatal("completion successor admitted unreviewed authority")
		}
	}
	if found != 1 {
		t.Fatalf("completion successor has %d recipe owners, want1", found)
	}
}
