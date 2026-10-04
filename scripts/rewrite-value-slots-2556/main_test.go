package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func readPlan(t *testing.T) []change {
	t.Helper()
	data, err := os.ReadFile("intent.json")
	if err != nil {
		t.Fatal(err)
	}
	var plan []change
	if err := json.Unmarshal(data, &plan); err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestRewrite2556CheckedInIntentLedger(t *testing.T) {
	plan := readPlan(t)
	if len(plan) < 200 {
		t.Fatalf("missing finite corpus decisions: %d", len(plan))
	}
	files := map[string]bool{}
	for _, c := range plan {
		if files[c.File] || len(c.Slots) == 0 || c.BeforeHash == c.AfterHash || len(c.Edits) == 0 {
			t.Fatalf("ambiguous/unclassified edit: %s", c.File)
		}
		files[c.File] = true
		end := 0
		for _, e := range c.Edits {
			if e.Start < end || len(e.Remove) == 0 && len(e.Add) == 0 {
				t.Fatalf("%s: overlapping/empty edits", c.File)
			}
			end = e.Start + len(e.Remove)
		}
	}
}

func TestRewrite2556EmbeddedSourcesAndBothReplacementAnchors(t *testing.T) {
	before := []byte("package sample\nfunc fixture(){body:=`fields:\nvalue: ${payload.value}\n`;old:=`fields:\nvalue: ${payload.value}\n`;_,_=body,old}\n")
	after := []byte(strings.ReplaceAll(string(before), "${payload.value}", "payload.value"))
	plan := []change{{File: "fixture.go", BeforeHash: digest(before), AfterHash: digest(after), Edits: []lineEdit{
		{Start: 2, Remove: []string{"value: ${payload.value}"}, Add: []string{"value: payload.value"}},
		{Start: 4, Remove: []string{"value: ${payload.value}"}, Add: []string{"value: payload.value"}},
	}}}
	output, err := rewriteFile("fixture.go", before, plan)
	if err != nil || string(output) != string(after) || strings.Count(string(output), "value: payload.value") != 2 {
		t.Fatalf("paired source/anchor diverged: %s, %v", output, err)
	}
	if _, err := rewriteFile("fixture.go", []byte("package sample\nfunc fixture(){}\n"), plan); err == nil {
		t.Fatal("unknown source accepted")
	}
}

func TestRewrite2556NegativeOracleOwnership(t *testing.T) {
	owners := map[string]string{
		"tests/tier8-boot-verification/test-boot-on-complete-dict/nodes.yaml": "fallback:",
		"tests/tier8-boot-verification/test-boot-cel-parse-error/nodes.yaml":  "when:",
	}
	for _, c := range readPlan(t) {
		if owner, ok := owners[c.File]; ok {
			for _, e := range c.Edits {
				for _, line := range e.Remove {
					if strings.Contains(line, owner) {
						t.Fatalf("negative owning declaration removed: %s: %s", c.File, line)
					}
				}
			}
			delete(owners, c.File)
		}
	}
	if len(owners) > 0 {
		t.Fatalf("missing negative corpus: %#v", owners)
	}
}

func TestRewrite2556IdempotenceAndDataExclusion(t *testing.T) {
	for _, c := range readPlan(t) {
		if !strings.HasSuffix(c.File, ".go") && !strings.HasSuffix(c.File, "/nodes.yaml") && !strings.HasSuffix(c.File, "/schema.yaml") {
			t.Fatalf("data file rewritten: %s", c.File)
		}
	}
	before, after := []byte("policy: literal\nvalue: ${payload.value}\nfrom: payload.path\n"), []byte("policy: literal\nvalue: payload.value\nfrom: payload.path\n")
	plan := []change{{File: "fixture.yaml", BeforeHash: digest(before), AfterHash: digest(after), Edits: []lineEdit{{Start: 1, Remove: []string{"value: ${payload.value}"}, Add: []string{"value: payload.value"}}}}}
	for _, source := range [][]byte{before, after} {
		got, err := rewriteFile("fixture.yaml", source, plan)
		if err != nil || string(got) != string(after) {
			t.Fatalf("got %s, %v", got, err)
		}
	}
}

func TestRewrite2556ExactFilePlanOutsideRepository(t *testing.T) {
	before, after := []byte("header\nvalue: ${payload.value}\nfooter\n"), []byte("header\nvalue: payload.value\nfooter\n")
	plan := []change{{File: "nodes.yaml", BeforeHash: digest(before), AfterHash: digest(after), Edits: []lineEdit{{Start: 1, Remove: []string{"value: ${payload.value}"}, Add: []string{"value: payload.value"}}}}}
	root := t.TempDir()
	if err := os.WriteFile(root+"/nodes.yaml", before, 0600); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(root+"/plan.json", data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := apply(root, "plan.json", true, false); err != nil {
		t.Fatal(err)
	}
	if err := apply(root, "plan.json", false, true); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(root+"/nodes.yaml", []byte("drift\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := apply(root, "plan.json", true, false); err == nil {
		t.Fatal("unknown source accepted")
	}
}
