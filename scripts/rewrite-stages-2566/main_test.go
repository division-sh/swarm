package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func readPlan(t *testing.T) plan {
	t.Helper()
	body, err := os.ReadFile("intent.json")
	if err != nil {
		t.Fatal(err)
	}
	var p plan
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestRewrite2566ReviewedPlanHasExplicitEntryAndFragmentDecisions(t *testing.T) {
	p := readPlan(t)
	sites, moved := 0, 0
	for _, c := range p.Changes {
		if len(c.Edits) == 0 || c.BeforeHash == c.AfterHash || (len(c.Equivalence) == 0 && c.Review == "") {
			t.Fatalf("%s: missing exact edit or reviewed disposition", c.File)
		}
		for _, proof := range c.Equivalence {
			sites++
			if len(proof.BeforeOrder) == 0 || len(proof.AfterOrder) == 0 || proof.AfterOrder[0] != proof.Entry {
				t.Fatalf("%s: missing entry equivalence: %#v", c.File, proof)
			}
			if proof.Reordered {
				moved++
				if c.File != "internal/runtime/pipeline/workflow_gate_lifecycle_test.go" && c.File != "internal/runtime/pipeline/workflow_join_lifecycle_test.go" {
					t.Fatalf("unreviewed declaration move in %s", c.File)
				}
			} else if !reflect.DeepEqual(proof.BeforeOrder, proof.AfterOrder) {
				t.Fatalf("%s: ordinary declaration order changed", c.File)
			}
		}
	}
	// One retired deactivation fixture leaves 552 originals, plus 3 and 11 incoming sites.
	if sites != 566 || moved != 2 {
		t.Fatalf("missing positive preparation: sites=%d moves=%d", sites, moved)
	}
}

func fixtureChange(name string) change {
	before := "stages: {initial: {initial: true}, terminal: {terminal: true}}\ninstance_variables: {variables: {data: {type: json, default: {initial: true, terminal: false}}}}\n"
	after := "stages: {initial: {}, terminal: {final: true}}\ninstance_variables: {variables: {data: {type: json, default: {initial: true, terminal: false}}}}\n"
	return change{
		File: name, BeforeHash: digest([]byte(before)), AfterHash: digest([]byte(after)),
		Edits:       []edit{{Offset: 0, Before: strings.SplitAfter(before, "\n")[0], After: strings.SplitAfter(after, "\n")[0]}},
		Equivalence: []equivalence{{Site: "whole-file", Entry: "initial", BeforeOrder: []string{"initial", "terminal"}, AfterOrder: []string{"initial", "terminal"}, Finals: []string{"terminal"}}},
	}
}

func writePlan(t *testing.T, root string, changes []change) {
	t.Helper()
	body, err := json.Marshal(plan{Baseline: "test-base", Scope: "test", Changes: changes})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "intent.json"), body, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestRewrite2566DataAndStageNamesStayIndependent(t *testing.T) {
	c := fixtureChange("schema.yaml")
	before := []byte(c.Edits[0].Before + "instance_variables: {variables: {data: {type: json, default: {initial: true, terminal: false}}}}\n")
	output, err := rewrite(c, before)
	if err != nil || !strings.Contains(string(output), "stages: {initial: {}, terminal: {final: true}}") || !strings.Contains(string(output), "default: {initial: true, terminal: false}") {
		t.Fatalf("unrelated data or stage IDs changed: %s, %v", output, err)
	}
}

func TestRewrite2566DryRunDoesNotWrite(t *testing.T) {
	root := t.TempDir()
	c := fixtureChange("schema.yaml")
	before := []byte(c.Edits[0].Before + "instance_variables: {variables: {data: {type: json, default: {initial: true, terminal: false}}}}\n")
	if err := os.WriteFile(filepath.Join(root, c.File), before, 0600); err != nil {
		t.Fatal(err)
	}
	writePlan(t, root, []change{c})
	if err := apply(root, "intent.json", false, false); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, c.File))
	if err != nil || !bytes.Equal(got, before) {
		t.Fatalf("dry run wrote source: %q, %v", got, err)
	}
	if err := apply(root, "intent.json", false, true); err == nil {
		t.Fatal("check accepted unapplied output")
	}
	if err := apply(root, "intent.json", true, false); err != nil {
		t.Fatal(err)
	}
	if err := apply(root, "intent.json", false, true); err != nil {
		t.Fatal(err)
	}
}

func TestRewrite2566RefusesEntirePlanBeforeWrites(t *testing.T) {
	for _, invalid := range []string{"hash", "anchor", "output", "overlap", "duplicate", "path"} {
		t.Run(invalid, func(t *testing.T) {
			root := t.TempDir()
			first, last := fixtureChange("first.yaml"), fixtureChange("last.yaml")
			before := []byte(first.Edits[0].Before + "instance_variables: {variables: {data: {type: json, default: {initial: true, terminal: false}}}}\n")
			for _, name := range []string{first.File, last.File} {
				if err := os.WriteFile(filepath.Join(root, name), before, 0600); err != nil {
					t.Fatal(err)
				}
			}
			switch invalid {
			case "hash":
				last.BeforeHash = digest([]byte("different source"))
			case "anchor":
				last.Edits[0].Before = "incorrect anchor"
			case "output":
				last.AfterHash = digest([]byte("incorrect output"))
			case "overlap":
				last.Edits = append(last.Edits, last.Edits[0])
			case "duplicate":
				last.File = first.File
			case "path":
				last.File = "../outside.yaml"
			}
			writePlan(t, root, []change{first, last})
			if err := apply(root, "intent.json", true, false); err == nil {
				t.Fatalf("accepted invalid %s plan", invalid)
			}
			got, err := os.ReadFile(filepath.Join(root, first.File))
			if err != nil || !bytes.Equal(got, before) {
				t.Fatalf("partially wrote first file: %q, %v", got, err)
			}
		})
	}
}
