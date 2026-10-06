package contracts

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

type stage2566PreparedEdit struct {
	Offset int    `json:"offset"`
	Before string `json:"before"`
	After  string `json:"after"`
}

type stage2566Equivalence struct {
	Site        string   `json:"site"`
	Entry       string   `json:"entry"`
	BeforeOrder []string `json:"before_order"`
	AfterOrder  []string `json:"after_order"`
	Finals      []string `json:"finals"`
	Reordered   bool     `json:"reordered,omitempty"`
}

type stage2566PreparedChange struct {
	File        string                  `json:"file"`
	BeforeHash  string                  `json:"before_hash"`
	AfterHash   string                  `json:"after_hash"`
	Edits       []stage2566PreparedEdit `json:"edits"`
	Equivalence []stage2566Equivalence  `json:"equivalence"`
}

// Diagnostic preparation only: outputs a finite script ledger, never corpus files.
func TestIssue2566PrepareCorpusPlan(t *testing.T) {
	repo := repoRootForContractsTest(t)
	command := exec.Command("git", "ls-tree", "-r", "--name-only", stage2566CorrectedBaseline)
	command.Dir = repo
	names, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	var changes []stage2566PreparedChange
	sites, moved := 0, 0
	for _, name := range strings.Fields(string(names)) {
		if !strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(repo, name))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "initial:") && !strings.Contains(string(body), "terminal:") {
			continue
		}
		change := stage2566PreparedChange{File: name, BeforeHash: stage2566Digest(string(body))}
		add := func(offset int, before, after string, proof stage2566Equivalence) {
			if before == after {
				t.Fatalf("empty preparation at %s:%s", name, proof.Site)
			}
			// Keep only the differing bytes, leaving all surrounding source untouched.
			prefix := 0
			for prefix < len(before) && prefix < len(after) && before[prefix] == after[prefix] {
				prefix++
			}
			suffix := 0
			for suffix < len(before)-prefix && suffix < len(after)-prefix && before[len(before)-1-suffix] == after[len(after)-1-suffix] {
				suffix++
			}
			change.Edits = append(change.Edits, stage2566PreparedEdit{offset + prefix, before[prefix : len(before)-suffix], after[prefix : len(after)-suffix]})
			change.Equivalence = append(change.Equivalence, proof)
			sites++
			if proof.Reordered {
				moved++
			}
		}
		switch {
		case strings.HasSuffix(name, ".go"):
			positions := token.NewFileSet()
			file, err := parser.ParseFile(positions, name, body, 0)
			if err != nil {
				t.Fatal(err)
			}
			ast.Inspect(file, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				text, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Fatal(err)
				}
				site := fmt.Sprintf("%s:%d:%d", name, positions.Position(literal.Pos()).Line, positions.Position(literal.Pos()).Column)
				after, proof, ok := stage2566PreparePositive(t, name, site, text)
				if ok {
					encoded := strconv.Quote(after)
					if strings.HasPrefix(literal.Value, "`") {
						if strings.Contains(after, "`") {
							t.Fatalf("raw Go literal changes representation at %s", site)
						}
						encoded = "`" + after + "`"
					}
					add(positions.Position(literal.Pos()).Offset, literal.Value, encoded, proof)
				}
				return true
			})
		case name == "platform-spec.yaml":
			snapshot, err := yamlsource.Load(body)
			if err != nil {
				t.Fatal(err)
			}
			value := snapshot.Document(name).Root()
			for _, key := range []string{"contract_formats", "hello_world_example", "schema_yaml"} {
				field, err := value.Lookup(key)
				if err != nil {
					t.Fatal(err)
				}
				value = field.Value
			}
			scalar, err := value.Scalar()
			if err != nil {
				t.Fatal(err)
			}
			after, proof, ok := stage2566PreparePositive(t, name, value.SemanticPath(), scalar.Value)
			if !ok {
				t.Fatal("hello-world example lost positive classification")
			}
			beforeQuoted := strconv.Quote(scalar.Value)
			if scalar.Style != yamlsource.DoubleQuotedStyle || strings.Count(string(body), beforeQuoted) != 1 {
				t.Fatal("reviewed hello-world source anchor drift")
			}
			add(strings.Index(string(body), beforeQuoted), beforeQuoted, strconv.Quote(after), proof)
		default:
			if after, proof, ok := stage2566PreparePositive(t, name, "whole-file", string(body)); ok {
				add(0, string(body), after, proof)
			}
		}
		if len(change.Edits) == 0 {
			continue
		}
		sort.Slice(change.Edits, func(i, j int) bool { return change.Edits[i].Offset < change.Edits[j].Offset })
		after := string(body)
		last := len(body)
		for i := len(change.Edits) - 1; i >= 0; i-- {
			edit := change.Edits[i]
			if edit.Offset+len(edit.Before) > last || string(body[edit.Offset:edit.Offset+len(edit.Before)]) != edit.Before {
				t.Fatalf("overlapping/incorrect edit in %s", name)
			}
			after = after[:edit.Offset] + edit.After + after[edit.Offset+len(edit.Before):]
			last = edit.Offset
		}
		if strings.HasSuffix(name, ".go") {
			if _, err := parser.ParseFile(token.NewFileSet(), name, after, 0); err != nil {
				t.Fatalf("prepared Go source corrupt at %s: %v", name, err)
			}
		}
		change.AfterHash = stage2566Digest(after)
		changes = append(changes, change)
	}
	if sites != 553 || moved != 2 {
		t.Fatalf("positive corpus coverage changed: sites=%d moved=%d", sites, moved)
	}
	t.Logf("prepared %d sites in %d files; %d explicit entry moves; corpus untouched", sites, len(changes), moved)
	if output := os.Getenv("ISSUE2566_PLAN_OUTPUT"); output != "" {
		if filepath.Base(output) != "intent.json" {
			t.Fatal("preparation writes only intent.json, not corpus files")
		}
		encoded, err := json.MarshalIndent(struct {
			Baseline string                    `json:"baseline"`
			Scope    string                    `json:"scope"`
			Changes  []stage2566PreparedChange `json:"changes"`
		}{stage2566CorrectedBaseline, "positive source preparation only; 551 simple sites and 2 explicit entry moves; remaining census dispositions and runtime final grammar are not yet implemented", changes}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(repo, output), append(encoded, '\n'), 0644); err != nil {
			t.Fatal(err)
		}
	}
}

func stage2566PreparePositive(t *testing.T, name, site, text string) (string, stage2566Equivalence, bool) {
	t.Helper()
	if !strings.Contains(text, "initial:") && !strings.Contains(text, "terminal:") {
		return "", stage2566Equivalence{}, false
	}
	before, err := admitSchemaFragment(text)
	if err != nil || before.StageDeclarations.InitialCount() != 1 {
		return "", stage2566Equivalence{}, false
	}
	proof := stage2566Equivalence{Site: site, Entry: before.LoweredInitialState(), Finals: []string{}}
	for _, stage := range before.StageDeclarations.Entries {
		proof.BeforeOrder = append(proof.BeforeOrder, stage.ID)
		if stage.Terminal {
			proof.Finals = append(proof.Finals, stage.ID)
		}
	}
	snapshot, err := yamlsource.Load([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	stages, _ := snapshot.Document(name).Root().Lookup("stages")
	entries, _ := stages.Value.Mapping()
	if proof.BeforeOrder[0] != proof.Entry {
		allowed := name == "internal/runtime/pipeline/workflow_gate_lifecycle_test.go" && before.Name == "gate-test" && proof.Entry == "awaiting_review" || name == "internal/runtime/pipeline/workflow_join_lifecycle_test.go" && before.Name == "orders" && proof.Entry == "awaiting"
		if !allowed || len(entries) < 3 || entries[1].Name != proof.Entry {
			return "", stage2566Equivalence{}, false
		}
		lines := strings.SplitAfter(text, "\n")
		offset := func(line int) int { return len(strings.Join(lines[:line-1], "")) }
		first, entry, next := offset(entries[0].KeyLocation.Line), offset(entries[1].KeyLocation.Line), offset(entries[2].KeyLocation.Line)
		text = text[:first] + text[entry:next] + text[first:entry] + text[next:]
		proof.Reordered = true
		snapshot, err = yamlsource.Load([]byte(text))
		if err != nil {
			t.Fatal(err)
		}
		stages, _ = snapshot.Document(name).Root().Lookup("stages")
		entries, _ = stages.Value.Mapping()
		before.StageDeclarations.Entries[0], before.StageDeclarations.Entries[1] = before.StageDeclarations.Entries[1], before.StageDeclarations.Entries[0]
	}
	intermediate, err := stage2566DryErase(text, entries)
	if err != nil {
		return "", stage2566Equivalence{}, false
	}
	after, err := admitSchemaFragment(intermediate)
	if err != nil {
		t.Fatalf("prepared source invalid at %s: %v", site, err)
	}
	for i := range before.StageDeclarations.Entries {
		before.StageDeclarations.Entries[i].Initial = false
	}
	// Admission provenance moves with deleted bytes; it is not business metadata.
	before.admissionProvenance, after.admissionProvenance = nil, nil
	for _, schema := range []*FlowSchemaDocument{&before, &after} {
		for i := range schema.Connect {
			schema.Connect[i].SourceLine = 0
		}
		for i := range schema.Pins.Inputs.EventPins {
			schema.Pins.Inputs.EventPins[i].sourceLine, schema.Pins.Inputs.EventPins[i].sourceCol = 0, 0
		}
		for i := range schema.Pins.Outputs.EventPins {
			schema.Pins.Outputs.EventPins[i].sourceLine, schema.Pins.Outputs.EventPins[i].sourceCol = 0, 0
		}
	}
	if !reflect.DeepEqual(before, after) {
		left, right := reflect.ValueOf(before), reflect.ValueOf(after)
		for i := 0; i < left.NumField(); i++ {
			if left.Field(i).CanInterface() && !reflect.DeepEqual(left.Field(i).Interface(), right.Field(i).Interface()) {
				t.Errorf("%s:%s: changed %s: before=%#v after=%#v", name, site, left.Type().Field(i).Name, left.Field(i).Interface(), right.Field(i).Interface())
			}
		}
		t.Fatalf("prepared source changes typed schema/metadata at %s:%s", name, site)
	}
	snapshot, _ = yamlsource.Load([]byte(intermediate))
	stages, _ = snapshot.Document(name).Root().Lookup("stages")
	entries, _ = stages.Value.Mapping()
	finalText, err := stage2566DryRenameFinal(intermediate, entries)
	if err != nil {
		return "", stage2566Equivalence{}, false
	}
	snapshot, err = yamlsource.Load([]byte(finalText))
	if err != nil {
		t.Fatal(err)
	}
	stages, _ = snapshot.Document(name).Root().Lookup("stages")
	finalEntries, err := stages.Value.Mapping()
	if err != nil || len(entries) != len(finalEntries) {
		t.Fatalf("final source changed declarations at %s: %v", site, err)
	}
	for i, entry := range finalEntries {
		proof.AfterOrder = append(proof.AfterOrder, entry.Name)
		old, _ := entries[i].Value.Lookup("terminal")
		final, _ := entry.Value.Lookup("final")
		initial, _ := entry.Value.Lookup("initial")
		terminal, _ := entry.Value.Lookup("terminal")
		if entry.Name != entries[i].Name || old.Presence != final.Presence || initial.Presence != yamlsource.PresenceMissing || terminal.Presence != yamlsource.PresenceMissing {
			t.Fatalf("final source changed order/marker presence at %s", site)
		}
		if old.Presence != yamlsource.PresenceMissing {
			oldScalar, _ := old.Value.Scalar()
			finalScalar, _ := final.Value.Scalar()
			oldScalar.Location, oldScalar.ResolvedLocation = finalScalar.Location, finalScalar.ResolvedLocation
			if !reflect.DeepEqual(oldScalar, finalScalar) {
				t.Fatalf("final source changed scalar meaning/style at %s", site)
			}
		}
	}
	if proof.Entry != proof.AfterOrder[0] {
		t.Fatalf("entry changed at %s: %#v", site, proof)
	}
	second, err := stage2566DryErase(finalText, finalEntries)
	if err == nil {
		second, err = stage2566DryRenameFinal(second, finalEntries)
	}
	if err != nil || second != finalText {
		t.Fatalf("prepared source not byte-idempotent at %s: %v", site, err)
	}
	return finalText, proof, true
}
