package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestNodeShapeGoPlanRewritesOnlySelectedCompleteLiterals(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture.go")
	source := []byte("package fixture\n\nfunc selected() string {\n return \"worker:\\n  subscribes_to: [ready]\\n  event_handlers:\\n    ready: {}\\n\"\n}\n\nfunc untouched() string { return \"event_handlers: an assertion, not YAML\" }\n")
	if err := os.WriteFile(path, source, 0600); err != nil {
		t.Fatal(err)
	}
	inputs, err := prepareGoInputs([]string{path})
	if err != nil || len(inputs) != 1 || inputs[0].Declaration != "selected" || inputs[0].Ordinal != 0 || inputs[0].Counts != (rewriteCounts{Subscriptions: 1, HandlerContainers: 1}) {
		t.Fatalf("unexpected literal selection: %+v error=%v", inputs, err)
	}
	plan := writeGoPlanTest(t, inputs)
	if err := applyGoInputs(plan, false); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, source) {
		t.Fatalf("dry run mutated source: %s error=%v", got, err)
	}
	if err := applyGoInputs(plan, true); err != nil {
		t.Fatal(err)
	}
	got, err = os.ReadFile(path)
	if err != nil || !strings.Contains(string(got), `"worker:\n  ready: {}\n"`) || !strings.Contains(string(got), `"event_handlers: an assertion, not YAML"`) {
		t.Fatalf("unrelated literal or selected handler changed: %s error=%v", got, err)
	}
	if err := applyGoInputs(plan, true); err != nil {
		t.Fatal(err)
	}
	again, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, again) {
		t.Fatalf("literal rewrite is not byte-idempotent: %s error=%v", again, err)
	}
	for _, hostile := range []string{"output", "location", "duplicate"} {
		t.Run(hostile, func(t *testing.T) {
			changed := append([]goRewriteInput{}, inputs...)
			switch hostile {
			case "output":
				changed[0].After = `"invented"`
			case "location":
				changed[0].Declaration = "untouched"
			case "duplicate":
				changed = append(changed, changed[0])
			}
			if err := applyGoInputs(writeGoPlanTest(t, changed), true); err == nil {
				t.Fatal("foreign output or selection admitted")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(after, got) {
				t.Fatalf("invalid selection partially mutated source: %s error=%v", after, err)
			}
		})
	}
}

func TestNodeShapeGoPlanRejectsPartialGenerators(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture.go")
	if err := os.WriteFile(path, []byte("package fixture\nfunc source() string { return \"worker:\\n  event_handlers:\\n    ready:\\n      \" }\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if inputs, err := prepareGoInputs([]string{path}); err == nil || inputs != nil || !strings.Contains(err.Error(), "incomplete source fragment; disposition required") {
		t.Fatalf("partial generator silently rewritten: %+v error=%v", inputs, err)
	}
}

func TestNodeShapeGoProjectionLedgerReplaysReviewedLiterals(t *testing.T) {
	proveNodeShapeGoLedger(t, "go-projection-inputs.json", 14)
}

func TestNodeShapeGoProducerLedgerReplaysReviewedLiteral(t *testing.T) {
	proveNodeShapeGoLedger(t, "go-producer-inputs.json", 1)
}

func TestNodeShapeGoActivityForkLedgerReplaysReviewedLiteral(t *testing.T) {
	proveNodeShapeGoLedger(t, "go-activity-fork-inputs.json", 1)
}

func proveNodeShapeGoLedger(t *testing.T, path string, count int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var inputs []goRewriteInput
	if err := json.Unmarshal(data, &inputs); err != nil {
		t.Fatal(err)
	}
	if len(inputs) != count {
		t.Fatalf("projection input membership changed: %d", len(inputs))
	}
	for _, input := range inputs {
		after, counts, err := rewriteGoNodeLiteral(input.Before)
		if err != nil || after != input.After || counts != input.Counts {
			t.Fatalf("%s:%s literal %d no longer replays: counts=%+v error=%v", input.Path, input.Declaration, input.Ordinal, counts, err)
		}
		beforeBody, err := strconv.Unquote(input.Before)
		if err != nil {
			t.Fatal(err)
		}
		afterBody, err := strconv.Unquote(after)
		if err != nil {
			t.Fatal(err)
		}
		var before, rewritten yaml.Node
		if err := yaml.Unmarshal([]byte(beforeBody), &before); err != nil {
			t.Fatal(err)
		}
		if err := yaml.Unmarshal([]byte(afterBody), &rewritten); err != nil {
			t.Fatal(err)
		}
		oldRoot, newRoot := before.Content[0], rewritten.Content[0]
		if len(oldRoot.Content) != len(newRoot.Content) {
			t.Fatal("projection declaration membership changed")
		}
		for i := 0; i < len(oldRoot.Content); i += 2 {
			assertSameYAMLValue(t, oldRoot.Content[i], newRoot.Content[i])
			handlers := requireMappingValue(t, oldRoot.Content[i+1], "event_handlers")
			for j := 0; j < len(handlers.Content); j += 2 {
				assertSameYAMLValue(t, handlers.Content[j+1], requireMappingValue(t, newRoot.Content[i+1], handlers.Content[j].Value))
			}
		}
	}
}

func writeGoPlanTest(t *testing.T, inputs []goRewriteInput) string {
	t.Helper()
	data, err := json.Marshal(inputs)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "go-inputs.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
