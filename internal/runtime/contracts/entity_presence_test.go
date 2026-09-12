package contracts

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestEntityMutationClearGrammar(t *testing.T) {
	for _, tc := range []struct {
		name, row string
		valid     bool
	}{
		{"clear", "{op: clear, target: entity.note}", true},
		{"value", "{op: clear, target: entity.note, value: x}", false},
		{"null_value", "{op: clear, target: entity.note, value: null}", false},
		{"key", "{op: clear, target: entity.note, key: x}", false},
		{"index", "{op: clear, target: entity.note, index: 0}", false},
		{"source", "{op: clear, target: entity.note, source_field: note}", false},
		{"expression", "{op: clear, target: entity.note, expression: '1'}", false},
		{"bare_target", "{op: clear, target: note}", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var spec WorkflowDataAccumulation
			err := yaml.Unmarshal([]byte("writes:\n  - "+tc.row+"\n"), &spec)
			if (err == nil) != tc.valid {
				t.Fatalf("decode=%v, valid=%v", err, tc.valid)
			}
			if tc.valid && (spec.Writes[0].IsContainedOperation() || !spec.Writes[0].SourceExpression().IsZero()) {
				t.Fatal("clear acquired a contained or source operation")
			}
		})
	}
}

func TestEntityMutationRetiredClearSyntax(t *testing.T) {
	for _, target := range []string{"entity.note", "note", "metadata.note", "pending_dedup"} {
		var handler SystemNodeEventHandler
		err := yaml.Unmarshal([]byte("clear:\n  targets: ["+target+"]\n"), &handler)
		if err == nil || !strings.Contains(err.Error(), "RETIRED") {
			t.Fatalf("target %q decode=%v", target, err)
		}
	}
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte("clear:\n  targets: [accumulator_state]\n"), &handler); err != nil {
		t.Fatal(err)
	}
}

func TestEntityInitializerRejectsNull(t *testing.T) {
	var doc EntityContractsDocument
	if err := yaml.Unmarshal([]byte("work:\n  note:\n    type: text?\n    initial: null\n"), &doc); err == nil {
		t.Fatal("null initializer accepted")
	}
}
