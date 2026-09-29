package yamlsource

import (
	"fmt"
	"strings"
	"testing"
)

func TestValueExpansionBudget(t *testing.T) {
	for _, depth := range []int{10, 18, 24} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			var raw strings.Builder
			raw.WriteString("note:\n  a0: &a0 [x, x]\n")
			for i := 1; i <= depth; i++ {
				fmt.Fprintf(&raw, "  a%d: &a%d [*a%d, *a%d]\n", i, i, i-1, i-1)
			}
			fmt.Fprintf(&raw, "data: *a%d\n", depth)
			snapshot, err := Load([]byte(raw.String()))
			if err != nil {
				t.Fatal(err)
			}
			root := snapshot.Document("nodes.yaml").Root()
			if err := root.ValidateAcyclic(); err != nil {
				t.Fatal(err)
			}
			note, err := root.Lookup("note")
			if err != nil {
				t.Fatal(err)
			}
			err = root.ValidateExpansion(note.Value)
			if depth == 10 {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "YAML-EXPANSION-LIMIT") {
				t.Fatalf("active alias is not exempted by ignored anchor occurrence: %v", err)
			}
			if err := note.Value.ValidateExpansion(note.Value); err != nil {
				t.Fatalf("annotation alone should not expand: %v", err)
			}
		})
	}
}

func TestValueMergeExpansionBudget(t *testing.T) {
	var raw strings.Builder
	raw.WriteString("a0: &a0 {x: y}\n")
	for i := 1; i <= 17; i++ {
		fmt.Fprintf(&raw, "a%d: &a%d {<<: [*a%d, *a%d]}\n", i, i, i-1, i-1)
	}
	snapshot, err := Load([]byte(raw.String()))
	if err != nil {
		t.Fatal(err)
	}
	alias, err := snapshot.Document("nodes.yaml").Root().Lookup("a17")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := alias.Value.Mapping(); err == nil || !strings.Contains(err.Error(), "YAML-EXPANSION-LIMIT") {
		t.Fatalf("mapping allocation must be bounded before materialization: %v", err)
	}
}
