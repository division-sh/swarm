package contracts

import (
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

// Retired fields cannot have readers once they are absent from the typed model.
// This replaces the old presentation-reader allowance inventory, not its ceiling.
func TestHandlerRuleDisplayLabelReaderBoundary(t *testing.T) {
	if fields := retiredHandlerRuleFields(reflect.TypeOf(HandlerRuleEntry{})); len(fields) != 0 {
		t.Fatalf("HandlerRuleEntry restored authored presentation authority: %v", fields)
	}
}

func TestHandlerRuleDisplayLabelReaderBoundaryRejectsHostileSemanticUse(t *testing.T) {
	for _, name := range []string{"ID", "Description"} {
		t.Run(name, func(t *testing.T) {
			hostile := reflect.StructOf([]reflect.StructField{{Name: name, Type: reflect.TypeOf("")}})
			if got := retiredHandlerRuleFields(hostile); !slices.Equal(got, []string{name}) {
				t.Fatalf("restored %s authority escaped the typed model boundary: %v", name, got)
			}
		})
	}
	if fields := retiredHandlerRuleFields(reflect.TypeOf(struct{ Condition string }{})); len(fields) != 0 {
		t.Fatalf("predicate text was incorrectly retired: %v", fields)
	}
	if _, exists := reflect.TypeOf(ActivitySpec{}).FieldByName("ID"); !exists {
		t.Fatal("the rule model cut removed explicit activity identity")
	}
}

func retiredHandlerRuleFields(row reflect.Type) []string {
	var fields []string
	for _, name := range []string{"ID", "Description"} {
		if _, present := row.FieldByName(name); present {
			fields = append(fields, name)
		}
	}
	return fields
}

func handlerRuleIdentityGuardRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}
