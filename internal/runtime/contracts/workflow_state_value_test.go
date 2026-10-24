package contracts

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

// Invariant: retained state.yaml syntax has one unwrapped, style-preserving
// declaration owner; literals cannot be silently interpreted as type names.
func TestStateDeclarationRetainedStyleAndLiteralAdmission(t *testing.T) {
	catalog := TypeCatalogDocument{Types: map[string]NamedTypeDecl{
		"Profile": {Fields: map[string]TypeFieldSpec{"name": {Type: "text"}}},
	}}
	for _, tc := range []struct {
		name, source, typeName, refusal string
		initial                         any
		optional, indexed               bool
	}{
		{name: "type", source: "x: text\n", typeName: "text"},
		{name: "optional", source: "x: Profile?\n", typeName: "Profile", optional: true},
		{name: "list_type", source: "x: [Profile]\n", typeName: "[Profile]"},
		{name: "text", source: "x: \"new\"\n", typeName: "text", initial: "new"},
		{name: "empty_text", source: "x: ''\n", typeName: "text", initial: ""},
		{name: "integer", source: "x: 0\n", typeName: "integer", initial: 0},
		{name: "numeric", source: "x: 1.5\n", typeName: "numeric", initial: 1.5},
		{name: "boolean", source: "x: false\n", typeName: "boolean", initial: false},
		{name: "literal_list", source: "x: [\"urgent\", \"review\"]\n", typeName: "[text]", initial: []any{"urgent", "review"}},
		{name: "indexed", source: "x: {type: text, indexed: true}\n", typeName: "text", indexed: true},
		{name: "plain_nontype", source: "x: new\n", refusal: "text initial"},
		{name: "quoted_type", source: "x: \"text\"\n", refusal: "remove the quotes"},
		{name: "quoted_record", source: "x: 'Profile'\n", refusal: "remove the quotes"},
		{name: "empty_list", source: "x: []\n", refusal: "empty lists"},
		{name: "mixed_list", source: "x: [1, true]\n", refusal: "homogeneous"},
		{name: "multiple_types", source: "x: [text, integer]\n", refusal: "must be quoted"},
		{name: "unknown_type_list", source: "x: [Missing]\n", refusal: "not a declared type"},
		{name: "initial_mapping", source: "x: {type: text, initial: new}\n", refusal: "retired"},
		{name: "plain_mapping", source: "x: {type: text}\n", refusal: "only state field mapping"},
		{name: "indexed_false", source: "x: {type: text, indexed: false}\n", refusal: "indexed: true"},
		{name: "unused", source: "x: {type: text, _unused_reason: fixture only}\n", refusal: "retired"},
		{name: "null", source: "x: null\n", refusal: "non-null literal"},
		{name: "wrapper", source: "account:\n  x: text\n", refusal: "not supported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, err := yamlsource.Load([]byte(tc.source))
			if err != nil {
				t.Fatal(err)
			}
			declarations, err := projectStateContract(source.Document("state.yaml").Root(), catalog)
			if tc.refusal != "" {
				if err == nil || !strings.Contains(err.Error(), tc.refusal) {
					t.Fatalf("admission error = %v, want %q", err, tc.refusal)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			field := declarations.Fields["x"]
			if field.Type != tc.typeName || field.IsOptional != tc.optional || field.Indexed != tc.indexed || !reflect.DeepEqual(field.Initial, tc.initial) {
				t.Fatalf("state field = %#v, want type=%s initial=%#v optional=%v indexed=%v", field, tc.typeName, tc.initial, tc.optional, tc.indexed)
			}
		})
	}
}
