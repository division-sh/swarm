package contracts

import (
	"path/filepath"
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
		{name: "nested_list_type", source: "x: [[Profile]]\n", typeName: "[[Profile]]"},
		{name: "deep_list_type", source: "x: [[[integer]]]\n", typeName: "[[[integer]]]"},
		{name: "indexed_nested_list_type", source: "x: {type: [[Profile]], indexed: true}\n", typeName: "[[Profile]]", indexed: true},
		{name: "nested_unknown_type", source: "x: [[Missing]]\n", refusal: "not a declared type"},
		{name: "nested_empty_list", source: "x: [[]]\n", refusal: "[T]"},
		{name: "nested_mixed_list", source: "x: [[1, true]]\n", refusal: "[T]"},
		{name: "nested_multiple_types", source: "x: [[text, integer]]\n", refusal: "[T]"},
		{name: "nested_quoted_type", source: "x: [['Profile']]\n", refusal: "quoted literals are data"},
		{name: "text", source: "x: \"new\"\n", typeName: "text", initial: "new"},
		{name: "empty_text", source: "x: ''\n", typeName: "text", initial: ""},
		{name: "integer", source: "x: 0\n", typeName: "integer", initial: 0},
		{name: "numeric", source: "x: 1.5\n", typeName: "numeric", initial: 1.5},
		{name: "boolean", source: "x: false\n", typeName: "boolean", initial: false},
		{name: "literal_list", source: "x: [\"urgent\", \"review\"]\n", typeName: "[text]", initial: []any{"urgent", "review"}},
		{name: "indexed", source: "x: {type: text, indexed: true}\n", typeName: "text", indexed: true},
		{name: "plain_nontype", source: "x: new\n", refusal: "text initial"},
		{name: "quoted_type", source: "x: \"text\"\n", typeName: "text", initial: "text"},
		{name: "quoted_record", source: "x: 'Profile'\n", typeName: "text", initial: "Profile"},
		{name: "quoted_list", source: "x: '[text]'\n", typeName: "text", initial: "[text]"},
		{name: "quoted_retired_list", source: "x: 'list<text>'\n", typeName: "text", initial: "list<text>"},
		{name: "quoted_indexed_type", source: "x: {type: 'Profile?', indexed: true}\n", typeName: "Profile", optional: true, indexed: true},
		{name: "indexed_list", source: "x: {type: '[Profile]', indexed: true}\n", typeName: "[Profile]", indexed: true},
		{name: "retired_list_type", source: "x: list<text>\n", refusal: "[T]"},
		{name: "retired_indexed_list_type", source: "x: {type: 'list<text>', indexed: true}\n", refusal: "[T]"},
		{name: "retired_long_list", source: "x: {type: list, of: text, indexed: true}\n", refusal: "[T]"},
		{name: "retired_nested_list", source: "x: map[text][]text\n", refusal: "use [text]"},
		{name: "empty_list", source: "x: []\n", refusal: "empty lists"},
		{name: "mixed_list", source: "x: [1, true]\n", refusal: "homogeneous"},
		{name: "multiple_types", source: "x: [text, integer]\n", refusal: "must be quoted"},
		{name: "unknown_type_list", source: "x: [Missing]\n", refusal: "not a declared type"},
		{name: "initial_mapping", source: "x: {type: text, initial: new}\n", refusal: "retired"},
		{name: "retired_materialize_from", source: "x: {type: [Profile], materialize_from: collector.items}\n", refusal: "The accumulate target is the collected list"},
		{name: "retired_project", source: "x: {type: [Profile], project: {name: source.name}}\n", refusal: "The accumulate target is the collected list"},
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
	t.Run("ordinary_field_names", func(t *testing.T) {
		source, err := yamlsource.Load([]byte("_description: text\n_owner: text\nstate: text\nid: text\nstage: text\ncurrent_state: text\nflow_instance: text\ngates: text\n"))
		if err != nil {
			t.Fatal(err)
		}
		state, err := projectStateContract(source.Document("state.yaml").Root(), catalog)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"_description", "_owner", "state", "id", "stage", "current_state", "flow_instance", "gates"} {
			if field, ok := state.Fields[name]; !ok || field.Type != "text" {
				t.Fatalf("ordinary state field %q = %#v, present=%v", name, field, ok)
			}
		}
	})
	t.Run("retained_flow_binding", func(t *testing.T) {
		root := t.TempDir()
		for file, body := range map[string]string{
			"schema.yaml":           "name: provisional-root\nstages: {done: {final: true}}\n",
			"types.yaml":            "types:\n  Profile:\n    name: text\n",
			"state.yaml":            "profile: Profile\nready: false\n",
			"child/schema.yaml":     "name: child\nstages: {done: {final: true}}\n",
			"child/types.yaml":      "types:\n  Local:\n    count: integer\n",
			"child/state.yaml":      "profile: Profile\nlocal: Local\n",
			"stateless/schema.yaml": "name: stateless\nstages: {done: {final: true}}\n",
		} {
			writeFixtureFile(t, filepath.Join(root, file), body)
		}
		repo := repoRootForContractsTest(t)
		bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
		if err != nil {
			t.Fatal(err)
		}
		primary, err := bundle.ResolveRootPrimaryEntity()
		if err != nil || primary.EntityType != stateEntityType || primary.Contract.Fields["ready"].Initial != false {
			t.Fatalf("root retained state binding: %#v, %v", primary, err)
		}
		child, err := bundle.ResolveFlowPrimaryEntity("child")
		if err != nil || child.EntityType != stateEntityType || child.Contract.Fields["profile"].Type != "Profile" || child.Contract.Fields["local"].Type != "Local" {
			t.Fatalf("child state/catalog binding: %#v, %v", child, err)
		}
		if _, _, present := bundle.FlowPrimaryEntityContract("stateless"); present {
			t.Fatal("flow without state.yaml acquired a state contract")
		}
		if bundle.FlowSources["."].State != "state.yaml" || bundle.FlowSources["child"].State != "child/state.yaml" {
			t.Fatal("state binding lost the exact retained source labels")
		}
	})
}
