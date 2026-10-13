package contracts

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestCatalogAdmissionAuthoredNodeLists(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte(`labels: [text]
amounts:
  - numeric(5,2)
items: [DimensionScore]
amount: {type: 'numeric(5,2)', default: 0}
snapshot: jsonb
explicit: {type: '[text]', default: []}
`))
	if err != nil {
		t.Fatal(err)
	}
	fields, err := projectNodeStateFieldsValue(snapshot.Document("nodes.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	types := make(map[string]string, len(fields))
	for _, field := range fields {
		types[field.Name] = field.Type
		if field.Name == "explicit" && !reflect.DeepEqual(field.Default, []any{}) {
			t.Fatalf("explicit empty-array default changed: %#v", field.Default)
		}
	}
	if want := (map[string]string{
		"labels": "[text]", "amounts": "[numeric(5,2)]", "items": "[DimensionScore]",
		"amount": "numeric(5,2)", "snapshot": "jsonb", "explicit": "[text]",
	}); !reflect.DeepEqual(types, want) {
		t.Fatalf("authored node declaration lost exact type/precision: %v, want %v", types, want)
	}
	for _, ref := range []string{"text[]", "[]text", "DimensionScore[]", "[]DimensionScore", "numeric(5,2)[]", "[]numeric(5,2)", " text[] "} {
		t.Run(ref, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte("items: {type: '" + ref + "'}\n"))
			if err != nil {
				t.Fatal(err)
			}
			_, err = projectNodeStateFieldsValue(snapshot.Document("nodes.yaml").Root())
			diagnostic, typed := AsLoaderDiagnostic(err)
			if !typed || diagnostic.Code != "contract_loader.invalid_type_reference" || !strings.Contains(err.Error(), "nodes.yaml") || !strings.Contains(err.Error(), "retired") {
				t.Fatalf("authored node retired type lacks typed located refusal: %v", err)
			}
		})
	}
	for _, source := range []string{
		"items: '[text]'\n", "items: ['text']\n", "items: [numeric(5,2)]\n",
		"items: ' [text]'\n", "items: \" [text]\"\n",
		"items: |\n  [text]\n", "items: >\n  [text]\n",
		"items: {type: [text]}\n", "items: []\n", "items: [[text]]\n",
	} {
		t.Run(source, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte(source))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := projectNodeStateFieldsValue(snapshot.Document("nodes.yaml").Root()); err == nil {
				t.Fatalf("invalid node declaration style admitted: %q", source)
			}
		})
	}
}

func TestCatalogAdmissionAuthoredNodeNamedListIdentity(t *testing.T) {
	for _, entry := range []struct{ source, want string }{
		{"List", "List"}, {"LIST", "LIST"},
		{"[List]", "[List]"}, {"[LIST]", "[LIST]"},
		{"{type: List}", "List"}, {"{type: LIST}", "LIST"},
		{"{type: '[List]'}", "[List]"}, {"{type: '[LIST]'}", "[LIST]"},
	} {
		t.Run(entry.source, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte("items: " + entry.source + "\n"))
			if err != nil {
				t.Fatal(err)
			}
			fields, err := projectNodeStateFieldsValue(snapshot.Document("nodes.yaml").Root())
			if err != nil || len(fields) != 1 || fields[0].Type != entry.want {
				t.Fatalf("declared named identity changed: %+v, %v; want %s", fields, err, entry.want)
			}
		})
	}
	for _, source := range []string{"list", "{type: list}", "'[List]'", "{type: list, of: List}"} {
		t.Run(source, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte("items: " + source + "\n"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := projectNodeStateFieldsValue(snapshot.Document("nodes.yaml").Root()); err == nil {
				t.Fatalf("invalid style or unknown metadata gained authority: %q", source)
			}
		})
	}
}

func TestCatalogAdmissionNodeRetirementAllocation(t *testing.T) {
	for _, source := range []string{
		"items: list<DimensionScore>\n",
		"items: 'list<DimensionScore>'\n",
		"items: {type: list<DimensionScore>, default: []}\n",
		"items: {type: ' list<DimensionScore> ', default: []}\n",
		"- name: items\n  type: list<DimensionScore>\n  default: []\n",
	} {
		t.Run(source, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte(source))
			if err != nil {
				t.Fatal(err)
			}
			fields, err := projectNodeStateFieldsValue(snapshot.Document("nodes.yaml").Root())
			if err != nil || len(fields) != 1 || fields[0].Name != "items" || fields[0].Type != "list<DimensionScore>" {
				t.Fatalf("D-owned retirement changed existing node carrier: %+v, %v", fields, err)
			}
			if strings.Contains(source, "default:") && !reflect.DeepEqual(fields[0].Default, []any{}) {
				t.Fatalf("existing metadata default changed: %#v", fields[0].Default)
			}
		})
	}
	snapshot, err := yamlsource.Load([]byte("items: {type: list, of: DimensionScore}\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = projectNodeStateFieldsValue(snapshot.Document("nodes.yaml").Root())
	diagnostic, typed := AsLoaderDiagnostic(err)
	if !typed || diagnostic.Code != "contract_loader.undefined_field" || !strings.Contains(err.Error(), "of") {
		t.Fatalf("existing unknown metadata refusal changed: %v", err)
	}
}
