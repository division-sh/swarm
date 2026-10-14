package contracts

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// C01-C19 characterize the distinct laws of the complete factoring family.
func TestCatalogStructuralTypeFactoring(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"C01_Clone", catalogFactoringClone},
		{"C02_StructuralEqual", catalogFactoringEqual},
		{"C03_StructuralAssignable", catalogFactoringAssignable},
		{"C04_StructuralSyntax", catalogFactoringSyntax},
		{"C05_Field", catalogFactoringField},
		{"C06_FieldPath", catalogFactoringFieldPath},
		{"C07_ResolveJSONSchemaStructuralType", catalogFactoringJSONEntrance},
		{"C08_resolveJSONSchemaStructuralType", catalogFactoringJSONLowering},
		{"C09_Empty", catalogFactoringEmpty},
		{"C10_Resolve", catalogFactoringResolveScope},
		{"C11_ResolveReference", catalogFactoringResolveReference},
		{"C12_ResolveMapKey", catalogFactoringMapKey},
		{"C13_validateCatalogMapKey", catalogFactoringMapKeyMembership},
		{"C14_PlainText", catalogFactoringPlainText},
		{"C15_ComparableDomain", catalogFactoringComparableDomain},
		{"C16_catalogComparableDomain", catalogFactoringDomainTokens},
		{"C17_resolveCatalogTypeReference", catalogFactoringResolver},
		{"C18_ResolveEventFieldType", catalogFactoringEventField},
		{"C19_ResolveExecutableNodeEventFieldType", catalogFactoringNodeField},
	} {
		t.Run(tc.name, tc.run)
	}
}

func TestCatalogStructuralTypeFactoringPostDPrecedence(t *testing.T) {
	catalog := catalogFactoringCatalog()
	catalog.Scalars["Retired"] = ScalarTypeDecl{Base: "list<text>"}
	for _, ref := range []string{"list", "list<text>", "[list<text>]", "map[integer]list<Missing>", "map[Missing]list<text>", "Retired"} {
		t.Run("retirement_before_nested_resolution/"+ref, func(t *testing.T) {
			input := CatalogTypeReference{Type: ref, Catalog: catalog}
			for name, resolve := range map[string]func() (ResolvedCatalogType, error){
				"Resolve": input.Resolve,
				"ResolveReference": func() (ResolvedCatalogType, error) {
					return (CatalogTypeReference{Type: "Missing", Catalog: catalog}).ResolveReference(" " + ref + " ")
				},
				"ResolveMapKey": input.ResolveMapKey,
			} {
				t.Run(name, func(t *testing.T) {
					resolved, err := resolve()
					retired := ref
					if ref == "Retired" {
						retired = "list<text>"
					}
					want := ErrInvalidField.Error() + ": catalog type list type " + strconv.Quote(retired) + " is not supported; use [T]"
					if err == nil || err.Error() != want || !errors.Is(err, ErrInvalidField) || !reflect.DeepEqual(resolved, ResolvedCatalogType{}) {
						t.Fatalf("ordered retirement refusal = %#v / %v, want %q", resolved, err, want)
					}
				})
			}
			if input.Empty() || input.ComparableDomain() != "" {
				t.Fatal("a supplied retired reference obtained omission or domain credit")
			}
		})
	}
	for _, tc := range []struct{ ref, problem string }{
		{"text[]", `type reference "text[]" is retired; use [text]`},
		{"[]text", `type reference "[]text" is retired; use [text]`},
		{"[text[]]", `type reference "text[]" is retired; use [text]`},
		{"map[integer]text[]", `type reference "map[integer]text[]" is retired; use [map[integer]text]`},
	} {
		t.Run("B_spelling_gate/"+tc.ref, func(t *testing.T) {
			_, err := (CatalogTypeReference{Type: tc.ref, Catalog: catalog}).Resolve()
			catalogFactoringRefusal(t, err, tc.problem)
		})
	}
	for _, surface := range []string{"types", "entities", "events", "nodes"} {
		t.Run("source_decoder/"+surface, func(t *testing.T) {
			decode := func(source string) (string, error) {
				snapshot, err := yamlsource.Load([]byte("items: " + source + "\n"))
				if err != nil {
					t.Fatal(err)
				}
				fields, err := snapshot.Document("post-d.yaml").Root().Mapping()
				if err != nil || len(fields) != 1 {
					t.Fatalf("source mapping: %v", err)
				}
				value := fields[0].Value
				switch surface {
				case "types":
					field, err := projectTypeFieldSpec(value)
					return field.Type, err
				case "entities":
					field, err := projectEntityFieldDecl(value)
					return field.Type, err
				case "events":
					field, _, _, err := admitEventPayloadField(value)
					return field.Type, err
				case "nodes":
					field, err := projectNodeStateFieldValue("items", value)
					return field.Type, err
				default:
					t.Fatalf("unclassified surface %s", surface)
					return "", nil
				}
			}
			for _, source := range []string{"{type: Missing, of: null}", "{type: 'list<Missing>', of: null}"} {
				_, err := decode(source)
				if err == nil || !errors.Is(err, ErrInvalidField) || !strings.Contains(err.Error(), `field "of"`) || !strings.Contains(err.Error(), "post-d.yaml:") || !strings.Contains(err.Error(), "[T]") {
					t.Fatalf("long-form presence must win before reference admission: %s / %v", source, err)
				}
			}
			for _, source := range []string{"'[text]'", "['text']"} {
				if _, err := decode(source); err == nil || !errors.Is(err, ErrInvalidField) || !strings.Contains(err.Error(), "[T]") {
					t.Fatalf("styled data became a bare declaration: %s / %v", source, err)
				}
			}
			for _, source := range []string{"[text]", "\n  - text", "{type: '[text]'}"} {
				if typeRef, err := decode(source); err != nil || typeRef != "[text]" {
					t.Fatalf("canonical declaration = %s / %v", typeRef, err)
				}
			}
		})
	}
	bundle := catalogFactoringScopedBundle(t)
	for _, tc := range []struct {
		flow string
		want CatalogTypeKind
	}{{".", CatalogTypeText}, {"child", CatalogTypeInteger}} {
		t.Run("scoped_list/"+tc.flow, func(t *testing.T) {
			ref, found := ResolveEventFieldType(bundle, tc.flow, "record.ready", "items")
			if !found || ref.Type != "[Box]" {
				t.Fatalf("declaring list field missing: %#v / %v", ref, found)
			}
			resolved := catalogFactoringResolved(t, ref.Type, ref.Catalog)
			if resolved.Kind != CatalogTypeList || resolved.Element == nil {
				t.Fatalf("declaring list shape lost: %#v", resolved)
			}
			field, found := resolved.Element.Field("value")
			if !found || field.Type.Kind != tc.want {
				t.Fatalf("list element chose the wrong declaring catalog: %#v / %v", field, found)
			}
			if tc.flow == "child" {
				nodeRef, found := ResolveExecutableNodeEventFieldType(bundle, identitytest.ExecutableNode(t, "child", "consumer"), "record.ready", "items")
				if !found || nodeRef.Type != "[Box]" || nodeRef.ComparableDomain() != ref.ComparableDomain() {
					t.Fatalf("executable node lost the scoped list reference: %#v / %v", nodeRef, found)
				}
			}
		})
	}
}

func catalogFactoringCatalog() TypeCatalogDocument {
	return TypeCatalogDocument{
		Scalars: map[string]ScalarTypeDecl{"Label": {Base: "text"}, "ID": {Base: "uuid"}, "Time": {Base: "timestamp"}},
		Enums: map[string]EnumTypeDecl{
			"State": {Values: []string{"ready", "done"}, Default: "ready"},
			"Other": {Values: []string{"ready", "done"}, Default: "ready"},
		},
		Types: map[string]NamedTypeDecl{
			"Leaf": {Fields: map[string]TypeFieldSpec{"value": {Type: "Label"}}},
			"Pair": {Fields: map[string]TypeFieldSpec{"first": {Type: "Leaf"}, "second": {Type: "Leaf", IsOptional: true}}},
		},
	}
}

func catalogFactoringResolved(t *testing.T, ref string, catalog TypeCatalogDocument) ResolvedCatalogType {
	t.Helper()
	resolved, err := (CatalogTypeReference{Type: ref, Catalog: catalog}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func catalogFactoringRefusal(t *testing.T, err error, problem string) {
	t.Helper()
	diagnostic, ok := AsLoaderDiagnostic(err)
	if !ok || diagnostic.Code != "contract_loader.invalid_type_reference" || diagnostic.Problem != problem ||
		diagnostic.Remediation != "Declare a supported, nonrecursive catalog type." || !errors.Is(err, ErrInvalidField) {
		t.Fatalf("refusal = %v, want exact typed problem %q", err, problem)
	}
}

func catalogFactoringClone(t *testing.T) {
	minRange, maxRange, minLength, maxLength := 1.0, 9.0, 2, 8
	original := ResolvedCatalogType{Kind: CatalogTypeObject, Name: "Snapshot", Fields: []ResolvedCatalogField{
		{Name: "bounds", TypeRef: "text", Type: ResolvedCatalogType{Kind: CatalogTypeText}, IsOptional: true,
			Refinements: SchemaRefinements{Pattern: "^value", EqualTo: "peer", Length: SchemaLengthRefinement{Min: &minLength, Max: &maxLength}}},
		{Name: "score", TypeRef: "numeric", Type: ResolvedCatalogType{Kind: CatalogTypeNumber},
			Refinements: SchemaRefinements{EqualTo: "other", Range: SchemaRangeRefinement{Min: &minRange, Max: &maxRange}}},
		{Name: "items", TypeRef: "[map[uuid]Leaf]", Type: ResolvedCatalogType{Kind: CatalogTypeList, Element: &ResolvedCatalogType{
			Kind: CatalogTypeMap, Key: &ResolvedCatalogType{Kind: CatalogTypeText, Format: "uuid"}, Value: &ResolvedCatalogType{Kind: CatalogTypeObject, Name: "Leaf", Fields: []ResolvedCatalogField{
				{Name: "body", Type: ResolvedCatalogType{Kind: CatalogTypeDynamic, primitive: "json"}},
			}},
		}}},
		{Name: "peer", TypeRef: "text", Type: ResolvedCatalogType{Kind: CatalogTypeText}},
		{Name: "other", TypeRef: "numeric", Type: ResolvedCatalogType{Kind: CatalogTypeNumber}},
	}}
	before, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	copy := original.Clone()
	if copy.Name != "Snapshot" || copy.Fields[0].Name != "bounds" || copy.Fields[0].TypeRef != "text" || !copy.Fields[0].IsOptional ||
		copy.Fields[0].Refinements.Pattern != "^value" || copy.Fields[0].Refinements.EqualTo != "peer" ||
		copy.Fields[2].Type.Element.Key.Format != "uuid" || copy.Fields[2].Type.Element.Value.Fields[0].Type.primitive != "json" {
		t.Fatalf("clone erased metadata/order: %#v", copy)
	}
	copy.Fields[0].Name = "changed"
	*copy.Fields[1].Refinements.Range.Min = 100
	*copy.Fields[1].Refinements.Range.Max = 200
	*copy.Fields[0].Refinements.Length.Min = 100
	*copy.Fields[0].Refinements.Length.Max = 200
	copy.Fields[2].Type.Element.Key.Format = "changed"
	copy.Fields[2].Type.Element.Value.Fields[0].Type.primitive = "changed"
	after, err := json.Marshal(original)
	if err != nil || string(after) != string(before) || original.Fields[2].Type.Element.Value.Fields[0].Type.primitive != "json" {
		t.Fatalf("clone mutation changed original: %s -> %s, %v", before, after, err)
	}
	for _, fields := range [][]ResolvedCatalogField{nil, {}} {
		if got := (ResolvedCatalogType{Fields: fields}).Clone(); got.Fields == nil || len(got.Fields) != 0 {
			t.Fatalf("empty clone slice behavior changed: %#v", got.Fields)
		}
	}
	// The immutable compiled owner, unlike a caller-owned Field lookup, must
	// return a fresh view after a returned structural value was mutated.
	compiled, err := CompileImportedEventSchema(".", "snapshot.ready", EventCatalogEntry{Payload: EventPayloadSpec{
		Properties: map[string]EventFieldSpec{"label": {Type: "text", Refinements: SchemaRefinements{Length: SchemaLengthRefinement{Min: &minLength}}}},
		Required:   []string{"label"},
	}}, CompiledEventSchemaSource{})
	if err != nil {
		t.Fatal(err)
	}
	acceptance := compiled.CanonicalAcceptanceSchema()
	view, ok := compiled.StructuralType()
	if !ok {
		t.Fatal("compiled structure missing")
	}
	*view.Fields[0].Refinements.Length.Min = 100
	fresh, ok := compiled.StructuralType()
	if !ok || *fresh.Fields[0].Refinements.Length.Min != 2 || string(compiled.CanonicalAcceptanceSchema()) != string(acceptance) {
		t.Fatal("returned view changed retained compiled evidence")
	}
}

func catalogFactoringEqual(t *testing.T) {
	left := ResolvedCatalogType{Kind: CatalogTypeObject, Name: "Left", Fields: []ResolvedCatalogField{
		{Name: "z", Type: ResolvedCatalogType{Kind: CatalogTypeInteger}},
		{Name: "a", Type: ResolvedCatalogType{Kind: CatalogTypeText}, IsOptional: true},
	}}
	right := ResolvedCatalogType{Kind: CatalogTypeObject, Name: "Right", Fields: []ResolvedCatalogField{left.Fields[1], left.Fields[0]}}
	if !StructuralCatalogTypesEqual(left, right) || !StructuralCatalogTypesEqual(right, left) {
		t.Fatal("field order/diagnostic name became executable shape")
	}
	for _, change := range []string{"presence", "name", "kind", "count"} {
		t.Run(change, func(t *testing.T) {
			changed := right.Clone()
			switch change {
			case "presence":
				changed.Fields[0].IsOptional = false
			case "name":
				changed.Fields[0].Name = "other"
			case "kind":
				changed.Fields[0].Type.Kind = CatalogTypeBoolean
			case "count":
				changed.Fields = changed.Fields[:1]
			}
			if StructuralCatalogTypesEqual(left, changed) || StructuralCatalogTypesEqual(changed, left) {
				t.Fatalf("%s difference ignored", change)
			}
		})
	}
	for _, pair := range [][2]string{{"text", "State"}, {"uuid", "timestamp"}, {"json", "object"}} {
		left := catalogFactoringResolved(t, pair[0], catalogFactoringCatalog())
		right := catalogFactoringResolved(t, pair[1], catalogFactoringCatalog())
		if !StructuralCatalogTypesEqual(left, right) {
			t.Fatal("structural equality substituted declared-domain comparison")
		}
	}
	for _, incomplete := range []ResolvedCatalogType{{Kind: CatalogTypeList}, {Kind: CatalogTypeMap, Key: &ResolvedCatalogType{Kind: CatalogTypeText}}} {
		if StructuralCatalogTypesEqual(incomplete, incomplete) {
			t.Fatal("missing container children received shape equality")
		}
	}
}

func catalogFactoringAssignable(t *testing.T) {
	integer, number := ResolvedCatalogType{Kind: CatalogTypeInteger}, ResolvedCatalogType{Kind: CatalogTypeNumber}
	for _, tc := range []struct {
		name           string
		source, target ResolvedCatalogType
		want           bool
	}{
		{"scalar_widening", integer, number, true},
		{"reverse", number, integer, false},
		{"text_mismatch", ResolvedCatalogType{Kind: CatalogTypeText}, number, false},
		{"equal_lists", ResolvedCatalogType{Kind: CatalogTypeList, Element: &integer}, ResolvedCatalogType{Kind: CatalogTypeList, Element: &integer}, true},
		{"no_recursive_widening", ResolvedCatalogType{Kind: CatalogTypeList, Element: &integer}, ResolvedCatalogType{Kind: CatalogTypeList, Element: &number}, false},
		{"required_optional", ResolvedCatalogType{Kind: CatalogTypeObject, Fields: []ResolvedCatalogField{{Name: "value", Type: integer}}}, ResolvedCatalogType{Kind: CatalogTypeObject, Fields: []ResolvedCatalogField{{Name: "value", Type: integer, IsOptional: true}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := StructuralCatalogTypeAssignable(tc.source, tc.target); got != tc.want {
				t.Fatalf("assignment = %v, want %v", got, tc.want)
			}
		})
	}
}

func catalogFactoringSyntax(t *testing.T) {
	integer := ResolvedCatalogType{Kind: CatalogTypeInteger}
	object := ResolvedCatalogType{Kind: CatalogTypeObject, Name: "Ignored", Fields: []ResolvedCatalogField{
		{Name: "z", Type: ResolvedCatalogType{Kind: CatalogTypeList, Element: &integer}},
		{Name: "a", Type: ResolvedCatalogType{Kind: CatalogTypeText, Format: "uuid"}, IsOptional: true},
	}}
	for _, tc := range []struct {
		typ  ResolvedCatalogType
		want string
	}{
		{object, "{a:text?,z:[integer]}"},
		{ResolvedCatalogType{Kind: CatalogTypeMap, Key: &ResolvedCatalogType{Kind: CatalogTypeText}, Value: &object}, "map<text,{a:text?,z:[integer]}>"},
		{ResolvedCatalogType{Kind: CatalogTypeList}, "[dynamic]"},
		{ResolvedCatalogType{Kind: CatalogTypeMap}, "map<dynamic,dynamic>"},
		{ResolvedCatalogType{Kind: CatalogTypeNumber}, "numeric"},
		{ResolvedCatalogType{Kind: CatalogTypeBoolean}, "boolean"},
		{ResolvedCatalogType{}, "dynamic"},
	} {
		if got := StructuralCatalogTypeSyntax(tc.typ); got != tc.want {
			t.Fatalf("syntax = %q, want %q", got, tc.want)
		}
	}
	if object.Fields[0].Name != "z" || object.Fields[1].Name != "a" {
		t.Fatal("diagnostic sorting mutated receiver field order")
	}
}

func catalogFactoringField(t *testing.T) {
	minimum := 2
	owner := ResolvedCatalogType{Kind: CatalogTypeObject, Fields: []ResolvedCatalogField{{Name: "value", TypeRef: "Label", Type: ResolvedCatalogType{Kind: CatalogTypeText}, IsOptional: true, Refinements: SchemaRefinements{Length: SchemaLengthRefinement{Min: &minimum}}}}}
	field, found := owner.Field(" value ")
	if !found || field.Name != "value" || field.TypeRef != "Label" || !field.IsOptional || *field.Refinements.Length.Min != 2 {
		t.Fatalf("field metadata = %#v/%v", field, found)
	}
	for _, name := range []string{"", "missing", "Value"} {
		if field, found := owner.Field(name); found || !reflect.DeepEqual(field, ResolvedCatalogField{}) {
			t.Fatalf("nonexistent field %q resolved: %#v/%v", name, field, found)
		}
	}
	*field.Refinements.Length.Min = 3
	if *owner.Fields[0].Refinements.Length.Min != 3 {
		t.Fatal("Field unexpectedly acquired deep-copy semantics for caller-owned data")
	}
}

func catalogFactoringFieldPath(t *testing.T) {
	leaf := ResolvedCatalogField{Name: "label", TypeRef: "Label", Type: ResolvedCatalogType{Kind: CatalogTypeText}, IsOptional: true}
	owner := ResolvedCatalogType{Kind: CatalogTypeObject, Fields: []ResolvedCatalogField{
		{Name: "child", Type: ResolvedCatalogType{Kind: CatalogTypeObject, Fields: []ResolvedCatalogField{leaf}}},
		{Name: "dictionary", Type: ResolvedCatalogType{Kind: CatalogTypeMap, Key: &ResolvedCatalogType{Kind: CatalogTypeText}, Value: &leaf.Type}},
		{Name: "items", Type: ResolvedCatalogType{Kind: CatalogTypeList, Element: &leaf.Type}},
	}}
	if got, found := owner.FieldPath(" child . label "); !found || !reflect.DeepEqual(got, leaf) {
		t.Fatalf("nested field = %#v/%v", got, found)
	}
	for _, path := range []string{"", " ", ".child", "child.", "child..label", "child.missing", "dictionary.label", "items.label", "child.label.extra"} {
		if got, found := owner.FieldPath(path); found || !reflect.DeepEqual(got, ResolvedCatalogField{}) {
			t.Fatalf("non-record path %q resolved: %#v/%v", path, got, found)
		}
	}
}

func catalogFactoringJSONEntrance(t *testing.T) {
	schema := map[string]any{"type": "object", "properties": map[string]any{
		"revision": map[string]any{"type": "string", "format": "uuid", "enum": []any{"00000000-0000-4000-8000-000000000001"}},
		"child":    map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "integer"}}},
	}, "required": []string{"revision"}}
	admitted, err := AdmitToolInputSchemaMap(schema)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := ResolveJSONSchemaStructuralType(admitted.Projection(), " Imported ")
	if err != nil || resolved.Name != "Imported" {
		t.Fatalf("diagnostic identity = %#v/%v", resolved, err)
	}
	revision, found := resolved.Field("revision")
	if !found || revision.IsOptional || revision.Type.Kind != CatalogTypeText || revision.Type.Format != "" || revision.Type.Name != "" {
		t.Fatal("shape lowering inferred declared enum/format authority or lost presence")
	}
	child, found := resolved.Field("child")
	if !found || !child.IsOptional || child.Type.Name != "Imported.child" {
		t.Fatalf("nested identity/presence = %#v/%v", child, found)
	}
	if resolved.Fields[0].Name != "child" || resolved.Fields[1].Name != "revision" {
		t.Fatal("JSON field lowering lost canonical name order")
	}
	_, err = ResolveJSONSchemaStructuralType(map[string]any{"type": "unsupported"}, "Imported")
	if err == nil || err.Error() != `unsupported structural schema type "unsupported"` {
		t.Fatalf("defensive input error = %v", err)
	}
}

func catalogFactoringJSONLowering(t *testing.T) {
	for _, tc := range []struct {
		typeName string
		kind     CatalogTypeKind
	}{
		{"string", CatalogTypeText}, {"integer", CatalogTypeInteger}, {"number", CatalogTypeNumber},
		{"boolean", CatalogTypeBoolean}, {"json", CatalogTypeDynamic}, {"jsonb", CatalogTypeDynamic},
	} {
		got, err := resolveJSONSchemaStructuralType(map[string]any{"type": tc.typeName}, "Imported", "")
		if err != nil || got.Kind != tc.kind {
			t.Fatalf("lowering %s = %#v/%v", tc.typeName, got, err)
		}
	}
	array, err := resolveJSONSchemaStructuralType(map[string]any{"type": "array"}, "Imported", "")
	if err != nil || array.Kind != CatalogTypeList || array.Element == nil || array.Element.Kind != CatalogTypeDynamic {
		t.Fatalf("absent items shape = %#v/%v", array, err)
	}
	// Missing items above is a defensive projection, not a supported producer
	// schema. This independent positive passes the actual JSON-schema owner.
	admittedArray := MustToolInputSchema(ToolSchemaArray, ToolSchemaItems(MustToolInputSchema(ToolSchemaObject,
		ToolSchemaProperties(map[string]ToolInputSchema{"z": MustToolInputSchema(ToolSchemaInteger), "a": MustToolInputSchema(ToolSchemaString)}), ToolSchemaRequired("z"))))
	array, err = resolveJSONSchemaStructuralType(admittedArray.Projection(), "Imported", "rows")
	if err != nil || array.Element == nil || array.Element.Name != "Imported.rows[]" || len(array.Element.Fields) != 2 ||
		array.Element.Fields[0].Name != "a" || !array.Element.Fields[0].IsOptional || array.Element.Fields[1].Name != "z" || array.Element.Fields[1].IsOptional {
		t.Fatalf("admitted array/object child path and presence = %#v/%v", array, err)
	}
	dictionary, err := resolveJSONSchemaStructuralType(map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "number"}}, "Imported", "")
	if err != nil || dictionary.Kind != CatalogTypeMap || dictionary.Key == nil || dictionary.Key.Kind != CatalogTypeText || dictionary.Value == nil || dictionary.Value.Kind != CatalogTypeNumber {
		t.Fatalf("dictionary shape = %#v/%v", dictionary, err)
	}
	for _, tc := range []struct {
		schema map[string]any
		want   string
	}{
		{map[string]any{"type": "object", "properties": map[string]any{"broken": true}}, "structural schema field broken is bool, want object"},
		{map[string]any{"type": "object", "properties": map[string]any{"outer": map[string]any{"type": "object", "properties": map[string]any{"inner": map[string]any{"type": "unsupported"}}}}}, `structural schema field outer: structural schema field outer.inner: unsupported structural schema type "unsupported"`},
	} {
		_, err := resolveJSONSchemaStructuralType(tc.schema, "Imported", "")
		if err == nil || err.Error() != tc.want {
			t.Fatalf("defensive nested error = %v, want %q", err, tc.want)
		}
	}
}

func catalogFactoringEmpty(t *testing.T) {
	for _, raw := range []string{"", " \t "} {
		ref := CatalogTypeReference{Type: raw, Catalog: catalogFactoringCatalog()}
		if !ref.Empty() {
			t.Fatalf("omission predicate false for %q", raw)
		}
		_, err := ref.Resolve()
		catalogFactoringRefusal(t, err, "catalog type reference is required")
	}
	if (CatalogTypeReference{Type: " text "}).Empty() {
		t.Fatal("supplied reference became omission")
	}
}

func catalogFactoringResolveScope(t *testing.T) {
	left := catalogFactoringCatalog()
	right := catalogFactoringCatalog()
	right.Scalars["Label"] = ScalarTypeDecl{Base: "integer"}
	for _, tc := range []struct {
		catalog TypeCatalogDocument
		want    CatalogTypeKind
	}{{left, CatalogTypeText}, {right, CatalogTypeInteger}} {
		resolved := catalogFactoringResolved(t, "Leaf", tc.catalog)
		field, found := resolved.Field("value")
		if !found || field.Type.Kind != tc.want || field.TypeRef != "Label" {
			t.Fatalf("receiver catalog was replaced: %#v/%v", field, found)
		}
	}
	_, err := (CatalogTypeReference{Type: "Missing", Catalog: left}).Resolve()
	catalogFactoringRefusal(t, err, `unknown catalog type "Missing"`)
}

func catalogFactoringResolveReference(t *testing.T) {
	ref := CatalogTypeReference{Type: "integer", Catalog: catalogFactoringCatalog()}
	ref.Catalog.Types["Cycle"] = NamedTypeDecl{Fields: map[string]TypeFieldSpec{"child": {Type: "Cycle"}}}
	resolved, err := ref.ResolveReference(" Label ")
	if err != nil || !resolved.PlainText() {
		t.Fatalf("explicit reference did not override receiver.Type: %#v/%v", resolved, err)
	}
	_, err = ref.ResolveReference("Cycle")
	catalogFactoringRefusal(t, err, `recursive catalog type "Cycle" is unsupported`)
	for index := 0; index < 2; index++ {
		resolved, err := ref.ResolveReference("Pair")
		if err != nil || len(resolved.Fields) != 2 || !resolved.Fields[1].IsOptional || !StructuralCatalogTypesEqual(resolved.Fields[0].Type, resolved.Fields[1].Type) {
			t.Fatalf("failed call/shared DAG contaminated resolution: %#v/%v", resolved, err)
		}
	}
}

func catalogFactoringMapKey(t *testing.T) {
	catalog := catalogFactoringCatalog()
	for _, ref := range []string{"text", "Label", "uuid", "ID", "State"} {
		key, err := (CatalogTypeReference{Type: ref, Catalog: catalog}).ResolveMapKey()
		if err != nil || !reflect.DeepEqual(key, catalogFactoringResolved(t, ref, catalog)) {
			t.Fatalf("key domain %s was erased: %#v/%v", ref, key, err)
		}
		// Actual dictionary schema projection must retain the same key domain.
		schema := eventSchemaForResolvedType("map["+ref+"]Leaf", catalog, map[string]struct{}{})
		if schema["type"] != "object" || !reflect.DeepEqual(schema["propertyNames"], eventSchemaForResolvedType(ref, catalog, map[string]struct{}{})) {
			t.Fatalf("map schema lost key domain %s: %#v", ref, schema)
		}
	}
	for _, ref := range []string{"integer", "numeric", "boolean", "timestamp", "Time", "Leaf", "[Label]", "json"} {
		_, err := (CatalogTypeReference{Type: ref, Catalog: catalog}).ResolveMapKey()
		catalogFactoringRefusal(t, err, "map key type \""+ref+"\" is unsupported; map keys must be text, enum or uuid")
	}
	_, err := (CatalogTypeReference{Type: "Missing", Catalog: catalog}).ResolveMapKey()
	catalogFactoringRefusal(t, err, `unknown catalog type "Missing"`)
}

func catalogFactoringMapKeyMembership(t *testing.T) {
	catalog := catalogFactoringCatalog()
	for _, ref := range []string{"Label", "ID", "State"} {
		if err := validateCatalogMapKey(catalogFactoringResolved(t, ref, catalog), ref, catalog); err != nil {
			t.Fatal(err)
		}
	}
	foreign := TypeCatalogDocument{Enums: map[string]EnumTypeDecl{"Foreign": {Values: []string{"ready"}, Default: "ready"}}}
	key := catalogFactoringResolved(t, "Foreign", foreign)
	catalogFactoringRefusal(t, validateCatalogMapKey(key, "Foreign", catalog), `map key type "Foreign" is unsupported; map keys must be text, enum or uuid`)
	for _, ref := range []string{"Time", "integer", "Leaf"} {
		catalogFactoringRefusal(t, validateCatalogMapKey(catalogFactoringResolved(t, ref, catalog), ref, catalog), "map key type \""+ref+"\" is unsupported; map keys must be text, enum or uuid")
	}
}

func catalogFactoringPlainText(t *testing.T) {
	catalog := catalogFactoringCatalog()
	for _, tc := range []struct {
		ref  string
		want bool
	}{{"text", true}, {"Label", true}, {"uuid", false}, {"ID", false}, {"timestamp", false}, {"Time", false}, {"State", false}, {"[Label]", false}, {"Leaf", false}, {"json", false}} {
		resolved := catalogFactoringResolved(t, tc.ref, catalog)
		if got := resolved.PlainText(); got != tc.want {
			t.Fatalf("plain-text domain %s = %v", tc.ref, got)
		}
		if got := compiledFileTextField(ResolvedCatalogField{TypeRef: tc.ref, Type: resolved}, catalog); got != tc.want {
			t.Fatalf("file-text consumer %s = %v", tc.ref, got)
		}
		if got := criteriaCitationTypeAllowed(CatalogTypeReference{Type: tc.ref, Catalog: catalog}); got != (tc.want || tc.ref == "[Label]") {
			t.Fatalf("citation consumer %s = %v", tc.ref, got)
		}
	}
}

func catalogFactoringComparableDomain(t *testing.T) {
	catalog := catalogFactoringCatalog()
	for _, pair := range [][2]string{{"Label", "text"}, {"ID", "uuid"}, {"Time", "timestamp"}, {"[Label]", "[text]"}, {"map[ID]Leaf", "map[uuid]Leaf"}} {
		left := (CatalogTypeReference{Type: pair[0], Catalog: catalog}).ComparableDomain()
		right := (CatalogTypeReference{Type: pair[1], Catalog: catalog}).ComparableDomain()
		if left == "" || left != right {
			t.Fatalf("equivalent domains %v diverged: %q/%q", pair, left, right)
		}
		if errs := validateSchemaRefinementFields("factoring", catalog, map[string]schemaRefinementField{
			"left": {Name: "left", TypeRef: pair[0], Refinements: SchemaRefinements{EqualTo: "right"}}, "right": {Name: "right", TypeRef: pair[1]},
		}); len(errs) != 0 {
			t.Fatalf("actual equality consumer refused equivalent domains %v: %v", pair, errs)
		}
	}
	for _, pair := range [][2]string{{"State", "Other"}, {"State", "text"}, {"uuid", "text"}, {"timestamp", "uuid"}, {"integer", "numeric"}, {"json", "object"}} {
		left := (CatalogTypeReference{Type: pair[0], Catalog: catalog}).ComparableDomain()
		right := (CatalogTypeReference{Type: pair[1], Catalog: catalog}).ComparableDomain()
		if left == right {
			t.Fatalf("distinct domains %v collapsed to %q", pair, left)
		}
		if errs := validateSchemaRefinementFields("factoring", catalog, map[string]schemaRefinementField{
			"left": {Name: "left", TypeRef: pair[0], Refinements: SchemaRefinements{EqualTo: "right"}}, "right": {Name: "right", TypeRef: pair[1]},
		}); len(errs) == 0 {
			t.Fatalf("actual equality consumer accepted different domains %v", pair)
		}
	}
	if got := (CatalogTypeReference{Type: "Missing", Catalog: catalog}).ComparableDomain(); got != "" {
		t.Fatalf("unresolved reference obtained domain credit: %q", got)
	}
}

func catalogFactoringDomainTokens(t *testing.T) {
	catalog := catalogFactoringCatalog()
	for _, tc := range []struct{ ref, want string }{
		{"Label", "string"}, {"ID", "uuid"}, {"Time", "timestamp"}, {"State", "enum:State"},
		{"integer", "integer"}, {"numeric", "number"}, {"boolean", "boolean"}, {"jsonb", "json"},
		{"object", "object"}, {"array", "array"}, {"Leaf", "type:Leaf"},
		{"[ID]", "list:uuid"}, {"map[State][Time]", "map:enum:State:list:timestamp"},
	} {
		resolved := catalogFactoringResolved(t, tc.ref, catalog)
		if got := catalogComparableDomain(resolved); got != tc.want {
			t.Fatalf("domain token %s = %q, want %q", tc.ref, got, tc.want)
		}
	}
	if got := catalogComparableDomain(ResolvedCatalogType{Kind: CatalogTypeDynamic}); got != "" {
		t.Fatalf("unowned JSON shape acquired declared domain: %q", got)
	}
}

func catalogFactoringResolver(t *testing.T) {
	catalog := catalogFactoringCatalog()
	for _, ref := range []string{"Label", "State", "Pair", "[Pair]", "map[Label]Pair"} {
		resolving := map[string]struct{}{"unrelated": {}}
		if _, err := resolveCatalogTypeReference(ref, catalog, resolving); err != nil || len(resolving) != 1 {
			t.Fatalf("canonical resolution %s = %v, recursion state %#v", ref, err, resolving)
		}
	}
	catalog.Types["Cycle"] = NamedTypeDecl{Fields: map[string]TypeFieldSpec{"child": {Type: "Cycle"}}}
	catalog.Types["Left"] = NamedTypeDecl{Fields: map[string]TypeFieldSpec{"right": {Type: "[Right]"}}}
	catalog.Types["Right"] = NamedTypeDecl{Fields: map[string]TypeFieldSpec{"left": {Type: "map[text]Left"}}}
	for _, tc := range []struct{ ref, problem string }{
		{"", "catalog type reference is required"}, {"Missing", `unknown catalog type "Missing"`},
		{"Cycle", `recursive catalog type "Cycle" is unsupported`},
		{"Left", `recursive catalog type "Left" is unsupported`},
		{"map[NoKey]NoValue", `unknown catalog type "NoKey"`},
		{"map[integer]NoValue", `map key type "integer" is unsupported; map keys must be text, enum or uuid`},
	} {
		resolving := map[string]struct{}{"unrelated": {}}
		_, err := resolveCatalogTypeReference(tc.ref, catalog, resolving)
		catalogFactoringRefusal(t, err, tc.problem)
		if len(resolving) != 1 {
			t.Fatalf("failed resolution leaked recursion state: %#v", resolving)
		}
	}
}

func catalogFactoringScopedBundle(t *testing.T) *WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	for file, body := range map[string]string{
		"schema.yaml":       "name: catalog-factoring\nstages: {done: {final: true}}\n",
		"types.yaml":        "scalars: {Label: text}\ntypes:\n  Box:\n    value: Label\n",
		"events.yaml":       "record.ready:\n  item: Box\n  items: [Box]\n",
		"child/schema.yaml": "name: child\nstages: {done: {final: true}}\n",
		"child/types.yaml":  "scalars: {Label: integer}\n",
		"child/events.yaml": "record.ready:\n  item: Box\n  items: [Box]\n",
		"child/nodes.yaml":  "consumer:\n  execution_type: system_node\n  event_handlers:\n    record.ready: {}\n",
	} {
		writeFixtureFile(t, filepath.Join(root, file), body)
	}
	repo := repoRootForContractsTest(t)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func catalogFactoringEventField(t *testing.T) {
	bundle := catalogFactoringScopedBundle(t)
	for _, tc := range []struct {
		flow string
		want CatalogTypeKind
	}{{".", CatalogTypeText}, {"child", CatalogTypeInteger}} {
		ref, found := ResolveEventFieldType(bundle, tc.flow, "record.ready", " item ")
		if !found || ref.Type != "Box" {
			t.Fatalf("declaring field missing: %#v/%v", ref, found)
		}
		resolved := catalogFactoringResolved(t, ref.Type, ref.Catalog)
		if field, _ := resolved.Field("value"); field.Type.Kind != tc.want {
			t.Fatalf("wrong declaring catalog for %s: %#v", tc.flow, field)
		}
		ref.Catalog.Scalars["Label"] = ScalarTypeDecl{Base: "uuid"}
		fresh, found := ResolveEventFieldType(bundle, tc.flow, "record.ready", "item")
		field, _ := catalogFactoringResolved(t, fresh.Type, fresh.Catalog).Field("value")
		if !found || field.Type.Kind != tc.want {
			t.Fatal("returned reference mutation changed retained event evidence")
		}
	}
	for _, tc := range [][3]string{{".", "missing", "item"}, {"child", "record.ready", "missing"}, {"missing", "record.ready", "item"}, {"child", "record.ready", " "}} {
		if ref, found := ResolveEventFieldType(bundle, tc[0], tc[1], tc[2]); found || !reflect.DeepEqual(ref, CatalogTypeReference{}) {
			t.Fatalf("absent scope/event/field %v resolved: %#v/%v", tc, ref, found)
		}
	}
}

func catalogFactoringNodeField(t *testing.T) {
	bundle := catalogFactoringScopedBundle(t)
	node := identitytest.ExecutableNode(t, "child", "consumer")
	ref, found := ResolveExecutableNodeEventFieldType(bundle, node, "record.ready", "item")
	if !found {
		t.Fatal("owned executable node field missing")
	}
	field, _ := catalogFactoringResolved(t, ref.Type, ref.Catalog).Field("value")
	if field.Type.Kind != CatalogTypeInteger {
		t.Fatal("node lookup chose root declaration rather than its flow")
	}
	ref.Catalog.Scalars["Label"] = ScalarTypeDecl{Base: "uuid"}
	fresh, found := ResolveExecutableNodeEventFieldType(bundle, node, "record.ready", "item")
	field, _ = catalogFactoringResolved(t, fresh.Type, fresh.Catalog).Field("value")
	if !found || field.Type.Kind != CatalogTypeInteger {
		t.Fatal("node reference capture was not detached")
	}
	platform, found := ResolveExecutableNodeEventFieldType(bundle, node, "mailbox.card_decided", "card_id")
	if !found || catalogFactoringResolved(t, platform.Type, platform.Catalog).Format != "uuid" {
		t.Fatalf("legitimate platform fallback = %#v/%v", platform, found)
	}
	for _, tc := range []struct {
		bundle *WorkflowContractBundle
		node   identity.ExecutableNode
		event  string
		field  string
	}{{nil, node, "record.ready", "item"}, {bundle, identity.ExecutableNode{}, "record.ready", "item"}, {bundle, node, "missing", "item"}, {bundle, node, "record.ready", ""}} {
		if ref, found := ResolveExecutableNodeEventFieldType(tc.bundle, tc.node, tc.event, tc.field); found || !reflect.DeepEqual(ref, CatalogTypeReference{}) {
			t.Fatalf("incomplete node/event/field evidence resolved: %#v/%v", ref, found)
		}
	}
}
