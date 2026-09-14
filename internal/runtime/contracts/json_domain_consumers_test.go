package contracts

import "testing"

func TestJSONDomainRefinementsAndInstanceIdentity(t *testing.T) {
	for _, other := range []string{"json", "object", "text", "numeric", "list<json>"} {
		fields := map[string]schemaRefinementField{
			"a": {Name: "a", TypeRef: "json", Refinements: SchemaRefinements{EqualTo: "b"}},
			"b": {Name: "b", TypeRef: other},
		}
		errs := validateSchemaRefinementFields("test", TypeCatalogDocument{}, fields)
		if (len(errs) == 0) != (other == "json") {
			t.Fatalf("json equal_to %s: %v", other, errs)
		}
	}
	for _, refs := range []SchemaRefinements{{Pattern: "x"}, {Range: SchemaRangeRefinement{Min: floatPointerForJSONTest(0)}}, {Length: SchemaLengthRefinement{Min: intPointerForJSONTest(0)}}} {
		fields := map[string]schemaRefinementField{"a": {Name: "a", TypeRef: "json", Refinements: refs}}
		if errs := validateSchemaRefinementFields("test", TypeCatalogDocument{}, fields); len(errs) == 0 {
			t.Fatalf("opaque json gained refinement: %#v", refs)
		}
	}
	primary := PrimaryEntityContract{EntityType: "record", Contract: EntityContract{Fields: map[string]EntityFieldDecl{"key": {Type: "json"}}}}
	if got := templateInstanceFieldLeafKind(primary, "json"); got != "json" {
		t.Fatalf("json kind=%s", got)
	}
	field, err := ParseTemplateInstanceField("key")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validateTemplateInstanceField("child", field, primary); err == nil {
		t.Fatal("JSON accepted as scalar instance identity")
	}
	for _, ref := range []string{"json", "map[text]json", "list<json>", "map[text]list<json>"} {
		if err := singletonCoordinatorValidateTypeRef(primary.Types, ref); err != nil {
			t.Fatalf("contained JSON value %s rejected: %v", ref, err)
		}
	}
}

func floatPointerForJSONTest(v float64) *float64 { return &v }
func intPointerForJSONTest(v int) *int           { return &v }
