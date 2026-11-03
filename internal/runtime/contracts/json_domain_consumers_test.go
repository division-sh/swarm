package contracts

import "testing"

func TestJSONDomainRefinementsAndInstanceIdentity(t *testing.T) {
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
	for _, ref := range []string{"json", "map[text]json", "[json]", "map[text][json]"} {
		if err := singletonCoordinatorValidateTypeRef(primary.Types, ref); err != nil {
			t.Fatalf("contained JSON value %s rejected: %v", ref, err)
		}
	}
}
