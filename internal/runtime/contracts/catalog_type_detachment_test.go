package contracts

import "testing"

func TestCatalogTypeDetachmentFlowOverrideEnum(t *testing.T) {
	bundle, err := loadNamespaceTestBundle(t, "Choice: [\"root\"]\n", "Choice: [\"child\"]\n")
	if err != nil {
		t.Fatal(err)
	}
	view := bundle.ResolvedTypeCatalogForFlow("child")
	view.Enums["Choice"].Values[0] = "changed"
	if got := bundle.ResolvedTypeCatalogForFlow("child").Enums["Choice"].Values[0]; got != "child" {
		t.Fatalf("flow enum readback aliases retained declaration: %s", got)
	}
}
