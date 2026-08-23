package contractelementidentity

import "testing"

func TestContractElementIdentityRequiresCanonicalNonZeroUUID(t *testing.T) {
	const canonical = "d8fe3c3e-55c6-4f27-8eb4-dcd76a07982c"
	id, err := ParseContractElementID(canonical)
	if err != nil || id.String() != canonical {
		t.Fatalf("ParseContractElementID() = %q, %v", id.String(), err)
	}
	for _, hostile := range []string{
		"", "00000000-0000-0000-0000-000000000000", " D8FE3C3E-55C6-4F27-8EB4-DCD76A07982C ",
		"D8FE3C3E-55C6-4F27-8EB4-DCD76A07982C", "d8fe3c3e55c64f278eb4dcd76a07982c",
	} {
		if _, err := ParseContractElementID(hostile); err == nil {
			t.Fatalf("ParseContractElementID(%q) succeeded", hostile)
		}
	}
}

func TestContractElementRefKeepsPackageAndElementAsDistinctAxes(t *testing.T) {
	const element = "d8fe3c3e-55c6-4f27-8eb4-dcd76a07982c"
	root, err := ParseContractElementRef(".", element)
	if err != nil {
		t.Fatal(err)
	}
	nested, err := ParseContractElementRef("flows/scout", element)
	if err != nil {
		t.Fatal(err)
	}
	if root.Equal(nested) {
		t.Fatal("same element UUID in distinct packages collapsed to one identity")
	}
	if root.ElementID().String() != nested.ElementID().String() || root.PackageKey().Equal(nested.PackageKey()) {
		t.Fatalf("identity axes were not preserved: root=%#v nested=%#v", root, nested)
	}
}
