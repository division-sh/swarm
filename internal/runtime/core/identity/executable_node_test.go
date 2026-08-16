package identity

import (
	"encoding/json"
	"testing"
)

func TestExecutableNodeDeclarationAndStrictHydration(t *testing.T) {
	ref, err := AdmitExecutableNodeDeclaration("", " orders ", " shared ")
	if err != nil {
		t.Fatalf("admit declaration: %v", err)
	}
	if ref.PackageKey() != RootPackageKey || ref.FlowID() != "orders" || ref.NodeID() != "shared" || !ref.Valid() {
		t.Fatalf("admitted identity = %#v", ref)
	}
	if _, err := ParseExecutableNode("", "orders", "shared"); err == nil {
		t.Fatal("strict hydration accepted noncanonical root package")
	}
	if _, err := ParseExecutableNode(".", " orders ", "shared"); err == nil {
		t.Fatal("strict hydration accepted noncanonical flow")
	}
	parsed, err := ParseExecutableNode(".", "orders", "shared")
	if err != nil || !parsed.Equal(ref) {
		t.Fatalf("strict hydration = %#v, %v", parsed, err)
	}
}

func TestExecutableNodeJSONIsStrict(t *testing.T) {
	ref, err := ParseExecutableNode("flows/orders", "orders", "shared")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(ref)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ExecutableNode
	if err := json.Unmarshal(raw, &decoded); err != nil || !decoded.Equal(ref) {
		t.Fatalf("round trip = %#v, %v", decoded, err)
	}
	for _, hostile := range []string{
		`{"package_key":"flows/orders","flow_id":"orders","node_id":"shared","extra":true}`,
		`{"package_key":"flows//orders","flow_id":"orders","node_id":"shared"}`,
		`{"package_key":"flows/orders","flow_id":"orders","node_id":""}`,
	} {
		if err := json.Unmarshal([]byte(hostile), &decoded); err == nil {
			t.Fatalf("accepted hostile identity %s", hostile)
		}
	}
}

func TestExecutableNodeKeyRoundTripIsStrict(t *testing.T) {
	ref, err := AdmitExecutableNodeDeclaration("flows/orders", "orders", "worker")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseExecutableNodeKey(ref.Key())
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.Equal(ref) {
		t.Fatalf("parsed key = %#v, want %#v", parsed, ref)
	}
	for _, hostile := range []string{"", "one.two", ref.Key() + ".extra", " " + ref.Key(), "@@@.b3JkZXJz.d29ya2Vy"} {
		if _, err := ParseExecutableNodeKey(hostile); err == nil {
			t.Fatalf("hostile key %q was accepted", hostile)
		}
	}
}
