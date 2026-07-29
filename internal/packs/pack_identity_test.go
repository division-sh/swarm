package packs

import "testing"

func TestPackSourceKeepsDelimiterBearingProvenanceAndPathDistinct(t *testing.T) {
	source, err := NewPackSource("external:vendor", "/tmp/packs/acme:latest")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := NewPackIdentity("pack.acme", "1.0.0", "sha256:fixture", TypeConnector, source)
	if err != nil {
		t.Fatal(err)
	}
	if identity.Source().Provenance() != "external:vendor" || identity.Source().Path() != "/tmp/packs/acme:latest" {
		t.Fatalf("pack source = %#v, want exact typed provenance/path", identity.Source())
	}
}
