package packs

import (
	"strings"
	"testing"
)

func TestPackSourceKeepsDelimiterBearingProvenanceAndPathDistinct(t *testing.T) {
	source, err := NewPackSource("external:vendor", "/tmp/packs/acme:latest")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := NewPackIdentity("pack.acme", "1.0.0", "sha256:"+strings.Repeat("a", 64), TypeConnector, source)
	if err != nil {
		t.Fatal(err)
	}
	if identity.Source().Provenance() != "external:vendor" || identity.Source().Path() != "/tmp/packs/acme:latest" {
		t.Fatalf("pack source = %#v, want exact typed provenance/path", identity.Source())
	}
}

func TestPackIdentityRejectsInvalidIdentityFieldsAtAdmission(t *testing.T) {
	source := MustPackSource("external", "/tmp/packs/acme")
	for _, test := range []struct {
		name     string
		id       string
		version  string
		packType string
	}{
		{name: "id", id: "Pack Acme", version: "1.0.0", packType: TypeConnector},
		{name: "id whitespace", id: " pack.acme", version: "1.0.0", packType: TypeConnector},
		{name: "version", id: "pack.acme", version: "current", packType: TypeConnector},
		{name: "version whitespace", id: "pack.acme", version: "1.0.0 ", packType: TypeConnector},
		{name: "type", id: "pack.acme", version: "1.0.0", packType: "adapter"},
		{name: "type whitespace", id: "pack.acme", version: "1.0.0", packType: " connector"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewPackIdentity(test.id, test.version, "sha256:"+strings.Repeat("a", 64), test.packType, source); err == nil {
				t.Fatal("NewPackIdentity error = nil")
			}
		})
	}
}
