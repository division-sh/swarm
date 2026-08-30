package correlation

import (
	"reflect"
	"testing"
)

const testCanonicalBundleHash = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestSourceArtifactFactConstructorsRequireCanonicalExecutableIdentity(t *testing.T) {
	fact, err := NewSourceArtifactFact(testCanonicalBundleHash)
	if err != nil {
		t.Fatalf("construct fact: %v", err)
	}
	if got := fact.BundleHash(); got != testCanonicalBundleHash {
		t.Fatalf("bundle hash = %q, want %q", got, testCanonicalBundleHash)
	}

	for _, invalid := range []string{
		"",
		" " + testCanonicalBundleHash,
		"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bundle-v2:sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
	} {
		if _, err := NewSourceArtifactFact(invalid); err == nil {
			t.Fatalf("NewSourceArtifactFact(%q) error = nil", invalid)
		}
	}
}

func TestSourceArtifactFactRejectsMissingIdentity(t *testing.T) {
	if err := (SourceArtifactFact{}).Validate(); err == nil {
		t.Fatal("zero SourceArtifactFact validates")
	}
}

func TestSourceArtifactFactMatchesCompleteOpaqueIdentity(t *testing.T) {
	persisted, err := NewSourceArtifactFact(testCanonicalBundleHash)
	if err != nil {
		t.Fatal(err)
	}
	same, err := NewSourceArtifactFact(testCanonicalBundleHash)
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewSourceArtifactFact("bundle-v2:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatal(err)
	}

	if !persisted.Matches(same) {
		t.Fatal("identical persisted facts do not match")
	}
	for name, candidate := range map[string]SourceArtifactFact{
		"different hash": other,
		"zero":           {},
	} {
		if persisted.Matches(candidate) || candidate.Matches(persisted) {
			t.Fatalf("%s unexpectedly matches", name)
		}
	}
}

func TestSourceArtifactFactHasClosedConstructionSurface(t *testing.T) {
	typ := reflect.TypeOf(SourceArtifactFact{})
	for i := 0; i < typ.NumField(); i++ {
		if typ.Field(i).IsExported() {
			t.Fatalf("SourceArtifactFact field %s is exported", typ.Field(i).Name)
		}
	}
	for _, prohibited := range []string{"Normalized", "SetBundleHash", "SetSource"} {
		if _, ok := typ.MethodByName(prohibited); ok {
			t.Fatalf("SourceArtifactFact exposes prohibited method %s", prohibited)
		}
	}
}
