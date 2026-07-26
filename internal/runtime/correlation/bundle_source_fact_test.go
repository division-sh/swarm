package correlation

import (
	"reflect"
	"testing"
)

const testCanonicalBundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestBundleSourceFactConstructorsRequireCanonicalExecutableIdentity(t *testing.T) {
	for _, tc := range []struct {
		name      string
		construct func(string) (BundleSourceFact, error)
		source    string
	}{
		{name: "persisted", construct: NewPersistedBundleSourceFact, source: "persisted"},
		{name: "ephemeral", construct: NewEphemeralBundleSourceFact, source: "ephemeral"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fact, err := tc.construct(testCanonicalBundleHash)
			if err != nil {
				t.Fatalf("construct fact: %v", err)
			}
			hash, source := fact.StorageValues()
			if hash != testCanonicalBundleHash || source != tc.source {
				t.Fatalf("storage values = %q/%q, want %q/%q", hash, source, testCanonicalBundleHash, tc.source)
			}
		})
	}

	for _, invalid := range []string{
		"",
		" " + testCanonicalBundleHash,
		"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bundle-v1:sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
	} {
		if _, err := NewPersistedBundleSourceFact(invalid); err == nil {
			t.Fatalf("NewPersistedBundleSourceFact(%q) error = nil", invalid)
		}
		if _, err := NewEphemeralBundleSourceFact(invalid); err == nil {
			t.Fatalf("NewEphemeralBundleSourceFact(%q) error = nil", invalid)
		}
	}
}

func TestBundleSourceFactRejectsNonExecutableSources(t *testing.T) {
	for _, source := range []string{"", "deleted", "legacy", " persisted ", "PERSISTED"} {
		if _, err := DecodeBundleSourceFact(testCanonicalBundleHash, source); err == nil {
			t.Fatalf("DecodeBundleSourceFact source %q error = nil", source)
		}
	}
	if err := (BundleSourceFact{}).Validate(); err == nil {
		t.Fatal("zero BundleSourceFact validates")
	}
}

func TestBundleSourceFactMatchesCompleteOpaqueIdentity(t *testing.T) {
	persisted, err := NewPersistedBundleSourceFact(testCanonicalBundleHash)
	if err != nil {
		t.Fatal(err)
	}
	same, err := NewPersistedBundleSourceFact(testCanonicalBundleHash)
	if err != nil {
		t.Fatal(err)
	}
	ephemeral, err := NewEphemeralBundleSourceFact(testCanonicalBundleHash)
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewPersistedBundleSourceFact("bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatal(err)
	}

	if !persisted.Matches(same) {
		t.Fatal("identical persisted facts do not match")
	}
	for name, candidate := range map[string]BundleSourceFact{
		"same hash different source": ephemeral,
		"different hash":             other,
		"zero":                       {},
	} {
		if persisted.Matches(candidate) || candidate.Matches(persisted) {
			t.Fatalf("%s unexpectedly matches", name)
		}
	}
}

func TestBundleSourceFactHasClosedConstructionSurface(t *testing.T) {
	typ := reflect.TypeOf(BundleSourceFact{})
	for i := 0; i < typ.NumField(); i++ {
		if typ.Field(i).IsExported() {
			t.Fatalf("BundleSourceFact field %s is exported", typ.Field(i).Name)
		}
	}
	for _, prohibited := range []string{"Normalized", "SetBundleHash", "SetSource"} {
		if _, ok := typ.MethodByName(prohibited); ok {
			t.Fatalf("BundleSourceFact exposes prohibited method %s", prohibited)
		}
	}
}
