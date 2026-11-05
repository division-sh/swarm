package identity

import (
	"strings"
	"testing"
)

func TestParseRequiresCanonicalManifestHash(t *testing.T) {
	valid := "sha256:" + strings.Repeat("a", 64)
	hash, err := ParseManifestHash(valid)
	if err != nil {
		t.Fatal(err)
	}
	if !hash.Valid() || hash.String() != valid {
		t.Fatalf("hash = %q valid=%t", hash.String(), hash.Valid())
	}
	for _, invalid := range []string{
		"",
		strings.Repeat("a", 64),
		"sha256:" + strings.Repeat("A", 64),
		"sha256:short",
		" sha256:" + strings.Repeat("a", 64),
		"sha256:" + strings.Repeat("a", 64) + " ",
	} {
		if _, err := ParseManifestHash(invalid); err == nil {
			t.Fatalf("Parse(%q) error = nil", invalid)
		}
	}
}

func TestFromBytesProducesCanonicalManifestHash(t *testing.T) {
	hash := ManifestHashFromBytes([]byte("manifest"))
	if !hash.Valid() {
		t.Fatalf("FromBytes() = %q, want canonical hash", hash.String())
	}
	parsed, err := ParseManifestHash(hash.String())
	if err != nil || !parsed.Equal(hash) {
		t.Fatalf("Parse(FromBytes()) = (%q, %v)", parsed.String(), err)
	}
}
