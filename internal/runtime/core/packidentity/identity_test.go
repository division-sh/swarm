package packidentity

import "testing"

func TestPackIdentityValuesRejectNonCanonicalSyntax(t *testing.T) {
	for _, raw := range []string{"", "Pack.ID", "pack/id", " pack.id", "pack.id "} {
		if _, err := ParseID(raw); err == nil {
			t.Fatalf("ParseID(%q) error = nil", raw)
		}
	}
	for _, raw := range []string{"", "1", "1.0", "v1.0.0", " 1.0.0", "1.0.0 "} {
		if _, err := ParseVersion(raw); err == nil {
			t.Fatalf("ParseVersion(%q) error = nil", raw)
		}
	}
	if got := MustID("pack.id").String(); got != "pack.id" {
		t.Fatalf("ID = %q", got)
	}
	if got := MustVersion("1.2.3").String(); got != "1.2.3" {
		t.Fatalf("Version = %q", got)
	}
}
