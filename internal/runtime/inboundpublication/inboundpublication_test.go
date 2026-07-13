package inboundpublication

import "testing"

func TestCanonicalRecipientManifestUsesEmptyArrayForNoRoutes(t *testing.T) {
	manifest, fingerprint, count, err := CanonicalRecipientManifest(nil)
	if err != nil {
		t.Fatalf("CanonicalRecipientManifest: %v", err)
	}
	if got, want := string(manifest), "[]"; got != want {
		t.Fatalf("manifest = %s, want %s", got, want)
	}
	if got, want := fingerprint, "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"; got != want {
		t.Fatalf("fingerprint = %s, want %s", got, want)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
}
