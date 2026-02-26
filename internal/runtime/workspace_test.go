package runtime

import "testing"

func TestSanitizeWorkspaceSlug(t *testing.T) {
	got := sanitizeWorkspaceSlug(" Pet Grooming_2026 ")
	if got != "pet-grooming-2026" {
		t.Fatalf("unexpected slug: %s", got)
	}
}
