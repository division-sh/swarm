package store

import "testing"

func TestJoinGeographyLabel_DedupesRepeatedParts(t *testing.T) {
	got := joinGeographyLabel("United States", "", "United States")
	if got != "United States" {
		t.Fatalf("expected deduped geography label, got %q", got)
	}
}

func TestJoinGeographyLabel_PreservesDistinctParts(t *testing.T) {
	got := joinGeographyLabel("San Jose", "CA", "United States")
	if got != "San Jose, CA, United States" {
		t.Fatalf("unexpected geography label, got %q", got)
	}
}
