package eventidentity

import "testing"

func TestLeafName_LocalizesScopedEvent(t *testing.T) {
	if got := LeafName("scoring/vertical.shortlisted"); got != "vertical.shortlisted" {
		t.Fatalf("LeafName = %q, want %q", got, "vertical.shortlisted")
	}
}

func TestExternalizeForFlow_QualifiesDeclaredLocalEvent(t *testing.T) {
	if got := ExternalizeForFlow("scoring", []string{"vertical.shortlisted"}, "vertical.shortlisted"); got != "scoring/vertical.shortlisted" {
		t.Fatalf("ExternalizeForFlow = %q, want %q", got, "scoring/vertical.shortlisted")
	}
}

func TestExternalizeForFlow_LeavesNonLocalEventUnchanged(t *testing.T) {
	if got := ExternalizeForFlow("scoring", []string{"vertical.shortlisted"}, "vertical.approved"); got != "vertical.approved" {
		t.Fatalf("ExternalizeForFlow = %q, want %q", got, "vertical.approved")
	}
}

func TestExternalizeDescendantForFlow_QualifiesDescendantEventAgainstParent(t *testing.T) {
	got, ok := ExternalizeDescendantForFlow("operating", "child/launch.ready", map[string]map[string]struct{}{
		"operating/child": {"launch.ready": {}},
	})
	if !ok {
		t.Fatal("expected descendant event to qualify")
	}
	if got != "operating/child/launch.ready" {
		t.Fatalf("ExternalizeDescendantForFlow = %q, want %q", got, "operating/child/launch.ready")
	}
}

func TestExternalizeDescendantForFlow_LeavesAlreadyQualifiedParentEventAlone(t *testing.T) {
	if got, ok := ExternalizeDescendantForFlow("operating", "operating/child/launch.ready", map[string]map[string]struct{}{
		"operating/child": {"launch.ready": {}},
	}); ok || got != "" {
		t.Fatalf("ExternalizeDescendantForFlow = (%q, %v), want (\"\", false)", got, ok)
	}
}

func TestMatchPattern_Exact(t *testing.T) {
	if !MatchPattern("scoring/vertical.shortlisted", "scoring/vertical.shortlisted") {
		t.Fatal("expected exact pattern to match")
	}
}

func TestMatchPattern_SingleSegmentWildcard(t *testing.T) {
	if !MatchPattern("scoring/*", "scoring/vertical.shortlisted") {
		t.Fatal("expected single wildcard pattern to match")
	}
	if MatchPattern("scoring/*", "scoring/a/b") {
		t.Fatal("did not expect single wildcard to span multiple segments")
	}
}

func TestMatchPattern_MultiSegmentWildcard(t *testing.T) {
	if !MatchPattern("operating/**/opco.launched", "operating/child/grandchild/opco.launched") {
		t.Fatal("expected recursive wildcard pattern to match")
	}
}

func TestSplitRouteSegments_NormalizesInput(t *testing.T) {
	got := SplitRouteSegments(" /scoring/vertical.shortlisted/ ")
	if len(got) != 2 || got[0] != "scoring" || got[1] != "vertical.shortlisted" {
		t.Fatalf("SplitRouteSegments = %#v", got)
	}
}
