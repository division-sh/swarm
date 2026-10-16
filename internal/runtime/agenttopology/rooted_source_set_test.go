package agenttopology

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1SourceSetRevisionFreezesRootWithoutAddingPublicContexts(t *testing.T) {
	root, _ := sourceartifactfixture.Rooted(t, "first", sourceartifactfixture.Artifact())
	makeCoordinate := func(directory string) SourceCoordinate {
		t.Helper()
		wire, err := json.Marshal(map[string]string{"bundle_hash": root.Artifact().BundleHash(), "root_directory": directory})
		if err != nil {
			t.Fatal(err)
		}
		var coordinate SourceCoordinate
		if err := json.Unmarshal(wire, &coordinate); err != nil {
			t.Fatal(err)
		}
		return coordinate
	}
	a, b := makeCoordinate("first"), makeCoordinate("second")
	first, err := NewSourceSetPlan([]SourceCoordinate{a}, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewSourceSetPlan([]SourceCoordinate{b}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.Revision == second.Revision {
		t.Fatal("same-content different-root source sets share a revision")
	}
	if a.Key() != b.Key() {
		t.Fatal("root directory introduced another public content context")
	}
	if _, err := NewSourceSetPlan([]SourceCoordinate{a, b}, nil); err == nil {
		t.Fatal("two roots for one public content context were admitted")
	}
	for _, directory := range []string{"", "first ", "../first", "first/child"} {
		if _, err := NewSourceSetPlan([]SourceCoordinate{makeCoordinate(directory)}, nil); err == nil {
			t.Fatalf("invalid rooted plan coordinate %q was admitted", directory)
		}
	}
	first.Sources[0] = b
	if err := first.Validate(); err == nil {
		t.Fatal("retargeted root preserved the predecessor source-set revision")
	}
}
