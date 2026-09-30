package timeridentity

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

func TestAccumulatorKeyRejectsMalformedGeneration(t *testing.T) {
	node, err := runtimeidentity.AdmitExecutableNodeDeclaration(".", "collector")
	if err != nil {
		t.Fatal(err)
	}
	g := attemptgeneration.Generation{LoopID: "loop", ActivationID: "activation", RevisionField: "revision", RevisionID: "revision-1", Attempt: 1}
	{
		bucket := NewAccumulatorBucketRefForGeneration(node, "result", g)
		key := bucket.Key()
		got, ok := ParseAccumulatorBucketKey(key)
		if !ok || got.Generation.RevisionField != "" || got.Generation.Valid() {
			t.Fatal("key did not preserve partial coordinates")
		}
		got.Generation.RevisionField = g.RevisionField
		if got.Key() != key {
			t.Fatal("key changed after exact completion")
		}
		base := NewAccumulatorBucketRef(node, "result").Key()
		for _, bad := range []struct{ name, key string }{
			{"empty", base + "@generation="},
			{"malformed", base + "@generation=invalid"},
			{"duplicate", key + "@generation=" + g.KeySuffix()},
			{"suffix_space", base + "@generation= " + g.KeySuffix()},
			{"trailing_space", key + " "},
			{"leading_space", " " + key},
			{"base_space", strings.Replace(key, "@generation=", " @generation=", 1)},
			{"retired_window", base + "@window=old"},
		} {
			t.Run(bad.name, func(t *testing.T) {
				if _, ok := ParseAccumulatorBucketKey(bad.key); ok {
					t.Fatalf("accepted %q", bad.key)
				}
			})
		}
	}
}
