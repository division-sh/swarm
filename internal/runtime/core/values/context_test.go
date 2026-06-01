package values

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
)

func TestBucketLookupAndSetPath(t *testing.T) {
	root := Wrap(map[string]any{
		"nested": map[string]any{
			"value": 7,
		},
	})
	if got, ok := root.Lookup(paths.Parse("nested.value")); !ok || got != 7 {
		t.Fatalf("Lookup() = %#v, %v", got, ok)
	}
	root.SetPath(paths.Parse("nested.extra"), "x")
	if got, ok := root.Lookup(paths.Parse("nested.extra")); !ok || got != "x" {
		t.Fatalf("SetPath()/Lookup() = %#v, %v", got, ok)
	}
}

func TestContextLookup_ByRoot(t *testing.T) {
	ctx := NewContext()
	ctx.Entity = Wrap(map[string]any{"status": "ready"})
	ctx.Payload = Wrap(map[string]any{"score": 9})
	ctx.Accumulated = Wrap(map[string]any{"count": 3})

	if got, ok := ctx.Lookup(paths.Parse("entity.status")); !ok || got != "ready" {
		t.Fatalf("entity lookup = %#v, %v", got, ok)
	}
	if got, ok := ctx.Lookup(paths.Parse("payload.score")); !ok || got != 9 {
		t.Fatalf("payload lookup = %#v, %v", got, ok)
	}
	if got, ok := ctx.Lookup(paths.Parse("accumulated.count")); !ok || got != 3 {
		t.Fatalf("accumulated lookup = %#v, %v", got, ok)
	}
}
