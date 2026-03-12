package values

import "testing"

func TestBucket_MapAccessAndSetters(t *testing.T) {
	root := Wrap(map[string]any{})
	child := root.EnsureMap("state")
	child.Set("name", "ready")
	child.Set("count", 3)
	child.Set("active", true)

	got, ok := root.Map("state")
	if !ok {
		t.Fatal("expected nested map")
	}
	if got.String("name") != "ready" {
		t.Fatalf("name = %q", got.String("name"))
	}
	if got.Int("count") != 3 {
		t.Fatalf("count = %d", got.Int("count"))
	}
	if !got.Bool("active") {
		t.Fatal("expected active bool")
	}
}
