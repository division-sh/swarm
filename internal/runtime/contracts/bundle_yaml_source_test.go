package contracts

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProductionTypedHashAndIdentityEntrypointsShareAuthoritativeYAMLParse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "package.yaml")
	raw := []byte(fmt.Sprintf("name: %q\nvalues: &values [one, two]\nalias: *values\n", path))
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	before := yamlsource.DefaultStats()
	var typed map[string]any
	if err := loadYAMLFile(path, &typed); err != nil {
		t.Fatal(err)
	}
	hashProjection, err := canonicalBundleHashContent(path, bundleHashYAML)
	if err != nil {
		t.Fatal(err)
	}
	identityProjection, err := canonicalBundleIdentityContent(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(hashProjection) == 0 || len(identityProjection) == 0 {
		t.Fatalf("empty derivation: hash=%q identity=%q", hashProjection, identityProjection)
	}
	after := yamlsource.DefaultStats()
	if delta := after.ParseCount - before.ParseCount; delta != 1 {
		t.Fatalf("production parse count delta = %d, want 1", delta)
	}
	if delta := after.Hits - before.Hits; delta != 2 {
		t.Fatalf("production owner hit delta = %d, want 2", delta)
	}

	typed["name"] = "mutated"
	var fresh map[string]any
	if err := loadYAMLFile(path, &fresh); err != nil {
		t.Fatal(err)
	}
	if fresh["name"] != path {
		t.Fatalf("fresh decode contaminated: %#v", fresh)
	}
}
