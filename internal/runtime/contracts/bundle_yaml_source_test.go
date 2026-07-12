package contracts

import (
	"bytes"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestAuthoritativeYAMLSourceServesTypedHashAndIdentityDerivationsOnce(t *testing.T) {
	raw := []byte("name: example\nvalues: &values [one, two]\nalias: *values\n")
	store := yamlsource.NewStore(yamlsource.Limits{MaxEntries: 8, MaxSourceBytes: 4096})
	snapshot, err := store.Load(raw)
	if err != nil {
		t.Fatal(err)
	}

	var typed map[string]any
	if err := snapshot.Decode(&typed); err != nil {
		t.Fatal(err)
	}
	hashProjection, err := canonicalBundleHashYAMLSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	identityProjection, err := canonicalBundleIdentityYAMLSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(hashProjection) == 0 || len(identityProjection) == 0 {
		t.Fatalf("empty derivation: hash=%q identity=%q", hashProjection, identityProjection)
	}
	if stats := store.Stats(); stats.ParseCount != 1 {
		t.Fatalf("parse count = %d, want 1", stats.ParseCount)
	}

	typed["name"] = "mutated"
	var fresh map[string]any
	if err := snapshot.Decode(&fresh); err != nil {
		t.Fatal(err)
	}
	if fresh["name"] != "example" {
		t.Fatalf("fresh decode contaminated: %#v", fresh)
	}
	wantHash, err := canonicalBundleHashYAML(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(hashProjection, wantHash) {
		t.Fatalf("hash projection changed: got %q want %q", hashProjection, wantHash)
	}
}
