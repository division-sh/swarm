package sourceartifactfixture

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestRootedFixtureKeepsExactDetachedContent(t *testing.T) {
	input, err := sourceartifact.AdmitMembers(map[string][]byte{
		"schema.yaml": []byte("stages: []\n"),
		"docs/a.md":   []byte("exact documentation\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, directory := range []string{"shop-a", "shop-b"} {
		rooted, root := Rooted(t, directory, input)
		if rooted.Validate() != nil || rooted.Admission().RootDirectory() != directory ||
			rooted.Admission().BundleHash() != input.BundleHash() || !bytes.Equal(rooted.Artifact().LogicalBlob(), input.LogicalBlob()) {
			t.Fatal("fixture lost genuine admission or exact bytes")
		}
		if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("required_agents: []\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(root); err != nil {
			t.Fatal(err)
		}
		if rooted.Validate() != nil || !bytes.Equal(rooted.Artifact().LogicalBlob(), input.LogicalBlob()) {
			t.Fatal("fixture mutation/removal changed captured evidence")
		}
	}
	if _, present := input.RootAdmission(); present {
		t.Fatal("fixture mutated the original content-only artifact")
	}
}

func TestRootedFixtureCopyDoesNotRetargetOriginalAdmission(t *testing.T) {
	first, _ := Rooted(t, "shop-a", New("nodes.yaml", []byte("worker: {type: procedure}\n")))
	second, _ := Rooted(t, "shop-b", first.Artifact())
	if first.Admission().Equal(second.Admission()) || first.Admission().RootDirectory() != "shop-a" || second.Admission().RootDirectory() != "shop-b" ||
		first.Artifact().BundleHash() != second.Artifact().BundleHash() {
		t.Fatal("fresh fixture copy changed the original root or content")
	}
}
