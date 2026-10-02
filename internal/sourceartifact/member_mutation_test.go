package sourceartifact

import (
	"bytes"
	"reflect"
	"testing"
)

func Test2376MemberDispositionMutationParity(t *testing.T) {
	for _, label := range []string{
		"schema.yaml", "types.yaml", "entities.yaml", "nodes.yaml", "events.yaml", "agents.yaml", "tools.yaml", "policy.yaml", "rules.yaml",
		"manifest.yaml", "child/manifest.yaml", "prompts/worker.md", "tests/journey.yaml", "data/rows.jsonl", "mocks/worker.yaml", "modules/worker.py", "packs/pack.yaml", "docs/manifest.yaml", "README", "README.md", "LICENSE", "LICENSE.md",
	} {
		t.Run(label, func(t *testing.T) {
			root := t.TempDir()
			writeTestFile(t, root, "schema.yaml", "description: root\n")
			body := "description: original\n"
			if label == "manifest.yaml" || label == "child/manifest.yaml" {
				body = manifest2376
			}
			if label == "child/manifest.yaml" {
				writeTestFile(t, root, "child/schema.yaml", "description: child\n")
			}
			writeTestFile(t, root, label, body)
			before, err := AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			beforeTable, beforeBlob := before.MemberTable(), before.LogicalBlob()
			writeTestFile(t, root, label, body+"# exact-byte edit\n")
			after, err := AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			if before.BundleHash() == after.BundleHash() || bytes.Equal(beforeBlob, after.LogicalBlob()) {
				t.Fatal("included mutation did not change exact identity")
			}
			if !reflect.DeepEqual(before.MemberTable(), beforeTable) || !bytes.Equal(before.LogicalBlob(), beforeBlob) {
				t.Fatal("later source mutation changed the admitted generation")
			}
			old, err := DecodeLogical(beforeBlob)
			if err != nil || old.BundleHash() != before.BundleHash() || !reflect.DeepEqual(old.MemberTable(), beforeTable) {
				t.Fatalf("stored generation was not preserved: %v", err)
			}
		})
	}
}

func Test2376SourceHashIndependentOfPlatformAdmission(t *testing.T) {
	artifact, err := newArtifact([]Entry{{label: "schema.yaml", body: []byte("description: root\n")}, {label: "manifest.yaml", body: []byte(manifest2376)}})
	if err != nil {
		t.Fatal(err)
	}
	hash, blob := artifact.BundleHash(), artifact.LogicalBlob()
	for _, version := range []string{"0.7.0", "0.8.0", "9.0.0", "not-a-version"} {
		err := artifact.ValidatePlatformVersion(version)
		if (version == "0.7.0" || version == "0.8.0") != (err == nil) {
			t.Fatalf("platform %s: %v", version, err)
		}
		if artifact.BundleHash() != hash || !bytes.Equal(blob, artifact.LogicalBlob()) {
			t.Fatal("platform compatibility contaminated source identity")
		}
	}
}
