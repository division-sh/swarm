package sourceartifact

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packmodel"
)

func TestS1CopiedProviderNeedsNoBodyOrManifestEdits(t *testing.T) {
	manifest := []byte("version: 1.2.3\nplatform_version: '*'\ncapabilities: {can: {emit_events: [text_message]}, cannot: [execute arbitrary code]}\n")
	body := []byte("event_name: {literal: raw}\n")
	root := filepath.Join(t.TempDir(), "shop")
	writeTestFile(t, root, "schema.yaml", "description: root\n")
	for _, directory := range []string{"telegram", "telegram-sales", "telegram_sales"} {
		writeTestFile(t, root, directory+"/manifest.yaml", string(manifest))
		writeTestFile(t, root, directory+"/trigger.yaml", string(body))
	}
	artifact, err := AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeLogical(artifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []*AdmittedSourceArtifact{artifact, decoded} {
		if len(source.ProviderDirectories()) != 3 || len(source.Root().Children()) != 0 {
			t.Fatal("provider copy collapsed or acquired flow identity")
		}
		for _, directory := range []string{"telegram", "telegram-sales", "telegram_sales"} {
			provider, present := source.ProviderDirectory(directory)
			if !present || provider.Path() != directory || provider.FlowPath() != "." {
				t.Fatalf("copy lost exact directory identity: %+v", provider)
			}
			label, present := provider.Body(packmodel.BodyTrigger)
			entry, member := source.Entry(label)
			metadata, hasMetadata := source.Entry(directory + "/manifest.yaml")
			if !present || !member || !hasMetadata || !bytes.Equal(entry.Bytes(), body) || !bytes.Equal(metadata.Bytes(), manifest) {
				t.Fatal("provider naming rewrote authored bytes")
			}
		}
	}
}

func TestS1RootedPresentationDoesNotInventContentName(t *testing.T) {
	root := filepath.Join(t.TempDir(), "shop")
	writeTestFile(t, root, "schema.yaml", "description: root\n")
	writeTestFile(t, root, "manifest.yaml", "version: 1.2.3\nplatform_version: '*'\n")
	artifact, err := AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	admission, present := artifact.RootAdmission()
	if !present || artifact.HumanLabel() != "shop@1.2.3" {
		t.Fatal("fresh presentation lost its exact root")
	}
	row, err := PersistedFromArtifact(artifact, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	rooted, err := RestoreRootedSource(row, admission.Reference())
	if err != nil || rooted.HumanLabel() != "shop@1.2.3" {
		t.Fatalf("retained presentation lost its bound root: %v", err)
	}
	if rooted.Artifact().HumanLabel() != ShortHashLabel(artifact.BundleHash()) || !bytes.Equal(rooted.Artifact().LogicalBlob(), artifact.LogicalBlob()) {
		t.Fatal("rooted presentation mutated content-only evidence")
	}
	if (RootedSource{}).HumanLabel() != "" {
		t.Fatal("missing evidence acquired a human name")
	}
}
