package packadmission

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packartifact"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/manifesthash"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"gopkg.in/yaml.v3"
)

func TestAdmitRejectsMalformedBodiesForEveryPackKind(t *testing.T) {
	base, err := packartifact.LoadEmbeddedPlatformPackInventory("0.7.0")
	if err != nil {
		t.Fatal(err)
	}
	platformBody, err := os.ReadFile(runtimecontracts.DefaultPlatformSpecFile(repoRoot(t)))
	if err != nil {
		t.Fatal(err)
	}
	platform, err := runtimecontracts.ParsePlatformSpecDocument(platformBody, "platform-spec.yaml")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		packID  string
		wantErr string
	}{
		{name: "trigger", packID: "provider.telegram", wantErr: "admit provider trigger packs"},
		{name: "connector", packID: "provider.telegram.connector", wantErr: "admit provider connector packs"},
		{name: "channel", packID: "provider.telegram.hitl_channel", wantErr: "admit channel packs"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entry, ok := base.Lookup(tc.packID)
			if !ok {
				t.Fatalf("embedded pack %q is missing", tc.packID)
			}
			envelope := entry.Envelope()
			envelope.Provenance.Source = packartifact.ProvenanceProject
			envelope.ManifestHash = packartifact.ManifestHashDerived
			envelopeBody, err := yaml.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			effective, err := packartifact.NewEffectivePackInventory(base, []packartifact.ProjectPackSource{{
				Path: tc.packID, EnvelopeBody: envelopeBody, ManifestBody: []byte("unknown_field: true\n"),
				Origin: packartifact.ImportOrigin{
					Source: packartifact.ProvenanceEmbedded, ID: entry.ID(), Version: entry.Version(), ManifestHash: entry.ManifestHash(),
					EnvelopeHash: manifesthash.FromBytes(envelopeBody).String(),
				},
			}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Admit(effective, platform); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("malformed %s body error = %v", tc.name, err)
			}
		})
	}
}

func TestPackPlatformAdmissionDiskLogicalRetainedParity(t *testing.T) {
	repo := repoRoot(t)
	root := t.TempDir()
	if err := os.CopyFS(root, os.DirFS(filepath.Join(repo, "tests/tier1-primitives/test-emits-multiple"))); err != nil {
		t.Fatal(err)
	}
	base, err := packartifact.LoadEmbeddedPlatformPackInventory("0.7.0")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"provider.telegram", "provider.telegram.connector", "provider.telegram.hitl_channel"} {
		if changed, err := packartifact.ImportEmbeddedPack(root, id, base); err != nil || !changed {
			t.Fatalf("import %s: %t %v", id, changed, err)
		}
	}
	options := runtimecontracts.WorkflowContractLoadOptions{PlatformPackBase: base, AdmitPackInventory: AdmitInventory}
	disk, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo), options)
	if err != nil {
		t.Fatal(err)
	}
	diskProjection, err := FromBundle(disk)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := sourceartifact.PersistedFromArtifact(disk.SourceArtifact, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	logical, err := sourceartifact.DecodeLogical(disk.SourceArtifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	retained, err := persisted.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	for name, source := range map[string]*sourceartifact.AdmittedSourceArtifact{"logical": logical, "retained": retained} {
		bundle, err := runtimecontracts.LoadWorkflowContractBundleFromArtifact(repo, source, runtimecontracts.DefaultPlatformSpecFile(repo), options)
		if err != nil {
			t.Fatal(err)
		}
		projection, err := FromBundle(bundle)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(disk.SourceArtifact.LogicalBlob(), source.LogicalBlob()) || source.BundleHash() != disk.SourceArtifact.BundleHash() || !reflect.DeepEqual(disk.EffectiveProvenance().Entries(), bundle.EffectiveProvenance().Entries()) || !reflect.DeepEqual(diskProjection.ChannelPlans, projection.ChannelPlans) || disk.PackInventory.Digest() != bundle.PackInventory.Digest() {
			t.Fatalf("%s changed bytes/hash/provenance/compiled inventory", name)
		}
		for _, entry := range disk.PackInventory.Entries() {
			other, ok := bundle.PackInventory.Lookup(entry.ID())
			if !ok || !bytes.Equal(entry.ManifestBody(), other.ManifestBody()) || !bytes.Equal(entry.EnvelopeBody(), other.EnvelopeBody()) || entry.ManifestHash() != other.ManifestHash() {
				t.Fatalf("%s changed pack %s", name, entry.ID())
			}
		}
	}
	for _, name := range []string{"provider.telegram.connector.connector", "provider.telegram.hitl_channel.channel", "provider.github.connector.generated_profile", "generated_index"} {
		if proof, ok := disk.EffectiveProvenance().Lookup("pack_sources[\"" + name + "\"]"); !ok || proof.SourceLine == 0 {
			t.Fatalf("body/profile source evidence missing: %s %#v", name, proof)
		}
	}
	derived, ok := disk.EffectiveProvenance().Lookup("packs[\"provider.telegram.connector\"].envelope.effective_manifest_hash")
	if !ok || derived.Origin != runtimecontracts.EffectiveValueOriginDerived {
		t.Fatal("effective digest replaced authored evidence")
	}
	for _, input := range derived.InputPaths {
		if _, ok := disk.EffectiveProvenance().Lookup(input); !ok {
			t.Fatalf("derived proof cites missing input %s", input)
		}
	}
	views := diskProjection.PackSourceValues()
	delete(views, "provider.telegram.connector.connector")
	if _, ok := diskProjection.PackSourceValues()["provider.telegram.connector.connector"]; !ok {
		t.Fatal("returned view map mutated canonical owner")
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}
