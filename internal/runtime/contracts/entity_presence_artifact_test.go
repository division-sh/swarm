package contracts

import (
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestEntityPresenceAdmissionAndArtifactIdentity(t *testing.T) {
	repo := repoRootForContractsTest(t)
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: presence-artifact\n")
	writeFixtureFile(t, filepath.Join(root, "types.yaml"), "scalars:\n  Label: text\ntypes:\n  Profile:\n    name: Label\n    note: Label?\n")
	writeFixtureFile(t, filepath.Join(root, "entities.yaml"), "work:\n  label: Label?\n  profile: Profile\n  counter: {type: integer, initial: 0}\n")
	writeFixtureFile(t, filepath.Join(root, "child", "schema.yaml"), "name: child\nmode: static\n")
	writeFixtureFile(t, filepath.Join(root, "child", "entities.yaml"), "child_work:\n  label: Label?\n  profile: Profile\n")
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	for _, selected := range []*sourceartifact.AdmittedSourceArtifact{artifact, decoded} {
		bundle, err := LoadWorkflowContractBundleFromArtifact(repo, selected, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for _, flow := range []string{".", "child"} {
			primary, err := bundle.ResolveRootPrimaryEntity()
			if flow != "." {
				primary, err = bundle.ResolveFlowPrimaryEntity(flow)
			}
			if err != nil {
				t.Fatal(err)
			}
			typ, err := primary.StructuralType()
			if err != nil {
				t.Fatal(err)
			}
			label, labelOK := typ.Field("label")
			profile, profileOK := typ.Field("profile")
			name, nameOK := profile.Type.Field("name")
			note, noteOK := profile.Type.Field("note")
			if !labelOK || !label.IsOptional || label.Type.Kind != CatalogTypeText || !profileOK || profile.IsOptional || !nameOK || name.IsOptional || !noteOK || !note.IsOptional {
				t.Fatalf("flow %s lost alias/inherited-catalog presence: %#v", flow, typ)
			}
		}
		if selected.BundleHash() != artifact.BundleHash() {
			t.Fatal("selected source reconstruction changed identity")
		}
	}
	// Presence is semantic input to artifact identity; re-reading a changed
	// filesystem must not reinterpret the already-admitted source generation.
	writeFixtureFile(t, filepath.Join(root, "entities.yaml"), "work:\n  label: Label\n  profile: Profile\n  counter: {type: integer, initial: 0}\n")
	changed, err := sourceartifact.AdmitDirectory(root)
	if err != nil || changed.BundleHash() == artifact.BundleHash() {
		t.Fatalf("presence change did not change source identity: %v", err)
	}
	bundle, err := LoadWorkflowContractBundleFromArtifact(repo, decoded, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, entity, _ := bundle.RootPrimaryEntityContract()
	if !entity.Fields["label"].IsOptional || entity.Fields["counter"].Initial != 0 {
		t.Fatalf("retained declaration changed with disk: %#v", entity.Fields)
	}
}
