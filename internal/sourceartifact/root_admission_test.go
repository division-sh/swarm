package sourceartifact

import (
	"bytes"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestS1RootAdmissionSeparatesEqualContent(t *testing.T) {
	var previous *AdmittedSourceArtifact
	var original RootAdmission
	for _, directory := range []string{"shop-a", "shop-b"} {
		root := filepath.Join(t.TempDir(), directory)
		writeTestFile(t, root, "nodes.yaml", "worker: {type: procedure}\n")
		artifact, err := AdmitDirectory(root)
		if err != nil {
			t.Fatal(err)
		}
		admission, present := artifact.RootAdmission()
		if !present || admission.Validate() != nil || admission.RootDirectory() != directory || admission.BundleHash() != artifact.BundleHash() {
			t.Fatalf("filesystem root lost its exact admission: present=%t root=%q hash=%q", present, admission.RootDirectory(), admission.BundleHash())
		}
		rooted, err := artifact.RootedSource()
		if err != nil || rooted.Validate() != nil || rooted.Artifact() != artifact || !rooted.Admission().Equal(admission) {
			t.Fatalf("rooted source detached from admitted members: %+v, %v", rooted, err)
		}
		if previous != nil {
			if !bytes.Equal(previous.LogicalBlob(), artifact.LogicalBlob()) || previous.BundleHash() != artifact.BundleHash() {
				t.Fatal("root directory contaminated byte-content identity")
			}
			if original.Equal(admission) {
				t.Fatal("distinct roots acquired the same rooted admission")
			}
		}
		previous, original = artifact, admission
	}
}

func TestS1RootAdmissionContentDecodeCannotMintRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "shop-a")
	writeTestFile(t, root, "nodes.yaml", "worker: {type: procedure}\n")
	artifact, err := AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := artifact.RootAdmission(); !present {
		t.Fatal("fresh admission did not capture its root")
	}
	row, err := PersistedFromArtifact(artifact, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := row.Decode()
	if err != nil {
		t.Fatal(err)
	}
	members, err := AdmitMembers(map[string][]byte{"nodes.yaml": []byte("worker: {type: procedure}\n")})
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []*AdmittedSourceArtifact{decoded, members, nil} {
		if _, present := source.RootAdmission(); present {
			t.Fatal("content-only source invented root authority")
		}
		if _, err := source.RootedSource(); !errors.Is(err, ErrRootAdmissionMissing) {
			t.Fatalf("content-only source became executable rooted evidence: %v", err)
		}
	}
}

func TestS1RootAdmissionValidatesSelectedDirectory(t *testing.T) {
	for _, directory := range []string{"My Shop", "shop.root", "SHOP", "-shop", "con"} {
		t.Run(directory, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), directory)
			writeTestFile(t, root, "nodes.yaml", "worker: {type: procedure}\n")
			if _, err := AdmitDirectory(root); err == nil || !strings.Contains(err.Error(), "rename it") {
				t.Fatalf("invalid selected root gained admission: %v", err)
			}
		})
	}
}

func TestS1RootAdmissionRejectsContradictoryOrZeroValues(t *testing.T) {
	if err := (RootAdmission{}).Validate(); !errors.Is(err, ErrRootAdmissionMissing) {
		t.Fatalf("zero root admission: %v", err)
	}
	if err := (RootedSource{}).Validate(); !errors.Is(err, ErrRootAdmissionMissing) {
		t.Fatalf("zero rooted source: %v", err)
	}
	root := filepath.Join(t.TempDir(), "shop-a")
	writeTestFile(t, root, "nodes.yaml", "worker: {type: procedure}\n")
	artifact, err := AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, admission := range []RootAdmission{
		{bundleHash: artifact.BundleHash(), rootDirectory: "SHOP"},
		{bundleHash: artifact.BundleHash(), rootDirectory: "shop-b"},
		{bundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), rootDirectory: "shop-a"},
		{bundleHash: "bad", rootDirectory: "shop-a"},
	} {
		if err := (RootedSource{artifact: artifact, admission: admission}).Validate(); err == nil {
			t.Fatalf("contradictory root evidence accepted: %+v", admission)
		}
	}
}

func TestS1RootAdmissionRetainedDecoderBindsContentWithoutMutatingIt(t *testing.T) {
	root := filepath.Join(t.TempDir(), "shop-a")
	writeTestFile(t, root, "nodes.yaml", "worker: {type: procedure}\n")
	artifact, err := AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	admission, _ := artifact.RootAdmission()
	row, err := PersistedFromArtifact(artifact, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	rooted, err := RestoreRootedSource(row, admission.Reference())
	if err != nil || rooted.Validate() != nil || !rooted.Admission().Equal(admission) {
		t.Fatalf("retained decode lost exact rooted evidence: %+v, %v", rooted, err)
	}
	if _, present := rooted.Artifact().RootAdmission(); present {
		t.Fatal("retained decoder changed the content artifact's authority")
	}
	foreign := admission.Reference()
	foreign.BundleHash = "bundle-v2:sha256:" + strings.Repeat("a", 64)
	if _, err := RestoreRootedSource(row, foreign); err == nil {
		t.Fatal("retained decode accepted another artifact's root reference")
	}
	invalid := admission.Reference()
	invalid.RootDirectory = "My Shop"
	if _, err := RestoreRootedSource(row, invalid); err == nil {
		t.Fatal("retained decode accepted a noncanonical root")
	}
}
