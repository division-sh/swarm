package runlifecycle_test

import (
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1RunCreationRequiresRootAdmission(t *testing.T) {
	rooted, _ := sourceartifactfixture.Rooted(t, "shop-a", sourceartifactfixture.Artifact())
	request := runlifecycle.CreateRequest{
		RunID: "rooted-run", Origin: runlifecycle.DeploymentRunOrigin(),
		Source: sourceartifactfixture.FactFor(rooted.Artifact()), StartedAt: time.Now().UTC(),
	}
	if err := request.Validate(); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) {
		t.Fatalf("content-only run creation acquired rooted authority: %v", err)
	}
	request.RootAdmission = rooted.Admission()
	if err := request.Validate(); err != nil {
		t.Fatalf("exact rooted run creation refused: %v", err)
	}
	foreign, _ := sourceartifactfixture.Rooted(t, "shop-b", otherRunAdmissionArtifact(t))
	request.RootAdmission = foreign.Admission()
	if err := request.Validate(); err == nil {
		t.Fatal("another source artifact supplied run root authority")
	}
}

func TestS1RunSourceRevisionRequiresRootAdmission(t *testing.T) {
	rooted, _ := sourceartifactfixture.Rooted(t, "shop-a", sourceartifactfixture.Artifact())
	request := runlifecycle.SourceRevisionRequest{RunID: "rooted-run", Source: sourceartifactfixture.FactFor(rooted.Artifact())}
	if err := request.Validate(); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) {
		t.Fatalf("content-only source revision acquired rooted authority: %v", err)
	}
	request.RootAdmission = rooted.Admission()
	if err := request.Validate(); err != nil {
		t.Fatalf("complete rooted source revision refused: %v", err)
	}
	foreign, _ := sourceartifactfixture.Rooted(t, "shop-b", otherRunAdmissionArtifact(t))
	request.RootAdmission = foreign.Admission()
	if err := request.Validate(); err == nil {
		t.Fatal("another source artifact supplied revision root authority")
	}
}

func TestS1RunSnapshotRequiresExactRootReference(t *testing.T) {
	rooted, _ := sourceartifactfixture.Rooted(t, "shop-a", sourceartifactfixture.Artifact())
	snapshot := runlifecycle.Snapshot{
		RunID: "rooted-run", Origin: runlifecycle.DeploymentRunOrigin(), State: runlifecycle.StateRunning,
		BundleHash: rooted.Artifact().BundleHash(), StartedAt: time.Now().UTC(),
	}
	if err := snapshot.Validate(); err == nil {
		t.Fatal("content-only run snapshot accepted")
	}
	snapshot.RootReference = rooted.Admission().Reference()
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("exact run source reference refused: %v", err)
	}
	snapshot.RootReference.RootDirectory = " shop-a"
	if err := snapshot.Validate(); err == nil {
		t.Fatal("run snapshot accepted normalized root spelling")
	}
	snapshot.RootReference = rooted.Admission().Reference()
	snapshot.RootReference.BundleHash = otherRunAdmissionArtifact(t).BundleHash()
	if err := snapshot.Validate(); err == nil {
		t.Fatal("run snapshot accepted a crossed source/root pair")
	}
}

func otherRunAdmissionArtifact(t *testing.T) *sourceartifact.AdmittedSourceArtifact {
	t.Helper()
	artifact, err := sourceartifact.AdmitMembers(map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.1\nplatform_version: '*'\n"),
		"schema.yaml":   []byte("stages: []\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return artifact
}
