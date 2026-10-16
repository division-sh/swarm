package runfork

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestS1ForkOperationRequiresItsExactTargetRoot(t *testing.T) {
	rootA, _ := sourceartifactfixture.Rooted(t, "fork-a", sourceartifactfixture.Artifact())
	rootB, _ := sourceartifactfixture.Rooted(t, "fork-b", sourceartifactfixture.Artifact())
	request := ForkOperationRequest{
		OperationID: uuid.NewString(), SourceRunID: uuid.NewString(), Actor: "operator", TransportHash: "wire-request",
		TargetBundleHash: rootA.Artifact().BundleHash(), TargetRootReference: rootA.Admission().Reference(),
		ContractSelection: RunForkContractSelection{Mode: RunForkContractSelectionModeBundleHash, BundleHash: rootA.Artifact().BundleHash()},
	}
	_, first, err := request.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	request.TargetRootReference = rootB.Admission().Reference()
	_, second, err := request.Canonical()
	if err != nil || second == first {
		t.Errorf("different same-content target roots reused operation meaning: %q, %q, %v", first, second, err)
	}
	request.TargetRootReference = rootA.Admission().Reference()
	for _, cell := range []struct {
		name string
		ref  sourceartifact.RootAdmissionReference
	}{
		{"missing", sourceartifact.RootAdmissionReference{}},
		{"hash_only", sourceartifact.RootAdmissionReference{BundleHash: request.TargetBundleHash}},
		{"malformed", sourceartifact.RootAdmissionReference{BundleHash: request.TargetBundleHash, RootDirectory: "fork-a "}},
		{"crossed_content", sourceartifact.RootAdmissionReference{BundleHash: sourceartifactfixture.New("nodes.yaml", []byte("worker: {}\n")).BundleHash(), RootDirectory: "fork-a"}},
	} {
		t.Run(cell.name, func(t *testing.T) {
			broken := request
			broken.TargetRootReference = cell.ref
			if _, _, err := broken.Canonical(); err == nil {
				t.Fatal("operation accepted missing, malformed or crossed target root evidence")
			}
		})
	}
	bound, err := request.BindResolvedPoint(RunForkPoint{Kind: RunForkPointDeploymentRevision, Revision: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, boundHash, err := bound.Canonical(); err != nil || boundHash != first || bound.TargetRootReference != request.TargetRootReference {
		t.Fatalf("point binding changed the captured root/request meaning: %+v, %q, %v", bound, boundHash, err)
	}
	raw, err := json.Marshal(bound)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ForkOperationRequest
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, decodedHash, err := decoded.Canonical(); err != nil || decodedHash != first || decoded.TargetRootReference != request.TargetRootReference {
		t.Fatalf("operation wire evidence lost the exact target pair: %+v, %q, %v", decoded, decodedHash, err)
	}
}
