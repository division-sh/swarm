package pipeline

import (
	"errors"
	"testing"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1StandingCandidateRequiresExactRootAdmission(t *testing.T) {
	source, _ := sourceartifactfixture.Rooted(t, "shop-a", sourceartifactfixture.Artifact())
	other, _ := sourceartifactfixture.Rooted(t, "other-content", sourceartifactfixture.New("schema.yaml", []byte("description: another source\n")))
	candidate := StandingServiceCandidate{
		BindingEnabled: true, ServiceID: runtimeflowidentity.StandingServiceID("."), FlowPath: ".",
		Source: sourceartifactfixture.FactFor(source.Artifact()),
	}
	if err := candidate.Validate(); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) {
		t.Errorf("content alone obtained standing declaration authority: %v", err)
	}
	candidate.RootAdmission = source.Admission()
	if err := candidate.Validate(); err != nil {
		t.Fatal(err)
	}
	candidate.RootAdmission = other.Admission()
	if err := candidate.Validate(); err == nil {
		t.Error("root admission for other content obtained standing declaration authority")
	}
}

func TestS1StandingAuthorityIncludesExactRoot(t *testing.T) {
	sourceA, _ := sourceartifactfixture.Rooted(t, "shop-a", sourceartifactfixture.Artifact())
	sourceB, _ := sourceartifactfixture.Rooted(t, "shop-b", sourceA.Artifact())
	serviceID := runtimeflowidentity.StandingServiceID(".")
	current := StandingServiceReconciliation{
		ServiceID: serviceID, FlowPath: ".", RunID: runtimeflowidentity.StandingGenerationRunID(serviceID, 1),
		Generation: 1, BundleHash: sourceA.Artifact().BundleHash(), RootReference: sourceA.Admission().Reference(),
	}
	if !current.SameAuthority(current) {
		t.Fatal("exact standing authority was not stable")
	}
	foreign := current
	foreign.RootReference = sourceB.Admission().Reference()
	if current.SameAuthority(foreign) || foreign.SameAuthority(current) {
		t.Error("same-content different root matched standing operation authority")
	}
	missing := current
	missing.RootReference = sourceartifact.RootAdmissionReference{}
	if current.SameAuthority(missing) || missing.SameAuthority(missing) {
		t.Error("missing root obtained standing operation authority")
	}
	contradictory := current
	contradictory.RootReference.BundleHash = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if contradictory.SameAuthority(contradictory) {
		t.Error("crossed standing source/root evidence obtained operation authority")
	}
}
