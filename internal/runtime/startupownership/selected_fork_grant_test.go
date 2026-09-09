package startupownership

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestSelectedForkGrantEvidenceHasOneAuthority(t *testing.T) {
	evidence := GrantEvidence{
		GrantID: uuid.NewString(), ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "owner",
		ProcessBootID: uuid.NewString(), BundleHash: startupBundleHashA, RuntimeInstanceID: uuid.NewString(),
		RuntimeGeneration: 1, StateVersion: 1, State: GrantPrepared,
		SelectedFork: &SelectedForkGrantBinding{
			BindingID: uuid.NewString(), ForkRunID: uuid.NewString(), ExecutionID: uuid.NewString(),
			ExecutionGeneration: 1, FenceGeneration: 1, ExecutionOwner: "execution-owner",
			AdmissionFingerprint:       "sha256:" + strings.Repeat("1", 64),
			ContainerPlanFingerprint:   "sha256:" + strings.Repeat("2", 64),
			ActorCensusFingerprint:     "sha256:" + strings.Repeat("3", 64),
			EffectiveConfigFingerprint: "sha256:" + strings.Repeat("4", 64),
			DeclarationPlanFingerprint: "sha256:" + strings.Repeat("5", 64),
			PreparationFingerprint:     "sha256:" + strings.Repeat("6", 64),
		},
	}
	if err := evidence.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*GrantEvidence)
	}{
		{"dual authority", func(e *GrantEvidence) { e.SourceSetRevision = "live-revision" }},
		{"missing authority", func(e *GrantEvidence) { e.SelectedFork = nil }},
		{"generation mismatch", func(e *GrantEvidence) { e.RuntimeGeneration++ }},
		{"zero binding", func(e *GrantEvidence) { e.SelectedFork.BindingID = uuid.Nil.String() }},
		{"zero fence", func(e *GrantEvidence) { e.SelectedFork.FenceGeneration = 0 }},
		{"malformed fingerprint", func(e *GrantEvidence) { e.SelectedFork.AdmissionFingerprint = "sha256:admission" }},
		{"missing owner", func(e *GrantEvidence) { e.SelectedFork.ExecutionOwner = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := evidence.clone()
			tc.mutate(&changed)
			if err := changed.Validate(); err == nil {
				t.Fatal("invalid selected grant evidence accepted")
			}
			if err := evidence.Validate(); err != nil {
				t.Fatalf("copied evidence changed original: %v", err)
			}
		})
	}
}
