package runlifecycle

import (
	"context"
	"errors"
	"reflect"

	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
)

// Materialization parks the child until the live ACK. Only its original
// selected executor may settle preparation work before that handoff.
func ObserveMaterializedSelectedExecution(ctx context.Context, q sourceadmission.RowQueryer, runID, bundleHash string) (bool, error) {
	authority, err := selectedRunAuthority(ctx, runID, bundleHash)
	if err != nil {
		return false, nil
	}
	original, err := startupownership.CaptureMutationAuthority(ctx)
	if errors.Is(err, startupownership.ErrMutationAuthorityMissing) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !original.HasRuntimeGrant() || !selectedMaterializationGrantMatches(original.GrantEvidence(), authority, bundleHash) {
		return false, nil
	}
	evidence := original.GrantEvidence()
	if copied, present := startupownership.GenerationGrantEvidenceFromContext(ctx); present && !reflect.DeepEqual(copied, evidence) {
		return false, nil
	}
	return sourceadmission.ObserveMaterializedSelectedGrant(ctx, q, evidence)
}

func selectedMaterializationGrantMatches(evidence startupownership.GrantEvidence, authority effects.Authority, bundleHash string) bool {
	selected := evidence.SelectedFork
	if evidence.State != startupownership.GrantAdmitted || evidence.BundleHash != bundleHash || selected == nil {
		return false
	}
	expected := effects.SelectedContractForkAuthority{
		ExecutionID: selected.ExecutionID, ForkRunID: selected.ForkRunID, Generation: selected.ExecutionGeneration,
		AdmissionFingerprint: selected.AdmissionFingerprint, ContainerPlanFingerprint: selected.ContainerPlanFingerprint,
		ActorCensusFingerprint: selected.ActorCensusFingerprint, EffectiveConfigFingerprint: selected.EffectiveConfigFingerprint,
	}
	return selected.ExecutionOwner == authority.ExecutionOwner && selected.FenceGeneration == authority.FenceGeneration && expected == authority.SelectedFork
}
