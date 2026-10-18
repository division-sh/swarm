package pipelinepersistence

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

type WorkflowMutationCommitObservationForTest = transactiontest.WorkflowDomainObservation

func (s *PipelinePostgresOwner) HoldWorkflowMutationCommitForTest(ctx context.Context, claim deliverylifecycle.Claim) (*WorkflowMutationCommitObservationForTest, error) {
	snapshot, err := s.Snapshot(ctx, claim.DeliveryID())
	if err != nil {
		return nil, err
	}
	original, err := workflowObservationAuthorityForTest(ctx, claim, snapshot)
	if err != nil {
		return nil, err
	}
	return s.backend.HoldWorkflowDomainForTest(original, claim)
}

func (s *PipelineSQLiteOwner) HoldWorkflowMutationCommitForTest(ctx context.Context, claim deliverylifecycle.Claim) (*WorkflowMutationCommitObservationForTest, error) {
	snapshot, err := s.Snapshot(ctx, claim.DeliveryID())
	if err != nil {
		return nil, err
	}
	original, err := workflowObservationAuthorityForTest(ctx, claim, snapshot)
	if err != nil {
		return nil, err
	}
	return s.backend.HoldWorkflowDomainForTest(original, claim)
}

func workflowObservationAuthorityForTest(ctx context.Context, claim deliverylifecycle.Claim, snapshot deliverylifecycle.Snapshot) (startupownership.MutationAuthority, error) {
	var empty startupownership.MutationAuthority
	if err := claim.Validate(); err != nil {
		return empty, err
	}
	inbound, found := deliverylifecycle.ClaimFromContext(ctx)
	if !found || inbound != claim || snapshot.Status != deliverylifecycle.StatusInProgress || snapshot.ClaimVersion != claim.Version() ||
		snapshot.DeliveryID != claim.DeliveryID() || snapshot.RunID != claim.RunID() || snapshot.SubscriberID != claim.SubscriberID() {
		return empty, errors.New("workflow observation requires the original owner's exact active claim")
	}
	original, err := startupownership.CaptureMutationAuthority(ctx)
	if err != nil {
		return empty, err
	}
	grant := original.GrantEvidence()
	if !original.HasRuntimeGrant() || snapshot.Authority.ExecutionID() != grant.GrantID || snapshot.Authority.Generation() != grant.RuntimeGeneration ||
		snapshot.Authority.SourceArtifact().BundleHash() != grant.BundleHash {
		return empty, errors.New("workflow observation claim and original generation differ")
	}
	return original, nil
}
