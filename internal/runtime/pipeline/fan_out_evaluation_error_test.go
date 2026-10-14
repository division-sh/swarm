package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type joinedFanOutEvaluationOwner struct {
	*fanOutClaimOutcomeOwner
	failure error
	group   pipelineobligation.PublicationGroup
}

func (o *joinedFanOutEvaluationOwner) BeginFanOutEvaluation(context.Context, fanoutobligation.Claim, func(FanOutEvaluationInput) error) (FanOutEvaluationInput, pipelineobligation.PublicationGroup, error) {
	return FanOutEvaluationInput{}, o.group, o.failure
}

func (o *joinedFanOutEvaluationOwner) BlockFanOutClaim(_ context.Context, block FanOutBlockRequest) (FanOutClaimSettlement, error) {
	o.blocks = append(o.blocks, block)
	return FanOutClaimSettlement{}, nil
}

type failedFanOutEvaluationClose struct {
	pipelineobligation.PublicationGroup
	failure error
}

func (g failedFanOutEvaluationClose) Close(context.Context) error { return g.failure }

func TestFanOutCompositeLoadPreservesJoinedFailureAndCleanup(t *testing.T) {
	hydration := errors.New("independent hydration failure")
	sibling := errors.New("independent composite cleanup failure")
	closing := errors.New("group close failure")
	releasing := errors.New("exact claim release failure")
	claim := fanOutFailureTestClaim()
	owner := &joinedFanOutEvaluationOwner{
		failure: errors.Join(&FanOutEvaluationLoadError{Cause: hydration}, sibling),
		group:   failedFanOutEvaluationClose{failure: closing},
		fanOutClaimOutcomeOwner: &fanOutClaimOutcomeOwner{
			claimFn: func(context.Context, FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
				return fanoutobligation.Intent{Request: fanoutobligation.IntentRequest{Key: claim.Key}}, claim, true, nil
			},
			releaseFn: func(ctx context.Context, got fanoutobligation.Claim) (FanOutClaimSettlement, error) {
				if got != claim || ctx.Err() != nil {
					t.Fatal("release lost exact claim or retained cancellation")
				}
				return FanOutClaimSettlement{}, releasing
			},
		},
	}
	pc := &PipelineCoordinator{sourceArtifactFact: mustPipelineTestSourceArtifactFact(pipelineTestBundleHash), fanOutOwnerID: claim.Owner}
	_, err := pc.claimAndServeFanOutTurn(context.Background(), owner, nil, time.Now())
	for _, cause := range []error{hydration, sibling, closing, releasing} {
		if !errors.Is(err, cause) {
			t.Errorf("lost joined failure %v: %v", cause, err)
		}
	}
	if len(owner.retryRelease) != 0 || len(owner.commands) != 0 {
		t.Fatal("load failure entered retry or durable commit")
	}
}
