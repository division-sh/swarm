package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

type fanOutAcknowledgedFaultOwner struct {
	*fanOutFailureTestOwner
	fault error
}

func (o *fanOutAcknowledgedFaultOwner) ReleaseFanOutRetryable(ctx context.Context, request FanOutRetryableRelease) (FanOutClaimSettlement, error) {
	settlement, err := o.fanOutFailureTestOwner.ReleaseFanOutRetryable(ctx, request)
	if settlement.Acknowledged {
		err = errors.Join(err, o.fault)
	}
	return settlement, err
}

func (o *fanOutAcknowledgedFaultOwner) BlockFanOutClaim(ctx context.Context, request FanOutBlockRequest) (FanOutClaimSettlement, error) {
	settlement, err := o.fanOutFailureTestOwner.BlockFanOutClaim(ctx, request)
	if settlement.Acknowledged {
		err = errors.Join(err, o.fault)
	}
	return settlement, err
}

func TestFanOutAcknowledgedBlockAndRetryErrorsKeepDurableDisposition(t *testing.T) {
	claim := fanOutFailureTestClaim()
	postCommitFault := errors.New("claim postcommit cleanup failed")
	retryCause := runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "fan_out_ack_retry", "test", "commit", nil)
	for _, operation := range []string{"precommit_retry", "commit_retry", "commit_block"} {
		t.Run(operation, func(t *testing.T) {
			base := &fanOutFailureTestOwner{}
			owner := &fanOutAcknowledgedFaultOwner{fanOutFailureTestOwner: base, fault: postCommitFault}
			planner := &fanOutFailureTestPlanner{}
			var disposition fanOutTurnDisposition
			var err error
			switch operation {
			case "precommit_retry":
				var released bool
				released, err = releaseFanOutPrecommitRetry(context.Background(), owner, planner, claim, nil, retryCause, time.Now())
				if !released {
					t.Fatal("acknowledged retry release was classified as unsettled")
				}
				disposition = fanOutTurnRetryWait
			case "commit_retry":
				base.commit = func(FanOutChunkCommand) (CommittedFanOutChunk, error) { return CommittedFanOutChunk{}, retryCause }
				_, disposition, err = new(PipelineCoordinator).commitFanOutRange(context.Background(), owner, planner, nil, claim, fanOutFailureOutcomes(0, 1), time.Now())
			case "commit_block":
				base.commit = func(FanOutChunkCommand) (CommittedFanOutChunk, error) {
					return CommittedFanOutChunk{}, errors.New("commit failed")
				}
				_, disposition, err = new(PipelineCoordinator).commitFanOutRange(context.Background(), owner, planner, nil, claim, fanOutFailureOutcomes(0, 1), time.Now())
			}
			wantDisposition, wantRetry, wantBlock := fanOutTurnRetryWait, 1, 0
			if operation == "commit_block" {
				wantDisposition, wantRetry, wantBlock = fanOutTurnBlocked, 0, 1
			}
			if disposition != wantDisposition || !errors.Is(err, postCommitFault) || len(base.retryRelease) != wantRetry || len(base.blocks) != wantBlock {
				t.Fatalf("%s disposition=%v err=%v retry=%d block=%d", operation, disposition, err, len(base.retryRelease), len(base.blocks))
			}
		})
	}
}

type fanOutAcknowledgedBlockOwner struct {
	*fanOutClaimOutcomeOwner
	fault  error
	blocks int
}

func (o *fanOutAcknowledgedBlockOwner) BlockFanOutClaim(context.Context, FanOutBlockRequest) (FanOutClaimSettlement, error) {
	o.blocks++
	return FanOutClaimSettlement{Acknowledged: true}, o.fault
}

func TestFanOutPreparationBlockAckSuppressesReleaseDespiteCleanupError(t *testing.T) {
	claim := fanOutFailureTestClaim()
	fault := errors.New("block postcommit cleanup failed")
	releases := 0
	owner := &fanOutAcknowledgedBlockOwner{
		fault: fault,
		fanOutClaimOutcomeOwner: &fanOutClaimOutcomeOwner{
			claimFn: func(context.Context, FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
				return fanoutobligation.Intent{Request: fanoutobligation.IntentRequest{Key: claim.Key}}, claim, true, nil
			},
			releaseFn: func(context.Context, fanoutobligation.Claim) (FanOutClaimSettlement, error) {
				releases++
				return FanOutClaimSettlement{Acknowledged: true}, nil
			},
		},
	}
	pc := &PipelineCoordinator{
		workflowStore:      &workflowInstanceStore{fanOutObligations: owner},
		sourceArtifactFact: mustPipelineTestSourceArtifactFact(pipelineTestBundleHash),
		fanOutOwnerID:      claim.Owner,
	}
	disposition, err := pc.serveFanOutTurn(context.Background(), time.Now())
	if disposition != fanOutTurnBlocked || !errors.Is(err, fault) || owner.blocks != 1 || releases != 0 {
		t.Fatalf("acknowledged block disposition=%v err=%v blocks=%d releases=%d", disposition, err, owner.blocks, releases)
	}
}
