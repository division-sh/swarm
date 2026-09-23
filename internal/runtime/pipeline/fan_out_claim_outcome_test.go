package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type fanOutClaimOutcomeOwner struct {
	fanOutFailureTestOwner
	claimFn   func(context.Context, FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error)
	releaseFn func(context.Context, fanoutobligation.Claim) (FanOutClaimSettlement, error)
	loads     int
	groups    int
}

func (o *fanOutClaimOutcomeOwner) BeginFanOutPublicationGroup(context.Context, fanoutobligation.Claim) (runtimepipelineobligation.PublicationGroup, error) {
	o.groups++
	return nil, errors.New("claim/evaluation error must stop publication group admission")
}

func (o *fanOutClaimOutcomeOwner) ClaimFanOutIntent(ctx context.Context, request FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	return o.claimFn(ctx, request)
}

func (o *fanOutClaimOutcomeOwner) ReleaseFanOutClaim(ctx context.Context, claim fanoutobligation.Claim) (FanOutClaimSettlement, error) {
	return o.releaseFn(ctx, claim)
}

func (o *fanOutClaimOutcomeOwner) LoadFanOutEvaluation(context.Context, fanoutobligation.Claim) (FanOutEvaluationInput, error) {
	o.loads++
	return FanOutEvaluationInput{}, errors.New("claim error must stop evaluation")
}

type fanOutClaimOutcomeBus struct {
	recordingPipelineBus
	prepares  int
	finalizes int
}

var _ FanOutPublicationPlanner = (*fanOutClaimOutcomeBus)(nil)

func (b *fanOutClaimOutcomeBus) PrepareEnginePublications(context.Context, []runtimeengine.EmitIntent) ([]runtimeengine.DurablePublicationPlan, error) {
	b.prepares++
	return nil, errors.New("claim error must stop publication preparation")
}

func (b *fanOutClaimOutcomeBus) PrepareFanOutPublication(context.Context, runtimepipelineobligation.PublicationGroup, int, runtimeengine.EmitIntent) (runtimeengine.DurablePublicationPlan, error) {
	b.prepares++
	return nil, errors.New("claim error must stop fan-out publication preparation")
}

func (b *fanOutClaimOutcomeBus) PrepareFanOutPublications(context.Context, runtimepipelineobligation.PublicationGroup, []FanOutPublicationRequest) ([]FanOutPublicationPreparation, error) {
	b.prepares++
	return nil, errors.New("claim error must stop batch fan-out publication preparation")
}

func (b *fanOutClaimOutcomeBus) FinalizeFanOutPublications(context.Context, runtimepipelineobligation.PublicationGroup, []runtimeengine.CommittedDurablePublication) error {
	b.finalizes++
	return errors.New("claim error must stop fan-out publication finalization")
}

func TestFanOutClaimErrorSettlesAcknowledgedOwnershipWithoutRetry(t *testing.T) {
	claimErr := errors.New("acknowledged claim cleanup failed")
	releaseErr := errors.New("claim release failed")
	for _, tc := range []struct {
		name       string
		found      bool
		claimErr   error
		releaseErr error
		releaseAck bool
		cancel     bool
	}{
		{name: "acknowledged", found: true, claimErr: claimErr, releaseAck: true},
		{name: "acknowledged release error", found: true, claimErr: claimErr, releaseErr: releaseErr, releaseAck: true},
		{name: "acknowledged caller canceled", found: true, claimErr: errors.Join(claimErr, context.Canceled), releaseAck: true, cancel: true},
		{name: "acknowledged canceled release error", found: true, claimErr: errors.Join(claimErr, context.Canceled), releaseErr: releaseErr, releaseAck: true, cancel: true},
		{name: "unacknowledged", claimErr: claimErr},
		{name: "no work"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type contextKey struct{}
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "claim-scope"))
			defer cancel()
			claim := fanOutFailureTestClaim()
			claims, releases := 0, 0
			owner := &fanOutClaimOutcomeOwner{
				claimFn: func(gotCtx context.Context, request FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
					claims++
					if gotCtx != ctx || request.Owner != claim.Owner || request.BundleHash != pipelineTestBundleHash {
						t.Fatalf("claim admission context/request changed: %#v", request)
					}
					if tc.cancel {
						cancel()
					}
					if !tc.found {
						return fanoutobligation.Intent{}, fanoutobligation.Claim{}, false, tc.claimErr
					}
					return fanoutobligation.Intent{Request: fanoutobligation.IntentRequest{Key: claim.Key}}, claim, true, tc.claimErr
				},
				releaseFn: func(releaseCtx context.Context, got fanoutobligation.Claim) (FanOutClaimSettlement, error) {
					releases++
					if got != claim {
						t.Fatalf("released claim = %#v, want %#v", got, claim)
					}
					if releaseCtx.Err() != nil || releaseCtx.Done() != nil || releaseCtx.Value(contextKey{}) != "claim-scope" {
						t.Fatal("claim settlement must preserve context values without caller cancellation")
					}
					return FanOutClaimSettlement{Acknowledged: tc.releaseAck}, tc.releaseErr
				},
			}
			owner.commit = func(FanOutChunkCommand) (CommittedFanOutChunk, error) {
				t.Fatal("claim error must not commit or retry a chunk")
				return CommittedFanOutChunk{}, nil
			}
			bus := &fanOutClaimOutcomeBus{}
			pc := &PipelineCoordinator{
				workflowStore:      &workflowInstanceStore{fanOutObligations: owner},
				sourceArtifactFact: mustPipelineTestSourceArtifactFact(pipelineTestBundleHash),
				fanOutOwnerID:      claim.Owner,
				bus:                bus,
			}
			reenter, err := pc.serveFanOutTurn(ctx, time.Now())
			wantDisposition := fanOutTurnAwaitScan
			if tc.releaseAck {
				wantDisposition = fanOutTurnReleased
			} else if !tc.found && tc.claimErr == nil {
				wantDisposition = fanOutTurnExhausted
			}
			if reenter != wantDisposition || !errors.Is(err, tc.claimErr) || (tc.releaseErr != nil && !errors.Is(err, tc.releaseErr)) {
				t.Fatalf("turn = reenter:%v err:%v, want claim:%v release:%v", reenter, err, tc.claimErr, tc.releaseErr)
			}
			wantReleases := 0
			if tc.found {
				wantReleases = 1
			}
			if claims != 1 || releases != wantReleases {
				t.Fatalf("claims:%d releases:%d, want 1/%d before return", claims, releases, wantReleases)
			}
			if owner.loads != 0 || owner.groups != 0 || len(owner.commands) != 0 || len(owner.retryRelease) != 0 || len(owner.blocks) != 0 || bus.prepares != 0 || bus.finalizes != 0 || len(bus.outboxIntents) != 0 || len(bus.publishes) != 0 || len(bus.directPublishes) != 0 {
				t.Fatalf("claim outcome entered evaluation/chunk/retry/provider work: owner:%#v bus:%#v", owner, bus)
			}
		})
	}
}

type fanOutCleanupOutcomeOwner struct {
	*fanOutClaimOutcomeOwner
	blockErr error
}

func (o *fanOutCleanupOutcomeOwner) BlockFanOutClaim(context.Context, FanOutBlockRequest) (FanOutClaimSettlement, error) {
	return FanOutClaimSettlement{}, o.blockErr
}

func TestFanOutPreparationFailureReportsUnsettledCleanup(t *testing.T) {
	blockErr := errors.New("blocking transaction failed")
	releaseErr := errors.New("cleanup transaction failed")
	for _, cleanupFails := range []bool{false, true} {
		t.Run(fmt.Sprint(cleanupFails), func(t *testing.T) {
			claim := fanOutFailureTestClaim()
			releases := 0
			owner := &fanOutCleanupOutcomeOwner{
				blockErr: blockErr,
				fanOutClaimOutcomeOwner: &fanOutClaimOutcomeOwner{
					claimFn: func(context.Context, FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
						return fanoutobligation.Intent{Request: fanoutobligation.IntentRequest{Key: claim.Key}}, claim, true, nil
					},
					releaseFn: func(ctx context.Context, got fanoutobligation.Claim) (FanOutClaimSettlement, error) {
						releases++
						if ctx.Err() != nil || got != claim {
							t.Fatal("cleanup lost exact claim or inherited cancellation")
						}
						if cleanupFails {
							return FanOutClaimSettlement{}, releaseErr
						}
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
			wantDisposition := fanOutTurnReleased
			if cleanupFails {
				wantDisposition = fanOutTurnAwaitScan
			}
			if disposition != wantDisposition || !errors.Is(err, blockErr) || errors.Is(err, releaseErr) != cleanupFails {
				t.Fatalf("disposition=%v err=%v; failed cleanup must remain observable", disposition, err)
			}
			if releases != 1 || owner.loads != 1 || owner.groups != 0 || len(owner.commands) != 0 || len(owner.retryRelease) != 0 {
				t.Fatalf("loads=%d releases=%d; preparation failure must not enter publication or retry", owner.loads, releases)
			}
		})
	}
}
