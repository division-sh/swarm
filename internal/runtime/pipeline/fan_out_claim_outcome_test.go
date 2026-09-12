package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

type fanOutClaimOutcomeOwner struct {
	fanOutFailureTestOwner
	claimFn   func(context.Context, FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error)
	releaseFn func(context.Context, fanoutobligation.Claim) error
	loads     int
}

func (o *fanOutClaimOutcomeOwner) ClaimFanOutIntent(ctx context.Context, request FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	return o.claimFn(ctx, request)
}

func (o *fanOutClaimOutcomeOwner) ReleaseFanOutClaim(ctx context.Context, claim fanoutobligation.Claim) error {
	return o.releaseFn(ctx, claim)
}

func (o *fanOutClaimOutcomeOwner) LoadFanOutEvaluation(context.Context, fanoutobligation.Claim) (FanOutEvaluationInput, error) {
	o.loads++
	return FanOutEvaluationInput{}, errors.New("claim error must stop evaluation")
}

type fanOutClaimOutcomeBus struct {
	recordingPipelineBus
	prepares int
}

func (b *fanOutClaimOutcomeBus) PrepareEnginePublications(context.Context, []runtimeengine.EmitIntent) ([]runtimeengine.DurablePublicationPlan, error) {
	b.prepares++
	return nil, errors.New("claim error must stop publication preparation")
}

func TestFanOutClaimErrorSettlesAcknowledgedOwnershipWithoutRetry(t *testing.T) {
	claimErr := errors.New("acknowledged claim cleanup failed")
	releaseErr := errors.New("claim release failed")
	for _, tc := range []struct {
		name       string
		found      bool
		claimErr   error
		releaseErr error
		cancel     bool
	}{
		{name: "acknowledged", found: true, claimErr: claimErr},
		{name: "acknowledged release error", found: true, claimErr: claimErr, releaseErr: releaseErr},
		{name: "acknowledged caller canceled", found: true, claimErr: errors.Join(claimErr, context.Canceled), cancel: true},
		{name: "acknowledged canceled release error", found: true, claimErr: errors.Join(claimErr, context.Canceled), releaseErr: releaseErr, cancel: true},
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
				releaseFn: func(releaseCtx context.Context, got fanoutobligation.Claim) error {
					releases++
					if got != claim {
						t.Fatalf("released claim = %#v, want %#v", got, claim)
					}
					if releaseCtx.Err() != nil || releaseCtx.Done() != nil || releaseCtx.Value(contextKey{}) != "claim-scope" {
						t.Fatal("claim settlement must preserve context values without caller cancellation")
					}
					return tc.releaseErr
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
			if reenter || !errors.Is(err, tc.claimErr) || (tc.releaseErr != nil && !errors.Is(err, tc.releaseErr)) {
				t.Fatalf("turn = reenter:%v err:%v, want claim:%v release:%v", reenter, err, tc.claimErr, tc.releaseErr)
			}
			wantReleases := 0
			if tc.found {
				wantReleases = 1
			}
			if claims != 1 || releases != wantReleases {
				t.Fatalf("claims:%d releases:%d, want 1/%d before return", claims, releases, wantReleases)
			}
			if owner.loads != 0 || len(owner.commands) != 0 || len(owner.retryRelease) != 0 || len(owner.blocks) != 0 || bus.prepares != 0 || len(bus.outboxIntents) != 0 || len(bus.publishes) != 0 || len(bus.directPublishes) != 0 {
				t.Fatalf("claim outcome entered evaluation/chunk/retry/provider work: owner:%#v bus:%#v", owner, bus)
			}
		})
	}
}
