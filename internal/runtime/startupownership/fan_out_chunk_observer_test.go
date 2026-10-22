package startupownership

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type fanOutChunkObservationOwner struct {
	pipeline.FanOutObligationOwner
	result   pipeline.CommittedFanOutChunk
	err      error
	called   bool
	observed bool
}

func (o *fanOutChunkObservationOwner) CommitFanOutChunk(_ context.Context, command pipeline.FanOutChunkCommand) (pipeline.CommittedFanOutChunk, error) {
	o.called = true
	if !o.observed || command.Outcomes[0].Ordinal != 0 {
		return pipeline.CommittedFanOutChunk{}, errors.New("observer changed the actual command or ran after the owner")
	}
	return o.result, o.err
}

func TestFanOutChunkObserverPreservesNativeCommandOutsideOwnerMutex(t *testing.T) {
	for _, fault := range []error{nil, errors.New("native chunk diagnostic")} {
		claim := fanoutobligation.Claim{Key: fanoutobligation.IntentKey{RunID: uuid.NewString(), DeploymentFeedID: uuid.NewString()}, Owner: "native", Generation: 2, LeaseUntil: time.Now().Add(time.Minute)}
		command := pipeline.FanOutChunkCommand{Claim: claim, Now: time.Now(), Outcomes: []pipeline.FanOutChunkOutcome{{Ordinal: 0}}}
		native := &fanOutChunkObservationOwner{err: fault, result: pipeline.CommittedFanOutChunk{Intent: fanoutobligation.Intent{ClaimGeneration: 2}}}
		owner := &processCapability{fanOutCapacity: &fanOutCapacityState{registrations: make(map[string]*FanOutServingRegistration)}}
		observer := func(_ context.Context, observed pipeline.FanOutChunkCommand) {
			if native.called || observed.Claim != claim || !owner.mu.TryLock() {
				t.Fatal("observer changed claim evidence or ran after the native call/inside the owner mutex")
			}
			owner.mu.Unlock()
			native.observed = true
			observed.Outcomes[0].Ordinal = 99
		}
		ctx := WithFanOutChunkObserverForTest(context.Background(), observer)
		registration := &FanOutServingRegistration{}
		registration.testChunkObserver, _ = ctx.Value(fanOutChunkObserverContextKey{}).(func(context.Context, pipeline.FanOutChunkCommand))
		owner.fanOutCapacity.registrations["exact-grant"] = registration
		service := &fanOutServingService{owner: owner}
		bound := &fanOutObservedClaimOwner{FanOutObligationOwner: native, service: service, candidate: FanOutCandidate{GrantID: "exact-grant", Key: claim.Key}}
		result, err := bound.CommitFanOutChunk(ctx, command)
		if !native.called || result.Intent.ClaimGeneration != 2 || err != fault || command.Outcomes[0].Ordinal != 0 {
			t.Fatalf("observer changed native command/result/error: %+v error=%v", result, err)
		}
	}
}
