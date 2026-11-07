//go:build linux || darwin

package sessionprovider

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

type registeredAckOutcomeFixture struct {
	result effects.OperationOutcome
	found  bool
}

func (s registeredAckOutcomeFixture) GetExternalEffectOutcome(context.Context, string) (effects.OperationOutcome, bool, error) {
	return s.result, s.found, nil
}

func TestRegisteredAcknowledgementReplayRefusesWrongIdentityAndUncertainStates(t *testing.T) {
	id := uuid.NewString()
	base := effects.OperationOutcome{OperationID: id, Kind: effects.KindChannelConfirmation,
		AuthorityKind: effects.AuthorityChannelConfirmation, AuthorityID: id, State: effects.StateSettled, AttemptState: effects.StateSettled}
	if settled, err := registeredAcknowledgementSettled(t.Context(), registeredAckOutcomeFixture{base, true}, id); err != nil || !settled {
		t.Fatal("exact terminal acknowledgement was not reused", settled, err)
	}
	for _, mutate := range []func(*effects.OperationOutcome){
		func(o *effects.OperationOutcome) { o.OperationID = uuid.NewString() },
		func(o *effects.OperationOutcome) { o.AuthorityID = uuid.NewString() },
		func(o *effects.OperationOutcome) { o.Kind = effects.KindChannelDelivery },
		func(o *effects.OperationOutcome) { o.AuthorityKind = effects.AuthorityChannelDelivery },
		func(o *effects.OperationOutcome) { o.AttemptState = effects.StateLaunched },
		func(o *effects.OperationOutcome) { o.AttemptState = effects.StateResponseObserved },
		func(o *effects.OperationOutcome) { o.AttemptState = effects.StateOutcomeUncertain },
	} {
		changed := base
		mutate(&changed)
		if settled, err := registeredAcknowledgementSettled(t.Context(), registeredAckOutcomeFixture{changed, true}, id); err == nil || settled {
			t.Fatal("wrong or uncertain acknowledgement was reused", changed, settled, err)
		}
	}
	for _, state := range []effects.State{effects.StateAuthorized, effects.StateTerminalFailure} {
		unlaunched := base
		unlaunched.State, unlaunched.AttemptState = state, state
		if settled, err := registeredAcknowledgementSettled(t.Context(), registeredAckOutcomeFixture{unlaunched, true}, id); err != nil || settled {
			t.Fatal("unlaunched outcome fabricated success or blocked exact journal admission", state, settled, err)
		}
	}
}
