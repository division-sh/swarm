package eventpersistence

import (
	"context"
	"fmt"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func commitFanOutPublicationTx(ctx context.Context, attempt *mutationprotocol.Attempt,
	store eventCommitTxStore, command runtimebus.PublicationCommand, projection fanoutobligation.OrdinalEmission,
) (runtimebus.CommittedPublication, error) {
	if attempt == nil {
		return runtimebus.CommittedPublication{}, fmt.Errorf("fan-out publication requires the named chunk mutation attempt")
	}
	if err := command.ValidateFanOut(); err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	if err := projection.ValidateEvent(command.Commit.Event.Event()); err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	return commitValidatedPublicationTx(ctx, attempt, store, command)
}

func (s *EventPostgresOwner) CommitFanOutPublicationTx(ctx context.Context, attempt *mutationprotocol.Attempt,
	command runtimebus.PublicationCommand, projection fanoutobligation.OrdinalEmission,
) (runtimebus.CommittedPublication, error) {
	return commitFanOutPublicationTx(ctx, attempt, s, command, projection)
}

func (s *EventSQLiteOwner) CommitFanOutPublicationTx(ctx context.Context, attempt *mutationprotocol.Attempt,
	command runtimebus.PublicationCommand, projection fanoutobligation.OrdinalEmission,
) (runtimebus.CommittedPublication, error) {
	return commitFanOutPublicationTx(ctx, attempt, s, command, projection)
}
