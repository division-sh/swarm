package eventpersistence

import (
	"context"
	"database/sql"
	"fmt"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

func commitFanOutPublicationTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *revisionEffects,
	store eventCommitTxStore, postgres bool, command runtimebus.PublicationCommand, projection fanoutobligation.OrdinalEmission,
	handoff *runLifecycleCandidateHandoffReservation,
) (runtimebus.CommittedPublication, error) {
	if tx == nil || story == nil {
		return runtimebus.CommittedPublication{}, fmt.Errorf("fan-out publication requires the named chunk transaction")
	}
	if err := command.ValidateFanOut(); err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	if err := projection.ValidateEvent(command.Commit.Event.Event()); err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	return commitValidatedPublicationTx(ctx, tx, story, effects, store, postgres, command, handoff)
}

func (s *EventPostgresOwner) CommitFanOutPublicationTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *revisionEffects,
	command runtimebus.PublicationCommand, projection fanoutobligation.OrdinalEmission, handoff *runLifecycleCandidateHandoffReservation,
) (runtimebus.CommittedPublication, error) {
	return commitFanOutPublicationTx(ctx, tx, story, effects, s, true, command, projection, handoff)
}

func (s *EventSQLiteOwner) CommitFanOutPublicationTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *revisionEffects,
	command runtimebus.PublicationCommand, projection fanoutobligation.OrdinalEmission, handoff *runLifecycleCandidateHandoffReservation,
) (runtimebus.CommittedPublication, error) {
	return commitFanOutPublicationTx(ctx, tx, story, effects, s, false, command, projection, handoff)
}
