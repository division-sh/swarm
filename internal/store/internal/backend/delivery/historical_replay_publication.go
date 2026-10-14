package delivery

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type historicalReplayEventCommitOwner interface {
	AppendAdmittedEventTxOutcome(context.Context, *mutationprotocol.Attempt, events.AdmittedEvent, events.RouteSettlement) (bus.EventAppendOutcome, error)
}

// Replay lineage references its delivery, so the first write cannot reread that
// lineage yet. Appending here binds validation and insertion to the exact event
// writer and mutation; claims and recovery still require complete durable facts.
func (a *Adapter) CommitHistoricalReplayInitialPublication(ctx context.Context, attempt *mutationprotocol.Attempt, publisher historicalReplayEventCommitOwner, prepared bus.PreparedPublishEvent, authority deliverylifecycle.ExecutionAuthority) (bus.EventAppendOutcome, []deliverylifecycle.DurableHandoffProof, error) {
	if attempt == nil || publisher == nil {
		return bus.EventAppendOutcomeUnknown, nil, fmt.Errorf("historical replay publication requires its event writer and mutation")
	}
	obligations, err := historicalReplayInitialObligations(prepared, authority)
	if err != nil {
		return bus.EventAppendOutcomeUnknown, nil, err
	}
	outcome, err := publisher.AppendAdmittedEventTxOutcome(ctx, attempt, prepared.Event, prepared.Settlement)
	if err != nil {
		return bus.EventAppendOutcomeUnknown, nil, err
	}
	if outcome == bus.EventAppendExactDuplicate {
		proofs, err := a.historicalReplayDuplicateProofs(ctx, attempt, prepared, authority)
		return outcome, proofs, err
	}
	if outcome != bus.EventAppendInserted {
		return bus.EventAppendOutcomeUnknown, nil, fmt.Errorf("historical replay event writer did not acknowledge an exact publication")
	}
	proofs, err := withDeliverySQL(ctx, attempt, func(ctx context.Context, tx *sql.Tx) ([]deliverylifecycle.DurableHandoffProof, error) {
		proofs := make([]deliverylifecycle.DurableHandoffProof, 0, len(obligations))
		for _, obligation := range obligations {
			proof, err := a.insertExactObligation(ctx, tx, attempt, obligation)
			if err != nil {
				return nil, err
			}
			proofs = append(proofs, proof)
		}
		return proofs, nil
	})
	return outcome, proofs, err
}

func (a *Adapter) historicalReplayDuplicateProofs(ctx context.Context, attempt *mutationprotocol.Attempt, prepared bus.PreparedPublishEvent, authority deliverylifecycle.ExecutionAuthority) ([]deliverylifecycle.DurableHandoffProof, error) {
	return withDeliverySQL(ctx, attempt, func(ctx context.Context, tx *sql.Tx) ([]deliverylifecycle.DurableHandoffProof, error) {
		snapshots, err := a.SnapshotsForEvent(ctx, tx, prepared.Event.ID())
		if err != nil {
			return nil, err
		}
		routes := make([]events.DeliveryRoute, 0, len(snapshots))
		for _, snapshot := range snapshots {
			routes = append(routes, snapshot.Route)
		}
		if !reflect.DeepEqual(events.NormalizeDeliveryRoutes(routes), events.NormalizeDeliveryRoutes(prepared.DeliveryRoutes)) {
			return nil, fmt.Errorf("historical replay duplicate crosses its complete durable delivery aggregate")
		}
		return a.commitInitialTx(ctx, tx, attempt, prepared.Event.ID(), prepared.Event.Event().RunID(), prepared.DeliveryRoutes, authority)
	})
}

func historicalReplayInitialObligations(prepared bus.PreparedPublishEvent, authority deliverylifecycle.ExecutionAuthority) ([]deliverylifecycle.Obligation, error) {
	event := prepared.Event.Event()
	lineage, captured := event.SelectedForkLineage()
	if event.AdmissionClass() != events.EventAdmissionSelectedForkReplay || !captured ||
		lineage.AuthorityStamp() != runfork.RunForkDeliveryEventReplayOwner ||
		prepared.Settlement.WriteClass() != events.EventWriteHistoricalRunForkReplay || len(prepared.DeliveryRoutes) == 0 ||
		authority.Kind() != deliverylifecycle.ExecutionAuthorityNormalRuntime {
		return nil, fmt.Errorf("historical replay initial publication requires its exact admitted replay owner")
	}
	if err := prepared.Validate(); err != nil {
		return nil, err
	}
	if err := authority.Validate(); err != nil {
		return nil, err
	}
	obligations := make([]deliverylifecycle.Obligation, 0, len(prepared.DeliveryRoutes))
	for _, route := range events.NormalizeDeliveryRoutes(prepared.DeliveryRoutes) {
		obligation, err := deliverylifecycle.NewObligation(event.ID(), event.RunID(), route, authority)
		if err != nil {
			return nil, err
		}
		obligations = append(obligations, obligation)
	}
	return obligations, nil
}
