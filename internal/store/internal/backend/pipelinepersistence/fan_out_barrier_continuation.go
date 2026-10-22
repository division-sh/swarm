package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
)

// LoadFanOutBarrierContinuationTx consumes the existing row reader. It grants
// neither a historical cut nor delivery/execution authority.
func LoadFanOutBarrierContinuationTx(ctx context.Context, tx *sql.Tx, postgres bool, key fanoutobligation.IntentKey) (fanoutbarrier.Barrier, error) {
	return readFanOutBarrierContinuationTx(ctx, tx, postgres, key, true)
}

// ReadFanOutBarrierContinuationTx shares the complete decoder without acquiring
// a mutation lock. It is observation only, never a completion admission.
func ReadFanOutBarrierContinuationTx(ctx context.Context, tx *sql.Tx, postgres bool, key fanoutobligation.IntentKey) (fanoutbarrier.Barrier, error) {
	return readFanOutBarrierContinuationTx(ctx, tx, postgres, key, false)
}

func readFanOutBarrierContinuationTx(ctx context.Context, tx *sql.Tx, postgres bool, key fanoutobligation.IntentKey, forUpdate bool) (fanoutbarrier.Barrier, error) {
	if err := key.Validate(); err != nil {
		return fanoutbarrier.Barrier{}, err
	}
	rows, err := loadFanOutBarriers(ctx, tx, postgres, key.RunID, "", &key, forUpdate)
	if err != nil {
		return fanoutbarrier.Barrier{}, err
	}
	for _, row := range rows {
		if row.Registration.IntentKey != key {
			continue
		}
		if row.TransferredOccurrence == nil {
			return fanoutbarrier.Barrier{}, fmt.Errorf("materialized barrier lacks its retained publication")
		}
		return row, nil
	}
	return fanoutbarrier.Barrier{}, fmt.Errorf("materialized barrier lacks its exact unfinished owner")
}

func requireTransferredBarrierCompletionClaim(ctx context.Context, tx *sql.Tx, postgres bool, owner fanoutbarrier.Barrier, claim deliverylifecycle.Claim) error {
	if err := claim.Validate(); err != nil {
		return err
	}
	adapter := sqliteDeliveryAdapter
	if postgres {
		adapter = postgresDeliveryAdapter
	}
	inbound, err := adapter.Snapshot(ctx, tx, claim.DeliveryID())
	if err != nil {
		return err
	}
	ref, _ := owner.Registration.Handle.JoinRef()
	target := events.RouteIdentity{FlowID: ref.FlowPath(), FlowInstance: owner.Registration.Route.InstancePath, EntityID: owner.Registration.EntityID}
	if inbound.EventID != owner.TransferredOccurrence.Publication.EventID || inbound.RunID != owner.Registration.IntentKey.RunID ||
		inbound.Route.Recipient != events.MustNodeDeliveryRecipient(ref.Node()) || !events.SameRouteIdentity(inbound.Route.Target.Route(), target) {
		return fmt.Errorf("transferred barrier completion requires its exact inbound publication and receiver")
	}
	original, _, err := loadFanOutSourceEvent(ctx, tx, inbound.EventID, postgres)
	if err != nil {
		return err
	}
	occurrence, err := genericschedule.NewTransferredFanOutBarrierOccurrence(owner)
	if err != nil {
		return err
	}
	return occurrence.ValidateEvent(original)
}
