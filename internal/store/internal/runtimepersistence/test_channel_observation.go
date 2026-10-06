package runtimepersistence

import (
	"context"
	"database/sql"

	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
)

func ObserveChannelIntentForTest(ctx context.Context, selected any, demand render.IntentObservationQuery) (render.IntentObservation, bool, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return render.IntentObservation{}, false, err
	}
	_, postgres := selected.(*PostgresStore)
	var result render.IntentObservation
	var found bool
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		result, found, err = channeldelivery.ObserveIntent(ctx, tx, demand, postgres)
		return err
	})
	return result, found, err
}

func ObserveChannelDeliveryPlansForTest(ctx context.Context, selected any, cursor string, limit int) ([]render.Candidate, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	_, postgres := selected.(*PostgresStore)
	var plans []channeldelivery.Plan
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		plans, err = channeldelivery.ListCurrentPlans(ctx, tx, cursor, limit, postgres)
		return err
	})
	return projectDeliveryCandidates(plans), err
}

func ObserveChannelSentReceiptForTest(ctx context.Context, selected any, deliveryID, operationID string) (render.SentReceipt, bool, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return render.SentReceipt{}, false, err
	}
	_, postgres := selected.(*PostgresStore)
	var result render.SentReceipt
	var found bool
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		result, found, err = channeldelivery.ReadCurrentSentReceiptTx(ctx, tx, deliveryID, operationID, postgres)
		return err
	})
	return result, found, err
}
