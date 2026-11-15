package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
)

func ObserveChannelAuthorActivityForTest(ctx context.Context, selected any, opts authoractivity.ListOptions) (authoractivity.ListResult, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return authoractivity.ListResult{}, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.activityPostgresOwner.ObserveChannelAuthorActivityForTest(ctx, opts)
	case *SQLiteRuntimeStore:
		return owner.activitySQLiteOwner.ObserveChannelAuthorActivityForTest(ctx, opts)
	}
	return authoractivity.ListResult{}, nil
}

func ObserveChannelIntentForTest(ctx context.Context, selected any, demand render.IntentObservationQuery) (render.IntentObservation, bool, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return render.IntentObservation{}, false, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return channeldelivery.ObserveChannelIntentForTest(ctx, owner.backend, demand, true)
	case *SQLiteRuntimeStore:
		return channeldelivery.ObserveChannelIntentForTest(ctx, owner.backend, demand, false)
	}
	return render.IntentObservation{}, false, nil
}

func ObserveChannelDeliveryPlansForTest(ctx context.Context, selected any, cursor string, limit int) ([]render.Candidate, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var plans []channeldelivery.Plan
	var err error
	switch owner := selected.(type) {
	case *PostgresStore:
		plans, err = channeldelivery.ObserveChannelDeliveryPlansForTest(ctx, owner.backend, cursor, limit, true)
	case *SQLiteRuntimeStore:
		plans, err = channeldelivery.ObserveChannelDeliveryPlansForTest(ctx, owner.backend, cursor, limit, false)
	}
	return projectDeliveryCandidates(plans), err
}

func ObserveChannelSentReceiptForTest(ctx context.Context, selected any, deliveryID, operationID string) (render.SentReceipt, bool, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return render.SentReceipt{}, false, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return channeldelivery.ObserveChannelSentReceiptForTest(ctx, owner.backend, deliveryID, operationID, true)
	case *SQLiteRuntimeStore:
		return channeldelivery.ObserveChannelSentReceiptForTest(ctx, owner.backend, deliveryID, operationID, false)
	}
	return render.SentReceipt{}, false, nil
}
