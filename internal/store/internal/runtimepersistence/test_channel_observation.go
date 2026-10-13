package runtimepersistence

import (
	"context"
	"database/sql"

	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	decisionstorage "github.com/division-sh/swarm/internal/store/internal/backend/decisioncard"
	"github.com/division-sh/swarm/internal/store/internal/backend/standingdisposition"
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

type ChannelAnchorCardStorage = decisionstorage.ChannelAnchorCardStorage
type ChannelAnchorDecisionStorage = decisionstorage.ChannelAnchorDecisionStorage
type StandingFlowStorage = standingdisposition.StandingFlowStorage

func ReadStandingFlowStorageForTest(ctx context.Context, selected any, flowID string) (StandingFlowStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return StandingFlowStorage{}, err
	}
	_, postgres := selected.(*PostgresStore)
	var result StandingFlowStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = standingdisposition.ReadStandingFlowStorageTx(ctx, tx, postgres, flowID)
		return err
	})
	if err != nil {
		return StandingFlowStorage{}, err
	}
	return result, nil
}

func ReadChannelAnchorCardsStorageForTest(ctx context.Context, selected any, runID, kind, status string) ([]ChannelAnchorCardStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var result []ChannelAnchorCardStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = decisionstorage.ReadChannelAnchorCardsStorageTx(ctx, tx, runID, kind, status)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func ReadChannelAnchorDecisionStorageForTest(ctx context.Context, selected any, cardID string) (ChannelAnchorDecisionStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return ChannelAnchorDecisionStorage{}, err
	}
	var result ChannelAnchorDecisionStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = decisionstorage.ReadChannelAnchorDecisionStorageTx(ctx, tx, cardID)
		return err
	})
	if err != nil {
		return ChannelAnchorDecisionStorage{}, err
	}
	return result, nil
}

func ReadFirstSentSourceReceiptStorageForTest(ctx context.Context, selected any, sourceID string) (string, bool, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return "", false, err
	}
	var result string
	var found bool
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = channeldelivery.ReadFirstSentSourceReceiptStorageTx(ctx, tx, sourceID)
		if err == sql.ErrNoRows {
			return nil
		}
		found = err == nil
		return err
	})
	if err != nil {
		return "", false, err
	}
	return result, found, nil
}
