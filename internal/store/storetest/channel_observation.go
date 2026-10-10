package storetest

import (
	"context"
	"database/sql"

	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ChannelObservation interface {
	render.Observer
	ChannelAnchorObservation
	ChannelSettlementObservation
	Close() error
}

type ChannelAnchorCardStorage = private.ChannelAnchorCardStorage
type ChannelAnchorDecisionStorage = private.ChannelAnchorDecisionStorage
type StandingFlowStorage = private.StandingFlowStorage

type ChannelAnchorObservation interface {
	CountEventNameStorage(context.Context, string) (int, error)
	CountRunNamedEventStorage(context.Context, string, string) (int, error)
	ReadChannelAnchorCardsStorage(context.Context, string, string, string) ([]ChannelAnchorCardStorage, error)
	ReadChannelAnchorDecisionStorage(context.Context, string) (ChannelAnchorDecisionStorage, error)
	ReadFirstSentSourceReceiptStorage(context.Context, string) (string, error)
	ReadStandingFlowStorage(context.Context, string) (StandingFlowStorage, error)
}

type channelObservation struct {
	selected ReleaseProcessReadOnlyInspection
}

func OpenChannelObservation(backend, location string) (ChannelObservation, error) {
	selected, err := OpenReleaseProcessReadOnlyInspection(backend, location)
	if err != nil {
		return nil, err
	}
	return channelObservation{selected: selected}, nil
}

func (o channelObservation) Close() error { return o.selected.Close() }

func (o channelObservation) CountEventNameStorage(ctx context.Context, eventName string) (int, error) {
	var count int
	err := o.selected.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		count, err = private.CountEventNameStorageForTest(ctx, o.selected, eventName)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (o channelObservation) CountRunNamedEventStorage(ctx context.Context, runID, eventName string) (int, error) {
	var count int
	err := o.selected.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		count, err = private.CountRunNamedEventStorageForTest(ctx, o.selected, runID, eventName)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (o channelObservation) ObserveChannelIntent(ctx context.Context, demand render.IntentObservationQuery) (render.IntentObservation, bool, error) {
	if err := demand.Validate(); err != nil {
		return render.IntentObservation{}, false, err
	}
	var result render.IntentObservation
	var found bool
	err := o.selected.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		result, found, err = private.ObserveChannelIntentForTest(ctx, o.selected, demand)
		return err
	})
	return result, found, err
}

func (o channelObservation) ListCurrentChannelDeliveryPlans(ctx context.Context, cursor string, limit int) ([]render.Candidate, error) {
	var result []render.Candidate
	err := o.selected.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		result, err = private.ObserveChannelDeliveryPlansForTest(ctx, o.selected, cursor, limit)
		return err
	})
	return result, err
}

func (o channelObservation) GetCurrentChannelSentReceipt(ctx context.Context, deliveryID, operationID string) (render.SentReceipt, bool, error) {
	var result render.SentReceipt
	var found bool
	err := o.selected.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		result, found, err = private.ObserveChannelSentReceiptForTest(ctx, o.selected, deliveryID, operationID)
		return err
	})
	return result, found, err
}

func (o channelObservation) ReadChannelAnchorCardsStorage(ctx context.Context, runID, kind, status string) ([]ChannelAnchorCardStorage, error) {
	var result []ChannelAnchorCardStorage
	err := o.selected.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		result, err = private.ReadChannelAnchorCardsStorageForTest(ctx, o.selected, runID, kind, status)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (o channelObservation) ReadChannelAnchorDecisionStorage(ctx context.Context, cardID string) (ChannelAnchorDecisionStorage, error) {
	var result ChannelAnchorDecisionStorage
	err := o.selected.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		result, err = private.ReadChannelAnchorDecisionStorageForTest(ctx, o.selected, cardID)
		return err
	})
	if err != nil {
		return ChannelAnchorDecisionStorage{}, err
	}
	return result, nil
}

func (o channelObservation) ReadFirstSentSourceReceiptStorage(ctx context.Context, sourceID string) (string, error) {
	var result string
	var found bool
	err := o.selected.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		result, found, err = private.ReadFirstSentSourceReceiptStorageForTest(ctx, o.selected, sourceID)
		return err
	})
	if err != nil {
		return "", err
	}
	if !found {
		return "", sql.ErrNoRows
	}
	return result, nil
}

func (o channelObservation) ReadStandingFlowStorage(ctx context.Context, flowID string) (StandingFlowStorage, error) {
	var result StandingFlowStorage
	err := o.selected.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		result, err = private.ReadStandingFlowStorageForTest(ctx, o.selected, flowID)
		return err
	})
	if err != nil {
		return StandingFlowStorage{}, err
	}
	return result, nil
}
