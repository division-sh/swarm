package storetest

import (
	"context"

	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ChannelObservation interface {
	render.Observer
	Close() error
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

func (o channelObservation) ObserveChannelIntent(ctx context.Context, demand render.IntentObservationQuery) (render.IntentObservation, bool, error) {
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
