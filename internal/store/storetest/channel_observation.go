package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ChannelObservation interface {
	render.Observer
	ListAuthorActivity(context.Context, authoractivity.ListOptions) (authoractivity.ListResult, error)
	NativeConsumerParent(context.Context, string, bool) (channelnative.TransportParent, error)
	NativeHistory(context.Context) ([8]string, error)
	PreservedHistory(context.Context, []string) (string, error)
	FrozenCardHistory(context.Context, string) (string, error)
	HistoricalReceipt(context.Context, string) (string, error)
	GenerationZeroSentPlan(context.Context, string) (string, error)
	ControlCounts(context.Context, string) (ChannelControlCounts, error)
	CountSourceCards(context.Context, string, string) (int, error)
	CountEntryIntents(context.Context) (int, error)
	RejectedEntryCount(context.Context, string) (int, error)
	ProviderActionDisposition(context.Context, string) (int, string, string, error)
	RegistrationIndex(context.Context) (string, error)
	RegistrationCounts(context.Context) (int, int, int, error)
	DeliveryDefaultState(context.Context) (string, error)
	CountUnsettledResponsibilities(context.Context) (int, error)
	EventCardinality(context.Context, string, string) (int, error)
	Close() error
}

type ChannelControlCounts = private.ChannelControlCounts

func (o channelObservation) NativeConsumerParent(ctx context.Context, operation string, currentOnly bool) (channelnative.TransportParent, error) {
	return private.ReadChannelNativeConsumerForTest(ctx, o.selected, operation, currentOnly)
}
func (o channelObservation) NativeHistory(ctx context.Context) ([8]string, error) {
	return private.ReadChannelNativeHistoryForTest(ctx, o.selected)
}
func (o channelObservation) PreservedHistory(ctx context.Context, successors []string) (string, error) {
	return private.ReadChannelPreservedHistoryForTest(ctx, o.selected, successors)
}
func (o channelObservation) FrozenCardHistory(ctx context.Context, card string) (string, error) {
	return private.ReadChannelFrozenCardHistoryForTest(ctx, o.selected, card)
}
func (o channelObservation) HistoricalReceipt(ctx context.Context, card string) (string, error) {
	return private.ReadChannelHistoricalReceiptForTest(ctx, o.selected, card)
}
func (o channelObservation) GenerationZeroSentPlan(ctx context.Context, card string) (string, error) {
	return private.ReadChannelGenerationZeroSentPlanForTest(ctx, o.selected, card)
}
func (o channelObservation) ControlCounts(ctx context.Context, token string) (ChannelControlCounts, error) {
	return private.ReadChannelControlCountsForTest(ctx, o.selected, token)
}
func (o channelObservation) CountSourceCards(ctx context.Context, run, source string) (int, error) {
	return private.CountChannelSourceCardsForTest(ctx, o.selected, run, source)
}
func (o channelObservation) CountEntryIntents(ctx context.Context) (int, error) {
	return private.CountChannelEntryIntentsForTest(ctx, o.selected)
}
func (o channelObservation) RejectedEntryCount(ctx context.Context, event string) (int, error) {
	return private.CountChannelRejectedEntriesForTest(ctx, o.selected, event)
}
func (o channelObservation) ProviderActionDisposition(ctx context.Context, event string) (int, string, string, error) {
	return private.ReadChannelProviderActionForTest(ctx, o.selected, event)
}
func (o channelObservation) RegistrationIndex(ctx context.Context) (string, error) {
	return private.ReadChannelRegistrationIndexForTest(ctx, o.selected)
}
func (o channelObservation) RegistrationCounts(ctx context.Context) (int, int, int, error) {
	return private.ReadChannelRegistrationCountsForTest(ctx, o.selected)
}
func (o channelObservation) DeliveryDefaultState(ctx context.Context) (string, error) {
	return private.ReadChannelDeliveryDefaultStateForTest(ctx, o.selected)
}
func (o channelObservation) CountUnsettledResponsibilities(ctx context.Context) (int, error) {
	return private.CountUnsettledChannelResponsibilitiesForTest(ctx, o.selected)
}
func (o channelObservation) EventCardinality(ctx context.Context, run, event string) (int, error) {
	return private.CountChannelObservedEventsForTest(ctx, o.selected, run, event)
}

func ReadServedChannelConnection(ctx context.Context, selected ReleaseProcessReadOnlyInspection, operationID string) (string, error) {
	var connection string
	err := selected.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		connection, err = private.ReadServedChannelConnectionForTest(ctx, selected, operationID)
		return err
	})
	return connection, err
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

func (o channelObservation) ListAuthorActivity(ctx context.Context, opts authoractivity.ListOptions) (authoractivity.ListResult, error) {
	var result authoractivity.ListResult
	err := o.selected.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		result, err = private.ObserveChannelAuthorActivityForTest(ctx, o.selected, opts)
		return err
	})
	return result, err
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
