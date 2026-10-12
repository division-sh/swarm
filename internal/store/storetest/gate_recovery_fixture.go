package storetest

import (
	"context"
	"time"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func CountGateRunProposedEffectContinuations(ctx context.Context, selected any, run string) (int, error) {
	return private.CountGateRunProposedEffectContinuationsForTest(ctx, selected, run)
}

func ReadGateNodeDeliveryStatus(ctx context.Context, selected any, event, node string) (string, error) {
	return private.ReadGateNodeDeliveryStatusForTest(ctx, selected, event, node)
}

func CountGateEventSubscriberDeliveries(ctx context.Context, selected any, event, subscriber string) (int, error) {
	return private.CountGateEventSubscriberDeliveriesForTest(ctx, selected, event, subscriber)
}

func ReadGateLatestDeadLetterFailure(ctx context.Context, selected any, event string) (string, error) {
	return private.ReadGateLatestDeadLetterFailureForTest(ctx, selected, event)
}

func CountGateSourceNamedEvents(ctx context.Context, selected any, name, source string) (int, error) {
	return private.CountGateSourceNamedEventsForTest(ctx, selected, name, source)
}

func ReadGateRunEventNames(ctx context.Context, selected any, run string) ([]string, error) {
	return private.ReadGateRunEventNamesForTest(ctx, selected, run)
}

func InstallGateProposedEffectCreationFailure(ctx context.Context, selected any) error {
	return private.InstallGateProposedEffectCreationFailureForTest(ctx, selected)
}

func SetGateDecisionRouteAttemptFault(ctx context.Context, selected any, event string, attempt int, due time.Time) error {
	return private.SetGateDecisionRouteAttemptFaultForTest(ctx, selected, event, attempt, due)
}

func SetGateDecisionRouteDueFault(ctx context.Context, selected any, event string, due time.Time) error {
	return private.SetGateDecisionRouteDueFaultForTest(ctx, selected, event, due)
}
