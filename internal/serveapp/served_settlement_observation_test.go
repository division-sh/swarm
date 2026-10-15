package serveapp

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/store/storetest"
)

func readServedExactRunNodeDeliveryCount(ctx context.Context, reader servedEventCountReader, runID, eventID, nodeID string) (int, error) {
	if reader == nil {
		return 0, fmt.Errorf("node delivery count requires the original selected reader")
	}
	var count int
	err := reader.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		count, err = storetest.CountExactRunNodeEventDeliveryStorage(ctx, reader, runID, eventID, nodeID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func readServedCurrentDeliveryOutcomeCount(ctx context.Context, reader servedEventCountReader, eventID, subscriberType, subscriberID, outcome string) (int, error) {
	if reader == nil {
		return 0, fmt.Errorf("current delivery outcome count requires the original selected reader")
	}
	var count int
	err := reader.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		count, err = storetest.CountCurrentEventDeliveryOutcomeStorage(ctx, reader, eventID, subscriberType, subscriberID, outcome)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func readServedSubscriberReceiptOutcomeCount(ctx context.Context, reader servedEventCountReader, eventID, subscriberType, subscriberID, outcome string) (int, error) {
	if reader == nil {
		return 0, fmt.Errorf("subscriber receipt outcome count requires the original selected reader")
	}
	var count int
	err := reader.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		count, err = storetest.CountSubscriberEventReceiptOutcomeStorage(ctx, reader, eventID, subscriberType, subscriberID, outcome)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func readServedExactReplayScopeCount(ctx context.Context, reader servedEventCountReader, runID, eventID, scope string) (int, error) {
	if reader == nil {
		return 0, fmt.Errorf("replay scope count requires the original selected reader")
	}
	var count int
	err := reader.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		count, err = storetest.CountExactRunEventReplayScopeStorage(ctx, reader, runID, eventID, scope)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func readServedRunNamedPublicationIdentity(ctx context.Context, reader servedEventCountReader, runID, eventName string) (string, error) {
	if reader == nil {
		return "", fmt.Errorf("named publication lookup requires the original selected reader")
	}
	var id string
	err := reader.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		id, err = storetest.ReadWorkflowTimerEventIdentity(ctx, reader, runID, eventName)
		return err
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

func readServedPublicationPayloadText(ctx context.Context, reader servedEventCountReader, eventID string) (string, error) {
	if reader == nil {
		return "", fmt.Errorf("publication payload lookup requires the original selected reader")
	}
	var raw string
	err := reader.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		raw, err = storetest.ReadTemplateEventPayloadText(ctx, reader, eventID)
		return err
	})
	if err != nil {
		return "", err
	}
	return raw, nil
}

func readServedPublicationEntityIdentity(ctx context.Context, reader servedEventCountReader, eventID string) (string, error) {
	if reader == nil {
		return "", fmt.Errorf("publication entity lookup requires the original selected reader")
	}
	var id string
	err := reader.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		id, err = storetest.ReadPhysicalPublicationEntityIdentityStorage(ctx, reader, eventID)
		return err
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

func readServedSubscriberReceiptCount(ctx context.Context, reader servedEventCountReader, eventID string, subscribers []string) (int, error) {
	if reader == nil {
		return 0, fmt.Errorf("subscriber receipt count requires the original selected reader")
	}
	var count int
	err := reader.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		count, err = storetest.CountEventSubscriberReceiptStorage(ctx, reader, eventID, subscribers)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}
