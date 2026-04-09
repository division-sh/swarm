package bus

import (
	"context"
	"strings"

	"swarm/internal/events"
	runtimereplayclaim "swarm/internal/runtime/replayclaim"
)

func (eb *EventBus) authoritativeRecipientsForEvent(ctx context.Context, eventID string) ([]string, error) {
	if !runtimereplayclaim.SupportsPersistedReplay(eb.store) {
		return nil, runtimereplayclaim.ErrAuthoritativeRecipientManifestUnavailable
	}
	recipients, err := eb.store.ListEventDeliveryRecipients(ctx, eventID)
	if err != nil {
		return nil, err
	}
	for i := range recipients {
		recipients[i] = strings.TrimSpace(recipients[i])
	}
	if recipients == nil {
		return []string{}, nil
	}
	return uniqueStrings(recipients), nil
}

func (eb *EventBus) currentInternalRecipientsForCommittedEvent(ctx context.Context, evt events.Event) ([]string, error) {
	plan, err := eb.deliveryPlanner.Plan(ctx, evt)
	if err != nil {
		return nil, err
	}
	return filterOutAgentIDs(plan.Recipients, plan.PersistedRecipients), nil
}

func (eb *EventBus) committedLiveRecipients(ctx context.Context, evt events.Event, persisted []string, includeInternal bool) ([]string, []string, error) {
	persisted = uniqueStrings(persisted)
	if !includeInternal {
		return persisted, nil, nil
	}
	internal, err := eb.currentInternalRecipientsForCommittedEvent(ctx, evt)
	if err != nil {
		return nil, nil, err
	}
	live := uniqueStrings(append(append([]string(nil), persisted...), internal...))
	return live, internal, nil
}
