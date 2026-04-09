package bus

import (
	"context"
	"fmt"
	"strings"

	"swarm/internal/events"
	runtimereplayclaim "swarm/internal/runtime/replayclaim"
)

func (eb *EventBus) authoritativeReplayScopeForEvent(ctx context.Context, eventID string) (runtimereplayclaim.CommittedReplayScope, error) {
	reader, ok := eb.store.(runtimereplayclaim.ScopeReader)
	if !ok || reader == nil {
		return "", runtimereplayclaim.ErrMissingCommittedReplayScope
	}
	scope, err := reader.LoadCommittedReplayScope(ctx, eventID)
	if err != nil {
		return "", err
	}
	switch scope {
	case runtimereplayclaim.CommittedReplayScopeDirect, runtimereplayclaim.CommittedReplayScopeSubscribed:
		return scope, nil
	default:
		return "", fmt.Errorf("authoritative replay scope: unsupported scope %q", strings.TrimSpace(string(scope)))
	}
}

func (eb *EventBus) currentInternalRecipientsForCommittedEvent(ctx context.Context, evt events.Event) ([]string, error) {
	plan, err := eb.deliveryPlanner.Plan(ctx, evt)
	if err != nil {
		return nil, err
	}
	return filterOutAgentIDs(plan.Recipients, plan.PersistedRecipients), nil
}

func (eb *EventBus) replayRecipientsForCommittedEvent(
	ctx context.Context,
	evt events.Event,
	persisted []string,
	scope runtimereplayclaim.CommittedReplayScope,
) ([]string, []string, error) {
	persisted = uniqueStrings(persisted)
	switch scope {
	case runtimereplayclaim.CommittedReplayScopeDirect:
		return persisted, nil, nil
	case runtimereplayclaim.CommittedReplayScopeSubscribed:
		internal, err := eb.currentInternalRecipientsForCommittedEvent(ctx, evt)
		if err != nil {
			return nil, nil, err
		}
		live := uniqueStrings(append(append([]string(nil), persisted...), internal...))
		return live, internal, nil
	default:
		return nil, nil, fmt.Errorf("replay recipients: unsupported scope %q", strings.TrimSpace(string(scope)))
	}
}

func replayScopePersistenceRequired(store any) bool {
	_, hasLister := store.(runtimereplayclaim.Lister)
	_, hasOwner := store.(runtimereplayclaim.Owner)
	return hasLister && hasOwner
}
