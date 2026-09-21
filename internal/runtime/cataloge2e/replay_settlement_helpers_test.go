package cataloge2e

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
)

// A durable publication is not proof that its receiver finished. Failed
// terminal deliveries must fail a success barrier rather than satisfy it.
func catalogEventSuccessfullySettled(full operatorread.OperatorEventFull) (bool, error) {
	if len(full.DeadLetters) != 0 {
		return false, fmt.Errorf("event %s (%s) has dead letters", full.EventID, full.EventName)
	}
	complete := len(full.Deliveries) != 0
	for _, delivery := range full.Deliveries {
		if delivery.Failure != nil || len(delivery.DeadLetters) != 0 || delivery.Status == "dead_letter" || (delivery.Terminal && delivery.Status != "delivered") {
			return false, fmt.Errorf("event %s (%s) delivery %s to %s/%s failed: status=%s reason=%s failure=%+v", full.EventID, full.EventName, delivery.DeliveryID, delivery.SubscriberType, delivery.SubscriberID, delivery.Status, delivery.ReasonCode, delivery.Failure)
		}
		if delivery.Status != "delivered" || !delivery.Terminal || delivery.RetryScheduled || delivery.FinishedAt == nil || delivery.FinishedAt.IsZero() {
			complete = false
		}
	}
	return complete, nil
}

func countCatalogSettledAutomaticEvents(ctx context.Context, lister catalogOperatorEventLister, eventName string, minimum int, authoredIDs map[string]struct{}) (int, error) {
	count := 0
	opts := operatorread.OperatorEventListOptions{
		Filter: operatorread.OperatorEventListFilter{RunID: catalogRuntimeRunID, EventName: eventName},
		Limit:  minimum,
	}
	seen := map[string]bool{}
	for {
		page, err := lister.ListOperatorEvents(ctx, opts)
		if err != nil {
			return 0, err
		}
		for _, full := range page.Events {
			eventID := strings.TrimSpace(full.EventID)
			if seen[eventID] {
				return 0, fmt.Errorf("automatic event barrier repeated event %s", eventID)
			}
			seen[eventID] = true
			if _, authored := authoredIDs[eventID]; authored {
				continue
			}
			event, err := full.EventSnapshot()
			if err != nil {
				return 0, err
			}
			if event.AdmissionClass() == events.EventAdmissionRootIngress {
				continue
			}
			settled, err := catalogEventSuccessfullySettled(full)
			if err != nil {
				return 0, err
			}
			if settled {
				count++
			}
		}
		if count >= minimum || strings.TrimSpace(page.NextCursor) == "" {
			return count, nil
		}
		if page.NextCursor == opts.Cursor {
			return 0, fmt.Errorf("automatic event barrier repeated cursor %s", page.NextCursor)
		}
		opts.Cursor = page.NextCursor
	}
}

// Replay equivalence alone can accept three identically failed executions.
// These fixture-specific success obligations are checked before comparison.
func validateCatalogSuccessfulDeliveries(full map[string]operatorread.OperatorEventFull, required map[string]int) error {
	counts := map[string]int{}
	for _, event := range full {
		settled, err := catalogEventSuccessfullySettled(event)
		if err != nil {
			return err
		}
		if !settled {
			return fmt.Errorf("event %s (%s) lacks successfully settled deliveries", event.EventID, event.EventName)
		}
		counts[event.EventName]++
	}
	for eventName, minimum := range required {
		if counts[eventName] < minimum {
			return fmt.Errorf("event %s has %d successfully settled occurrences, want at least %d", eventName, counts[eventName], minimum)
		}
	}
	return nil
}

func validateCatalogCreationDeliveries(full map[string]operatorread.OperatorEventFull, required map[string]int, conflictEventID string) error {
	if conflictEventID == "" {
		return validateCatalogSuccessfulDeliveries(full, required)
	}
	conflict, found := full[conflictEventID]
	if !found || conflict.EventName != "flow.spawn_requested" || len(conflict.Deliveries) != 1 || conflict.Deliveries[0].Status != "dead_letter" || !conflict.Deliveries[0].Terminal {
		return fmt.Errorf("exact conflicting second root %s did not settle as a dead letter", conflictEventID)
	}
	event, err := conflict.EventSnapshot()
	if err != nil || event.AdmissionClass() != events.EventAdmissionRootIngress {
		return fmt.Errorf("conflict exception is not an admitted root: %s: %v", conflictEventID, err)
	}
	// Receipt assertions independently require the declared conflicting-duplicate
	// class/detail. Only that exact root may fail, never the creation's children.
	successful := make(map[string]operatorread.OperatorEventFull, len(full)-1)
	for id, event := range full {
		if id != conflictEventID {
			successful[id] = event
		}
	}
	return validateCatalogSuccessfulDeliveries(successful, required)
}
