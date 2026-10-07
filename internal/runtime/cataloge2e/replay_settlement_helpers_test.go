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

func validateCatalogRefusedPublication(full map[string]operatorread.OperatorEventFull, causeID string, want catalogRefusedPublication) (string, error) {
	cause, found := full[causeID]
	if !found {
		return "", fmt.Errorf("refused publication cause %s is missing", causeID)
	}
	settled, err := catalogEventSuccessfullySettled(cause)
	if err != nil || !settled {
		return "", fmt.Errorf("refused publication cause %s is not successfully settled: %v", causeID, err)
	}
	var matches []operatorread.OperatorEventFull
	for _, child := range full {
		if child.SourceEventID == causeID && child.EventName == want.Event {
			matches = append(matches, child)
		}
	}
	if len(matches) != 1 {
		return "", fmt.Errorf("cause %s has %d %s publications, want exactly one", causeID, len(matches), want.Event)
	}
	child := matches[0]
	event, err := child.EventSnapshot()
	if err != nil || event.AdmissionClass() == events.EventAdmissionRootIngress {
		return "", fmt.Errorf("refusal %s is not an admitted child publication: %v", child.EventID, err)
	}
	if len(child.Deliveries) != 0 || child.NoDelivery == nil || child.NoDelivery.Reason != want.Reason || len(child.DeadLetters) != 1 {
		return "", fmt.Errorf("refusal %s changed its exact no-delivery/dead-letter evidence: %+v", child.EventID, child)
	}
	failure := child.DeadLetters[0]
	if string(failure.Failure.Class) != want.FailureClass || failure.Failure.Detail.Code != want.FailureDetail || failure.HandlerNode != "pin_routing" {
		return "", fmt.Errorf("refusal %s failure = %+v, want %s/%s from pin_routing", child.EventID, failure, want.FailureClass, want.FailureDetail)
	}
	return child.EventID, nil
}

func validateCatalogCreationDeliveries(full map[string]operatorread.OperatorEventFull, required map[string]int, conflictEventID string, refusal *catalogRefusedPublication) error {
	if conflictEventID == "" {
		return validateCatalogSuccessfulDeliveries(full, required)
	}
	if refusal == nil {
		return fmt.Errorf("creation refusal %s lacks an exact expectation", conflictEventID)
	}
	refusedID, err := validateCatalogRefusedPublication(full, conflictEventID, *refusal)
	if err != nil {
		return err
	}
	// Only this cause-bound child may refuse. Its parent and every sibling must
	// still prove successful settlement, including both authored parent triggers.
	successful := make(map[string]operatorread.OperatorEventFull, len(full)-1)
	for id, event := range full {
		if id != refusedID {
			successful[id] = event
		}
	}
	return validateCatalogSuccessfulDeliveries(successful, required)
}
