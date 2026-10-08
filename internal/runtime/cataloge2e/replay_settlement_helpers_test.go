package cataloge2e

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
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

func validateCatalogCreationDeliveries(full map[string]operatorread.OperatorEventFull, required map[string]int, conflict catalogTriggerStep, receipt *catalogReceiptOutcome) error {
	if conflict.eventID == "" {
		return validateCatalogSuccessfulDeliveries(full, required)
	}
	refused, found := full[conflict.eventID]
	if !found {
		return fmt.Errorf("exact conflicting second root %s is missing", conflict.eventID)
	}
	if err := validateCatalogCreateConflict(refused, conflict); err != nil {
		return err
	}
	if err := validateCatalogCreateConflictReceipt(receipt, conflict); err != nil {
		return err
	}
	// Only the exact pre-commit conflicting cause may fail. No business output
	// from that cause exists, and the first creation and every sibling must settle.
	successful := make(map[string]operatorread.OperatorEventFull, len(full)-1)
	for id, event := range full {
		if event.SourceEventID == conflict.eventID {
			return fmt.Errorf("rejected creation %s produced child %s (%s)", conflict.eventID, id, event.EventName)
		}
		if id != conflict.eventID {
			successful[id] = event
		}
	}
	return validateCatalogSuccessfulDeliveries(successful, required)
}

func validateCatalogCreateConflict(refused operatorread.OperatorEventFull, want catalogTriggerStep) error {
	if want.Event != "flow.spawn_requested" || want.ReceiptOutcome != "dead_letter" || want.ReceiptFailureClass != "platform.conflicting_duplicate" || want.ReceiptFailureDetail != "flow_instance_already_exists" {
		return fmt.Errorf("creation refusal %s lacks an exact atomic-conflict expectation", want.eventID)
	}
	if refused.EventID != want.eventID || refused.EventName != want.Event || len(refused.Deliveries) != 1 || len(refused.DeadLetters) != 1 || refused.NoDelivery != nil {
		return fmt.Errorf("exact conflicting second root %s lost its delivery evidence", want.eventID)
	}
	event, err := refused.EventSnapshot()
	if err != nil || event.AdmissionClass() != events.EventAdmissionRootIngress {
		return fmt.Errorf("conflict exception is not an admitted root: %s: %v", want.eventID, err)
	}
	delivery := refused.Deliveries[0]
	root := catalogRootWorkflowRoute()
	if refused.RunID != root.RunID || delivery.Target.FlowID != root.Route.ScopeKey || delivery.Target.FlowInstance != root.Route.InstancePath || delivery.Target.EntityID != flowidentity.EntityID(root.Route.InstancePath) {
		return fmt.Errorf("creation refusal %s lost its exact causing-handler target: %+v", want.eventID, delivery.Target)
	}
	if delivery.Status != "dead_letter" || !delivery.Terminal || delivery.RetryScheduled || delivery.FinishedAt == nil || delivery.FinishedAt.IsZero() || len(delivery.DeadLetters) != 1 {
		return fmt.Errorf("exact conflicting second root %s did not finish as a dead letter", want.eventID)
	}
	for _, failure := range []*runtimefailures.Envelope{delivery.Failure, &delivery.DeadLetters[0].Failure, &refused.DeadLetters[0].Failure} {
		if failure == nil || string(failure.Class) != want.ReceiptFailureClass || failure.Detail.Code != want.ReceiptFailureDetail {
			return fmt.Errorf("creation refusal %s failure = %+v, want %s/%s", want.eventID, failure, want.ReceiptFailureClass, want.ReceiptFailureDetail)
		}
	}
	return nil
}

func validateCatalogCreateConflictReceipt(receipt *catalogReceiptOutcome, want catalogTriggerStep) error {
	if receipt == nil || receipt.Outcome != want.ReceiptOutcome || receipt.Failure == nil || string(receipt.Failure.Class) != want.ReceiptFailureClass || receipt.Failure.Detail.Code != want.ReceiptFailureDetail {
		return fmt.Errorf("creation refusal %s lost its exact causing-handler receipt: %+v", want.eventID, receipt)
	}
	for key, expected := range want.ReceiptFailureAttributes {
		if !reflect.DeepEqual(receipt.Failure.Detail.Attributes[key], expected) {
			return fmt.Errorf("creation refusal %s receipt failure attribute %s = %#v, want %#v", want.eventID, key, receipt.Failure.Detail.Attributes[key], expected)
		}
	}
	return nil
}
