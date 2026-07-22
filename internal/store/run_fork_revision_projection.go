package store

import (
	"sort"
	"strings"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func loadRunForkPendingWorkFromRevision(snapshot *runForkRevisionSnapshot) ([]RunForkPendingWork, error) {
	if snapshot == nil {
		return nil, nil
	}
	events := make(map[string]runForkRevisionEvent, len(snapshot.Events))
	for _, event := range snapshot.Events {
		events[strings.TrimSpace(event.EventID)] = event
	}
	deadLetters := make(map[string]struct{}, len(snapshot.DeadLetters))
	for _, deadLetter := range snapshot.DeadLetters {
		if deliveryID := strings.TrimSpace(deadLetter.DeliveryID); deliveryID != "" {
			deadLetters[deliveryID] = struct{}{}
		}
	}
	deliveryKeys := make(map[string]struct{}, len(snapshot.Deliveries))
	out := make([]RunForkPendingWork, 0, len(snapshot.Deliveries)+len(snapshot.Receipts))
	for _, delivery := range snapshot.Deliveries {
		durable := delivery.Snapshot
		event, ok := events[strings.TrimSpace(durable.EventID)]
		if !ok {
			return nil, runForkRevisionLineageError("event_deliveries", durable.DeliveryID, durable.EventID)
		}
		key := runForkRevisionSubscriberKey(durable.EventID, string(durable.SubscriberClass), durable.SubscriberID)
		deliveryKeys[key] = struct{}{}
		item := RunForkPendingWork{
			EventID:         strings.TrimSpace(durable.EventID),
			EventName:       strings.TrimSpace(event.EventName),
			FlowInstance:    strings.TrimSpace(event.FlowInstance),
			SourceRoute:     event.SourceRoute.Normalized(),
			DeliveryID:      strings.TrimSpace(durable.DeliveryID),
			SubscriberType:  string(durable.SubscriberClass),
			SubscriberID:    strings.TrimSpace(durable.SubscriberID),
			Status:          string(durable.Status),
			RetryCount:      durable.RetryCount,
			ReasonCode:      strings.TrimSpace(durable.ReasonCode),
			ActiveSessionID: strings.TrimSpace(durable.ActiveSessionID),
			CreatedAt:       durable.CreatedAt,
			StartedAt:       traceTimePtr(durable.StartedAt),
			DeliveredAt:     traceTimePtr(durable.SettledAt),
		}
		_, deadLetter := deadLetters[item.DeliveryID]
		item.Classification = classifyRunForkDeliverySnapshot(durable, deadLetter)
		out = append(out, item)
	}
	for _, receipt := range snapshot.Receipts {
		key := runForkRevisionSubscriberKey(receipt.EventID, receipt.SubscriberType, receipt.SubscriberID)
		if _, ok := deliveryKeys[key]; ok {
			continue
		}
		if receipt.SubscriberType != "platform" {
			continue
		}
		event, ok := events[strings.TrimSpace(receipt.EventID)]
		if !ok {
			return nil, runForkRevisionLineageError("event_receipts", receipt.ReceiptID, receipt.EventID)
		}
		receiptAt := receipt.ProcessedAt
		item := RunForkPendingWork{
			EventID:        strings.TrimSpace(receipt.EventID),
			EventName:      strings.TrimSpace(event.EventName),
			FlowInstance:   strings.TrimSpace(event.FlowInstance),
			SourceRoute:    event.SourceRoute.Normalized(),
			SubscriberType: strings.TrimSpace(receipt.SubscriberType),
			SubscriberID:   strings.TrimSpace(receipt.SubscriberID),
			ReasonCode:     strings.TrimSpace(receipt.ReasonCode),
			CreatedAt:      receipt.ProcessedAt,
			DeliveredAt:    &receiptAt,
			ReceiptOutcome: strings.TrimSpace(receipt.Outcome),
			ReceiptAt:      &receiptAt,
		}
		if item.ReceiptOutcome == "dead_letter" {
			item.Classification = RunForkPendingClassificationDeadLetter
		} else {
			item.Classification = RunForkPendingClassificationDeliveredCompleted
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		left := runForkRevisionSubscriberKey(out[i].EventID, out[i].SubscriberType, out[i].SubscriberID) + "/" + out[i].DeliveryID
		right := runForkRevisionSubscriberKey(out[j].EventID, out[j].SubscriberType, out[j].SubscriberID) + "/" + out[j].DeliveryID
		return left < right
	})
	return out, nil
}

func classifyRunForkDeliverySnapshot(snapshot runtimedelivery.Snapshot, deadLetter bool) string {
	if deadLetter || snapshot.Status == runtimedelivery.StatusDeadLetter {
		return RunForkPendingClassificationDeadLetter
	}
	switch snapshot.Status {
	case runtimedelivery.StatusPending:
		return RunForkPendingClassificationPending
	case runtimedelivery.StatusInProgress:
		return RunForkPendingClassificationInProgress
	case runtimedelivery.StatusFailed:
		return RunForkPendingClassificationFailedRetryable
	case runtimedelivery.StatusDelivered:
		return RunForkPendingClassificationDeliveredCompleted
	default:
		return ""
	}
}

func loadRunForkAdmissionEvidenceFromRevision(snapshot *runForkRevisionSnapshot, entities []RunForkEntityState, pending []RunForkPendingWork) (runForkAdmissionEvidence, error) {
	facts := loadRunForkSourceFactsFromRevision(snapshot, entities)
	relevantTimer := false
	entityIDs := stringSliceSet(facts.EntityIDs)
	flowInstances := stringSliceSet(facts.FlowInstances)
	for _, timer := range snapshot.Timers {
		if _, ok := entityIDs[strings.TrimSpace(timer.EntityID)]; ok && strings.TrimSpace(timer.EntityID) != "" {
			relevantTimer = true
			break
		}
		if _, ok := flowInstances[strings.TrimSpace(timer.FlowInstance)]; ok && strings.TrimSpace(timer.FlowInstance) != "" {
			relevantTimer = true
			break
		}
	}
	routeState := RunForkRouteHistoryNotApplicable
	if len(facts.FlowInstances) > 0 || len(facts.SourceFlows) > 0 {
		routeState = RunForkRouteHistoryUnknownUnversioned
	}
	activeSessions := map[string]struct{}{}
	for _, session := range snapshot.Sessions {
		if session.Status == "active" || session.Status == "suspended" {
			activeSessions[strings.TrimSpace(session.SessionID)] = struct{}{}
		}
	}
	activeSession := runForkPendingReferencesActiveSession(pending) || len(activeSessions) > 0
	activeTurn := runForkPendingReferencesActiveSession(pending)
	if !activeTurn {
		for _, turn := range snapshot.Turns {
			if _, ok := activeSessions[strings.TrimSpace(turn.SessionID)]; ok {
				activeTurn = true
				break
			}
		}
	}
	openReplyContext := false
	for _, replyContext := range snapshot.ReplyContexts {
		if strings.TrimSpace(replyContext.State) == "open" {
			openReplyContext = true
			break
		}
	}
	return runForkAdmissionEvidence{
		Pending:                 pending,
		RelevantTimer:           relevantTimer,
		RouteHistory:            RunForkRouteHistoryProjection{State: routeState},
		ActiveSession:           activeSession,
		ActiveConversationAudit: len(snapshot.ConversationAudits) > 0,
		ActiveTurn:              activeTurn,
		OpenReplyContext:        openReplyContext,
	}, nil
}

func loadRunForkSourceFactsFromRevision(snapshot *runForkRevisionSnapshot, entities []RunForkEntityState) runForkSourceFacts {
	entitySet := map[string]struct{}{}
	flowSet := map[string]struct{}{}
	sourceFlowSet := map[string]struct{}{}
	for _, entity := range entities {
		if entityID := strings.TrimSpace(entity.EntityID); entityID != "" {
			entitySet[entityID] = struct{}{}
		}
	}
	if snapshot != nil {
		for _, event := range snapshot.Events {
			if entityID := strings.TrimSpace(event.EntityID); entityID != "" {
				entitySet[entityID] = struct{}{}
			}
			if flowInstance := strings.TrimSpace(event.FlowInstance); flowInstance != "" {
				flowSet[flowInstance] = struct{}{}
			}
			sourceRoute := event.SourceRoute.Normalized()
			sourceFlow := strings.Trim(strings.TrimSpace(sourceRoute.FlowID), "/")
			if sourceFlow == "" {
				sourceFlow = runtimeflowidentity.SemanticScope(sourceRoute.FlowInstance)
			}
			if sourceFlow != "" {
				sourceFlowSet[sourceFlow] = struct{}{}
			}
		}
	}
	return runForkSourceFacts{
		EntityIDs:     stringSetValues(entitySet),
		FlowInstances: stringSetValues(flowSet),
		SourceFlows:   stringSetValues(sourceFlowSet),
	}
}

func runForkRevisionSubscriberKey(eventID, subscriberType, subscriberID string) string {
	return strings.TrimSpace(eventID) + "/" + strings.TrimSpace(subscriberType) + "/" + strings.TrimSpace(subscriberID)
}

func runForkRevisionLineageError(family, factKey, eventID string) error {
	return runForkReplayResumeError(
		RunForkBlockerDeliveryHistoryUnproven,
		RunForkReplayResumeFactDeliveryPendingHistory,
		"revisioned "+strings.TrimSpace(family)+" fact "+strings.TrimSpace(factKey)+" has no revisioned source event "+strings.TrimSpace(eventID),
	)
}

func stringSliceSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}
