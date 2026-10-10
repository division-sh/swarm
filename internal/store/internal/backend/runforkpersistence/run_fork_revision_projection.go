package runforkpersistence

import (
	"fmt"
	"sort"
	"strings"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func loadRunForkPendingWorkFromRevision(snapshot *runForkRevisionSnapshot) ([]runfork.RunForkPendingWork, error) {
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
	out := make([]runfork.RunForkPendingWork, 0, len(snapshot.Deliveries)+len(snapshot.Receipts))
	for _, delivery := range snapshot.Deliveries {
		durable := delivery.Snapshot
		event, ok := events[strings.TrimSpace(durable.EventID)]
		if !ok {
			return nil, runForkRevisionLineageError("event_deliveries", durable.DeliveryID, durable.EventID)
		}
		key := runForkRevisionSubscriberKey(durable.EventID, string(durable.SubscriberClass), durable.SubscriberID)
		deliveryKeys[key] = struct{}{}
		item := runfork.RunForkPendingWork{
			ClaimVersion:          durable.ClaimVersion,
			EventID:               strings.TrimSpace(durable.EventID),
			EventName:             strings.TrimSpace(event.EventName),
			FlowInstance:          strings.TrimSpace(event.FlowInstance),
			RoutingSource:         event.RoutingSource,
			DeliveryRoute:         durable.Route,
			DeliveryID:            strings.TrimSpace(durable.DeliveryID),
			SubscriberType:        string(durable.SubscriberClass),
			SubscriberID:          strings.TrimSpace(durable.SubscriberID),
			Status:                string(durable.Status),
			RetryCount:            durable.RetryCount,
			ReasonCode:            strings.TrimSpace(durable.ReasonCode),
			ActiveSessionID:       strings.TrimSpace(durable.ActiveSessionID),
			CreatedAt:             durable.CreatedAt,
			StartedAt:             traceTimePtr(durable.StartedAt),
			ContinuationHandoffAt: traceTimePtr(durable.ContinuationHandoffAt),
			DeliveredAt:           traceTimePtr(durable.SettledAt),
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
		item := runfork.RunForkPendingWork{
			EventID:        strings.TrimSpace(receipt.EventID),
			EventName:      strings.TrimSpace(event.EventName),
			FlowInstance:   strings.TrimSpace(event.FlowInstance),
			RoutingSource:  event.RoutingSource,
			SubscriberType: strings.TrimSpace(receipt.SubscriberType),
			SubscriberID:   strings.TrimSpace(receipt.SubscriberID),
			ReasonCode:     strings.TrimSpace(receipt.ReasonCode),
			CreatedAt:      receipt.ProcessedAt,
			DeliveredAt:    &receiptAt,
			ReceiptOutcome: strings.TrimSpace(receipt.Outcome),
			ReceiptAt:      &receiptAt,
		}
		if item.ReceiptOutcome == "dead_letter" {
			item.Classification = runfork.RunForkPendingClassificationDeadLetter
		} else {
			item.Classification = runfork.RunForkPendingClassificationDeliveredCompleted
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
		return runfork.RunForkPendingClassificationDeadLetter
	}
	switch snapshot.Status {
	case runtimedelivery.StatusPending:
		return runfork.RunForkPendingClassificationPending
	case runtimedelivery.StatusInProgress:
		return runfork.RunForkPendingClassificationInProgress
	case runtimedelivery.StatusFailed:
		return runfork.RunForkPendingClassificationFailedRetryable
	case runtimedelivery.StatusDelivered:
		return runfork.RunForkPendingClassificationDeliveredCompleted
	case runtimedelivery.StatusCanceled:
		return runfork.RunForkPendingClassificationCanceled
	default:
		return ""
	}
}

func loadRunForkAdmissionEvidenceFromRevision(snapshot *runForkRevisionSnapshot, entities []runfork.RunForkEntityState, pending []runfork.RunForkPendingWork, fanOut []runfork.RunForkFanOutObligation) (runForkAdmissionEvidence, error) {
	facts := loadRunForkSourceFactsFromRevision(snapshot, entities)
	ownedSchedules, err := validateRunForkBarrierSchedules(snapshot, fanOut)
	if err != nil {
		return runForkAdmissionEvidence{}, err
	}
	if err := admitRunForkTerminalBarrierHistory(snapshot, fanOut, pending); err != nil {
		return runForkAdmissionEvidence{}, err
	}
	arrivals, err := loadRunForkArrivalJoinSchedules(snapshot, entities)
	if err != nil {
		return runForkAdmissionEvidence{}, err
	}
	timerHistory, err := loadRunForkTimerHistoryInventory(snapshot, facts, ownedSchedules, arrivals)
	if err != nil {
		return runForkAdmissionEvidence{}, err
	}
	routeState := runfork.RunForkRouteHistoryNotApplicable
	if len(facts.FlowInstances) > 0 || len(facts.SourceFlows) > 0 {
		routeState = runfork.RunForkRouteHistoryUnknownUnversioned
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
		if replyContext.Record.State == "open" {
			openReplyContext = true
			break
		}
	}
	return runForkAdmissionEvidence{
		Pending:                 pending,
		RelevantTimer:           len(timerHistory.WorkflowTimerIDs)+len(timerHistory.ArrivalScheduleIDs)+len(timerHistory.UnresolvedTimerIDs) != 0,
		TimerHistory:            timerHistory,
		JoinSchedules:           arrivals,
		RouteHistory:            runfork.RunForkRouteHistoryProjection{State: routeState},
		ActiveSession:           activeSession,
		ActiveConversationAudit: len(snapshot.ConversationAudits) > 0,
		ActiveTurn:              activeTurn,
		OpenReplyContext:        openReplyContext,
	}, nil
}

func loadRunForkTimerHistoryInventory(snapshot *runForkRevisionSnapshot, facts runForkSourceFacts, ownedSchedules map[string]struct{}, arrivals []genericschedule.Activation) (runForkTimerHistoryInventory, error) {
	entityIDs, flowInstances := stringSliceSet(facts.EntityIDs), stringSliceSet(facts.FlowInstances)
	seen := make(map[string]struct{}, len(snapshot.Timers))
	var records []pipeline.WorkflowTimerActivationPersistenceRecord
	var unresolved []string
	arrivalByID := make(map[string]genericschedule.Activation, len(arrivals))
	for _, arrival := range arrivals {
		if _, duplicate := arrivalByID[arrival.ID]; duplicate {
			return runForkTimerHistoryInventory{}, fmt.Errorf("arrival inventory repeats source activation")
		}
		arrivalByID[arrival.ID] = arrival
	}
	for _, timer := range snapshot.Timers {
		if _, owned := ownedSchedules[timer.TimerID]; owned {
			continue
		}
		_, entityRelevant := entityIDs[strings.TrimSpace(timer.EntityID)]
		_, flowRelevant := flowInstances[strings.TrimSpace(timer.FlowInstance)]
		if timer.RunID != snapshot.RunID && !(entityRelevant && strings.TrimSpace(timer.EntityID) != "") && !(flowRelevant && strings.TrimSpace(timer.FlowInstance) != "") {
			continue
		}
		if timer.TimerID == "" || timer.TimerID != strings.TrimSpace(timer.TimerID) {
			return runForkTimerHistoryInventory{}, fmt.Errorf("relevant timer history requires exact row identity")
		}
		if _, duplicate := seen[timer.TimerID]; duplicate {
			return runForkTimerHistoryInventory{}, fmt.Errorf("relevant timer history repeats row identity %s", timer.TimerID)
		}
		seen[timer.TimerID] = struct{}{}
		if arrival, owned := arrivalByID[timer.TimerID]; owned {
			if err := requireRunForkArrivalSourceRecord(timer, arrival); err != nil {
				return runForkTimerHistoryInventory{}, err
			}
			delete(arrivalByID, timer.TimerID)
			continue
		}
		if timer.TaskType != "workflow_timer" || timer.RunID != snapshot.RunID {
			unresolved = append(unresolved, timer.TimerID)
			continue
		}
		activation, err := workflowTimerActivationFromSnapshot(timer.TimerSnapshot)
		if err != nil {
			return runForkTimerHistoryInventory{}, err
		}
		records = append(records, activation.PersistenceRecord())
	}
	if len(arrivalByID) != 0 {
		return runForkTimerHistoryInventory{}, fmt.Errorf("arrival inventory omits its physical source row")
	}
	inventory, err := runForkTimerRecordInventory(snapshot.RunID, records, arrivals)
	if err != nil {
		return runForkTimerHistoryInventory{}, err
	}
	sort.Strings(unresolved)
	inventory.Complete, inventory.UnresolvedTimerIDs = true, unresolved
	return inventory, nil
}

func requireRunForkArrivalSourceRecord(timer runForkRevisionTimer, arrival genericschedule.Activation) error {
	actual, err := projectRunForkGenericActivation(timer)
	if err != nil {
		return err
	}
	want, err := arrival.EvidenceDigest()
	if err != nil {
		return err
	}
	got, err := actual.EvidenceDigest()
	if err != nil || got != want {
		return fmt.Errorf("arrival inventory differs from its exact source row")
	}
	return nil
}

func loadRunForkSourceFactsFromRevision(snapshot *runForkRevisionSnapshot, entities []runfork.RunForkEntityState) runForkSourceFacts {
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
			sourceRoute := event.RoutingSource.Route()
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
		runfork.RunForkBlockerDeliveryHistoryUnproven,
		runfork.RunForkReplayResumeFactDeliveryPendingHistory,
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

func StringSliceSet(values []string) map[string]struct{} { return stringSliceSet(values) }
