package runforkpersistence

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func loadRunForkFanOutObligationsFromRevision(snapshot *runForkRevisionSnapshot, pending []runfork.RunForkPendingWork) ([]runfork.RunForkFanOutObligation, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("run-fork fan-out projection requires revision snapshot")
	}
	eventsByID := make(map[string]runForkRevisionEvent, len(snapshot.Events))
	for _, event := range snapshot.Events {
		eventsByID[strings.TrimSpace(event.EventID)] = event
	}
	deliveriesByEvent := make(map[string][]runForkRevisionDelivery)
	for _, delivery := range snapshot.Deliveries {
		eventID := strings.TrimSpace(delivery.Snapshot.EventID)
		deliveriesByEvent[eventID] = append(deliveriesByEvent[eventID], delivery)
	}
	pendingByDelivery := make(map[string]runfork.RunForkPendingWork, len(pending))
	for _, item := range pending {
		if deliveryID := strings.TrimSpace(item.DeliveryID); deliveryID != "" {
			pendingByDelivery[deliveryID] = item
		}
	}
	type aggregate struct {
		intent   *runForkRevisionFanOutFact
		outcomes []runForkRevisionFanOutFact
		barrier  *runForkRevisionFanOutFact
	}
	byKey := make(map[string]*aggregate)
	for index := range snapshot.FanOutFacts {
		fact := snapshot.FanOutFacts[index]
		key := strings.Join([]string{fact.TriggeringDeliveryID, fact.PackageKey, fact.ElementID}, "|")
		item := byKey[key]
		if item == nil {
			item = &aggregate{}
			byKey[key] = item
		}
		switch strings.TrimSpace(fact.FactKind) {
		case "intent":
			if item.intent != nil {
				return nil, fmt.Errorf("run-fork fan-out %s has duplicate intent facts", key)
			}
			item.intent = &snapshot.FanOutFacts[index]
		case "outcome":
			item.outcomes = append(item.outcomes, fact)
		case "barrier":
			if item.barrier != nil {
				return nil, fmt.Errorf("run-fork fan-out %s has duplicate barrier facts", key)
			}
			item.barrier = &snapshot.FanOutFacts[index]
		default:
			return nil, fmt.Errorf("run-fork fan-out %s has unknown fact kind %q", key, fact.FactKind)
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]runfork.RunForkFanOutObligation, 0, len(keys))
	for _, key := range keys {
		aggregate := byKey[key]
		if aggregate.intent == nil {
			return nil, fmt.Errorf("run-fork fan-out %s has outcomes without intent", key)
		}
		fact := *aggregate.intent
		var capsule fanoutobligation.Capsule
		if err := json.Unmarshal(fact.Capsule, &capsule); err != nil {
			return nil, fmt.Errorf("decode run-fork fan-out %s capsule: %w", key, err)
		}
		source := fanoutobligation.SourceRef{
			Kind: fanoutobligation.SourceKind(fact.SourceKind), EventID: fact.SourceEventID, RunID: fact.SourceRunID,
			EntityID: fact.SourceEntityID, Field: fact.SourceField, MutationID: fact.SourceMutationID,
			Declaration: durabledata.DeclarationRef{PackageKey: fact.SourceResourcePackageKey, EventName: fact.SourceResourceEventName},
			VersionID:   durabledata.VersionID(fact.SourceResourceVersionID),
		}
		requestSource := source
		if requestSource.Kind == fanoutobligation.SourceEntityField {
			requestSource.MutationID = ""
		}
		element := runtimecontracts.FanOutElementRef{PackageKey: fact.PackageKey, ElementID: fact.ElementID}
		intent := fanoutobligation.Intent{
			Request: fanoutobligation.IntentRequest{
				Key:     fanoutobligation.IntentKey{RunID: snapshot.RunID, TriggeringDeliveryID: fact.TriggeringDeliveryID, ElementRef: element},
				PlanRef: runtimecontracts.FanOutPlanRef{BundleHash: fact.BundleHash, ElementRef: element, SemanticDigest: fact.SemanticDigest},
				Source:  requestSource, Cardinality: fact.Cardinality, Capsule: capsule,
			},
			Source: source, Cursor: fact.Cursor, Status: fanoutobligation.Status(fact.Status), NextChunkSize: fanoutobligation.InitialChunkSize,
			CreatedAt: fact.CreatedAt, UpdatedAt: fact.CreatedAt, BlockedReason: fact.BlockedReason,
		}
		if err := intent.Validate(); err != nil {
			return nil, fmt.Errorf("validate run-fork fan-out %s intent: %w", key, err)
		}
		sort.Slice(aggregate.outcomes, func(i, j int) bool {
			return outcomeOrdinal(aggregate.outcomes[i]) < outcomeOrdinal(aggregate.outcomes[j])
		})
		outcomes := make([]fanoutobligation.Outcome, 0, len(aggregate.outcomes))
		pendingReplays := make([]runfork.RunForkFanOutPendingReplay, 0)
		for index, outcomeFact := range aggregate.outcomes {
			if outcomeFact.Ordinal == nil || *outcomeFact.Ordinal != index || index >= intent.Cursor {
				return nil, fmt.Errorf("run-fork fan-out %s outcomes are not the exact contiguous cursor prefix", key)
			}
			failure := outcomeFact.Failure
			if string(failure) == "null" {
				failure = nil
			}
			outcome := fanoutobligation.Outcome{
				Ordinal: *outcomeFact.Ordinal, Kind: fanoutobligation.OutcomeKind(outcomeFact.OutcomeKind),
				EventID: outcomeFact.EventID, SourceEventID: outcomeFact.SourceOutcomeEventID,
				InheritedDisposition: fanoutobligation.InheritedTerminalDisposition(outcomeFact.InheritedDisposition),
				Failure:              failure, CreatedAt: outcomeFact.CreatedAt,
			}
			if outcome.Kind == fanoutobligation.OutcomeCommitted && strings.TrimSpace(outcome.EventID) != "" {
				terminal, disposition, err := fixedRunForkFanOutEventDisposition(
					outcome.EventID, eventsByID, deliveriesByEvent, pendingByDelivery,
				)
				if err != nil {
					return nil, fmt.Errorf("project run-fork fan-out %s outcome %d: %w", key, index, err)
				}
				if !terminal {
					pendingReplays = append(pendingReplays, runfork.RunForkFanOutPendingReplay{Ordinal: outcome.Ordinal, SourceEventID: outcome.EventID})
					continue
				}
				outcome.SourceEventID = outcome.EventID
				outcome.EventID = ""
				outcome.InheritedDisposition = disposition
			}
			if err := outcome.Validate(); err != nil {
				return nil, fmt.Errorf("validate run-fork fan-out %s outcome %d: %w", key, index, err)
			}
			outcomes = append(outcomes, outcome)
		}
		if len(outcomes)+len(pendingReplays) != intent.Cursor {
			return nil, fmt.Errorf("run-fork fan-out %s cursor %d has %d terminal outcomes and %d pending replays", key, intent.Cursor, len(outcomes), len(pendingReplays))
		}
		barrier, err := projectRunForkFanOutBarrier(snapshot.RunID, aggregate.barrier, intent.Request.Key)
		if err != nil {
			return nil, fmt.Errorf("project run-fork fan-out %s barrier: %w", key, err)
		}
		out = append(out, runfork.RunForkFanOutObligation{Intent: intent, Outcomes: outcomes, PendingReplays: pendingReplays, Barrier: barrier})
	}
	return out, nil
}

func projectRunForkFanOutBarrier(runID string, fact *runForkRevisionFanOutFact, key fanoutobligation.IntentKey) (*fanoutbarrier.Barrier, error) {
	if fact == nil {
		return nil, nil
	}
	var routingSource events.RoutingSource
	if err := json.Unmarshal(fact.BarrierRoutingSource, &routingSource); err != nil {
		return nil, fmt.Errorf("decode routing source: %w", err)
	}
	var handle timeridentity.TimerHandle
	if err := json.Unmarshal(fact.BarrierTimerHandle, &handle); err != nil {
		return nil, fmt.Errorf("decode timer handle: %w", err)
	}
	joinRef, ok := handle.JoinRef()
	if !ok {
		return nil, fmt.Errorf("timer handle is not a join declaration")
	}
	fanOutRef, ok := joinRef.FanOutDelivery()
	if !ok {
		return nil, fmt.Errorf("timer handle is not a fan-out delivery join")
	}
	if fanOutRef.PackageKey() != fact.PackageKey || fanOutRef.ElementID() != fact.ElementID ||
		fanOutRef.BundleHash() != fact.BundleHash || fanOutRef.SemanticDigest() != fact.SemanticDigest ||
		joinRef.Node().PackageKey() != fact.BarrierTargetPackageKey || joinRef.Node().FlowID() != fact.BarrierTargetFlowID ||
		joinRef.Node().NodeID() != fact.BarrierTargetNodeID || joinRef.HandlerEvent() != fact.BarrierHandlerEvent ||
		joinRef.JoinID() != fact.BarrierJoinID {
		return nil, fmt.Errorf("typed timer handle contradicts projected barrier identity")
	}
	mode, ok := executionmode.Parse(fact.BarrierExecutionMode)
	if !ok {
		return nil, fmt.Errorf("invalid execution mode %q", fact.BarrierExecutionMode)
	}
	registration := fanoutbarrier.Registration{
		IntentKey: key,
		PlanRef: runtimecontracts.FanOutPlanRef{
			BundleHash: fact.BundleHash, ElementRef: key.ElementRef, SemanticDigest: fact.SemanticDigest,
		},
		Handle: handle,
		Route: runtimeflowidentity.StoredRoute(
			fact.BarrierRouteScopeKey, fact.BarrierRouteInstanceID, fact.BarrierRouteInstancePath,
		),
		EntityID:      fact.BarrierEntityID,
		RoutingSource: routingSource,
		ExecutionMode: mode,
		CreatedAt:     fact.CreatedAt,
	}
	registration.IntentKey.RunID = strings.TrimSpace(runID)
	barrier := &fanoutbarrier.Barrier{
		Registration: registration,
		Status:       fanoutbarrier.Status(fact.BarrierStatus),
		ScheduleKey:  strings.TrimSpace(fact.BarrierScheduleKey),
		UpdatedAt:    fact.BarrierUpdatedAt,
	}
	if len(fact.BarrierSummary) != 0 && string(fact.BarrierSummary) != "null" {
		var summary fanoutbarrier.Summary
		if err := json.Unmarshal(fact.BarrierSummary, &summary); err != nil {
			return nil, fmt.Errorf("decode summary: %w", err)
		}
		barrier.Summary = &summary
	}
	if err := barrier.Validate(); err != nil {
		return nil, err
	}
	return barrier, nil
}

func fixedRunForkFanOutEventDisposition(
	eventID string,
	eventsByID map[string]runForkRevisionEvent,
	deliveriesByEvent map[string][]runForkRevisionDelivery,
	pendingByDelivery map[string]runfork.RunForkPendingWork,
) (bool, fanoutobligation.InheritedTerminalDisposition, error) {
	eventID = strings.TrimSpace(eventID)
	event, ok := eventsByID[eventID]
	if !ok {
		return false, "", fmt.Errorf("owned event %s is absent from fixed revision", eventID)
	}
	var settlement events.RouteSettlement
	if err := json.Unmarshal(event.RouteSettlement, &settlement); err != nil {
		return false, "", fmt.Errorf("decode event %s route settlement: %w", eventID, err)
	}
	deliveries := deliveriesByEvent[eventID]
	routes := make([]events.DeliveryRoute, 0, len(deliveries))
	for _, delivery := range deliveries {
		routes = append(routes, delivery.Snapshot.Route)
	}
	if err := settlement.Validate(routes); err != nil {
		return false, "", fmt.Errorf("event %s route settlement contradicts fixed deliveries: %w", eventID, err)
	}
	if settlement.NoDelivery() {
		return true, fanoutobligation.InheritedNoRoute, nil
	}
	deadLettered := false
	allTerminal := true
	for _, delivery := range deliveries {
		if delivery.Snapshot.Terminal() {
			if string(delivery.Snapshot.Status) == "dead_letter" {
				deadLettered = true
			}
			continue
		}
		allTerminal = false
	}
	if allTerminal {
		if deadLettered {
			return true, fanoutobligation.InheritedDeadLettered, nil
		}
		return true, fanoutobligation.InheritedSucceeded, nil
	}
	for _, delivery := range deliveries {
		if delivery.Snapshot.Terminal() {
			return false, "", fmt.Errorf("nonterminal event %s mixes terminal and pending routes; exact fork replay is unsupported", eventID)
		}
		pending, ok := pendingByDelivery[strings.TrimSpace(delivery.Snapshot.DeliveryID)]
		if !ok || strings.TrimSpace(pending.EventID) != eventID || !runfork.RunForkPendingWorkReplayableForHistoricalReplay(pending) {
			return false, "", fmt.Errorf("nonterminal event %s delivery %s has no supported historical replay", eventID, delivery.Snapshot.DeliveryID)
		}
	}
	return false, "", nil
}

func outcomeOrdinal(fact runForkRevisionFanOutFact) int {
	if fact.Ordinal == nil {
		return -1
	}
	return *fact.Ordinal
}
