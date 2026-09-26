package runforkpersistence

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
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
	if err := admitRunForkInheritedFanOutHistory(snapshot); err != nil {
		return nil, err
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
	receiptsByEvent := make(map[string][]runForkRevisionReceipt)
	for _, receipt := range snapshot.Receipts {
		receiptsByEvent[strings.TrimSpace(receipt.EventID)] = append(receiptsByEvent[strings.TrimSpace(receipt.EventID)], receipt)
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
		identity, err := runForkFanOutFactIntentKey(snapshot.RunID, fact)
		if err != nil {
			return nil, fmt.Errorf("run-fork fan-out fact identity: %w", err)
		}
		key := identity.String()
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
		if fact.OriginKind == string(fanoutobligation.OriginHandler) {
			if err := canonicaljson.DecodePreservingNumberLexemes(fact.Capsule, &capsule); err != nil {
				return nil, fmt.Errorf("decode run-fork fan-out %s capsule: %w", key, err)
			}
		} else if len(fact.Capsule) != 0 && string(fact.Capsule) != "null" {
			return nil, fmt.Errorf("deployment fan-out %s cannot carry a handler capsule", key)
		}
		source := fanoutobligation.SourceRef{
			Kind: fanoutobligation.SourceKind(fact.SourceKind), EventID: fact.SourceEventID, RunID: fact.SourceRunID,
			EntityID: fact.SourceEntityID, Field: fact.SourceField, MutationID: fact.SourceMutationID,
			Declaration: durabledata.DeclarationRef{FlowPath: fact.SourceResourceFlowPath, EventName: fact.SourceResourceEventName},
			VersionID:   durabledata.VersionID(fact.SourceResourceVersionID),
		}
		requestSource := source
		if requestSource.Kind == fanoutobligation.SourceEntityField {
			requestSource.MutationID = ""
		}
		intentKey, err := runForkFanOutFactIntentKey(snapshot.RunID, fact)
		if err != nil {
			return nil, err
		}
		element := intentKey.ElementRef
		request := fanoutobligation.IntentRequest{
			Key: intentKey, Source: requestSource, Cardinality: fact.Cardinality, Capsule: capsule,
		}
		if fact.OriginKind == string(fanoutobligation.OriginDeployment) {
			request.Deployment = &fanoutobligation.DeploymentOrigin{
				BundleHash: fact.BundleHash, Declaration: source.Declaration,
				VersionID: source.VersionID, SchemaDigest: durabledata.SchemaDigest(fact.DeploymentSchemaDigest),
			}
			if fact.SemanticDigest != "" || source.Kind != fanoutobligation.SourceResourceVersion {
				return nil, fmt.Errorf("deployment fan-out %s has handler plan or non-resource source", key)
			}
		} else {
			request.PlanRef = runtimecontracts.FanOutPlanRef{BundleHash: fact.BundleHash, ElementRef: element, SemanticDigest: fact.SemanticDigest}
			if fact.DeploymentSchemaDigest != "" {
				return nil, fmt.Errorf("handler fan-out %s has deployment schema", key)
			}
		}
		intent := fanoutobligation.Intent{
			Request: request,
			Source:  source, Cursor: fact.Cursor, Status: fanoutobligation.Status(fact.Status), NextChunkSize: fanoutobligation.InitialChunkSize,
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
		pendingDeployment := make([]runfork.RunForkDeploymentPendingEvent, 0)
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
				if request.Deployment != nil {
					if err := fanoutobligation.ValidateCommittedDeploymentOrdinalEvent(intent, outcome.Ordinal, outcome.EventID); err != nil {
						return nil, fmt.Errorf("project run-fork deployment %s outcome %d: %w", key, index, err)
					}
				}
				terminal, disposition, deliveryIDs, phase, err := fixedRunForkFanOutEventDisposition(
					outcome.EventID, snapshot.RunID, request.Deployment, eventsByID, deliveriesByEvent, receiptsByEvent, pendingByDelivery,
				)
				if err != nil {
					return nil, fmt.Errorf("project run-fork fan-out %s outcome %d: %w", key, index, err)
				}
				if !terminal {
					if request.Deployment != nil {
						pendingDeployment = append(pendingDeployment, runfork.RunForkDeploymentPendingEvent{
							Ordinal: outcome.Ordinal, SourceEventID: outcome.EventID,
							SourceDeliveryIDs: deliveryIDs, Phase: phase,
						})
					} else {
						pendingReplays = append(pendingReplays, runfork.RunForkFanOutPendingReplay{Ordinal: outcome.Ordinal, SourceEventID: outcome.EventID})
					}
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
		if len(outcomes)+len(pendingReplays)+len(pendingDeployment) != intent.Cursor {
			return nil, fmt.Errorf("run-fork fan-out %s cursor %d has %d terminal outcomes, %d pending replays and %d deployment events", key, intent.Cursor, len(outcomes), len(pendingReplays), len(pendingDeployment))
		}
		barrier, err := projectRunForkFanOutBarrier(snapshot.RunID, aggregate.barrier, intent.Request.Key)
		if err != nil {
			return nil, fmt.Errorf("project run-fork fan-out %s barrier: %w", key, err)
		}
		out = append(out, runfork.RunForkFanOutObligation{Intent: intent, Outcomes: outcomes, PendingReplays: pendingReplays, PendingDeployment: pendingDeployment, Barrier: barrier})
	}
	return out, nil
}

func runForkFanOutFactIntentKey(runID string, fact runForkRevisionFanOutFact) (fanoutobligation.IntentKey, error) {
	var key fanoutobligation.IntentKey
	key.RunID = runID
	switch fanoutobligation.OriginKind(fact.OriginKind) {
	case fanoutobligation.OriginDeployment:
		if fact.FactKind == "barrier" || fact.TriggeringDeliveryID != "" || fact.FlowPath != "" || fact.DeclarationFamily != "" || fact.SemanticPath != "" {
			return key, fmt.Errorf("deployment fact carries handler coordinates or barrier")
		}
		key.DeploymentFeedID = fact.DeploymentFeedID
	case fanoutobligation.OriginHandler:
		if fact.DeploymentFeedID != "" {
			return key, fmt.Errorf("handler fact carries deployment feed identity")
		}
		key.TriggeringDeliveryID = fact.TriggeringDeliveryID
		key.ElementRef = runtimecontracts.FanOutElementRef{FlowPath: fact.FlowPath, Family: fact.DeclarationFamily, SemanticPath: fact.SemanticPath}
	default:
		return key, fmt.Errorf("unknown fan-out origin kind %q", fact.OriginKind)
	}
	return key, key.Validate()
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
	declaration, declarationErr := key.ElementRef.DeclarationIdentity()
	if declarationErr != nil || !fanOutRef.DeclarationIdentity().Equal(declaration) ||
		fanOutRef.BundleHash() != fact.BundleHash || fanOutRef.SemanticDigest() != fact.SemanticDigest ||
		joinRef.Node().FlowPath() != fact.BarrierTargetFlowPath ||
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
		Registration:         registration,
		Status:               fanoutbarrier.Status(fact.BarrierStatus),
		ScheduleKey:          strings.TrimSpace(fact.BarrierScheduleKey),
		ScheduleActivationID: strings.TrimSpace(fact.BarrierScheduleActivationID),
		UpdatedAt:            fact.BarrierUpdatedAt,
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
	runID string,
	deployment *fanoutobligation.DeploymentOrigin,
	eventsByID map[string]runForkRevisionEvent,
	deliveriesByEvent map[string][]runForkRevisionDelivery,
	receiptsByEvent map[string][]runForkRevisionReceipt,
	pendingByDelivery map[string]runfork.RunForkPendingWork,
) (bool, fanoutobligation.InheritedTerminalDisposition, []string, runfork.RunForkDeploymentPendingPhase, error) {
	eventID = strings.TrimSpace(eventID)
	event, ok := eventsByID[eventID]
	if !ok {
		return false, "", nil, "", fmt.Errorf("owned event %s is absent from fixed revision", eventID)
	}
	if event.RunID != runID {
		return false, "", nil, "", fmt.Errorf("owned event %s belongs to another run", eventID)
	}
	if deployment != nil && (event.RoutingSource.Kind() != events.RoutingSourceDeploymentFeed ||
		event.RoutingSource.Route().FlowID != deployment.Declaration.FlowPath ||
		event.EventName != deployment.Declaration.EventName ||
		event.PayloadSchemaBundleHash != deployment.BundleHash ||
		event.PayloadSchemaFlowID != deployment.Declaration.FlowPath ||
		event.PayloadSchemaEventKey != deployment.Declaration.EventName ||
		event.PayloadSchemaDigest == "") {
		return false, "", nil, "", fmt.Errorf("deployment event %s contradicts its fixed feed declaration or event schema binding", eventID)
	}
	var settlement events.RouteSettlement
	if err := json.Unmarshal(event.RouteSettlement, &settlement); err != nil {
		return false, "", nil, "", fmt.Errorf("decode event %s route settlement: %w", eventID, err)
	}
	deliveries := deliveriesByEvent[eventID]
	routes := make([]events.DeliveryRoute, 0, len(deliveries))
	for _, delivery := range deliveries {
		routes = append(routes, delivery.Snapshot.Route)
	}
	if err := settlement.Validate(routes); err != nil {
		return false, "", nil, "", fmt.Errorf("event %s route settlement contradicts fixed deliveries: %w", eventID, err)
	}
	if settlement.NoDelivery() {
		return true, fanoutobligation.InheritedNoRoute, nil, "", nil
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
			return true, fanoutobligation.InheritedDeadLettered, nil, "", nil
		}
		return true, fanoutobligation.InheritedSucceeded, nil, "", nil
	}
	var phase runfork.RunForkDeploymentPendingPhase
	if deployment != nil {
		pipelineReceipts := 0
		for _, receipt := range receiptsByEvent[eventID] {
			if receipt.SubscriberType == "platform" && receipt.SubscriberID == "pipeline" {
				if receipt.Outcome != "success" || pipelineReceipts != 0 {
					return false, "", nil, "", fmt.Errorf("deployment event %s has contradictory pipeline receipt", eventID)
				}
				pipelineReceipts++
			}
		}
		if pipelineReceipts == 0 {
			phase = runfork.RunForkDeploymentPendingPublication
		} else {
			phase = runfork.RunForkDeploymentPendingReceiver
		}
	}
	deliveryIDs := make([]string, 0, len(deliveries))
	for _, delivery := range deliveries {
		if delivery.Snapshot.Terminal() {
			return false, "", nil, "", fmt.Errorf("nonterminal event %s mixes terminal and pending routes; exact fork replay is unsupported", eventID)
		}
		pending, ok := pendingByDelivery[strings.TrimSpace(delivery.Snapshot.DeliveryID)]
		if !ok || strings.TrimSpace(pending.EventID) != eventID {
			return false, "", nil, "", fmt.Errorf("nonterminal event %s delivery %s has no fixed pending evidence", eventID, delivery.Snapshot.DeliveryID)
		}
		if deployment == nil && !runfork.RunForkPendingWorkReplayableForHistoricalReplay(pending) {
			return false, "", nil, "", fmt.Errorf("nonterminal event %s delivery %s has no supported historical replay", eventID, delivery.Snapshot.DeliveryID)
		}
		if deployment != nil && (pending.Classification != runfork.RunForkPendingClassificationPending || pending.Status != "pending" ||
			pending.RetryCount != 0 || pending.ActiveSessionID != "" || pending.StartedAt != nil || pending.DeliveredAt != nil || pending.ReceiptAt != nil ||
			((phase == runfork.RunForkDeploymentPendingPublication) != (pending.ContinuationHandoffAt == nil))) {
			return false, "", nil, "", fmt.Errorf("deployment event %s delivery %s contradicts its pending handoff phase", eventID, delivery.Snapshot.DeliveryID)
		}
		deliveryIDs = append(deliveryIDs, delivery.Snapshot.DeliveryID)
	}
	sort.Strings(deliveryIDs)
	return false, "", deliveryIDs, phase, nil
}

func outcomeOrdinal(fact runForkRevisionFanOutFact) int {
	if fact.Ordinal == nil {
		return -1
	}
	return *fact.Ordinal
}
