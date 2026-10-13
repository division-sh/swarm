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
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
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
	byKey, err := groupRunForkFanOutFacts(snapshot.RunID, snapshot.FanOutFacts)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]runfork.RunForkFanOutObligation, 0, len(keys))
	for _, key := range keys {
		aggregate := byKey[key]
		intent, err := projectRunForkFanOutIntent(snapshot.RunID, key, aggregate.intent)
		if err != nil {
			return nil, err
		}
		obligation, err := projectRunForkFanOutOutcomePrefix(
			snapshot.RunID, key, intent, aggregate.outcomes, eventsByID, deliveriesByEvent, receiptsByEvent, pendingByDelivery,
		)
		if err != nil {
			return nil, err
		}
		barrier, err := projectRunForkFanOutBarrier(snapshot.RunID, aggregate.barrier, intent.Request.Key)
		if err != nil {
			return nil, fmt.Errorf("project run-fork fan-out %s barrier: %w", key, err)
		}
		obligation.Barrier = barrier
		out = append(out, obligation)
	}
	return out, nil
}

type runForkFanOutFactGroup struct {
	intent   *runForkRevisionFanOutFact
	outcomes []runForkRevisionFanOutFact
	barrier  *runForkRevisionFanOutFact
}

func groupRunForkFanOutFacts(runID string, facts []runForkRevisionFanOutFact) (map[string]*runForkFanOutFactGroup, error) {
	byKey := make(map[string]*runForkFanOutFactGroup)
	for index := range facts {
		fact := facts[index]
		identity, err := runForkFanOutFactIntentKey(runID, fact)
		if err != nil {
			return nil, fmt.Errorf("run-fork fan-out fact identity: %w", err)
		}
		key := identity.String()
		item := byKey[key]
		if item == nil {
			item = &runForkFanOutFactGroup{}
			byKey[key] = item
		}
		switch strings.TrimSpace(fact.FactKind) {
		case "intent":
			if item.intent != nil {
				return nil, fmt.Errorf("run-fork fan-out %s has duplicate intent facts", key)
			}
			item.intent = &facts[index]
		case "outcome":
			item.outcomes = append(item.outcomes, fact)
		case "barrier":
			if item.barrier != nil {
				return nil, fmt.Errorf("run-fork fan-out %s has duplicate barrier facts", key)
			}
			item.barrier = &facts[index]
		default:
			return nil, fmt.Errorf("run-fork fan-out %s has unknown fact kind %q", key, fact.FactKind)
		}
	}
	return byKey, nil
}

func projectRunForkFanOutIntent(runID, key string, intentFact *runForkRevisionFanOutFact) (fanoutobligation.Intent, error) {
	if intentFact == nil {
		return fanoutobligation.Intent{}, fmt.Errorf("run-fork fan-out %s has outcomes without intent", key)
	}
	fact := *intentFact
	var capsule fanoutobligation.Capsule
	if fact.OriginKind == string(fanoutobligation.OriginHandler) {
		if err := canonicaljson.DecodePreservingNumberLexemes(fact.Capsule, &capsule); err != nil {
			return fanoutobligation.Intent{}, fmt.Errorf("decode run-fork fan-out %s capsule: %w", key, err)
		}
	} else if len(fact.Capsule) != 0 && string(fact.Capsule) != "null" {
		return fanoutobligation.Intent{}, fmt.Errorf("deployment fan-out %s cannot carry a handler capsule", key)
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
	intentKey, err := runForkFanOutFactIntentKey(runID, fact)
	if err != nil {
		return fanoutobligation.Intent{}, err
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
			return fanoutobligation.Intent{}, fmt.Errorf("deployment fan-out %s has handler plan or non-resource source", key)
		}
	} else {
		request.PlanRef = runtimecontracts.FanOutPlanRef{BundleHash: fact.BundleHash, ElementRef: element, SemanticDigest: fact.SemanticDigest}
		if fact.DeploymentSchemaDigest != "" {
			return fanoutobligation.Intent{}, fmt.Errorf("handler fan-out %s has deployment schema", key)
		}
	}
	intent := fanoutobligation.Intent{
		Request: request,
		Source:  source, Cursor: fact.Cursor, Status: fanoutobligation.Status(fact.Status), NextChunkSize: fanoutobligation.InitialChunkSize,
		CreatedAt: fact.CreatedAt, UpdatedAt: fact.CreatedAt, BlockedReason: fact.BlockedReason,
	}
	if err := intent.Validate(); err != nil {
		return fanoutobligation.Intent{}, fmt.Errorf("validate run-fork fan-out %s intent: %w", key, err)
	}
	return intent, nil
}

func projectRunForkFanOutOutcomePrefix(
	runID, key string,
	intent fanoutobligation.Intent,
	facts []runForkRevisionFanOutFact,
	eventsByID map[string]runForkRevisionEvent,
	deliveriesByEvent map[string][]runForkRevisionDelivery,
	receiptsByEvent map[string][]runForkRevisionReceipt,
	pendingByDelivery map[string]runfork.RunForkPendingWork,
) (runfork.RunForkFanOutObligation, error) {
	sort.Slice(facts, func(i, j int) bool {
		return outcomeOrdinal(facts[i]) < outcomeOrdinal(facts[j])
	})
	outcomes := make([]fanoutobligation.Outcome, 0, len(facts))
	pendingReplays := make([]runfork.RunForkFanOutPendingReplay, 0)
	pendingDeployment := make([]runfork.RunForkDeploymentPendingEvent, 0)
	for index, outcomeFact := range facts {
		outcome, err := projectRunForkFanOutOutcomeFact(key, index, intent.Cursor, outcomeFact)
		if err != nil {
			return runfork.RunForkFanOutObligation{}, err
		}
		if strings.TrimSpace(outcome.EventID) != "" {
			if err := admitRunForkFanOutOutcomePublication(runID, key, index, intent, outcome, eventsByID); err != nil {
				return runfork.RunForkFanOutObligation{}, err
			}
			terminal, disposition, deliveryIDs, phase, err := fixedRunForkFanOutEventDisposition(
				outcome.EventID, runID, intent.Request.Deployment, outcome.InheritedDisposition, eventsByID, deliveriesByEvent, receiptsByEvent, pendingByDelivery,
			)
			if err != nil {
				return runfork.RunForkFanOutObligation{}, fmt.Errorf("project run-fork fan-out %s outcome %d: %w", key, index, err)
			}
			if !terminal {
				if intent.Request.Deployment != nil {
					pendingDeployment = append(pendingDeployment, runfork.RunForkDeploymentPendingEvent{
						Ordinal: outcome.Ordinal, SourceEventID: outcome.EventID,
						SourceDeliveryIDs: deliveryIDs, Phase: phase, InheritedDisposition: disposition,
					})
				} else {
					pendingReplays = append(pendingReplays, runfork.RunForkFanOutPendingReplay{
						Ordinal: outcome.Ordinal, SourceEventID: outcome.EventID,
						SourceDeliveryIDs: deliveryIDs, InheritedDisposition: disposition,
					})
				}
				continue
			}
			outcome.SourceEventID = outcome.EventID
			outcome.EventID = ""
			outcome.Kind = fanoutobligation.OutcomeCommitted
			outcome.InheritedDisposition = disposition
		}
		if err := outcome.Validate(); err != nil {
			return runfork.RunForkFanOutObligation{}, fmt.Errorf("validate run-fork fan-out %s outcome %d: %w", key, index, err)
		}
		outcomes = append(outcomes, outcome)
	}
	if len(outcomes)+len(pendingReplays)+len(pendingDeployment) != intent.Cursor {
		return runfork.RunForkFanOutObligation{}, fmt.Errorf("run-fork fan-out %s cursor %d has %d terminal outcomes, %d pending replays and %d deployment events", key, intent.Cursor, len(outcomes), len(pendingReplays), len(pendingDeployment))
	}
	return runfork.RunForkFanOutObligation{Intent: intent, Outcomes: outcomes, PendingReplays: pendingReplays, PendingDeployment: pendingDeployment}, nil
}

func projectRunForkFanOutOutcomeFact(key string, index, cursor int, fact runForkRevisionFanOutFact) (fanoutobligation.Outcome, error) {
	if fact.Ordinal == nil || *fact.Ordinal != index || index >= cursor {
		return fanoutobligation.Outcome{}, fmt.Errorf("run-fork fan-out %s outcomes are not the exact contiguous cursor prefix", key)
	}
	failure := fact.Failure
	if string(failure) == "null" {
		failure = nil
	}
	outcome := fanoutobligation.Outcome{
		Ordinal: *fact.Ordinal, Kind: fanoutobligation.OutcomeKind(fact.OutcomeKind),
		EventID: fact.EventID, SourceEventID: fact.SourceOutcomeEventID,
		InheritedDisposition: fanoutobligation.InheritedTerminalDisposition(fact.InheritedDisposition),
		Failure:              failure, CreatedAt: fact.CreatedAt,
	}
	if err := outcome.Validate(); err != nil {
		return fanoutobligation.Outcome{}, fmt.Errorf("validate run-fork fan-out %s outcome %d: %w", key, index, err)
	}
	return outcome, nil
}

func admitRunForkFanOutOutcomePublication(runID, key string, index int, intent fanoutobligation.Intent, outcome fanoutobligation.Outcome, eventsByID map[string]runForkRevisionEvent) error {
	if outcome.Kind == fanoutobligation.OutcomeContinued {
		publication, present := eventsByID[outcome.EventID]
		if !present {
			return fmt.Errorf("continued fan-out event %s is absent from the cut", outcome.EventID)
		}
		admitted, err := decodeRunForkRevisionEvent(publication)
		if err != nil {
			return err
		}
		lineage, present := admitted.Event().SelectedForkLineage()
		if !present || lineage.SourceEventID() != outcome.SourceEventID || lineage.DestinationRunID() != runID {
			return fmt.Errorf("continued fan-out event %s contradicts its source lineage", outcome.EventID)
		}
	}
	if intent.Request.Deployment != nil {
		if outcome.Kind == fanoutobligation.OutcomeCommitted {
			if err := fanoutobligation.ValidateCommittedDeploymentOrdinalEvent(intent, outcome.Ordinal, outcome.EventID); err != nil {
				return fmt.Errorf("project run-fork deployment %s outcome %d: %w", key, index, err)
			}
		}
	}
	return nil
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
	transferred, err := fanoutbarrier.DecodeTransferredOccurrence(fact.BarrierTransferredOccurrence)
	if err != nil {
		return nil, fmt.Errorf("decode transferred barrier occurrence: %w", err)
	}
	barrier.TransferredOccurrence = transferred
	if err := barrier.Validate(); err != nil {
		return nil, err
	}
	return barrier, nil
}

func fixedRunForkFanOutEventDisposition(
	eventID string,
	runID string,
	deployment *fanoutobligation.DeploymentOrigin,
	retained fanoutobligation.InheritedTerminalDisposition,
	eventsByID map[string]runForkRevisionEvent,
	deliveriesByEvent map[string][]runForkRevisionDelivery,
	receiptsByEvent map[string][]runForkRevisionReceipt,
	pendingByDelivery map[string]runfork.RunForkPendingWork,
) (bool, fanoutobligation.InheritedTerminalDisposition, []string, runfork.RunForkDeploymentPendingPhase, error) {
	eventID = strings.TrimSpace(eventID)
	event, err := admitFixedRunForkFanOutEvent(eventID, runID, deployment, eventsByID)
	if err != nil {
		return false, "", nil, "", err
	}
	deliveries := deliveriesByEvent[eventID]
	settlement, err := decodeFixedRunForkFanOutRouteSettlement(eventID, event, deliveries)
	if err != nil {
		return false, "", nil, "", err
	}
	if settlement.NoDelivery() && retained != "" {
		return false, "", nil, "", fmt.Errorf("continued event %s lost its unfinished recipients", eventID)
	}
	if settlement.NoDelivery() {
		return true, fanoutobligation.InheritedNoRoute, nil, "", nil
	}
	recipients, err := fixedRunForkFanOutRecipients(eventID, runID, deliveries)
	if err != nil {
		return false, "", nil, "", err
	}
	disposition, terminal, err := fanoutbarrier.RecipientDisposition(recipients)
	if retained != "" {
		disposition, terminal, err = fanoutbarrier.RecipientDispositionWithRetained(recipients, retained)
	}
	if err != nil {
		return false, "", nil, "", fmt.Errorf("event %s recipient disposition: %w", eventID, err)
	}
	if terminal {
		return true, disposition, nil, "", nil
	}
	var phase runfork.RunForkDeploymentPendingPhase
	if deployment != nil {
		phase, err = fixedRunForkDeploymentPendingPhase(eventID, receiptsByEvent[eventID])
		if err != nil {
			return false, "", nil, "", err
		}
	}
	deliveryIDs := make([]string, 0, len(deliveries))
	settled := make([]runtimedelivery.Snapshot, 0, len(deliveries))
	for _, delivery := range deliveries {
		if delivery.Snapshot.Terminal() {
			settled = append(settled, delivery.Snapshot)
			continue
		}
		if err := admitFixedRunForkFanOutPendingDelivery(eventID, delivery.Snapshot, deployment, phase, pendingByDelivery); err != nil {
			return false, "", nil, "", err
		}
		deliveryIDs = append(deliveryIDs, delivery.Snapshot.DeliveryID)
	}
	if len(settled) != 0 {
		disposition, _, err = fanoutbarrier.RecipientDisposition(settled)
		if retained != "" {
			disposition, _, err = fanoutbarrier.RecipientDispositionWithRetained(settled, retained)
		}
		if err != nil {
			return false, "", nil, "", err
		}
	} else {
		disposition = retained
	}
	sort.Strings(deliveryIDs)
	return false, disposition, deliveryIDs, phase, nil
}

func admitFixedRunForkFanOutEvent(eventID, runID string, deployment *fanoutobligation.DeploymentOrigin, eventsByID map[string]runForkRevisionEvent) (runForkRevisionEvent, error) {
	event, ok := eventsByID[eventID]
	if !ok {
		return runForkRevisionEvent{}, fmt.Errorf("owned event %s is absent from fixed revision", eventID)
	}
	if event.RunID != runID {
		return runForkRevisionEvent{}, fmt.Errorf("owned event %s belongs to another run", eventID)
	}
	if deployment != nil && (event.RoutingSource.Kind() != events.RoutingSourceDeploymentFeed ||
		event.RoutingSource.Route().FlowID != deployment.Declaration.FlowPath ||
		event.EventName != deployment.Declaration.EventName ||
		event.PayloadSchemaBundleHash != deployment.BundleHash ||
		event.PayloadSchemaFlowID != deployment.Declaration.FlowPath ||
		event.PayloadSchemaEventKey != deployment.Declaration.EventName ||
		event.PayloadSchemaDigest == "") {
		return runForkRevisionEvent{}, fmt.Errorf("deployment event %s contradicts its fixed feed declaration or event schema binding", eventID)
	}
	return event, nil
}

func decodeFixedRunForkFanOutRouteSettlement(eventID string, event runForkRevisionEvent, deliveries []runForkRevisionDelivery) (events.RouteSettlement, error) {
	var settlement events.RouteSettlement
	if err := json.Unmarshal(event.RouteSettlement, &settlement); err != nil {
		return settlement, fmt.Errorf("decode event %s route settlement: %w", eventID, err)
	}
	routes := make([]events.DeliveryRoute, 0, len(deliveries))
	for _, delivery := range deliveries {
		routes = append(routes, delivery.Snapshot.Route)
	}
	if err := settlement.Validate(routes); err != nil {
		return settlement, fmt.Errorf("event %s route settlement contradicts fixed deliveries: %w", eventID, err)
	}
	return settlement, nil
}

func fixedRunForkFanOutRecipients(eventID, runID string, deliveries []runForkRevisionDelivery) ([]runtimedelivery.Snapshot, error) {
	recipients := make([]runtimedelivery.Snapshot, len(deliveries))
	seen := make(map[string]bool, len(deliveries))
	for i, delivery := range deliveries {
		if delivery.Snapshot.RunID != runID || delivery.Snapshot.EventID != eventID || seen[delivery.Snapshot.DeliveryID] {
			return nil, fmt.Errorf("event %s has foreign or duplicate recipient evidence", eventID)
		}
		seen[delivery.Snapshot.DeliveryID] = true
		recipients[i] = delivery.Snapshot
	}
	return recipients, nil
}

func fixedRunForkDeploymentPendingPhase(eventID string, receipts []runForkRevisionReceipt) (runfork.RunForkDeploymentPendingPhase, error) {
	pipelineReceipts := 0
	for _, receipt := range receipts {
		if receipt.SubscriberType == "platform" && receipt.SubscriberID == "pipeline" {
			if receipt.Outcome != "success" || pipelineReceipts != 0 {
				return "", fmt.Errorf("deployment event %s has contradictory pipeline receipt", eventID)
			}
			pipelineReceipts++
		}
	}
	if pipelineReceipts == 0 {
		return runfork.RunForkDeploymentPendingPublication, nil
	}
	return runfork.RunForkDeploymentPendingReceiver, nil
}

func admitFixedRunForkFanOutPendingDelivery(eventID string, delivery runtimedelivery.Snapshot, deployment *fanoutobligation.DeploymentOrigin, phase runfork.RunForkDeploymentPendingPhase, pendingByDelivery map[string]runfork.RunForkPendingWork) error {
	pending, ok := pendingByDelivery[strings.TrimSpace(delivery.DeliveryID)]
	if !ok || pending.EventID != eventID || pending.DeliveryID != delivery.DeliveryID ||
		!events.SameDeliveryRouteIdentity(pending.DeliveryRoute, delivery.Route) || pending.SubscriberType != string(delivery.SubscriberClass) ||
		pending.SubscriberID != delivery.SubscriberID || pending.Status != string(delivery.Status) ||
		pending.Classification != classifyRunForkDeliverySnapshot(delivery, false) {
		return fmt.Errorf("nonterminal event %s delivery %s has no fixed pending evidence", eventID, delivery.DeliveryID)
	}
	if deployment == nil && delivery.SubscriberClass != runtimedelivery.SubscriberNode && delivery.SubscriberClass != runtimedelivery.SubscriberAgent {
		return fmt.Errorf("nonterminal fan-out event %s has an unsupported recipient class", eventID)
	}
	if deployment != nil && (pending.Classification != runfork.RunForkPendingClassificationPending || pending.Status != "pending" ||
		pending.RetryCount != 0 || pending.ActiveSessionID != "" || pending.StartedAt != nil || pending.DeliveredAt != nil || pending.ReceiptAt != nil) {
		return fmt.Errorf("deployment event %s delivery %s contradicts its pending handoff phase", eventID, delivery.DeliveryID)
	}
	return nil
}

func outcomeOrdinal(fact runForkRevisionFanOutFact) int {
	if fact.Ordinal == nil {
		return -1
	}
	return *fact.Ordinal
}
