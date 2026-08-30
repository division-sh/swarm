package runforkadmission

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type SelectedContractRouteHistoryRequest struct {
	Plan              runfork.RunForkPlan
	Source            semanticview.Source
	ContractSelection runfork.RunForkContractSelection
	FrontierAdmission runfork.RunForkContractFrontierAdmission
}

func AdmitSelectedContractRouteHistory(req SelectedContractRouteHistoryRequest) (runfork.RunForkSelectedContractRouteAdmission, error) {
	if req.Source == nil {
		return runfork.RunForkSelectedContractRouteAdmission{}, fmt.Errorf("selected route admission requires selected contract semantic source")
	}
	selection := req.ContractSelection
	if strings.TrimSpace(selection.Mode) == "" {
		selection = SelectedContractSelection(req.Source)
	}
	if strings.TrimSpace(req.FrontierAdmission.Owner) != runfork.RunForkContractFrontierAdmissionOwner {
		return runfork.RunForkSelectedContractRouteAdmission{}, fmt.Errorf("selected route admission requires %s frontier admission; got %q", runfork.RunForkContractFrontierAdmissionOwner, req.FrontierAdmission.Owner)
	}
	if !req.FrontierAdmission.NonMutating {
		return runfork.RunForkSelectedContractRouteAdmission{}, fmt.Errorf("selected route admission requires non-mutating frontier admission")
	}

	routeTable, err := runtimebus.DeriveRouteTable(req.Source)
	if err != nil {
		return runfork.RunForkSelectedContractRouteAdmission{}, fmt.Errorf("derive selected route admission routes: %w", err)
	}
	if err := installContractFrontierFlowInstanceRoutes(routeTable, req.Source, req.Plan.PendingWork); err != nil {
		return runfork.RunForkSelectedContractRouteAdmission{}, err
	}
	connectGraph := runtimepinrouting.CompileConnectGraph(req.Source)
	connectIssues := connectGraph.Issues()
	if len(connectIssues) != 0 {
		return runfork.RunForkSelectedContractRouteAdmission{}, fmt.Errorf("derive selected route admission connect routes: %#v", connectIssues)
	}
	routeEvents, incompleteRoutes := selectedRouteHistoryEvents(routeTable, connectGraph, selectedRouteHistoryEventEvidence(req.Plan, req.FrontierAdmission))
	dynamicFlowInstances := selectedRouteHistoryDynamicFlowInstances(req.Source, req.Plan, req.FrontierAdmission)
	blockers := []runfork.RunForkUnsupportedBlocker{{
		Code:    runfork.RunForkBlockerSelectedContractRouteAdmissionNonMutating,
		Message: "selected-contract route admission is non-mutating; route persistence, recipient delivery writes, and handler execution remain separately gated",
	}}
	if selectedRouteHistoryHasSourceRouteFacts(req.Plan) {
		blockers = appendRunForkBlocker(blockers, runfork.RunForkUnsupportedBlocker{
			Code:    runfork.RunForkBlockerFlowRouteHistoryUnproven,
			Message: "source route rows are current operational state and remain evidence-only until selected route reconstruction is separately approved",
		})
	}
	if incompleteRoutes {
		blockers = appendRunForkBlocker(blockers, runfork.RunForkUnsupportedBlocker{
			Code:    runfork.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven,
			Message: "selected route history has a matched connect receiver that still requires runtime resolution",
		})
	}
	frontierEventCount, frontierSourceEventIDs, frontierFingerprint := runfork.RunForkContractFrontierEvidenceBinding(req.FrontierAdmission)

	return runfork.RunForkSelectedContractRouteAdmission{
		Owner:                          runfork.RunForkSelectedContractRouteAdmissionOwner,
		FutureRouteReconstructionOwner: runfork.RunForkSelectedContractExecutionOwner + ".route_reconstruction",
		NonMutating:                    true,
		RouteReconstructionSupported:   false,
		ContractSelection:              selection,
		SourceRouteFactsPresent:        selectedRouteHistoryHasSourceRouteFacts(req.Plan),
		SelectedRouteEvents:            routeEvents,
		DynamicFlowInstances:           dynamicFlowInstances,
		FrontierAdmissionOwner:         req.FrontierAdmission.Owner,
		FrontierEventCount:             frontierEventCount,
		FrontierSourceEventIDs:         frontierSourceEventIDs,
		FrontierEvidenceFingerprint:    frontierFingerprint,
		RequiredConsumers:              selectedRouteHistoryRequiredConsumers(),
		BlockedSiblings:                selectedRouteHistoryBlockedSiblings(),
		InvalidPaths:                   selectedRouteHistoryInvalidPaths(),
		UnsupportedBlockers:            blockers,
	}, nil
}

func selectedRouteHistoryHasSourceRouteFacts(plan runfork.RunForkPlan) bool {
	if hasUnsupportedBlocker(plan.UnsupportedBlockers, runfork.RunForkBlockerFlowRouteHistoryUnproven) {
		return true
	}
	for _, blocker := range plan.ReplayResumeAdmission.UnsupportedBlockers {
		if strings.TrimSpace(blocker.Code) == runfork.RunForkBlockerFlowRouteHistoryUnproven {
			return true
		}
	}
	for _, disposition := range plan.ReplayResumeAdmission.Dispositions {
		if strings.TrimSpace(disposition.Fact) == runfork.RunForkReplayResumeFactRouteHistory &&
			strings.TrimSpace(disposition.Disposition) == runfork.RunForkReplayResumeDispositionFailClosedBlocker {
			return true
		}
	}
	return false
}

type selectedRouteHistoryEvent struct {
	sourceEventID  string
	eventName      string
	routingSource  events.RoutingSource
	deliveryRoutes []events.DeliveryRoute
}

func selectedRouteHistoryEventEvidence(plan runfork.RunForkPlan, frontier runfork.RunForkContractFrontierAdmission) []selectedRouteHistoryEvent {
	frontierEventIDs := map[string]struct{}{}
	for _, event := range frontier.FrontierEvents {
		if sourceEventID := strings.TrimSpace(event.SourceEventID); sourceEventID != "" {
			frontierEventIDs[sourceEventID] = struct{}{}
		}
	}
	seen := map[string]*selectedRouteHistoryEvent{}
	add := func(sourceEventID, eventName string, routingSource events.RoutingSource, deliveryRoute events.DeliveryRoute) {
		sourceEventID = strings.TrimSpace(sourceEventID)
		eventName = strings.TrimSpace(eventName)
		if eventName == "" {
			return
		}
		if _, isFrontier := frontierEventIDs[sourceEventID]; sourceEventID != "" && isFrontier {
			return
		}
		key := sourceEventID
		if key == "" {
			key = eventName
		}
		event := seen[key]
		if event == nil {
			event = &selectedRouteHistoryEvent{sourceEventID: sourceEventID, eventName: eventName, routingSource: routingSource}
			seen[key] = event
		}
		if event.routingSource.Empty() && !routingSource.Empty() {
			event.routingSource = routingSource
		}
		if !deliveryRoute.ConnectClaim.Empty() {
			event.deliveryRoutes = append(event.deliveryRoutes, deliveryRoute)
		}
	}
	add(plan.ForkPoint.EventID, plan.ForkPoint.EventName, plan.ForkPoint.RoutingSource, events.DeliveryRoute{})
	for _, item := range plan.PendingWork {
		if strings.TrimSpace(item.Classification) == runfork.RunForkPendingClassificationDeliveredCompleted {
			add(item.EventID, item.EventName, item.RoutingSource, item.DeliveryRoute)
		}
	}
	keys := make(map[string]struct{}, len(seen))
	for key := range seen {
		keys[key] = struct{}{}
	}
	ordered := sortedSet(keys)
	out := make([]selectedRouteHistoryEvent, 0, len(ordered))
	for _, key := range ordered {
		out = append(out, *seen[key])
	}
	return out
}

func selectedRouteHistoryEvents(routeTable *runtimebus.RouteTable, connectGraph runtimepinrouting.CompiledConnectGraph, events []selectedRouteHistoryEvent) ([]runfork.RunForkSelectedContractRouteEvent, bool) {
	out := make([]runfork.RunForkSelectedContractRouteEvent, 0, len(events))
	incomplete := false
	for _, event := range events {
		if recipients := selectedRouteHistoryStampedRecipients(event.deliveryRoutes); len(recipients) > 0 {
			out = append(out, runfork.RunForkSelectedContractRouteEvent{
				SourceEventID: event.sourceEventID, EventName: event.eventName,
				DerivedRecipients: recipients,
				Disposition:       runfork.RunForkSelectedContractDispositionEvidenceOnly,
			})
			continue
		}
		evaluation := contractFrontierRouteEvaluation(routeTable, connectGraph, event.eventName, event.routingSource)
		eventIncomplete := evaluation.requiresRuntimeResolution
		incomplete = incomplete || eventIncomplete
		disposition := runfork.RunForkSelectedContractDispositionEvidenceOnly
		if eventIncomplete {
			disposition = runfork.RunForkSelectedContractDispositionFailClosed
		}
		recipients := evaluation.recipients
		if !evaluation.connectMatched {
			recipients = contractFrontierRecipients(routeTable.Resolve(event.eventName))
		}
		out = append(out, runfork.RunForkSelectedContractRouteEvent{
			SourceEventID:     event.sourceEventID,
			EventName:         event.eventName,
			DerivedRecipients: recipients,
			Disposition:       disposition,
		})
	}
	return out, incomplete
}

func selectedRouteHistoryStampedRecipients(routes []events.DeliveryRoute) []runfork.RunForkContractFrontierRecipient {
	type recipientKey struct {
		recipient     events.DeliveryRecipient
		path          string
		routeSource   string
		agentIdentity runtimeagentidentity.Identity
	}
	seen := map[recipientKey]runfork.RunForkContractFrontierRecipient{}
	for _, route := range routes {
		recipient, ok := contractFrontierRecipientFromStampedRoute(route)
		if !ok {
			continue
		}
		key := recipientKey{
			recipient:     recipient.Recipient,
			path:          recipient.Path,
			routeSource:   recipient.RouteSourceCode(),
			agentIdentity: recipient.AgentIdentity,
		}
		seen[key] = recipient
	}
	out := make([]runfork.RunForkContractFrontierRecipient, 0, len(seen))
	for _, recipient := range seen {
		out = append(out, recipient)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Recipient.Code() != out[j].Recipient.Code() {
			return out[i].Recipient.Code() < out[j].Recipient.Code()
		}
		if out[i].Recipient.ID() != out[j].Recipient.ID() {
			return out[i].Recipient.ID() < out[j].Recipient.ID()
		}
		return out[i].Path < out[j].Path
	})
	return out
}

func selectedRouteHistoryDynamicFlowInstances(source semanticview.Source, plan runfork.RunForkPlan, frontier runfork.RunForkContractFrontierAdmission) []string {
	seen := map[string]struct{}{}
	add := func(value string) {
		value = strings.Trim(strings.TrimSpace(value), "/")
		if value != "" && isContractFrontierTemplateInstancePath(source, value) {
			seen[value] = struct{}{}
		}
	}
	add(plan.ForkPoint.RoutingSource.Route().FlowInstance)
	for _, item := range plan.PendingWork {
		add(item.RoutingSource.Route().FlowInstance)
	}
	for _, event := range frontier.FrontierEvents {
		for _, flowInstance := range event.SourceFlowInstances {
			add(flowInstance)
		}
	}
	return sortedSet(seen)
}

func selectedRouteHistoryRequiredConsumers() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "selected_source_route_derivation",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       "internal/runtime/bus.DeriveRouteTable",
			Reason:      "route-history admission consumes selected-source route derivation instead of copying source route rows",
		},
		{
			Concept:     "fork_local_recipient_planning",
			Disposition: runfork.RunForkSelectedContractDispositionFutureOwnerRequired,
			Owner:       runfork.RunForkSelectedContractRecipientPlanningOwner,
			Reason:      "executable route reconstruction must feed the canonical recipient-planning owner before delivery rows can be created",
		},
	}
}

func selectedRouteHistoryBlockedSiblings() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "mutating_route_reconstruction",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       runfork.RunForkSelectedContractExecutionOwner + ".route_reconstruction",
			Reason:      "this route admission model is non-mutating and does not persist fork-local route rows",
		},
		{
			Concept:     "dynamic_flow_instance_route_reconstruction",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       "internal/runtime/bus.RouteTable.AddFlowInstanceRoute",
			Reason:      "dynamic flow-instance route reconstruction needs fork-local flow-instance ownership before route persistence",
		},
		{
			Concept:     "recipient_delivery_writes",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       "delivery_and_replay_ownership",
			Reason:      "recipient derivation becomes executable only after a delivery owner approves fork-local delivery writes",
		},
		{
			Concept:     "timer_reconstruction",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "timer reconstruction is scheduler lifecycle history, not route/subscription admission",
		},
	}
}

func selectedRouteHistoryInvalidPaths() []runfork.RunForkSelectedContractExecutionBoundary {
	return []runfork.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "copy_source_routing_rules",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source routing_rules are current operational evidence, not selected-fork route truth",
		},
		{
			Concept:     "copy_source_flow_instance_routes",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source materialized route rows lack selected-fork provenance and must not be copied",
		},
		{
			Concept:     "reuse_source_recipients",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "source recipient decisions were made under the source run and source contracts",
		},
		{
			Concept:     "cli_api_dashboard_owned_routes",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "operator surfaces may consume route admission but must not own selected route reconstruction semantics",
		},
	}
}

func hasUnsupportedBlocker(blockers []runfork.RunForkUnsupportedBlocker, code string) bool {
	code = strings.TrimSpace(code)
	for _, blocker := range blockers {
		if strings.TrimSpace(blocker.Code) == code {
			return true
		}
	}
	return false
}
