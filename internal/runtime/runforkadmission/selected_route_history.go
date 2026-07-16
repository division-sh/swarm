package runforkadmission

import (
	"fmt"
	"strings"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
)

type SelectedContractRouteHistoryRequest struct {
	Plan              store.RunForkPlan
	Source            semanticview.Source
	ContractSelection store.RunForkContractSelection
	FrontierAdmission store.RunForkContractFrontierAdmission
}

func AdmitSelectedContractRouteHistory(req SelectedContractRouteHistoryRequest) (store.RunForkSelectedContractRouteAdmission, error) {
	if req.Source == nil {
		return store.RunForkSelectedContractRouteAdmission{}, fmt.Errorf("selected route admission requires selected contract semantic source")
	}
	selection := req.ContractSelection
	if strings.TrimSpace(selection.Mode) == "" {
		selection = SelectedContractSelection(req.Source, selection.ContractsRoot)
	}
	if strings.TrimSpace(selection.WorkflowName) == "" {
		selection.WorkflowName = strings.TrimSpace(req.Source.WorkflowName())
	}
	if strings.TrimSpace(selection.WorkflowVersion) == "" {
		selection.WorkflowVersion = strings.TrimSpace(req.Source.WorkflowVersion())
	}
	if strings.TrimSpace(req.FrontierAdmission.Owner) != store.RunForkContractFrontierAdmissionOwner {
		return store.RunForkSelectedContractRouteAdmission{}, fmt.Errorf("selected route admission requires %s frontier admission; got %q", store.RunForkContractFrontierAdmissionOwner, req.FrontierAdmission.Owner)
	}
	if !req.FrontierAdmission.NonMutating {
		return store.RunForkSelectedContractRouteAdmission{}, fmt.Errorf("selected route admission requires non-mutating frontier admission")
	}

	routeTable, err := runtimebus.DeriveRouteTable(req.Source)
	if err != nil {
		return store.RunForkSelectedContractRouteAdmission{}, fmt.Errorf("derive selected route admission routes: %w", err)
	}
	if err := installContractFrontierFlowInstanceRoutes(routeTable, req.Source, req.Plan.PendingWork); err != nil {
		return store.RunForkSelectedContractRouteAdmission{}, err
	}
	connectPlans, connectIssues := runtimepinrouting.LowerCompositionConnectRoutePlans(req.Source)
	if len(connectIssues) != 0 {
		return store.RunForkSelectedContractRouteAdmission{}, fmt.Errorf("derive selected route admission connect routes: %#v", connectIssues)
	}
	routeEvents := selectedRouteHistoryEvents(routeTable, connectPlans, selectedRouteHistoryEventEvidence(req.Plan, req.FrontierAdmission))
	dynamicFlowInstances := selectedRouteHistoryDynamicFlowInstances(req.Source, req.Plan, req.FrontierAdmission)
	blockers := []store.RunForkUnsupportedBlocker{{
		Code:    store.RunForkBlockerSelectedContractRouteAdmissionNonMutating,
		Message: "selected-contract route admission is non-mutating; route persistence, recipient delivery writes, and handler execution remain separately gated",
	}}
	if selectedRouteHistoryHasSourceRouteFacts(req.Plan) {
		blockers = appendRunForkBlocker(blockers, store.RunForkUnsupportedBlocker{
			Code:    store.RunForkBlockerFlowRouteHistoryUnproven,
			Message: "source route rows are current operational state and remain evidence-only until selected route reconstruction is separately approved",
		})
	}
	frontierEventCount, frontierSourceEventIDs, frontierFingerprint := store.RunForkContractFrontierEvidenceBinding(req.FrontierAdmission)

	return store.RunForkSelectedContractRouteAdmission{
		Owner:                          store.RunForkSelectedContractRouteAdmissionOwner,
		FutureRouteReconstructionOwner: store.RunForkSelectedContractExecutionOwner + ".route_reconstruction",
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

func selectedRouteHistoryHasSourceRouteFacts(plan store.RunForkPlan) bool {
	if hasUnsupportedBlocker(plan.UnsupportedBlockers, store.RunForkBlockerFlowRouteHistoryUnproven) {
		return true
	}
	for _, blocker := range plan.ReplayResumeAdmission.UnsupportedBlockers {
		if strings.TrimSpace(blocker.Code) == store.RunForkBlockerFlowRouteHistoryUnproven {
			return true
		}
	}
	for _, disposition := range plan.ReplayResumeAdmission.Dispositions {
		if strings.TrimSpace(disposition.Fact) == store.RunForkReplayResumeFactRouteHistory &&
			strings.TrimSpace(disposition.Disposition) == store.RunForkReplayResumeDispositionFailClosedBlocker {
			return true
		}
	}
	return false
}

type selectedRouteHistoryEvent struct {
	sourceEventID string
	eventName     string
	flowInstance  string
}

func selectedRouteHistoryEventEvidence(plan store.RunForkPlan, frontier store.RunForkContractFrontierAdmission) []selectedRouteHistoryEvent {
	frontierEventIDs := map[string]struct{}{}
	for _, event := range frontier.FrontierEvents {
		if sourceEventID := strings.TrimSpace(event.SourceEventID); sourceEventID != "" {
			frontierEventIDs[sourceEventID] = struct{}{}
		}
	}
	seen := map[string]selectedRouteHistoryEvent{}
	add := func(sourceEventID, eventName, flowInstance string) {
		sourceEventID = strings.TrimSpace(sourceEventID)
		eventName = strings.TrimSpace(eventName)
		flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
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
		seen[key] = selectedRouteHistoryEvent{sourceEventID: sourceEventID, eventName: eventName, flowInstance: flowInstance}
	}
	add(plan.ForkPoint.EventID, plan.ForkPoint.EventName, "")
	for _, item := range plan.PendingWork {
		if strings.TrimSpace(item.Classification) == store.RunForkPendingClassificationDeliveredCompleted {
			add(item.EventID, item.EventName, item.FlowInstance)
		}
	}
	keys := make(map[string]struct{}, len(seen))
	for key := range seen {
		keys[key] = struct{}{}
	}
	ordered := sortedSet(keys)
	out := make([]selectedRouteHistoryEvent, 0, len(ordered))
	for _, key := range ordered {
		out = append(out, seen[key])
	}
	return out
}

func selectedRouteHistoryEvents(routeTable *runtimebus.RouteTable, connectPlans []runtimepinrouting.ConnectRoutePlan, events []selectedRouteHistoryEvent) []store.RunForkSelectedContractRouteEvent {
	out := make([]store.RunForkSelectedContractRouteEvent, 0, len(events))
	for _, event := range events {
		flowInstances := []string(nil)
		if event.flowInstance != "" {
			flowInstances = []string{event.flowInstance}
		}
		routeKeys, connectOwned := contractFrontierRouteKeys(event.eventName, flowInstances, connectPlans)
		out = append(out, store.RunForkSelectedContractRouteEvent{
			SourceEventID:     event.sourceEventID,
			EventName:         event.eventName,
			DerivedRecipients: contractFrontierRecipients(resolveContractFrontierRoutes(routeTable, routeKeys, connectOwned)),
			Disposition:       store.RunForkSelectedContractDispositionEvidenceOnly,
		})
	}
	return out
}

func selectedRouteHistoryDynamicFlowInstances(source semanticview.Source, plan store.RunForkPlan, frontier store.RunForkContractFrontierAdmission) []string {
	seen := map[string]struct{}{}
	add := func(value string) {
		value = strings.Trim(strings.TrimSpace(value), "/")
		if value != "" && isContractFrontierTemplateInstancePath(source, value) {
			seen[value] = struct{}{}
		}
	}
	for _, item := range plan.PendingWork {
		add(item.FlowInstance)
	}
	for _, event := range frontier.FrontierEvents {
		for _, flowInstance := range event.SourceFlowInstances {
			add(flowInstance)
		}
	}
	return sortedSet(seen)
}

func selectedRouteHistoryRequiredConsumers() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "selected_source_route_derivation",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       "internal/runtime/bus.DeriveRouteTable",
			Reason:      "route-history admission consumes selected-source route derivation instead of copying source route rows",
		},
		{
			Concept:     "fork_local_recipient_planning",
			Disposition: store.RunForkSelectedContractDispositionFutureOwnerRequired,
			Owner:       store.RunForkSelectedContractRecipientPlanningOwner,
			Reason:      "executable route reconstruction must feed the canonical recipient-planning owner before delivery rows can be created",
		},
	}
}

func selectedRouteHistoryBlockedSiblings() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "mutating_route_reconstruction",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       store.RunForkSelectedContractExecutionOwner + ".route_reconstruction",
			Reason:      "this route admission model is non-mutating and does not persist fork-local route rows",
		},
		{
			Concept:     "dynamic_flow_instance_route_reconstruction",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       "internal/runtime/bus.RouteTable.AddFlowInstanceRoute",
			Reason:      "dynamic flow-instance route reconstruction needs fork-local flow-instance ownership before route persistence",
		},
		{
			Concept:     "recipient_delivery_writes",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       "delivery_and_replay_ownership",
			Reason:      "recipient derivation becomes executable only after a delivery owner approves fork-local delivery writes",
		},
		{
			Concept:     "timer_reconstruction",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Reason:      "timer reconstruction is scheduler lifecycle history, not route/subscription admission",
		},
	}
}

func selectedRouteHistoryInvalidPaths() []store.RunForkSelectedContractExecutionBoundary {
	return []store.RunForkSelectedContractExecutionBoundary{
		{
			Concept:     "copy_source_routing_rules",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source routing_rules are current operational evidence, not selected-fork route truth",
		},
		{
			Concept:     "copy_source_flow_instance_routes",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source materialized route rows lack selected-fork provenance and must not be copied",
		},
		{
			Concept:     "reuse_source_recipients",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "source recipient decisions were made under the source run and source contracts",
		},
		{
			Concept:     "cli_api_dashboard_owned_routes",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "operator surfaces may consume route admission but must not own selected route reconstruction semantics",
		},
	}
}

func hasUnsupportedBlocker(blockers []store.RunForkUnsupportedBlocker, code string) bool {
	code = strings.TrimSpace(code)
	for _, blocker := range blockers {
		if strings.TrimSpace(blocker.Code) == code {
			return true
		}
	}
	return false
}
