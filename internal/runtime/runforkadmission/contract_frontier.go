package runforkadmission

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
)

type ContractFrontierRequest struct {
	Plan              store.RunForkPlan
	Source            semanticview.Source
	ContractSelection store.RunForkContractSelection
}

func SelectedContractSelection(source semanticview.Source, contractsRoot string) store.RunForkContractSelection {
	selection := store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: strings.TrimSpace(contractsRoot),
	}
	if source != nil {
		selection.WorkflowName = strings.TrimSpace(source.WorkflowName())
		selection.WorkflowVersion = strings.TrimSpace(source.WorkflowVersion())
	}
	return selection
}

func AdmitContractFrontier(req ContractFrontierRequest) (store.RunForkContractFrontierAdmission, error) {
	if req.Source == nil {
		return store.RunForkContractFrontierAdmission{}, fmt.Errorf("selected contract semantic source is required")
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

	routeTable, err := runtimebus.DeriveRouteTable(req.Source)
	if err != nil {
		return store.RunForkContractFrontierAdmission{}, fmt.Errorf("derive selected-contract fork routes: %w", err)
	}
	if err := installContractFrontierFlowInstanceRoutes(routeTable, req.Source, req.Plan.PendingWork); err != nil {
		return store.RunForkContractFrontierAdmission{}, err
	}
	workflowNodes, err := runtimepipeline.LoadWorkflowNodes(req.Source)
	if err != nil {
		return store.RunForkContractFrontierAdmission{}, fmt.Errorf("derive selected-contract workflow nodes: %w", err)
	}
	connectPlans, connectIssues := runtimepinrouting.LowerCompositionConnectRoutePlans(req.Source)
	if len(connectIssues) != 0 {
		return store.RunForkContractFrontierAdmission{}, fmt.Errorf("derive selected-contract connect routes: %#v", connectIssues)
	}
	frontier, lineageOnly := runForkFrontierEvents(req.Plan.PendingWork)
	for i := range frontier {
		eventName := frontier[i].EventName
		sourceRoute := contractFrontierSourceRoute(req.Plan.PendingWork, frontier[i].SourceEventID)
		routeKeys, connectOwned := contractFrontierRouteKeys(eventName, sourceRoute, connectPlans)
		frontier[i].RuntimeEventOwners = sortedUnique(req.Source.RuntimeEventOwners(eventName))
		frontier[i].WorkflowNodeSubscribers = workflowNodeSubscribers(workflowNodes, routeKeys...)
		frontier[i].DerivedRecipients = contractFrontierRecipients(resolveContractFrontierRoutes(routeTable, routeKeys, connectOwned))
	}
	sort.Slice(frontier, func(i, j int) bool {
		if frontier[i].EventName != frontier[j].EventName {
			return frontier[i].EventName < frontier[j].EventName
		}
		return frontier[i].SourceEventID < frontier[j].SourceEventID
	})

	blockers := []store.RunForkUnsupportedBlocker{}
	if len(frontier) > 0 {
		blockers = appendRunForkBlocker(blockers, store.RunForkUnsupportedBlocker{
			Code:    store.RunForkBlockerContractFrontierExecutionUnsupported,
			Message: "selected-contract frontier admission is non-mutating; handler execution and fork-local delivery writes remain separately gated",
		})
	}
	for _, event := range frontier {
		if len(event.DerivedRecipients) > 0 || len(event.RuntimeEventOwners) > 0 || len(event.WorkflowNodeSubscribers) > 0 {
			continue
		}
		blockers = appendRunForkBlocker(blockers, store.RunForkUnsupportedBlocker{
			Code:    store.RunForkBlockerContractFrontierRouteUnresolved,
			Message: "selected-contract frontier event has no derived route, workflow subscriber, or runtime event owner",
		})
	}

	return store.RunForkContractFrontierAdmission{
		Owner:                        store.RunForkContractFrontierAdmissionOwner,
		ContractSelection:            selection,
		NonMutating:                  true,
		HistoricalExecutionSupported: false,
		FrontierEventCount:           len(frontier),
		FrontierEvents:               frontier,
		LineageOnlyEvents:            lineageOnly,
		UnsupportedBlockers:          blockers,
	}, nil
}

func runForkFrontierEvents(pending []store.RunForkPendingWork) ([]store.RunForkContractFrontierEvent, []store.RunForkContractFrontierLineageEvent) {
	type aggregate struct {
		event           store.RunForkContractFrontierEvent
		classifications map[string]struct{}
		flowInstances   map[string]struct{}
		subscriberTypes map[string]struct{}
		subscriberIDs   map[string]struct{}
	}
	type lineageAggregate struct {
		event           store.RunForkContractFrontierLineageEvent
		classifications map[string]struct{}
		flowInstances   map[string]struct{}
		subscriberTypes map[string]struct{}
		subscriberIDs   map[string]struct{}
	}
	byEvent := map[string]*aggregate{}
	lineageByEvent := map[string]*lineageAggregate{}
	for _, item := range pending {
		switch strings.TrimSpace(item.Classification) {
		case store.RunForkPendingClassificationDeliveredCompleted, store.RunForkPendingClassificationCommittedReplay:
			continue
		}
		eventID := strings.TrimSpace(item.EventID)
		if eventID == "" {
			continue
		}
		if store.RunForkSelectedContractDiagnosticPlatformOutcomePolicyApplies(item) {
			agg := lineageByEvent[eventID]
			if agg == nil {
				agg = &lineageAggregate{
					event: store.RunForkContractFrontierLineageEvent{
						SourceEventID: eventID,
						EventName:     strings.TrimSpace(item.EventName),
						Owner:         store.RunForkSelectedContractDiagnosticPlatformOutcomePolicyOwner,
						Disposition:   store.RunForkContractFrontierDispositionLineageNoAction,
						Reason:        "spec-declared diagnostic platform outcome facts are persisted for lineage and are not selected-contract frontier work",
					},
					classifications: map[string]struct{}{},
					flowInstances:   map[string]struct{}{},
					subscriberTypes: map[string]struct{}{},
					subscriberIDs:   map[string]struct{}{},
				}
				lineageByEvent[eventID] = agg
			}
			addString(agg.classifications, item.Classification)
			addString(agg.flowInstances, item.SourceRoute.Normalized().FlowInstance)
			addString(agg.subscriberTypes, item.SubscriberType)
			addString(agg.subscriberIDs, item.SubscriberID)
			continue
		}
		agg := byEvent[eventID]
		if agg == nil {
			agg = &aggregate{
				event: store.RunForkContractFrontierEvent{
					SourceEventID: eventID,
					EventName:     strings.TrimSpace(item.EventName),
				},
				classifications: map[string]struct{}{},
				flowInstances:   map[string]struct{}{},
				subscriberTypes: map[string]struct{}{},
				subscriberIDs:   map[string]struct{}{},
			}
			byEvent[eventID] = agg
		}
		addString(agg.classifications, item.Classification)
		addString(agg.flowInstances, item.SourceRoute.Normalized().FlowInstance)
		addString(agg.subscriberTypes, item.SubscriberType)
		addString(agg.subscriberIDs, item.SubscriberID)
	}
	out := make([]store.RunForkContractFrontierEvent, 0, len(byEvent))
	for _, agg := range byEvent {
		agg.event.SourceClassifications = sortedSet(agg.classifications)
		agg.event.SourceFlowInstances = sortedSet(agg.flowInstances)
		agg.event.SourceSubscriberTypes = sortedSet(agg.subscriberTypes)
		agg.event.SourceSubscriberIDs = sortedSet(agg.subscriberIDs)
		out = append(out, agg.event)
	}
	lineage := make([]store.RunForkContractFrontierLineageEvent, 0, len(lineageByEvent))
	for _, agg := range lineageByEvent {
		agg.event.SourceClassifications = sortedSet(agg.classifications)
		agg.event.SourceFlowInstances = sortedSet(agg.flowInstances)
		agg.event.SourceSubscriberTypes = sortedSet(agg.subscriberTypes)
		agg.event.SourceSubscriberIDs = sortedSet(agg.subscriberIDs)
		lineage = append(lineage, agg.event)
	}
	sort.Slice(lineage, func(i, j int) bool {
		if lineage[i].EventName != lineage[j].EventName {
			return lineage[i].EventName < lineage[j].EventName
		}
		return lineage[i].SourceEventID < lineage[j].SourceEventID
	})
	return out, lineage
}

func installContractFrontierFlowInstanceRoutes(routeTable *runtimebus.RouteTable, source semanticview.Source, pending []store.RunForkPendingWork) error {
	for _, route := range contractFrontierFlowInstanceRoutes(source, pending) {
		if err := routeTable.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: route}); err != nil {
			return fmt.Errorf("derive selected-contract flow-instance route %s: %w", route.InstancePath, err)
		}
	}
	return nil
}

func contractFrontierFlowInstanceRoutes(source semanticview.Source, pending []store.RunForkPendingWork) []runtimeflowidentity.Route {
	seen := map[string]struct{}{}
	out := make([]runtimeflowidentity.Route, 0)
	for _, item := range pending {
		for _, instancePath := range contractFrontierFlowInstances(source, item) {
			route := runtimeflowidentity.StoredRoute("", "", instancePath)
			if !route.Valid() {
				continue
			}
			key := strings.Join([]string{route.ScopeKey, route.InstanceID, route.InstancePath}, "\x00")
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, route)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.Join([]string{out[i].ScopeKey, out[i].InstanceID, out[i].InstancePath}, "\x00") <
			strings.Join([]string{out[j].ScopeKey, out[j].InstanceID, out[j].InstancePath}, "\x00")
	})
	return out
}

func contractFrontierFlowInstances(source semanticview.Source, item store.RunForkPendingWork) []string {
	instancePath := item.SourceRoute.Normalized().FlowInstance
	if isContractFrontierTemplateInstancePath(source, instancePath) {
		return []string{instancePath}
	}
	return nil
}

func isContractFrontierTemplateInstancePath(source semanticview.Source, instancePath string) bool {
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	if source == nil || instancePath == "" {
		return false
	}
	for _, scope := range source.FlowScopes() {
		if !strings.EqualFold(strings.TrimSpace(scope.Mode), "template") {
			continue
		}
		scopePath := strings.Trim(strings.TrimSpace(scope.Path), "/")
		if scopePath == "" || instancePath == scopePath {
			continue
		}
		if strings.HasPrefix(instancePath, scopePath+"/") {
			return true
		}
	}
	return false
}

func contractFrontierSourceRoute(pending []store.RunForkPendingWork, eventID string) events.RouteIdentity {
	eventID = strings.TrimSpace(eventID)
	for _, item := range pending {
		if strings.TrimSpace(item.EventID) == eventID {
			return item.SourceRoute.Normalized()
		}
	}
	return events.RouteIdentity{}
}

func contractFrontierRouteKeys(eventName string, sourceRoute events.RouteIdentity, plans []runtimepinrouting.ConnectRoutePlan) ([]string, bool) {
	eventName = strings.Trim(strings.TrimSpace(eventName), "/")
	if eventName == "" {
		return nil, false
	}
	matchesSource := func(endpoint runtimepinrouting.ConnectRoutePlanEndpoint) bool {
		return runtimepinrouting.ConnectSourceEndpointMatches(endpoint, eventName, sourceRoute)
	}
	matched := false
	receiverEvents := map[string]struct{}{}
	for _, plan := range plans {
		if !matchesSource(plan.Source) {
			continue
		}
		matched = true
		if plan.RequiresRuntimeResolution {
			continue
		}
		local := strings.Trim(strings.TrimSpace(plan.Receiver.Event), "/")
		receiverPath := strings.Trim(strings.TrimSpace(plan.Receiver.FlowPath), "/")
		if receiverPath == "" {
			receiverPath = strings.Trim(strings.TrimSpace(plan.Receiver.FlowID), "/")
		}
		addString(receiverEvents, plan.Receiver.ResolvedEvent)
		if receiverPath != "" && local != "" {
			addString(receiverEvents, receiverPath+"/"+local)
		}
		if target := plan.Target.Normalized(); target.FlowInstance != "" && local != "" {
			addString(receiverEvents, target.FlowInstance+"/"+local)
		}
	}
	if matched {
		return sortedSet(receiverEvents), true
	}
	return []string{eventName}, false
}

func resolveContractFrontierRoutes(routeTable *runtimebus.RouteTable, eventNames []string, connectOwned bool) []runtimebus.Subscriber {
	var out []runtimebus.Subscriber
	for _, eventName := range eventNames {
		for _, subscriber := range routeTable.Resolve(eventName) {
			if connectOwned && strings.TrimSpace(subscriber.RouteSource) != "receiver_carrier" {
				continue
			}
			out = append(out, subscriber)
		}
	}
	return out
}

func workflowNodeSubscribers(nodes []runtimepipeline.WorkflowNode, eventNames ...string) []string {
	wanted := map[string]struct{}{}
	for _, eventName := range eventNames {
		addString(wanted, eventName)
	}
	if len(wanted) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	for _, node := range nodes {
		for _, subscription := range node.Subscriptions {
			if _, ok := wanted[strings.TrimSpace(string(subscription))]; !ok {
				continue
			}
			addString(seen, node.ID)
		}
	}
	return sortedSet(seen)
}

func contractFrontierRecipients(in []runtimebus.Subscriber) []store.RunForkContractFrontierRecipient {
	out := make([]store.RunForkContractFrontierRecipient, 0, len(in))
	seen := map[string]struct{}{}
	for _, subscriber := range in {
		recipient := store.RunForkContractFrontierRecipient{
			SubscriberType: strings.TrimSpace(subscriber.Type),
			SubscriberID:   strings.TrimSpace(subscriber.ID),
			Path:           strings.TrimSpace(subscriber.Path),
			RouteSource:    strings.TrimSpace(subscriber.RouteSource),
		}
		if recipient.SubscriberID == "" || recipient.SubscriberType == "" {
			continue
		}
		key := strings.Join([]string{recipient.SubscriberType, recipient.SubscriberID, recipient.Path, recipient.RouteSource}, "\x00")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, recipient)
	}
	sort.Slice(out, func(i, j int) bool {
		left := strings.Join([]string{out[i].SubscriberType, out[i].SubscriberID, out[i].Path, out[i].RouteSource}, "\x00")
		right := strings.Join([]string{out[j].SubscriberType, out[j].SubscriberID, out[j].Path, out[j].RouteSource}, "\x00")
		return left < right
	})
	return out
}

func appendRunForkBlocker(blockers []store.RunForkUnsupportedBlocker, blocker store.RunForkUnsupportedBlocker) []store.RunForkUnsupportedBlocker {
	code := strings.TrimSpace(blocker.Code)
	if code == "" {
		return blockers
	}
	for _, existing := range blockers {
		if strings.TrimSpace(existing.Code) == code {
			return blockers
		}
	}
	blocker.Code = code
	blocker.Message = strings.TrimSpace(blocker.Message)
	return append(blockers, blocker)
}

func sortedUnique(values []string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		addString(seen, value)
	}
	return sortedSet(seen)
}

func sortedSet(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func addString(values map[string]struct{}, value string) {
	value = strings.TrimSpace(value)
	if value != "" {
		values[value] = struct{}{}
	}
}
