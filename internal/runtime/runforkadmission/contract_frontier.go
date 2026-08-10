package runforkadmission

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type ContractFrontierRequest struct {
	Plan              runfork.RunForkPlan
	Source            semanticview.Source
	ContractSelection runfork.RunForkContractSelection
}

func SelectedContractSelection(source semanticview.Source, contractsRoot string) runfork.RunForkContractSelection {
	selection := runfork.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: strings.TrimSpace(contractsRoot),
	}
	if source != nil {
		selection.WorkflowName = strings.TrimSpace(source.WorkflowName())
		selection.WorkflowVersion = strings.TrimSpace(source.WorkflowVersion())
	}
	return selection
}

func AdmitContractFrontier(req ContractFrontierRequest) (runfork.RunForkContractFrontierAdmission, error) {
	if req.Source == nil {
		return runfork.RunForkContractFrontierAdmission{}, fmt.Errorf("selected contract semantic source is required")
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
		return runfork.RunForkContractFrontierAdmission{}, fmt.Errorf("derive selected-contract fork routes: %w", err)
	}
	if err := installContractFrontierFlowInstanceRoutes(routeTable, req.Source, req.Plan.PendingWork); err != nil {
		return runfork.RunForkContractFrontierAdmission{}, err
	}
	workflowNodes, err := runtimepipeline.LoadWorkflowNodes(req.Source)
	if err != nil {
		return runfork.RunForkContractFrontierAdmission{}, fmt.Errorf("derive selected-contract workflow nodes: %w", err)
	}
	connectGraph := runtimepinrouting.CompileConnectGraph(req.Source)
	connectIssues := connectGraph.Issues()
	if len(connectIssues) != 0 {
		return runfork.RunForkContractFrontierAdmission{}, fmt.Errorf("derive selected-contract connect routes: %#v", connectIssues)
	}
	frontier, lineageOnly := runForkFrontierEvents(req.Plan.PendingWork)
	incompleteRoutes := map[string]bool{}
	for i := range frontier {
		eventName := frontier[i].EventName
		runtimeOwners := append([]string(nil), req.Source.RuntimeEventOwners(eventName)...)
		if owner, ok := runfork.RunForkSelectedContractPlatformRuntimeOwner(eventName); ok {
			runtimeOwners = append(runtimeOwners, owner)
		}
		if len(frontier[i].DerivedRecipients) > 0 {
			frontier[i].RuntimeEventOwners = sortedUnique(runtimeOwners)
			continue
		}
		source := contractFrontierRoutingSource(req.Plan.PendingWork, frontier[i].SourceEventID)
		evaluation := contractFrontierRouteEvaluation(routeTable, eventName, source)
		incompleteRoutes[frontier[i].SourceEventID] = incompleteRoutes[frontier[i].SourceEventID] || evaluation.requiresRuntimeResolution
		frontier[i].RuntimeEventOwners = sortedUnique(runtimeOwners)
		if evaluation.connectMatched {
			frontier[i].WorkflowNodeSubscribers = evaluation.nodeIDs
			frontier[i].DerivedRecipients = evaluation.recipients
		} else {
			frontier[i].WorkflowNodeSubscribers = workflowNodeSubscribers(workflowNodes, eventName)
			frontier[i].DerivedRecipients = contractFrontierRecipients(routeTable.Resolve(eventName))
		}
	}
	sort.Slice(frontier, func(i, j int) bool {
		if frontier[i].EventName != frontier[j].EventName {
			return frontier[i].EventName < frontier[j].EventName
		}
		return frontier[i].SourceEventID < frontier[j].SourceEventID
	})

	blockers := []runfork.RunForkUnsupportedBlocker{}
	if len(frontier) > 0 {
		blockers = appendRunForkBlocker(blockers, runfork.RunForkUnsupportedBlocker{
			Code:    runfork.RunForkBlockerContractFrontierExecutionUnsupported,
			Message: "selected-contract frontier admission is non-mutating; handler execution and fork-local delivery writes remain separately gated",
		})
	}
	for _, event := range frontier {
		if incompleteRoutes[event.SourceEventID] {
			blockers = appendRunForkBlocker(blockers, runfork.RunForkUnsupportedBlocker{
				Code:    runfork.RunForkBlockerContractFrontierRouteUnresolved,
				Message: "selected-contract frontier has a matched connect receiver that still requires runtime resolution",
			})
			continue
		}
		if len(event.DerivedRecipients) > 0 || len(event.RuntimeEventOwners) > 0 || len(event.WorkflowNodeSubscribers) > 0 {
			continue
		}
		blockers = appendRunForkBlocker(blockers, runfork.RunForkUnsupportedBlocker{
			Code:    runfork.RunForkBlockerContractFrontierRouteUnresolved,
			Message: "selected-contract frontier event has no derived route, workflow subscriber, or runtime event owner",
		})
	}

	return runfork.RunForkContractFrontierAdmission{
		Owner:                        runfork.RunForkContractFrontierAdmissionOwner,
		ContractSelection:            selection,
		NonMutating:                  true,
		HistoricalExecutionSupported: false,
		FrontierEventCount:           len(frontier),
		FrontierEvents:               frontier,
		LineageOnlyEvents:            lineageOnly,
		UnsupportedBlockers:          blockers,
	}, nil
}

func runForkFrontierEvents(pending []runfork.RunForkPendingWork) ([]runfork.RunForkContractFrontierEvent, []runfork.RunForkContractFrontierLineageEvent) {
	type aggregate struct {
		event             runfork.RunForkContractFrontierEvent
		classifications   map[string]struct{}
		flowInstances     map[string]struct{}
		subscriberTypes   map[string]struct{}
		subscriberIDs     map[string]struct{}
		stampedRecipients map[contractFrontierRecipientIdentity]runfork.RunForkContractFrontierRecipient
		stampedNodes      map[string]struct{}
	}
	type lineageAggregate struct {
		event           runfork.RunForkContractFrontierLineageEvent
		classifications map[string]struct{}
		flowInstances   map[string]struct{}
		subscriberTypes map[string]struct{}
		subscriberIDs   map[string]struct{}
	}
	byEvent := map[string]*aggregate{}
	lineageByEvent := map[string]*lineageAggregate{}
	for _, item := range pending {
		switch strings.TrimSpace(item.Classification) {
		case runfork.RunForkPendingClassificationDeliveredCompleted, runfork.RunForkPendingClassificationCommittedReplay:
			continue
		}
		eventID := strings.TrimSpace(item.EventID)
		if eventID == "" {
			continue
		}
		if runfork.RunForkSelectedContractDiagnosticPlatformOutcomePolicyApplies(item) {
			agg := lineageByEvent[eventID]
			if agg == nil {
				agg = &lineageAggregate{
					event: runfork.RunForkContractFrontierLineageEvent{
						SourceEventID: eventID,
						EventName:     strings.TrimSpace(item.EventName),
						Owner:         runfork.RunForkSelectedContractDiagnosticPlatformOutcomePolicyOwner,
						Disposition:   runfork.RunForkContractFrontierDispositionLineageNoAction,
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
			addString(agg.flowInstances, item.RoutingSource.Route().FlowInstance)
			addString(agg.subscriberTypes, item.SubscriberType)
			addString(agg.subscriberIDs, item.SubscriberID)
			continue
		}
		agg := byEvent[eventID]
		if agg == nil {
			agg = &aggregate{
				event: runfork.RunForkContractFrontierEvent{
					SourceEventID: eventID,
					EventName:     strings.TrimSpace(item.EventName),
				},
				classifications:   map[string]struct{}{},
				flowInstances:     map[string]struct{}{},
				subscriberTypes:   map[string]struct{}{},
				subscriberIDs:     map[string]struct{}{},
				stampedRecipients: map[contractFrontierRecipientIdentity]runfork.RunForkContractFrontierRecipient{},
				stampedNodes:      map[string]struct{}{},
			}
			byEvent[eventID] = agg
		}
		addString(agg.classifications, item.Classification)
		addString(agg.flowInstances, item.RoutingSource.Route().FlowInstance)
		addString(agg.subscriberTypes, item.SubscriberType)
		addString(agg.subscriberIDs, item.SubscriberID)
		if !item.DeliveryRoute.ConnectClaim.Empty() {
			if recipient, ok := contractFrontierRecipientFromStampedRoute(item.DeliveryRoute); ok {
				key := contractFrontierRecipientKey(recipient)
				agg.stampedRecipients[key] = recipient
				if recipient.Recipient.IsNode() {
					agg.stampedNodes[recipient.Recipient.ID()] = struct{}{}
				}
			}
		}
	}
	out := make([]runfork.RunForkContractFrontierEvent, 0, len(byEvent))
	for _, agg := range byEvent {
		agg.event.SourceClassifications = sortedSet(agg.classifications)
		agg.event.SourceFlowInstances = sortedSet(agg.flowInstances)
		agg.event.SourceSubscriberTypes = sortedSet(agg.subscriberTypes)
		agg.event.SourceSubscriberIDs = sortedSet(agg.subscriberIDs)
		for _, recipient := range agg.stampedRecipients {
			agg.event.DerivedRecipients = append(agg.event.DerivedRecipients, recipient)
		}
		sort.Slice(agg.event.DerivedRecipients, func(i, j int) bool {
			left := agg.event.DerivedRecipients[i]
			right := agg.event.DerivedRecipients[j]
			return contractFrontierRecipientLess(left, right)
		})
		agg.event.WorkflowNodeSubscribers = sortedSet(agg.stampedNodes)
		out = append(out, agg.event)
	}
	lineage := make([]runfork.RunForkContractFrontierLineageEvent, 0, len(lineageByEvent))
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

func contractFrontierRecipientFromStampedRoute(route events.DeliveryRoute) (runfork.RunForkContractFrontierRecipient, bool) {
	route = route.Normalized()
	if route.ConnectClaim.Empty() || route.Recipient.Empty() {
		return runfork.RunForkContractFrontierRecipient{}, false
	}
	return runfork.NewRunForkContractFrontierRecipient(
		route.Recipient, route.Target.Route().FlowInstance, "stamped_connect_claim", route.AgentIdentity,
	), true
}

func installContractFrontierFlowInstanceRoutes(routeTable *runtimebus.RouteTable, source semanticview.Source, pending []runfork.RunForkPendingWork) error {
	for _, route := range contractFrontierFlowInstanceRoutes(source, pending) {
		if err := routeTable.AddFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: route}); err != nil {
			return fmt.Errorf("derive selected-contract flow-instance route %s: %w", route.InstancePath, err)
		}
	}
	return nil
}

func contractFrontierFlowInstanceRoutes(source semanticview.Source, pending []runfork.RunForkPendingWork) []runtimeflowidentity.Route {
	seen := map[runtimeflowidentity.Route]struct{}{}
	out := make([]runtimeflowidentity.Route, 0)
	for _, item := range pending {
		for _, instancePath := range contractFrontierFlowInstances(source, item) {
			route := runtimeflowidentity.StoredRoute("", "", instancePath)
			if !route.Valid() {
				continue
			}
			if _, ok := seen[route]; ok {
				continue
			}
			seen[route] = struct{}{}
			out = append(out, route)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ScopeKey != out[j].ScopeKey {
			return out[i].ScopeKey < out[j].ScopeKey
		}
		if out[i].InstanceID != out[j].InstanceID {
			return out[i].InstanceID < out[j].InstanceID
		}
		return out[i].InstancePath < out[j].InstancePath
	})
	return out
}

func contractFrontierFlowInstances(source semanticview.Source, item runfork.RunForkPendingWork) []string {
	instancePath := item.RoutingSource.Route().FlowInstance
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

func contractFrontierRoutingSource(pending []runfork.RunForkPendingWork, eventID string) events.RoutingSource {
	eventID = strings.TrimSpace(eventID)
	for _, item := range pending {
		if strings.TrimSpace(item.EventID) == eventID {
			return item.RoutingSource
		}
	}
	return events.NoRoutingSource()
}

type contractFrontierEvaluatedRoute struct {
	connectMatched            bool
	requiresRuntimeResolution bool
	recipients                []runfork.RunForkContractFrontierRecipient
	nodeIDs                   []string
}

func contractFrontierRouteEvaluation(routeTable *runtimebus.RouteTable, eventName string, source events.RoutingSource) contractFrontierEvaluatedRoute {
	eventName = strings.Trim(strings.TrimSpace(eventName), "/")
	if routeTable == nil || eventName == "" {
		return contractFrontierEvaluatedRoute{}
	}
	sourceEvent, err := runtimepinrouting.AdmitSourceEvent(events.EventType(eventName), source)
	if err != nil {
		return contractFrontierEvaluatedRoute{}
	}
	evaluation := routeTable.EvaluateConnectSource(sourceEvent)
	if !evaluation.Matched() {
		return contractFrontierEvaluatedRoute{}
	}
	out := contractFrontierEvaluatedRoute{
		connectMatched:            true,
		requiresRuntimeResolution: evaluation.RequiresRuntimeResolution(),
	}
	seenNodes := map[string]struct{}{}
	seenRecipients := map[contractFrontierRecipientIdentity]struct{}{}
	for _, recipient := range evaluation.Recipients() {
		var typedRecipient events.DeliveryRecipient
		if recipient.Kind() == runtimepinrouting.ConnectRecipientAgent {
			typedRecipient = events.MustAgentDeliveryRecipient(recipient.ID())
		} else {
			typedRecipient = events.MustNodeDeliveryRecipient(recipient.ID())
			seenNodes[recipient.ID()] = struct{}{}
		}
		projected := runfork.NewRunForkContractFrontierRecipient(
			typedRecipient, recipient.Path(), "connect_route_plan", recipient.AgentIdentity(),
		)
		key := contractFrontierRecipientIdentity{
			recipient:     projected.Recipient,
			path:          projected.Path,
			routeSource:   projected.RouteSourceCode(),
			agentIdentity: projected.AgentIdentity,
		}
		if _, exists := seenRecipients[key]; exists {
			continue
		}
		seenRecipients[key] = struct{}{}
		out.recipients = append(out.recipients, projected)
	}
	out.nodeIDs = sortedSet(seenNodes)
	sort.Slice(out.recipients, func(i, j int) bool {
		return contractFrontierRecipientLess(out.recipients[i], out.recipients[j])
	})
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

func contractFrontierRecipients(in []runtimebus.Subscriber) []runfork.RunForkContractFrontierRecipient {
	out := make([]runfork.RunForkContractFrontierRecipient, 0, len(in))
	seen := map[contractFrontierRecipientIdentity]struct{}{}
	for _, subscriber := range in {
		recipient := runfork.NewRunForkContractFrontierRecipient(
			subscriber.Recipient, subscriber.Path, subscriber.RouteSourceCode(), subscriber.AgentIdentity,
		)
		if recipient.Recipient.Empty() {
			continue
		}
		key := contractFrontierRecipientKey(recipient)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, recipient)
	}
	sort.Slice(out, func(i, j int) bool {
		return contractFrontierRecipientLess(out[i], out[j])
	})
	return out
}

type contractFrontierRecipientIdentity struct {
	recipient     events.DeliveryRecipient
	path          string
	routeSource   string
	agentIdentity runtimeagentidentity.Identity
}

func contractFrontierRecipientKey(recipient runfork.RunForkContractFrontierRecipient) contractFrontierRecipientIdentity {
	return contractFrontierRecipientIdentity{
		recipient:     recipient.Recipient,
		path:          recipient.Path,
		routeSource:   recipient.RouteSourceCode(),
		agentIdentity: recipient.AgentIdentity,
	}
}

func contractFrontierRecipientLess(left, right runfork.RunForkContractFrontierRecipient) bool {
	if left.Recipient.Code() != right.Recipient.Code() {
		return left.Recipient.Code() < right.Recipient.Code()
	}
	if left.Recipient.ID() != right.Recipient.ID() {
		return left.Recipient.ID() < right.Recipient.ID()
	}
	if left.Path != right.Path {
		return left.Path < right.Path
	}
	return left.RouteSourceCode() < right.RouteSourceCode()
}

func appendRunForkBlocker(blockers []runfork.RunForkUnsupportedBlocker, blocker runfork.RunForkUnsupportedBlocker) []runfork.RunForkUnsupportedBlocker {
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
