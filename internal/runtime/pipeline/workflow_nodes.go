package pipeline

import (
	"fmt"
	"sort"
	"strings"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
)

type WorkflowEventPolicy struct {
	Consume           bool
	RequireVertical   bool
	VisibleDownstream bool
}

type WorkflowNode struct {
	ID               string
	Subscriptions    []events.EventType
	Produces         []events.EventType
	OwnedTransitions []string
	Timers           []string
	ExecutionType    string
	Implementation   string
	StateTable       string
	IdempotencyTable string
	Policies         map[string]WorkflowEventPolicy
}

func DefaultPipelineWorkflowNodes() []WorkflowNode {
	nodes := defaultWorkflowModule().WorkflowNodes()
	out := make([]WorkflowNode, 0, len(nodes))
	for _, node := range nodes {
		nodeCopy := node
		out = append(out, nodeCopy)
	}
	return out
}

func defaultPipelineSubscriptions() []events.EventType {
	return workflowSubscriptions(DefaultPipelineWorkflowNodes())
}

func workflowSubscriptions(nodes []WorkflowNode) []events.EventType {
	seen := make(map[events.EventType]struct{})
	out := make([]events.EventType, 0, 32)
	for _, node := range nodes {
		for _, evt := range node.Subscriptions {
			if _, ok := seen[evt]; ok {
				continue
			}
			seen[evt] = struct{}{}
			out = append(out, evt)
		}
	}
	sort.Slice(out, func(i, j int) bool { return strings.Compare(string(out[i]), string(out[j])) < 0 })
	return out
}

func defaultPipelineEventPolicy(eventType string) (WorkflowEventPolicy, bool) {
	eventType = strings.TrimSpace(eventType)
	return workflowNodeEventPolicy("", eventType)
}

func workflowNodeSubscriptions(nodeID string) []events.EventType {
	nodeID = strings.TrimSpace(nodeID)
	for _, node := range DefaultPipelineWorkflowNodes() {
		if strings.TrimSpace(node.ID) != nodeID {
			continue
		}
		return append([]events.EventType{}, node.Subscriptions...)
	}
	return nil
}

func workflowNodeEventPolicy(nodeID, eventType string) (WorkflowEventPolicy, bool) {
	eventType = strings.TrimSpace(eventType)
	nodeID = strings.TrimSpace(nodeID)
	for _, node := range DefaultPipelineWorkflowNodes() {
		if nodeID != "" && strings.TrimSpace(node.ID) != nodeID {
			continue
		}
		if policy, ok := node.Policies[eventType]; ok {
			return policy, true
		}
	}
	return WorkflowEventPolicy{}, false
}

func LoadWorkflowNodes(bundle *runtimecontracts.WorkflowContractBundle) ([]WorkflowNode, error) {
	if bundle == nil {
		return nil, fmt.Errorf("workflow contract bundle is nil")
	}
	path := bundle.Paths.SystemNodesFile

	wantIDs := workflowRuntimeNodeIDs(bundle)
	out := make([]WorkflowNode, 0, len(wantIDs))
	for _, nodeID := range wantIDs {
		entry, ok := bundle.Nodes[nodeID]
		if !ok {
			return nil, fmt.Errorf("system node %q missing from %s", nodeID, path)
		}
		subscriptions := make([]events.EventType, 0, len(entry.SubscribesTo))
		for _, evt := range entry.SubscribesTo {
			evt = strings.TrimSpace(evt)
			if evt == "" {
				continue
			}
			subscriptions = append(subscriptions, events.EventType(evt))
		}
		produces := make([]events.EventType, 0, len(entry.Produces))
		for _, evt := range entry.Produces {
			evt = strings.TrimSpace(evt)
			if evt == "" {
				continue
			}
			produces = append(produces, events.EventType(evt))
		}
		out = append(out, WorkflowNode{
			ID:               nodeID,
			Subscriptions:    subscriptions,
			Produces:         produces,
			OwnedTransitions: append([]string{}, entry.OwnedTransitions...),
			Timers:           workflowNodeTimerIDs(entry.Timers),
			ExecutionType:    strings.TrimSpace(entry.ExecutionType),
			Implementation:   strings.TrimSpace(entry.Implementation),
			StateTable:       strings.TrimSpace(entry.StateTable),
			IdempotencyTable: strings.TrimSpace(entry.IdempotencyTable),
			Policies:         buildWorkflowNodePolicies(bundle, nodeID, subscriptions),
		})
	}
	return out, nil
}

func workflowRuntimeNodeIDs(bundle *runtimecontracts.WorkflowContractBundle) []string {
	if bundle == nil {
		return nil
	}
	seen := make(map[string]struct{})
	out := make([]string, 0, len(bundle.Nodes))
	for _, transition := range bundle.WorkflowTransitions() {
		nodeID := strings.TrimSpace(transition.Node)
		if nodeID == "" {
			continue
		}
		if _, ok := bundle.Nodes[nodeID]; !ok {
			continue
		}
		if _, ok := seen[nodeID]; ok {
			continue
		}
		seen[nodeID] = struct{}{}
		out = append(out, nodeID)
	}
	for _, entry := range bundle.Events {
		nodeID := strings.TrimSpace(entry.OwningNode)
		if nodeID == "" {
			continue
		}
		if _, ok := bundle.Nodes[nodeID]; !ok {
			continue
		}
		if _, ok := seen[nodeID]; ok {
			continue
		}
		seen[nodeID] = struct{}{}
		out = append(out, nodeID)
	}
	for _, transition := range bundle.DerivedHandlerTransitions() {
		nodeID := strings.TrimSpace(transition.NodeID)
		if nodeID == "" {
			continue
		}
		if _, ok := bundle.Nodes[nodeID]; !ok {
			continue
		}
		if _, ok := seen[nodeID]; ok {
			continue
		}
		if strings.TrimSpace(transition.AdvancesTo) == "" {
			continue
		}
		seen[nodeID] = struct{}{}
		out = append(out, nodeID)
	}
	for nodeID, node := range bundle.Nodes {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" {
			continue
		}
		if _, ok := seen[nodeID]; ok {
			continue
		}
		if len(node.SubscribesTo) == 0 && len(node.OwnedTransitions) == 0 && len(node.Timers) == 0 {
			continue
		}
		seen[nodeID] = struct{}{}
		out = append(out, nodeID)
	}
	sort.Strings(out)
	return out
}

func buildWorkflowNodePolicies(bundle *runtimecontracts.WorkflowContractBundle, nodeID string, subscriptions []events.EventType) map[string]WorkflowEventPolicy {
	allowed := workflowNodeRuntimePolicyEvents(bundle, strings.TrimSpace(nodeID), subscriptions)
	if len(allowed) == 0 {
		return nil
	}
	subscribed := make(map[string]struct{}, len(subscriptions))
	for _, evt := range subscriptions {
		name := strings.TrimSpace(string(evt))
		if name != "" {
			subscribed[name] = struct{}{}
		}
	}
	transitionTriggers := workflowNodeTransitionTriggers(bundle, nodeID)
	policies := make(map[string]WorkflowEventPolicy, len(allowed))
	for eventType := range allowed {
		if _, ok := subscribed[eventType]; !ok {
			continue
		}
		policy := deriveWorkflowEventPolicy(bundle, eventType, transitionTriggers[eventType])
		if override, ok := workflowNodeRuntimePolicyOverride(nodeID, eventType); ok {
			policy = override
		}
		policies[eventType] = policy
	}
	if len(policies) == 0 {
		return nil
	}
	return policies
}

func workflowNodeTimerIDs(timers []runtimecontracts.WorkflowTimerContract) []string {
	if len(timers) == 0 {
		return nil
	}
	out := make([]string, 0, len(timers))
	for _, timer := range timers {
		if id := strings.TrimSpace(timer.ID); id != "" {
			out = append(out, id)
		}
	}
	return out
}

func workflowNodeRuntimePolicyEvents(bundle *runtimecontracts.WorkflowContractBundle, nodeID string, subscriptions []events.EventType) map[string]struct{} {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" || bundle == nil {
		return nil
	}
	out := make(map[string]struct{}, len(subscriptions)+8)
	for _, evt := range subscriptions {
		name := strings.TrimSpace(string(evt))
		if name != "" {
			out[name] = struct{}{}
		}
	}
	for eventType := range bundle.Events {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		for _, owner := range bundle.RuntimeEventOwners(eventType) {
			if strings.TrimSpace(owner) == nodeID {
				out[eventType] = struct{}{}
				break
			}
		}
	}
	for eventType := range bundle.NodeEventHandlers(nodeID) {
		eventType = strings.TrimSpace(eventType)
		if eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	for _, transition := range bundle.WorkflowTransitions() {
		if strings.TrimSpace(transition.Node) != nodeID {
			continue
		}
		trigger := strings.TrimSpace(transition.Trigger)
		if trigger != "" {
			out[trigger] = struct{}{}
		}
	}
	if source, ok := bundle.NodeContractSource(nodeID); ok {
		flowID := strings.TrimSpace(source.FlowID)
		if flowID != "" {
			for _, eventType := range bundle.FlowInputEvents(flowID) {
				eventType = strings.TrimSpace(eventType)
				if eventType != "" {
					out[eventType] = struct{}{}
				}
			}
			for _, eventType := range bundle.FlowOutputEvents(flowID) {
				eventType = strings.TrimSpace(eventType)
				if eventType != "" {
					out[eventType] = struct{}{}
				}
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func workflowNodeRuntimePolicyOverride(nodeID, eventType string) (WorkflowEventPolicy, bool) {
	nodeID = strings.TrimSpace(nodeID)
	eventType = strings.TrimSpace(eventType)
	switch nodeID {
	case "portfolio-node":
		switch eventType {
		case "timer.portfolio_digest", "runtime.reset", "system.directive":
			return WorkflowEventPolicy{Consume: true}, true
		case "budget.threshold_crossed":
			return WorkflowEventPolicy{Consume: false, VisibleDownstream: true}, true
		}
	case "scan-orchestrator":
		switch eventType {
		case "scan.requested":
			return WorkflowEventPolicy{Consume: true}, true
		}
	case "scoring-node":
		switch eventType {
		case "score.dimension_complete", "scoring.contest_resolved":
			return WorkflowEventPolicy{Consume: false, RequireVertical: true, VisibleDownstream: true}, true
		}
	case "validation-orchestrator":
		switch eventType {
		case "spec.validation_passed", "spec.validation_failed":
			return WorkflowEventPolicy{Consume: false, RequireVertical: true, VisibleDownstream: true}, true
		}
	}
	return WorkflowEventPolicy{}, false
}

func workflowNodeTransitionTriggers(bundle *runtimecontracts.WorkflowContractBundle, nodeID string) map[string]bool {
	out := make(map[string]bool)
	if bundle == nil {
		return out
	}
	nodeID = strings.TrimSpace(nodeID)
	for _, transition := range bundle.WorkflowTransitions() {
		if strings.TrimSpace(transition.Node) != nodeID {
			continue
		}
		trigger := strings.TrimSpace(transition.Trigger)
		if trigger != "" {
			out[trigger] = true
		}
	}
	for _, transition := range bundle.DerivedHandlerTransitions() {
		if strings.TrimSpace(transition.NodeID) != nodeID {
			continue
		}
		if strings.TrimSpace(transition.AdvancesTo) == "" {
			continue
		}
		trigger := strings.TrimSpace(transition.EventType)
		if trigger != "" {
			out[trigger] = true
		}
	}
	return out
}

func deriveWorkflowEventPolicy(bundle *runtimecontracts.WorkflowContractBundle, eventType string, drivesTransition bool) WorkflowEventPolicy {
	eventType = strings.TrimSpace(eventType)
	entry, ok := bundle.Events[eventType]
	if !ok {
		return WorkflowEventPolicy{RequireVertical: drivesTransition}
	}
	requireVertical := drivesTransition
	consume, visible := deriveWorkflowEventDelivery(entry)
	return WorkflowEventPolicy{
		Consume:           consume,
		RequireVertical:   requireVertical,
		VisibleDownstream: visible,
	}
}

func deriveWorkflowEventDelivery(entry runtimecontracts.EventCatalogEntry) (consume bool, visible bool) {
	switch strings.TrimSpace(entry.RuntimeHandling) {
	case "consuming":
		return true, false
	case "dual_delivery":
		return false, true
	case "passthrough":
		return false, true
	case "projection", "stage_projection":
		return false, true
	}
	consumerType := strings.TrimSpace(asString(entry.ConsumerType))
	intercepted := truthyContractFlag(entry.Intercepted)
	passthrough := truthyContractFlag(entry.Passthrough)
	if consumerType == "system_component" && intercepted && !passthrough {
		return true, false
	}
	return false, true
}

func truthyContractFlag(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		s := strings.ToLower(strings.TrimSpace(t))
		return s == "true" || s == "conditional" || s == "projection" || s == "consuming"
	default:
		return false
	}
}
