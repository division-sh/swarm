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
	seen := make(map[events.EventType]struct{})
	out := make([]events.EventType, 0, 32)
	for _, node := range DefaultPipelineWorkflowNodes() {
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

	// Current runtime execution still groups scan/discovery and validation-gate
	// behavior under the pipeline-coordinator implementation, while scoring is a
	// separate workflow node. Consume/visibility semantics remain runtime policy.
	wantIDs := []string{"pipeline-coordinator", "scoring-node"}
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
			Timers:           append([]string{}, entry.Timers...),
			ExecutionType:    strings.TrimSpace(entry.ExecutionType),
			Implementation:   strings.TrimSpace(entry.Implementation),
			StateTable:       strings.TrimSpace(entry.StateTable),
			IdempotencyTable: strings.TrimSpace(entry.IdempotencyTable),
			Policies:         buildWorkflowNodePolicies(bundle, nodeID, subscriptions),
		})
	}
	return out, nil
}

func buildWorkflowNodePolicies(bundle *runtimecontracts.WorkflowContractBundle, nodeID string, subscriptions []events.EventType) map[string]WorkflowEventPolicy {
	allowed := workflowNodeRuntimePolicyEvents(strings.TrimSpace(nodeID))
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

func workflowNodeRuntimePolicyEvents(nodeID string) map[string]struct{} {
	switch strings.TrimSpace(nodeID) {
	case "pipeline-coordinator":
		return map[string]struct{}{
			"timer.portfolio_digest":            {},
			"runtime.reset":                     {},
			"scan.requested":                    {},
			"category.assessed":                 {},
			"trend.identified":                  {},
			"source.scraped":                    {},
			"market_research.scan_complete":     {},
			"trend_research.scan_complete":      {},
			"scanner.google_maps.scan_complete": {},
			"scanner.instagram.scan_complete":   {},
			"scanner.reviews.scan_complete":     {},
			"scanner.directories.scan_complete": {},
			"scanner.yelp.scan_complete":        {},
			"dedup.resolved":                    {},
			"synthesis.resolved":                {},
			"vertical.shortlisted":              {},
			"research.completed":                {},
			"research.vertical_rejected":        {},
			"spec.revision_requested":           {},
			"spec.approved":                     {},
			"spec.validation_passed":            {},
			"spec.validation_failed":            {},
			"cto.spec_approved":                 {},
			"cto.spec_revision_needed":          {},
			"cto.spec_vetoed":                   {},
			"brand.candidates_ready":            {},
			"vertical.ready_for_review":         {},
			"vertical.needs_more_data":          {},
			"brand.revision_needed":             {},
			"vertical.approved":                 {},
			"vertical.killed":                   {},
			"vertical.resumed":                  {},
			"opco.ceo_ready":                    {},
		}
	case "scoring-node":
		return map[string]struct{}{
			"vertical.discovered":      {},
			"vertical.derived":         {},
			"score.dimension_complete": {},
			"scoring.contest_resolved": {},
			"vertical.scored":          {},
		}
	default:
		return nil
	}
}

func workflowNodeRuntimePolicyOverride(nodeID, eventType string) (WorkflowEventPolicy, bool) {
	switch strings.TrimSpace(nodeID) {
	case "pipeline-coordinator":
		switch strings.TrimSpace(eventType) {
		case "timer.portfolio_digest":
			return WorkflowEventPolicy{Consume: true}, true
		case "runtime.reset":
			return WorkflowEventPolicy{Consume: false}, true
		case "spec.revision_requested":
			return WorkflowEventPolicy{Consume: false, RequireVertical: true, VisibleDownstream: true}, true
		case "spec.validation_passed", "spec.validation_failed":
			return WorkflowEventPolicy{Consume: true, RequireVertical: true}, true
		case "brand.revision_needed":
			return WorkflowEventPolicy{Consume: true, RequireVertical: true}, true
		}
	case "scoring-node":
		switch strings.TrimSpace(eventType) {
		case "vertical.derived":
			return WorkflowEventPolicy{Consume: true}, true
		case "score.dimension_complete", "scoring.contest_resolved":
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
	for _, transition := range bundle.Workflow.Workflow.Transitions {
		if strings.TrimSpace(transition.Node) != nodeID {
			continue
		}
		trigger := strings.TrimSpace(transition.Trigger)
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
