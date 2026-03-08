package pipeline

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"empireai/internal/events"
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

var (
	empireWorkflowNodesOnce sync.Once
	empireWorkflowNodes     []WorkflowNode
	empireWorkflowNodesErr  error
)

func empirePipelineWorkflowNodes() []WorkflowNode {
	empireWorkflowNodesOnce.Do(func() {
		empireWorkflowNodes, empireWorkflowNodesErr = loadWorkflowNodesFromContracts()
	})
	if empireWorkflowNodesErr != nil {
		panic(empireWorkflowNodesErr)
	}
	out := make([]WorkflowNode, 0, len(empireWorkflowNodes))
	for _, node := range empireWorkflowNodes {
		nodeCopy := node
		out = append(out, nodeCopy)
	}
	return out
}

func empirePipelineSubscriptions() []events.EventType {
	seen := make(map[events.EventType]struct{})
	out := make([]events.EventType, 0, 32)
	for _, node := range empirePipelineWorkflowNodes() {
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

func empirePipelineEventPolicy(eventType string) (WorkflowEventPolicy, bool) {
	eventType = strings.TrimSpace(eventType)
	return workflowNodeEventPolicy("", eventType)
}

func workflowNodeSubscriptions(nodeID string) []events.EventType {
	nodeID = strings.TrimSpace(nodeID)
	for _, node := range empirePipelineWorkflowNodes() {
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
	for _, node := range empirePipelineWorkflowNodes() {
		if nodeID != "" && strings.TrimSpace(node.ID) != nodeID {
			continue
		}
		if policy, ok := node.Policies[eventType]; ok {
			return policy, true
		}
	}
	return WorkflowEventPolicy{}, false
}

func loadWorkflowNodesFromContracts() ([]WorkflowNode, error) {
	bundle := empireContractBundle()
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
			Policies:         workflowNodePolicyOverlay(nodeID),
		})
	}
	return out, nil
}

func workflowNodePolicyOverlay(nodeID string) map[string]WorkflowEventPolicy {
	switch strings.TrimSpace(nodeID) {
	case "pipeline-coordinator":
		return map[string]WorkflowEventPolicy{
			"timer.portfolio_digest":            {Consume: true},
			"runtime.reset":                     {Consume: false},
			"scan.requested":                    {Consume: true},
			"category.assessed":                 {Consume: true},
			"trend.identified":                  {Consume: true},
			"source.scraped":                    {Consume: true},
			"market_research.scan_complete":     {Consume: true},
			"trend_research.scan_complete":      {Consume: true},
			"scanner.google_maps.scan_complete": {Consume: true},
			"scanner.instagram.scan_complete":   {Consume: true},
			"scanner.reviews.scan_complete":     {Consume: true},
			"scanner.directories.scan_complete": {Consume: true},
			"scanner.yelp.scan_complete":        {Consume: true},
			"dedup.resolved":                    {Consume: true},
			"synthesis.resolved":                {Consume: true},
			"vertical.shortlisted":              {Consume: true, RequireVertical: true},
			"research.completed":                {Consume: true, RequireVertical: true},
			"research.vertical_rejected":        {Consume: true, RequireVertical: true},
			"spec.revision_requested":           {Consume: false, RequireVertical: true, VisibleDownstream: true},
			"spec.approved":                     {Consume: true, RequireVertical: true},
			"spec.validation_passed":            {Consume: true, RequireVertical: true},
			"spec.validation_failed":            {Consume: true, RequireVertical: true},
			"cto.spec_approved":                 {Consume: true, RequireVertical: true},
			"cto.spec_revision_needed":          {Consume: true, RequireVertical: true},
			"cto.spec_vetoed":                   {Consume: true, RequireVertical: true},
			"brand.candidates_ready":            {Consume: true, RequireVertical: true},
			"vertical.ready_for_review":         {Consume: false, RequireVertical: true, VisibleDownstream: true},
			"vertical.needs_more_data":          {Consume: true, RequireVertical: true},
			"brand.revision_needed":             {Consume: false, RequireVertical: true, VisibleDownstream: true},
			"vertical.approved":                 {Consume: false, RequireVertical: true, VisibleDownstream: true},
			"vertical.killed":                   {Consume: false, RequireVertical: true, VisibleDownstream: true},
			"vertical.resumed":                  {Consume: true, RequireVertical: true},
			"opco.ceo_ready":                    {Consume: false, RequireVertical: true, VisibleDownstream: true},
		}
	case "scoring-node":
		return map[string]WorkflowEventPolicy{
			"vertical.discovered":      {Consume: false, RequireVertical: true, VisibleDownstream: true},
			"vertical.derived":         {Consume: true},
			"score.dimension_complete": {Consume: false, RequireVertical: true, VisibleDownstream: true},
			"scoring.contest_resolved": {Consume: false, RequireVertical: true, VisibleDownstream: true},
			"vertical.scored":          {Consume: false},
		}
	default:
		return nil
	}
}
