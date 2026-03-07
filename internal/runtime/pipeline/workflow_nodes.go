package pipeline

import (
	"sort"
	"strings"

	"empireai/internal/events"
)

type WorkflowEventPolicy struct {
	Consume          bool
	RequireVertical  bool
	VisibleDownstream bool
}

type WorkflowNode struct {
	ID           string
	Subscriptions []events.EventType
	Policies     map[string]WorkflowEventPolicy
}

func empirePipelineWorkflowNodes() []WorkflowNode {
	return []WorkflowNode{
		{
			ID: "scan-coordinator",
			Subscriptions: []events.EventType{
				events.EventType("timer.portfolio_digest"),
				events.EventType("scan.requested"),
				events.EventType("category.assessed"),
				events.EventType("trend.identified"),
				events.EventType("source.scraped"),
				events.EventType("market_research.scan_complete"),
				events.EventType("trend_research.scan_complete"),
				events.EventType("scanner.google_maps.scan_complete"),
				events.EventType("scanner.instagram.scan_complete"),
				events.EventType("scanner.reviews.scan_complete"),
				events.EventType("scanner.directories.scan_complete"),
				events.EventType("scanner.yelp.scan_complete"),
				events.EventType("dedup.resolved"),
				events.EventType("synthesis.resolved"),
				events.EventType("runtime.reset"),
			},
			Policies: map[string]WorkflowEventPolicy{
				"timer.portfolio_digest":            {Consume: true},
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
				"runtime.reset":                     {Consume: false},
			},
		},
		{
			ID: "scoring-coordinator",
			Subscriptions: []events.EventType{
				events.EventType("vertical.derived"),
				events.EventType("vertical.scored"),
			},
			Policies: map[string]WorkflowEventPolicy{
				"vertical.derived": {Consume: true},
				"vertical.scored":  {Consume: false},
			},
		},
		{
			ID: "validation-gate",
			Subscriptions: []events.EventType{
				events.EventType("vertical.shortlisted"),
				events.EventType("research.completed"),
				events.EventType("research.vertical_rejected"),
				events.EventType("spec.revision_requested"),
				events.EventType("spec.approved"),
				events.EventType("spec.validation_passed"),
				events.EventType("spec.validation_failed"),
				events.EventType("vertical.approved"),
				events.EventType("vertical.killed"),
				events.EventType("opco.ceo_ready"),
				events.EventType("cto.spec_approved"),
				events.EventType("cto.spec_revision_needed"),
				events.EventType("cto.spec_vetoed"),
				events.EventType("brand.candidates_ready"),
				events.EventType("vertical.ready_for_review"),
				events.EventType("vertical.needs_more_data"),
				events.EventType("brand.revision_needed"),
				events.EventType("vertical.resumed"),
			},
			Policies: map[string]WorkflowEventPolicy{
				"vertical.shortlisted":     {Consume: true, RequireVertical: true},
				"research.completed":       {Consume: true, RequireVertical: true},
				"research.vertical_rejected": {Consume: true, RequireVertical: true},
				"spec.revision_requested":  {Consume: false, RequireVertical: true, VisibleDownstream: true},
				"spec.approved":            {Consume: true, RequireVertical: true},
				"spec.validation_passed":   {Consume: true, RequireVertical: true},
				"spec.validation_failed":   {Consume: true, RequireVertical: true},
				"vertical.approved":        {Consume: false, RequireVertical: true, VisibleDownstream: true},
				"vertical.killed":          {Consume: false, RequireVertical: true, VisibleDownstream: true},
				"opco.ceo_ready":           {Consume: false, RequireVertical: true, VisibleDownstream: true},
				"cto.spec_approved":        {Consume: true, RequireVertical: true},
				"cto.spec_revision_needed": {Consume: true, RequireVertical: true},
				"cto.spec_vetoed":          {Consume: true, RequireVertical: true},
				"brand.candidates_ready":   {Consume: true, RequireVertical: true},
				"vertical.ready_for_review": {Consume: false, RequireVertical: true, VisibleDownstream: true},
				"vertical.needs_more_data": {Consume: true, RequireVertical: true},
				"brand.revision_needed":    {Consume: false, RequireVertical: true, VisibleDownstream: true},
				"vertical.resumed":         {Consume: true, RequireVertical: true},
			},
		},
	}
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
	for _, node := range empirePipelineWorkflowNodes() {
		if policy, ok := node.Policies[eventType]; ok {
			return policy, true
		}
	}
	return WorkflowEventPolicy{}, false
}
