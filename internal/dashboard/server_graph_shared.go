package dashboard

import (
	"encoding/json"
	"sort"
	"strings"
)

func flowInterceptPolicy(eventType string, payloadRaw []byte) (intercepted bool, passthrough bool) {
	switch strings.TrimSpace(eventType) {
	case "timer.portfolio_digest", "timer.marginal_review", "timer.marginal_kill", "timer.scan_timeout", "timer.campaign_deadline":
		var payload map[string]any
		_ = json.Unmarshal(payloadRaw, &payload)
		if strings.TrimSpace(eventType) == "timer.portfolio_digest" && boolFromAny(payload["scoring_rejections_injected"]) {
			return false, false
		}
		return true, true
	case "vertical.scored":
		var payload map[string]any
		_ = json.Unmarshal(payloadRaw, &payload)
		result := strings.ToLower(strings.TrimSpace(asString(payload["result"])))
		switch result {
		case "marginal", "rejected":
			return true, true
		default:
			return false, true
		}
	case "scan.requested",
		"vertical.discovered",
		"score.dimension_complete",
		"scoring.contest_resolved",
		"category.assessed",
		"trend.identified",
		"source.scraped",
		"market_research.scan_complete",
		"trend_research.scan_complete",
		"scanner.google_maps.scan_complete",
		"scanner.instagram.scan_complete",
		"scanner.reviews.scan_complete",
		"scanner.directories.scan_complete",
		"scanner.yelp.scan_complete",
		"dedup.resolved",
		"synthesis.resolved",
		"vertical.shortlisted",
		"research.completed",
		"research.vertical_rejected",
		"spec.revision_requested",
		"spec.approved",
		"cto.spec_approved",
		"cto.spec_revision_needed",
		"cto.spec_vetoed",
		"brand.candidates_ready",
		"vertical.needs_more_data",
		"brand.revision_needed",
		"vertical.resumed":
		return true, true
	case "spec.validation_passed", "spec.validation_failed":
		return true, true
	case "vertical.approved", "vertical.killed", "vertical.ready_for_review":
		return false, true
	case "runtime.reset":
		return false, true
	default:
		return false, false
	}
}

func defaultFlowTargetNodes(eventType string) []string {
	switch strings.TrimSpace(eventType) {
	case "timer.portfolio_digest", "timer.marginal_review":
		return []string{"empire-coordinator"}
	case "timer.scan_timeout", "timer.campaign_deadline":
		return []string{"pipeline-coordinator"}
	case "timer.marginal_kill":
		return []string{"runtime"}
	default:
		if handler := strings.TrimSpace(pipelineHandlerRef(eventType)); handler != "" {
			return []string{"pipeline-coordinator"}
		}
		return nil
	}
}

func pipelineHandlerRef(eventType string) string {
	switch strings.TrimSpace(eventType) {
	case "scan.requested":
		return "pipeline_coordinator.go:handleScanRequested"
	case "category.assessed", "trend.identified", "source.scraped":
		return "pipeline_coordinator.go:handleDiscoveryReport"
	case "dedup.resolved":
		return "pipeline_coordinator.go:handleDedupResolved"
	case "vertical.discovered":
		return "pipeline_coordinator.go:handleScoringRequested"
	case "score.dimension_complete":
		return "pipeline_coordinator.go:handleScoreDimensionComplete"
	case "scoring.contest_resolved":
		return "pipeline_coordinator.go:handleScoringContestResolved"
	case "vertical.shortlisted":
		return "pipeline_coordinator.go:handleValidationStarted"
	case "research.completed", "spec.approved", "brand.candidates_ready":
		return "pipeline_coordinator.go:handleValidationGate"
	case "spec.validation_passed":
		return "pipeline_coordinator.go:handleSpecValidationPassed"
	case "spec.validation_failed":
		return "pipeline_coordinator.go:handleSpecValidationFailed"
	case "cto.spec_approved":
		return "pipeline_coordinator.go:handleCTOApproved"
	case "cto.spec_revision_needed":
		return "pipeline_coordinator.go:handleCTORevisionNeeded"
	case "research.vertical_rejected", "cto.spec_vetoed":
		return "pipeline_coordinator.go:handleValidationRejected"
	case "vertical.needs_more_data":
		return "pipeline_coordinator.go:handleValidationMoreData"
	case "brand.revision_needed":
		return "pipeline_coordinator.go:handleBrandRevision"
	case "spec.revision_requested":
		return "pipeline_coordinator.go:handleSpecRevisionRequested"
	case "vertical.resumed":
		return "pipeline_coordinator.go:handleVerticalResumed"
	case "timer.portfolio_digest":
		return "pipeline_coordinator.go:handlePortfolioDigestTimer"
	case "runtime.reset":
		return "pipeline_coordinator.go:resetInMemoryState"
	default:
		return ""
	}
}

func eventSchemaRequired(raw map[string]any) []string {
	requiredRaw, ok := raw["required"]
	if !ok || requiredRaw == nil {
		return nil
	}
	switch t := requiredRaw.(type) {
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			v := strings.TrimSpace(asString(item))
			if v != "" {
				out = append(out, v)
			}
		}
		sort.Strings(out)
		return out
	case []string:
		out := make([]string, 0, len(t))
		for _, item := range t {
			v := strings.TrimSpace(item)
			if v != "" {
				out = append(out, v)
			}
		}
		sort.Strings(out)
		return out
	default:
		return nil
	}
}

func eventSchemaProperties(raw map[string]any) []string {
	propsRaw, ok := raw["properties"].(map[string]any)
	if !ok || len(propsRaw) == 0 {
		return nil
	}
	out := make([]string, 0, len(propsRaw))
	for k := range propsRaw {
		v := strings.TrimSpace(k)
		if v != "" {
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}

func parseAgentRuntimeConfig(raw []byte) (systemPrompt string, tools []string, subs []string, constraints map[string]any) {
	if len(raw) == 0 || !json.Valid(raw) {
		return "", nil, nil, nil
	}
	var obj map[string]any
	if json.Unmarshal(raw, &obj) != nil {
		return "", nil, nil, nil
	}
	systemPrompt = strings.TrimSpace(asString(obj["system_prompt"]))
	if systemPrompt == "" {
		systemPrompt = strings.TrimSpace(asString(obj["prompt"]))
	}
	if arr, ok := obj["tools"].([]any); ok {
		for _, v := range arr {
			s := strings.TrimSpace(asString(v))
			if s != "" {
				tools = append(tools, s)
			}
		}
	}
	if arr, ok := obj["subscriptions"].([]any); ok {
		for _, v := range arr {
			s := strings.TrimSpace(asString(v))
			if s != "" {
				subs = append(subs, s)
			}
		}
	}
	if m, ok := obj["constraints"].(map[string]any); ok && len(m) > 0 {
		constraints = m
	}
	return systemPrompt, tools, subs, constraints
}

func normalizeStrings(in []string) []string {
	out := make([]string, 0, len(in))
	seen := map[string]struct{}{}
	for _, raw := range in {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
