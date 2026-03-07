package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func flowInterceptPolicy(eventType string, payloadRaw []byte) (intercepted bool, passthrough bool) {
	switch strings.TrimSpace(eventType) {
	case "timer.portfolio_digest":
		var payload map[string]any
		_ = json.Unmarshal(payloadRaw, &payload)
		if boolFromAny(payload["scoring_rejections_injected"]) {
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
		// Some older configs stored prompt in nested "prompt".
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

func (s *Server) buildHoldingGraph(ctx context.Context) ([]graphNode, []graphEdge, error) {
	if s.db == nil {
		return nil, nil, fmt.Errorf("db unavailable")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			a.id,
			COALESCE(a.role,''),
			COALESCE(a.mode,''),
			COALESCE(a.status,''),
			COALESCE(a.parent_agent_id,''),
			COALESCE(a.config, '{}'::jsonb)
		FROM agents a
		WHERE COALESCE(a.status,'') <> 'terminated'
		  AND (a.vertical_id IS NULL OR COALESCE(a.mode,'') <> 'operating')
		ORDER BY a.id ASC
	`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	nodes := make([]graphNode, 0, 32)
	edges := make([]graphEdge, 0, 64)
	seen := map[string]struct{}{}
	eventSeen := map[string]struct{}{}

	for rows.Next() {
		var id, role, mode, status, parent string
		var cfgRaw []byte
		if err := rows.Scan(&id, &role, &mode, &status, &parent, &cfgRaw); err != nil {
			return nil, nil, err
		}
		sp, tools, subs, cons := parseAgentRuntimeConfig(cfgRaw)
		nodes = append(nodes, graphNode{
			ID:            id,
			Kind:          "agent",
			Label:         id,
			Group:         "holding",
			Role:          role,
			Mode:          mode,
			Status:        status,
			ParentID:      parent,
			SystemPrompt:  sp,
			Tools:         tools,
			Subscriptions: subs,
			Constraints:   cons,
		})
		seen[id] = struct{}{}
		if strings.TrimSpace(parent) != "" {
			edges = append(edges, graphEdge{
				From:   parent,
				To:     id,
				Kind:   "management",
				Label:  "manages",
				Status: "active",
				Source: "org",
			})
		}

		for _, pat := range subs {
			evtID := "evt:" + pat
			if _, ok := eventSeen[evtID]; !ok {
				eventSeen[evtID] = struct{}{}
				nodes = append(nodes, graphNode{
					ID:    evtID,
					Kind:  "event",
					Label: pat,
					Group: "holding",
				})
			}
			edges = append(edges, graphEdge{
				From:   evtID,
				To:     id,
				Kind:   "subscription",
				Label:  pat,
				Status: "active",
				Source: "subscription",
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	// Filter management edges to existing nodes only (defensive against stale parent ids).
	filtered := edges[:0]
	for _, e := range edges {
		if e.Kind == "management" {
			if _, ok := seen[e.From]; !ok {
				continue
			}
			if _, ok := seen[e.To]; !ok {
				continue
			}
		}
		filtered = append(filtered, e)
	}
	edges = filtered
	nodes, edges = s.enrichCommunicationGraph("holding", nodes, edges)
	return nodes, edges, nil
}

func (s *Server) buildTemplateGraph(ctx context.Context, version string) ([]graphNode, []graphEdge, string, error) {
	if s.db == nil {
		return nil, nil, "", fmt.Errorf("db unavailable")
	}
	var ver string
	var agentsRaw, bootstrapRaw, seededRaw []byte
	if strings.TrimSpace(version) == "" {
		if err := s.db.QueryRowContext(ctx, `
			SELECT version, COALESCE(agents,'[]'::jsonb), COALESCE(bootstrap_routes,'[]'::jsonb), COALESCE(seeded_routes,'[]'::jsonb)
			FROM org_templates
			ORDER BY created_at DESC
			LIMIT 1
		`).Scan(&ver, &agentsRaw, &bootstrapRaw, &seededRaw); err != nil {
			return nil, nil, "", err
		}
	} else {
		if err := s.db.QueryRowContext(ctx, `
			SELECT version, COALESCE(agents,'[]'::jsonb), COALESCE(bootstrap_routes,'[]'::jsonb), COALESCE(seeded_routes,'[]'::jsonb)
			FROM org_templates
			WHERE version = $1
		`, strings.TrimSpace(version)).Scan(&ver, &agentsRaw, &bootstrapRaw, &seededRaw); err != nil {
			return nil, nil, "", err
		}
	}

	type tmplAgent struct {
		Role          string         `json:"role"`
		ParentRole    string         `json:"parent_role"`
		Type          string         `json:"type"`
		SystemPrompt  string         `json:"system_prompt"`
		Tools         []string       `json:"tools"`
		Subscriptions []string       `json:"subscriptions"`
		Constraints   map[string]any `json:"constraints,omitempty"`
	}
	type tmplRoute struct {
		EventPattern   string `json:"event_pattern"`
		SubscriberRole string `json:"subscriber_role"`
		SubscriberID   string `json:"subscriber_id"`
		Reason         string `json:"reason"`
	}

	var agents []tmplAgent
	_ = json.Unmarshal(agentsRaw, &agents)
	var bootstrap []tmplRoute
	_ = json.Unmarshal(bootstrapRaw, &bootstrap)
	var seeded []tmplRoute
	_ = json.Unmarshal(seededRaw, &seeded)

	nodes := make([]graphNode, 0, 64)
	edges := make([]graphEdge, 0, 128)
	seenAgents := map[string]struct{}{}

	for _, a := range agents {
		role := strings.TrimSpace(a.Role)
		if role == "" {
			continue
		}
		nodes = append(nodes, graphNode{
			ID:            role,
			Kind:          "agent",
			Label:         role,
			Group:         "template",
			Role:          role,
			Mode:          "operating",
			Status:        "template",
			ParentID:      strings.TrimSpace(a.ParentRole),
			SystemPrompt:  strings.TrimSpace(a.SystemPrompt),
			Tools:         normalizeStrings(a.Tools),
			Subscriptions: normalizeStrings(a.Subscriptions),
			Constraints:   a.Constraints,
		})
		seenAgents[role] = struct{}{}
		if strings.TrimSpace(a.ParentRole) != "" {
			edges = append(edges, graphEdge{
				From:   strings.TrimSpace(a.ParentRole),
				To:     role,
				Kind:   "management",
				Label:  "manages",
				Status: "active",
				Source: "template",
			})
		}
	}

	addRoute := func(rt tmplRoute, source string) {
		pat := strings.TrimSpace(rt.EventPattern)
		if pat == "" {
			return
		}
		sub := strings.TrimSpace(coalesce(rt.SubscriberRole, rt.SubscriberID))
		if sub == "" {
			return
		}
		evtID := "evt:" + pat
		nodes = append(nodes, graphNode{
			ID:    evtID,
			Kind:  "event",
			Label: pat,
			Group: "template",
		})
		edges = append(edges, graphEdge{
			From:   evtID,
			To:     sub,
			Kind:   "routing",
			Label:  pat,
			Status: "active",
			Source: source,
			Reason: strings.TrimSpace(rt.Reason),
		})
	}
	for _, rt := range bootstrap {
		addRoute(rt, "bootstrap")
	}
	for _, rt := range seeded {
		addRoute(rt, "seeded")
	}

	// Deduplicate event nodes (we appended without tracking above).
	uniqNodes := make([]graphNode, 0, len(nodes))
	seenNodes := map[string]struct{}{}
	for _, n := range nodes {
		if n.ID == "" {
			continue
		}
		if _, ok := seenNodes[n.ID]; ok {
			continue
		}
		seenNodes[n.ID] = struct{}{}
		uniqNodes = append(uniqNodes, n)
	}
	nodes = uniqNodes

	// Filter edges that point at unknown agent nodes (defensive).
	uniqEdges := make([]graphEdge, 0, len(edges))
	for _, e := range edges {
		if e.Kind == "management" {
			if _, ok := seenAgents[e.From]; !ok {
				continue
			}
			if _, ok := seenAgents[e.To]; !ok {
				continue
			}
		}
		if e.Kind == "routing" {
			if _, ok := seenAgents[e.To]; !ok {
				// subscriber_role should exist in template; skip if not.
				continue
			}
		}
		uniqEdges = append(uniqEdges, e)
	}
	edges = uniqEdges
	nodes, edges = s.enrichCommunicationGraph("template", nodes, edges)
	return nodes, edges, strings.TrimSpace(ver), nil
}

func (s *Server) buildOpCoGraph(ctx context.Context, vertical string) ([]graphNode, []graphEdge, map[string]any, error) {
	if s.db == nil {
		return nil, nil, nil, fmt.Errorf("db unavailable")
	}
	vertical = strings.TrimSpace(vertical)

	var verticalID, slug, name, geo, templateVersion string
	if err := s.db.QueryRowContext(ctx, `
		SELECT id::text, COALESCE(slug,''), COALESCE(name,''), COALESCE(geography,''), COALESCE(template_version,'')
		FROM verticals
		WHERE id::text = $1 OR COALESCE(slug,'') = $1
		LIMIT 1
	`, vertical).Scan(&verticalID, &slug, &name, &geo, &templateVersion); err != nil {
		return nil, nil, nil, fmt.Errorf("resolve vertical: %w", err)
	}

	nodes := make([]graphNode, 0, 64)
	edges := make([]graphEdge, 0, 128)
	seenAgents := map[string]struct{}{}

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			a.id,
			COALESCE(a.role,''),
			COALESCE(a.mode,''),
			COALESCE(a.status,''),
			COALESCE(a.parent_agent_id,''),
			COALESCE(a.config, '{}'::jsonb)
		FROM agents a
		WHERE COALESCE(a.status,'') <> 'terminated'
		  AND COALESCE(a.vertical_id::text,'') = $1
		ORDER BY a.id ASC
	`, verticalID)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id, role, mode, status, parent string
		var cfgRaw []byte
		if err := rows.Scan(&id, &role, &mode, &status, &parent, &cfgRaw); err != nil {
			return nil, nil, nil, err
		}
		sp, tools, subs, cons := parseAgentRuntimeConfig(cfgRaw)
		nodes = append(nodes, graphNode{
			ID:            id,
			Kind:          "agent",
			Label:         id,
			Group:         "opco",
			Role:          role,
			Mode:          mode,
			Status:        status,
			VerticalID:    verticalID,
			VerticalSlug:  slug,
			ParentID:      parent,
			SystemPrompt:  sp,
			Tools:         tools,
			Subscriptions: subs,
			Constraints:   cons,
		})
		seenAgents[id] = struct{}{}
		if strings.TrimSpace(parent) != "" {
			edges = append(edges, graphEdge{
				From:   parent,
				To:     id,
				Kind:   "management",
				Label:  "manages",
				Status: "active",
				Source: "org",
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, err
	}

	type rr struct {
		EventPattern string
		SubscriberID string
		Status       string
		Source       string
		Reason       string
	}
	routes := make([]rr, 0, 64)
	rows2, err := s.db.QueryContext(ctx, `
		SELECT
			event_pattern,
			subscriber_id,
			COALESCE(status,'active'),
			COALESCE(source,'discovered'),
			COALESCE(reason,'')
		FROM routing_rules
		WHERE vertical_id = $1::uuid
		ORDER BY created_at ASC
	`, verticalID)
	if err == nil {
		defer rows2.Close()
		for rows2.Next() {
			var r rr
			if err := rows2.Scan(&r.EventPattern, &r.SubscriberID, &r.Status, &r.Source, &r.Reason); err != nil {
				break
			}
			routes = append(routes, r)
		}
		_ = rows2.Close()
	}

	eventSeen := map[string]struct{}{}
	for _, rt := range routes {
		pat := strings.TrimSpace(rt.EventPattern)
		sub := strings.TrimSpace(rt.SubscriberID)
		if pat == "" || sub == "" {
			continue
		}
		if _, ok := seenAgents[sub]; !ok {
			// Skip routes to agents not present (stale config).
			continue
		}
		evtID := "evt:" + pat
		if _, ok := eventSeen[evtID]; !ok {
			eventSeen[evtID] = struct{}{}
			nodes = append(nodes, graphNode{
				ID:    evtID,
				Kind:  "event",
				Label: pat,
				Group: "opco",
			})
		}
		edges = append(edges, graphEdge{
			From:   evtID,
			To:     sub,
			Kind:   "routing",
			Label:  pat,
			Status: strings.TrimSpace(rt.Status),
			Source: strings.TrimSpace(rt.Source),
			Reason: strings.TrimSpace(rt.Reason),
		})
	}

	resolved := map[string]any{
		"id":               verticalID,
		"slug":             slug,
		"name":             name,
		"geography":        geo,
		"template_version": templateVersion,
	}
	nodes, edges = s.enrichCommunicationGraph("opco", nodes, edges)
	return nodes, edges, resolved, nil
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
