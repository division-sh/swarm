package dashboard

import (
	"context"
	"fmt"
	"strings"
)

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
			edges = append(edges, graphEdge{From: parent, To: id, Kind: "management", Label: "manages", Status: "active", Source: "org"})
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
			continue
		}
		evtID := "evt:" + pat
		if _, ok := eventSeen[evtID]; !ok {
			eventSeen[evtID] = struct{}{}
			nodes = append(nodes, graphNode{ID: evtID, Kind: "event", Label: pat, Group: "opco"})
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
