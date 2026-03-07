package dashboard

import (
	"context"
	"fmt"
	"strings"
)

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
			edges = append(edges, graphEdge{From: parent, To: id, Kind: "management", Label: "manages", Status: "active", Source: "org"})
		}
		for _, pat := range subs {
			evtID := "evt:" + pat
			if _, ok := eventSeen[evtID]; !ok {
				eventSeen[evtID] = struct{}{}
				nodes = append(nodes, graphNode{ID: evtID, Kind: "event", Label: pat, Group: "holding"})
			}
			edges = append(edges, graphEdge{From: evtID, To: id, Kind: "subscription", Label: pat, Status: "active", Source: "subscription"})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

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
	nodes, edges = s.enrichCommunicationGraph("holding", nodes, filtered)
	return nodes, edges, nil
}
