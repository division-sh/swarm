package dashboard

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

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
			if err == sql.ErrNoRows {
				return s.buildTemplateGraphFromYAML()
			}
			return nil, nil, "", err
		}
	} else {
		if err := s.db.QueryRowContext(ctx, `
			SELECT version, COALESCE(agents,'[]'::jsonb), COALESCE(bootstrap_routes,'[]'::jsonb), COALESCE(seeded_routes,'[]'::jsonb)
			FROM org_templates
			WHERE version = $1
		`, strings.TrimSpace(version)).Scan(&ver, &agentsRaw, &bootstrapRaw, &seededRaw); err != nil {
			if err == sql.ErrNoRows {
				return s.buildTemplateGraphFromYAML()
			}
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
			edges = append(edges, graphEdge{From: strings.TrimSpace(a.ParentRole), To: role, Kind: "management", Label: "manages", Status: "active", Source: "template"})
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
		nodes = append(nodes, graphNode{ID: evtID, Kind: "event", Label: pat, Group: "template"})
		edges = append(edges, graphEdge{From: evtID, To: sub, Kind: "routing", Label: pat, Status: "active", Source: source, Reason: strings.TrimSpace(rt.Reason)})
	}
	for _, rt := range bootstrap {
		addRoute(rt, "bootstrap")
	}
	for _, rt := range seeded {
		addRoute(rt, "seeded")
	}

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
				continue
			}
		}
		uniqEdges = append(uniqEdges, e)
	}
	nodes, edges = s.enrichCommunicationGraph("template", nodes, uniqEdges)
	return nodes, edges, strings.TrimSpace(ver), nil
}
