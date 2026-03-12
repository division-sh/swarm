package dashboard

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"

	"empireai/internal/templateops"
)

func (s *Server) buildTemplateGraphFromYAML() ([]graphNode, []graphEdge, string, error) {
	agentsDir := resolvePipelineAgentsDir(s.cfg)
	if strings.TrimSpace(agentsDir) == "" {
		return nil, nil, "", sql.ErrNoRows
	}
	templateDir := filepath.Join(agentsDir, "templates")
	routesPath := filepath.Join(templateDir, "routes.yaml")
	agentsJSON, routesJSON, _, err := templateops.CompileTemplateFromYAML(templateDir, routesPath)
	if err != nil {
		return nil, nil, "", err
	}

	type tmplAgent struct {
		Role          string         `json:"role"`
		ParentRole    string         `json:"parent_role"`
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
	if err := json.Unmarshal(agentsJSON, &agents); err != nil {
		return nil, nil, "", err
	}
	var bootstrap []tmplRoute
	if err := json.Unmarshal(routesJSON, &bootstrap); err != nil {
		return nil, nil, "", err
	}

	nodes := make([]graphNode, 0, len(agents)+len(bootstrap))
	edges := make([]graphEdge, 0, len(agents)+len(bootstrap))
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
		if strings.TrimSpace(a.ParentRole) != "" {
			edges = append(edges, graphEdge{From: strings.TrimSpace(a.ParentRole), To: role, Kind: "management", Label: "manages", Status: "active", Source: "template"})
		}
	}
	for _, rt := range bootstrap {
		pat := strings.TrimSpace(rt.EventPattern)
		sub := strings.TrimSpace(coalesce(rt.SubscriberRole, rt.SubscriberID))
		if pat == "" || sub == "" {
			continue
		}
		nodes = append(nodes, graphNode{ID: "evt:" + pat, Kind: "event", Label: pat, Group: "template"})
		edges = append(edges, graphEdge{From: "evt:" + pat, To: sub, Kind: "routing", Label: pat, Status: "active", Source: "bootstrap", Reason: strings.TrimSpace(rt.Reason)})
	}
	nodes, edges = s.enrichCommunicationGraph("template", nodes, edges)
	return nodes, edges, "", nil
}
