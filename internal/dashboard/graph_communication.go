package dashboard

import (
	"fmt"
	"sort"
	"strings"

	"empireai/internal/commgraph"
)

func (s *Server) enrichCommunicationGraph(mode string, nodes []graphNode, edges []graphEdge) ([]graphNode, []graphEdge) {
	mode = strings.TrimSpace(strings.ToLower(mode))
	if mode == "" {
		mode = "holding"
	}

	nodeMap := make(map[string]graphNode, len(nodes)+64)
	edgesOut := make([]graphEdge, 0, len(edges)+256)
	addNode := func(n graphNode) {
		if strings.TrimSpace(n.ID) == "" {
			return
		}
		if prev, ok := nodeMap[n.ID]; ok {
			nodeMap[n.ID] = mergeGraphNode(prev, n)
			return
		}
		nodeMap[n.ID] = n
	}
	for _, n := range nodes {
		addNode(n)
	}
	addEdge := func(e graphEdge) {
		e.From = strings.TrimSpace(e.From)
		e.To = strings.TrimSpace(e.To)
		if e.From == "" || e.To == "" || e.From == e.To {
			return
		}
		if strings.TrimSpace(e.Status) == "" {
			e.Status = "active"
		}
		edgesOut = append(edgesOut, e)
	}
	for _, e := range edges {
		addEdge(e)
	}

	defaultGroup := mode
	for _, n := range nodeMap {
		if n.Group != "" {
			defaultGroup = n.Group
			break
		}
	}

	ensureEventNode := func(eventType, group string) string {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			return ""
		}
		if group == "" {
			group = defaultGroup
		}
		id := graphEventNodeID(eventType)
		addNode(graphNode{ID: id, Kind: "event", Label: eventType, Group: group})
		return id
	}

	// 1) Event producer registry edges.
	for _, n := range nodeMap {
		if n.Kind != "agent" {
			continue
		}
		for _, evt := range commgraph.ProducerEventsForRole(n.Role) {
			evtID := ensureEventNode(evt, n.Group)
			if evtID == "" {
				continue
			}
			addEdge(graphEdge{
				From:   n.ID,
				To:     evtID,
				Kind:   "producer",
				Label:  evt,
				Status: "active",
				Source: "producer_registry",
			})
		}
	}
	addNode(graphNode{ID: commgraph.RuntimeProducerID, Kind: "system", Label: "Runtime", Group: defaultGroup})
	for _, evt := range commgraph.RuntimeEvents() {
		evtID := ensureEventNode(evt, defaultGroup)
		if evtID == "" {
			continue
		}
		addEdge(graphEdge{From: commgraph.RuntimeProducerID, To: evtID, Kind: "producer", Label: evt, Status: "active", Source: "producer_registry"})
	}
	addNode(graphNode{ID: commgraph.HumanProducerID, Kind: "human", Label: "Human Board", Group: defaultGroup})
	for _, evt := range commgraph.HumanEvents() {
		evtID := ensureEventNode(evt, defaultGroup)
		if evtID == "" {
			continue
		}
		addEdge(graphEdge{From: commgraph.HumanProducerID, To: evtID, Kind: "producer", Label: evt, Status: "active", Source: "producer_registry"})
	}

	// Build role -> agent IDs index for message/mailbox projection.
	roles := make(map[string][]string)
	agentIDs := make([]string, 0, len(nodeMap))
	for _, n := range nodeMap {
		if n.Kind != "agent" {
			continue
		}
		role := strings.TrimSpace(strings.ToLower(n.Role))
		if role == "" {
			continue
		}
		roles[role] = append(roles[role], n.ID)
		agentIDs = append(agentIDs, n.ID)
	}
	for k := range roles {
		sort.Strings(roles[k])
	}
	resolveRole := func(role string) []string {
		role = strings.TrimSpace(strings.ToLower(role))
		if role == "" {
			return nil
		}
		ids := roles[role]
		if len(ids) == 0 {
			return nil
		}
		out := make([]string, len(ids))
		copy(out, ids)
		return out
	}

	// 2) Message authority edges.
	for _, rule := range commgraph.MessageAuthorities() {
		if !messageScopeEnabled(mode, rule.Scope) {
			continue
		}
		senders := resolveRole(rule.SenderRole)
		if len(senders) == 0 {
			continue
		}
		recipients := make([]string, 0, 16)
		for _, role := range rule.RecipientRoles {
			recipients = append(recipients, resolveRole(role)...)
		}
		recipients = uniqueStrings(recipients)
		for _, from := range senders {
			for _, to := range recipients {
				addEdge(graphEdge{From: from, To: to, Kind: "message", Label: "agent_message", Status: "allowed", Source: "authority"})
			}
		}
	}

	// Management-chain message authority (manager can message descendants).
	mgmtChildren := make(map[string][]string)
	for _, e := range edgesOut {
		if e.Kind != "management" {
			continue
		}
		if nodeMap[e.From].Kind != "agent" {
			continue
		}
		if nodeMap[e.To].Kind != "agent" {
			continue
		}
		mgmtChildren[e.From] = append(mgmtChildren[e.From], e.To)
	}
	for manager := range mgmtChildren {
		desc := descendants(manager, mgmtChildren)
		for _, to := range desc {
			addEdge(graphEdge{From: manager, To: to, Kind: "message", Label: "agent_message", Status: "allowed", Source: "management_chain"})
		}
	}

	// 3) Mailbox loops (agent -> mailbox -> human -> target).
	mailboxActivated := false
	for _, loop := range commgraph.MailboxRoundTrips() {
		senders := resolveRole(loop.SenderRole)
		if len(senders) == 0 {
			continue
		}
		mailboxActivated = true
		addNode(graphNode{ID: commgraph.MailboxNodeID, Kind: "mailbox", Label: "Mailbox", Group: defaultGroup})
		addNode(graphNode{ID: commgraph.HumanProducerID, Kind: "human", Label: "Human Board", Group: defaultGroup})

		targets := make([]string, 0, len(senders))
		switch strings.TrimSpace(strings.ToLower(loop.ReturnToRole)) {
		case "", "requesting-agent":
			targets = append(targets, senders...)
		default:
			targets = append(targets, resolveRole(loop.ReturnToRole)...)
			if len(targets) == 0 {
				targets = append(targets, senders...)
			}
		}
		targets = uniqueStrings(targets)
		decisionLabel := strings.Join(loop.DecisionEvents, " | ")
		if decisionLabel == "" {
			decisionLabel = "decision"
		}
		for _, from := range senders {
			addEdge(graphEdge{From: from, To: commgraph.MailboxNodeID, Kind: "mailbox", Label: loop.MailboxType, Status: "active", Source: "mailbox"})
		}
		addEdge(graphEdge{From: commgraph.MailboxNodeID, To: commgraph.HumanProducerID, Kind: "mailbox", Label: "human_review", Status: "active", Source: "mailbox"})
		for _, to := range targets {
			addEdge(graphEdge{From: commgraph.HumanProducerID, To: to, Kind: "mailbox", Label: decisionLabel, Status: "active", Source: "mailbox"})
		}
	}
	if !mailboxActivated {
		// Keep human node when producer edges exist; mailbox node is only shown when a loop is present.
		delete(nodeMap, commgraph.MailboxNodeID)
	}

	finalNodes, finalEdges := finalizeGraph(nodeMap, edgesOut)
	return finalNodes, finalEdges
}

func graphEventNodeID(eventType string) string {
	return "evt:" + strings.TrimSpace(eventType)
}

func mergeGraphNode(old, next graphNode) graphNode {
	out := old
	if out.Kind == "" {
		out.Kind = next.Kind
	}
	if out.Label == "" {
		out.Label = next.Label
	}
	if out.Group == "" {
		out.Group = next.Group
	}
	if out.Role == "" {
		out.Role = next.Role
	}
	if out.Mode == "" {
		out.Mode = next.Mode
	}
	if out.Status == "" {
		out.Status = next.Status
	}
	if out.VerticalID == "" {
		out.VerticalID = next.VerticalID
	}
	if out.VerticalSlug == "" {
		out.VerticalSlug = next.VerticalSlug
	}
	if out.ParentID == "" {
		out.ParentID = next.ParentID
	}
	if out.SystemPrompt == "" {
		out.SystemPrompt = next.SystemPrompt
	}
	if len(out.Tools) == 0 && len(next.Tools) > 0 {
		out.Tools = append([]string(nil), next.Tools...)
	}
	if len(out.Subscriptions) == 0 && len(next.Subscriptions) > 0 {
		out.Subscriptions = append([]string(nil), next.Subscriptions...)
	}
	if len(out.Constraints) == 0 && len(next.Constraints) > 0 {
		out.Constraints = next.Constraints
	}
	return out
}

func finalizeGraph(nodeMap map[string]graphNode, edges []graphEdge) ([]graphNode, []graphEdge) {
	nodes := make([]graphNode, 0, len(nodeMap))
	for _, n := range nodeMap {
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	allowed := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		allowed[n.ID] = struct{}{}
	}

	uniq := make([]graphEdge, 0, len(edges))
	seen := make(map[string]struct{}, len(edges))
	for _, e := range edges {
		if _, ok := allowed[e.From]; !ok {
			continue
		}
		if _, ok := allowed[e.To]; !ok {
			continue
		}
		key := fmt.Sprintf("%s|%s|%s|%s|%s|%s", e.From, e.To, e.Kind, e.Label, e.Source, e.Status)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		uniq = append(uniq, e)
	}
	sort.Slice(uniq, func(i, j int) bool {
		if uniq[i].Kind != uniq[j].Kind {
			return uniq[i].Kind < uniq[j].Kind
		}
		if uniq[i].From != uniq[j].From {
			return uniq[i].From < uniq[j].From
		}
		if uniq[i].To != uniq[j].To {
			return uniq[i].To < uniq[j].To
		}
		if uniq[i].Source != uniq[j].Source {
			return uniq[i].Source < uniq[j].Source
		}
		return uniq[i].Label < uniq[j].Label
	})
	return nodes, uniq
}

func messageScopeEnabled(mode, scope string) bool {
	scope = strings.TrimSpace(strings.ToLower(scope))
	mode = strings.TrimSpace(strings.ToLower(mode))
	switch scope {
	case "", "any":
		return true
	case "holding":
		return mode == "holding"
	case "opco":
		return mode == "opco" || mode == "template"
	default:
		return true
	}
}

func descendants(root string, children map[string][]string) []string {
	seen := make(map[string]struct{}, 16)
	queue := append([]string(nil), children[root]...)
	out := make([]string, 0, len(queue))
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
		queue = append(queue, children[n]...)
	}
	sort.Strings(out)
	return out
}

func uniqueStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
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
	sort.Strings(out)
	return out
}
