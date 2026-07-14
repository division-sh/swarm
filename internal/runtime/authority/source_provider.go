package authority

import (
	"slices"
	"sort"
	"strings"
	"sync"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type sourceProvider struct {
	mu               sync.RWMutex
	mailboxSendRoles []string
	producerRoles    []string
	agentEvents      map[string][]string
	parentByAgent    map[string]string
}

func NewSourceProvider(source semanticview.Source) Provider {
	return buildSourceProvider(source)
}

func buildSourceProvider(source semanticview.Source) Provider {
	if source == nil {
		return noopProvider{}
	}
	allRoles, _ := sourceRolesAndToolGrants(source)
	mailboxSendRoles := append([]string(nil), allRoles...)

	agentEvents := buildProducerRegistry(source)
	producerRoles := make([]string, 0, len(agentEvents))
	for role := range agentEvents {
		producerRoles = append(producerRoles, role)
	}
	sort.Strings(producerRoles)

	return &sourceProvider{
		mailboxSendRoles: cloneRoles(mailboxSendRoles),
		producerRoles:    producerRoles,
		agentEvents:      agentEvents,
		parentByAgent:    buildManagerFallbackGraph(source),
	}
}

func (p *sourceProvider) CanonicalRole(role string) string {
	return canonicalRole(role)
}

func (p *sourceProvider) ProducerRoles() []string {
	return append([]string(nil), p.producerRoles...)
}

func (p *sourceProvider) ProducerEventsForRole(role string) []string {
	role = canonicalRole(role)
	if role == "" {
		return nil
	}
	events := p.agentEvents[role]
	out := make([]string, 0, len(events))
	seen := make(map[string]struct{}, len(events))
	for _, evt := range events {
		evt = strings.TrimSpace(evt)
		if evt == "" {
			continue
		}
		if _, ok := seen[evt]; ok {
			continue
		}
		seen[evt] = struct{}{}
		out = append(out, evt)
	}
	sort.Strings(out)
	return out
}

func (p *sourceProvider) HasMessageAuthority(actor, target models.AgentConfig) bool {
	sender := canonicalRole(actor.Role)
	recipient := canonicalRole(target.Role)
	if sender == "" || recipient == "" {
		return false
	}
	if strings.TrimSpace(actor.ID) == strings.TrimSpace(target.ID) {
		return true
	}
	if !SameFlowInstance(actor, target) {
		return false
	}
	switch strongestMessagePermission(permissionSet(actor.Permissions)) {
	case "message_flow":
		return true
	case "message_peers":
		return PeerManagerFallback(actor, target)
	default:
		return false
	}
}

func (p *sourceProvider) AuthorizeRouting(actor, target models.AgentConfig, status string) error {
	_ = strings.TrimSpace(strings.ToLower(status))
	if !hasToolGrant(permissionSet(actor.Permissions), "configure_routing") {
		return authorizationDenied("configure_routing", actor, target)
	}
	if strings.TrimSpace(actor.ID) == strings.TrimSpace(target.ID) {
		return nil
	}
	if err := p.AuthorizeManagement(actor, target); err != nil {
		return authorizationDenied("configure_routing", actor, target)
	}
	return nil
}

func (p *sourceProvider) AuthorizeManagement(actor, target models.AgentConfig) error {
	if !hasAnyToolGrant(permissionSet(actor.Permissions), "agent_hire", "agent_fire", "agent_reconfigure") {
		return authorizationDenied("agent_manage", actor, target)
	}
	if strings.TrimSpace(actor.ID) == "" || strings.TrimSpace(target.ID) == "" {
		return authorizationDenied("agent_manage", actor, target)
	}
	if strings.TrimSpace(actor.ID) == strings.TrimSpace(target.ID) {
		return authorizationDenied("agent_manage", actor, target)
	}
	if !SameFlowInstance(actor, target) {
		return authorizationDenied("agent_manage", actor, target)
	}
	if p.isManagedDescendant(actor, target) {
		return nil
	}
	return authorizationDenied("agent_manage", actor, target)
}

func (p *sourceProvider) UpsertManagedAgent(cfg models.AgentConfig) {
	if p == nil {
		return
	}
	agentID := strings.TrimSpace(cfg.ID)
	if agentID == "" {
		return
	}
	parent := strings.TrimSpace(cfg.ParentAgent)
	if parent == "" {
		parent = strings.TrimSpace(cfg.ManagerFallback)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if parent == "" || parent == agentID {
		delete(p.parentByAgent, agentID)
		return
	}
	p.parentByAgent[agentID] = parent
}

func (p *sourceProvider) RemoveManagedAgent(agentID string) {
	if p == nil {
		return
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.parentByAgent, agentID)
}

func (p *sourceProvider) AuthorizeMailboxSend(actor models.AgentConfig) error {
	if containsCanonical(p.mailboxSendRoles, actor.Role) {
		return nil
	}
	return authorizationDenied("mailbox_send", actor, models.AgentConfig{})
}

func authorizationDenied(action string, actor, target models.AgentConfig) error {
	return failures.New(
		failures.ClassAuthorizationDenied,
		"runtime_authority_denied",
		"runtime-authority",
		"authorize",
		map[string]any{
			"action":          strings.TrimSpace(action),
			"actor_id":        strings.TrimSpace(actor.ID),
			"target_agent_id": strings.TrimSpace(target.ID),
		},
	)
}

func sourceRolesAndToolGrants(source semanticview.Source) ([]string, map[string]map[string]struct{}) {
	if source == nil {
		return nil, map[string]map[string]struct{}{}
	}
	roles := make([]string, 0, len(source.AgentEntries()))
	grants := make(map[string]map[string]struct{}, len(source.AgentEntries()))
	for key, entry := range source.AgentEntries() {
		role := canonicalRole(firstNonEmpty(entry.Role, key))
		if role == "" {
			continue
		}
		if _, ok := grants[role]; !ok {
			roles = append(roles, role)
			grants[role] = map[string]struct{}{}
		}
		for _, toolName := range entry.ConfiguredTools() {
			toolName = strings.TrimSpace(toolName)
			if toolName == "" {
				continue
			}
			grants[role][toolName] = struct{}{}
		}
	}
	sort.Strings(roles)
	return roles, grants
}

func buildProducerRegistry(source semanticview.Source) map[string][]string {
	if source == nil {
		return map[string][]string{}
	}
	agentEvents := make(map[string][]string)
	for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(source).Producers() {
		role := ""
		switch endpoint.Kind {
		case semanticview.EventEndpointAgent:
			role = canonicalRole(firstNonEmpty(endpoint.Role, endpoint.AgentID))
		case semanticview.EventEndpointNodeHandler, semanticview.EventEndpointNodeGenerated:
			role = canonicalRole(endpoint.NodeID)
		default:
			continue
		}
		if role == "" {
			continue
		}
		agentEvents[role] = appendUniqueSortedEvent(agentEvents[role], endpoint.Event.Authored)
	}
	return agentEvents
}

func buildManagerFallbackGraph(source semanticview.Source) map[string]string {
	if source == nil {
		return map[string]string{}
	}
	graph := make(map[string]string)
	for key, entry := range source.AgentEntries() {
		agentID := strings.TrimSpace(firstNonEmpty(entry.ID, key))
		if agentID == "" {
			continue
		}
		parent := strings.TrimSpace(entry.ManagerFallback)
		if parent == "" || parent == agentID {
			continue
		}
		graph[agentID] = parent
		agentRole := canonicalRole(firstNonEmpty(entry.Role, key))
		parentRole := canonicalRole(parent)
		if agentRole != "" && parentRole != "" && agentRole != parentRole {
			graph[agentRole] = parentRole
		}
	}
	return graph
}

func (p *sourceProvider) isManagedDescendant(actor, target models.AgentConfig) bool {
	actorIDs := uniqueGraphCandidates(strings.TrimSpace(actor.ID), canonicalRole(actor.Role))
	targetIDs := uniqueGraphCandidates(strings.TrimSpace(target.ID), canonicalRole(target.Role))
	if len(actorIDs) == 0 || len(targetIDs) == 0 {
		return false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	actorSet := make(map[string]struct{}, len(actorIDs))
	for _, actorID := range actorIDs {
		if actorID == "" {
			continue
		}
		actorSet[actorID] = struct{}{}
	}
	for _, targetID := range targetIDs {
		if targetID == "" {
			continue
		}
		current := targetID
		visited := map[string]struct{}{current: {}}
		for {
			parent := strings.TrimSpace(p.parentByAgent[current])
			if parent == "" {
				break
			}
			if _, ok := actorSet[parent]; ok {
				return true
			}
			if _, seen := visited[parent]; seen {
				break
			}
			visited[parent] = struct{}{}
			current = parent
		}
	}
	return false
}

func uniqueGraphCandidates(values ...string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func strongestMessagePermission(grants map[string]struct{}) string {
	switch {
	case hasToolGrant(grants, "message_flow"):
		return "message_flow"
	case hasToolGrant(grants, "message_peers"):
		return "message_peers"
	default:
		return ""
	}
}

func permissionSet(perms []string) map[string]struct{} {
	out := make(map[string]struct{}, len(perms))
	for _, perm := range perms {
		perm = strings.TrimSpace(perm)
		if perm == "" {
			continue
		}
		out[perm] = struct{}{}
	}
	return out
}

func hasToolGrant(grants map[string]struct{}, toolName string) bool {
	if len(grants) == 0 {
		return false
	}
	_, ok := grants[strings.TrimSpace(toolName)]
	return ok
}

func hasAnyToolGrant(grants map[string]struct{}, toolNames ...string) bool {
	for _, toolName := range toolNames {
		if hasToolGrant(grants, toolName) {
			return true
		}
	}
	return false
}

func canonicalRole(role string) string {
	role = strings.TrimSpace(strings.ToLower(role))
	role = strings.ReplaceAll(role, "_", "-")
	role = strings.Join(strings.Fields(role), "-")
	return role
}

func cloneRoles(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, role := range in {
		role = canonicalRole(role)
		if role == "" {
			continue
		}
		if _, ok := seen[role]; ok {
			continue
		}
		seen[role] = struct{}{}
		out = append(out, role)
	}
	sort.Strings(out)
	return out
}

func containsCanonical(items []string, target string) bool {
	target = canonicalRole(target)
	return slices.ContainsFunc(items, func(item string) bool {
		return canonicalRole(item) == target
	})
}

func containsNormalized(items []string, target string) bool {
	target = strings.TrimSpace(strings.ToLower(target))
	return slices.ContainsFunc(items, func(item string) bool {
		return strings.TrimSpace(strings.ToLower(item)) == target
	})
}

func appendUniqueSortedEvent(events []string, eventType string) []string {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return events
	}
	for _, existing := range events {
		if strings.TrimSpace(existing) == eventType {
			return events
		}
	}
	events = append(events, eventType)
	sort.Strings(events)
	return events
}

func SameFlowInstance(actor, target models.AgentConfig) bool {
	actorFlow := actor.CanonicalFlowPath()
	targetFlow := target.CanonicalFlowPath()
	return actorFlow != "" && actorFlow == targetFlow
}

func PeerManagerFallback(actor, target models.AgentConfig) bool {
	actorFallback := strings.TrimSpace(actor.ManagerFallback)
	targetFallback := strings.TrimSpace(target.ManagerFallback)
	return actorFallback != "" && actorFallback == targetFallback
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
