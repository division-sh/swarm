package authority

import (
	"slices"
	"sort"
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type sourceProvider struct {
	mailboxSendRoles []string
	producerRoles    []string
	agentEvents      map[string][]string
}

func NewSourceProvider(source semanticview.Source) Provider {
	return buildSourceProvider(source)
}

func buildSourceProvider(source semanticview.Source) Provider {
	if source == nil {
		return noopProvider{}
	}
	allRoles := sourceRoles(source)
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
	if same, err := SameAgent(actor, target); err == nil && same {
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

func sourceRoles(source semanticview.Source) []string {
	if source == nil {
		return nil
	}
	declarations := semanticview.AgentDeclarations(source)
	roles := make([]string, 0, len(declarations))
	seen := make(map[string]struct{}, len(declarations))
	for _, declaration := range declarations {
		plan, err := semanticview.ScopedAgentNamePlan(source, declaration)
		if err != nil {
			continue
		}
		role := canonicalRole(plan.EffectiveRole(declaration.Entry))
		if role == "" {
			continue
		}
		if _, ok := seen[role]; !ok {
			roles = append(roles, role)
			seen[role] = struct{}{}
		}
	}
	sort.Strings(roles)
	return roles
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
