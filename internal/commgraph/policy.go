package commgraph

import (
	"sort"
	"strings"

	"empireai/internal/runtime/semanticview"
)

type Policy interface {
	MessageAuthorities() []MessageAuthority
	MailboxRoundTrips() []MailboxRoundTrip
	HumanTaskDecisionRoles() []string
	RoutingAuthorities() []RoutingAuthority
	ManagementAuthorities() []ManagementAuthority
	MailboxSendRoles() []string
}

var defaultPolicyFactory func() Policy

func SetDefaultPolicyFactory(factory func() Policy) {
	defaultPolicyFactory = factory
	resetDerivedCaches()
}

func defaultPolicyOrNil() Policy {
	if defaultPolicyFactory == nil {
		return nil
	}
	return defaultPolicyFactory()
}

type sourcePolicy struct {
	messageAuthorities []MessageAuthority
	mailboxRoundTrips  []MailboxRoundTrip
	humanTaskRoles     []string
	routingRules       []RoutingAuthority
	managementRules    []ManagementAuthority
	mailboxSendRoles   []string
}

func NewSourcePolicy(source semanticview.Source) Policy {
	allRoles, toolGrants := sourceRolesAndToolGrants(source)

	messageAuthorities := make([]MessageAuthority, 0, len(allRoles))
	routingRules := make([]RoutingAuthority, 0, len(allRoles))
	managementRules := make([]ManagementAuthority, 0, len(allRoles))
	humanTaskRoles := make([]string, 0, len(allRoles))
	mailboxSendRoles := append([]string(nil), allRoles...)

	for _, role := range allRoles {
		grants := toolGrants[role]
		switch strongestMessagePermission(grants) {
		case "message_all":
			messageAuthorities = append(messageAuthorities, MessageAuthority{
				SenderRole:     role,
				RecipientRoles: append([]string(nil), allRoles...),
				Scope:          "any",
			})
		case "message_domain":
			messageAuthorities = append(messageAuthorities, MessageAuthority{
				SenderRole:     role,
				RecipientRoles: append([]string(nil), allRoles...),
				Scope:          "entity",
			})
		case "message_peers":
			messageAuthorities = append(messageAuthorities, MessageAuthority{
				SenderRole:     role,
				RecipientRoles: append([]string(nil), allRoles...),
				Scope:          "local",
			})
		}
		if hasToolGrant(grants, "configure_routing") {
			routingRules = append(routingRules, RoutingAuthority{
				ActorRole:          role,
				AllowedTargetRoles: append([]string(nil), allRoles...),
			})
		}
		if hasAnyToolGrant(grants, "agent_hire", "agent_fire", "agent_reconfigure") {
			managementRules = append(managementRules, ManagementAuthority{
				ActorRole:          role,
				AllowedTargetRoles: append([]string(nil), allRoles...),
				AllowCrossEntity:   strongestMessagePermission(grants) == "message_all",
			})
		}
		if hasToolGrant(grants, "human_task_decide") {
			humanTaskRoles = append(humanTaskRoles, role)
		}
	}

	return sourcePolicy{
		messageAuthorities: messageAuthorities,
		humanTaskRoles:     humanTaskRoles,
		routingRules:       routingRules,
		managementRules:    managementRules,
		mailboxSendRoles:   mailboxSendRoles,
	}
}

func (p sourcePolicy) MessageAuthorities() []MessageAuthority {
	return cloneAuthorities(p.messageAuthorities)
}

func (p sourcePolicy) MailboxRoundTrips() []MailboxRoundTrip {
	out := make([]MailboxRoundTrip, len(p.mailboxRoundTrips))
	copy(out, p.mailboxRoundTrips)
	return out
}

func (p sourcePolicy) HumanTaskDecisionRoles() []string {
	return cloneRoles(p.humanTaskRoles)
}

func (p sourcePolicy) RoutingAuthorities() []RoutingAuthority {
	return cloneRoutingAuthorities(p.routingRules)
}

func (p sourcePolicy) ManagementAuthorities() []ManagementAuthority {
	return cloneManagementAuthorities(p.managementRules)
}

func (p sourcePolicy) MailboxSendRoles() []string {
	return cloneRoles(p.mailboxSendRoles)
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
		for _, toolName := range entry.ToolsTier2 {
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

func strongestMessagePermission(grants map[string]struct{}) string {
	switch {
	case hasToolGrant(grants, "message_all"):
		return "message_all"
	case hasToolGrant(grants, "message_domain"):
		return "message_domain"
	case hasToolGrant(grants, "message_peers"):
		return "message_peers"
	default:
		return ""
	}
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
