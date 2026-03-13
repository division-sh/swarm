package commgraph

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	models "empireai/internal/runtime/core/actors"
)

type RoutingAuthority struct {
	ActorRole          string
	AllowedTargetRoles []string
	AllowedStatuses    []string
	StatusDenyReason   string
	TargetDenyReason   string
}

type ManagementAuthority struct {
	ActorRole             string
	AllowedTargetRoles    []string
	AllowCrossEntity      bool
	CrossEntityDenyReason string
	TargetDenyReason      string
}

var (
	routingAuthorityOnce    sync.Once
	routingAuthorityData    []RoutingAuthority
	managementAuthorityOnce sync.Once
	managementAuthorityData []ManagementAuthority
	mailboxSendRoleOnce     sync.Once
	mailboxSendRoleData     []string
)

func RoutingAuthorities() []RoutingAuthority {
	routingAuthorityOnce.Do(func() {
		routingAuthorityData = cloneRoutingAuthorities(baseRoutingAuthorities())
	})
	return cloneRoutingAuthorities(routingAuthorityData)
}

func ManagementAuthorities() []ManagementAuthority {
	managementAuthorityOnce.Do(func() {
		managementAuthorityData = cloneManagementAuthorities(baseManagementAuthorities())
	})
	return cloneManagementAuthorities(managementAuthorityData)
}

func MailboxSendRoles() []string {
	mailboxSendRoleOnce.Do(func() {
		mailboxSendRoleData = cloneRoles(baseMailboxSendRoles())
	})
	return cloneRoles(mailboxSendRoleData)
}

func HasMessageAuthority(actor, target models.AgentConfig) bool {
	sender := canonicalRole(actor.Role)
	recipient := canonicalRole(target.Role)
	if sender == "" || recipient == "" {
		return false
	}
	for _, rule := range MessageAuthorities() {
		if canonicalRole(rule.SenderRole) != sender {
			continue
		}
		if !messageScopeAllowed(actor, target, rule.Scope) {
			continue
		}
		if containsCanonical(rule.RecipientRoles, recipient) {
			return true
		}
	}
	return false
}

func AuthorizeRouting(actor, target models.AgentConfig, status string) error {
	actorRole := canonicalRole(actor.Role)
	targetRole := canonicalRole(target.Role)
	status = strings.TrimSpace(strings.ToLower(status))
	for _, rule := range RoutingAuthorities() {
		if canonicalRole(rule.ActorRole) != actorRole {
			continue
		}
		if len(rule.AllowedStatuses) > 0 && !containsNormalized(rule.AllowedStatuses, status) {
			if strings.TrimSpace(rule.StatusDenyReason) != "" {
				return errors.New(strings.TrimSpace(rule.StatusDenyReason))
			}
			return fmt.Errorf("role %s is not authorized to configure routing", actor.Role)
		}
		if len(rule.AllowedTargetRoles) > 0 && !containsCanonical(rule.AllowedTargetRoles, targetRole) {
			if strings.TrimSpace(rule.TargetDenyReason) != "" {
				return errors.New(strings.TrimSpace(rule.TargetDenyReason))
			}
			return fmt.Errorf("role %s is not authorized to configure routing", actor.Role)
		}
		return nil
	}
	return fmt.Errorf("role %s is not authorized to configure routing", actor.Role)
}

func AuthorizeManagement(actor models.AgentConfig, targetRole, targetEntityID string) error {
	actorRole := canonicalRole(actor.Role)
	targetRole = canonicalRole(targetRole)
	targetEntityID = strings.TrimSpace(targetEntityID)
	actorEntityID := actor.EffectiveEntityID()
	for _, rule := range ManagementAuthorities() {
		if canonicalRole(rule.ActorRole) != actorRole {
			continue
		}
		if !rule.AllowCrossEntity && actorEntityID != "" && targetEntityID != "" && actorEntityID != targetEntityID {
			if strings.TrimSpace(rule.CrossEntityDenyReason) != "" {
				return errors.New(strings.TrimSpace(rule.CrossEntityDenyReason))
			}
			return errors.New("cross-entity management is not allowed")
		}
		if len(rule.AllowedTargetRoles) > 0 && !containsCanonical(rule.AllowedTargetRoles, targetRole) {
			if strings.TrimSpace(rule.TargetDenyReason) != "" {
				return errors.New(strings.TrimSpace(rule.TargetDenyReason))
			}
			return fmt.Errorf("role %s is not authorized to manage agents", actor.Role)
		}
		return nil
	}
	return fmt.Errorf("role %s is not authorized to manage agents", actor.Role)
}

func AuthorizeMailboxSend(actor models.AgentConfig) error {
	if containsCanonical(MailboxSendRoles(), actor.Role) {
		return nil
	}
	return fmt.Errorf("role %s is not authorized to send mailbox items", actor.Role)
}

func baseRoutingAuthorities() []RoutingAuthority {
	policy := defaultPolicyOrNil()
	if policy == nil {
		return nil
	}
	return policy.RoutingAuthorities()
}

func baseManagementAuthorities() []ManagementAuthority {
	policy := defaultPolicyOrNil()
	if policy == nil {
		return nil
	}
	return policy.ManagementAuthorities()
}

func baseMailboxSendRoles() []string {
	policy := defaultPolicyOrNil()
	if policy == nil {
		return nil
	}
	return policy.MailboxSendRoles()
}

func cloneRoutingAuthorities(in []RoutingAuthority) []RoutingAuthority {
	out := make([]RoutingAuthority, len(in))
	for i, rule := range in {
		out[i] = RoutingAuthority{
			ActorRole:          rule.ActorRole,
			AllowedTargetRoles: append([]string(nil), rule.AllowedTargetRoles...),
			AllowedStatuses:    append([]string(nil), rule.AllowedStatuses...),
			StatusDenyReason:   rule.StatusDenyReason,
			TargetDenyReason:   rule.TargetDenyReason,
		}
	}
	return out
}

func cloneManagementAuthorities(in []ManagementAuthority) []ManagementAuthority {
	out := make([]ManagementAuthority, len(in))
	for i, rule := range in {
		out[i] = ManagementAuthority{
			ActorRole:             rule.ActorRole,
			AllowedTargetRoles:    append([]string(nil), rule.AllowedTargetRoles...),
			AllowCrossEntity:      rule.AllowCrossEntity,
			CrossEntityDenyReason: rule.CrossEntityDenyReason,
			TargetDenyReason:      rule.TargetDenyReason,
		}
	}
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

func messageScopeAllowed(actor, target models.AgentConfig, scope string) bool {
	scope = strings.TrimSpace(strings.ToLower(scope))
	switch scope {
	case "", "any":
		return true
	case "global":
		return actor.EffectiveEntityID() == "" && target.EffectiveEntityID() == ""
	case "entity":
		actorEntityID := actor.EffectiveEntityID()
		targetEntityID := target.EffectiveEntityID()
		return actorEntityID != "" && actorEntityID == targetEntityID
	case "local":
		return actor.EffectiveEntityID() == target.EffectiveEntityID()
	default:
		return false
	}
}
