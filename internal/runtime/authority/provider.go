package authority

import (
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

type Provider interface {
	CanonicalRole(role string) string
	ProducerRoles() []string
	ProducerEventsForRole(role string) []string
	HasMessageAuthority(actor, target models.AgentConfig) bool
	AuthorizeRouting(actor, target models.AgentConfig, status string) error
	AuthorizeManagement(actor, target models.AgentConfig) error
	AuthorizeMailboxSend(actor models.AgentConfig) error
}

type graphMutableProvider interface {
	UpsertManagedAgent(cfg models.AgentConfig)
	RemoveManagedAgent(agentID string)
}

type noopProvider struct{}

func (noopProvider) CanonicalRole(role string) string {
	role = strings.TrimSpace(strings.ToLower(role))
	role = strings.ReplaceAll(role, "_", "-")
	role = strings.Join(strings.Fields(role), "-")
	return role
}

func (noopProvider) ProducerRoles() []string { return nil }

func (noopProvider) ProducerEventsForRole(string) []string { return nil }

func (noopProvider) HasMessageAuthority(actor, target models.AgentConfig) bool {
	if strings.TrimSpace(actor.ID) == "" || strings.TrimSpace(target.ID) == "" {
		return false
	}
	return strings.TrimSpace(actor.ID) == strings.TrimSpace(target.ID)
}

func (noopProvider) AuthorizeRouting(actor, target models.AgentConfig, status string) error {
	return failures.NewDetail("dependency_unavailable", "runtime-authority", "authorize_routing", map[string]any{"dependency": "routing_authority_provider"})
}

func (noopProvider) AuthorizeManagement(actor, target models.AgentConfig) error {
	return failures.NewDetail("dependency_unavailable", "runtime-authority", "authorize_management", map[string]any{"dependency": "management_authority_provider"})
}

func (noopProvider) AuthorizeMailboxSend(actor models.AgentConfig) error {
	return failures.NewDetail("dependency_unavailable", "runtime-authority", "authorize_mailbox", map[string]any{"dependency": "mailbox_authority_provider"})
}

func NoopProvider() Provider {
	return noopProvider{}
}

func ProviderOrNoop(provider Provider) Provider {
	if provider == nil {
		return noopProvider{}
	}
	return provider
}

func UpsertManagedAgent(provider Provider, cfg models.AgentConfig) {
	if mutable, ok := ProviderOrNoop(provider).(graphMutableProvider); ok && mutable != nil {
		mutable.UpsertManagedAgent(cfg)
	}
}

func RemoveManagedAgent(provider Provider, agentID string) {
	if mutable, ok := ProviderOrNoop(provider).(graphMutableProvider); ok && mutable != nil {
		mutable.RemoveManagedAgent(agentID)
	}
}
