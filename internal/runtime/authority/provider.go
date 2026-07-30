package authority

import (
	"fmt"
	"strings"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
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
	UpsertManagedAgent(identity, parent agentidentity.Identity) error
	RemoveManagedAgent(identity agentidentity.Identity) error
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
	same, err := SameAgent(actor, target)
	return err == nil && same
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

// SameAgent compares the complete concrete identity carried by two runtime
// descriptors. Malformed descriptors cannot participate in authorization.
func SameAgent(actor, target models.AgentConfig) (bool, error) {
	actorIdentity, err := actor.ConcreteIdentity()
	if err != nil {
		return false, fmt.Errorf("actor concrete identity: %w", err)
	}
	targetIdentity, err := target.ConcreteIdentity()
	if err != nil {
		return false, fmt.Errorf("target concrete identity: %w", err)
	}
	return agentidentity.Equal(actorIdentity, targetIdentity)
}

func UpsertManagedAgent(provider Provider, identity, parent agentidentity.Identity) error {
	if mutable, ok := ProviderOrNoop(provider).(graphMutableProvider); ok && mutable != nil {
		return mutable.UpsertManagedAgent(identity, parent)
	}
	return nil
}

func RemoveManagedAgent(provider Provider, identity agentidentity.Identity) error {
	if mutable, ok := ProviderOrNoop(provider).(graphMutableProvider); ok && mutable != nil {
		return mutable.RemoveManagedAgent(identity)
	}
	return nil
}
