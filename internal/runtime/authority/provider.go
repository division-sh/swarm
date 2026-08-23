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
	AuthorizeNotifyHuman(actor models.AgentConfig) error
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

func (noopProvider) AuthorizeNotifyHuman(actor models.AgentConfig) error {
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
