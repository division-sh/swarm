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
