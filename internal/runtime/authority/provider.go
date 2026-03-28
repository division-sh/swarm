package authority

import (
	"errors"
	"strings"
	"sync"

	models "swarm/internal/runtime/core/actors"
)

type Provider interface {
	CanonicalRole(role string) string
	ProducerRoles() []string
	ProducerEventsForRole(role string) []string
	HasMessageAuthority(actor, target models.AgentConfig) bool
	AuthorizeRouting(actor, target models.AgentConfig, status string) error
	AuthorizeManagement(actor, target models.AgentConfig) error
	AuthorizeMailboxSend(actor models.AgentConfig) error
	CanDecideHumanTasks(role string) bool
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
	return errors.New("routing authority provider is not configured")
}

func (noopProvider) AuthorizeManagement(actor, target models.AgentConfig) error {
	return errors.New("management authority provider is not configured")
}

func (noopProvider) AuthorizeMailboxSend(actor models.AgentConfig) error {
	return errors.New("mailbox authority provider is not configured")
}

func (noopProvider) CanDecideHumanTasks(role string) bool { return false }

var (
	providerMu     sync.RWMutex
	activeProvider Provider = noopProvider{}
)

func SetProvider(provider Provider) {
	providerMu.Lock()
	defer providerMu.Unlock()
	if provider == nil {
		activeProvider = noopProvider{}
		return
	}
	activeProvider = provider
}

func Active() Provider {
	providerMu.RLock()
	defer providerMu.RUnlock()
	if activeProvider == nil {
		return noopProvider{}
	}
	return activeProvider
}

func UpsertManagedAgent(cfg models.AgentConfig) {
	providerMu.RLock()
	provider := activeProvider
	providerMu.RUnlock()
	if mutable, ok := provider.(graphMutableProvider); ok && mutable != nil {
		mutable.UpsertManagedAgent(cfg)
	}
}

func RemoveManagedAgent(agentID string) {
	providerMu.RLock()
	provider := activeProvider
	providerMu.RUnlock()
	if mutable, ok := provider.(graphMutableProvider); ok && mutable != nil {
		mutable.RemoveManagedAgent(agentID)
	}
}
