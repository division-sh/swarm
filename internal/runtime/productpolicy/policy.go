package productpolicy

import (
	"empireai/internal/events"
	"empireai/internal/models"
)

type Policy interface {
	EnforcePostTurn(role string, inbound events.Event, emitted []events.Event) error
	AdditionalTurnRequirement(role string, inbound events.Event) string
	ContractRemediationPrompt(role string, inbound events.Event, contractErr error) (string, bool)
	PreNormalizeEmitPayload(role string, inbound events.Event, eventType string, payload map[string]any) (map[string]any, bool)
	NormalizeEmitPayload(role string, inbound events.Event, eventType string, payload map[string]any) (map[string]any, bool)
	ValidateEmitTransition(role string, inbound events.Event, emitted events.Event) error
	InterceptRuntimeHandledDirective(agent models.AgentConfig, inbound events.Event) bool
	AllowHumanTaskDecision(actor models.AgentConfig) bool
	AllowGlobalRouting(actor models.AgentConfig) bool
	AllowGlobalManagement(actor models.AgentConfig) bool
	AllowMailboxSend(actor models.AgentConfig) bool
	ManagerFallbackAgentID(agent models.AgentConfig) string
}

var defaultPolicyFactory func() Policy

func SetDefaultFactory(factory func() Policy) {
	defaultPolicyFactory = factory
}

func DefaultOrNil() Policy {
	if defaultPolicyFactory == nil {
		return nil
	}
	return defaultPolicyFactory()
}
