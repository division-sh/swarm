package productpolicy

import (
	"empireai/internal/events"
	"empireai/internal/models"
	"strings"
)

type Policy interface {
	EnforcePostTurn(role string, inbound events.Event, emitted []events.Event) error
	AdditionalTurnRequirement(role string, inbound events.Event) string
	ContractRemediationPrompt(role string, inbound events.Event, contractErr error) (string, bool)
	PreNormalizeEmitPayload(role string, inbound events.Event, eventType string, payload map[string]any) (map[string]any, bool)
	NormalizeEmitPayload(role string, inbound events.Event, eventType string, payload map[string]any) (map[string]any, bool)
	ValidateEmitTransition(role string, inbound events.Event, emitted events.Event) error
	NormalizeScanMode(raw string) string
	NormalizeScanPriority(raw string) string
	DefaultScanMode() string
	DiscoveryFallbackMode() string
	RubricNameForScanMode(mode string) string
	EmitsCategorySignals(mode string) bool
	EmitsTrendSignals(mode string) bool
	ExpectedScannerCount(mode string) int
	ScanDispatchKind(mode string) string
	ScanShardStage(mode string) string
	IsCorpusScanMode(mode string) bool
	CampaignModesForDirective(initialMode string, explicit bool) []string
	ParseDirectiveMode(text string) (string, bool)
	InterceptRuntimeHandledDirective(agent models.AgentConfig, inbound events.Event) bool
	AllowHumanTaskDecision(actor models.AgentConfig) bool
	AllowGlobalRouting(actor models.AgentConfig) bool
	AllowGlobalManagement(actor models.AgentConfig) bool
	AllowMailboxSend(actor models.AgentConfig) bool
	ManagerFallbackAgentID(agent models.AgentConfig) string
	WorkspaceClass(actor models.AgentConfig) string
	DiagnosticWorkspaceClass(role string) string
	PromptSchemaGuards() []PromptSchemaGuard
}

type PromptSchemaGuard struct {
	PromptFile       string
	EmitTool         string
	RequiredTopLevel []string
	ForbiddenTokens  []string
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

func ControlPlaneAgentID() string {
	policy := DefaultOrNil()
	if policy == nil {
		return ""
	}
	return strings.TrimSpace(policy.ManagerFallbackAgentID(models.AgentConfig{}))
}

func NormalizeScanMode(raw string) string {
	policy := DefaultOrNil()
	if policy == nil {
		return ""
	}
	return strings.TrimSpace(policy.NormalizeScanMode(raw))
}

func NormalizeScanPriority(raw string) string {
	policy := DefaultOrNil()
	if policy == nil {
		return ""
	}
	return strings.TrimSpace(policy.NormalizeScanPriority(raw))
}

func DefaultScanMode() string {
	policy := DefaultOrNil()
	if policy == nil {
		return ""
	}
	return strings.TrimSpace(policy.DefaultScanMode())
}

func DiscoveryFallbackMode() string {
	policy := DefaultOrNil()
	if policy == nil {
		return ""
	}
	return strings.TrimSpace(policy.DiscoveryFallbackMode())
}

func RubricNameForScanMode(mode string) string {
	policy := DefaultOrNil()
	if policy == nil {
		return ""
	}
	return strings.TrimSpace(policy.RubricNameForScanMode(mode))
}

func EmitsCategorySignals(mode string) bool {
	policy := DefaultOrNil()
	if policy == nil {
		return false
	}
	return policy.EmitsCategorySignals(mode)
}

func EmitsTrendSignals(mode string) bool {
	policy := DefaultOrNil()
	if policy == nil {
		return false
	}
	return policy.EmitsTrendSignals(mode)
}

func ExpectedScannerCount(mode string) int {
	policy := DefaultOrNil()
	if policy == nil {
		return 0
	}
	return policy.ExpectedScannerCount(mode)
}

func ScanDispatchKind(mode string) string {
	policy := DefaultOrNil()
	if policy == nil {
		return ""
	}
	return strings.TrimSpace(policy.ScanDispatchKind(mode))
}

func ScanShardStage(mode string) string {
	policy := DefaultOrNil()
	if policy == nil {
		return ""
	}
	return strings.TrimSpace(policy.ScanShardStage(mode))
}

func IsCorpusScanMode(mode string) bool {
	policy := DefaultOrNil()
	if policy == nil {
		return false
	}
	return policy.IsCorpusScanMode(mode)
}

func CampaignModesForDirective(initialMode string, explicit bool) []string {
	policy := DefaultOrNil()
	if policy == nil {
		return nil
	}
	return append([]string(nil), policy.CampaignModesForDirective(initialMode, explicit)...)
}

func ParseDirectiveMode(text string) (string, bool) {
	policy := DefaultOrNil()
	if policy == nil {
		return "", false
	}
	return policy.ParseDirectiveMode(text)
}
