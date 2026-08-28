package agentpersistence

import (
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"

	"github.com/division-sh/swarm/internal/stringsutil"
)

func DecodePersistedAgentRuntimeDescriptor(raw []byte) (PersistedAgentRuntimeDescriptor, error) {
	return decodePersistedAgentRuntimeDescriptor(raw)
}

func ExtractSubscriptions(raw []byte) []string { return extractSubscriptions(raw) }

func ValidateOpaqueAgentConfig(raw []byte) error { return validateOpaqueAgentConfig(raw) }

func NormalizeJSONPayload(raw []byte) string { return normalizeJSONPayload(raw) }

func Nullable(value, fallback string) string { return nullable(value, fallback) }

func SanitizeSchemaIdent(raw string) string { return sanitizeSchemaIdent(raw) }

func QuoteIdent(value string) string { return quoteIdent(value) }

func RedactPayloadValue(key string, value any) any { return redactPayloadValue(key, value) }

func RedactText(value string) string { return redactText(value) }

func RedactName(value string) string { return redactName(value) }

func IsNameKey(value string) bool { return isNameKey(value) }

func IsPaymentKey(value string) bool { return isPaymentKey(value) }

func ProjectAgentConfig(config runtimeactors.AgentConfig, parentAgentID string) (PersistedAgentProjection, error) {
	return ProjectPersistedAgentConfig(config, parentAgentID)
}

func HydrateAgentConfig(projection PersistedAgentProjection) (runtimeactors.AgentConfig, error) {
	return HydratePersistedAgentConfig(projection)
}

func PersistedStatus(raw string) string { return agentPersistedStatus(raw) }

func Coalesce(values ...string) string { return stringsutil.FirstNonEmpty(values...) }
