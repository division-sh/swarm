package platformschema

// RetiredPlatformTable identifies a table formerly owned by the platform that
// cannot coexist with the current pre-1.0 schema.
type RetiredPlatformTable string

const (
	RetiredPlatformSchemaVersion                 RetiredPlatformTable = "schema_version"
	RetiredPlatformDecisionCardLifecycleOutbox   RetiredPlatformTable = "decision_card_lifecycle_outbox"
	RetiredPlatformAgentExternalEffectOperations RetiredPlatformTable = "agent_external_effect_operations"
	RetiredPlatformAgentExternalEffectAttempts   RetiredPlatformTable = "agent_external_effect_attempts"
	RetiredPlatformSchedules                     RetiredPlatformTable = "schedules"
)

func RetiredPlatformTables() []RetiredPlatformTable {
	return []RetiredPlatformTable{
		RetiredPlatformSchemaVersion,
		RetiredPlatformDecisionCardLifecycleOutbox,
		RetiredPlatformAgentExternalEffectOperations,
		RetiredPlatformAgentExternalEffectAttempts,
		RetiredPlatformSchedules,
	}
}
